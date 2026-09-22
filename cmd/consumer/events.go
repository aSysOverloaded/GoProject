package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Job lifecycle event types.
const (
	EventStarted   = "started"
	EventSucceeded = "succeeded"
	EventFailed    = "failed"
	EventRetried   = "retried"
	EventBuried    = "buried"
	EventRecovered = "recovered"
	EventPoisoned  = "poisoned"
	EventDiscarded = "discarded" // lost the ownership handshake
)

// JobEvent is one transition in a job's life, published to Kafka.
//
// Redis remains the work queue; this is the event log *about* that queue.
// The distinction matters: Redis answers "what should be worked on next",
// Kafka answers "what happened, in order, and can it be replayed".
type JobEvent struct {
	// EventID makes the event idempotent for consumers. A consumer that
	// crashes after writing to its database but before committing its offset
	// will see this event again; keying on EventID turns the redelivery into
	// a no-op. This is the same at-least-once-plus-idempotency argument the
	// job queue itself relies on.
	EventID string `json:"event_id"`

	JobID   string `json:"job_id"`
	JobType string `json:"job_type,omitempty"`
	Event   string `json:"event"`
	Attempt int    `json:"attempt"`

	DurationMS int64  `json:"duration_ms,omitempty"`
	Error      string `json:"error,omitempty"`
	Reason     string `json:"reason,omitempty"`

	Worker    string    `json:"worker,omitempty"`
	Host      string    `json:"host,omitempty"`
	Timestamp time.Time `json:"ts"`
}

// publisher wraps the Kafka client. A nil publisher is valid and does
// nothing, so Kafka stays entirely optional: with KAFKA_BROKERS unset the
// queue behaves exactly as it did before.
type publisher struct {
	client *kgo.Client
	topic  string
	host   string

	published atomic.Int64
	failed    atomic.Int64
}

var events *publisher

// newPublisher connects to Kafka, or returns nil if no brokers are
// configured.
func newPublisher() *publisher {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		log.Println("\033[1;30m[Events] KAFKA_BROKERS not set; event publishing disabled\033[0m")
		return nil
	}

	topic := envString("KAFKA_TOPIC", "job-events")
	maxBuffered := envInt("KAFKA_MAX_BUFFERED_RECORDS", 10000)
	deliveryTimeout := envDuration("KAFKA_DELIVERY_TIMEOUT", 30*time.Second)
	if deliveryTimeout < time.Second {
		// franz-go rejects a record timeout below one second, and a rejected
		// client config would silently disable events altogether.
		log.Printf("\033[1;33m[Events] KAFKA_DELIVERY_TIMEOUT %v is below the 1s minimum; using 1s\033[0m", deliveryTimeout)
		deliveryTimeout = time.Second
	}

	p, err := buildPublisher(strings.Split(brokers, ","), topic, maxBuffered, deliveryTimeout)
	if err != nil {
		log.Printf("\033[31m[Events] Failed to create Kafka client, continuing without events: %v\033[0m", err)
		return nil
	}

	// Create the topic explicitly rather than relying on the broker's
	// auto-create. Auto-create is disabled on most real clusters, and even
	// where it is on it silently gives you the broker's default partition
	// count and replication factor - which is how a topic ends up with one
	// partition and no redundancy in production. Being explicit means the
	// partition count is a decision, not an accident.
	ensureTopic(p.client, topic)

	log.Printf("\033[1;34m[Events] Publishing job events to Kafka topic %q via %s\033[0m", topic, brokers)
	return p
}

// buildPublisher creates the Kafka client. It is separate from newPublisher
// so tests can build one against an unreachable broker without environment
// variables or the topic-creation round trip.
func buildPublisher(brokers []string, topic string, maxBuffered int, deliveryTimeout time.Duration) (*publisher, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),

		// Together these two make a Kafka outage cost bounded memory rather
		// than unbounded memory. The cap limits how many events can wait at
		// once; the delivery timeout releases an event that could not be
		// delivered in time, instead of retrying it forever. Without the
		// timeout an unreachable broker holds every event indefinitely, and
		// the buffer never drains.
		kgo.MaxBufferedRecords(maxBuffered),
		kgo.RecordDeliveryTimeout(deliveryTimeout),

		// Idempotent producer: the broker de-duplicates retries using a
		// producer ID and sequence number, so a network retry cannot write
		// the same record twice. franz-go enables this by default; it is
		// spelled out here because it is a deliberate choice, not a default
		// we happened to inherit.
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),

		// Wait for all in-sync replicas. With one broker this is the same as
		// acks=1, but it is the correct setting for a real cluster and the
		// difference is exactly the replication trade-off: acks=1 returns
		// faster but loses the write if the leader dies before replicating.
		kgo.RequiredAcks(kgo.AllISRAcks()),

		// Small linger batches events without adding meaningful latency,
		// since production is asynchronous anyway.
		kgo.ProducerLinger(20*time.Millisecond),

		// Surface client-level problems (connection refused, unknown topic,
		// auth) instead of letting them vanish into a counter.
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelWarn, func() string {
			return "[Kafka] "
		})),
	)
	if err != nil {
		return nil, err
	}

	host, _ := os.Hostname()
	if podName := os.Getenv("POD_NAME"); podName != "" {
		host = podName
	}

	return &publisher{client: client, topic: topic, host: host}, nil
}

// ensureTopic creates the events topic if it does not exist. Several
// consumers starting at once will race; all but one get TOPIC_ALREADY_EXISTS,
// which is not an error condition.
func ensureTopic(client *kgo.Client, topic string) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	partitions := int32(3)
	if v := os.Getenv("KAFKA_PARTITIONS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			partitions = int32(n)
		}
	}

	// Replication factor 1 because this runs against a single broker. A real
	// cluster wants 3, together with min.insync.replicas=2 and acks=all -
	// all three are needed for durability, and acks=all alone on an
	// unreplicated topic buys nothing.
	replication := int16(1)
	if v := os.Getenv("KAFKA_REPLICATION_FACTOR"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			replication = int16(n)
		}
	}

	admin := kadm.NewClient(client)
	resp, err := admin.CreateTopic(ctx, partitions, replication, nil, topic)
	if err != nil {
		log.Printf("\033[1;33m[Events] Could not create topic %q (it may already exist): %v\033[0m", topic, err)
		return
	}
	if resp.Err != nil {
		if strings.Contains(resp.Err.Error(), "TOPIC_ALREADY_EXISTS") {
			log.Printf("\033[1;30m[Events] Topic %q already exists\033[0m", topic)
			return
		}
		log.Printf("\033[1;33m[Events] Topic %q creation reported: %v\033[0m", topic, resp.Err)
		return
	}
	log.Printf("\033[1;32m[Events] Created topic %q with %d partitions, replication factor %d\033[0m",
		topic, partitions, replication)
}

// publish sends an event without blocking the caller.
//
// This is the most important design decision in this file. Job processing
// must never wait on Kafka, and must never fail because Kafka is down. The
// event stream is observability, not the critical path: if the broker is
// unreachable the queue keeps draining and only the audit trail suffers.
// Making this synchronous would add a network round trip to every job and
// turn a Kafka outage into a job queue outage.
//
// "Asynchronous" was not enough on its own. The original code used Produce,
// which is asynchronous right up until the client's record buffer is full -
// and then it BLOCKS until space frees. With the broker unreachable, nothing
// ever frees space, so after ~10,000 events every worker hung here and the
// whole queue stalled. TryProduce fails immediately instead (ErrMaxBuffered),
// so a full buffer costs an audit event, never a job.
func (p *publisher) publish(ev JobEvent) {
	if p == nil {
		return
	}

	ev.Host = p.host
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now().UTC()
	}
	if ev.EventID == "" {
		ev.EventID = newEventID(ev)
	}

	payload, err := json.Marshal(ev)
	if err != nil {
		p.failed.Add(1)
		return
	}

	record := &kgo.Record{
		// Partitioning by job ID is what gives per-job ordering. Kafka
		// guarantees order within a partition, never across a topic, so
		// every event for one job must hash to the same partition -
		// otherwise "succeeded" could be consumed before "started".
		Key:   []byte(ev.JobID),
		Value: payload,
		Headers: []kgo.RecordHeader{
			{Key: "event-type", Value: []byte(ev.Event)},
		},
	}

	p.client.TryProduce(context.Background(), record, func(_ *kgo.Record, err error) {
		if err != nil {
			n := p.failed.Add(1)
			kafkaPublishErrors.Inc()
			// Log the first failure and then every 100th. Silently counting
			// errors makes a broken event stream impossible to diagnose,
			// but logging every one would drown the job logs.
			if n == 1 || n%100 == 0 {
				if errors.Is(err, kgo.ErrMaxBuffered) {
					log.Printf("\033[31m[Events] Kafka buffer full, dropping events (%d so far) - jobs are unaffected\033[0m", n)
				} else {
					log.Printf("\033[31m[Events] Publish failed (%d so far): %v\033[0m", n, err)
				}
			}
			return
		}
		p.published.Add(1)
		kafkaPublished.WithLabelValues(ev.Event).Inc()
	})
}

// close flushes anything still buffered. Called during shutdown, after the
// workers have stopped, so the audit trail is not truncated by a deploy.
func (p *publisher) close(ctx context.Context) {
	if p == nil {
		return
	}
	if err := p.client.Flush(ctx); err != nil {
		log.Printf("\033[31m[Events] Error flushing Kafka buffer: %v\033[0m", err)
	}
	log.Printf("\033[1;30m[Events] Published %d events (%d failed)\033[0m", p.published.Load(), p.failed.Load())
	p.client.Close()
}

// newEventID builds a deterministic-ish identifier for de-duplication. Job
// ID plus attempt plus event type plus timestamp is unique in practice for
// this workload; a real system would use a UUID.
func newEventID(ev JobEvent) string {
	return ev.JobID + "|" + ev.Event + "|" + strconv.Itoa(ev.Attempt) + "|" + ev.Timestamp.Format(time.RFC3339Nano)
}
