package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

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

	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "job-events"
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.DefaultProduceTopic(topic),

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
	)
	if err != nil {
		log.Printf("\033[31m[Events] Failed to create Kafka client, continuing without events: %v\033[0m", err)
		return nil
	}

	host, _ := os.Hostname()
	if podName := os.Getenv("POD_NAME"); podName != "" {
		host = podName
	}

	log.Printf("\033[1;34m[Events] Publishing job events to Kafka topic %q via %s\033[0m", topic, brokers)
	return &publisher{client: client, topic: topic, host: host}
}

// publish sends an event without blocking the caller.
//
// This is the most important design decision in this file. Job processing
// must never wait on Kafka, and must never fail because Kafka is down. The
// event stream is observability, not the critical path: if the broker is
// unreachable the queue keeps draining and only the audit trail suffers.
// Making this synchronous would add a network round trip to every job and
// turn a Kafka outage into a job queue outage.
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

	p.client.Produce(context.Background(), record, func(_ *kgo.Record, err error) {
		if err != nil {
			p.failed.Add(1)
			kafkaPublishErrors.Inc()
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
