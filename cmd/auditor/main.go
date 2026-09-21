// Command auditor consumes the job event stream and archives it to Postgres.
//
// This is the consumer that justifies Kafka. Redis could dispatch work and
// Prometheus could count it, but neither can give you "replay the last three
// days of events into a new database" or "let three unrelated services each
// read the whole stream at their own pace". That is what a log gives you and
// a queue does not: in a queue, a consumed message is gone.
//
// Delivery is at-least-once by construction - the offset is committed only
// after the database write succeeds, so a crash in between replays the
// event. The write is idempotent on event_id, which turns at-least-once into
// effectively-once. That is the same argument the job queue itself makes
// about side effects, applied one layer up.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/twmb/franz-go/pkg/kgo"
)

// JobEvent mirrors the producer's schema. In a larger system this would be a
// shared package or, better, a schema registry with Avro or Protobuf so the
// two sides cannot drift apart silently.
type JobEvent struct {
	EventID    string    `json:"event_id"`
	JobID      string    `json:"job_id"`
	JobType    string    `json:"job_type"`
	Event      string    `json:"event"`
	Attempt    int       `json:"attempt"`
	DurationMS int64     `json:"duration_ms"`
	Error      string    `json:"error"`
	Reason     string    `json:"reason"`
	Worker     string    `json:"worker"`
	Host       string    `json:"host"`
	Timestamp  time.Time `json:"ts"`
}

func main() {
	fromStart := flag.Bool("from-start", false,
		"consume the topic from the beginning instead of resuming the group's committed offsets; this is the replay path")
	flag.Parse()

	if err := godotenv.Load(); err != nil {
		log.Println("[System] No .env file found, using system environment variables")
	}

	fmt.Println("==================================================")
	fmt.Println("     📜 Job Queue - Event Auditor (Kafka → SQL)    ")
	fmt.Println("==================================================")

	brokers := envOr("KAFKA_BROKERS", "localhost:9092")
	topic := envOr("KAFKA_TOPIC", "job-events")
	group := envOr("KAFKA_GROUP", "job-auditor")
	dbURL := envOr("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/jobqueue?sslmode=disable")

	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("[DB] Invalid DATABASE_URL: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)

	pingCtx, cancelPing := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelPing()
	if err := db.PingContext(pingCtx); err != nil {
		log.Fatalf("[DB] Cannot reach Postgres: %v", err)
	}
	log.Println("[DB] Connected to Postgres.")

	if err := ensureSchema(db); err != nil {
		// Not fatal. In Kubernetes, Postgres applies the schema itself from
		// an initdb ConfigMap, so schema.sql may legitimately be absent from
		// this container. Verify the table exists and carry on.
		log.Printf("[DB] Could not apply schema.sql (%v); checking whether the table already exists", err)
		if err := requireTable(db); err != nil {
			log.Fatalf("[DB] job_events table is missing and schema.sql could not be applied: %v", err)
		}
		log.Println("[DB] job_events table already present.")
	}

	resetOffset := kgo.NewOffset().AtCommitted()
	if *fromStart {
		// Replay. Because the writes are idempotent on event_id, re-reading
		// the whole topic re-derives the same table rather than duplicating
		// it. This is the property that makes a log worth keeping.
		log.Println("[Kafka] --from-start: replaying the topic from the beginning")
		resetOffset = kgo.NewOffset().AtStart()
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.ConsumeTopics(topic),

		// A consumer group is what lets several auditor pods share the
		// partitions of a topic, with Kafka reassigning them when a member
		// joins or leaves. A second, differently-named group would receive
		// its own independent copy of every event - that is the fan-out.
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(resetOffset),

		// Manual commits. With autocommit, Kafka could advance the offset
		// for records still sitting unwritten in memory, and a crash would
		// lose them outright - at-most-once. Committing only after the
		// database write makes it at-least-once, which the idempotent
		// insert then makes safe.
		kgo.DisableAutoCommit(),

		// Bound how long a batch can take before the group decides this
		// member is dead and rebalances its partitions away.
		kgo.SessionTimeout(30*time.Second),
	)
	if err != nil {
		log.Fatalf("[Kafka] Failed to create client: %v", err)
	}
	defer client.Close()

	log.Printf("[Kafka] Consuming topic %q as group %q via %s", topic, group, brokers)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	run(ctx, client, db)

	log.Println("[System] Shutdown complete.")
}

func run(ctx context.Context, client *kgo.Client, db *sql.DB) {
	var total, duplicates int

	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			log.Printf("[System] Stopping. Recorded %d events (%d were duplicates).", total, duplicates)
			return
		}

		// Fetch errors are per-topic-partition and usually transient
		// (rebalances, leader elections). Log and carry on; the records that
		// did arrive are still valid.
		fetches.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.Canceled) {
				log.Printf("[Kafka] Fetch error on %s[%d]: %v", t, p, err)
			}
		})

		batch := make([]record, 0, 64)
		fetches.EachRecord(func(r *kgo.Record) {
			var ev JobEvent
			if err := json.Unmarshal(r.Value, &ev); err != nil {
				// A malformed event must not stall the partition forever.
				// Log it, skip it, let the offset advance. In production
				// this would go to a dead-letter topic rather than a log
				// line.
				log.Printf("[Kafka] Skipping unparseable record at %s[%d]@%d: %v",
					r.Topic, r.Partition, r.Offset, err)
				return
			}
			batch = append(batch, record{event: ev, partition: r.Partition, offset: r.Offset})
		})

		if len(batch) == 0 {
			continue
		}

		inserted, err := writeBatch(ctx, db, batch)
		if err != nil {
			// Do NOT commit. The same records will be redelivered on the
			// next poll, which is exactly what should happen when the sink
			// is unavailable.
			log.Printf("[DB] Batch write failed, not committing offsets (will retry): %v", err)
			time.Sleep(time.Second)
			continue
		}

		// Only now is it safe to advance the offset.
		if err := client.CommitUncommittedOffsets(ctx); err != nil {
			// The rows are already written. Redelivery is harmless because
			// the insert is idempotent.
			log.Printf("[Kafka] Commit failed (rows are written; duplicates will be ignored on replay): %v", err)
		}

		total += len(batch)
		duplicates += len(batch) - inserted
		log.Printf("[Auditor] Recorded %d event(s), %d new, %d already present (running total %d)",
			len(batch), inserted, len(batch)-inserted, total)
	}
}

type record struct {
	event     JobEvent
	partition int32
	offset    int64
}

// writeBatch inserts a batch in one transaction and reports how many rows
// were genuinely new.
func writeBatch(ctx context.Context, db *sql.DB, batch []record) (int, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op once committed

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO job_events (
			event_id, job_id, job_type, event, attempt,
			duration_ms, error, reason, worker, host,
			occurred_at, kafka_partition, kafka_offset
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (event_id) DO NOTHING
	`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	inserted := 0
	for _, r := range batch {
		ev := r.event

		var durationMS any
		if ev.DurationMS > 0 {
			durationMS = ev.DurationMS
		}

		res, err := stmt.ExecContext(ctx,
			ev.EventID, ev.JobID, ev.JobType, ev.Event, ev.Attempt,
			durationMS, nullIfEmpty(ev.Error), nullIfEmpty(ev.Reason),
			nullIfEmpty(ev.Worker), nullIfEmpty(ev.Host),
			ev.Timestamp, r.partition, r.offset,
		)
		if err != nil {
			return inserted, fmt.Errorf("insert %s: %w", ev.EventID, err)
		}
		if n, err := res.RowsAffected(); err == nil && n > 0 {
			inserted++
		}
	}

	return inserted, tx.Commit()
}

func ensureSchema(db *sql.DB) error {
	// Read the checked-in schema if it is next to us, otherwise fall back to
	// the minimum needed to run. Keeping the file authoritative avoids two
	// copies of the DDL drifting apart.
	for _, path := range []string{"schema.sql", "../../schema.sql", "/schema.sql"} {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if _, err := db.Exec(string(data)); err != nil {
			return fmt.Errorf("applying %s: %w", path, err)
		}
		log.Printf("[DB] Applied schema from %s", path)
		return nil
	}
	return errors.New("schema.sql not found in ., ../.., or /")
}

// requireTable confirms the audit table exists, for the case where the
// schema was applied out of band.
func requireTable(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var exists bool
	err := db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'job_events')`,
	).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("table job_events does not exist")
	}
	return nil
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
