package main

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"jobqueue/internal/startup"
)

// testDB connects to DATABASE_URL. The rest of the suite needs no services;
// this one needs a real Postgres, because it is the SQL itself that is under
// test.
//
// It skips ONLY when DATABASE_URL is unset, so `go test ./...` still runs with
// nothing installed. When the variable IS set - as it always is in CI - a
// database that cannot be reached is a FAILURE, never a skip. A test that
// quietly does not run is how the bug this file covers survived in the first
// place.
func testDB(t *testing.T) *sql.DB {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping the Postgres-backed schema tests")
	}

	db, err := sql.Open("postgres", url)
	if err != nil {
		t.Fatalf("DATABASE_URL is set but unusable (%s): %v", url, err)
	}
	// One connection, so the search_path set below applies to every query.
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	// Tolerate a database that is still starting, then fail for real.
	if err := startup.Retry("Postgres", 20*time.Second, db.PingContext); err != nil {
		t.Fatalf("DATABASE_URL is set but Postgres is not reachable: %v", err)
	}
	return db
}

// tempSchema gives the test its own schema, so it never touches an existing
// job_events table, and drops it afterwards.
func tempSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	name := fmt.Sprintf("view_test_%d", time.Now().UnixNano())
	if _, err := db.Exec("CREATE SCHEMA " + name); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() { db.Exec("DROP SCHEMA " + name + " CASCADE") })
	if _, err := db.Exec("SET search_path TO " + name); err != nil {
		t.Fatalf("set search_path: %v", err)
	}

	ddl, err := os.ReadFile("../../schema.sql")
	if err != nil {
		t.Fatalf("read schema.sql: %v", err)
	}
	if _, err := db.Exec(string(ddl)); err != nil {
		t.Fatalf("apply schema.sql: %v", err)
	}
}

func insertEvent(t *testing.T, db *sql.DB, id, job, event string, occurredAt time.Time, offset int64) {
	t.Helper()
	_, err := db.Exec(`
		INSERT INTO job_events (event_id, job_id, job_type, event, attempt, occurred_at, kafka_partition, kafka_offset)
		VALUES ($1,$2,'',$3,1,$4,0,$5)`, id, job, event, occurredAt, offset)
	if err != nil {
		t.Fatalf("insert %s: %v", event, err)
	}
}

// TestJobCurrentStateUsesEventOrderNotClocks covers a flaw found by building
// the same system in Java: the view picked a job's latest event by comparing
// occurred_at, which is stamped by whichever process published the event.
//
// A job's events routinely come from different processes - the worker
// publishes "failed", the sweeper publishes "buried" - and on Kubernetes
// those are different pods with slightly different clocks. A few milliseconds
// of skew is enough to make the view report a stale event as the current one.
//
// Kafka's offsets are the authoritative order here: records are keyed by job
// ID, so all of one job's events go to a single partition, and within a
// partition offsets are monotonic in the order the broker accepted them.
func TestJobCurrentStateUsesEventOrderNotClocks(t *testing.T) {
	db := testDB(t)
	tempSchema(t, db)

	base := time.Now().UTC().Truncate(time.Millisecond)

	// Kafka accepted "failed" first (offset 10), then "buried" (offset 11).
	// But the pod that published "buried" has a clock 5ms behind, so by
	// timestamp alone "buried" looks older than "failed".
	insertEvent(t, db, "e-failed", "job-1", "failed", base, 10)
	insertEvent(t, db, "e-buried", "job-1", "buried", base.Add(-5*time.Millisecond), 11)

	var lastEvent string
	if err := db.QueryRow(`SELECT last_event FROM job_current_state WHERE job_id = 'job-1'`).Scan(&lastEvent); err != nil {
		t.Fatalf("query view: %v", err)
	}

	if lastEvent != "buried" {
		t.Errorf("view reports last_event = %q, want \"buried\": it is ordering by a per-process clock, "+
			"so a few milliseconds of skew between the worker's pod and the sweeper's pod reports a stale event as current", lastEvent)
	}
}

// TestJobCurrentStateStillWorksWithoutKafkaCoordinates guards the fallback:
// rows written without Kafka offsets must still yield a current state rather
// than disappearing from the view.
func TestJobCurrentStateStillWorksWithoutKafkaCoordinates(t *testing.T) {
	db := testDB(t)
	tempSchema(t, db)

	base := time.Now().UTC().Truncate(time.Millisecond)
	for _, r := range []struct {
		id, event string
		at        time.Time
	}{
		{"n-started", "started", base},
		{"n-succeeded", "succeeded", base.Add(time.Millisecond)},
	} {
		if _, err := db.Exec(`
			INSERT INTO job_events (event_id, job_id, job_type, event, attempt, occurred_at)
			VALUES ($1,'job-2','',$2,1,$3)`, r.id, r.event, r.at); err != nil {
			t.Fatalf("insert %s: %v", r.event, err)
		}
	}

	var lastEvent string
	if err := db.QueryRow(`SELECT last_event FROM job_current_state WHERE job_id = 'job-2'`).Scan(&lastEvent); err != nil {
		t.Fatalf("query view: %v", err)
	}
	if lastEvent != "succeeded" {
		t.Errorf("last_event = %q, want \"succeeded\" (rows without Kafka coordinates must fall back to timestamps)", lastEvent)
	}
}
