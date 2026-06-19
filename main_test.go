package main

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// getTestDB attempts to open a database connection and pings it.
// If it fails, it skips the test so that the test suite does not fail when no DB is available.
func getTestDB(t *testing.T) *sql.DB {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/jobqueue?sslmode=disable"
	}
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		t.Skipf("Skipping database-dependent test: failed to open connection: %v", err)
	}
	err = db.Ping()
	if err != nil {
		t.Skipf("Skipping database-dependent test: PostgreSQL not running on %s", dbURL)
	}
	return db
}

func TestDbWorkerGracefulShutdown(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	// Initialize the schema
	if err := initSchema(db); err != nil {
		t.Fatalf("Failed to initialize schema: %v", err)
	}

	// Clear out any old jobs for a clean test
	_, err := db.Exec("DELETE FROM jobs")
	if err != nil {
		t.Fatalf("Failed to clear jobs table: %v", err)
	}

	// Enqueue a test job
	testJobID := "test-job-graceful-1"
	err = enqueueJob(context.Background(), db, testJobID, "test-payload")
	if err != nil {
		t.Fatalf("Failed to enqueue job: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// Start 1 worker
	wg.Add(1)
	go worker(ctx, 1, db, &wg)

	// Let the worker run and pick up the job
	time.Sleep(100 * time.Millisecond)

	// Cancel context (shutdown signal)
	cancel()

	// Wait for workers to finish
	wg.Wait()

	// Verify that the job was processed and state transitioned out of 'running'
	var status string
	var attempts int
	err = db.QueryRow("SELECT status, attempts FROM jobs WHERE id = $1", testJobID).Scan(&status, &attempts)
	if err != nil {
		t.Fatalf("Failed to fetch job status from DB: %v", err)
	}

	t.Logf("Job status after shutdown: %s, attempts: %d", status, attempts)

	// The status should not be 'running' because worker must finish processing before returning
	if status == "running" {
		t.Errorf("Expected job status to not be 'running', but got: %s", status)
	}
}
