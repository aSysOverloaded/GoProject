package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestWorkerGracefulShutdown(t *testing.T) {
	// Create a channel with a buffer
	jobsChan := make(chan Job, 10)

	// Setup context that can be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// Start one worker
	wg.Add(1)
	go worker(ctx, 1, jobsChan, &wg)

	// Queue a job
	job := Job{
		ID:       "test-job-1",
		Name:     "Graceful test job",
		Payload:  "payload",
		Attempts: 0,
	}
	jobsChan <- job

	// Give the worker a split second to pick up the job
	time.Sleep(50 * time.Millisecond)

	// Signal stop by cancelling context while job is processing
	cancel()

	// Wait for the worker to finish
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// The worker should finish the job and exit. We enforce a timeout.
	select {
	case <-done:
		// Success! Worker shut down gracefully after finishing the job
	case <-time.After(2 * time.Second):
		t.Fatal("Worker did not shut down gracefully within timeout")
	}
}
