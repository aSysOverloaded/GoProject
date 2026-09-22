package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestWorkerStopsTakingJobsOnceShutdownBegins reproduces what a rollout on
// Kubernetes showed: pods that were already shutting down took new jobs off
// the queue. A worker waiting for work sits inside a blocking BLMOVE for up to
// blockTimeout (15s in production). Shutdown cancels the run context - but if
// that does not interrupt the blocking read, a job that arrives during the
// drain is handed to a worker that is supposed to be stopping.
func TestWorkerStopsTakingJobsOnceShutdownBegins(t *testing.T) {
	useFastTimings(t)
	blockTimeout = 5 * time.Second // a long park, as in production (useFastTimings restores it)
	rdb := newTestRedis(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go worker(ctx, 1, rdb, &wg)

	// Let the worker park in BLMOVE on the empty queue, then begin shutdown.
	time.Sleep(300 * time.Millisecond)
	cancel()
	shutdownAt := time.Now()

	// A job arrives while the consumer is draining.
	time.Sleep(200 * time.Millisecond)
	job := mustMarshal(t, Job{ID: "late-arrival", EnqueueID: "x"})
	if err := rdb.LPush(context.Background(), queueKey, job).Err(); err != nil {
		t.Fatal(err)
	}

	stopped := waitTimeout(&wg, 8*time.Second)

	if got := queueContents(t, rdb, queueKey); len(got) != 1 || got[0] != job {
		t.Errorf("a worker that was already shutting down took a new job off the queue (queue now holds %v)", got)
	}
	if got := queueContents(t, rdb, inflightKey); len(got) != 0 {
		t.Errorf("job left in flight: %v", got)
	}
	if n, _ := rdb.ZCard(context.Background(), deadlinesKey).Result(); n != 0 {
		t.Errorf("%d deadline(s) left behind", n)
	}
	if !stopped {
		t.Fatal("worker never stopped")
	}
	if took := time.Since(shutdownAt); took > time.Second {
		t.Errorf("worker took %v to stop after shutdown began; it stayed parked in the blocking read", took.Round(time.Millisecond))
	}
}
