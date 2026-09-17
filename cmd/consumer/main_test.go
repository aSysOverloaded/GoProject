package main

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestRedis spins up an in-process Redis so the suite needs no external
// server. miniredis implements BLMOVE, ZADD NX/XX and the ZSET ordering we
// rely on.
func newTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	s := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// useFastTimings compresses the production timings so the tests run in
// milliseconds rather than tens of seconds.
func useFastTimings(t *testing.T) {
	t.Helper()
	oldVis, oldHB, oldSweep, oldBlock := visTimeout, heartbeatTick, sweepTick, blockTimeout
	visTimeout = 150 * time.Millisecond
	heartbeatTick = 40 * time.Millisecond
	sweepTick = 25 * time.Millisecond
	// Redis rejects sub-second blocking timeouts, so this one stays at 1s.
	blockTimeout = 1 * time.Second
	t.Cleanup(func() {
		visTimeout, heartbeatTick, sweepTick, blockTimeout = oldVis, oldHB, oldSweep, oldBlock
	})
}

func mustMarshal(t *testing.T, job Job) string {
	t.Helper()
	b, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}
	return string(b)
}

// startSweeper runs the real sweeper and returns a stop function.
func startSweeper(t *testing.T, rdb *redis.Client) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go sweeper(ctx, rdb, &wg)
	stop := func() {
		cancel()
		wg.Wait()
	}
	t.Cleanup(stop)
	return stop
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", within, what)
}

func queueContents(t *testing.T, rdb *redis.Client, key string) []string {
	t.Helper()
	out, err := rdb.LRange(context.Background(), key, 0, -1).Result()
	if err != nil {
		t.Fatalf("LRANGE %s: %v", key, err)
	}
	return out
}

// TestCrashBetweenPopAndClaimDoesNotLoseJob covers the BRPOP window that used
// to destroy jobs outright: a worker that dies after taking the job but before
// recording a deadline. The job must still be recoverable.
func TestCrashBetweenPopAndClaimDoesNotLoseJob(t *testing.T) {
	useFastTimings(t)
	rdb := newTestRedis(t)
	ctx := context.Background()

	original := Job{ID: "job-1", Payload: "work", Attempts: 0}
	if err := rdb.LPush(ctx, queueKey, mustMarshal(t, original)).Err(); err != nil {
		t.Fatalf("seed queue: %v", err)
	}

	// Simulate exactly what a worker does first - and then nothing else,
	// as if the process died on the very next instruction.
	popped, err := rdb.BLMove(ctx, queueKey, inflightKey, "RIGHT", "LEFT", blockTimeout).Result()
	if err != nil {
		t.Fatalf("BLMOVE: %v", err)
	}
	if popped == "" {
		t.Fatal("BLMOVE returned an empty job")
	}

	// The job is out of the queue but still referenced by the in-flight list,
	// which is the whole point of using BLMOVE instead of BRPOP.
	if got := queueContents(t, rdb, inflightKey); len(got) != 1 {
		t.Fatalf("expected 1 in-flight job, got %d", len(got))
	}

	startSweeper(t, rdb)

	waitFor(t, 3*time.Second, "the orphaned job to be requeued", func() bool {
		return len(queueContents(t, rdb, queueKey)) == 1
	})

	var recovered Job
	if err := json.Unmarshal([]byte(queueContents(t, rdb, queueKey)[0]), &recovered); err != nil {
		t.Fatalf("unmarshal recovered job: %v", err)
	}
	if recovered.ID != original.ID {
		t.Errorf("recovered job ID = %q, want %q", recovered.ID, original.ID)
	}
	if recovered.Attempts != 1 {
		t.Errorf("recovered job attempts = %d, want 1", recovered.Attempts)
	}
	if got := queueContents(t, rdb, inflightKey); len(got) != 0 {
		t.Errorf("in-flight list should be empty after reclaim, got %d entries", len(got))
	}
}

// TestHeartbeatKeepsSlowWorkerFromBeingReclaimed is the regression test for the
// original race: a healthy worker that simply takes longer than the visibility
// timeout used to be declared dead and have its job requeued underneath it.
func TestHeartbeatKeepsSlowWorkerFromBeingReclaimed(t *testing.T) {
	useFastTimings(t)
	rdb := newTestRedis(t)
	ctx := context.Background()

	job := Job{ID: "job-slow", Payload: "long work", Attempts: 0}
	jobJSON := mustMarshal(t, job)
	if err := rdb.LPush(ctx, queueKey, jobJSON).Err(); err != nil {
		t.Fatalf("seed queue: %v", err)
	}
	if _, err := rdb.BLMove(ctx, queueKey, inflightKey, "RIGHT", "LEFT", blockTimeout).Result(); err != nil {
		t.Fatalf("BLMOVE: %v", err)
	}
	if err := rdb.ZAdd(ctx, deadlinesKey, redis.Z{Score: deadlineFrom(time.Now()), Member: jobJSON}).Err(); err != nil {
		t.Fatalf("claim job: %v", err)
	}

	hbCtx, stopHeartbeat := context.WithCancel(context.Background())
	go heartbeat(hbCtx, 1, rdb, job.ID, jobJSON)

	startSweeper(t, rdb)

	// Stay "busy" for several visibility timeouts. The heartbeat should carry
	// the claim across all of them.
	time.Sleep(5 * visTimeout)

	if got := queueContents(t, rdb, queueKey); len(got) != 0 {
		t.Fatalf("sweeper requeued a job that was still being worked on: %v", got)
	}
	if got := queueContents(t, rdb, inflightKey); len(got) != 1 {
		t.Fatalf("expected the job to still be in flight, got %d entries", len(got))
	}

	// Once the heartbeat stops, the job must become reclaimable again -
	// otherwise we would have traded a false positive for a stuck job.
	stopHeartbeat()
	waitFor(t, 3*time.Second, "the job to be reclaimed after the heartbeat stops", func() bool {
		return len(queueContents(t, rdb, queueKey)) == 1
	})
}

// TestSettleDiscardsResultAfterSweeperReclaim covers the double-requeue bug: if
// the sweeper wins the race, the original worker's late result must be dropped
// rather than requeued a second time.
func TestSettleDiscardsResultAfterSweeperReclaim(t *testing.T) {
	useFastTimings(t)
	rdb := newTestRedis(t)
	ctx := context.Background()

	job := Job{ID: "job-contested", Payload: "work", Attempts: 0}
	jobJSON := mustMarshal(t, job)
	if err := rdb.RPush(ctx, inflightKey, jobJSON).Err(); err != nil {
		t.Fatalf("seed in-flight: %v", err)
	}
	// An already-expired claim, i.e. the sweeper is about to act.
	if err := rdb.ZAdd(ctx, deadlinesKey, redis.Z{
		Score:  float64(time.Now().Add(-time.Minute).UnixMilli()),
		Member: jobJSON,
	}).Err(); err != nil {
		t.Fatalf("claim job: %v", err)
	}

	// The sweeper reclaims and requeues: that is one requeue.
	reclaim(ctx, rdb, jobJSON)
	if got := queueContents(t, rdb, queueKey); len(got) != 1 {
		t.Fatalf("expected sweeper to requeue exactly 1 job, got %d", len(got))
	}

	// The original worker now finishes and reports a failure. It lost the
	// ownership handshake, so it must not requeue anything.
	settle(1, rdb, job, jobJSON, false, 10*time.Millisecond)

	if got := queueContents(t, rdb, queueKey); len(got) != 1 {
		t.Errorf("job was requeued twice: queue holds %d entries, want 1 (%v)", len(got), got)
	}

	var requeued Job
	if err := json.Unmarshal([]byte(queueContents(t, rdb, queueKey)[0]), &requeued); err != nil {
		t.Fatalf("unmarshal requeued job: %v", err)
	}
	if requeued.Attempts != 1 {
		t.Errorf("attempt counter = %d, want 1 (double-counting inflates this)", requeued.Attempts)
	}
}

// TestSettleSuccessLeavesNoResidue guards against leaking bookkeeping entries,
// which would make the sweeper rediscover completed work forever.
func TestSettleSuccessLeavesNoResidue(t *testing.T) {
	useFastTimings(t)
	rdb := newTestRedis(t)
	ctx := context.Background()

	job := Job{ID: "job-ok", Payload: "work", Attempts: 0}
	jobJSON := mustMarshal(t, job)
	if err := rdb.RPush(ctx, inflightKey, jobJSON).Err(); err != nil {
		t.Fatalf("seed in-flight: %v", err)
	}
	if err := rdb.ZAdd(ctx, deadlinesKey, redis.Z{Score: deadlineFrom(time.Now()), Member: jobJSON}).Err(); err != nil {
		t.Fatalf("claim job: %v", err)
	}

	settle(1, rdb, job, jobJSON, true, 10*time.Millisecond)

	if got := queueContents(t, rdb, inflightKey); len(got) != 0 {
		t.Errorf("in-flight list not cleared: %v", got)
	}
	if n, _ := rdb.ZCard(ctx, deadlinesKey).Result(); n != 0 {
		t.Errorf("deadlines ZSET not cleared: %d entries remain", n)
	}
	if got := queueContents(t, rdb, queueKey); len(got) != 0 {
		t.Errorf("successful job should not be requeued: %v", got)
	}
	if got := queueContents(t, rdb, dlqKey); len(got) != 0 {
		t.Errorf("successful job should not reach the DLQ: %v", got)
	}
}

// TestRetryOrBuryRoutesToDLQAtMaxAttempts pins the retry policy that both the
// worker and the sweeper now share.
func TestRetryOrBuryRoutesToDLQAtMaxAttempts(t *testing.T) {
	rdb := newTestRedis(t)
	ctx := context.Background()

	retryOrBury(ctx, rdb, "Worker 1", Job{ID: "job-retry", Attempts: maxAttempts - 1})
	if got := queueContents(t, rdb, queueKey); len(got) != 1 {
		t.Errorf("job below the attempt limit should be requeued, queue has %d entries", len(got))
	}
	if got := queueContents(t, rdb, dlqKey); len(got) != 0 {
		t.Errorf("job below the attempt limit should not be buried, DLQ has %d entries", len(got))
	}

	retryOrBury(ctx, rdb, "Worker 1", Job{ID: "job-dead", Attempts: maxAttempts})
	if got := queueContents(t, rdb, dlqKey); len(got) != 1 {
		t.Errorf("job at the attempt limit should go to the DLQ, DLQ has %d entries", len(got))
	}
	if got := queueContents(t, rdb, queueKey); len(got) != 1 {
		t.Errorf("job at the attempt limit should not be requeued, queue has %d entries", len(got))
	}
}
