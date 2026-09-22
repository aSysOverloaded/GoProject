package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
	oldBase, oldMax, oldPromote := baseRetryDelay, maxRetryDelay, promoteTick

	visTimeout = 150 * time.Millisecond
	heartbeatTick = 40 * time.Millisecond
	sweepTick = 25 * time.Millisecond
	// Redis rejects sub-second blocking timeouts, so this one stays at 1s.
	blockTimeout = 1 * time.Second
	baseRetryDelay = 10 * time.Millisecond
	maxRetryDelay = 50 * time.Millisecond
	promoteTick = 10 * time.Millisecond

	t.Cleanup(func() {
		visTimeout, heartbeatTick, sweepTick, blockTimeout = oldVis, oldHB, oldSweep, oldBlock
		baseRetryDelay, maxRetryDelay, promoteTick = oldBase, oldMax, oldPromote
	})
}

// delayedContents returns the jobs currently waiting out their retry backoff.
func delayedContents(t *testing.T, rdb *redis.Client) []string {
	t.Helper()
	out, err := rdb.ZRange(context.Background(), delayedKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("ZRANGE %s: %v", delayedKey, err)
	}
	return out
}

// drainDelayed promotes every delayed job onto the main queue, waiting for
// backoffs to elapse. Tests that care about the eventual queue state use this
// instead of running a promoter goroutine.
func drainDelayed(t *testing.T, rdb *redis.Client) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(delayedContents(t, rdb)) == 0 {
			return
		}
		promoteDue(context.Background(), rdb)
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("delayed set did not drain within 2s: %v", delayedContents(t, rdb))
}

// errFail is a generic retryable handler failure.
var errFail = errors.New("handler failed")

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

	// The reclaimed job goes into the delayed set to serve its backoff, then
	// the promoter returns it to the queue.
	waitFor(t, 3*time.Second, "the orphaned job to be scheduled for retry", func() bool {
		return len(delayedContents(t, rdb)) == 1
	})
	drainDelayed(t, rdb)

	if got := queueContents(t, rdb, queueKey); len(got) != 1 {
		t.Fatalf("expected 1 job back on the queue, got %d", len(got))
	}

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
	if got := delayedContents(t, rdb); len(got) != 0 {
		t.Fatalf("sweeper scheduled a retry for a job still being worked on: %v", got)
	}
	if got := queueContents(t, rdb, inflightKey); len(got) != 1 {
		t.Fatalf("expected the job to still be in flight, got %d entries", len(got))
	}

	// Once the heartbeat stops, the job must become reclaimable again -
	// otherwise we would have traded a false positive for a stuck job.
	stopHeartbeat()
	waitFor(t, 3*time.Second, "the job to be reclaimed after the heartbeat stops", func() bool {
		return len(delayedContents(t, rdb)) == 1
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

	// The sweeper reclaims and schedules a retry: that is one requeue.
	reclaim(ctx, rdb, jobJSON)
	if got := delayedContents(t, rdb); len(got) != 1 {
		t.Fatalf("expected sweeper to schedule exactly 1 retry, got %d", len(got))
	}

	staleBefore := testutil.ToFloat64(staleResults)
	workerRetriesBefore := testutil.ToFloat64(jobsRetried.WithLabelValues("worker"))

	// The original worker now finishes and reports a failure. It lost the
	// ownership handshake, so it must not requeue anything.
	settle(1, rdb, job, jobJSON, errFail, 10*time.Millisecond)

	// The counters are the reliable check here. A second requeue of this job
	// produces a byte-identical retry payload, and jobs:delayed is a ZSET,
	// which silently merges identical members - so counting the delayed set
	// alone cannot see a double requeue. This test used to rely on that
	// count, and removing the ownership check did not make it fail.
	if got := testutil.ToFloat64(staleResults) - staleBefore; got != 1 {
		t.Errorf("stale_results moved by %v, want 1: the worker did not recognise it had lost the job", got)
	}
	if got := testutil.ToFloat64(jobsRetried.WithLabelValues("worker")) - workerRetriesBefore; got != 0 {
		t.Errorf("worker scheduled %v retry/retries for a job the sweeper already took; want 0", got)
	}
	if got := delayedContents(t, rdb); len(got) != 1 {
		t.Errorf("job was requeued twice: delayed set holds %d entries, want 1 (%v)", len(got), got)
	}

	drainDelayed(t, rdb)
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

// TestLateSuccessAfterReclaimIsNotCounted covers the other half of the
// handshake: the worker SUCCEEDS after the sweeper has already rescheduled
// the job. The job will run again - that is the at-least-once guarantee, and
// the worker cannot take the retry back - but the worker must not also
// record a success for a job it no longer owns.
func TestLateSuccessAfterReclaimIsNotCounted(t *testing.T) {
	useFastTimings(t)
	rdb := newTestRedis(t)
	ctx := context.Background()

	job := Job{ID: "job-late", Payload: "work", EnqueueID: "late"}
	jobJSON := mustMarshal(t, job)
	rdb.RPush(ctx, inflightKey, jobJSON)
	rdb.ZAdd(ctx, deadlinesKey, redis.Z{
		Score:  float64(time.Now().Add(-time.Minute).UnixMilli()),
		Member: jobJSON,
	})
	reclaim(ctx, rdb, jobJSON)

	staleBefore := testutil.ToFloat64(staleResults)
	successBefore := testutil.ToFloat64(jobsProcessed.WithLabelValues("success"))

	settle(1, rdb, job, jobJSON, nil, 10*time.Millisecond)

	if got := testutil.ToFloat64(jobsProcessed.WithLabelValues("success")) - successBefore; got != 0 {
		t.Errorf("worker recorded %v success(es) for a job the sweeper had already taken", got)
	}
	if got := testutil.ToFloat64(staleResults) - staleBefore; got != 1 {
		t.Errorf("stale_results moved by %v, want 1", got)
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

	settle(1, rdb, job, jobJSON, nil, 10*time.Millisecond)

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

// failInFlight puts a job in flight with a live claim, exactly as a worker
// holds it, then settles it as failed with runErr - the real release path.
// settle counts the attempt that just ran, so a job seeded with Attempts n
// comes out with n+1.
func failInFlight(t *testing.T, rdb *redis.Client, job Job, runErr error) {
	t.Helper()
	ctx := context.Background()
	jobJSON := mustMarshal(t, job)
	rdb.RPush(ctx, inflightKey, jobJSON)
	rdb.ZAdd(ctx, deadlinesKey, redis.Z{Score: deadlineFrom(time.Now()), Member: jobJSON})
	settle(1, rdb, job, jobJSON, runErr, time.Millisecond)
}

// TestFailedJobRoutesToDLQAtMaxAttempts pins the retry policy that both the
// worker and the sweeper share.
func TestFailedJobRoutesToDLQAtMaxAttempts(t *testing.T) {
	useFastTimings(t)
	rdb := newTestRedis(t)

	// Fails on its second-to-last allowed attempt: must be retried.
	failInFlight(t, rdb, Job{ID: "job-retry", Attempts: maxAttempts - 2}, errFail)
	if got := delayedContents(t, rdb); len(got) != 1 {
		t.Errorf("job below the attempt limit should be scheduled for retry, delayed set has %d entries", len(got))
	}
	if got := queueContents(t, rdb, dlqKey); len(got) != 0 {
		t.Errorf("job below the attempt limit should not be buried, DLQ has %d entries", len(got))
	}

	// Fails on its last allowed attempt: must be buried.
	failInFlight(t, rdb, Job{ID: "job-dead", Attempts: maxAttempts - 1}, errFail)
	if got := queueContents(t, rdb, dlqKey); len(got) != 1 {
		t.Errorf("job at the attempt limit should go to the DLQ, DLQ has %d entries", len(got))
	}
	if got := delayedContents(t, rdb); len(got) != 1 {
		t.Errorf("job at the attempt limit should not be retried, delayed set has %d entries", len(got))
	}
}

// TestPermanentErrorSkipsRetries checks that a handler reporting ErrPermanent
// is buried immediately instead of burning its whole retry budget and the
// backoff waits on an outcome that cannot change.
func TestPermanentErrorSkipsRetries(t *testing.T) {
	useFastTimings(t)
	rdb := newTestRedis(t)

	// Attempt 1 of 3: normally this would be retried.
	permanent := fmt.Errorf("bad payload: %w", ErrPermanent)
	failInFlight(t, rdb, Job{ID: "job-poisonous", Attempts: 0}, permanent)

	if got := delayedContents(t, rdb); len(got) != 0 {
		t.Errorf("permanent failure should not be retried, delayed set has %d entries: %v", len(got), got)
	}
	if got := queueContents(t, rdb, dlqKey); len(got) != 1 {
		t.Fatalf("permanent failure should go straight to the DLQ, DLQ has %d entries", len(got))
	}
}

// TestBackoffGrowsAndIsCapped covers the shape of the retry delay.
func TestBackoffGrowsAndIsCapped(t *testing.T) {
	oldBase, oldMax := baseRetryDelay, maxRetryDelay
	baseRetryDelay = 1 * time.Second
	maxRetryDelay = 10 * time.Second
	t.Cleanup(func() { baseRetryDelay, maxRetryDelay = oldBase, oldMax })

	// Jitter keeps each delay within [50%, 100%] of the nominal value, so the
	// assertions are on bounds rather than exact numbers.
	for _, tc := range []struct {
		attempt int
		nominal time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 10 * time.Second}, // 16s, capped
		{9, 10 * time.Second}, // would overflow without the cap
	} {
		for i := 0; i < 50; i++ {
			got := backoffFor(tc.attempt)
			if got < tc.nominal/2 || got > tc.nominal {
				t.Fatalf("backoffFor(%d) = %v, want within [%v, %v]",
					tc.attempt, got, tc.nominal/2, tc.nominal)
			}
		}
	}

	// Jitter must actually vary, otherwise a thundering herd survives.
	seen := map[time.Duration]bool{}
	for i := 0; i < 50; i++ {
		seen[backoffFor(3)] = true
	}
	if len(seen) < 10 {
		t.Errorf("backoff jitter produced only %d distinct values across 50 calls; retries would still be synchronised", len(seen))
	}
}

// TestPromoterReturnsJobsWhenDue checks that delayed jobs are held until their
// backoff elapses, then moved back exactly once.
func TestPromoterReturnsJobsWhenDue(t *testing.T) {
	useFastTimings(t)
	rdb := newTestRedis(t)
	ctx := context.Background()

	ready := mustMarshal(t, Job{ID: "job-due"})
	notYet := mustMarshal(t, Job{ID: "job-not-due"})

	rdb.ZAdd(ctx, delayedKey, redis.Z{
		Score:  float64(time.Now().Add(-time.Second).UnixMilli()),
		Member: ready,
	})
	rdb.ZAdd(ctx, delayedKey, redis.Z{
		Score:  float64(time.Now().Add(time.Hour).UnixMilli()),
		Member: notYet,
	})

	if n := promoteDue(ctx, rdb); n != 1 {
		t.Fatalf("promoteDue promoted %d jobs, want 1", n)
	}

	queued := queueContents(t, rdb, queueKey)
	if len(queued) != 1 || queued[0] != ready {
		t.Errorf("queue = %v, want exactly the due job", queued)
	}
	if got := delayedContents(t, rdb); len(got) != 1 || got[0] != notYet {
		t.Errorf("delayed set = %v, want only the not-yet-due job", got)
	}

	// A second pass must not promote the same job again.
	if n := promoteDue(ctx, rdb); n != 0 {
		t.Errorf("second promoteDue promoted %d jobs, want 0", n)
	}
}

// TestUnparseablePayloadIsQuarantined covers what used to be silent data loss:
// a payload that cannot be parsed was deleted outright.
func TestUnparseablePayloadIsQuarantined(t *testing.T) {
	useFastTimings(t)
	rdb := newTestRedis(t)
	ctx := context.Background()

	garbage := "{not valid json at all"
	rdb.RPush(ctx, inflightKey, garbage)

	handleJob(1, rdb, garbage)

	poisoned := queueContents(t, rdb, poisonKey)
	if len(poisoned) != 1 || poisoned[0] != garbage {
		t.Errorf("poison list = %v, want the raw payload preserved", poisoned)
	}
	if got := queueContents(t, rdb, inflightKey); len(got) != 0 {
		t.Errorf("quarantined payload left in the in-flight list: %v", got)
	}
	if got := queueContents(t, rdb, dlqKey); len(got) != 0 {
		t.Errorf("unparseable payload must not pollute the DLQ: %v", got)
	}
}

// TestHandlerRegistryDispatchesByType checks that job types reach their own
// handler, and that an unknown type is a permanent failure rather than three
// doomed attempts.
func TestHandlerRegistryDispatchesByType(t *testing.T) {
	var ran string
	register(JobHandler{
		Name:    "test-echo",
		Timeout: time.Second,
		Run: func(ctx context.Context, job Job) error {
			ran = job.Payload
			return nil
		},
	})
	t.Cleanup(func() { delete(handlers, "test-echo") })

	if err, _ := dispatch(Job{ID: "a", Type: "test-echo", Payload: "hello"}); err != nil {
		t.Fatalf("dispatch returned %v, want nil", err)
	}
	if ran != "hello" {
		t.Errorf("handler saw payload %q, want %q", ran, "hello")
	}

	err, _ := dispatch(Job{ID: "b", Type: "no-such-type"})
	if !errors.Is(err, ErrPermanent) {
		t.Errorf("unknown job type gave %v, want it to wrap ErrPermanent", err)
	}
}

// TestHandlerTimeoutIsEnforced checks the per-type timeout actually reaches
// the handler's context.
func TestHandlerTimeoutIsEnforced(t *testing.T) {
	register(JobHandler{
		Name:    "test-slow",
		Timeout: 50 * time.Millisecond,
		Run: func(ctx context.Context, job Job) error {
			select {
			case <-time.After(5 * time.Second):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	t.Cleanup(func() { delete(handlers, "test-slow") })

	start := time.Now()
	err, took := dispatch(Job{ID: "c", Type: "test-slow"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("dispatch error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("handler ran for %v; the timeout was not applied", elapsed)
	}
	if took < 40*time.Millisecond {
		t.Errorf("reported duration %v is implausibly short", took)
	}
}

// TestPerTypeMaxAttempts checks a handler can override the global retry budget.
func TestPerTypeMaxAttempts(t *testing.T) {
	register(JobHandler{
		Name:        "test-once",
		Timeout:     time.Second,
		MaxAttempts: 1,
		Run:         func(ctx context.Context, job Job) error { return nil },
	})
	t.Cleanup(func() { delete(handlers, "test-once") })

	if got := attemptsFor("test-once"); got != 1 {
		t.Errorf("attemptsFor(test-once) = %d, want 1", got)
	}
	if got := attemptsFor(""); got != maxAttempts {
		t.Errorf("attemptsFor(default) = %d, want the global %d", got, maxAttempts)
	}
}

// TestWaitTimeout covers the shutdown deadline helper.
func TestWaitTimeout(t *testing.T) {
	var quick sync.WaitGroup
	quick.Add(1)
	go func() { time.Sleep(10 * time.Millisecond); quick.Done() }()
	if !waitTimeout(&quick, time.Second) {
		t.Error("waitTimeout reported a timeout for a group that finished in time")
	}

	var stuck sync.WaitGroup
	stuck.Add(1) // never Done
	if waitTimeout(&stuck, 50*time.Millisecond) {
		t.Error("waitTimeout reported success for a group that never finished")
	}
}

// TestMetricsTrackJobOutcomes checks the counters actually move, and move on
// the right paths. Counters are process-global, so every assertion here is a
// delta rather than an absolute value.
func TestMetricsTrackJobOutcomes(t *testing.T) {
	useFastTimings(t)
	rdb := newTestRedis(t)
	ctx := context.Background()

	before := map[string]float64{
		"success":  testutil.ToFloat64(jobsProcessed.WithLabelValues("success")),
		"failure":  testutil.ToFloat64(jobsProcessed.WithLabelValues("failure")),
		"retried":  testutil.ToFloat64(jobsRetried.WithLabelValues("worker")),
		"buried":   testutil.ToFloat64(jobsBuried.WithLabelValues("worker", "exhausted")),
		"perm":     testutil.ToFloat64(jobsBuried.WithLabelValues("worker", "permanent")),
		"stale":    testutil.ToFloat64(staleResults),
		"recovers": testutil.ToFloat64(jobsRecovered),
	}
	delta := func(key string, now float64) float64 { return now - before[key] }

	// A job that succeeds.
	ok := Job{ID: "m-ok"}
	okJSON := mustMarshal(t, ok)
	rdb.RPush(ctx, inflightKey, okJSON)
	rdb.ZAdd(ctx, deadlinesKey, redis.Z{Score: deadlineFrom(time.Now()), Member: okJSON})
	settle(1, rdb, ok, okJSON, nil, 300*time.Millisecond)

	// A job that fails and still has attempts left.
	bad := Job{ID: "m-bad"}
	badJSON := mustMarshal(t, bad)
	rdb.RPush(ctx, inflightKey, badJSON)
	rdb.ZAdd(ctx, deadlinesKey, redis.Z{Score: deadlineFrom(time.Now()), Member: badJSON})
	settle(1, rdb, bad, badJSON, errFail, 400*time.Millisecond)

	// A job whose handler reported a permanent failure: buried on the spot.
	doomed := Job{ID: "m-doomed"}
	doomedJSON := mustMarshal(t, doomed)
	rdb.RPush(ctx, inflightKey, doomedJSON)
	rdb.ZAdd(ctx, deadlinesKey, redis.Z{Score: deadlineFrom(time.Now()), Member: doomedJSON})
	settle(1, rdb, doomed, doomedJSON, fmt.Errorf("nope: %w", ErrPermanent), 50*time.Millisecond)

	// A worker reporting on a job the sweeper already took.
	lost := Job{ID: "m-lost"}
	lostJSON := mustMarshal(t, lost)
	settle(1, rdb, lost, lostJSON, nil, 100*time.Millisecond)

	// A sweeper reclaim.
	orphan := Job{ID: "m-orphan"}
	orphanJSON := mustMarshal(t, orphan)
	rdb.RPush(ctx, inflightKey, orphanJSON)
	rdb.ZAdd(ctx, deadlinesKey, redis.Z{
		Score:  float64(time.Now().Add(-time.Minute).UnixMilli()),
		Member: orphanJSON,
	})
	reclaim(ctx, rdb, orphanJSON)

	checks := []struct {
		name string
		got  float64
		want float64
	}{
		{"jobs_processed_total{result=success}", delta("success", testutil.ToFloat64(jobsProcessed.WithLabelValues("success"))), 1},
		{"jobs_processed_total{result=failure}", delta("failure", testutil.ToFloat64(jobsProcessed.WithLabelValues("failure"))), 2},
		{"jobs_retried_total{source=worker}", delta("retried", testutil.ToFloat64(jobsRetried.WithLabelValues("worker"))), 1},
		{"jobs_dlq_total{source=worker,reason=exhausted}", delta("buried", testutil.ToFloat64(jobsBuried.WithLabelValues("worker", "exhausted"))), 0},
		{"jobs_dlq_total{source=worker,reason=permanent}", delta("perm", testutil.ToFloat64(jobsBuried.WithLabelValues("worker", "permanent"))), 1},
		{"stale_results_discarded_total", delta("stale", testutil.ToFloat64(staleResults)), 1},
		{"jobs_recovered_total", delta("recovers", testutil.ToFloat64(jobsRecovered)), 1},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s moved by %v, want %v", c.name, c.got, c.want)
		}
	}
}

// TestMetricsEndpointServesRegistry is a smoke test that the HTTP surface
// Prometheus scrapes is actually wired up.
func TestMetricsEndpointServesRegistry(t *testing.T) {
	jobsProcessed.WithLabelValues("success").Inc()

	srv := httptest.NewServer(promhttp.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	for _, want := range []string{
		"jobqueue_jobs_processed_total",
		"jobqueue_jobs_recovered_total",
		"jobqueue_stale_results_discarded_total",
		"jobqueue_job_duration_seconds_bucket",
		"jobqueue_jobs_poisoned_total",
		"jobqueue_jobs_promoted_total",
		"jobqueue_retry_delay_seconds_bucket",
		"jobqueue_forced_shutdowns_total",
		"jobqueue_deadlines_reaped_total",
		`jobqueue_depth{structure="delayed"}`,
		`jobqueue_depth{structure="poison"}`,
		`jobqueue_jobs_dlq_total{reason="permanent"`,
		// Registered automatically by client_golang, no code required.
		"go_goroutines",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("/metrics output is missing %q", want)
		}
	}
}

// TestPollQueueDepthsReportsRedisState checks the gauges reflect Redis rather
// than drifting from it.
func TestPollQueueDepthsReportsRedisState(t *testing.T) {
	rdb := newTestRedis(t)
	ctx := context.Background()

	oldInterval := depthPollInterval
	depthPollInterval = 20 * time.Millisecond
	t.Cleanup(func() { depthPollInterval = oldInterval })

	rdb.RPush(ctx, queueKey, "a", "b", "c")
	rdb.RPush(ctx, inflightKey, "d")
	rdb.RPush(ctx, dlqKey, "e", "f")

	pollCtx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go pollQueueDepths(pollCtx, rdb, &wg)
	t.Cleanup(func() { cancel(); wg.Wait() })

	waitFor(t, 2*time.Second, "gauges to match Redis", func() bool {
		return testutil.ToFloat64(queueDepth.WithLabelValues("pending")) == 3 &&
			testutil.ToFloat64(queueDepth.WithLabelValues("inflight")) == 1 &&
			testutil.ToFloat64(queueDepth.WithLabelValues("dlq")) == 2
	})
}
