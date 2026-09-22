package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
)

// Regression tests for two bugs found by running the system on Kubernetes.

// TestOrphanDeadlineIsReaped reproduces the leak observed on the cluster: a
// deadline entry whose job is no longer in the in-flight list. The sweeper
// iterates jobs:inflight, so an entry that exists only in jobs:deadlines is
// invisible to it and was never removed.
func TestOrphanDeadlineIsReaped(t *testing.T) {
	useFastTimings(t)
	rdb := newTestRedis(t)
	ctx := context.Background()

	orphan := mustMarshal(t, Job{ID: "job-48", Payload: "p"})
	if err := rdb.ZAdd(ctx, deadlinesKey, redis.Z{
		Score:  float64(time.Now().Add(-time.Minute).UnixMilli()),
		Member: orphan,
	}).Err(); err != nil {
		t.Fatalf("seed orphan: %v", err)
	}

	startSweeper(t, rdb)

	waitFor(t, 3*time.Second, "the orphaned deadline to be reaped", func() bool {
		n, _ := rdb.ZCard(ctx, deadlinesKey).Result()
		return n == 0
	})
}

// --- Guards: the fixes must not damage anything that already worked. ---

// TestReaperLeavesLiveClaimsAlone guards against the reaper's worst possible
// failure: deleting a claim that a live worker still holds. A claim always
// scores in the future, and one that has only just expired is still the
// reclaim path's business, so neither may be touched.
func TestReaperLeavesLiveClaimsAlone(t *testing.T) {
	useFastTimings(t)
	rdb := newTestRedis(t)
	ctx := context.Background()

	fresh := mustMarshal(t, Job{ID: "live", EnqueueID: "a"})
	justExpired := mustMarshal(t, Job{ID: "recent", EnqueueID: "b"})

	rdb.ZAdd(ctx, deadlinesKey,
		redis.Z{Score: deadlineFrom(time.Now()), Member: fresh},
		// Expired, but by less than the extra visibility-timeout margin.
		redis.Z{Score: float64(time.Now().Add(-visTimeout / 2).UnixMilli()), Member: justExpired},
	)

	// Neither is in the in-flight snapshot - the most dangerous case, since
	// that is exactly what an orphan looks like too.
	if n := reapOrphanDeadlines(ctx, rdb, nil); n != 0 {
		t.Fatalf("reaper removed %d live or recently-expired claim(s)", n)
	}
	if n, _ := rdb.ZCard(ctx, deadlinesKey).Result(); n != 2 {
		t.Errorf("deadlines = %d, want both claims untouched", n)
	}
}

// TestReaperLeavesInFlightJobsToReclaim guards against the reaper stealing
// crash recovery. An expired deadline whose job IS in the in-flight list means
// its worker died - that job must be requeued by reclaim, not have its
// deadline silently dropped.
func TestReaperLeavesInFlightJobsToReclaim(t *testing.T) {
	useFastTimings(t)
	rdb := newTestRedis(t)
	ctx := context.Background()

	dead := mustMarshal(t, Job{ID: "crashed", EnqueueID: "c"})
	rdb.RPush(ctx, inflightKey, dead)
	rdb.ZAdd(ctx, deadlinesKey, redis.Z{
		Score:  float64(time.Now().Add(-time.Minute).UnixMilli()),
		Member: dead,
	})

	if n := reapOrphanDeadlines(ctx, rdb, []string{dead}); n != 0 {
		t.Fatalf("reaper removed the deadline of an in-flight job; that job's recovery belongs to reclaim")
	}

	// And the normal recovery still happens end to end.
	before := testutil.ToFloat64(jobsRecovered)
	startSweeper(t, rdb)
	waitFor(t, 3*time.Second, "the crashed job to be reclaimed", func() bool {
		return len(delayedContents(t, rdb)) == 1
	})
	if got := testutil.ToFloat64(jobsRecovered) - before; got != 1 {
		t.Errorf("jobs_recovered moved by %v, want 1", got)
	}
}

// TestReapIsAtomicAgainstReclaim guards the race the Lua script exists for:
// an orphan's member being re-claimed by a worker between the reaper reading
// it and removing it. The fresh claim scores in the future, so the atomic
// check must leave it in place.
func TestReapIsAtomicAgainstReclaim(t *testing.T) {
	useFastTimings(t)
	rdb := newTestRedis(t)
	ctx := context.Background()

	member := mustMarshal(t, Job{ID: "raced"})
	cutoff := time.Now().Add(-visTimeout).UnixMilli()

	// The reaper saw it as old - but a worker has just claimed it afresh.
	rdb.ZAdd(ctx, deadlinesKey, redis.Z{Score: deadlineFrom(time.Now()), Member: member})

	n, err := reapScript.Run(ctx, rdb, []string{deadlinesKey}, member, cutoff).Int()
	if err != nil {
		t.Fatalf("reap script: %v", err)
	}
	if n != 0 {
		t.Fatal("reap script removed a freshly re-claimed deadline")
	}
	if _, err := rdb.ZScore(ctx, deadlinesKey, member).Result(); err != nil {
		t.Errorf("fresh claim is gone: %v", err)
	}
}

// TestDuplicateEnqueuesAreTrackedIndependently is the end-to-end check for the
// bug seen on the cluster: two copies of job-48 in flight at once. With
// distinct enqueue IDs each has its own claim, so both results count and
// nothing is left behind.
func TestDuplicateEnqueuesAreTrackedIndependently(t *testing.T) {
	useFastTimings(t)
	rdb := newTestRedis(t)
	ctx := context.Background()

	first := Job{ID: "job-48", Payload: "p", EnqueueID: "first"}
	second := Job{ID: "job-48", Payload: "p", EnqueueID: "second"}
	firstJSON, secondJSON := mustMarshal(t, first), mustMarshal(t, second)

	for _, j := range []string{firstJSON, secondJSON} {
		rdb.RPush(ctx, inflightKey, j)
		rdb.ZAdd(ctx, deadlinesKey, redis.Z{Score: deadlineFrom(time.Now()), Member: j})
	}

	staleBefore := testutil.ToFloat64(staleResults)
	successBefore := testutil.ToFloat64(jobsProcessed.WithLabelValues("success"))

	settle(1, rdb, first, firstJSON, nil, time.Millisecond)
	settle(2, rdb, second, secondJSON, nil, time.Millisecond)

	if got := testutil.ToFloat64(staleResults) - staleBefore; got != 0 {
		t.Errorf("a valid result was discarded as stale (%v); the two copies still share a claim", got)
	}
	if got := testutil.ToFloat64(jobsProcessed.WithLabelValues("success")) - successBefore; got != 2 {
		t.Errorf("successes recorded = %v, want 2", got)
	}
	if got := queueContents(t, rdb, inflightKey); len(got) != 0 {
		t.Errorf("in-flight not cleared: %v", got)
	}
	if n, _ := rdb.ZCard(ctx, deadlinesKey).Result(); n != 0 {
		t.Errorf("%d deadline(s) left behind", n)
	}
}

// TestDuplicateRetriesAreNotCollapsed covers the most damaging form of the
// identity bug: job LOSS. The delayed set is a ZSET, and a ZSET silently
// merges identical members. Two enqueues of the same job that both fail once
// serialise to the same retry payload, so the second ZADD overwrote the first
// and one job disappeared - never retried, never buried, never counted.
func TestDuplicateRetriesAreNotCollapsed(t *testing.T) {
	useFastTimings(t)
	rdb := newTestRedis(t)

	// Two enqueues of job-48, each failing its first attempt.
	failInFlight(t, rdb, Job{ID: "job-48", Payload: "p", Attempts: 0, EnqueueID: "first"}, errFail)
	failInFlight(t, rdb, Job{ID: "job-48", Payload: "p", Attempts: 0, EnqueueID: "second"}, errFail)

	if got := delayedContents(t, rdb); len(got) != 2 {
		t.Fatalf("delayed set holds %d retries, want 2 - one job was lost by ZSET de-duplication: %v", len(got), got)
	}
}

// TestPayloadWithoutEnqueueIDIsUnchanged guards backward compatibility. The
// serialised job is its own identity in Redis, so a producer that does not
// know about enqueue_id - or a job already sitting in the queue from before
// this change - must serialise exactly as it always did.
func TestPayloadWithoutEnqueueIDIsUnchanged(t *testing.T) {
	legacy := `{"id":"job-1","payload":"p","attempts":0}`

	var job Job
	if err := json.Unmarshal([]byte(legacy), &job); err != nil {
		t.Fatalf("legacy payload no longer parses: %v", err)
	}
	remarshalled, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	if string(remarshalled) != legacy {
		t.Errorf("legacy payload changed shape:\n got  %s\n want %s", remarshalled, legacy)
	}
}

// TestEnqueueIDSurvivesRetry guards that the uniqueness carries through the
// retry path: the rescheduled copy keeps its enqueue ID, so it cannot
// collide with another enqueue of the same logical job either.
func TestEnqueueIDSurvivesRetry(t *testing.T) {
	useFastTimings(t)
	rdb := newTestRedis(t)

	failInFlight(t, rdb, Job{ID: "job-7", Payload: "p", Attempts: 0, EnqueueID: "keep-me"}, errFail)

	delayed := delayedContents(t, rdb)
	if len(delayed) != 1 {
		t.Fatalf("delayed set = %v, want one rescheduled job", delayed)
	}
	var got Job
	if err := json.Unmarshal([]byte(delayed[0]), &got); err != nil {
		t.Fatal(err)
	}
	if got.EnqueueID != "keep-me" {
		t.Errorf("enqueue_id after retry = %q, want it preserved", got.EnqueueID)
	}
}

// --- Atomic state transitions ---

// TestAdoptionNeverResurrectsAFinishedJob covers how orphaned deadlines were
// being created in normal operation. The sweeper reads the in-flight list,
// sees a job with no deadline, and adopts it - but by the time it acts, the
// worker may have finished and released that job. Adopting it then creates a
// deadline for a job that is gone, which nothing would ever reclaim.
func TestAdoptionNeverResurrectsAFinishedJob(t *testing.T) {
	useFastTimings(t)
	rdb := newTestRedis(t)
	ctx := context.Background()

	finished := mustMarshal(t, Job{ID: "finished", EnqueueID: "x"})
	// Not in jobs:inflight any more: its worker already released it.
	adopt(ctx, rdb, finished)

	if n, _ := rdb.ZCard(ctx, deadlinesKey).Result(); n != 0 {
		t.Fatalf("adoption created a deadline for a job that is no longer in flight; it can never be reclaimed and sits in Redis until reaped")
	}
}

// TestAdoptionStillCoversACrashedWorker guards the reason adoption exists: a
// worker that died between BLMOVE and its claiming ZADD leaves a job in
// flight with no deadline, and that job must still get one.
func TestAdoptionStillCoversACrashedWorker(t *testing.T) {
	useFastTimings(t)
	rdb := newTestRedis(t)
	ctx := context.Background()

	orphan := mustMarshal(t, Job{ID: "orphan", EnqueueID: "y"})
	rdb.RPush(ctx, inflightKey, orphan)

	adopt(ctx, rdb, orphan)

	if _, err := rdb.ZScore(ctx, deadlinesKey, orphan).Result(); err != nil {
		t.Fatalf("in-flight job with no deadline was not adopted: %v", err)
	}
}

// TestReclaimOfUnparseablePayloadQuarantinesIt covers silent data loss in
// crash recovery. reclaim removed the job from Redis first and parsed it
// second, so a payload that could not be parsed was simply dropped - never
// retried, never quarantined.
func TestReclaimOfUnparseablePayloadQuarantinesIt(t *testing.T) {
	useFastTimings(t)
	rdb := newTestRedis(t)
	ctx := context.Background()

	garbage := "{not valid json"
	rdb.RPush(ctx, inflightKey, garbage)
	rdb.ZAdd(ctx, deadlinesKey, redis.Z{
		Score:  float64(time.Now().Add(-time.Minute).UnixMilli()),
		Member: garbage,
	})

	reclaim(ctx, rdb, garbage)

	if got := queueContents(t, rdb, poisonKey); len(got) != 1 || got[0] != garbage {
		t.Fatalf("poison list = %v, want the unparseable payload preserved there", got)
	}
	if got := queueContents(t, rdb, inflightKey); len(got) != 0 {
		t.Errorf("payload left in flight: %v", got)
	}
	if n, _ := rdb.ZCard(ctx, deadlinesKey).Result(); n != 0 {
		t.Errorf("%d deadline(s) left behind", n)
	}
}

// TestPublishNeverBlocksWhenKafkaIsDown covers the more serious half of the
// Kafka bug. With the broker unreachable, records never complete, so the
// client's buffer fills and stays full. publish() used the blocking Produce,
// and it runs inside the worker's job path - so once the buffer was full,
// every worker hung inside publish and the whole queue stalled.
func TestPublishNeverBlocksWhenKafkaIsDown(t *testing.T) {
	// Nothing listens on port 1, so every record is undeliverable. A tiny
	// buffer makes the cap reachable in a test instead of after 10,000
	// events.
	p, err := buildPublisher([]string{"127.0.0.1:1"}, "test-events", 5, time.Second)
	if err != nil {
		t.Fatalf("buildPublisher: %v", err)
	}
	t.Cleanup(p.client.Close)

	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			p.publish(JobEvent{JobID: fmt.Sprintf("job-%d", i), Event: EventStarted})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("publish blocked with Kafka unreachable: once the buffer filled, " +
			"every worker would hang inside publish and the queue would stall")
	}
}

// TestUndeliverableEventsAreDroppedNotHeldForever covers the memory half:
// without a delivery timeout an undeliverable record is retried forever and
// held in memory, so a long Kafka outage grows the process until it is
// OOM-killed.
func TestUndeliverableEventsAreDroppedNotHeldForever(t *testing.T) {
	p, err := buildPublisher([]string{"127.0.0.1:1"}, "test-events", 100, time.Second)
	if err != nil {
		t.Fatalf("buildPublisher: %v", err)
	}
	t.Cleanup(p.client.Close)

	for i := 0; i < 10; i++ {
		p.publish(JobEvent{JobID: fmt.Sprintf("job-%d", i), Event: EventStarted})
	}

	waitFor(t, 8*time.Second, "undeliverable events to time out of the buffer", func() bool {
		return p.client.BufferedProduceRecords() == 0
	})

	if got := p.failed.Load(); got < 10 {
		t.Errorf("failed counter = %d, want >= 10: dropped events must be counted, not vanish silently", got)
	}
}
