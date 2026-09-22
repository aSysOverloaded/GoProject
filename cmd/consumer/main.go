package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

type Job struct {
	ID string `json:"id"`
	// Type selects the handler. omitempty keeps the serialised form identical
	// to the old one for untyped jobs, which matters because the JSON string
	// is itself the ZSET member used for ownership matching.
	Type     string `json:"type,omitempty"`
	Payload  string `json:"payload"`
	Attempts int    `json:"attempts"`

	// EnqueueID is unique per enqueue, set by the producer. It exists because
	// the serialised job is its own identity in Redis: the in-flight list
	// element and the deadlines ZSET member are the raw JSON. Two enqueues of
	// the same logical job ("job-48" twice) used to serialise identically, so
	// while both were in flight they shared ONE deadline member - one
	// worker's ZREM then released the other's claim, a valid result was
	// discarded as stale, and a deadline could be left behind with no job.
	// A unique field per enqueue makes every in-flight copy distinct.
	//
	// omitempty keeps payloads from producers that do not set it
	// byte-identical to before, so they still parse and still work - they
	// just do not get the protection.
	EnqueueID string `json:"enqueue_id,omitempty"`
}

func main() {
	// Go 1.20+ seeds the global source automatically; rand.Seed is deprecated.

	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		log.Println("\033[1;30m[System] No .env file found, using system environment variables\033[0m")
	}

	fmt.Println("\033[1;35m==================================================\033[0m")
	fmt.Println("\033[1;35m      🚀 Distributed Job Queue - Consumer (M6)     \033[0m")
	fmt.Println("\033[1;35m==================================================\033[0m")

	cfg := loadConfig()
	cfg.apply()

	log.Printf("\033[1;34m[System] Connecting to Redis...\033[0m")
	opt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		log.Fatalf("\033[1;31m[System] Invalid Redis URL: %v\033[0m", err)
	}

	rdb := redis.NewClient(opt)
	defer rdb.Close()

	ctx, cancelConn := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelConn()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("\033[1;31m[System] Error connecting to Redis: %v\033[0m", err)
		log.Println("\033[1;33m[System] Please make sure Redis is running locally or check your REDIS_URL.\033[0m")
		os.Exit(1)
	}
	log.Printf("\033[1;32m[System] Connected to Redis successfully.\033[0m")

	// Context for graceful shutdown
	runCtx, cancelRun := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	metricsSrv := serveMetrics(cfg.MetricsAddr)

	// Optional: with KAFKA_BROKERS unset this is nil and every publish call
	// is a no-op, so the queue runs exactly as before.
	events = newPublisher()

	wg.Add(1)
	go pollQueueDepths(runCtx, rdb, &wg)

	log.Printf("\033[1;34m[System] Starting %d workers (%d handler(s) registered)...\033[0m", cfg.NumWorkers, len(handlers))
	for i := 1; i <= cfg.NumWorkers; i++ {
		wg.Add(1)
		go worker(runCtx, i, rdb, &wg)
	}

	// Start background sweeper for crash recovery
	wg.Add(1)
	go sweeper(runCtx, rdb, &wg)

	// Start the promoter, which moves jobs whose backoff has elapsed from the
	// delayed ZSET back onto the main queue.
	wg.Add(1)
	go promoter(runCtx, rdb, &wg)

	// Set up OS signal channel for graceful shutdown detection
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Block until an OS signal is received
	sig := <-sigChan
	log.Printf("\033[1;31m[System] Received signal %v. Initiating graceful shutdown...\033[0m", sig)

	// Cancel context to notify workers and sweeper to stop
	cancelRun()

	// Wait for the background goroutines, but not forever. A handler that
	// hangs must not stop the process from exiting: an orchestrator that gets
	// no response to SIGTERM escalates to SIGKILL, which is exactly the
	// ungraceful crash the whole sweeper exists to clean up after. Better to
	// exit deliberately and let the sweeper reclaim whatever was in flight.
	log.Printf("\033[1;33m[System] Waiting up to %v for workers and sweeper to complete...\033[0m", cfg.ShutdownTimeout)
	if !waitTimeout(&wg, cfg.ShutdownTimeout) {
		log.Printf("\033[1;31m[System] Shutdown deadline exceeded; exiting with jobs still in flight. The sweeper will reclaim them after the visibility timeout.\033[0m")
		shutdownsForced.Inc()
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()

	// Flush buffered events before exiting, otherwise a deploy silently
	// truncates the audit trail.
	events.close(shutdownCtx)

	// Stop serving metrics last, so a final scrape can still catch the
	// counters from the jobs we just drained.
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("\033[31m[Metrics] Error during shutdown: %v\033[0m", err)
	}

	log.Printf("\033[1;32m[System] Shutdown complete.\033[0m")
}

// waitTimeout waits for wg, returning false if the deadline passes first.
// sync.WaitGroup has no timed Wait, so the wait is moved onto a channel that
// can be raced against a timer.
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

const (
	queueKey     = "jobs:queue"
	inflightKey  = "jobs:inflight"
	deadlinesKey = "jobs:deadlines"
	dlqKey       = "jobs:dlq"
	// delayedKey holds jobs waiting out their retry backoff, scored by the
	// epoch-ms at which they become eligible to run again.
	delayedKey = "jobs:delayed"
	// poisonKey holds payloads that could not be parsed at all. They are kept
	// separate from the DLQ so that the DLQ stays machine-readable and can be
	// replayed without tripping over garbage.
	poisonKey = "jobs:poison"
)

// Timings live in vars rather than consts so the tests can compress them.
var (
	// visTimeout is how long a job may stay in-flight without a heartbeat
	// before the sweeper treats its owner as dead.
	visTimeout = 15 * time.Second
	// heartbeatTick must be comfortably shorter than visTimeout so a healthy
	// worker always refreshes its claim before the sweeper can expire it.
	heartbeatTick = 5 * time.Second
	sweepTick     = 3 * time.Second
	// blockTimeout is how long BLMOVE parks on an empty queue. Longer means
	// fewer commands billed against Upstash while idle.
	blockTimeout = 15 * time.Second

	maxAttempts = 3

	// Retry backoff. Delay for attempt n is baseRetryDelay * 2^(n-1), capped
	// at maxRetryDelay, then jittered.
	baseRetryDelay = 1 * time.Second
	maxRetryDelay  = 5 * time.Minute
	// promoteTick is how often the promoter moves due jobs out of the delayed
	// set and back onto the queue.
	promoteTick = 1 * time.Second
)

// deadlineFrom returns the ZSET score for a claim made now. Scores are
// milliseconds since the epoch: whole seconds were too coarse to express a
// visibility timeout accurately.
func deadlineFrom(t time.Time) float64 {
	return float64(t.Add(visTimeout).UnixMilli())
}

func worker(ctx context.Context, id int, rdb *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("\033[36m[Worker %d] Ready for jobs.\033[0m", id)

	for {
		select {
		case <-ctx.Done():
			log.Printf("\033[33m[Worker %d] Stopped.\033[0m", id)
			return
		default:
		}

		// BLMOVE pops from the queue AND records the job in the in-flight list
		// as a single atomic step. This is the reason we no longer use BRPOP:
		// with BRPOP a crash in the window between the pop and the bookkeeping
		// write destroyed the job, because nothing on the server side still
		// referenced it. Now every in-flight job is reachable from jobs:inflight
		// no matter when we die.
		jobJSON, err := rdb.BLMove(ctx, queueKey, inflightKey, "RIGHT", "LEFT", blockTimeout).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
				continue
			}
			log.Printf("\033[31m[Worker %d] Error popping from Redis: %v\033[0m", id, err)
			time.Sleep(1 * time.Second)
			continue
		}

		handleJob(id, rdb, jobJSON)
	}
}

// handleJob owns one job end to end: claim it, keep the claim alive while we
// work, then settle the outcome.
func handleJob(workerID int, rdb *redis.Client, jobJSON string) {
	var job Job
	if err := json.Unmarshal([]byte(jobJSON), &job); err != nil {
		log.Printf("\033[1;31m[Worker %d] ☠️ Unparseable payload, quarantining to %s: %v\033[0m", workerID, poisonKey, err)
		quarantine(rdb, jobJSON)
		return
	}

	log.Printf("\033[34m[Worker %d] ➡️ Processing Job %s (type %q) - Attempt %d/%d\033[0m",
		workerID, job.ID, job.Type, job.Attempts+1, attemptsFor(job.Type))

	events.publish(JobEvent{
		JobID:   job.ID,
		JobType: job.Type,
		Event:   EventStarted,
		Attempt: job.Attempts + 1,
		Worker:  fmt.Sprintf("worker-%d", workerID),
	})

	claimCtx, claimCancel := context.WithTimeout(context.Background(), 2*time.Second)
	err := rdb.ZAdd(claimCtx, deadlinesKey, redis.Z{
		Score:  deadlineFrom(time.Now()),
		Member: jobJSON,
	}).Err()
	claimCancel()
	if err != nil {
		log.Printf("\033[31m[Worker %d] Error claiming job %s: %v\033[0m", workerID, job.ID, err)
	}

	// Keep the claim fresh for as long as we are genuinely alive, so that a
	// slow-but-healthy worker is never mistaken for a crashed one.
	hbCtx, stopHeartbeat := context.WithCancel(context.Background())
	go heartbeat(hbCtx, workerID, rdb, job.ID, jobJSON)

	runErr, workDuration := dispatch(job)

	stopHeartbeat()
	settle(workerID, rdb, job, jobJSON, runErr, workDuration)
}

// quarantine moves a payload we cannot parse into the poison list. It used to
// be deleted outright, which is silent data loss in a project whose entire
// premise is not losing jobs. It cannot go to the DLQ, because the DLQ is
// expected to hold valid job JSON that a replay tool can read back.
func quarantine(rdb *redis.Client, rawPayload string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := rdb.LPush(ctx, poisonKey, rawPayload).Err(); err != nil {
		// Deliberately do NOT remove it from the in-flight list if we could
		// not save it: leaving it there means the sweeper retries later,
		// which is far better than dropping it.
		log.Printf("\033[31m[Worker] Failed to quarantine payload, leaving it in flight for the sweeper: %v\033[0m", err)
		return
	}

	rdb.LRem(ctx, inflightKey, 1, rawPayload)
	rdb.ZRem(ctx, deadlinesKey, rawPayload)
	jobsPoisoned.Inc()

	events.publish(JobEvent{
		JobID:  "unknown",
		Event:  EventPoisoned,
		Reason: "payload could not be parsed as a job",
	})
}

// heartbeat pushes the job's deadline further out at a steady tick. It only
// ever fires for jobs that outlive heartbeatTick, so in normal operation it
// costs zero extra Redis commands.
func heartbeat(ctx context.Context, workerID int, rdb *redis.Client, jobID, jobJSON string) {
	ticker := time.NewTicker(heartbeatTick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// XX: only extend a claim we still hold. If the sweeper already
			// reclaimed this job, ZADD XX is a no-op rather than resurrecting
			// a deadline for a job somebody else now owns.
			hbCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := rdb.ZAddXX(hbCtx, deadlinesKey, redis.Z{
				Score:  deadlineFrom(time.Now()),
				Member: jobJSON,
			}).Err()
			cancel()
			if err != nil {
				log.Printf("\033[31m[Worker %d] Heartbeat failed for Job %s: %v\033[0m", workerID, jobID, err)
			}
		}
	}
}

// next is where a job goes when its current holder releases it.
type next struct {
	mode    string // "none" (done), "zadd" (retry set) or "lpush" (DLQ, poison)
	key     string
	payload string
	score   float64
}

// releaseScript ends a holder's claim on a job and moves the job to its next
// state, as ONE atomic step. Both the worker (settle) and the sweeper
// (reclaim) release jobs through it.
//
//	KEYS[1] jobs:deadlines   KEYS[2] jobs:inflight   KEYS[3] destination
//	ARGV[1] the member being released
//	ARGV[2] "none" | "zadd" | "lpush"
//	ARGV[3] payload for the destination   ARGV[4] score, for "zadd"
//
// The ZREM is the ownership handshake: Redis runs one command at a time, so
// for a given job exactly one caller can get 1 back. Whoever gets 0 lost the
// job to someone else, and nothing is changed.
//
// It used to be three or four separate commands, which left two holes. A
// sweeper looking between the ZREM and the LREM saw an in-flight job with no
// deadline and "adopted" it, re-creating a deadline for a job that had already
// finished. And a failure after the job left the in-flight list but before
// its retry or DLQ write landed lost the job outright. A Lua script runs to
// completion with nothing interleaved, so neither can happen.
var releaseScript = redis.NewScript(`
if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then
  return 0
end
redis.call('LREM', KEYS[2], 1, ARGV[1])
if ARGV[2] == 'zadd' then
  redis.call('ZADD', KEYS[3], ARGV[4], ARGV[3])
elseif ARGV[2] == 'lpush' then
  redis.call('LPUSH', KEYS[3], ARGV[3])
end
return 1
`)

// release runs releaseScript and reports whether the caller owned the job.
func release(ctx context.Context, rdb *redis.Client, member string, to next) (bool, error) {
	dest := to.key
	if dest == "" {
		dest = dlqKey // unused for "none", but the script always takes three keys
	}
	n, err := releaseScript.Run(ctx, rdb,
		[]string{deadlinesKey, inflightKey, dest},
		member, to.mode, to.payload, to.score,
	).Int()
	return n == 1, err
}

// settle records the outcome of a job the worker just finished. A nil runErr
// means success.
func settle(workerID int, rdb *redis.Client, job Job, jobJSON string, runErr error, workDuration time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	workerName := fmt.Sprintf("worker-%d", workerID)
	attempt := job.Attempts + 1 // the attempt that just ran

	// Decide the outcome before touching Redis, so that releasing the job and
	// moving it on is a single atomic transition.
	to := next{mode: "none"}
	var plan failurePlan
	if runErr != nil {
		job.Attempts++
		p, err := planFailure(job, runErr)
		if err != nil {
			// Leave the job alone: its claim expires and the sweeper retries
			// it, rather than risk losing it.
			log.Printf("\033[31m[Worker %d] Error planning retry for Job %s: %v\033[0m", workerID, job.ID, err)
			return
		}
		plan, to = p, p.to
	}

	owned, err := release(ctx, rdb, jobJSON, to)
	if err != nil {
		// Nothing changed. The claim expires and the sweeper takes over.
		log.Printf("\033[31m[Worker %d] Error releasing Job %s: %v\033[0m", workerID, job.ID, err)
		return
	}
	if !owned {
		// The sweeper decided this worker was dead and already moved the job
		// on, so this result is stale and must be dropped - acting on it is
		// what used to requeue the job twice and inflate its attempt counter.
		staleResults.Inc()
		log.Printf("\033[1;33m[Worker %d] ⚠️ Job %s was reclaimed by the sweeper mid-flight. Discarding result.\033[0m", workerID, job.ID)
		events.publish(JobEvent{
			JobID:   job.ID,
			JobType: job.Type,
			Event:   EventDiscarded,
			Attempt: attempt,
			Worker:  workerName,
			Reason:  "lost ownership handshake to sweeper",
		})
		return
	}

	jobDuration.Observe(workDuration.Seconds())

	if runErr == nil {
		jobsProcessed.WithLabelValues("success").Inc()
		log.Printf("\033[32m[Worker %d] ✅ Job %s completed successfully in %v\033[0m", workerID, job.ID, workDuration.Round(time.Millisecond))
		events.publish(JobEvent{
			JobID:      job.ID,
			JobType:    job.Type,
			Event:      EventSucceeded,
			Attempt:    attempt,
			DurationMS: workDuration.Milliseconds(),
			Worker:     workerName,
		})
		return
	}

	jobsProcessed.WithLabelValues("failure").Inc()
	log.Printf("\033[31m[Worker %d] ❌ Job %s FAILED (Attempt %d/%d): %v\033[0m",
		workerID, job.ID, job.Attempts, attemptsFor(job.Type), runErr)
	events.publish(JobEvent{
		JobID:      job.ID,
		JobType:    job.Type,
		Event:      EventFailed,
		Attempt:    job.Attempts,
		DurationMS: workDuration.Milliseconds(),
		Error:      runErr.Error(),
		Worker:     workerName,
	})

	recordFailure(actor{label: "worker", name: fmt.Sprintf("Worker %d", workerID)}, job, plan, runErr)
}

// actor identifies who is applying the retry policy: name is for humans
// reading the log, label is the low-cardinality value used on metrics.
type actor struct {
	label string
	name  string
}

// sweeper looks for jobs stranded in the in-flight list by a crashed worker.
// The in-flight list is the source of truth for "what is being worked on"; the
// deadlines ZSET only answers "is its owner still alive".
func sweeper(ctx context.Context, rdb *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("\033[1;30m[Sweeper] Active and monitoring for orphaned jobs...\033[0m")

	ticker := time.NewTicker(sweepTick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("\033[1;30m[Sweeper] Stopped.\033[0m")
			return
		case <-ticker.C:
			inflight, err := rdb.LRange(ctx, inflightKey, 0, -1).Result()
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Printf("\033[31m[Sweeper] Error reading in-flight list: %v\033[0m", err)
				}
				continue
			}

			// Runs before the empty-list early exit on purpose: the leak it
			// cleans up is precisely a deadline with no in-flight job, which
			// is most likely to be sitting there when the list is empty.
			reapOrphanDeadlines(ctx, rdb, inflight)

			if len(inflight) == 0 {
				continue
			}

			// One round trip for all deadlines instead of one per job.
			pipe := rdb.Pipeline()
			scores := make([]*redis.FloatCmd, len(inflight))
			for i, jobJSON := range inflight {
				scores[i] = pipe.ZScore(ctx, deadlinesKey, jobJSON)
			}
			if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
				if !errors.Is(err, context.Canceled) {
					log.Printf("\033[31m[Sweeper] Error fetching deadlines: %v\033[0m", err)
				}
				continue
			}

			now := float64(time.Now().UnixMilli())
			for i, jobJSON := range inflight {
				deadline, err := scores[i].Result()

				if errors.Is(err, redis.Nil) {
					adopt(ctx, rdb, jobJSON)
					continue
				}
				if err != nil {
					continue
				}
				if deadline > now {
					// Owner is alive and heartbeating.
					continue
				}

				reclaim(ctx, rdb, jobJSON)
			}
		}
	}
}

// adoptScript gives a deadline to a job only if it is STILL in the in-flight
// list, checked and written as one step.
var adoptScript = redis.NewScript(`
if redis.call('LPOS', KEYS[1], ARGV[1]) then
  return redis.call('ZADD', KEYS[2], 'NX', ARGV[2], ARGV[1])
end
return 0
`)

// adopt gives a deadline to a job the sweeper saw in flight without one: its
// owner died in the narrow window between BLMOVE and the claiming ZADD. It
// gets a deadline rather than being reclaimed immediately - NX so a worker
// that is merely a millisecond slow to register its own claim is never
// stomped. If nobody heartbeats it, a later sweep reclaims it normally.
//
// The sweeper decided to adopt from a snapshot, and the job may have finished
// since. Adopting unconditionally re-created a deadline for a job that was
// gone - one that nothing would ever reclaim. The in-flight check inside the
// script rules that out.
func adopt(ctx context.Context, rdb *redis.Client, jobJSON string) {
	adoptScript.Run(ctx, rdb, []string{inflightKey, deadlinesKey}, jobJSON, deadlineFrom(time.Now()))
}

// reapScript removes a deadline only if it is still at or below the cutoff,
// as one atomic step. Checking and removing in two separate commands would
// race with a worker re-claiming the same member in between: a fresh claim
// could be deleted out from under it.
var reapScript = redis.NewScript(`
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if score and tonumber(score) <= tonumber(ARGV[2]) then
  return redis.call('ZREM', KEYS[1], ARGV[1])
end
return 0
`)

// reapOrphanDeadlines removes deadline entries that no longer have a job in
// the in-flight list.
//
// Normal operation no longer creates orphans: releases are one atomic script,
// and adoption checks the job is still in flight. Both used to leave them
// behind. What remains is two byte-identical payloads in flight at once,
// sharing one ZSET member - EnqueueID prevents that, so this is the defence
// for producers that do not set it.
//
// The sweeper walks the in-flight list, so nothing else would ever find
// these, and before this existed they sat in Redis permanently.
//
// Safety: only deadlines expired by a further full visibility timeout are
// considered, and each is removed atomically only if still that old. A live
// claim always scores in the future, so it can never qualify - even if its
// job was picked up after the in-flight snapshot was taken.
func reapOrphanDeadlines(ctx context.Context, rdb *redis.Client, inflight []string) int {
	cutoff := time.Now().Add(-visTimeout).UnixMilli()

	stale, err := rdb.ZRangeByScore(ctx, deadlinesKey, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   strconv.FormatInt(cutoff, 10),
		Count: 100,
	}).Result()
	if err != nil || len(stale) == 0 {
		return 0
	}

	live := make(map[string]struct{}, len(inflight))
	for _, member := range inflight {
		live[member] = struct{}{}
	}

	reaped := 0
	for _, member := range stale {
		if _, ok := live[member]; ok {
			// Its job is in flight, so its owner died: that is the reclaim
			// path's job, and it must be requeued rather than just dropped.
			continue
		}
		n, err := reapScript.Run(ctx, rdb, []string{deadlinesKey}, member, cutoff).Int()
		if err == nil && n == 1 {
			reaped++
		}
	}

	if reaped > 0 {
		deadlinesReaped.Add(float64(reaped))
		log.Printf("\033[1;33m[Sweeper] 🧹 Reaped %d deadline(s) with no in-flight job. This means a producer is enqueueing identical payloads without an enqueue_id.\033[0m", reaped)
	}
	return reaped
}

// reclaim takes ownership of an expired job and applies the retry policy.
// The release uses the same atomic handshake as settle, so the sweeper wins
// or loses cleanly against the job's original worker and any other
// consumer's sweeper.
func reclaim(ctx context.Context, rdb *redis.Client, jobJSON string) {
	// Parse before touching Redis. The old order removed the job first and
	// parsed it second, so a payload that could not be parsed was dropped -
	// never retried, never quarantined.
	var job Job
	if err := json.Unmarshal([]byte(jobJSON), &job); err != nil {
		owned, err := release(ctx, rdb, jobJSON, next{mode: "lpush", key: poisonKey, payload: jobJSON})
		if err == nil && owned {
			jobsPoisoned.Inc()
			log.Printf("\033[1;31m[Sweeper] ☠️ Reclaimed an unparseable payload; quarantined to %s\033[0m", poisonKey)
			events.publish(JobEvent{
				JobID:  "unknown",
				Event:  EventPoisoned,
				Worker: "sweeper",
				Reason: "reclaimed payload could not be parsed as a job",
			})
		}
		return
	}

	job.Attempts++
	// nil error: the worker died, so it never reported why. A crash is always
	// treated as retryable.
	plan, err := planFailure(job, nil)
	if err != nil {
		return // untouched; the next sweep tries again
	}

	owned, err := release(ctx, rdb, jobJSON, plan.to)
	if err != nil || !owned {
		return
	}

	jobsRecovered.Inc()
	log.Printf("\033[1;33m[Sweeper] ⚠️ Detected orphaned Job %s (Worker crashed). Reclaiming...\033[0m", job.ID)
	events.publish(JobEvent{
		JobID:   job.ID,
		JobType: job.Type,
		Event:   EventRecovered,
		Attempt: job.Attempts,
		Worker:  "sweeper",
		Reason:  "owner stopped heartbeating",
	})

	recordFailure(actor{label: "sweeper", name: "Sweeper"}, job, plan, nil)
}
