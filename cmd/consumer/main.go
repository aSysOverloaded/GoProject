package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
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

// settle records the outcome of a job the worker just finished. A nil runErr
// means success.
func settle(workerID int, rdb *redis.Client, job Job, jobJSON string, runErr error, workDuration time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// ZREM is the ownership handshake. Exactly one of {this worker, the sweeper}
	// can get 1 back for a given job. Getting 0 means the sweeper decided we
	// were dead and already requeued the job, so our result is stale and we
	// must drop it - requeueing here is what used to duplicate the job and
	// inflate its attempt counter.
	owned, err := rdb.ZRem(ctx, deadlinesKey, jobJSON).Result()
	if err != nil {
		log.Printf("\033[31m[Worker %d] Error releasing Job %s: %v\033[0m", workerID, job.ID, err)
		return
	}
	if owned == 0 {
		staleResults.Inc()
		log.Printf("\033[1;33m[Worker %d] ⚠️ Job %s was reclaimed by the sweeper mid-flight. Discarding result.\033[0m", workerID, job.ID)
		events.publish(JobEvent{
			JobID:   job.ID,
			JobType: job.Type,
			Event:   EventDiscarded,
			Attempt: job.Attempts + 1,
			Worker:  fmt.Sprintf("worker-%d", workerID),
			Reason:  "lost ownership handshake to sweeper",
		})
		return
	}

	if err := rdb.LRem(ctx, inflightKey, 1, jobJSON).Err(); err != nil {
		log.Printf("\033[31m[Worker %d] Error clearing Job %s from in-flight list: %v\033[0m", workerID, job.ID, err)
	}

	jobDuration.Observe(workDuration.Seconds())

	workerName := fmt.Sprintf("worker-%d", workerID)

	if runErr == nil {
		jobsProcessed.WithLabelValues("success").Inc()
		log.Printf("\033[32m[Worker %d] ✅ Job %s completed successfully in %v\033[0m", workerID, job.ID, workDuration.Round(time.Millisecond))
		events.publish(JobEvent{
			JobID:      job.ID,
			JobType:    job.Type,
			Event:      EventSucceeded,
			Attempt:    job.Attempts + 1,
			DurationMS: workDuration.Milliseconds(),
			Worker:     workerName,
		})
		return
	}

	jobsProcessed.WithLabelValues("failure").Inc()
	job.Attempts++
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

	retryOrBury(ctx, rdb, actor{label: "worker", name: fmt.Sprintf("Worker %d", workerID)}, job, runErr)
}

// actor identifies who is applying the retry policy: name is for humans
// reading the log, label is the low-cardinality value used on metrics.
type actor struct {
	label string
	name  string
}

// retryOrBury applies the retry policy to a failed job. Shared by the worker
// and the sweeper so both paths behave identically.
//
// runErr is the handler's error, or nil when the sweeper is recovering a job
// whose worker died and therefore never produced one.
func retryOrBury(ctx context.Context, rdb *redis.Client, who actor, job Job, runErr error) {
	jobJSON, err := json.Marshal(job)
	if err != nil {
		log.Printf("\033[31m[%s] Error marshalling Job %s: %v\033[0m", who.name, job.ID, err)
		return
	}

	// A permanent error will fail identically on every future attempt, so
	// spending the remaining budget (and the backoff waits) on it is pure
	// latency for a guaranteed outcome. Bury it immediately.
	permanent := errors.Is(runErr, ErrPermanent)
	budget := attemptsFor(job.Type)

	if !permanent && job.Attempts < budget {
		if err := scheduleRetry(ctx, rdb, who, job, string(jobJSON)); err != nil {
			log.Printf("\033[31m[%s] Error scheduling retry for job %s: %v\033[0m", who.name, job.ID, err)
		}
		return
	}

	if permanent {
		log.Printf("\033[1;31m[%s] 💀 Job %s failed permanently (not retryable). Sending to DLQ.\033[0m", who.name, job.ID)
	} else {
		log.Printf("\033[1;31m[%s] 💀 Job %s exhausted %d attempts. Sending to DLQ.\033[0m", who.name, job.ID, budget)
	}

	if err := rdb.LPush(ctx, dlqKey, jobJSON).Err(); err != nil {
		log.Printf("\033[31m[%s] Error sending job %s to DLQ: %v\033[0m", who.name, job.ID, err)
		return
	}

	reason := "exhausted"
	if permanent {
		reason = "permanent"
	}
	jobsBuried.WithLabelValues(who.label, reason).Inc()

	buried := JobEvent{
		JobID:   job.ID,
		JobType: job.Type,
		Event:   EventBuried,
		Attempt: job.Attempts,
		Worker:  who.name,
		Reason:  reason,
	}
	if runErr != nil {
		buried.Error = runErr.Error()
	}
	events.publish(buried)
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
					// In-flight but with no deadline at all: its owner died in
					// the narrow window between BLMOVE and the claiming ZADD.
					// Give it a deadline rather than reclaiming immediately -
					// NX so we never stomp a worker that is merely a
					// millisecond slow to register its own claim. If nobody
					// heartbeats it, a later sweep reclaims it normally.
					rdb.ZAddNX(ctx, deadlinesKey, redis.Z{
						Score:  deadlineFrom(time.Now()),
						Member: jobJSON,
					})
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

// reclaim takes ownership of an expired job and applies the retry policy.
func reclaim(ctx context.Context, rdb *redis.Client, jobJSON string) {
	// Same ownership handshake as settle: ZREM returning 1 means we won the
	// race against the job's original worker and any other consumer's sweeper.
	removed, err := rdb.ZRem(ctx, deadlinesKey, jobJSON).Result()
	if err != nil || removed == 0 {
		return
	}

	if err := rdb.LRem(ctx, inflightKey, 1, jobJSON).Err(); err != nil {
		log.Printf("\033[31m[Sweeper] Error clearing reclaimed job from in-flight list: %v\033[0m", err)
	}

	var job Job
	if err := json.Unmarshal([]byte(jobJSON), &job); err != nil {
		return
	}

	jobsRecovered.Inc()
	job.Attempts++
	log.Printf("\033[1;33m[Sweeper] ⚠️ Detected orphaned Job %s (Worker crashed). Reclaiming...\033[0m", job.ID)

	events.publish(JobEvent{
		JobID:   job.ID,
		JobType: job.Type,
		Event:   EventRecovered,
		Attempt: job.Attempts,
		Worker:  "sweeper",
		Reason:  "owner stopped heartbeating",
	})

	dbCtx, dbCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dbCancel()
	// nil error: the worker died, so it never reported why. A crash is always
	// treated as retryable.
	retryOrBury(dbCtx, rdb, actor{label: "sweeper", name: "Sweeper"}, job, nil)
}
