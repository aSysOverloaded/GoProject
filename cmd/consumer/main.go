package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

type Job struct {
	ID       string `json:"id"`
	Payload  string `json:"payload"`
	Attempts int    `json:"attempts"`
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		log.Println("\033[1;30m[System] No .env file found, using system environment variables\033[0m")
	}

	fmt.Println("\033[1;35m==================================================\033[0m")
	fmt.Println("\033[1;35m      🚀 Distributed Job Queue - Consumer (M5)     \033[0m")
	fmt.Println("\033[1;35m==================================================\033[0m")

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379/0"
	}

	log.Printf("\033[1;34m[System] Connecting to Redis...\033[0m")
	opt, err := redis.ParseURL(redisURL)
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

	numWorkers := 3
	log.Printf("\033[1;34m[System] Starting %d workers...\033[0m", numWorkers)
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(runCtx, i, rdb, &wg)
	}

	// Start background sweeper for crash recovery
	wg.Add(1)
	go sweeper(runCtx, rdb, &wg)

	// Set up OS signal channel for graceful shutdown detection
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Block until an OS signal is received
	sig := <-sigChan
	log.Printf("\033[1;31m[System] Received signal %v. Initiating graceful shutdown...\033[0m", sig)

	// Cancel context to notify workers and sweeper to stop
	cancelRun()

	// Wait for workers and sweeper to finish
	log.Printf("\033[1;33m[System] Waiting for workers and sweeper to complete...\033[0m")
	wg.Wait()

	log.Printf("\033[1;32m[System] Shutdown complete.\033[0m")
}

const (
	queueKey     = "jobs:queue"
	inflightKey  = "jobs:inflight"
	deadlinesKey = "jobs:deadlines"
	dlqKey       = "jobs:dlq"

	maxAttempts = 3
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
		log.Printf("\033[31m[Worker %d] Error parsing job JSON: %v\033[0m", workerID, err)
		// Drop the unparseable payload out of the in-flight list, otherwise the
		// sweeper would rediscover it forever.
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 2*time.Second)
		rdb.LRem(dropCtx, inflightKey, 1, jobJSON)
		rdb.ZRem(dropCtx, deadlinesKey, jobJSON)
		dropCancel()
		return
	}

	log.Printf("\033[34m[Worker %d] ➡️ Processing Job %s - Attempt %d/%d\033[0m", workerID, job.ID, job.Attempts+1, maxAttempts)

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

	success, workDuration := processJob(job)

	stopHeartbeat()
	settle(workerID, rdb, job, jobJSON, success, workDuration)
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

// processJob is the pure "do the work" step: no Redis, no bookkeeping.
func processJob(job Job) (success bool, workDuration time.Duration) {
	// Simulate work duration: random between 200ms and 800ms
	workDuration = time.Duration(200+rand.Intn(600)) * time.Millisecond
	time.Sleep(workDuration)

	// Simulate 30% failure rate
	return rand.Float32() >= 0.3, workDuration
}

// settle records the outcome of a job the worker just finished.
func settle(workerID int, rdb *redis.Client, job Job, jobJSON string, success bool, workDuration time.Duration) {
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
		log.Printf("\033[1;33m[Worker %d] ⚠️ Job %s was reclaimed by the sweeper mid-flight. Discarding result.\033[0m", workerID, job.ID)
		return
	}

	if err := rdb.LRem(ctx, inflightKey, 1, jobJSON).Err(); err != nil {
		log.Printf("\033[31m[Worker %d] Error clearing Job %s from in-flight list: %v\033[0m", workerID, job.ID, err)
	}

	if success {
		log.Printf("\033[32m[Worker %d] ✅ Job %s completed successfully in %v\033[0m", workerID, job.ID, workDuration)
		return
	}

	job.Attempts++
	log.Printf("\033[31m[Worker %d] ❌ Job %s FAILED (Attempt %d/%d)\033[0m", workerID, job.ID, job.Attempts, maxAttempts)
	retryOrBury(ctx, rdb, fmt.Sprintf("Worker %d", workerID), job)
}

// retryOrBury pushes a failed job back onto the queue, or into the DLQ once it
// has burned through its attempts. Shared by the worker and the sweeper so both
// paths apply the same retry policy.
func retryOrBury(ctx context.Context, rdb *redis.Client, actor string, job Job) {
	jobJSON, err := json.Marshal(job)
	if err != nil {
		log.Printf("\033[31m[%s] Error marshalling Job %s: %v\033[0m", actor, job.ID, err)
		return
	}

	if job.Attempts < maxAttempts {
		log.Printf("\033[35m[%s] 🔁 Requeueing Job %s (Attempt %d/%d)...\033[0m", actor, job.ID, job.Attempts, maxAttempts)
		if err := rdb.LPush(ctx, queueKey, jobJSON).Err(); err != nil {
			log.Printf("\033[31m[%s] Error requeueing job %s: %v\033[0m", actor, job.ID, err)
		}
		return
	}

	log.Printf("\033[1;31m[%s] 💀 Job %s failed permanently after %d attempts. Sending to DLQ.\033[0m", actor, job.ID, maxAttempts)
	if err := rdb.LPush(ctx, dlqKey, jobJSON).Err(); err != nil {
		log.Printf("\033[31m[%s] Error sending job %s to DLQ: %v\033[0m", actor, job.ID, err)
	}
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

	job.Attempts++
	log.Printf("\033[1;33m[Sweeper] ⚠️ Detected orphaned Job %s (Worker crashed). Reclaiming...\033[0m", job.ID)

	dbCtx, dbCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dbCancel()
	retryOrBury(dbCtx, rdb, "Sweeper", job)
}
