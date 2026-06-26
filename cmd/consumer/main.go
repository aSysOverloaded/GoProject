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
	fmt.Println("\033[1;35m      🚀 Distributed Job Queue - Consumer (M3)     \033[0m")
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
	queueKey      = "jobs:queue"
	processingKey = "jobs:processing"
	dlqKey        = "jobs:dlq"
	visTimeout    = 5 * time.Second // Visibility timeout
)

func worker(ctx context.Context, id int, rdb *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("\033[36m[Worker %d] Ready for jobs.\033[0m", id)

	for {
		select {
		case <-ctx.Done():
			log.Printf("\033[33m[Worker %d] Stopped.\033[0m", id)
			return
		default:
			// Pop a job from Redis. BRPop is blocking with a timeout.
			// We use a 5-second timeout to balance responsiveness with saving Upstash quota.
			res, err := rdb.BRPop(ctx, 5*time.Second, queueKey).Result()
			if err != nil {
				if errors.Is(err, redis.Nil) {
					continue
				}
				if errors.Is(err, context.Canceled) {
					continue
				}
				log.Printf("\033[31m[Worker %d] Error popping from Redis: %v\033[0m", id, err)
				time.Sleep(1 * time.Second)
				continue
			}

			jobJSON := res[1]
			var job Job
			if err := json.Unmarshal([]byte(jobJSON), &job); err != nil {
				log.Printf("\033[31m[Worker %d] Error parsing job JSON: %v\033[0m", id, err)
				continue
			}

			// 1. Acquire Visibility Lock: Add to processing ZSET with score = now + visibility timeout
			expireAt := time.Now().Add(visTimeout).Unix()
			err = rdb.ZAdd(ctx, processingKey, redis.Z{
				Score:  float64(expireAt),
				Member: jobJSON,
			}).Err()
			if err != nil {
				log.Printf("\033[31m[Worker %d] Error locking job %s in ZSET: %v\033[0m", id, job.ID, err)
				continue
			}

			// Process the job
			processJob(id, rdb, job, jobJSON)
		}
	}
}

func processJob(workerID int, rdb *redis.Client, job Job, jobJSON string) {
	log.Printf("\033[34m[Worker %d] ➡️ Processing Job %s - Attempt %d/3\033[0m", workerID, job.ID, job.Attempts+1)

	// Simulate work duration: random between 200ms and 800ms
	workDuration := time.Duration(200+rand.Intn(600)) * time.Millisecond
	time.Sleep(workDuration)

	dbCtx, dbCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dbCancel()

	// Simulate 30% failure rate
	if rand.Float32() < 0.3 {
		job.Attempts++
		log.Printf("\033[31m[Worker %d] ❌ Job %s FAILED (Attempt %d/3)\033[0m", workerID, job.ID, job.Attempts)

		// Remove from processing ZSET (release lock)
		rdb.ZRem(dbCtx, processingKey, jobJSON)

		if job.Attempts < 3 {
			log.Printf("\033[35m[Worker %d] 🔁 Requeueing Job %s...\033[0m", workerID, job.ID)
			newJobJSON, _ := json.Marshal(job)
			if err := rdb.LPush(dbCtx, queueKey, newJobJSON).Err(); err != nil {
				log.Printf("\033[31m[Worker %d] Error requeueing job %s: %v\033[0m", workerID, job.ID, err)
			}
		} else {
			log.Printf("\033[1;31m[Worker %d] 💀 Job %s failed permanently after 3 attempts. Sending to DLQ.\033[0m", workerID, job.ID)
			newJobJSON, _ := json.Marshal(job)
			if err := rdb.LPush(dbCtx, dlqKey, newJobJSON).Err(); err != nil {
				log.Printf("\033[31m[Worker %d] Error sending job %s to DLQ: %v\033[0m", workerID, job.ID, err)
			}
		}
	} else {
		log.Printf("\033[32m[Worker %d] ✅ Job %s completed successfully in %v\033[0m", workerID, job.ID, workDuration)
		// Success: Remove from processing ZSET
		rdb.ZRem(dbCtx, processingKey, jobJSON)
	}
}

// sweeper periodically checks for orphaned/timed-out jobs in the processing ZSET
func sweeper(ctx context.Context, rdb *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("\033[1;30m[Sweeper] Active and monitoring for orphaned jobs...\033[0m")

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("\033[1;30m[Sweeper] Stopped.\033[0m")
			return
		case <-ticker.C:
			now := time.Now().Unix()
			
			// Find all jobs whose visibility timeout has expired (score <= now)
			expiredJobs, err := rdb.ZRangeByScore(ctx, processingKey, &redis.ZRangeBy{
				Min: "-inf",
				Max: fmt.Sprintf("%d", now),
			}).Result()
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Printf("\033[31m[Sweeper] Error fetching expired jobs: %v\033[0m", err)
				}
				continue
			}

			for _, jobJSON := range expiredJobs {
				// Atomically attempt to remove the job from ZSET to claim ownership
				// ZREM returns the number of members removed. If 1, we successfully locked it.
				removed, err := rdb.ZRem(ctx, processingKey, jobJSON).Result()
				if err != nil {
					continue
				}
				if removed == 0 {
					// Another consumer's sweeper already reclaimed this job
					continue
				}

				var job Job
				if err := json.Unmarshal([]byte(jobJSON), &job); err != nil {
					continue
				}

				job.Attempts++
				log.Printf("\033[1;33m[Sweeper] ⚠️ Detected orphaned Job %s (Worker crashed). Reclaiming...\033[0m", job.ID)

				dbCtx, dbCancel := context.WithTimeout(context.Background(), 2*time.Second)
				if job.Attempts < 3 {
					log.Printf("\033[1;35m[Sweeper] 🔁 Requeueing Job %s (Attempt %d/3)\033[0m", job.ID, job.Attempts)
					newJobJSON, _ := json.Marshal(job)
					if err := rdb.LPush(dbCtx, queueKey, newJobJSON).Err(); err != nil {
						log.Printf("\033[31m[Sweeper] Error requeueing Job %s: %v\033[0m", job.ID, err)
					}
				} else {
					log.Printf("\033[1;31m[Sweeper] 💀 Job %s exceeded max attempts after crash. Sending to DLQ.\033[0m", job.ID)
					newJobJSON, _ := json.Marshal(job)
					if err := rdb.LPush(dbCtx, dlqKey, newJobJSON).Err(); err != nil {
						log.Printf("\033[31m[Sweeper] Error sending Job %s to DLQ: %v\033[0m", job.ID, err)
					}
				}
				dbCancel()
			}
		}
	}
}
