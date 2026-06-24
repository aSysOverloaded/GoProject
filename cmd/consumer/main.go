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

	// Set up OS signal channel for graceful shutdown detection
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Block until an OS signal is received
	sig := <-sigChan
	log.Printf("\033[1;31m[System] Received signal %v. Initiating graceful shutdown...\033[0m", sig)

	// Cancel context to notify workers to stop polling
	cancelRun()

	// Wait for workers to finish their active tasks
	log.Printf("\033[1;33m[System] Waiting for workers to complete active tasks...\033[0m")
	wg.Wait()

	log.Printf("\033[1;32m[System] Shutdown complete.\033[0m")
}

func worker(ctx context.Context, id int, rdb *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("\033[36m[Worker %d] Ready for jobs.\033[0m", id)

	queueKey := "jobs:queue"

	for {
		select {
		case <-ctx.Done():
			log.Printf("\033[33m[Worker %d] Stopped.\033[0m", id)
			return
		default:
			// Pop a job from Redis. BRPop is blocking with a timeout.
			// We use a 15-second timeout to reduce connection polling and save Upstash quota.
			res, err := rdb.BRPop(ctx, 15*time.Second, queueKey).Result()
			if err != nil {
				if errors.Is(err, redis.Nil) {
					// Timeout with no jobs popped, loop again
					continue
				}
				if errors.Is(err, context.Canceled) {
					// Context canceled during BRPOP, loop again to catch <-ctx.Done()
					continue
				}
				log.Printf("\033[31m[Worker %d] Error popping from Redis: %v\033[0m", id, err)
				time.Sleep(1 * time.Second)
				continue
			}

			// BRPOP returns a slice: [key, value]
			jobJSON := res[1]
			var job Job
			if err := json.Unmarshal([]byte(jobJSON), &job); err != nil {
				log.Printf("\033[31m[Worker %d] Error parsing job JSON: %v\033[0m", id, err)
				continue
			}

			processJob(id, rdb, job)
		}
	}
}

func processJob(workerID int, rdb *redis.Client, job Job) {
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

		if job.Attempts < 3 {
			log.Printf("\033[35m[Worker %d] 🔁 Requeueing Job %s...\033[0m", workerID, job.ID)
			jobJSON, _ := json.Marshal(job)
			// Push it back to the queue (LPUSH) so it can be picked up again
			if err := rdb.LPush(dbCtx, "jobs:queue", jobJSON).Err(); err != nil {
				log.Printf("\033[31m[Worker %d] Error requeueing job %s: %v\033[0m", workerID, job.ID, err)
			}
		} else {
			log.Printf("\033[1;31m[Worker %d] 💀 Job %s failed permanently after 3 attempts. Sending to DLQ.\033[0m", workerID, job.ID)
			jobJSON, _ := json.Marshal(job)
			// Push to dead-letter queue list
			if err := rdb.LPush(dbCtx, "jobs:dlq", jobJSON).Err(); err != nil {
				log.Printf("\033[31m[Worker %d] Error sending job %s to DLQ: %v\033[0m", workerID, job.ID, err)
			}
		}
	} else {
		log.Printf("\033[32m[Worker %d] ✅ Job %s completed successfully in %v\033[0m", workerID, job.ID, workDuration)
	}
}
