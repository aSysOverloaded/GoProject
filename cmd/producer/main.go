package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
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
	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		log.Println("\033[1;30m[System] No .env file found, using system environment variables\033[0m")
	}

	fmt.Println("\033[1;35m==================================================\033[0m")
	fmt.Println("\033[1;35m      🚀 Distributed Job Queue - Producer (M3)     \033[0m")
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

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("\033[1;31m[System] Error connecting to Redis: %v\033[0m", err)
		log.Println("\033[1;33m[System] Please make sure Redis is running locally or check your REDIS_URL.\033[0m")
		os.Exit(1)
	}
	log.Printf("\033[1;32m[System] Connected to Redis successfully.\033[0m")

	queueKey := "jobs:queue"
	numJobs := 10

	log.Printf("\033[1;32m[Producer] Enqueueing %d persistent jobs into Redis list '%s'...\033[0m", numJobs, queueKey)
	for i := 1; i <= numJobs; i++ {
		job := Job{
			ID:       fmt.Sprintf("job-%d", i),
			Payload:  fmt.Sprintf("Payload info for task %d", i),
			Attempts: 0,
		}

		jobJSON, err := json.Marshal(job)
		if err != nil {
			log.Fatalf("\033[1;31m[Producer] Failed to marshal job: %v\033[0m", err)
		}

		err = rdb.LPush(ctx, queueKey, jobJSON).Err()
		if err != nil {
			log.Fatalf("\033[1;31m[Producer] Failed to push job %s to Redis: %v\033[0m", job.ID, err)
		}
		log.Printf("\033[1;34m[Producer] ➕ Enqueued Job %s\033[0m", job.ID)
	}

	log.Printf("\033[1;32m[Producer] Finished enqueueing %d jobs. Exiting.\033[0m", numJobs)
}
