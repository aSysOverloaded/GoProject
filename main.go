package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Job represents a single item of work in our queue.
type Job struct {
	ID       string
	Name     string
	Payload  string
	Attempts int
}

func main() {
	// Initialize random seed (for Go versions before 1.20)
	rand.Seed(time.Now().UnixNano())

	fmt.Println("\033[1;35m==================================================\033[0m")
	fmt.Println("\033[1;35m       🚀 Distributed Job Queue - Milestone 1      \033[0m")
	fmt.Println("\033[1;35m==================================================\033[0m")

	// Create jobs channel with a buffer size of 100
	jobsChan := make(chan Job, 100)

	// Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// Start 3 worker goroutines
	numWorkers := 3
	log.Printf("\033[1;34m[System] Starting %d workers...\033[0m", numWorkers)
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(ctx, i, jobsChan, &wg)
	}

	// Push 15 initial jobs
	log.Printf("\033[1;32m[Producer] Enqueueing 15 initial jobs...\033[0m")
	for i := 1; i <= 15; i++ {
		jobsChan <- Job{
			ID:       fmt.Sprintf("job-%d", i),
			Name:     fmt.Sprintf("Task %d", i),
			Payload:  fmt.Sprintf("Payload info for task %d", i),
			Attempts: 0,
		}
	}
	log.Printf("\033[1;32m[Producer] Enqueued 15 jobs successfully.\033[0m")

	// Set up OS signal channel for graceful shutdown detection
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Block until an OS signal is received
	sig := <-sigChan
	log.Printf("\033[1;31m[System] Received signal %v. Initiating graceful shutdown...\033[0m", sig)

	// Cancel context to notify workers to stop taking new jobs
	cancel()
 
	// Wait for workers to finish their active tasks
	log.Printf("\033[1;33m[System] Waiting for, workers to complete active tasks...\033[0m")
	wg.Wait()

	// Close the channel and drain remaining jobs to display what was left
	close(jobsChan)
	remainingCount := len(jobsChan)
	if remainingCount > 0 {
		log.Printf("\033[1;33m[System] Shutdown complete. %d jobs remained unprocessed in queue:\033[0m", remainingCount)
		for job := range jobsChan {
			log.Printf("  - Job %s (Name: %s, Attempts: %d)", job.ID, job.Name, job.Attempts)
		}
	} else {
		log.Printf("\033[1;32m[System] Shutdown complete. All queued jobs processed.\033[0m")
	}
}

// worker loops and pulls jobs from the channel until context cancellation is received.
func worker(ctx context.Context, id int, jobs chan Job, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("\033[36m[Worker %d] Ready for jobs.\033[0m", id)
	for {
		select {
		case <-ctx.Done():
			log.Printf("\033[33m[Worker %d] Stopped.\033[0m", id)
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}
			processJob(ctx, id, job, jobs)
		}
	}
}

// processJob simulates processing, mock failure, and retry logic.
func processJob(ctx context.Context, workerID int, job Job, jobs chan Job) {
	log.Printf("\033[34m[Worker %d] ➡️ Processing Job %s (%s) - Attempt %d/3\033[0m", workerID, job.ID, job.Name, job.Attempts+1)

	// Simulate work duration: random between 200ms and 800ms
	workDuration := time.Duration(200+rand.Intn(600)) * time.Millisecond
	time.Sleep(workDuration)

	// Simulate 30% failure rate
	if rand.Float32() < 0.3 {
		job.Attempts++
		log.Printf("\033[31m[Worker %d] ❌ Job %s FAILED (Attempt %d/3)\033[0m", workerID, job.ID, job.Attempts)

		if job.Attempts < 3 {
			log.Printf("\033[35m[Worker %d] 🔁 Requeueing Job %s...\033[0m", workerID, job.ID)
			select {
			case jobs <- job:
			default:
				// Asynchronously requeue if buffer is full to prevent worker deadlock
				go func() {
					jobs <- job
				}()
			}
		} else {
			log.Printf("\033[1;31m[Worker %d] 💀 Job %s failed permanently after 3 attempts. Discarding.\033[0m", workerID, job.ID)
		}
	} else {
		log.Printf("\033[32m[Worker %d] ✅ Job %s completed successfully in %v\033[0m", workerID, job.ID, workDuration)
	}
}
