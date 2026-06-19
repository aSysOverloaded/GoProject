package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
)

// Job represents a single item of work stored in our database.
type Job struct {
	ID       string
	Payload  string
	Status   string
	Attempts int
}

func main() {
	// Initialize random seed
	rand.Seed(time.Now().UnixNano())

	fmt.Println("\033[1;35m==================================================\033[0m")
	fmt.Println("\033[1;35m       🚀 Distributed Job Queue - Milestone 2      \033[0m")
	fmt.Println("\033[1;35m==================================================\033[0m")

	// Get database URL from environment variable or fallback
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/jobqueue?sslmode=disable"
	}

	log.Printf("\033[1;34m[System] Connecting to database...\033[0m")
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("\033[1;31m[System] Failed to open database connection: %v\033[0m", err)
	}
	defer db.Close()

	// Ping database to verify connection
	ctx, cancelConn := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelConn()
	if err := db.PingContext(ctx); err != nil {
		log.Printf("\033[1;31m[System] Error connecting to PostgreSQL: %v\033[0m", err)
		log.Println("\033[1;33m[System] Please make sure PostgreSQL is running and you have created the 'jobqueue' database.\033[0m")
		log.Println("\033[1;33m[System] Connection string used:", dbURL)
		log.Println("\033[1;33m[System] You can customize it by setting the DATABASE_URL environment variable.\033[0m")
		os.Exit(1)
	}
	log.Printf("\033[1;32m[System] Database connection established successfully.\033[0m")

	// Initialize the schema automatically
	if err := initSchema(db); err != nil {
		log.Fatalf("\033[1;31m[System] Failed to initialize schema: %v\033[0m", err)
	}

	// Context for graceful shutdown
	runCtx, cancelRun := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// Enqueue 15 initial jobs
	log.Printf("\033[1;32m[Producer] Enqueueing 15 persistent jobs...\033[0m")
	for i := 1; i <= 15; i++ {
		jobID := fmt.Sprintf("job-%d", i)
		payload := fmt.Sprintf("Payload info for task %d", i)
		if err := enqueueJob(runCtx, db, jobID, payload); err != nil {
			log.Fatalf("\033[1;31m[Producer] Failed to enqueue job %s: %v\033[0m", jobID, err)
		}
	}
	log.Printf("\033[1;32m[Producer] Enqueued 15 jobs successfully.\033[0m")

	// Start 3 worker goroutines
	numWorkers := 3
	log.Printf("\033[1;34m[System] Starting %d workers...\033[0m", numWorkers)
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(runCtx, i, db, &wg)
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

	// Show final state summary of jobs in the database
	printJobSummary(db)
	log.Printf("\033[1;32m[System] Shutdown complete.\033[0m")
}

func initSchema(db *sql.DB) error {
	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		id VARCHAR(255) PRIMARY KEY,
		payload TEXT NOT NULL,
		status VARCHAR(50) NOT NULL DEFAULT 'pending',
		attempts INT NOT NULL DEFAULT 0,
		created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
	);
	CREATE INDEX IF NOT EXISTS idx_jobs_status_created_at ON jobs(status, created_at) WHERE status = 'pending';
	`
	_, err := db.Exec(schema)
	return err
}

func enqueueJob(ctx context.Context, db *sql.DB, id, payload string) error {
	query := `
		INSERT INTO jobs (id, payload, status, attempts, created_at, updated_at)
		VALUES ($1, $2, 'pending', 0, NOW(), NOW())
		ON CONFLICT (id) DO UPDATE 
		SET status = 'pending', attempts = 0, updated_at = NOW()
	`
	_, err := db.ExecContext(ctx, query, id, payload)
	return err
}

// worker loops and polls the database for new pending jobs.
func worker(ctx context.Context, id int, db *sql.DB, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("\033[36m[Worker %d] Ready for jobs.\033[0m", id)
	for {
		select {
		case <-ctx.Done():
			log.Printf("\033[33m[Worker %d] Stopped.\033[0m", id)
			return
		default:
			// Fetch next pending job using atomic lock
			job, found, err := acquireNextJob(ctx, db)
			if err != nil {
				log.Printf("\033[31m[Worker %d] Error acquiring job: %v\033[0m", id, err)
				time.Sleep(1 * time.Second) // Wait before retrying on database error
				continue
			}
			if !found {
				// No jobs available, sleep briefly before polling again
				time.Sleep(500 * time.Millisecond)
				continue
			}

			// Process the acquired job
			processJob(ctx, id, db, job)
		}
	}
}

// acquireNextJob uses SKIP LOCKED to atomically grab and lock a pending job.
func acquireNextJob(ctx context.Context, db *sql.DB) (Job, bool, error) {
	var job Job
	query := `
		UPDATE jobs
		SET status = 'running', updated_at = NOW()
		WHERE id = (
			SELECT id
			FROM jobs
			WHERE status = 'pending'
			ORDER BY created_at ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, payload, status, attempts;
	`
	err := db.QueryRowContext(ctx, query).Scan(&job.ID, &job.Payload, &job.Status, &job.Attempts)
	if err == sql.ErrNoRows {
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, err
	}
	return job, true, nil
}

// processJob executes job processing and saves updates back to database.
func processJob(ctx context.Context, workerID int, db *sql.DB, job Job) {
	log.Printf("\033[34m[Worker %d] ➡️ Processing Job %s - Attempt %d/3\033[0m", workerID, job.ID, job.Attempts+1)

	// Simulate work duration: random between 200ms and 800ms
	workDuration := time.Duration(200+rand.Intn(600)) * time.Millisecond
	
	// We use a select with time.Sleep to allow context checking if needed, 
	// but sleep is fine as workers finish current jobs anyway.
	time.Sleep(workDuration)

	// Simulate 30% failure rate
	if rand.Float32() < 0.3 {
		job.Attempts++
		log.Printf("\033[31m[Worker %d] ❌ Job %s FAILED (Attempt %d/3)\033[0m", workerID, job.ID, job.Attempts)

		if job.Attempts < 3 {
			log.Printf("\033[35m[Worker %d] 🔁 Requeueing Job %s...\033[0m", workerID, job.ID)
			if err := updateJobStatus(ctx, db, job.ID, "pending", job.Attempts); err != nil {
				log.Printf("\033[31m[Worker %d] Error requeueing job %s: %v\033[0m", workerID, job.ID, err)
			}
		} else {
			log.Printf("\033[1;31m[Worker %d] 💀 Job %s failed permanently after 3 attempts. Marking failed_permanently.\033[0m", workerID, job.ID)
			if err := updateJobStatus(ctx, db, job.ID, "failed_permanently", job.Attempts); err != nil {
				log.Printf("\033[31m[Worker %d] Error marking job %s as failed: %v\033[0m", workerID, job.ID, err)
			}
		}
	} else {
		log.Printf("\033[32m[Worker %d] ✅ Job %s completed successfully in %v\033[0m", workerID, job.ID, workDuration)
		if err := updateJobStatus(ctx, db, job.ID, "succeeded", job.Attempts); err != nil {
			log.Printf("\033[31m[Worker %d] Error completing job %s: %v\033[0m", workerID, job.ID, err)
		}
	}
}

func updateJobStatus(ctx context.Context, db *sql.DB, id, status string, attempts int) error {
	query := `
		UPDATE jobs
		SET status = $1, attempts = $2, updated_at = NOW()
		WHERE id = $3
	`
	_, err := db.ExecContext(ctx, query, status, attempts, id)
	return err
}

func printJobSummary(db *sql.DB) {
	query := `
		SELECT status, count(*) 
		FROM jobs 
		GROUP BY status
	`
	rows, err := db.Query(query)
	if err != nil {
		log.Printf("\033[31m[System] Error printing job summary: %v\033[0m", err)
		return
	}
	defer rows.Close()

	fmt.Println("\n\033[1;35m--- Final Job Status Summary in DB ---\033[0m")
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err == nil {
			color := "\033[32m" // Green for success
			if status == "pending" {
				color = "\033[33m" // Yellow
			} else if status == "running" {
				color = "\033[36m" // Cyan
			} else if status == "failed_permanently" {
				color = "\033[31m" // Red
			}
			fmt.Printf("  %s%s\033[0m: %d\n", color, status, count)
		}
	}
	fmt.Println()
}
