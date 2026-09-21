package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"
)

// ErrPermanent marks a failure that retrying cannot fix: a malformed payload,
// an unknown job type, a validation error. Wrapping it sends the job straight
// to the DLQ instead of burning three attempts and a backoff window on an
// outcome that will never change.
//
//	return fmt.Errorf("customer %s does not exist: %w", id, ErrPermanent)
var ErrPermanent = errors.New("permanent failure")

// JobHandler is the unit of real work. Registering one is how a caller
// actually uses this queue for something.
type JobHandler struct {
	// Name matches Job.Type. The empty name is the fallback for jobs that
	// carry no type, which keeps older payloads working.
	Name string

	// Timeout bounds a single attempt. The handler is given a context that
	// expires after this long.
	//
	// Go cannot forcibly stop a function, so this is only a real bound if the
	// handler actually honours ctx.Done(). A handler that ignores it will run
	// past its timeout and hold up shutdown until SHUTDOWN_TIMEOUT.
	Timeout time.Duration

	// MaxAttempts overrides the global MAX_ATTEMPTS for this type. Zero means
	// use the global value. Useful when, say, sending email is worth five
	// tries but generating a report is worth one.
	MaxAttempts int

	Run func(ctx context.Context, job Job) error
}

// handlers is the registry. It is written once at startup, before any worker
// goroutine starts, and only read afterwards - so it needs no mutex.
var handlers = map[string]JobHandler{}

func register(h JobHandler) {
	if h.Timeout == 0 {
		h.Timeout = 30 * time.Second
	}
	handlers[h.Name] = h
}

// attemptsFor returns the retry budget for a job's type.
func attemptsFor(jobType string) int {
	if h, ok := handlers[jobType]; ok && h.MaxAttempts > 0 {
		return h.MaxAttempts
	}
	return maxAttempts
}

func init() {
	// The default handler, used for jobs with no Type. This is the original
	// simulated workload: 200-800ms of work with a 30% failure rate. It is
	// what the producer generates and what the benchmark numbers describe.
	register(JobHandler{
		Name:    "",
		Timeout: 30 * time.Second,
		Run: func(ctx context.Context, job Job) error {
			work := time.Duration(200+rand.Intn(600)) * time.Millisecond

			// Sleep, but stay interruptible. This is what every handler should
			// do: never block on a bare time.Sleep when you hold a context.
			select {
			case <-time.After(work):
			case <-ctx.Done():
				return ctx.Err()
			}

			if rand.Float32() < 0.3 {
				return errors.New("simulated transient failure")
			}
			return nil
		},
	})

	// A second handler, to show the registry doing something a single
	// hardcoded function could not: different work, its own timeout, and its
	// own retry budget.
	register(JobHandler{
		Name:        "uppercase",
		Timeout:     5 * time.Second,
		MaxAttempts: 1, // deterministic; retrying a pure function is pointless
		Run: func(ctx context.Context, job Job) error {
			if job.Payload == "" {
				return fmt.Errorf("empty payload: %w", ErrPermanent)
			}
			return nil
		},
	})
}

// dispatch runs the handler for a job and reports how long the attempt took.
func dispatch(job Job) (err error, took time.Duration) {
	h, ok := handlers[job.Type]
	if !ok {
		// Nothing can process this job, and no amount of retrying will change
		// that, so treat it as permanent and bury it.
		return fmt.Errorf("no handler registered for job type %q: %w", job.Type, ErrPermanent), 0
	}

	// Deliberately rooted at Background rather than the run context: a job
	// already in progress is allowed to finish during a graceful shutdown.
	// SHUTDOWN_TIMEOUT is what bounds the overall wait.
	ctx, cancel := context.WithTimeout(context.Background(), h.Timeout)
	defer cancel()

	start := time.Now()
	err = h.Run(ctx, job)
	return err, time.Since(start)
}
