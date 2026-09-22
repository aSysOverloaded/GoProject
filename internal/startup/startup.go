// Package startup waits for a service's dependencies to become reachable.
//
// A process that makes one connection attempt and exits on failure turns
// every slow dependency into a crash. On Kubernetes the consumers and
// auditors did exactly that: they started before Redis and Postgres were
// ready and crash-looped until they were. Kubernetes papered over it by
// restarting them, but restart delays grow each time, it fills the event log
// with CrashLoopBackOff, and anywhere without an orchestrator - a laptop, a
// CI job - the process simply dies.
//
// Waiting a bounded time absorbs a slow start or a brief blip. A dependency
// that is genuinely down still fails startup, once the budget runs out, so
// the orchestrator can take over from there.
package startup

import (
	"context"
	"fmt"
	"log"
	"time"
)

const (
	firstDelay     = 250 * time.Millisecond
	maxDelay       = 5 * time.Second
	attemptTimeout = 3 * time.Second
)

// Retry calls attempt until it succeeds or within has elapsed, backing off
// exponentially from 250ms to 5s between tries. Each attempt gets its own
// timeout, so one hung call cannot use up the whole budget. It always makes
// at least one attempt.
func Retry(name string, within time.Duration, attempt func(ctx context.Context) error) error {
	deadline := time.Now().Add(within)
	delay := firstDelay

	for n := 1; ; n++ {
		timeout := attemptTimeout
		if left := time.Until(deadline); left > 0 && left < timeout {
			timeout = left
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		err := attempt(ctx)
		cancel()

		if err == nil {
			if n > 1 {
				log.Printf("[Startup] %s reachable after %d attempts", name, n)
			}
			return nil
		}

		left := time.Until(deadline)
		if left <= 0 {
			return fmt.Errorf("%s not reachable after %d attempt(s) over %v: %w", name, n, within, err)
		}
		if delay > left {
			delay = left
		}
		log.Printf("[Startup] %s not reachable yet (attempt %d): %v - retrying in %v", name, n, err, delay.Round(time.Millisecond))
		time.Sleep(delay)

		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
}
