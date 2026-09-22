package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// backoffFor returns how long to wait before attempt number `attempt`
// (1-based: the delay before the 2nd attempt is backoffFor(1)).
//
// Exponential: base * 2^(attempt-1), capped at maxRetryDelay.
//
// Then jittered to a random point in [50%, 100%] of that delay. Jitter is not
// cosmetic. Without it, a downstream outage that fails 500 jobs at once makes
// all 500 retry at the same instant, and again in lockstep on every
// subsequent attempt - a thundering herd that re-breaks whatever just
// recovered. Spreading retries over a window turns a spike into a trickle.
func backoffFor(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	delay := float64(baseRetryDelay) * math.Pow(2, float64(attempt-1))
	if delay > float64(maxRetryDelay) || math.IsInf(delay, 0) {
		delay = float64(maxRetryDelay)
	}

	// Full delay, minus up to half of it.
	jittered := delay * (0.5 + 0.5*rand.Float64())
	return time.Duration(jittered)
}

// failurePlan is where a failed job goes next. It is decided before Redis is
// touched, so that releasing the job and writing its next state can be one
// atomic transition (see releaseScript).
type failurePlan struct {
	to     next
	retry  bool
	delay  time.Duration // when retried
	reason string        // "exhausted" or "permanent", when buried
}

// planFailure applies the retry policy to a job whose Attempts already counts
// the attempt that just failed.
//
// Retries go into the delayed set, scored by the epoch-ms at which they
// become eligible again: a sorted set scored by a timestamp is a priority
// queue ordered by time, so "everything that is due" is one range query.
func planFailure(job Job, runErr error) (failurePlan, error) {
	payload, err := json.Marshal(job)
	if err != nil {
		return failurePlan{}, err
	}

	// A permanent error will fail identically on every future attempt, so
	// spending the remaining budget (and the backoff waits) on it is pure
	// latency for a guaranteed outcome. Bury it immediately.
	permanent := errors.Is(runErr, ErrPermanent)
	if !permanent && job.Attempts < attemptsFor(job.Type) {
		delay := backoffFor(job.Attempts)
		return failurePlan{
			retry: true,
			delay: delay,
			to: next{
				mode:    "zadd",
				key:     delayedKey,
				payload: string(payload),
				score:   float64(time.Now().Add(delay).UnixMilli()),
			},
		}, nil
	}

	reason := "exhausted"
	if permanent {
		reason = "permanent"
	}
	return failurePlan{
		reason: reason,
		to:     next{mode: "lpush", key: dlqKey, payload: string(payload)},
	}, nil
}

// recordFailure reports a failed job's next step. It runs only after the
// transition has actually happened, so the logs, metrics and events never
// describe a move that did not take place.
func recordFailure(who actor, job Job, plan failurePlan, runErr error) {
	if plan.retry {
		log.Printf("\033[35m[%s] 🔁 Job %s failed, retrying in %v (attempt %d/%d)\033[0m",
			who.name, job.ID, plan.delay.Round(time.Millisecond), job.Attempts, attemptsFor(job.Type))
		jobsRetried.WithLabelValues(who.label).Inc()
		retryDelay.Observe(plan.delay.Seconds())
		events.publish(JobEvent{
			JobID:      job.ID,
			JobType:    job.Type,
			Event:      EventRetried,
			Attempt:    job.Attempts,
			DurationMS: plan.delay.Milliseconds(),
			Worker:     who.name,
			Reason:     "scheduled for retry after backoff",
		})
		return
	}

	if plan.reason == "permanent" {
		log.Printf("\033[1;31m[%s] 💀 Job %s failed permanently (not retryable). Sending to DLQ.\033[0m", who.name, job.ID)
	} else {
		log.Printf("\033[1;31m[%s] 💀 Job %s exhausted %d attempts. Sending to DLQ.\033[0m", who.name, job.ID, attemptsFor(job.Type))
	}
	jobsBuried.WithLabelValues(who.label, plan.reason).Inc()

	buried := JobEvent{
		JobID:   job.ID,
		JobType: job.Type,
		Event:   EventBuried,
		Attempt: job.Attempts,
		Worker:  who.name,
		Reason:  plan.reason,
	}
	if runErr != nil {
		buried.Error = runErr.Error()
	}
	events.publish(buried)
}

// promoteScript moves one due job from the delayed set onto the queue as a
// single atomic step. ZREM is the claim, exactly like the ownership handshake
// elsewhere: with several consumers running promoters, only the one whose
// ZREM returns 1 pushes, so a job cannot be promoted twice. Doing the push in
// the same script means a job can never be removed from the delayed set
// without arriving on the queue - the old two-command version needed a
// "put it back" fallback for that, which could itself fail.
var promoteScript = redis.NewScript(`
if redis.call('ZREM', KEYS[1], ARGV[1]) == 1 then
  redis.call('LPUSH', KEYS[2], ARGV[1])
  return 1
end
return 0
`)

// promoter moves jobs whose backoff has elapsed from the delayed set back onto
// the main queue.
func promoter(ctx context.Context, rdb *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("\033[1;30m[Promoter] Watching for jobs whose backoff has elapsed...\033[0m")

	ticker := time.NewTicker(promoteTick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("\033[1;30m[Promoter] Stopped.\033[0m")
			return
		case <-ticker.C:
			promoteDue(ctx, rdb)
		}
	}
}

// promoteDue is one promotion pass, split out so tests can drive it directly.
func promoteDue(ctx context.Context, rdb *redis.Client) int {
	now := time.Now().UnixMilli()

	// Bounded batch: a huge backlog of simultaneously-due retries should be
	// drained over several ticks rather than in one enormous burst.
	due, err := rdb.ZRangeByScore(ctx, delayedKey, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   strconv.FormatInt(now, 10),
		Count: 100,
	}).Result()
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			log.Printf("\033[31m[Promoter] Error reading delayed set: %v\033[0m", err)
		}
		return 0
	}

	promoted := 0
	for _, jobJSON := range due {
		n, err := promoteScript.Run(ctx, rdb, []string{delayedKey, queueKey}, jobJSON).Int()
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				// Nothing moved: the job is still in the delayed set and the
				// next pass will try again.
				log.Printf("\033[31m[Promoter] Failed to promote a job, will retry next pass: %v\033[0m", err)
			}
			continue
		}
		promoted += n
	}

	if promoted > 0 {
		jobsPromoted.Add(float64(promoted))
		log.Printf("\033[35m[Promoter] ⏰ Promoted %d job(s) back onto the queue\033[0m", promoted)
	}
	return promoted
}
