package main

import (
	"context"
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

// scheduleRetry puts a job into the delayed set, scored by the epoch-ms at
// which it becomes eligible again.
//
// This is the same ZSET-as-timer pattern the deadline tracking already uses:
// a sorted set scored by a timestamp is a priority queue ordered by time, so
// "everything that is due" is one range query.
func scheduleRetry(ctx context.Context, rdb *redis.Client, who actor, job Job, jobJSON string) error {
	delay := backoffFor(job.Attempts)
	readyAt := time.Now().Add(delay)

	if err := rdb.ZAdd(ctx, delayedKey, redis.Z{
		Score:  float64(readyAt.UnixMilli()),
		Member: jobJSON,
	}).Err(); err != nil {
		return err
	}

	log.Printf("\033[35m[%s] 🔁 Job %s failed, retrying in %v (attempt %d/%d)\033[0m",
		who.name, job.ID, delay.Round(time.Millisecond), job.Attempts, attemptsFor(job.Type))

	jobsRetried.WithLabelValues(who.label).Inc()
	retryDelay.Observe(delay.Seconds())
	return nil
}

// promoter moves jobs whose backoff has elapsed from the delayed set back onto
// the main queue.
//
// Each job is claimed with ZREM before being pushed, exactly like the
// ownership handshake elsewhere: with several consumers all running promoters,
// only the one whose ZREM returns 1 gets to push, so a job cannot be promoted
// twice.
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
		// Claim it. Only the winner pushes.
		claimed, err := rdb.ZRem(ctx, delayedKey, jobJSON).Result()
		if err != nil || claimed == 0 {
			continue
		}

		if err := rdb.LPush(ctx, queueKey, jobJSON).Err(); err != nil {
			// Put it back so the job is not lost; it will be retried on the
			// next pass.
			log.Printf("\033[31m[Promoter] Failed to promote a job, returning it to the delayed set: %v\033[0m", err)
			rdb.ZAdd(ctx, delayedKey, redis.Z{Score: float64(now), Member: jobJSON})
			continue
		}
		promoted++
	}

	if promoted > 0 {
		jobsPromoted.Add(float64(promoted))
		log.Printf("\033[35m[Promoter] ⏰ Promoted %d job(s) back onto the queue\033[0m", promoted)
	}
	return promoted
}
