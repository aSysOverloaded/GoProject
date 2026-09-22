package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

// Metrics are held in memory and read by Prometheus when it scrapes
// /metrics. Nothing here talks to the network on the job hot path.
//
// Counters only ever increase; you chart rate() over them, not the raw
// value. Gauges move in both directions. Histograms bucket observations so
// you can ask for a percentile instead of a misleading average.
var (
	// jobsProcessed is labelled by outcome rather than split into two
	// metrics, so a single PromQL query can chart both and derive the
	// failure ratio between them.
	jobsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jobqueue_jobs_processed_total",
		Help: "Jobs that finished processing, by outcome.",
	}, []string{"result"})

	// jobsRetried and jobsBuried split the two ends of the retry policy.
	// The source label distinguishes an ordinary worker failure from a
	// crash recovery driven by the sweeper.
	jobsRetried = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jobqueue_jobs_retried_total",
		Help: "Jobs pushed back onto the queue for another attempt.",
	}, []string{"source"})

	// reason distinguishes a job that burned through its retry budget from one
	// whose handler reported ErrPermanent and was buried on the first failure.
	// They call for different responses: the first suggests a flaky
	// dependency, the second a bad payload or a bug.
	jobsBuried = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jobqueue_jobs_dlq_total",
		Help: "Jobs sent to the dead letter queue, by who buried it and why.",
	}, []string{"source", "reason"})

	// jobsPoisoned counts payloads that could not be parsed as a Job at all.
	// Non-zero means something is writing malformed data to the queue.
	jobsPoisoned = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobqueue_jobs_poisoned_total",
		Help: "Payloads quarantined because they could not be parsed as a job.",
	})

	// jobsPromoted counts jobs moved out of the delayed set once their retry
	// backoff elapsed.
	jobsPromoted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobqueue_jobs_promoted_total",
		Help: "Delayed jobs promoted back onto the main queue after their backoff elapsed.",
	})

	// retryDelay records how long jobs are made to wait before a retry, which
	// is how you confirm the exponential backoff is actually backing off
	// rather than hammering.
	retryDelay = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "jobqueue_retry_delay_seconds",
		Help:    "Backoff applied before a job's next attempt.",
		Buckets: prometheus.ExponentialBuckets(0.5, 2, 10),
	})

	// shutdownsForced counts exits where the shutdown deadline elapsed with
	// work still in flight. Non-zero means a handler is ignoring its context.
	shutdownsForced = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobqueue_forced_shutdowns_total",
		Help: "Shutdowns that hit the deadline with jobs still running.",
	})

	// deadlinesReaped counts deadline entries removed because no job in the
	// in-flight list referenced them. It should stay at zero.
	//
	// Every release now goes through one atomic script, and adoption checks
	// that the job is still in flight, so normal operation cannot leave an
	// orphan behind. What is left is identical payloads in flight at once,
	// from a producer that does not set enqueue_id - which also loses jobs
	// through ZSET de-duplication, so a non-zero value needs attention.
	deadlinesReaped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobqueue_deadlines_reaped_total",
		Help: "Orphaned deadline entries removed by the sweeper. Expected to stay at zero; non-zero means a producer is enqueueing identical payloads without an enqueue_id.",
	})

	// jobsHandedBack counts jobs a shutting-down worker received but returned
	// to the queue unrun. Movement during rollouts is expected: it is the
	// drain working correctly.
	jobsHandedBack = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobqueue_jobs_handed_back_total",
		Help: "Jobs a shutting-down worker received and returned to the queue without running.",
	})

	// kafkaPublished counts lifecycle events written to the event stream.
	kafkaPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jobqueue_events_published_total",
		Help: "Job lifecycle events successfully published to Kafka.",
	}, []string{"event"})

	// kafkaPublishErrors counts events that could not be published. Because
	// publishing is deliberately off the job hot path, this climbing means
	// the audit trail has gaps - not that job processing is affected.
	kafkaPublishErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobqueue_events_publish_errors_total",
		Help: "Job lifecycle events that failed to publish. Job processing is unaffected; the audit trail is incomplete.",
	})

	// jobsRecovered counts sweeper reclaims. Before the heartbeat fix this
	// climbed steadily even with no crashes, because slow-but-healthy
	// workers were being declared dead. On a healthy system it should only
	// move when a consumer actually dies.
	jobsRecovered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobqueue_jobs_recovered_total",
		Help: "Jobs reclaimed by the sweeper after their owner stopped heartbeating.",
	})

	// staleResults is the direct read-out of the ownership handshake: a
	// worker finished a job the sweeper had already taken away. Any value
	// above zero means a worker is outliving its visibility timeout, so
	// visTimeout or heartbeatTick needs adjusting.
	staleResults = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobqueue_stale_results_discarded_total",
		Help: "Worker results dropped because the sweeper had already reclaimed the job. Expected to stay at zero.",
	})

	// Buckets are tuned to the 200-800ms simulated workload: roughly 50ms
	// up to 6.4s. The default buckets top out too high to show useful
	// detail here.
	jobDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "jobqueue_job_duration_seconds",
		Help:    "Wall-clock time spent executing a job, excluding queue wait and bookkeeping.",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 8),
	})

	// queueDepth is sampled from Redis on a timer rather than tracked
	// incrementally, so it stays correct even when several consumers share
	// the same queue.
	queueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "jobqueue_depth",
		Help: "Current number of jobs held in each Redis structure.",
	}, []string{"structure"})
)

// depthPollInterval controls how often the gauges are refreshed. Each tick
// costs four Redis commands, which matters on a metered host like Upstash.
var depthPollInterval = 5 * time.Second

// A labelled metric exports nothing until a given label combination has been
// touched, which makes a fresh dashboard read "no data" rather than zero, and
// makes rate() blind to the very first event in a series. Creating every
// combination up front at zero avoids both.
func init() {
	for _, result := range []string{"success", "failure"} {
		jobsProcessed.WithLabelValues(result)
	}
	for _, source := range []string{"worker", "sweeper"} {
		jobsRetried.WithLabelValues(source)
		for _, reason := range []string{"exhausted", "permanent"} {
			jobsBuried.WithLabelValues(source, reason)
		}
	}
	for _, structure := range []string{"pending", "inflight", "dlq", "claimed", "delayed", "poison"} {
		queueDepth.WithLabelValues(structure)
	}
}

// serveMetrics exposes /metrics for Prometheus to scrape. It returns the
// server so main can shut it down cleanly.
func serveMetrics(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	// A trivial liveness endpoint, handy when this runs in a container.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		log.Printf("\033[1;34m[Metrics] Serving Prometheus metrics on %s/metrics\033[0m", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("\033[31m[Metrics] Server stopped: %v\033[0m", err)
		}
	}()

	return srv
}

// pollQueueDepths keeps the gauges in step with what is actually in Redis.
func pollQueueDepths(ctx context.Context, rdb *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(depthPollInterval)
	defer ticker.Stop()

	sample := func() {
		pipe := rdb.Pipeline()
		pending := pipe.LLen(ctx, queueKey)
		inflight := pipe.LLen(ctx, inflightKey)
		dead := pipe.LLen(ctx, dlqKey)
		poison := pipe.LLen(ctx, poisonKey)
		claimed := pipe.ZCard(ctx, deadlinesKey)
		delayed := pipe.ZCard(ctx, delayedKey)

		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			if !errors.Is(err, context.Canceled) {
				log.Printf("\033[31m[Metrics] Error sampling queue depths: %v\033[0m", err)
			}
			return
		}

		queueDepth.WithLabelValues("pending").Set(float64(pending.Val()))
		queueDepth.WithLabelValues("inflight").Set(float64(inflight.Val()))
		queueDepth.WithLabelValues("dlq").Set(float64(dead.Val()))
		queueDepth.WithLabelValues("poison").Set(float64(poison.Val()))
		queueDepth.WithLabelValues("claimed").Set(float64(claimed.Val()))
		queueDepth.WithLabelValues("delayed").Set(float64(delayed.Val()))
	}

	// Take one sample immediately so the dashboard is populated before the
	// first tick rather than showing a gap on startup.
	sample()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sample()
		}
	}
}
