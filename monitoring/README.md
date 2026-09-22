# Monitoring the job queue

## How the pieces fit

Prometheus **pulls**. The consumer does not send metrics anywhere — it keeps
counters in memory and exposes them as text at `http://localhost:2112/metrics`.
Prometheus fetches that URL every 5s and stores the result as time series.
Grafana queries Prometheus and draws the charts.

```
consumer :2112/metrics  <-- scrape --  Prometheus :9090  <-- query --  Grafana :3000
```

Nothing in the job hot path makes a network call, so the monitoring stack can
be down without affecting job processing.

## Running it

```bash
docker compose up -d          # redis + prometheus + grafana
go run ./cmd/consumer         # serves metrics on :2112
go run ./cmd/producer         # enqueues 100 jobs
```

| Service    | URL                     | Notes                          |
|------------|-------------------------|--------------------------------|
| Grafana    | http://localhost:3000   | anonymous admin, dashboard is pre-provisioned |
| Prometheus | http://localhost:9090   | check Status > Targets first   |
| Redis      | localhost:6379          | appendonly enabled             |

The consumer runs on the host while Prometheus runs in a container, so
`prometheus.yml` scrapes `host.docker.internal:2112`. If you containerise the
consumer later, change that to the compose service name.

**First thing to check if a panel is empty:** Prometheus > Status > Targets.
If `jobqueue-consumer` is not `UP`, the problem is the scrape, not your
queries or the dashboard.

## The metrics

| Metric | Type | Why it exists |
|---|---|---|
| `jobqueue_jobs_processed_total{result}` | counter | Throughput and failure ratio |
| `jobqueue_jobs_retried_total{source}` | counter | Retries, split by worker vs sweeper |
| `jobqueue_jobs_dlq_total{source,reason}` | counter | Jobs buried in the DLQ. `reason` is `exhausted` (ran out of attempts, suggests a flaky dependency) or `permanent` (the handler said retrying cannot help, suggests a bad payload or a bug) |
| `jobqueue_jobs_recovered_total` | counter | Sweeper reclaims after an owner stopped heartbeating |
| `jobqueue_stale_results_discarded_total` | counter | Workers that finished a job the sweeper had already taken. **Should stay at zero** |
| `jobqueue_deadlines_reaped_total` | counter | Deadline entries with no in-flight job, cleaned up by the sweeper. **Should stay at zero**; non-zero means a producer is not setting `enqueue_id` |
| `jobqueue_jobs_poisoned_total` | counter | Payloads that could not be parsed, moved to `jobs:poison` |
| `jobqueue_jobs_promoted_total` | counter | Retries moved back onto the queue once their backoff elapsed |
| `jobqueue_forced_shutdowns_total` | counter | Shutdowns that hit the deadline with jobs still running; usually a handler ignoring its context |
| `jobqueue_job_duration_seconds` | histogram | Execution time, for percentiles |
| `jobqueue_retry_delay_seconds` | histogram | Backoff applied before each retry, to confirm it is actually backing off |
| `jobqueue_depth{structure}` | gauge | Sampled size of `pending`, `inflight`, `delayed`, `dlq`, `poison` and `claimed` (the deadlines ZSET). A sample, not a live value: wait on the counters above when you need to know a job has finished |
| `jobqueue_events_published_total{event}` | counter | Lifecycle events written to Kafka |
| `jobqueue_events_publish_errors_total` | counter | Events that could not be published, including ones dropped because Kafka was unreachable. Jobs are unaffected; the audit trail has gaps |

### The two that are worth watching

`jobqueue_stale_results_discarded_total` **should stay at zero.** It counts
workers that lost the ownership handshake in `settle` — meaning a worker
outlived its visibility timeout and the sweeper reclaimed its job underneath
it. If this climbs, raise `visTimeout` or lower `heartbeatTick`. Before the
heartbeat was added, this condition happened silently and produced duplicate
job execution.

`jobqueue_jobs_recovered_total` **should only move when a consumer actually
dies.** A steady climb under normal load means the sweeper is misclassifying
healthy workers as dead — the same bug seen from the other side.

## PromQL you will actually use

Counters only ever increase, so charting the raw value is useless. `rate()`
converts a counter into a per-second rate over a window:

```promql
# Throughput, jobs/sec
sum(rate(jobqueue_jobs_processed_total[1m]))

# Failure ratio (clamp_min avoids dividing by zero when idle)
sum(rate(jobqueue_jobs_processed_total{result="failure"}[5m]))
  / clamp_min(sum(rate(jobqueue_jobs_processed_total[5m])), 0.0001)

# p95 job duration, derived from the histogram buckets
histogram_quantile(0.95, sum by (le) (rate(jobqueue_job_duration_seconds_bucket[5m])))

# Current backlog (a gauge, so no rate() needed)
jobqueue_depth{structure="pending"}
```

Rule of thumb: `rate()` for counters, raw value for gauges,
`histogram_quantile(...)` over `_bucket` for histograms.

## Verified behaviour

Draining 100 jobs through 3 workers with the built-in 30% failure rate:

```
jobqueue_jobs_processed_total{result="success"} 97
jobqueue_jobs_processed_total{result="failure"} 36
jobqueue_jobs_retried_total{source="worker"}    33
jobqueue_jobs_dlq_total{source="worker"}         3
jobqueue_jobs_recovered_total                    0
jobqueue_stale_results_discarded_total           0
```

Every job is accounted for: 97 succeeded + 3 buried = 100 enqueued, and
100 + 33 requeues = 133 total attempts. No job was lost or run twice.

Hard-killing the consumer while three jobs were in flight, then restarting it:

```
jobqueue_jobs_recovered_total                    3
jobqueue_jobs_retried_total{source="sweeper"}    3
jobqueue_stale_results_discarded_total           0
```

Exactly the three in-flight jobs were reclaimed, with no false positives.
