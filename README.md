# Distributed Job Queue

A background job queue written in Go and backed by Redis. Producers enqueue
jobs; a pool of consumer processes pick them up, run them, retry failures with
exponential backoff, and recover jobs from workers that crash. Every job's
lifecycle is published to Kafka and archived in Postgres, the system is
instrumented with Prometheus and Grafana, and it deploys to Kubernetes.

## Features

**Reliable delivery**
- **At-least-once delivery.** A job is never lost: every state change is a
  single atomic Redis operation, so there is no moment where a crash can drop
  it.
- **Crash recovery.** A job held by a worker that dies is detected and
  recovered by a background sweeper, using a visibility timeout (a lease the
  worker must keep renewing).
- **Heartbeats.** Long-running jobs keep their lease alive, so a slow but
  healthy worker is never mistaken for a dead one.
- **Ownership checks.** When a worker and the sweeper race for the same job,
  exactly one wins, so a job is never requeued twice.

**Failure handling**
- **Retries with exponential backoff and jitter**, so a failing dependency is
  not hammered and retries do not arrive in synchronized waves.
- **Dead letter queue** for jobs that exhaust their attempts or fail with a
  non-retryable error. Jobs are kept for inspection and replay, not deleted.
- **Poison message quarantine** for payloads that cannot be parsed.
- **Per-type retry policy.** A handler can mark a failure as permanent to skip
  the remaining retries.

**Running it**
- **Pluggable job handlers.** Each job type has its own handler, timeout and
  retry budget.
- **Graceful shutdown.** On SIGTERM, workers finish their current job and stop
  taking new ones; a bounded deadline stops a hung job from blocking shutdown.
- **Waits for dependencies at startup** instead of crashing when Redis or
  Postgres is slow to come up.
- **Horizontal scaling.** Run as many consumers as you like; they coordinate
  entirely through Redis.
- **Fully configurable** through environment variables.

**Visibility**
- **Prometheus metrics** for throughput, failures, latency percentiles, queue
  depth, retries and recoveries, plus a ready-made **Grafana dashboard**.
- **Kafka event stream** of every job transition (started, succeeded, failed,
  retried, buried, recovered), consumed by an **auditor** that archives it to
  Postgres for querying. The stream can be replayed from the beginning.
  Kafka is optional: without it the queue runs unchanged.

**Deployment**
- **Docker Compose** for the full local stack.
- **Kubernetes manifests** with health probes, a disruption budget,
  queue-depth autoscaling and alerting rules. Autoscaling needs
  prometheus-adapter or KEDA, and the alerts need the Prometheus Operator.
- **CI** runs the tests under Go's race detector plus an end-to-end test
  against a real Redis.

## Architecture

```
producer ──LPUSH──▶ jobs:queue (LIST)
                        │
                        │ BLMOVE (atomic pop + record)
                        ▼
                   jobs:inflight (LIST) ◀──── what is being worked on
                   jobs:deadlines (ZSET) ◀─── is its owner still alive?
                        │
         ┌──────────────┼───────────────┬──────────────────┐
         ▼              ▼               ▼                  ▼
     success      retry (backoff)   exhausted/permanent  unparseable
                  jobs:delayed ZSET  jobs:dlq LIST       jobs:poison LIST
                        │
                  promoter ──▶ back onto jobs:queue

consumer ──▶ :2112/metrics ──▶ Prometheus ──▶ Grafana
consumer ──▶ Kafka "job-events" (3 partitions) ──▶ auditor ──▶ Postgres
```

| Binary | What it does | Runs as |
|---|---|---|
| `cmd/producer` | Enqueues a batch of jobs and exits | Kubernetes Job |
| `cmd/consumer` | Workers, sweeper, promoter, metrics, event publishing | Deployment |
| `cmd/auditor` | Consumes the Kafka event stream into Postgres | Deployment |

### How a job moves through the system

1. **Enqueue.** The producer pushes the job onto `jobs:queue`.
2. **Pick up.** A worker takes it with `BLMOVE`, which removes it from the
   queue and records it in `jobs:inflight` in one atomic step.
3. **Claim.** The worker sets a deadline in `jobs:deadlines`, and a heartbeat
   extends it while the job runs.
4. **Run.** The handler registered for the job's type executes it.
5. **Settle.** The job is released and moved to its next state in one atomic
   step: done, scheduled for retry in `jobs:delayed`, or buried in `jobs:dlq`.
6. **Retry.** The promoter moves retries back onto the queue once their
   backoff has elapsed.
7. **Recover.** If a worker stops heartbeating, the sweeper reclaims its job
   and schedules a retry.

## Quick start

Requires Go and Docker.

```bash
docker compose up -d                                    # Redis, Kafka, Postgres, Prometheus, Grafana
KAFKA_BROKERS=localhost:9092 go run ./cmd/consumer     # metrics on :2112
DATABASE_URL=postgres://postgres:postgres@localhost:5433/jobqueue?sslmode=disable \
  KAFKA_BROKERS=localhost:9092 go run ./cmd/auditor
go run ./cmd/producer                                   # enqueue 100 jobs
```

| Service | URL |
|---|---|
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Kafka UI | http://localhost:8080 |
| Consumer metrics | http://localhost:2112/metrics |

Postgres is published on **5433**, not 5432, so it does not clash with a local
PostgreSQL install.

## Adding a job type

Register a handler in [`cmd/consumer/handlers.go`](cmd/consumer/handlers.go).
Jobs whose `type` matches its name are routed to it.

```go
register(JobHandler{
    Name:        "send-email",
    Timeout:     10 * time.Second, // per attempt; the handler gets a context that expires
    MaxAttempts: 5,                // overrides the global MAX_ATTEMPTS (0 = use global)
    Run: func(ctx context.Context, job Job) error {
        if job.Payload == "" {
            // Retrying cannot fix this: go straight to the DLQ.
            return fmt.Errorf("empty payload: %w", ErrPermanent)
        }
        return sendEmail(ctx, job.Payload) // any other error is retried with backoff
    },
})
```

Handlers should respect `ctx.Done()` so the timeout and graceful shutdown can
take effect, and should be idempotent, since delivery is at-least-once.

## Configuration

**Consumer**

| Variable | Default | Purpose |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection |
| `WORKER_COUNT` | `3` | Workers per consumer process |
| `MAX_ATTEMPTS` | `3` | Attempts before a job is buried |
| `BASE_RETRY_DELAY` | `1s` | First retry delay; doubles each attempt |
| `MAX_RETRY_DELAY` | `5m` | Cap on the retry delay |
| `VISIBILITY_TIMEOUT` | `15s` | How long a job may go without a heartbeat before it is recovered |
| `HEARTBEAT_INTERVAL` | `5s` | How often a running job renews its lease |
| `SWEEP_INTERVAL` | `3s` | How often the sweeper looks for abandoned jobs |
| `PROMOTE_INTERVAL` | `1s` | How often due retries return to the queue |
| `BLOCK_TIMEOUT` | `15s` | How long an idle worker waits for work per Redis call |
| `STARTUP_TIMEOUT` | `30s` | How long to wait for Redis at startup |
| `SHUTDOWN_TIMEOUT` | `30s` | How long to let in-flight jobs finish on shutdown |
| `METRICS_ADDR` | `:2112` | Metrics and health endpoint |
| `DEPTH_POLL_INTERVAL` | `5s` | How often queue-depth gauges are sampled |
| `KAFKA_BROKERS` | *(unset)* | Enables event publishing when set |
| `KAFKA_TOPIC` | `job-events` | Event topic |
| `KAFKA_PARTITIONS` | `3` | Partitions when the topic is created |
| `KAFKA_REPLICATION_FACTOR` | `1` | Replication factor when the topic is created |
| `KAFKA_MAX_BUFFERED_RECORDS` | `10000` | Events held in memory while Kafka is unreachable |
| `KAFKA_DELIVERY_TIMEOUT` | `30s` | When an undeliverable event is dropped (minimum 1s) |

The consumer warns at startup if the heartbeat interval is too close to the
visibility timeout.

**Auditor**

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Kafka connection |
| `KAFKA_TOPIC` | `job-events` | Topic to consume |
| `KAFKA_GROUP` | `job-auditor` | Consumer group |
| `DATABASE_URL` | `postgres://…@localhost:5432/jobqueue` | Postgres connection |
| `STARTUP_TIMEOUT` | `30s` | How long to wait for Postgres at startup |

Run the auditor with `--from-start` to replay the whole topic into Postgres.
Writes are idempotent, so a replay rebuilds the same table without duplicates.

## Observability

The consumer exposes Prometheus metrics at `/metrics`; the Grafana dashboard
is provisioned automatically. The metrics that matter most:

| Metric | What it tells you |
|---|---|
| `jobqueue_jobs_processed_total{result}` | Throughput and failure rate |
| `jobqueue_job_duration_seconds` | Latency percentiles |
| `jobqueue_depth{structure}` | Backlog, in-flight, delayed and DLQ sizes |
| `jobqueue_jobs_dlq_total{reason}` | Jobs given up on, and why |
| `jobqueue_jobs_recovered_total` | Jobs recovered from crashed workers |
| `jobqueue_stale_results_discarded_total` | Workers that lost a race for a job; should be zero |

The full list, with PromQL examples, is in
[monitoring/README.md](monitoring/README.md). Alerting rules are in
[k8s/06-monitoring.yaml](k8s/06-monitoring.yaml).

## Kubernetes

```bash
TAG=$(git rev-parse --short HEAD)
docker build -t jobqueue:$TAG .
kubectl apply -k k8s/
kubectl -n jobqueue set image deployment/jobqueue-consumer consumer=jobqueue:$TAG
```

The manifests cover Redis, Kafka and Postgres as StatefulSets, the consumer
and auditor as Deployments, and the producer as a Job. See
[k8s/README.md](k8s/README.md) for the full walkthrough, including how to test
crash recovery.

## Testing

```bash
go test ./...                                                  # unit tests, no services needed
REDIS_URL=redis://localhost:6379/0 bash scripts/smoke-test.sh  # end to end, against an empty Redis
```

The unit tests use an in-process Redis (miniredis), so they need nothing
running. The smoke test runs the real consumer and producer against a real
Redis and checks that all 100 jobs finish exactly once. CI runs both.

## Limitations

- **At-least-once, not exactly-once.** A job can run twice if a worker dies
  after doing its work but before recording that it did. Handlers should be
  idempotent.
- **Single-node dependencies.** One Redis, one Kafka broker and one Postgres;
  each is a single point of failure in this setup.
- **An idle worker can take up to `BLOCK_TIMEOUT` to notice shutdown.** It
  takes no new work in that window, but a drain can take up to 15 seconds.

## Further reading

- [docs/ENGINEERING_NOTES.md](docs/ENGINEERING_NOTES.md): the bugs found in
  this system and how each was fixed
- [k8s/README.md](k8s/README.md): deploying and testing on Kubernetes
- [monitoring/README.md](monitoring/README.md): metrics and dashboards
