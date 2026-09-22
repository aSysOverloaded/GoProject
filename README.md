# Distributed Job Queue

A job queue in Go on Redis: multiple consumer processes, each running a pool
of workers, with retries, exponential backoff, a dead letter queue, and crash
recovery through a visibility timeout. It is instrumented with Prometheus,
publishes a Kafka event stream archived to Postgres, and runs on Kubernetes.

Most of the interesting engineering here is in finding and fixing the ways
it failed. Every bug below was found in this codebase, and each fix is backed
by a test that fails without it. [Bugs found and fixed](#bugs-found-and-fixed)
is the main section of this document.

## Architecture

```
producer ──LPUSH──▶ jobs:queue (LIST)
                        │
                        │ BLMOVE (atomic pop + record)
                        ▼
                   jobs:inflight (LIST) ◀──── source of truth: what is being worked on
                   jobs:deadlines (ZSET) ◀─── liveness: is its owner still alive?
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

**Delivery guarantee: at-least-once.** A job is never lost. A job can run more
than once if a worker dies after doing its work but before recording that it
did; handlers should be idempotent. Exactly-once is not claimed anywhere.

## Quick start

```bash
docker compose up -d                                    # Redis, Kafka, Postgres, Prometheus, Grafana
KAFKA_BROKERS=localhost:9092 go run ./cmd/consumer     # metrics on :2112
DATABASE_URL=postgres://postgres:postgres@localhost:5433/jobqueue?sslmode=disable \
  KAFKA_BROKERS=localhost:9092 go run ./cmd/auditor
go run ./cmd/producer                                   # enqueue 100 jobs
```

Grafana at http://localhost:3000, Kafka UI at http://localhost:8080. Kafka is
optional: without `KAFKA_BROKERS` the consumer runs unchanged with event
publishing disabled. For Kubernetes see [k8s/README.md](k8s/README.md); for
the metrics see [monitoring/README.md](monitoring/README.md).

```bash
go test ./...        # 27 tests, no external services needed (uses miniredis)
```

CI additionally runs the suite under the race detector and an integration test
against a real Redis.

---

## Bugs found and fixed

In the order they were found. Each entry says what was happening, how it was
found, why it happened, what fixed it, and how the fix was proven.

A pattern runs through most of them: **the bug was silent.** No error, no
crash, no log line - the queue kept draining while quietly losing or
duplicating work. That is why most fixes come with a metric that turns the
failure into a number, and why each fix is backed by a test that was confirmed
to fail against the broken code.

### 1. A crash between two commands lost the job entirely

*Fixed in `453e99c`.*

**What was happening.** A worker took a job with `BRPOP`, then recorded it in
the processing set with a separate `ZADD`. Between those two commands, the job
existed only in a local variable in one process. It was already gone from the
queue and not yet written anywhere else.

```
BRPOP: job leaves the queue ─── crash here: job is gone ─── ZADD: job is tracked
```

A crash in that window (`kill -9`, an out-of-memory kill, a pod eviction)
destroyed the job with no trace in Redis. The recovery mechanism could not help, because the sweeper
looks for jobs in the processing set, and this job never reached it.

**The fix.** `BLMOVE` pops from the queue and appends to `jobs:inflight` as
one atomic operation. There is no instant at which the job exists in neither
list, so a crash at any point leaves it where the sweeper will find it.

A Lua script would also be atomic, but Lua cannot block, and blocking is what
keeps workers from polling Redis in a loop. `BLMOVE` is atomic and blocking.

**Proof.** A test simulates a worker that dies immediately after `BLMOVE` and
asserts the job is recovered.

### 2. A healthy but slow worker had its job taken and run twice

*Fixed in `453e99c`.*

**What was happening.** Each job's lease was a fixed 5-second deadline set
once at pickup. That deadline covered far more than the work itself: network
round trips to Redis, garbage-collection pauses, scheduler delays. A perfectly
healthy worker could exceed it.

```
t=0.0  worker 2 takes job-42, deadline t+5s
t=5.0  deadline passes - worker 2 is alive, still working
t=5.1  sweeper reclaims job-42 and requeues it
t=5.2  worker 3 starts job-42                      ← executed twice
t=6.0  worker 2 fails it and requeues it again     ← now in the queue twice
```

The first worker called `ZREM` to release its claim but ignored the return
value, so it never noticed the sweeper had already taken the job. The result
was duplicate execution, plus an attempt counter that no longer matched how
many times the job had really run.

**The fix, in two layers.**

- **Heartbeat.** Workers extend their deadline every 5 seconds with
  `ZADD XX`, which only updates an entry that still exists. A deadline now
  means "the owner has stopped checking in", not "this should be done by now".
  `XX` stops a late heartbeat from re-creating a deadline for a job the
  sweeper already took.
- **Ownership check.** `ZREM` returns how many entries it removed. Redis runs
  commands one at a time, so for a given job exactly one caller can get `1`.
  The worker only applies its result if it gets `1`; if it gets `0`, the
  sweeper got there first, and the worker's result is discarded.

The heartbeat makes the race rare, and the ownership check keeps the system
correct when it happens anyway (a long garbage-collection pause, a VM
suspend). `jobqueue_stale_results_discarded_total` counts how often a worker
loses that race.

**Proof.** Two regression tests, each confirmed to fail when its fix is
removed: with the ownership check disabled, `queue holds 2 entries, want 1`;
with the heartbeat disabled, `sweeper requeued a job that was still being
worked on`.

### 3. Unparseable payloads were deleted

*Fixed in `89ea16f`.*

A payload that could not be parsed as JSON was removed from the in-flight list
and discarded, which quietly lost data. It now goes to `jobs:poison`, which is
kept separate from the dead letter queue so that tools replaying the DLQ never
have to handle garbage. If writing to the poison list fails, the payload is
left in flight for the sweeper rather than dropped.

### 4. One hung handler could block shutdown forever

*Fixed in `89ea16f`.*

Shutdown waited on the workers with `wg.Wait()`, which has no timeout. One
handler that never returned kept the process alive until the orchestrator
gave up and sent SIGKILL: exactly the ungraceful crash the sweeper exists to
recover from. The wait is now bounded by `SHUTDOWN_TIMEOUT` (25s), which is
set below Kubernetes' `terminationGracePeriodSeconds` (30s) so the app's own
drain always gets to finish.

### 5. Every Kafka event was silently failing

*Fixed in `a60868c`.*

**What was happening.** On the first end-to-end run, all 317 event publishes
failed with `UNKNOWN_TOPIC_OR_PARTITION`. The Kafka client (franz-go) does not
auto-create topics unless explicitly told to, so the topic never existed.

**Why it took a while to find.** The publish callback counted errors but never
logged them. A completely broken event stream produced no log output at all.

**The fix.** The consumer creates the topic explicitly, with a known partition
count, instead of relying on the broker to auto-create it (many real clusters
disable auto-creation). Publish failures are now logged: the first one, then
every hundredth.

The same run exposed a related bug in the auditor. Its fallback read position
was set to `AtCommitted()`, which does not mean "resume where you left off";
franz-go already does that by default. The setting only applies when a
consumer group has no saved position yet. It is now `AtStart()`, so a new
group reads the whole topic. `--from-start` was also fixed: a group's saved
position always wins over the fallback, so replay now runs under a fresh
group name instead.

### 6. Kubernetes refused to update the workloads

*Fixed in `296c538`.*

Running `kubectl apply -k` over workloads that had been created earlier with
plain `kubectl apply -f` failed with `spec.selector: field is immutable`.
Kustomize's `commonLabels` adds its labels to every selector, and the selector
on a Deployment or StatefulSet cannot change after it is created. Switching to
`labels` with `includeSelectors: false` puts the label on metadata only.

### 7. Identical payloads shared one identity, and jobs were lost

*Fixed in the commit that adds this section.*

**What was happening.** After a crash-recovery test on Kubernetes,
`jobs:inflight` was empty but `jobs:deadlines` still held an entry for
`job-48`. Its deadline had passed nearly three minutes earlier, and nothing
ever removed it.

**Why.** Redis tracks each job by its serialized JSON: the entry in the
in-flight list and the entry in the deadlines ZSET are the raw payload bytes.
The producer generated byte-identical JSON on every run: `job-1` to `job-100`,
all with `attempts: 0`. Running it again while an earlier batch was still in
flight put two identical copies of `job-48` in flight at the same time.

A ZSET cannot hold two identical entries, so the two copies shared one
deadline:

- One worker's `ZREM` released the shared deadline. When the other worker
  finished, its `ZREM` returned `0`, and a perfectly valid result was thrown
  away as if the sweeper had taken it.
- The sweeper only walks `jobs:inflight`, so a deadline whose job had already
  left that list was never looked at again. It stayed in Redis permanently.

**It was worse than a leak.** The retry set `jobs:delayed` is also a ZSET, and
a ZSET quietly merges identical entries. Two copies of `job-48` that both
failed once serialised to the same retry payload, so the second retry
overwrote the first, and one job vanished. It was never retried, never sent
to the dead letter queue, and never counted.

**The fix.**

- The producer gives every job a random `enqueue_id` (128 random bits). Two
  enqueues of the same logical job now always serialise differently, so they
  can never merge in either ZSET. The field is omitted when empty, so payloads
  without it serialise exactly as before: jobs already in the queue, and
  producers that don't know about the field, keep working unchanged.
- The sweeper now removes orphaned deadlines: expired entries whose job is no
  longer in the in-flight list. It only considers entries that expired more
  than one full visibility timeout (15s) ago, and it checks the timestamp and
  removes the entry in one atomic Lua script. A live claim always has a
  timestamp in the future, so it can never be removed, even if a worker picks
  the job up between the sweeper's read and its removal. Expired deadlines
  whose job *is* still in flight are left alone: those belong to crashed
  workers, and the normal recovery path must retry them rather than drop them.

**Proof.** Failing tests were written first, and each failed on the old code
with the exact symptom (`two enqueues of job-48 serialised identically`,
`delayed set holds 1 retries, want 2`). Guard tests check that the cleanup
never touches a live claim, never takes over crash recovery, and survives the
race its Lua script exists for. On the cluster, the same scenario was run
twice: two producer batches launched at the same moment, so copies of each
job ID were in flight together.

| | Old producer | New producer |
|---|---|---|
| Jobs finished (success + buried) | **193 of 200** | **200 of 200** |
| Valid results discarded as stale | 15 | **0** |

**A third cause turned up during that run.** Even with unique IDs, one orphaned
deadline was cleaned up. Tracing it led to a race in ordinary operation: a
worker releases a job in two commands, `ZREM` the deadline and then `LREM` the
list entry. A sweeper that looks in the ~1 ms between them sees a job still in
flight with no deadline. It then "adopts" the job and creates a deadline for a
job that has already finished. This is harmless (the new cleanup removes it
within about 30 seconds), but before this fix those entries piled up forever.
It is also why `jobqueue_deadlines_reaped_total` can show an occasional single
increment on a healthy system. A steady climb is different: it means a
producer is enqueueing identical payloads.

### 8. A Kafka outage would have stalled the entire queue

*Fixed in the commit that adds this section.*

**What was happening.** The design goal was that Kafka is observability, not
the critical path: if the broker is down, jobs keep running and only the audit
trail suffers. The code did not deliver on that.

- Publishing used franz-go's `Produce`, which is asynchronous right up until
  its record buffer is full (10,000 records by default). **At that point it
  blocks** until space frees up.
- There was no delivery timeout, so a record for an unreachable broker is
  retried forever and never frees its space.
- `publish` runs inside the worker's job loop.

So with Kafka down, after roughly 10,000 events (about 3,300 jobs), **every
worker would hang inside `publish` and the queue would stop.** The first sign
of trouble was a monitoring cluster with no Kafka reporting zero publish
errors: events were piling up in memory, not failing.

**The fix.**

- `TryProduce` fails immediately with `ErrMaxBuffered` instead of blocking, so
  a full buffer costs an audit event, never a job.
- `RecordDeliveryTimeout` (30s by default, `KAFKA_DELIVERY_TIMEOUT`) releases
  records that could not be delivered, so a long outage uses a bounded amount
  of memory instead of growing until the process is killed. franz-go rejects
  timeouts under one second, so smaller values are raised to one second
  rather than disabling Kafka.
- The buffer cap is configurable through `KAFKA_MAX_BUFFERED_RECORDS`.

**Proof.** A test points the publisher at a port nothing listens on, with a
five-record buffer, and publishes 50 events. On the old code it hung (`publish
blocked with Kafka unreachable`); now it returns immediately. A second test
checks that undeliverable records leave the buffer at the delivery timeout. On
the cluster, with Kafka scaled to zero and the buffer cut to 20 records (the
old code would have stalled after about 6 jobs), 100 of 100 jobs completed in
14 seconds, and 300 events were dropped and counted. With Kafka back, all 100
jobs reached Postgres again.

### Every fix is mutation-tested

A passing test proves nothing unless it would fail on broken code. So each fix
was deliberately removed, one at a time, to confirm a test catches it:

| Fix removed | Caught by |
|---|---|
| Ownership check (`ZREM` result ignored) | `TestSettleDiscardsResultAfterSweeperReclaim` |
| Heartbeat | `TestHeartbeatKeepsSlowWorkerFromBeingReclaimed` |
| Orphan cleanup | `TestOrphanDeadlineIsReaped` |
| Atomic check in the cleanup script | `TestReapIsAtomicAgainstReclaim` |
| Cleanup's skip of in-flight jobs | `TestReaperLeavesInFlightJobsToReclaim` |
| Cleanup's safety margin | `TestReaperLeavesLiveClaimsAlone` |
| Unique enqueue IDs (consumer side) | `TestDuplicateEnqueuesAreTrackedIndependently` |
| Unique enqueue IDs (producer side) | `TestEveryEnqueueIsUnique` |
| Non-blocking publish | `TestPublishNeverBlocksWhenKafkaIsDown` |
| Delivery timeout | `TestUndeliverableEventsAreDroppedNotHeldForever` |

## Lessons from running it for real

Several of the most useful findings came from running the system on a real
cluster rather than from unit tests, which use an in-process Redis
(miniredis) and a fake Kafka address:

- **`kubectl delete pod --force --grace-period=0` is not a crash.** The kubelet
  still sends SIGTERM, and the pod shuts down cleanly. A real crash test needs
  a real SIGKILL (see [k8s/README.md](k8s/README.md)). Measured result: 3 jobs
  in flight at the kill were recovered exactly 15 seconds later (one
  visibility timeout) and completed on the other pod: 99 succeeded + 1 buried
  = 100, with no job succeeding twice.
- **Reusing an image tag means Kubernetes may run old code.** Docker Desktop's
  Kubernetes node keeps its own copy of each image. Rebuilding `jobqueue:dev`
  on the host did not replace the node's copy, so the cluster quietly kept
  running a day-old image. Build with a unique tag every time; tagging by git
  commit hash is the usual answer.
- **A local Postgres can silently take the container's port.** A Postgres
  service installed on the host already owned 5432, so connections to the
  container's published port reached the wrong database. The error was an
  authentication failure, which looks exactly like a wrong password. The
  compose file now publishes Postgres on 5433.

## Known limitations

- **At-least-once, not exactly-once.** The ownership check prevents a job being
  requeued twice. It does not prevent it running twice when a worker dies
  after finishing the work but before recording it. Handlers need to be
  idempotent, keyed on the job ID.
- **Releasing a job takes two Redis commands.** This leaves the small race
  described in bug 7. The cleanup handles it, but combining both steps into
  one atomic script would remove it at the source.
- **A pod that is shutting down can still pick up new jobs.** During a rollout,
  pods that were stopping took two jobs off the queue seconds after the new
  pods came up. Both completed, but ideally a stopping pod takes nothing new.
  The cause has not been confirmed.
- **Everything stateful is a single node.** One Redis, one Kafka broker (with a
  replication factor of 1), and one Postgres, so each is a single point of
  failure.
- **Identity is still the payload.** `enqueue_id` makes payloads unique, but
  Redis still identifies a job by its full serialized bytes. Using the job ID
  as the ZSET member and keeping job state in a Redis hash would be sturdier.
