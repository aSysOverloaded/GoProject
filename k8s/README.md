# Running the job queue on Kubernetes

## Why each object is the kind it is

| Object | Kind | Reason |
|---|---|---|
| consumer | Deployment | Stateless, horizontally scalable, replaceable |
| producer | **Job** | Runs to completion. A Deployment would restart it forever |
| redis, kafka, postgres | **StatefulSet** | Need stable identity and a volume that survives rescheduling |
| auditor | Deployment | Stateless, but replicas capped by Kafka partition count |

## The numbers that have to line up

```
SHUTDOWN_TIMEOUT (25s)  <  terminationGracePeriodSeconds (30s)
HEARTBEAT_INTERVAL (5s) <  VISIBILITY_TIMEOUT (15s), by at least 3x
```

**The first one is the one people get wrong.** Kubernetes sends SIGTERM,
waits `terminationGracePeriodSeconds`, then SIGKILLs. If the app's own
shutdown deadline is the longer of the two, it never gets to finish: the
kernel kills the process mid-job and the sweeper has to clean up what should
have been a clean drain. Every rollout becomes an ungraceful crash, on
purpose, several times per deploy.

The second is enforced at startup — `config.go` warns if the heartbeat is too
close to the visibility timeout, which is the exact misconfiguration that
reintroduces the race the heartbeat was added to fix.

## Deploy

```bash
# 1. Build the image. With Docker Desktop's Kubernetes the cluster shares
#    the local daemon, so no registry and no push is needed.
docker build -t jobqueue:dev .

# 2. Apply everything
kubectl apply -k k8s/

# 3. Watch it come up
kubectl -n jobqueue get pods -w

# 4. Enqueue a batch
kubectl -n jobqueue delete job jobqueue-producer --ignore-not-found
kubectl apply -f k8s/04-producer-job.yaml

# 5. Watch it work
kubectl -n jobqueue logs -l app.kubernetes.io/name=jobqueue-consumer -f --tail=50
```

Metrics and the audit trail:

```bash
kubectl -n jobqueue port-forward svc/jobqueue-consumer 2112:2112
curl -s localhost:2112/metrics | grep '^jobqueue_'

kubectl -n jobqueue exec -it postgres-0 -- \
  psql -U postgres -d jobqueue -c 'SELECT event, count(*) FROM job_events GROUP BY event;'
```

## Things worth demonstrating

**Pod eviction is the real version of your crash test.**

```bash
kubectl -n jobqueue delete pod -l app.kubernetes.io/name=jobqueue-consumer --force --grace-period=0
```

`--grace-period=0` skips SIGTERM entirely, so the pod dies mid-job with no
chance to drain. The surviving consumer's sweeper reclaims the orphans after
the visibility timeout. Watch `jobqueue_jobs_recovered_total` move and
`jobqueue_stale_results_discarded_total` stay at zero.

Then contrast with a **graceful** delete (no flags): SIGTERM, clean drain,
`jobs_recovered_total` does not move at all. Those two commands are the whole
reliability story in about thirty seconds.

**A rolling restart should recover nothing:**

```bash
kubectl -n jobqueue rollout restart deployment/jobqueue-consumer
kubectl -n jobqueue rollout status deployment/jobqueue-consumer
```

If `jobs_recovered_total` climbs during a rollout, the grace period is too
short for the drain.

## Optional pieces

| File | Requires | Note |
|---|---|---|
| `05-scaling.yaml` HPA | prometheus-adapter or KEDA | Without one the HPA cannot fetch `jobqueue_depth` and simply does not scale. KEDA's Redis list scaler reads `LLEN jobs:queue` directly and needs no adapter. |
| `06-monitoring.yaml` | Prometheus Operator | ServiceMonitor and PrometheusRule are CRDs. Excluded from `kustomization.yaml`; apply once the operator is installed. |

## Honest limitations

1. **Single replica of everything stateful.** Redis, Kafka and Postgres each
   run one pod. Kafka with replication factor 1 means a broker loss is data
   loss — Kafka's durability comes from `acks=all` **plus** `RF>=3` **plus**
   `min.insync.replicas`, and with one broker `acks=all` is only as safe as
   `acks=1`.
2. **Hand-rolled Kafka.** Production Kafka on Kubernetes means the Strimzi
   operator, which handles broker identity, rolling upgrades, rebalancing
   and cert rotation.
3. **Redis has no replica.** Still the system's single point of failure.
   Sentinel or Cluster is the fix, and failover is not free either: Redis
   replication is asynchronous, so a failover can lose recent writes.
4. **Secrets are plain.** A Kubernetes Secret is base64, not encryption.
   Real clusters use Sealed Secrets, External Secrets Operator, or the cloud
   provider's secret manager.
5. **No NetworkPolicy.** Every pod can reach every other pod.
6. **No resource limits on CPU for the consumer**, deliberately: CFS
   throttling causes latency spikes, and a throttled worker is exactly the
   "slow but healthy" case that used to trigger false sweeper reclaims.
