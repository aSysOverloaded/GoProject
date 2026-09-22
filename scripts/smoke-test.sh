#!/usr/bin/env bash
# Conservation smoke test against a real Redis.
#
# Runs the real consumer and producer, then checks the property the whole
# project is about: every one of the jobs enqueued finished exactly once,
# either succeeded or buried in the DLQ - none lost, none run to completion
# twice.
#
# CI runs this; you can run it locally too, against any empty Redis:
#   REDIS_URL=redis://localhost:6379/0 bash scripts/smoke-test.sh
set -euo pipefail

: "${REDIS_URL:=redis://localhost:6379/0}"
export REDIS_URL
JOBS=100 # the producer enqueues 100
TIMEOUT=${SMOKE_TIMEOUT:-120}

BIN=$(mktemp -d)
EXE=""
[ "$(go env GOOS)" = "windows" ] && EXE=".exe"
go build -o "$BIN/consumer$EXE" ./cmd/consumer
go build -o "$BIN/producer$EXE" ./cmd/producer

export WORKER_COUNT=${WORKER_COUNT:-4}
export METRICS_ADDR=${METRICS_ADDR:-:2112}
# Small backoffs and quick sweeps so the run takes seconds, not minutes.
export BASE_RETRY_DELAY=50ms MAX_RETRY_DELAY=200ms PROMOTE_INTERVAL=100ms SWEEP_INTERVAL=200ms
# The depth gauges are sampled on a timer. Keep it short so the final drain
# check reflects Redis rather than a sample from seconds ago.
export DEPTH_POLL_INTERVAL=250ms
PORT=${METRICS_ADDR##*:}

"$BIN/consumer$EXE" >"$BIN/consumer.log" 2>&1 &
CONSUMER=$!
trap 'kill "$CONSUMER" 2>/dev/null || true' EXIT

# On any failure, say whether the consumer is still alive and show its log,
# so a failure can be diagnosed from the output alone.
dump() {
  if kill -0 "$CONSUMER" 2>/dev/null; then echo "(consumer still running)"; else echo "(consumer process has exited)"; fi
  echo "--- last 25 lines of consumer log ---"
  tail -25 "$BIN/consumer.log" | sed 's/\x1b\[[0-9;]*m//g'
}

# Sum every sample of a metric whose line matches the given extended regex.
metric() {
  curl -sf "localhost:$PORT/metrics" | grep -E "^$1" | awk '{s += $NF} END {print s + 0}'
}

for _ in $(seq 1 30); do
  curl -sf "localhost:$PORT/healthz" >/dev/null && break
  sleep 1
done
curl -sf "localhost:$PORT/healthz" >/dev/null || { echo "FAIL: consumer never became healthy"; dump; exit 1; }

"$BIN/producer$EXE" >/dev/null

# Wait on COUNTERS, not gauges. Counters are updated the instant a job
# finishes; the depth gauges are samples. An earlier version of this test
# waited for the gauges to read "drained" and was fooled by the sample taken
# at startup, before any job existed - it exited on its first check while 97
# jobs were still queued or in flight, and failed every time.
finished() {
  local ok buried
  ok=$(metric 'jobqueue_jobs_processed_total\{result="success"\}')
  buried=$(metric 'jobqueue_jobs_dlq_total\{')
  echo $((ok + buried))
}

deadline=$((SECONDS + TIMEOUT))
until [ "$(finished)" -ge "$JOBS" ]; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "FAIL: only $(finished) of $JOBS jobs finished within ${TIMEOUT}s"
    dump
    exit 1
  fi
  sleep 1
done

# Reaching the target is not the whole check: a job finishing twice would
# push the total past it. Give any straggler time to show up, and let the
# gauges take a fresh sample.
sleep 5

total=$(finished)
ok=$(metric 'jobqueue_jobs_processed_total\{result="success"\}')
buried=$(metric 'jobqueue_jobs_dlq_total\{')
stale=$(metric 'jobqueue_stale_results_discarded_total ')
poisoned=$(metric 'jobqueue_jobs_poisoned_total ')
residue=$(metric 'jobqueue_depth\{structure="(pending|inflight|delayed|claimed)"\}')

echo "succeeded=$ok buried=$buried total=$total stale=$stale poisoned=$poisoned residue=$residue"

fail=0
[ "$total" -eq "$JOBS" ] || { echo "FAIL: $total jobs finished, want exactly $JOBS - a job was lost or finished twice"; fail=1; }
[ "$stale" -eq 0 ] || { echo "FAIL: $stale valid results discarded as stale, want 0"; fail=1; }
[ "$poisoned" -eq 0 ] || { echo "FAIL: $poisoned payloads quarantined, want 0"; fail=1; }
[ "$residue" -eq 0 ] || { echo "FAIL: $residue jobs left queued, in flight, delayed or claimed"; fail=1; }
[ "$fail" -eq 0 ] || { dump; exit 1; }

echo "PASS: $JOBS jobs, each finished exactly once; no stale results; queue drained clean"
