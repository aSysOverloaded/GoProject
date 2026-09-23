-- Audit store for the job event stream.
--
-- Kafka retains events for a bounded window and is optimised for sequential
-- reads. Postgres is where those events go to be queried: "what happened to
-- job-42", "which job types fail most", "show me everything buried today".
-- Two different access patterns, two different stores - which is the reason
-- the auditor exists as a separate consumer rather than a query against
-- Kafka.

CREATE TABLE IF NOT EXISTS job_events (
    -- The de-duplication key. The auditor commits its Kafka offset only
    -- after a successful write, so a crash between the write and the commit
    -- replays the event. ON CONFLICT DO NOTHING against this key turns that
    -- at-least-once delivery into an effectively-once write.
    event_id     TEXT PRIMARY KEY,

    job_id       TEXT        NOT NULL,
    job_type     TEXT        NOT NULL DEFAULT '',
    event        TEXT        NOT NULL,
    attempt      INTEGER     NOT NULL DEFAULT 0,

    duration_ms  BIGINT,
    error        TEXT,
    reason       TEXT,

    worker       TEXT,
    host         TEXT,

    occurred_at  TIMESTAMPTZ NOT NULL,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Kafka coordinates, kept so a record can be traced back to its source
    -- partition and offset when reconciling gaps.
    kafka_partition INTEGER,
    kafka_offset    BIGINT
);

-- "Show me the full history of one job" - the most common audit query.
CREATE INDEX IF NOT EXISTS job_events_job_id_idx
    ON job_events (job_id, occurred_at);

-- "What has been failing in the last hour", per type.
CREATE INDEX IF NOT EXISTS job_events_event_time_idx
    ON job_events (event, occurred_at DESC);

CREATE INDEX IF NOT EXISTS job_events_type_idx
    ON job_events (job_type, event);

-- Convenience view: the current state of every job, derived from its most
-- recent event. This is the read model; job_events is the log it is
-- projected from.
--
-- Ordered by Kafka offset, NOT by occurred_at. occurred_at is stamped by
-- whichever process published the event, and a job's events routinely come
-- from different processes - the worker publishes "failed", the sweeper
-- publishes "buried" - which on Kubernetes means different pods with
-- different clocks. A few milliseconds of skew was enough for this view to
-- report a stale event as the current one.
--
-- Offsets are authoritative here because records are keyed by job ID: all of
-- one job's events land in a single partition, and within a partition
-- offsets increase in the order the broker accepted them. (Across partitions
-- offsets are not comparable, which is why the key matters.) occurred_at
-- remains the tie-break for rows written without Kafka coordinates.
CREATE OR REPLACE VIEW job_current_state AS
SELECT DISTINCT ON (job_id)
    job_id,
    job_type,
    event    AS last_event,
    attempt,
    error,
    reason,
    worker,
    occurred_at
FROM job_events
ORDER BY job_id, kafka_offset DESC NULLS LAST, occurred_at DESC, recorded_at DESC;
