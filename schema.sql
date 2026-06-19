-- Schema for our persistent job queue
CREATE TABLE IF NOT EXISTS jobs (
    id VARCHAR(255) PRIMARY KEY,
    payload TEXT NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending', -- pending, running, succeeded, failed_permanently
    attempts INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Index on status + created_at to make polling fast
CREATE INDEX IF NOT EXISTS idx_jobs_status_created_at ON jobs(status, created_at) WHERE status = 'pending';
