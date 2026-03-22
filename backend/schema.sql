-- IJM database schema
-- Executed on startup via CREATE TABLE IF NOT EXISTS (idempotent).

CREATE TABLE IF NOT EXISTS jobs (
    id                 TEXT PRIMARY KEY,
    job_id             TEXT NOT NULL,
    image              TEXT NOT NULL,
    command            JSONB NOT NULL,
    script_path        TEXT,
    directory_to_mount TEXT,
    status             TEXT NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL,
    container_name     TEXT,
    exit_code          INT,
    progress           TEXT,
    priority           INT DEFAULT 3,
    deadline           TIMESTAMPTZ,
    batch_size         INT,
    epochs_total       INT,
    profiling_epochs_no INT,
    assigned_node      TEXT,
    assigned_gpu_config JSONB,
    is_profiling_run   BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_jobs_status     ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_job_id     ON jobs(job_id);

CREATE TABLE IF NOT EXISTS profiling_results (
    id               TEXT PRIMARY KEY,
    job_id           TEXT NOT NULL,
    gpu_config       JSONB NOT NULL,
    node_id          TEXT NOT NULL,
    duration_seconds FLOAT,
    created_at       TIMESTAMPTZ NOT NULL
);

CREATE INDEX  IF NOT EXISTS idx_profiling_results_job_id ON profiling_results(job_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_profiling_job_config ON profiling_results(job_id, gpu_config);
