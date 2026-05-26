-- 000003_dag.up.sql
-- DAG support: jobs can depend on other jobs. A job is 'blocked' until every
-- dependency reaches 'succeeded'; once the last one succeeds the scheduler
-- transitions it to 'pending' and enqueues the Asynq task.

-- 1. Widen the state CHECK constraint to include 'blocked'.
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_state_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_state_check
    CHECK (state IN ('blocked','pending','running','succeeded','failed','dead','cancelled'));

-- 2. job_dependencies links a dependent job to a job it waits on. Composite
-- PK prevents duplicate rows; the second index makes dependent-lookup cheap.
CREATE TABLE job_dependencies (
    job_id            UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    depends_on_job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (job_id, depends_on_job_id),
    CHECK (job_id <> depends_on_job_id)
);

CREATE INDEX idx_job_deps_depends_on ON job_dependencies (depends_on_job_id);
