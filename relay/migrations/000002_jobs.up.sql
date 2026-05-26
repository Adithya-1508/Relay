-- 000002_jobs.up.sql
-- Pipelines + jobs + job_events. Phase 3 stops short of DAG dependencies — a
-- pipeline is currently just a named container for jobs.

CREATE TABLE pipelines (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    name         TEXT NOT NULL,
    slug         TEXT NOT NULL,
    description  TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (workspace_id, slug)
);

CREATE INDEX idx_pipelines_workspace ON pipelines (workspace_id);

-- jobs.state values are intentionally lower-case kebab-friendly slugs that
-- match the Go JobState constants 1:1.
CREATE TABLE jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    workspace_id    UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    idempotency_key TEXT NULL,
    kind            TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    state           TEXT NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending','running','succeeded','failed','dead','cancelled')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 5,
    version         BIGINT NOT NULL DEFAULT 1,
    scheduled_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ NULL,
    finished_at     TIMESTAMPTZ NULL,
    last_error      TEXT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_jobs_pipeline ON jobs (pipeline_id);
CREATE INDEX idx_jobs_workspace_state ON jobs (workspace_id, state);
-- keyset pagination on (created_at DESC, id DESC).
CREATE INDEX idx_jobs_workspace_created ON jobs (workspace_id, created_at DESC, id DESC);

-- Idempotency: same (pipeline_id, idempotency_key) must collide. Partial index
-- so jobs with NULL key (caller did not request idempotency) are not deduped.
CREATE UNIQUE INDEX uq_jobs_idempotency
    ON jobs (pipeline_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- job_events is append-only. Each row captures one state transition or
-- informational event so the audit history is fully reconstructible.
CREATE TABLE job_events (
    id         BIGSERIAL PRIMARY KEY,
    job_id     UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    event      TEXT NOT NULL,
    from_state TEXT NULL,
    to_state   TEXT NULL,
    message    TEXT NOT NULL DEFAULT '',
    metadata   JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_job_events_job_created ON job_events (job_id, created_at);
