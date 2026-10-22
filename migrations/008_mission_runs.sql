-- Missions run as workflows (ADR 0020): one row per run, its state as a
-- document, with the columns the engine looks runs up by. Every transition
-- is an UPDATE guarded by the revision it read.
CREATE TABLE IF NOT EXISTS mission_runs (
    run_id text PRIMARY KEY,
    organization_id text NOT NULL,
    status text NOT NULL,
    run_at timestamptz,
    revision bigint NOT NULL,
    doc jsonb NOT NULL,
    created_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS mission_runs_org_created_idx
    ON mission_runs (organization_id, created_at DESC);
CREATE INDEX IF NOT EXISTS mission_runs_active_idx
    ON mission_runs (organization_id) WHERE status IN ('running', 'waiting');
