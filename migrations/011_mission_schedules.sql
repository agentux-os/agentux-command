CREATE TABLE IF NOT EXISTS mission_schedules (
    schedule_id text PRIMARY KEY,
    organization_id text NOT NULL,
    status text NOT NULL,
    next_run_at timestamptz,
    revision bigint NOT NULL,
    doc jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS mission_schedules_due_idx
    ON mission_schedules (next_run_at, schedule_id)
    WHERE status = 'active' AND next_run_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS mission_schedules_org_next_idx
    ON mission_schedules (organization_id, next_run_at);

CREATE TABLE IF NOT EXISTS mission_schedule_occurrences (
    schedule_id text NOT NULL REFERENCES mission_schedules(schedule_id),
    organization_id text NOT NULL,
    scheduled_for timestamptz NOT NULL,
    run_id text NOT NULL UNIQUE,
    status text NOT NULL,
    error text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL,
    claimed_at timestamptz,
    doc jsonb NOT NULL,
    PRIMARY KEY (schedule_id, scheduled_for)
);

CREATE INDEX IF NOT EXISTS mission_schedule_occurrences_org_schedule_idx
    ON mission_schedule_occurrences (organization_id, schedule_id, scheduled_for DESC);
