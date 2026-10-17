CREATE TABLE IF NOT EXISTS office_events (
    ingest_id bigserial NOT NULL,
    organization_id text NOT NULL,
    event_id text NOT NULL,
    node_id text NOT NULL DEFAULT '',
    sequence bigint NOT NULL CHECK (sequence > 0),
    payload jsonb NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, event_id),
    UNIQUE (organization_id, node_id, sequence)
) PARTITION BY HASH (organization_id);

CREATE TABLE IF NOT EXISTS office_events_p0 PARTITION OF office_events FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE IF NOT EXISTS office_events_p1 PARTITION OF office_events FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE IF NOT EXISTS office_events_p2 PARTITION OF office_events FOR VALUES WITH (MODULUS 8, REMAINDER 2);
CREATE TABLE IF NOT EXISTS office_events_p3 PARTITION OF office_events FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE IF NOT EXISTS office_events_p4 PARTITION OF office_events FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE IF NOT EXISTS office_events_p5 PARTITION OF office_events FOR VALUES WITH (MODULUS 8, REMAINDER 5);
CREATE TABLE IF NOT EXISTS office_events_p6 PARTITION OF office_events FOR VALUES WITH (MODULUS 8, REMAINDER 6);
CREATE TABLE IF NOT EXISTS office_events_p7 PARTITION OF office_events FOR VALUES WITH (MODULUS 8, REMAINDER 7);

CREATE INDEX IF NOT EXISTS office_events_org_received_idx
    ON office_events (organization_id, received_at, event_id);

CREATE TABLE IF NOT EXISTS office_event_cursors (
    organization_id text NOT NULL,
    node_id text NOT NULL DEFAULT '',
    sequence bigint NOT NULL CHECK (sequence > 0),
    last_event_id text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, node_id)
);

CREATE TABLE IF NOT EXISTS office_projections (
    organization_id text PRIMARY KEY,
    schema_version text NOT NULL,
    definition_version text NOT NULL,
    projected_at timestamptz NOT NULL,
    source text NOT NULL,
    freshness text NOT NULL,
    last_event_id text NOT NULL,
    last_node_id text NOT NULL DEFAULT '',
    last_sequence bigint NOT NULL DEFAULT 0,
    payload jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS office_approval_state (
    organization_id text NOT NULL,
    approval_id text NOT NULL,
    status text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    decision jsonb,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, approval_id)
);

CREATE TABLE IF NOT EXISTS office_approval_idempotency (
    organization_id text NOT NULL,
    approval_id text NOT NULL,
    idempotency_key text NOT NULL,
    response jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, approval_id, idempotency_key)
);
