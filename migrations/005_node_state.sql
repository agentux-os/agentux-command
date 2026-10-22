CREATE TABLE IF NOT EXISTS node_state (
    node_id text PRIMARY KEY,
    organization_id text NOT NULL,
    payload jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS node_state_org_idx ON node_state (organization_id)
