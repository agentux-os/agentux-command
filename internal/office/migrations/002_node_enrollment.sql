CREATE TABLE IF NOT EXISTS command_instance (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    instance_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS enrollment_tokens (
    token_hash text PRIMARY KEY,
    organization_id text NOT NULL,
    node_name text NOT NULL DEFAULT '',
    require_approval boolean NOT NULL DEFAULT false,
    expires_at timestamptz NOT NULL,
    created_by text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    used_at timestamptz,
    used_by_node text
);

CREATE TABLE IF NOT EXISTS node_credentials (
    node_id text PRIMARY KEY,
    organization_id text NOT NULL,
    secret_hash text NOT NULL UNIQUE,
    hostname text NOT NULL DEFAULT '',
    approved boolean NOT NULL,
    enrolled_at timestamptz NOT NULL DEFAULT now(),
    approved_at timestamptz,
    revoked_at timestamptz
);

CREATE INDEX IF NOT EXISTS node_credentials_org_idx ON node_credentials (organization_id)
