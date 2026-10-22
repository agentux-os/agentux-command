CREATE TABLE IF NOT EXISTS service_tokens (
    token_id text PRIMARY KEY,
    token_hash text NOT NULL UNIQUE,
    name text NOT NULL,
    organization_id text NOT NULL,
    capabilities text[] NOT NULL,
    created_by text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz,
    revoked_at timestamptz
);

CREATE INDEX IF NOT EXISTS service_tokens_org_idx ON service_tokens (organization_id)
