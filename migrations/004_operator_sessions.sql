CREATE TABLE IF NOT EXISTS operator_sessions (
    session_hash text PRIMARY KEY,
    subject text NOT NULL,
    principal jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS operator_sessions_expiry_idx ON operator_sessions (expires_at)
