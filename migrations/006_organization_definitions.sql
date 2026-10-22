CREATE TABLE IF NOT EXISTS organization_definitions (
    organization_id text NOT NULL,
    definition_version bigint NOT NULL CHECK (definition_version > 0),
    status text NOT NULL CHECK (status IN ('draft', 'published')),
    document jsonb NOT NULL,
    problems jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_by jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_by jsonb,
    published_at timestamptz,
    PRIMARY KEY (organization_id, definition_version)
);

CREATE TABLE IF NOT EXISTS organization_audit (
    audit_id bigserial PRIMARY KEY,
    organization_id text NOT NULL,
    action text NOT NULL,
    actor jsonb NOT NULL,
    definition_version bigint,
    occurred_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS organization_audit_org_idx ON organization_audit (organization_id, audit_id)
