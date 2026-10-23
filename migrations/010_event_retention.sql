-- Event retention (opt-in): an organization's projection as of the last
-- pruned event, so it can still be rebuilt and verified, and the archive
-- files pruned events went to.
CREATE TABLE IF NOT EXISTS office_projection_checkpoints (
    organization_id text PRIMARY KEY,
    last_ingest_id bigint NOT NULL,
    projection jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS office_event_archives (
    archive_id bigserial PRIMARY KEY,
    organization_id text NOT NULL,
    from_ingest_id bigint NOT NULL,
    to_ingest_id bigint NOT NULL,
    events bigint NOT NULL,
    path text NOT NULL,
    digest text NOT NULL,
    archived_at timestamptz NOT NULL DEFAULT now()
);
