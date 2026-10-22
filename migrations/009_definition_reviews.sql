-- Drafts carry a revision (optimistic concurrency for editors), their last
-- editor, and the reviews of their revisions.
ALTER TABLE organization_definitions
    ADD COLUMN IF NOT EXISTS revision bigint NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS updated_by jsonb,
    ADD COLUMN IF NOT EXISTS reviews jsonb NOT NULL DEFAULT '[]'::jsonb;
