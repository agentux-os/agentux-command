-- Current-state checkpoint projection inventories stay separate from the
-- append-only Office event log. All keys include authenticated tenant scope.
CREATE TABLE IF NOT EXISTS command_node_checkpoint_projections (
    organization_id text NOT NULL,
    node_id text NOT NULL,
    projection_type text NOT NULL CHECK (projection_type IN ('approval','artifact','command','gate','harness','limit','node','node_alert','run','session','task')),
    entry_id text NOT NULL,
    state jsonb,
    removed_at_sequence bigint,
    run_id text,
    checkpoint_id text NOT NULL,
    PRIMARY KEY (organization_id, node_id, projection_type, entry_id),
    CHECK ((state IS NOT NULL AND removed_at_sequence IS NULL) OR (state IS NULL AND removed_at_sequence IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS command_node_checkpoints (
    organization_id text NOT NULL,
    node_id text NOT NULL,
    checkpoint_id text NOT NULL,
    snapshot_id text NOT NULL,
    requested_after_sequence bigint NOT NULL,
    high_water_sequence bigint NOT NULL,
    content_sha256 text NOT NULL CHECK (length(content_sha256) = 64),
    applied_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, node_id),
    UNIQUE (organization_id, node_id, checkpoint_id),
    CHECK (requested_after_sequence >= 0 AND high_water_sequence >= requested_after_sequence)
);

CREATE TABLE IF NOT EXISTS command_node_history_gaps (
    organization_id text NOT NULL,
    node_id text NOT NULL,
    checkpoint_id text NOT NULL,
    from_sequence bigint NOT NULL CHECK (from_sequence > 0),
    through_sequence bigint NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, node_id, checkpoint_id),
    CHECK (through_sequence >= from_sequence)
);
