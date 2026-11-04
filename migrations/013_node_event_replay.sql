-- Durable receiver ledger for AUXP node notifications. The cursor is the
-- highest contiguous sequence; old deployments seed it from their existing
-- durable node and Office cursors before replay is negotiated.
CREATE TABLE IF NOT EXISTS command_node_event_cursors (
    organization_id text NOT NULL,
    node_id text NOT NULL,
    contiguous_sequence bigint NOT NULL DEFAULT 0 CHECK (contiguous_sequence >= 0),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, node_id)
);

CREATE TABLE IF NOT EXISTS command_node_capabilities (
    organization_id text NOT NULL,
    node_id text NOT NULL,
    accepted_capabilities jsonb NOT NULL DEFAULT '[]'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, node_id)
);

CREATE TABLE IF NOT EXISTS command_node_events (
    organization_id text NOT NULL,
    node_id text NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    event_id text NOT NULL,
    method text NOT NULL,
    payload_sha256 text NOT NULL CHECK (length(payload_sha256) = 64),
    received_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, node_id, sequence),
    UNIQUE (organization_id, node_id, event_id)
);

CREATE INDEX IF NOT EXISTS command_node_events_replay_idx
    ON command_node_events (organization_id, node_id, sequence);

INSERT INTO command_node_event_cursors (organization_id, node_id, contiguous_sequence)
SELECT ns.organization_id, ns.node_id,
       GREATEST(
           CASE WHEN ns.payload->>'lastSequence' ~ '^[0-9]+$'
                THEN (ns.payload->>'lastSequence')::bigint ELSE 0 END,
           COALESCE(oec.sequence, 0)
       )
FROM node_state ns
LEFT JOIN office_event_cursors oec
  ON oec.organization_id = ns.organization_id AND oec.node_id = ns.node_id
ON CONFLICT (organization_id, node_id) DO UPDATE
SET contiguous_sequence = GREATEST(command_node_event_cursors.contiguous_sequence, EXCLUDED.contiguous_sequence),
    updated_at = now();

INSERT INTO command_node_event_cursors (organization_id, node_id, contiguous_sequence)
SELECT organization_id, node_id, sequence FROM office_event_cursors
ON CONFLICT (organization_id, node_id) DO UPDATE
SET contiguous_sequence = GREATEST(command_node_event_cursors.contiguous_sequence, EXCLUDED.contiguous_sequence),
    updated_at = now();
