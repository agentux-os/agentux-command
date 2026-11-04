-- Migration 013 seeded legacy cursors from high-water marks. Neither source
-- proves that every earlier event exists, so do not acknowledge beyond zero
-- until the replay ledger has verified a contiguous prefix.
UPDATE command_node_event_cursors cursor
SET contiguous_sequence = 0,
    updated_at = now()
WHERE NOT EXISTS (
    SELECT 1
    FROM command_node_events event
    WHERE event.organization_id = cursor.organization_id
      AND event.node_id = cursor.node_id
);
