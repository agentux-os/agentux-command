-- The accepted-capability row is overwritten on each hello, so its absence
-- cannot prove that no replay ACK was sent in an earlier session. Preserve
-- every preexisting cursor conservatively and record the ledger-proven prefix
-- separately; lowering an ambiguous cursor may request history Core pruned.
ALTER TABLE command_node_event_cursors
    ADD COLUMN IF NOT EXISTS verified_sequence bigint NOT NULL DEFAULT 0
    CHECK (verified_sequence >= 0);

ALTER TABLE command_node_event_cursors
    ADD COLUMN IF NOT EXISTS history_gap_through_sequence bigint
    CHECK (history_gap_through_sequence IS NULL OR history_gap_through_sequence >= 0);

WITH ledger_prefix AS (
    SELECT cursor.organization_id,
           cursor.node_id,
           COALESCE((
               SELECT first_gap.ordinal - 1
               FROM (
                   SELECT event.sequence,
                          row_number() OVER (ORDER BY event.sequence) AS ordinal
                   FROM command_node_events event
                   WHERE event.organization_id = cursor.organization_id
                     AND event.node_id = cursor.node_id
               ) first_gap
               WHERE first_gap.sequence <> first_gap.ordinal
               ORDER BY first_gap.ordinal
               LIMIT 1
           ), (
               SELECT COALESCE(MAX(event.sequence), 0)
               FROM command_node_events event
               WHERE event.organization_id = cursor.organization_id
                 AND event.node_id = cursor.node_id
           )) AS verified_prefix,
           cursor.contiguous_sequence AS old_cursor
    FROM command_node_event_cursors cursor
)
UPDATE command_node_event_cursors cursor
SET history_gap_through_sequence = CASE
        WHEN prefix.old_cursor > prefix.verified_prefix THEN prefix.old_cursor
        ELSE NULL
    END,
    verified_sequence = prefix.verified_prefix,
    updated_at = now()
FROM ledger_prefix prefix
WHERE cursor.organization_id = prefix.organization_id
  AND cursor.node_id = prefix.node_id;
