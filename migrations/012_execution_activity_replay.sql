-- Run-scoped typed activity replay uses the durable Office event log. Keep a
-- partial expression index for these bounded AUXP records. Other event kinds
-- retain their existing access paths.
CREATE INDEX IF NOT EXISTS office_events_activity_run_idx
    ON office_events (organization_id, (payload->>'runId'), ingest_id)
    WHERE payload->>'eventType' = 'execution.activity';
