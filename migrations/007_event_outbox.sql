-- Events waiting to be delivered (ADR 0020): each is written in the same
-- transaction as the event log row and the state change it announces, and
-- the delivery task marks it delivered once the sink took it.
CREATE TABLE IF NOT EXISTS office_outbox (
    outbox_id bigserial PRIMARY KEY,
    organization_id text NOT NULL,
    event_id text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error text,
    delivered_at timestamptz,
    UNIQUE (organization_id, event_id)
);

CREATE INDEX IF NOT EXISTS office_outbox_pending_idx
    ON office_outbox (organization_id, outbox_id) WHERE delivered_at IS NULL;

-- Replay by organization, in the order events were taken.
CREATE INDEX IF NOT EXISTS office_events_org_ingest_idx
    ON office_events (organization_id, ingest_id);
