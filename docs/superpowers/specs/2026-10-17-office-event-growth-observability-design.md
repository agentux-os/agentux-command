# Office Event Growth Observability and Partition Policy

## Status

Approved for implementation on 2026-10-17. No retention duration or deletion authority has been established.

## Problem

Command stores an append-only Office event log in PostgreSQL. The current schema hash-partitions events by `organization_id` and indexes `received_at`; projections are verified by replaying the stored log. There is no approved retention period, archival procedure, capacity alerting system, or production workload baseline. Introducing automatic deletion or time partitions now could compromise replay and auditability while adding migration and operations complexity.

Temporal.io is present in the deployment bundle as a prospective workflow engine, but the Command service does not currently integrate with it. Temporal workflow timers are not needed to inspect PostgreSQL storage or provision database partitions.

## Goals

- Make event-log growth and partition distribution visible to operators using metadata only.
- Preserve all events and existing organization-scoped deduplication/replay behavior.
- Establish a repeatable way to collect a baseline and decide later whether time partitioning or archival is justified.
- Avoid binding this database-maintenance task to Temporal.io or introducing a metrics backend before the project selects one.

## Non-goals

- Automatic event deletion, pruning, or archival.
- Changing the existing event table partitioning, unique identities, migrations, or projection replay algorithm.
- Establishing a legal, contractual, or product retention period on behalf of operators.
- Adding a Prometheus server, alert manager, or Temporal workflow.
- Exposing per-organization storage information through the unauthenticated HTTP health endpoint.

## Proposed first increment

Add an operator-run, read-only PostgreSQL diagnostic and a runbook section. It reports:

- total event-log bytes and row count;
- bytes and row counts by physical event partition;
- oldest and newest `received_at` values;
- per-organization event counts and oldest/newest receive times, with no payload or event content;
- collection timestamp and database identity so measurements can be compared over time.

The diagnostic must use catalog metadata and aggregate queries only, require an explicitly configured database connection, and never write to or lock the event stream beyond ordinary read behavior. The exact invocation should follow the existing deployment/operator workflow; it must not require a network-facing application endpoint.

Document a baseline procedure (capture periodically, compare growth and partition skew) and a decision checklist for revisiting physical time partitions, archival, or retention. Do not invent a hard capacity threshold: alert thresholds require the target deployment size and operator SLO. Until those exist, the runbook instructs operators to alert using their selected monitoring system and review measured growth before storage becomes constrained.

## Data lifecycle invariants

- Events remain append-only and are retained indefinitely by default.
- No automated action drops, truncates, detaches, or archives event partitions.
- `organization_id` remains part of event identity; duplicate deliveries continue to be idempotent.
- `VerifyProjection` continues to replay the complete available event history and detect drift.
- Any future retention or archival proposal must first specify audit/replay requirements, late-event behavior, restore verification, and approval/authorization for destructive operations.

## Acceptance criteria

1. An operator can run the diagnostic against the configured PostgreSQL database without changing data.
2. Output includes total and per-partition storage/counts, time bounds, and per-organization aggregate counts, and excludes payloads/secrets.
3. A runbook explains how to collect/compare baselines and explicitly states that no retention policy or automatic deletion is enabled.
4. Tests cover diagnostic SQL/output shape and a PostgreSQL integration case; existing event dedupe, restart recovery, replay verification, and tenant isolation tests continue to pass.
5. No Temporal.io dependency or production HTTP endpoint is introduced for this increment.

## Risks and mitigations

- **Aggregate query cost on a large event table:** document that detailed per-organization scans may be expensive; use catalog size estimates for physical storage and allow detailed counts to be an explicit opt-in query.
- **Metrics mistaken for a retention policy:** label the runbook and output as observations, not deletion recommendations; no destructive commands are included.
- **Thresholds vary by deployment:** avoid fixed alert values until capacity, growth, and recovery objectives are supplied by operators.
- **Future time partitioning changes uniqueness constraints:** retain the current schema now; require a separate reviewed migration design for global organization/event deduplication across time partitions.

## Open decision for a later policy review

Operators/product owners must define audit obligations, minimum replay horizon, archival needs, acceptable recovery objectives, and whether historical events may ever be deleted. Until that decision exists, retain indefinitely.
