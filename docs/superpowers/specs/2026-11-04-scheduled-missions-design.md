# Scheduled missions design

## Intent

Let an organization start a published mission once or on a recurring cron
expression without requiring an operator to be present. The scheduler must
survive process restarts and concurrent Command replicas, and it must preserve
the existing mission engine's node policy and approval gates.

## Decisions

- Keep schedules and firing state in Command's PostgreSQL database, consistent
  with ADR 0020. Do not add Temporal or an external scheduler service.
- Store cron expressions with an explicit IANA time zone. A one-time schedule
  stores an absolute RFC 3339 timestamp. Use `cron-lite`'s five-field cron
  syntax and timezone support; reject invalid or missing IANA zones at write
  time. `next_run_at` is always stored as UTC.
- A schedule stores organization, mission ID, immutable input, optional node
  target, creator, status, timing, missed-run policy, concurrency policy,
  `next_run_at`, and last-run metadata. The published workflow definition is
  resolved by the existing engine when the run is created.
- Persist each due firing as an occurrence with a stable run ID and a unique
  `(schedule_id, scheduled_for)` key. Claim due occurrences in short
  transactions using `FOR UPDATE SKIP LOCKED`; a crashed worker leaves a
  recoverable pending occurrence. Insert the run and mark the occurrence
  dispatched atomically where they share the Command database. The run ID is
  deterministic from the occurrence identity, so retries cannot create a
  second logical run.
- Schedule advancement and occurrence creation commit together. `skip` drops
  missed slots and advances to the next future slot; `run_once` creates one
  occurrence for the latest missed slot; `run_all` creates each missed slot,
  subject to a bounded catch-up batch per scheduler tick.
- Concurrency policies are evaluated against active runs linked to the
  schedule. `skip` records a skipped occurrence; `queue` leaves it pending
  until no linked run is active; `allow` dispatches immediately. Each pending
  occurrence remains durable and has one auditable outcome.
- A one-time schedule becomes completed after its occurrence is dispatched.
  Pause prevents new occurrence claims; resume recalculates the next due
  instant from the current time. Delete is a soft delete so audit and prior
  runs remain visible, and no new occurrences are created.
- Operator endpoints are organization-scoped and use the existing
  `NODE_COMMAND` capability for create/change/pause/resume/delete and
  `ORGANIZATION_READ` for listing. Record schedule lifecycle events in the
  Office event log. Runs are started by the Command service actor and continue
  through the ordinary engine, including node policy and approvals.
- The Command API and persistence are this change's scope. Office and Command
  Console views belong in agentux-apps and will be tracked as a dependent
  issue; no UI is implied by this API implementation.
- Expose bounded-cardinality schedule counters and lag histograms/gauges in
  `/metrics`; never label metrics with schedule, mission, organization, node,
  or owner IDs.

## API shape

- `GET /v1/organizations/{org}/schedules`
- `POST /v1/organizations/{org}/schedules`
- `GET /v1/organizations/{org}/schedules/{schedule}`
- `PATCH /v1/organizations/{org}/schedules/{schedule}`
- `POST /v1/organizations/{org}/schedules/{schedule}/pause`
- `POST /v1/organizations/{org}/schedules/{schedule}/resume`
- `DELETE /v1/organizations/{org}/schedules/{schedule}`

Responses include the schedule, `nextRunAt`, `lastRunAt`, and linked run IDs.
Schedule updates use an expected revision to reject lost updates.

## Data and recovery

`mission_schedules` stores the current schedule document and indexed due time.
`mission_schedule_occurrences` stores each intended firing, scheduled UTC
instant, state (`pending`, `dispatched`, `skipped`, or `failed`), stable run
ID, and timestamps. Unique constraints prevent duplicate schedule/time pairs
and duplicate scheduled run IDs. The engine's existing run store remains the
source of truth for execution lifecycle. On startup and every scheduler tick,
Command claims pending due occurrences and dispatches/retries them. Existing
mission execution still reconciles separately through `Engine::tick`.

## Failure behavior

- Invalid cron, timezone, one-time timestamp, mission, or concurrency/misfire
  policy returns 422 without persisting a schedule.
- Database failure leaves the firing pending and retryable; it must not advance
  the schedule without recording its due occurrence.
- Engine start failure marks the occurrence failed with a safe error summary;
  transient storage failures remain pending. Retrying uses the same run ID.
- A paused, deleted, or completed one-time schedule is never claimed.
- Metrics expose schedule dispatch failures and lag without high-cardinality
  labels.

## Verification

Use a controllable clock for cron/DST calculation, one-time schedules, all
misfire and concurrency policies, pause/resume/update semantics, and stable
occurrence IDs. PostgreSQL integration coverage must demonstrate two workers
cannot dispatch the same occurrence and that a process restart recovers a
pending occurrence without creating a duplicate run. HTTP coverage verifies
organization scoping, required capabilities, audit events, and bounded metric
labels.
