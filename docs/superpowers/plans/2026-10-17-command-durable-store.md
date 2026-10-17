# Command Durable Projection Store Implementation Plan

**Goal:** Persist Office events, projection snapshots and approval decisions in PostgreSQL so Command can recover after restart and replay events without changing the Office contract.

**Architecture:** Keep the existing in-memory store for deterministic tests and local demos. Define a context-aware store interface, add a PostgreSQL implementation using the configured Command database, embed versioned SQL migrations, and atomically write event log, per-node cursor and materialized projection in one transaction. Scope event identity and approval idempotency by organization. Add projection verification by replaying its append-only event stream and comparing the rebuilt materialized state.

**Tech Stack:** Go 1.25, `database/sql`, pgx stdlib driver, PostgreSQL 16, embedded SQL migrations, integration tests gated by `AGENTUX_COMMAND_TEST_DATABASE_URL`.

## Constraints

- Preserve `office-projection.v1` and existing endpoint response shapes; adding `organizationId` to approval requests is additive and required for tenant-scoped idempotency.
- PostgreSQL writes for event, cursor and projection are atomic.
- Duplicate `(organizationId,eventId)` is idempotent; duplicate or regressing `(organizationId,nodeId,sequence)` with a different event is a conflict.
- Approval decisions and idempotency records are committed in one transaction and keyed by `(organizationId,approvalId,idempotencyKey)`.
- Runtime production startup requires `AGENTUX_COMMAND_DATABASE_URL`, pings the database, applies migrations, then serves requests. Unit tests keep using the in-memory implementation.
- Event log is append-only; projection JSON is a materialized read model. `VerifyProjection` rebuilds by replay and detects divergence.
- No destructive migration or data deletion.

## Tasks

### Task 1: Define durable store and database schema

- Add embedded PostgreSQL migration for event log, per-node cursors, projections and approval idempotency.
- Add a migration runner guarded by a PostgreSQL transaction advisory lock.
- Add `OpenPostgres` with URL validation, connection ping and automatic migrations.
- Add failing tests for migration versioning and missing database configuration, then implement.
- Run `go test ./...`; commit `feat: add Command PostgreSQL store schema`.

### Task 2: Persist events and rebuild projections

- Add failing tests for durable event deduplication, independent organization/node cursors, out-of-order conflict, restart recovery and projection replay verification.
- Implement transactional append, cursor advance, materialized projection update, load and `VerifyProjection`.
- Add optional PostgreSQL integration tests using an isolated temporary schema; skip only when the test database URL is absent.
- Run `go test ./...`; commit `feat: persist Office events and projections`.

### Task 3: Persist tenant-scoped approval decisions

- Add failing tests for approval decision persistence across store instances, organization-scoped idempotency, same-key concurrent retries and already-decided conflicts.
- Update request/adapter contract with `organizationId`; keep the field additive in AUXP schema and fixtures.
- Implement atomic decision and replay persistence with row locking and unique idempotency key.
- Run Go and Apps contract tests; commit `feat: persist idempotent approval decisions`.

### Task 4: Wire production runtime and operations docs

- Add failing startup/config tests for missing URL and database connection/migration failure.
- Wire `cmd/agentux-command` to PostgreSQL store and graceful shutdown; retain in-memory `NewHandler` for tests.
- Document database URL, migrations, backup/recovery and test setup; remove the stale README statement that database migrations are absent.
- Run `go test ./...`, database integration tests when configured, protocol fixtures and Apps tests; commit `docs: describe durable Command operations`.

## Review Focus

- Tenant key is included in every persistent identity and query.
- Concurrent events cannot advance the same node cursor out of order or leave a partial projection.
- Duplicate event delivery and approval retry return the original state after restart.
- Replay verification detects materialized read-model drift.
- PostgreSQL is unavailable: startup fails clearly, and the health endpoint never advertises a usable Command service.
- Schema changes preserve the existing HTTP projection contract.
