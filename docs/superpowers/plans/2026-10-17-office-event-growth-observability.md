# Office Event Growth Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Give Command operators read-only visibility into Office event storage growth without introducing retention, deletion, schema migration, or Temporal.io coupling.

**Architecture:** Add a standalone Go diagnostic command backed by a read-only repeatable-read PostgreSQL transaction. Its default summary uses PostgreSQL catalog/statistics for low-cost physical sizes and estimated row counts; an explicit detailed mode runs aggregate queries for exact time bounds and per-organization counts, never selecting event payloads. Add operator guidance describing baseline collection and the indefinite-retention default.

**Tech Stack:** Go 1.25, `database/sql`, existing pgx stdlib driver, PostgreSQL 16.

**Spec:** `docs/superpowers/specs/2026-10-17-office-event-growth-observability-design.md`

## Global Constraints

- Events remain append-only and are retained indefinitely by default.
- No automated action drops, truncates, detaches, or archives event partitions.
- `organization_id` remains part of event identity; duplicate deliveries continue to be idempotent.
- The diagnostic must not select payload or event content and must not expose per-organization information through the HTTP health endpoint.
- No Temporal.io dependency, production HTTP endpoint, metrics backend, or retention threshold is added.
- Opening the report database must not run migrations or execute writes.

## Review Focus

1. Missing or malformed database URL must fail before any connection or migration attempt; test the CLI input validation.
2. The default mode must remain metadata-only and must never select from the full event payload column; test query classification/summary fields and integration output.
3. Detailed aggregation may be expensive and reveals organization IDs; require explicit opt-in and test default omission and detailed presence.
4. Empty event history must serialize with stable zero/null values, not fail on SQL NULL; test empty-database integration case.
5. An interrupted/failed query must return a nonzero command result and no success-shaped partial report; test collector error propagation and CLI exit behavior.

---

### Task 1: Implement a read-only event growth report collector

**Files:**
- Create: `internal/office/growth_report.go`
- Test: `internal/office/growth_report_test.go`
- Modify: `internal/office/postgres_store_integration_test.go`

**Interfaces:**
- Produces `GrowthReport`, `PartitionGrowth`, and `OrganizationGrowth` JSON types plus `CollectGrowthReport(ctx context.Context, db *sql.DB, detailed bool) (GrowthReport, error)`.
- Summary fields: database name, collection timestamp, total event bytes, estimated event rows, and leaf-partition name/bytes/estimated rows.
- Detailed fields, present only when requested: exact oldest/newest `received_at` and per-organization event count plus oldest/newest receive time. No query may select `payload`.
- Caller owns and opens the database connection; collector starts a `sql.LevelRepeatableRead` read-only transaction and performs no migration or write.

- [ ] **Step 1: Write failing tests** for JSON shape/omission in summary mode and detailed mode, query/collector behavior on an empty event table, partition aggregates, and per-organization aggregates.
- [ ] **Step 2: Run the focused Go tests** with PostgreSQL integration enabled; confirm the missing report API fails compilation and that empty-history and opt-in cases are represented.
- [ ] **Step 3: Implement `CollectGrowthReport(ctx, db, detailed)`** using PostgreSQL catalogs/statistics for physical size and estimated rows; in detailed mode run explicit aggregate queries for receive-time bounds and organization groups. Handle SQL NULLs and empty partitions deterministically.
- [ ] **Step 4: Verify** with `go test ./internal/office -count=1` and `go test -race ./internal/office -count=1`; PostgreSQL cases must execute in CI against PostgreSQL 16, not silently skip there.
- [ ] **Step 5: Commit** as `feat: report Office event storage growth`.

### Task 2: Add the standalone CLI, operator runbook, and CI coverage

**Files:**
- Create: `cmd/agentux-command-storage-report/main.go`
- Create: `cmd/agentux-command-storage-report/main_test.go`
- Modify: `.github/workflows/test.yml`
- Modify: `deploy/README.md`
- Modify: `README.md`

**Interfaces:**
- CLI reads `AGENTUX_COMMAND_DATABASE_URL`; accepts `--detailed` (default false); writes one JSON report to stdout and errors to stderr with nonzero exit status.
- CLI uses `sql.Open("pgx", ...)` directly and does not call `office.OpenPostgres` (which applies migrations).
- Runbook names the command, explains estimated versus exact/expensive fields, recommends periodic baseline comparison, states indefinite retention/no automatic deletion, and leaves alert thresholds to deployment-specific operator SLOs.

- [ ] **Step 1: Write failing CLI tests** for missing URL, default summary mode, `--detailed`, JSON output, and report-query failure returning nonzero without a partial success report.
- [ ] **Step 2: Run the focused tests** and confirm expected missing command behavior.
- [ ] **Step 3: Implement the CLI** with context cancellation, read-only connection behavior, stable JSON output, and clear error handling; do not print credentials or connection URL.
- [ ] **Step 4: Add CI coverage** so PostgreSQL 16 integration tests execute (including the diagnostic collector) with `AGENTUX_COMMAND_TEST_DATABASE_URL`; retain the existing full `go test ./... -count=1` and `go vet ./...` checks.
- [ ] **Step 5: Document operator usage** in `deploy/README.md` and summarize the report and lifecycle guarantee in the root README.
- [ ] **Step 6: Verify** with `go test ./... -count=1`, `go test -race ./internal/office ./cmd/agentux-command-storage-report -count=1`, `go vet ./...`, and the PostgreSQL-backed CI workflow.
- [ ] **Step 7: Commit** as `docs: document Office event growth diagnostics`.

## Self-review

- **Spec coverage:** Collector/report and detailed-query opt-in cover the diagnostic; CLI/runbook cover secure operation and baseline procedure; tests cover empty data, tenancy aggregation, output omission, and failures. No deletion, partition migration, alert backend, or Temporal integration is included, matching explicit non-goals.
- **Step precision:** Each task specifies paths, Go interfaces, flags, query classes, and verification commands. A detailed query is intentionally opt-in because exact per-organization aggregates scan event rows and reveal tenant identifiers.
- **Type consistency:** The collector takes `context.Context`, `*sql.DB`, and a `bool`; CLI consumes its report directly. The existing `AGENTUX_COMMAND_TEST_DATABASE_URL` remains the integration-test convention.
- **Review focus:** All five failure classes map to the collector and CLI tests in Tasks 1–2.
- **Proportion:** Two tasks separate reusable data collection from operator interface/docs; no independent subsystem is introduced.
