# Scheduled missions implementation plan

> Follow the repository's existing `AGENTS.md` guidance. Keep this work in
> `codex/command-58-scheduled-missions`; do not merge it.

**Goal:** Add durable, organization-scoped one-time and recurring mission
schedules to Command.

**Architecture:** PostgreSQL stores schedules and unique firing occurrences.
The existing Command worker polls and claims due rows with row locks; all
mission execution continues through the existing engine and its node policy
and approval behavior.

**Tech:** Rust, Axum, PostgreSQL, chrono, cron-lite with timezone support.

**Spec:** `docs/superpowers/specs/2026-11-04-scheduled-missions-design.md`

## Task 1: Cron and timestamp domain model

Files: `Cargo.toml`, `Cargo.lock`, `src/schedules.rs`, module exports.

- [ ] Define validated schedule request, schedule document, and policy enums.
- [ ] Validate one-time RFC 3339 inputs and IANA timezone-aware cron syntax.
- [ ] Implement deterministic next-time and bounded missed-slot calculation.
- [ ] Cover cron parsing, UTC conversion, DST boundaries, and policy edge cases.

## Task 2: Durable schedule and occurrence storage

Files: new migration, `src/schedules.rs`, migrations registry, PostgreSQL tests.

- [ ] Add organization-scoped schedule and occurrence tables, indexes, revisions,
  uniqueness constraints, and soft deletion.
- [ ] Extend the store with CRUD, due claiming, atomic advancement/occurrence
  creation, and occurrence state transitions.
- [ ] Ensure row-lock claims use `SKIP LOCKED` and recover after a worker restart.
- [ ] Verify duplicate workers and deterministic run IDs at the PostgreSQL layer.

## Task 3: Scheduler engine and run linkage

Files: `src/schedules.rs`, `src/missions.rs`, migration and mission tests.

- [ ] Poll due schedules from the existing background worker.
- [ ] Apply `skip`, `run_once`, and bounded `run_all` missed-run behavior.
- [ ] Apply `skip`, `queue`, and `allow` overlap behavior.
- [ ] Start due missions through the existing mission engine and preserve
  policy gates and approvals.
- [ ] Make schedule-linked run insertion idempotent across restart and races.

## Task 4: Organization API, authorization, and audit

Files: `src/server.rs`, API/auth tests.

- [ ] Add list/create/get/update/pause/resume/soft-delete routes.
- [ ] Scope every lookup and mutation by organization.
- [ ] Require existing organization read or node command capabilities as designed.
- [ ] Emit Office audit events for lifecycle changes and firing outcomes.
- [ ] Return next/last firing metadata and linked run IDs.

## Task 5: Metrics and operator documentation

Files: `src/server.rs`, `README.md`, production readiness docs, metrics tests.

- [ ] Add bounded-cardinality schedule state, failure, and lag metrics.
- [ ] Document cron syntax/timezone semantics, missed runs, concurrency,
  permissions, recovery, and API examples.
- [ ] Ensure metrics omit all organization and schedule identifiers.

## Task 6: Apps follow-up and review

- [ ] Create a dependent agentux-apps issue for Office and Command Console
  schedule management and run linkage.
- [ ] Review migration rollback/startup behavior and API error semantics.
- [ ] Prepare a PR linked to Command #58 with an explicit separate-agent review
  checklist; do not merge.
