# Command Production Metrics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose operationally protected, bounded-cardinality metrics and actionable alerts for Command production deployments.

**Architecture:** Add a small in-process collector for HTTP counters and latency histograms, and collect database/outbox, mission, and node health at scrape time. Protect `/metrics` with a dedicated constant-time-checked bearer token; leave it disabled when no token is configured. Keep all labels from fixed enumerations, never tenant, user, node, run, URL, or secret values.

**Tech Stack:** Rust 2024, Axum 0.8, Tokio, PostgreSQL, Prometheus text exposition format.

**Spec:** agentux-command issue #50.

## Global Constraints

- Metrics access requires explicit operational configuration and authorization.
- Labels have fixed bounded values; never emit tenant/user identifiers, secrets, prompts, or event payloads.
- Metrics cover HTTP requests/errors/latency, database readiness, event outbox delivery/age, active and waiting mission runs, node heartbeat freshness/connectivity, and storage activity.
- Metrics and alert guidance remain useful when optional webhook/NATS/object-store services are unavailable.
- Runbook documents metric names, units, labels, cardinality, and alert examples.

## Review Focus

- Unconfigured metrics access must not expose operational details; test disabled and unauthorized responses.
- Dynamic paths and attacker-controlled methods must not create unbounded labels; test route/method normalization.
- Tenant, node, run, credentials, and event payload values must never appear as labels or exposition content.
- Database and optional sink outages must still return valid metrics and convey dependency state.
- Mission, outbox, and heartbeat gauges must use fixed status labels and correct age units.

---

### Task 1: Bounded HTTP collector

**Files:** Create `src/metrics.rs`; modify `src/lib.rs` and `src/server.rs`; test `tests/metrics.rs`.

**Interfaces:** `Metrics::record_request(method, path, status, elapsed)`; `Metrics::render_http()`; fixed route/method normalization.

- [ ] Write tests proving dynamic paths/methods collapse to fixed labels and requests increment counters/latency buckets.
- [ ] Run targeted tests and observe expected failures.
- [ ] Implement the collector and Axum request middleware using finite route/method/status labels.
- [ ] Run targeted tests and confirm output contains no raw path values.

### Task 2: Authorized scrape and service gauges

**Files:** Modify `src/server.rs`, `src/bin/agentux-command.rs`, `src/missions.rs` only if required; test `tests/metrics.rs`.

**Interfaces:** `Handler::with_metrics_token`; `/metrics` returns Prometheus exposition only for a configured, matching bearer token; scrape gauges include fixed-state runs/nodes, DB readiness, outbox backlog/age and database bytes.

- [ ] Write tests for disabled, unauthorized, and authorized scrape access and verify tenant/user/secret values are absent.
- [ ] Run targeted tests and observe expected failures.
- [ ] Implement secure endpoint and scrape-time gauges, tolerating unavailable optional event sinks.
- [ ] Run targeted tests and confirm HTTP exposition content type and gauge behavior.

### Task 3: Operations runbook

**Files:** Modify `deploy/README.md` and/or `docs/production-readiness.md`.

- [ ] Document env configuration, scrape authorization, metric names/units/labels/cardinality, and interpretation.
- [ ] Include alert examples for readiness loss, growing outbox backlog/age, stale nodes, stuck missions, and storage pressure.
- [ ] State optional sink outages do not prevent local database/mission/node metrics, and avoid universal thresholds without workload evidence.

### Task 4: Full verification and PR

**Files:** All changes above.

- [ ] Run the targeted and repository Rust test suites, formatting, and lint checks available in the environment.
- [ ] Review diff for accidental high-cardinality labels, sensitive exposition, and unrelated changes.
- [ ] Commit, push branch, open PR linked to #50, and comment requesting a different agent to review per `agentux/AGENTS.md`.
