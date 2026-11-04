# Command benchmarks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a repeatable bounded benchmark harness and publish only hardware-qualified capacity limits.

**Architecture:** A Python standard-library runner reads scenario request templates, uses a fixed-size worker pool and finite timeouts, and emits JSON summary/raw latency plus optional process and PostgreSQL samples. Documentation maps workloads to the #49 single-host profile and records current limits as unqualified.

**Tech Stack:** Python 3 standard library, existing Command HTTP APIs, optional `psql` and OS process inspection.

**Spec:** `docs/superpowers/specs/2026-11-04-command-benchmarks-design.md`

## Global Constraints

- Supported topology is one host, one Command process, one PostgreSQL instance.
- Never claim an unmeasured capacity or retention bound.
- Bound concurrency, duration/request timeout, and worker memory; expose errors and overload outcomes.
- Record hardware, OS, versions, workload configuration, throughput, latency percentiles, CPU/memory, database growth, and errors.
- Retention runs must be explicit, backed up, and reported as operator-controlled.

## Review Focus

- Invalid scenario values and zero/negative bounds fail before sending traffic; test validation.
- Concurrent workload never exceeds its configured worker count; test with a local HTTP fixture.
- HTTP failures and timeouts count as errors, not successes; test fixture responses.
- Empty samples produce stable null percentiles and no divide-by-zero; test zero requests.
- Missing optional process/database instrumentation is identified in output, never silently reported as zero; test unavailable collectors.

---

### Task 1: Build and test the bounded runner

**Files:** Create `benchmarks/command_bench.py`, `benchmarks/test_command_bench.py`, `benchmarks/scenarios.example.json`.

- [x] Test config validation, bounded concurrency, errors/timeouts, percentiles, and optional collector failures against a local HTTP server.
- [x] Run focused tests and observe expected missing-runner failures.
- [x] Implement the runner with bounded threads, deadlines, JSON output, hardware metadata, and optional target-process/DB sampling.
- [x] Run focused tests and the benchmark runner's help/config validation commands.

### Task 2: Publish workload procedure and qualification status

**Files:** Create `benchmarks/README.md`; modify `docs/production-readiness.md`.

- [x] Document preconditions and repeatable commands for all requested scenarios, including retention and overload procedures.
- [x] Define evidence fields and qualified-limit decision rules; leave all limits `UNQUALIFIED` until evidence from the target profile hardware exists.
- [x] Cross-check #52 requirements against the profile table and run the complete relevant validations.

### Task 3: Commit locally

- [x] Review diff and ensure only this worktree changed.
- [ ] Commit benchmark harness, tests, and docs locally; do not push or open a PR.
