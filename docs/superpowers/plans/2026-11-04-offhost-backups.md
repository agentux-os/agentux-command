# Command off-host backups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Schedule encrypted off-host Command backups and recurring full restore verification with observable outcomes.

**Architecture:** Host-side shell scripts call the existing dump and restore workflows, Restic, Podman, and the running Command image. Systemd timers schedule each operation; a shared status helper atomically writes node_exporter textfile metrics.

**Tech Stack:** POSIX shell, systemd, Restic, Podman, PostgreSQL 16, Prometheus textfile metrics.

**Spec:** `docs/superpowers/specs/2026-11-04-offhost-backups-design.md`

## Global Constraints

- Preserve manual `deploy/scripts/backup.sh` and `deploy/scripts/restore.sh` behavior.
- Secrets are operator-managed and never echoed to logs or metric labels.
- Restore verification uses a fresh temporary database and the currently deployed Command image.
- Failures return non-zero and leave success timestamps unchanged.
- Temporary files and containers are removed on every exit path.

## Review Focus

- Missing/malformed environment variables or unreadable Restic password file fail before data is uploaded.
- A Restic upload/retention or repository-check error must be visible and counted without changing last-success timestamps.
- A corrupt checksum or database that fails `agentux-command check` must fail verification and clean up its temporary container.
- Concurrent backup and verification timers must serialize without deleting each other's files.
- Metric output must remain valid Prometheus text and contain no repository URL, organization, or credential labels.

---

### Task 1: Shared backup status and metric state

**Files:**
- Create: `deploy/scripts/backup-status.sh`
- Test: `tests/backup-workflow.sh`

**Interfaces:**
- Consumes: action (`success|failure`), operation (`backup|restore_verification`), state directory, and metrics file from environment.
- Produces: persistent last-success and failure-count files; atomically replaced Prometheus metrics file.

- [x] Write tests for success timestamps, failure counters, unchanged last-success on failure, atomic textfile output, and secret-free labels.
- [ ] Run `sh tests/backup-workflow.sh` and confirm the new status behavior fails before implementation.
- [x] Implement the status helper; focused execution is pending a Linux shell runner because local Windows Git Bash and WSL are sandbox-blocked.

### Task 2: Scheduled off-host backup

**Files:**
- Create: `deploy/scripts/backup-offsite.sh`
- Create: `deploy/systemd/user/agentux-command-backup.service`
- Create: `deploy/systemd/user/agentux-command-backup.timer`
- Test: `tests/backup-workflow.sh`

**Interfaces:**
- Consumes: `RESTIC_REPOSITORY`, `RESTIC_PASSWORD_FILE`, `BACKUP_METRICS_FILE`, `BACKUP_STATE_DIR`, optional daily/weekly/monthly retention values, and existing Podman settings.
- Produces: tagged Restic snapshots of the existing dump and checksum from one stable source directory, with local staging cleaned after success or failure.

- [ ] Add mocked Restic/Podman tests for successful upload and retention, plus backup and retention failure paths.
- [ ] Verify those new assertions fail against the absent workflow.
- [ ] Implement backup orchestration and systemd timer/service with overridable scheduling and retention.
- [ ] Run focused shell tests and `sh -n` on all new shell scripts.

### Task 3: Recurring isolated restore verification

**Files:**
- Create: `deploy/scripts/verify-offsite-backup.sh`
- Create: `deploy/systemd/user/agentux-command-backup-verify.service`
- Create: `deploy/systemd/user/agentux-command-backup-verify.timer`
- Test: `tests/backup-workflow.sh`

**Interfaces:**
- Consumes: same Restic repository/credential and status configuration, plus `ENGINE`, Command/PostgreSQL container names, and restore polling limit.
- Produces: a successful full Restic check, verified restored dump, fresh PostgreSQL restore, and `agentux-command check` result.

- [ ] Add mocked tests for successful verification, corrupted dump, failed `check`, and cleanup after failure.
- [ ] Confirm the new tests fail against the absent verifier.
- [ ] Implement the isolated Podman restore workflow and monthly systemd timer.
- [ ] Run focused tests and shell syntax checks.

### Task 4: Operational setup, alerts, recovery, and CI

**Files:**
- Modify: `deploy/README.md`
- Modify: `.github/workflows/test.yml`
- Modify: `tests/backup-workflow.sh`

- [x] Document Restic installation, protected credentials, dedicated repository setup, textfile collector, timer overrides, retention, alerts, replacement-host recovery, and post-backup credential revocation.
- [ ] Add the workflow test and shell syntax checks to CI.
- [ ] Run the complete available local verification and inspect the final diff for secrets and unrelated changes.
- [ ] Commit and open a PR with a separate-agent review request; do not merge.
