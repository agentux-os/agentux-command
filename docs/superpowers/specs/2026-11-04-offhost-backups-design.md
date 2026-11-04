# Command off-host backups and restore verification

## Intent

Give Command operators a supported scheduled backup that is encrypted before
leaving the host, retained on an independent destination, and periodically
restored into a fresh PostgreSQL instance and checked with the deployed
`agentux-command check` binary. Failures and successful verification times
must be visible to systemd and Prometheus-compatible monitoring.

## Goals and non-goals

- Keep the existing manual `backup.sh` and `restore.sh` workflows usable.
- Schedule backups and restore verification with configurable systemd timers.
- Use operator-managed remote credentials and never print their values.
- Encrypt remote data, apply configurable retention, check repository
  integrity, and verify a restored database rather than only the dump file.
- Persist last-success timestamps and failure counters in a textfile collector
  metric file, with alerts documented.
- Do not add a cloud account, repository credentials, a hosted service, or a
  new dependency to the Command server image.

## Current behavior and constraints

`backup.sh` creates a PostgreSQL custom-format dump and SHA-256 sidecar.
`restore.sh` validates the sidecar and restores transactionally. CI's
`restore-drill.sh` checks a populated backup in a fresh PostgreSQL container.
The deployment bundle uses Podman Quadlets on the `agentux-command` network;
the running Command and PostgreSQL image IDs can be used for an isolated
verification container. Production `/metrics` runs inside Command, whereas
backup jobs run on the host, so the host job must publish its own metrics via
Prometheus node_exporter's textfile collector.

## Approaches

1. **Restic plus systemd timers (selected).** Restic encrypts repository data,
   deduplicates it, supports operator-selected remote backends, and provides
   snapshot retention and repository checks. Host services can reuse the
   existing dump and restore scripts without adding dependencies to the
   server image. The operator owns restic, credentials, destination, and
   node_exporter configuration.
2. **Age plus rclone.** Uses separate encryption, transfer, retention, and
   verification commands. It has a wider configuration surface and requires
   custom snapshot manifests and pruning behavior.
3. **A backup sidecar in the Command bundle.** Couples host scheduling and
   remote credentials to the server container lifecycle and grants the
   sidecar access to database and container control; rejected for this
   deployment's operator-managed backup boundary.

## Design

Install host scripts and systemd service/timer units. `backup-offsite.sh`
creates a restrictive dump in a dedicated, stable staging directory with the
existing script, uploads that directory to a dedicated Restic repository,
applies configurable daily/weekly/monthly retention, and removes the local
staging copy. A separate scheduled
`verify-offsite-backup.sh` checks repository data, restores the latest dump,
checks its checksum, restores it into a temporary PostgreSQL container on a
new internal-only Podman network, and runs `agentux-command check` from the
currently deployed Command image on that network. It removes temporary
containers, network, and files on success or error.

Both operations share a lock and persistent status directory. Atomic metrics
include last successful backup and restore-verification timestamps and
monotonic failure counters. Metrics are written to an operator-configured
node_exporter textfile collector file. Systemd journal and service exit
status remain the direct failure signal. Daily backups and monthly restore
verification are defaults; timer drop-ins let operators choose other
schedules. Restic retention defaults to 7 daily, 4 weekly, and 3 monthly
snapshots.

## Constraints and failure behavior

- Restic repository credentials, password file, and cloud credentials stay in
  an operator-managed root-only environment file or secret store; logs never
  echo environment variables or command traces.
- Backups use a repository dedicated to Command so `latest` is unambiguous.
- Missing configuration, upload, retention, checksum, container, restore, or
  `check` failures return non-zero, increment the corresponding metric, and
  leave the last-success timestamp unchanged.
- A verification failure does not stop Command or alter its production
  database.
- Local temporary database trust authentication is confined to a temporary
  internal-only network and is removed by a shell trap.
- Recovery documentation covers a replacement host, restic decryption,
  database restore, and revoking credentials/tokens issued after the snapshot.

## Open to implementation judgment

Exact package installation commands vary by host distribution. Document the
restic and node_exporter prerequisites and the supported Podman deployment,
without claiming the bundle installs or manages these host tools.
