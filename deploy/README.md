# AgentUX Command server bundle

For the supported production topology target, assumptions, Command/operator
responsibility split, unqualified objectives, and release gates, see the
[production readiness profile v1.0](../docs/production-readiness.md). The
bundle is a single-host deployment; its existence does not qualify availability
or capacity.

This directory contains the optional `command-server` profile bundle. It is
container-only: PostgreSQL, NATS, MinIO and the Command service run
as rootless Podman Quadlets. No host RPM installation or mutable `/usr` change
is required.

## Bootstrap on AgentUX OS

The OS profile manager creates `/var/lib/agentux/command`. An operator then:

1. copies `config/command.env.example` to
   `/var/lib/agentux/command/command.env` and replaces every placeholder;
2. creates the three Podman secrets named in the Quadlet files (the PostgreSQL
   password secret is shared with the Command service as `PGPASSWORD`);
3. copies the Quadlet files (the `.container`, `.volume` and `.network`
   units) to `/etc/containers/systemd/`;
4. pins the Command image by digest in a local drop-in or release update;
5. runs `systemctl daemon-reload` and enables the units in dependency order;
6. issues the first administrator's service token (shown once; keep it in the
   operators' secret store). This token is for CLI and automation use; web
   operators sign in separately through the configured OIDC provider:

   ```sh
   podman exec agentux-command agentux-command token issue \
     --organization <org> --name admin \
     --capabilities auth.admin,organization.read,node.enroll,node.command,approval.decide
   ```

   Further tokens come from `POST /v1/service-tokens`. The demo tokens are
   refused unless `AGENTUX_COMMAND_INSECURE_DEMO_TOKENS=1` (development only).

For interactive web access, configure the OIDC issuer, client, and role map in
`command.env.example`. The published image includes the version-pinned Office
and Console bundles and serves them at `/office/` and `/console/` on the same
origin. Console 0.1.1 is pinned with its SHA-256 in the Command `Containerfile`.
Each browser session is attributed to the signed-in operator and checked
against that operator's organization capabilities; do not share a service
token with web visitors.

Example secret setup (values must come from an operator-managed secret store):

```sh
printf '%s' "$POSTGRES_PASSWORD" | podman secret create agentux-command-postgres-password -
printf '%s' "$MINIO_ROOT_USER" | podman secret create agentux-command-minio-root-user -
printf '%s' "$MINIO_ROOT_PASSWORD" | podman secret create agentux-command-minio-root-password -
```

Do not commit the resulting env file, secrets or generated Quadlet drop-ins.

## Operations

What follows was run as written: the restore drill in CI
(`deploy/scripts/restore-drill.sh`, job `restore-drill`) backs up a filled
database, restores it into a fresh one and checks they match.

### Health

| Endpoint | Says | Used by |
|---|---|---|
| `/livez` | the process answers | the Quadlet's `HealthCmd`; with `HealthOnFailure=kill`, systemd restarts a Command that hangs |
| `/readyz` (and `/healthz`) | the process **and** PostgreSQL answer; 503 with `"database": "unreachable"` otherwise | monitoring, load balancers |

A database outage makes Command unready, not dead: it is not restarted in a
loop, and it answers again once PostgreSQL is back. Command refuses to start
if the database is unreachable or a migration fails (`journalctl -u
agentux-command`). PostgreSQL, NATS and the object store have their own health
commands (`podman healthcheck run <container>`).

Command `Requires=` PostgreSQL only. NATS and the object store come with the
bundle for what is next ([ADR 0020](https://github.com/agentux-os/agentux/blob/main/docs/adr/0020-command-workflows-without-temporal.md));
Command keeps nothing in them today, and their failure does not stop it.

### Metrics and alerting

Command exposes Prometheus text metrics at `/metrics` only when
`AGENTUX_COMMAND_METRICS_TOKEN` is configured. Supply a dedicated bearer token
of at least 32 random bytes through the operator's secret mechanism; do not
reuse an operator, node, OIDC, webhook, or database credential. Keep the
endpoint on a trusted monitoring network, restrict it at the firewall or
reverse proxy, and use TLS when the scrape crosses a host boundary. Without a
token, `/metrics` returns 404; a missing or incorrect bearer token receives
401. The token is never included in metric output or labels.

Example scrape from a monitoring host, with the token injected from its secret
store into the process environment:

```sh
curl --fail --silent -H "Authorization: Bearer ${AGENTUX_COMMAND_METRICS_TOKEN}" \
  https://command.example.invalid/metrics
```

Metrics use fixed label values only. They do not include organization, user,
node, run, URL, prompt, event payload, or credential values. HTTP `method` is
one of GET/POST/PUT/PATCH/DELETE/HEAD/OPTIONS/OTHER; `route` is a fixed route
group; `status_class` is 1xx–5xx/other. Run state, node presence, and
connection state use fixed enumerations. There are no tenant or resource ID
labels. Histograms use seconds and fixed buckets from 5 ms through 5 s plus
infinity.

| Metric | Type / unit | Labels | Meaning |
|---|---|---|---|
| `command_http_requests_total` | counter / requests | method, route, status_class | Requests handled by this process |
| `command_http_errors_total` | counter / responses | method, route, status_class | 4xx and 5xx responses |
| `command_http_request_duration_seconds` | histogram / seconds | method, route | Request latency |
| `command_database_configured` | gauge / boolean | none | PostgreSQL is configured |
| `command_database_ready` | gauge / boolean | none | PostgreSQL answered the scrape query |
| `command_event_sink_configured` | gauge / boolean | none | Optional webhook delivery sink is configured |
| `command_event_outbox_pending_events` | gauge / events | none | Undelivered database outbox rows |
| `command_event_outbox_failed_events` | gauge / events | none | Pending rows with a previous delivery failure |
| `command_event_outbox_oldest_age_seconds` | gauge / seconds | none | Age of the oldest pending event |
| `command_office_events_estimated_rows` | gauge / estimated rows | none | PostgreSQL statistics estimate of rows retained in the Office event log |
| `command_event_retention_enabled` | gauge / boolean | none | Archive-before-prune job is enabled |
| `command_database_size_bytes` | gauge / bytes | none | Current PostgreSQL database size |
| `command_mission_runs` | gauge / runs | state | Active runs in running or waiting state |
| `command_mission_waiting_oldest_age_seconds` | gauge / seconds | none | Age of the oldest waiting run |
| `command_nodes` | gauge / nodes | presence | Nodes grouped by heartbeat-derived online/degraded/offline state |
| `command_node_connections` | gauge / nodes | state | Connected or disconnected node protocol channels |
| `command_node_heartbeat_max_age_seconds` | gauge / seconds | none | Maximum age of a known node heartbeat |

Example Prometheus alert expressions (choose windows and thresholds to match
your measured workload and support objectives):

```promql
command_database_ready == 0
increase(command_http_errors_total{status_class="5xx"}[5m]) > 0
command_event_outbox_oldest_age_seconds > 300
command_mission_waiting_oldest_age_seconds > 1800
command_nodes{presence="offline"} > 0
node_filesystem_avail_bytes{mountpoint="/path/to/postgresql-volume"} / node_filesystem_size_bytes{mountpoint="/path/to/postgresql-volume"} < 0.15
```

Replace `/path/to/postgresql-volume` with the actual PostgreSQL volume
mountpoint reported by the host's filesystem exporter. Use the offline-node
alert only when every registered node is expected to be online. Gate outbox
backlog alerts on `command_event_sink_configured == 1`; an unconfigured sink
is an intentional operating mode. Set the outbox and waiting-run thresholds
from the deployment's delivery and workflow objectives; no universal
threshold is assumed. Compare database size with free space on the actual
PostgreSQL volume using host metrics. The scrape remains useful when the
optional event sink is not configured or unavailable: HTTP, database, outbox,
mission, and node metrics are local to Command and PostgreSQL. NATS and the
object store are not required for this scrape. Database query failures set
`command_database_ready` to 0 while process and in-memory metrics continue.

### Backups

All of Command's state is in PostgreSQL: event log, projections,
definitions, runs, node state, enrollment, service tokens (hashed),
sessions and the event outbox. A database dump is a complete backup.

```sh
deploy/scripts/backup.sh /var/lib/agentux/command/backups
# /var/lib/agentux/command/backups/agentux-command-20261022T120000Z.dump (+ .sha256)
```

For scheduled off-host backups, install Restic on the host and use the
provided user-systemd services. Restic encrypts repository contents before
upload; use a dedicated repository for Command so `latest` always refers to a
Command database snapshot. Run the timers as the same OS account that owns the
rootless Command Podman containers (`agentux` in the examples).

1. Create an operator-managed Restic password file and environment file. Make
   the environment file `root:agentux` mode `0640`; make the password file
   readable by `agentux` but not other users. Do not put credentials in shell
   history, unit files, or this repository. Start from
   [`config/command-backup.env.example`](config/command-backup.env.example).
   Add the chosen backend's credentials there (for example, the S3 access
   key variables). Keep the Restic password recovery key outside the backup
   destination.
2. Enable lingering so the rootless user manager and timers run when the
   account is logged out, then initialize the repository as that account:

   ```sh
   sudo loginctl enable-linger agentux
   sudo -iu agentux systemd-run --user --wait --pipe \
     --property=EnvironmentFile=/etc/agentux/command-backup.env /usr/bin/restic init
   ```

3. Install `backup-status.sh`, `backup-offsite.sh`,
   `backup-lock.sh`, `verify-offsite-backup.sh`, `backup.sh`, and `restore.sh` under
   `/usr/libexec/agentux-command/`, and the four units from `systemd/user/` in
   the account's `~/.config/systemd/user/`. Install the scripts as executable.
   Ensure the account can write `/var/lib/agentux/command` and the configured
   node_exporter textfile collector directory.
4. Configure node_exporter with
   `--collector.textfile.directory=/var/lib/node_exporter/textfile_collector`.
   The generated file contains only timestamps and counters; it has no labels
   or credential values. Scrape node_exporter for these host-job metrics;
   Command's `/metrics` endpoint itself runs inside the Command container.
5. Enable both timers:

   ```sh
   systemctl --user daemon-reload
   systemctl --user enable --now agentux-command-backup.timer
   systemctl --user enable --now agentux-command-backup-verify.timer
   ```

The backup timer runs daily by default. `BACKUP_KEEP_DAILY`,
`BACKUP_KEEP_WEEKLY`, and `BACKUP_KEEP_MONTHLY` default to 7, 4, and 3
snapshots. Keep `BACKUP_STAGING_DIR` the same across runs so Restic applies
retention to one stable source path. Override `OnCalendar=` with a timer
drop-in to change the interval; choose it from the deployment's recovery point
objective. The verification
timer runs monthly, checks the full Restic repository data, restores the
latest dump into a temporary PostgreSQL container, and runs
`agentux-command check` from the currently deployed Command image. It needs
network access to the Restic backend and the local Podman images. It does not
stop or write to production Command or PostgreSQL. The temporary database
uses trust authentication on a newly created internal-only Podman network
shared only with the one-shot check container; both the container and network
are removed when the job exits.

Inspect timer runs and logs with:

```sh
systemctl --user list-timers 'agentux-command-backup*'
journalctl --user -u agentux-command-backup.service -u agentux-command-backup-verify.service
```

Metrics published in the textfile collector:

| Metric | Meaning |
|---|---|
| `agentux_command_backup_last_success_timestamp_seconds` | Unix time of the last successful remote backup |
| `agentux_command_backup_failures_total` | Failed backup or retention attempts |
| `agentux_command_restore_verification_last_success_timestamp_seconds` | Unix time of the last successful full restore verification |
| `agentux_command_restore_verification_failures_total` | Failed repository, checksum, restore, or `check` attempts |

Example alerts (adjust for the configured backup and verification schedule):

```promql
agentux_command_backup_last_success_timestamp_seconds == 0 or time() - agentux_command_backup_last_success_timestamp_seconds > 36 * 3600
increase(agentux_command_backup_failures_total[1h]) > 0
agentux_command_restore_verification_last_success_timestamp_seconds == 0 or time() - agentux_command_restore_verification_last_success_timestamp_seconds > 45 * 86400
increase(agentux_command_restore_verification_failures_total[1d]) > 0
```

The systemd service's failed state and journal are also authoritative signals;
the metric file is refreshed on each attempt. Keep one manual pre-upgrade
backup with the existing `backup.sh` workflow as well.

### Restore

```sh
systemctl stop agentux-command
deploy/scripts/restore.sh --yes /var/lib/agentux/command/backups/agentux-command-<time>.dump
systemctl start agentux-command
podman exec agentux-command agentux-command check
```

The restore runs in one transaction (all or nothing) after checking the
dump's sha256. `agentux-command check` rebuilds every organization's Office
projection from its event log, compares it with the stored one, and prints
what the database holds (migration, organizations, events, definitions,
runs, nodes, tokens). It exits non-zero on a mismatch. To restore onto a
replacement host, install Restic and the operator-managed password/backend
credentials, then restore the latest snapshot to a temporary directory with
`restic restore latest --target <directory>`. Find the
`agentux-command-*.dump` and `.sha256` files there, bootstrap the Command
bundle, start PostgreSQL only, and run `deploy/scripts/restore.sh --yes
<restored-dump>`. Start Command and run `podman exec agentux-command
agentux-command check` before reconnecting operators and nodes. Keep the
Restic password recovery key separate from the remote repository; without it,
encrypted snapshots cannot be recovered.

Nodes reconnect by themselves, and events they sent after the backup are
deduplicated when they replay them. Operators' sessions and tokens are those
of the backup: revoke anything issued after it was taken.

### Upgrades and rollback

1. Take a backup (above) and note the running image digest:
   `podman inspect --format '{{.Image}}' agentux-command`.
2. Change `Image=` in `agentux-command.container`, pinned by digest,
   `systemctl daemon-reload`, `systemctl restart agentux-command`.
3. Command applies its migrations at startup. They are forward-only and
   additive; one that fails leaves the database as it was, and Command does
   not start. Check `/readyz` and `agentux-command check`.

**Rollback:** a migrated database may not suit the previous version. Stop
Command, restore the backup from step 1, put the previous digest back and
start it. Events taken in between are lost from the log, though nodes resend
what they still hold. Upgrade PostgreSQL itself (a major version) through a
dump and restore into a new volume, never in place. The named volumes are
never removed with a profile.

### Recovery objectives and qualification

The profile sets an initial target RPO of 24 hours (conditional on a successful
daily off-host backup) and target RTO of 8 hours to restore `/readyz` and pass
`agentux-command check`. Both are **UNQUALIFIED** until measured in the
operator's environment. The repository's current scripts are manual; they do
not schedule, encrypt, or copy backups off-host. The CI restore drill is not a
production-sized recovery-time measurement. See the profile's [objectives and
release qualification checklist](../docs/production-readiness.md#objectives-and-qualification-status).

The initial topology is one host, one Command process, and one PostgreSQL
instance, with no automatic failover. The profile's 99.5% monthly readiness
target is also **UNQUALIFIED**; an external one-minute probe and retained
monthly results are required. The liveness check/restart is a process
supervision mechanism, not an availability commitment.

No numeric production capacity is qualified for organizations, nodes,
concurrent missions/operators, event ingestion, database size, or retention.
The former development-machine baseline is not production sizing guidance.
Use the [event growth report](#office-event-growth-report) for observation;
the measured envelope will be published through
[#52](https://github.com/agentux-os/agentux-command/issues/52). Follow
[#50](https://github.com/agentux-os/agentux-command/issues/50) for metrics and
alerting, [#51](https://github.com/agentux-os/agentux-command/issues/51) for
scheduled off-host backup and recurring restore verification,
[#54](https://github.com/agentux-os/agentux-command/issues/54) for staging
release qualification, and [#55](https://github.com/agentux-os/agentux-command/issues/55)
for the topology decision.

### Office event growth report

The event log keeps every event unless retention is turned on (below).
Collect a read-only baseline from a trusted Linux build host using the
same architecture as the Command container:

```sh
cargo build --release --bin agentux-command-storage-report
cp target/release/agentux-command-storage-report /tmp/agentux-command-storage-report
podman cp /tmp/agentux-command-storage-report agentux-command:/tmp/agentux-command-storage-report
podman exec agentux-command /tmp/agentux-command-storage-report > office-event-growth.json
podman exec agentux-command /tmp/agentux-command-storage-report --detailed > office-event-growth-detailed.json
podman exec agentux-command rm -f /tmp/agentux-command-storage-report
rm -f /tmp/agentux-command-storage-report
```

The summary reports database/schema, collection time, physical event-table and
partition sizes, and PostgreSQL's estimated row counts. `--detailed` additionally
scans event metadata to report exact receive-time bounds and counts by
organization; it never emits event payloads, but the output contains tenant
identifiers and should be handled as operationally sensitive data. Detailed
aggregation can take longer on a large log.

Capture reports periodically and compare total growth and partition skew,
and alert in your monitoring system before the volume's capacity.

### Event retention

**The policy: keep every event.** Nothing is ever deleted unless an operator
turns retention on, and then only after archiving:

```sh
# Once, by hand (a dry run first):
podman exec agentux-command agentux-command prune-events --older-than-days 365 --archive /var/lib/agentux-command/archive --dry-run
podman exec agentux-command agentux-command prune-events --older-than-days 365 --archive /var/lib/agentux-command/archive

# Or daily, from command.env (both are needed):
AGENTUX_COMMAND_EVENT_RETENTION_DAYS=365
AGENTUX_COMMAND_EVENT_ARCHIVE_DIR=/var/lib/agentux-command/archive
```

For each organization, in one transaction:

1. Events taken before the cutoff are written to
   `<archive>/<organization>/events-<from>-<to>.ndjson`, in the audit
   export's tamper-evident format (each line chained by sha256, with a final
   `digest`), and flushed to disk.
2. They are folded into the organization's **checkpoint**: its projection as
   of the last pruned event. `agentux-command check` and the projection
   verification then replay the checkpoint plus the remaining events, so a
   pruned log still verifies.
3. Only then are they deleted.

If the archive cannot be written, nothing is deleted. The cutoff must be at
least 30 days back. A node resending a pruned event is still refused by its
sequence cursor, which pruning never touches. The archives are listed in the
`office_event_archives` table (path, range, count, digest). Back them up with
the database and keep them as long as audits may ask. The log's replay API
says where it starts after pruning (`prunedThrough`).

**Why delete by time rather than drop time partitions.** The log is
hash-partitioned by organization (even spread, cheap per-organization
replay). Pruning deletes in batches by `received_at` under each
organization's projection lock, with no partitions to provision ahead, no
late-event edge at a partition boundary, and the same ordering guarantees as
appends. If deletion ever becomes the bottleneck, time sub-partitions can be
added behind the same `prune-events` without changing the policy.

Command applies embedded, versioned PostgreSQL migrations at startup. Event
append, per-node cursor advancement and projection updates share a transaction;
approval decisions and their organization-scoped idempotency records are
durable. Keep a verified database backup before upgrading the service image.
The bundle runs one Command and one PostgreSQL; it is not highly available.
