# AgentUX Command server bundle

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
   operators' secret store):

   ```sh
   podman exec agentux-command agentux-command token issue \
     --organization <org> --name admin \
     --capabilities auth.admin,organization.read,node.enroll,node.command,approval.decide
   ```

   Further tokens come from `POST /v1/service-tokens`. The demo tokens are
   refused unless `AGENTUX_COMMAND_INSECURE_DEMO_TOKENS=1` (development only).

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

### Backups

All of Command's state is in PostgreSQL: event log, projections,
definitions, runs, node state, enrollment, service tokens (hashed),
sessions and the event outbox. A database dump is a complete backup.

```sh
deploy/scripts/backup.sh /var/lib/agentux/command/backups
# /var/lib/agentux/command/backups/agentux-command-20261022T120000Z.dump (+ .sha256)
```

- **When:** at least daily, and always before an upgrade, from a systemd
  timer or cron. The interval is your recovery point objective: what an
  incident can lose.
- **Keep:** for example 7 daily, 4 weekly and 3 monthly dumps, plus the one
  taken before each upgrade until the next upgrade has proven itself.
- **Encrypt** before a dump leaves the host. It holds tenant data and token
  hashes. For example: `age -r <recipient> -o dump.age dump`, with the
  identity kept away from the backup storage.
- **Test** restores (below) on a schedule, not only when needed.

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
runs, nodes, tokens). It exits non-zero on a mismatch. To restore onto a new
host, bootstrap the bundle as above, start PostgreSQL only, restore, then
start Command.

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

### Recovery objectives

| | Objective | How |
|---|---|---|
| Recovery point | your backup interval (daily: up to 24 h of changes) | `backup.sh` on a timer; shorten the interval for a tighter RPO |
| Recovery time | minutes for a database of a few GiB: the restore drill restores and checks in seconds | `restore.sh` + `check`; measure yours on your data |
| Process failure | ~1 min | `/livez` + `HealthOnFailure=kill` + `Restart=on-failure` |

This bundle is a single host: no high availability. Capacity baseline for
development: 4 vCPU, 8 GiB RAM, 50 GiB of persistent storage; production
sizing follows event volume (see the growth report below) and node count.

### Office event growth report

The event log is retained indefinitely; no automatic pruning or archival is
enabled. Collect a read-only baseline from a trusted Linux build host using the
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

Capture reports periodically and compare total growth and partition skew. These
measurements are observations, not a retention recommendation. Set alerts in the
deployment's chosen monitoring system using its capacity and recovery objectives;
this project does not prescribe a universal threshold. Do not delete or detach
event partitions: replay and audit depend on the retained log. Define and review
an audit/replay policy before any future archival or pruning workflow.

Command applies embedded, versioned PostgreSQL migrations at startup. Event
append, per-node cursor advancement and projection updates share a transaction;
approval decisions and their organization-scoped idempotency records are
durable. Keep a verified database backup before upgrading the service image.
The bundle runs one Command and one PostgreSQL; it is not highly available.
