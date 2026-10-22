# AgentUX Command server bundle

This directory contains the optional `command-server` profile bundle. It is
container-only: PostgreSQL, Temporal, NATS, MinIO and the Command service run
as rootless Podman Quadlets. No host RPM installation or mutable `/usr` change
is required.

## Bootstrap on AgentUX OS

The OS profile manager creates `/var/lib/agentux/command`. An operator then:

1. copies `config/command.env.example` to
   `/var/lib/agentux/command/command.env` and replaces every placeholder;
2. creates the three Podman secrets named in the Quadlet files (the PostgreSQL
   password secret is shared with the Command service as `PGPASSWORD`);
3. copies the Quadlet files to `/etc/containers/systemd/`;
4. pins the Command image by digest in a local drop-in or release update;
5. runs `systemctl daemon-reload` and enables the units in dependency order.

Example secret setup (values must come from an operator-managed secret store):

```sh
printf '%s' "$POSTGRES_PASSWORD" | podman secret create agentux-command-postgres-password -
printf '%s' "$MINIO_ROOT_USER" | podman secret create agentux-command-minio-root-user -
printf '%s' "$MINIO_ROOT_PASSWORD" | podman secret create agentux-command-minio-root-password -
```

Do not commit the resulting env file, secrets or generated Quadlet drop-ins.

## Operations policy

- **Health:** each container has a health command; systemd restart policy makes
  a failed dependency visible in `systemctl --failed` and `podman healthcheck`.
  Command also checks PostgreSQL on `/healthz` and refuses startup if the
  database is unreachable or migrations fail.
- **Persistence:** PostgreSQL, NATS JetStream and MinIO use named volumes under
  the Command data boundary. Removing a profile never removes these volumes.
- **Backup:** before an upgrade, dump PostgreSQL with `pg_dump`, export the
  NATS stream configuration, and mirror MinIO to operator-controlled storage.
  Restore is tested into a clean host before a production promotion.
- **Upgrade:** pin image digests, apply one dependency at a time, verify all
  health checks and then migrate Command data. Keep the previous digest for
  rollback; never run a destructive migration without a tested backup.
- **Recovery:** restart the failed unit first. If storage is corrupt, stop the
  stack, restore volumes into a new named volume, validate `/healthz`, then
  re-enable the service. Incident evidence remains in the audit log.
- **Capacity:** the development baseline is 4 vCPU, 8 GiB RAM and 50 GiB
  persistent storage. Production sizing depends on event retention and node
  count; this bundle is not a high-availability deployment.

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
Authentication/session integration and live event streaming are still
development work; this bundle is not a complete production deployment.
