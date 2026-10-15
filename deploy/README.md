# AgentUX Command server bundle

This directory contains the optional `command-server` profile bundle. It is
container-only: PostgreSQL, Temporal, NATS, MinIO and the Command service run
as rootless Podman Quadlets. No host RPM installation or mutable `/usr` change
is required.

## Bootstrap on AgentUX OS

The OS profile manager creates `/var/lib/agentux/command`. An operator then:

1. copies `config/command.env.example` to
   `/var/lib/agentux/command/command.env` and replaces every placeholder;
2. creates the three Podman secrets named in the Quadlet files;
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

The initial Command HTTP service does not yet implement database migrations,
authentication or durable event replay. Until those are shipped, this bundle
is an operationally explicit preview and must not be advertised as production
ready.
