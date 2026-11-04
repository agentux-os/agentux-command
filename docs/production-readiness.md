# AgentUX Command production readiness profile

**Profile version:** 1.0
**Status:** Initial qualification target; not a statement of measured availability or capacity.

**Applies to:** the PostgreSQL-backed Command service and the single-host Podman bundle in `deploy/`.

This document defines the initial deployment profile and the evidence required
before a deployment or release can claim to meet it. Targets marked
**UNQUALIFIED** have no measured result yet. Do not infer an availability,
capacity, recovery, or retention guarantee from the target alone. Update this
version and its evidence links when the profile changes.

## Supported topology target

The initial target is **one host, one Command process, and one PostgreSQL
instance**, deployed with the rootless Podman Quadlets in `deploy/`. The Command
container and PostgreSQL volume are persistent across service restarts. The
bundle also includes NATS and an object store for future capabilities; Command
does not currently require either to serve requests or persist product state.
This topology has a single host and database failure domain. It has no
automatic failover and is not a highly available configuration.

Only this topology is in the initial profile. Multiple Command replicas,
PostgreSQL failover, multi-host scheduling, and multi-region operation are
outside the supported envelope until separately designed and qualified.
Issue [#55](https://github.com/agentux-os/agentux-command/issues/55) decides
whether and how that boundary changes after capacity results from #52.

## Assumptions and operator prerequisites

An operator using this profile is responsible for all of the following:

- **Network and TLS:** expose Command only through an operator-managed TLS
  terminator or an equivalent secure ingress. Use HTTPS for browser, node,
  and identity-provider traffic. Restrict direct access to the container port;
  configure forwarded headers only where the proxy is trusted.
- **Persistent storage:** keep PostgreSQL on persistent storage with free-space
  monitoring and a tested recovery path. Protect the host and volume from
  unauthorized access. The current event log is retained indefinitely by
  default, so storage growth has no qualified upper bound.
- **Backups:** schedule consistent database backups at least daily and before
  upgrades; encrypt them and copy them to a separately recoverable off-host
  destination. Include event archives if event retention is enabled. The
  repository currently supplies manual backup/restore scripts and a CI drill;
  a production-grade scheduled off-host workflow and recurring restore
  verification are not yet qualified ([#51](https://github.com/agentux-os/agentux-command/issues/51)).
- **Identity and secrets:** configure an operator-managed OIDC provider and
  role map for people. Keep database, OIDC, and node credentials in an
  operator-managed secret store, rotate them, and restrict access. Do not use
  demo tokens or unauthenticated node protocol mode.
- **Operations:** monitor `/readyz` from outside the host, retain service and
  database logs, alert on failed checks and storage pressure, and apply pinned
  image upgrades in a maintenance window after a verified backup. Rollback of
  a migrated database requires restoring the pre-upgrade backup.

These are prerequisites, not capabilities supplied by Command. In particular,
the present bundle does not provide external TLS termination, off-host backup
storage, backup scheduling, a metrics/alerting service, an identity provider,
or host/database failover.

## Command and operator responsibilities

| Area | Command provides | Operator infrastructure and procedure provide |
|---|---|---|
| Durable state | PostgreSQL-backed state; event append, cursor advancement, and projection update are transactional; duplicate event IDs are idempotent within their organization. | Persistent database volume, database access controls, monitoring, backup destination, backup schedule, encryption, and restore execution. |
| Process health | `/livez` for process liveness; `/readyz` and `/healthz` report PostgreSQL readiness; the supplied Quadlet restarts an unhealthy Command process. | External probes, alert routing, host/systemd/Podman availability, PostgreSQL health and recovery, and response to alerts. Readiness endpoints are not an availability SLO. |
| Authentication | OIDC callback/session handling and capability checks when configured; hashed service-token/session storage. | OIDC provider availability and policy, TLS, secret distribution/rotation, account lifecycle, and safe initial administrator bootstrap. |
| Events | Durable event log and replay; delivery to a configured event webhook is at least once. | Consumer idempotency, webhook availability, monitoring/alerting (metrics work is #50), storage sizing, and any retention/archive policy. |
| Backup and recovery | Manual `backup.sh`, `restore.sh`, integrity checksum, projection check command, and a CI restore drill. | Schedule, off-host and encrypted copies, retention, restore drills against production-sized data, replacement-host readiness, and achieved RPO/RTO. |
| Capacity and availability | No measured production capacity or availability claim in this profile. | Select workloads within subsequently qualified limits; provide host, network, storage, database, and operational resilience. |

## Objectives and qualification status

The following are measurable **targets**, not observed results. Until the
verification work has produced evidence for the target workload, each remains
**UNQUALIFIED** and must not be advertised as achieved.

| Measure | Initial target | Status | Owner and verification method |
|---|---|---|---|
| Deployment topology | One host, one Command process, one PostgreSQL instance. No automatic failover. | Defined by the bundle; end-to-end production behavior **UNQUALIFIED**. | Command team verifies the Quadlet dependency and storage configuration; staging deployment and failure scenarios in [#54](https://github.com/agentux-os/agentux-command/issues/54). Topology decision follow-up: [#55](https://github.com/agentux-os/agentux-command/issues/55). |
| Availability | At least **99.5% successful readiness** in each calendar month, measured by an external probe once per minute with a 5-second timeout. Success means `/readyz` returns HTTP 200 within that timeout. Planned downtime counts as unavailable. | **UNQUALIFIED**; no production observation history or alerting surface is established. | Shared: Command team defines probe semantics; operator runs an external monitor and retains monthly results. Metrics/alerting follow-up: [#50](https://github.com/agentux-os/agentux-command/issues/50). Validate service and dependency failure behavior in [#54](https://github.com/agentux-os/agentux-command/issues/54). |
| Recovery point (RPO) | At most **24 hours** of committed database state lost, conditional on successful backups at intervals no longer than 24 hours and a recoverable off-host copy. | **UNQUALIFIED**; the repository does not schedule or copy production backups off-host. | Operator schedules and monitors backups; Command team supplies the backup/restore interface. Verify copy age and restore integrity through [#51](https://github.com/agentux-os/agentux-command/issues/51). |
| Recovery time (RTO) | Restore service to `/readyz` 200 and pass `agentux-command check` within **8 hours** of declaring a host/database recovery incident. | **UNQUALIFIED**; no production-sized or replacement-host duration has been measured. | Operator owns infrastructure and incident timing; shared restore procedure. Repeated recovery and fault scenarios in [#51](https://github.com/agentux-os/agentux-command/issues/51) and [#54](https://github.com/agentux-os/agentux-command/issues/54). |
| Single-process restart | The supplied liveness check detects a nonresponsive process and asks systemd to restart it. No end-to-end recovery duration is claimed. | Mechanism present; end-to-end target **UNQUALIFIED**. | Command team verifies liveness behavior; staging restart scenario in [#54](https://github.com/agentux-os/agentux-command/issues/54). |

For a 30-day month, 99.5% corresponds to a maximum error budget of 3 hours,
36 minutes. This is arithmetic from the target, not measured availability.

## Capacity and retention envelope

There is currently **no qualified numeric production range** for the following
dimensions. The lower and upper bounds are intentionally left unclaimed; the
development machine baseline in older runbook text is not a production
capacity result.

| Dimension | Initial supported range | Qualification owner and method |
|---|---|---|
| Organizations per deployment | **UNQUALIFIED** (no numeric range) | Command team: benchmark tenant mix, state size, and concurrent reads/writes in [#52](https://github.com/agentux-os/agentux-command/issues/52). |
| Nodes per deployment | **UNQUALIFIED** (no numeric range) | Command team: benchmark heartbeats, connections, reconnects, and event load in [#52](https://github.com/agentux-os/agentux-command/issues/52); exercise real gateway behavior in [#54](https://github.com/agentux-os/agentux-command/issues/54). |
| Concurrent missions and operators | **UNQUALIFIED** (no numeric range) | Command team: benchmark transitions, API reads, and live streams in [#52](https://github.com/agentux-os/agentux-command/issues/52). |
| Event ingestion | **UNQUALIFIED** (no sustained or burst rate) | Command team: report throughput, p50/p95/p99 latency, errors, resource use, and database growth in [#52](https://github.com/agentux-os/agentux-command/issues/52). |
| Database and event-log size | **UNQUALIFIED** (no maximum size or growth rate) | Operator measures its workload with the storage report; Command team establishes tested thresholds and overload behavior in [#52](https://github.com/agentux-os/agentux-command/issues/52). Alerting examples and signals are tracked in [#50](https://github.com/agentux-os/agentux-command/issues/50). |
| Event retention | Indefinite by default; no qualified duration or maximum retained volume. Optional pruning requires operator-selected age and archive destination. | Operator sets and backs up policy/archive; Command team verifies correctness and growth characteristics. Use the event-growth report; capacity tests in [#52](https://github.com/agentux-os/agentux-command/issues/52). |

Issue [#52](https://github.com/agentux-os/agentux-command/issues/52) owns
repeatable load benchmarks and publishes measured limits. Results must record
hardware, workload, throughput, p50/p95/p99 latency, CPU/memory, database
growth, error rate, and overload behavior. A release can claim only the ranges
covered by retained benchmark evidence. Issue
[#54](https://github.com/agentux-os/agentux-command/issues/54) qualifies the
release with a deployed image, a real node gateway, and controlled failures.
Issue [#55](https://github.com/agentux-os/agentux-command/issues/55) uses those
results to decide the future topology.

## Release qualification checklist

A release is qualified against profile 1.0 only when its release record links
evidence for each applicable item below. A missing result is a **fail / not
qualified**, not an assumed pass.

- [ ] State the exact Command image digest, host/OS, CPU, memory, disk, and
  PostgreSQL version; confirm the one-host/one-process topology.
- [ ] Run the workload benchmark from #52 and publish the measured envelope;
  mark every untested organizations, nodes, concurrency, event-rate, and size
  range unqualified.
- [ ] Complete the staging workflow and fault scenarios in #54; retain results
  for node reconnect, Command restart, delayed/duplicate events, database
  outage/recovery, and upgrade/restore.
- [ ] Record a monthly external `/readyz` measurement against the 99.5% target;
  report the achieved value and probe window. Until the observation period
  exists, availability remains unqualified.
- [ ] Demonstrate a backup no older than 24 hours at recovery start, with an
  encrypted off-host copy, and record the observed RPO. Until the automated
  workflow and recurring verification in #51 are complete, the profile's RPO
  target remains unqualified.
- [ ] Restore to a fresh or replacement database/host, pass `agentux-command
  check`, and record elapsed time against the 8-hour RTO target.
- [ ] Confirm external TLS, OIDC, secret handling, persistent storage,
  monitoring, storage alerting, and the operator's tested restore procedure.
- [ ] Record event-retention policy and archive backup procedure; do not claim
  a storage limit or retention duration without benchmark evidence.

An operator deployment can use this profile as its checklist, but only the
evidence-backed ranges and objectives may be described as qualified. Submit
changes to target values or topology as a versioned profile update with the
new benchmark and operational evidence.
