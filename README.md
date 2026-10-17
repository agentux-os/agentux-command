# AgentUX Command

<p align="center">
  <a href="https://agentuxos.org">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/agentux-os/agentux/main/brand/svg/lockup-dark.svg">
      <img alt="AgentUX" src="https://raw.githubusercontent.com/agentux-os/agentux/main/brand/svg/lockup-light.svg" width="320">
    </picture>
  </a>
</p>

<p>
  <a href="https://github.com/agentux-os/agentux-command/commits/main"><img alt="Last commit" src="https://img.shields.io/github/last-commit/agentux-os/agentux-command?label=updated&labelColor=0b0d10&color=4f7d0b"></a>
  <a href="https://github.com/agentux-os/agentux-command"><img alt="Top language" src="https://img.shields.io/github/languages/top/agentux-os/agentux-command?label=language&labelColor=0b0d10&color=4f7d0b"></a>
  <a href="https://github.com/agentux-os/agentux-protocol"><img alt="Protocol" src="https://img.shields.io/badge/protocol-AUXP%20v1-4f7d0b?labelColor=0b0d10"></a>
</p>

AgentUX Command is the remote control plane for organizations that run work on AgentUX nodes.

It models organizations, offices, missions, agents, policies and operations. It gives leadership a remote command center while AgentUX Core remains the local execution authority for worktrees, ACP sessions, permissions, gates and pull requests.

## Product boundary

```text
AgentUX Command
  organizations · missions · governance · scheduling · telemetry
                         │
                         ▼
AgentUX nodes
  agentuxd · ACP · worktrees · approvals · technical execution
```

Command must not manipulate a node's SQLite database or local Unix socket directly. The future node gateway will expose a versioned, authenticated contract for registration, capabilities, commands and events.

That contract is defined in [AgentUX Protocol](https://github.com/agentux-os/agentux-protocol).

## Initial stack

- Go HTTP/WebSocket service
- PostgreSQL for remote product state
- Temporal for durable organization-level workflows
- NATS for internal event distribution
- AgentUX node gateway for remote execution

## Planned capabilities

- Organizations, offices, missions and agent rosters
- Autonomy policies and human approval gates
- Node registration, health and capability-aware placement
- Aggregated run events, incidents, cost and telemetry
- Remote leadership views and intervention controls

## Non-goals

- Replacing `agentux-core`'s local execution engine
- Owning worktrees or ACP sessions directly
- Making the 2D Office view the source of truth
- Requiring AgentUX OS for every remote node

## Status

The repository is under active integration development. The current gateway surface is always represented by `main`; versioned remote compatibility belongs to [AgentUX Protocol](https://github.com/agentux-os/agentux-protocol), so this README does not carry release-specific version numbers.

## Office projection slice

The current gateway exposes a storage-independent first slice for AgentUX Office:

```text
GET  /v1/office/projection?organizationId=org-demo
POST /v1/approvals/{approvalId}/resolve
```

The demo server accepts `Bearer demo-leader` (`organization.read` and
`approval.decide`) and `Bearer demo-observer` (`organization.read`). Production
authentication and organization sessions are tracked in [#13](https://github.com/agentux-os/agentux-command/issues/13).

Projection responses carry `schemaVersion`, `definitionVersion`, `projectedAt`,
`source`, `freshness`, `lastEventId` and event replay metadata. Events use
organization/entity IDs, `eventId`, `nodeId` and a monotonic per-node sequence.
Both the in-memory and PostgreSQL stores deduplicate events before updating the
materialized read model. Approval mutations require an idempotency key and replay
the original decision on retry.

The local demo uses deterministic in-memory data. The deployed server requires
PostgreSQL and stores the append-only event log, per-node replay cursors,
materialized projection and organization-scoped approval idempotency records.
Event insertion, cursor advancement and projection update commit atomically.
The event table is hash-partitioned by organization and indexed by organization
and receive time for time-window replay and retention queries.
Operators can collect read-only partition-size and growth diagnostics with the
standalone `agentux-command-storage-report` command; detailed per-organization
aggregates are opt-in. See [the operations runbook](deploy/README.md#office-event-growth-report).
Events are retained indefinitely by default; no automated pruning or archival
is enabled.
Approval decisions are also appended to the event log, so the read model can be
rebuilt and checked against replay after restart. The Office JSON contract stays
independent of the storage implementation. Live updates depend on authenticated
organization sessions ([#13](https://github.com/agentux-os/agentux-command/issues/13)) and remain tracked in [#14](https://github.com/agentux-os/agentux-command/issues/14).

Production startup requires `AGENTUX_COMMAND_DATABASE_URL`, connects to the
database, applies versioned migrations and only then listens for requests.
`/healthz` checks database connectivity. The command-server Quadlet receives
the PostgreSQL password through the Podman secret; the database URL does not
contain the password.

Run the tests locally:

```sh
go test ./...
```

