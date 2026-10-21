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

## Node telemetry

AgentUX nodes report themselves with the AgentUX node gateway (`aux node enroll`, then `aux node gateway`, in agentux-core). They POST AUXP messages to `/v1/protocol/messages`: `node.hello`, `node.heartbeat`, and events (`harness.status`, `harness.limits_changed`, `approval.requested`, `run.updated`, `run.completed`, `run.failed`, ...). `GET /v1/nodes` (operators with `organization.read`) lists each node with its version, last heartbeat and capacity, its agent CLIs (installed, signed in, version, spend, plan usage), its usage limits, its latest run states and its pending approvals.

Events are applied once per node sequence (duplicates are ignored), and a node Command does not know yet (after a restart) is registered by its first event. The node registry is in memory for now. With `AGENTUX_COMMAND_NODE_TOKENS=node-1=token,...` each node must send its own `Authorization: Bearer` token; without it, protocol messages are not authenticated (development only). Enrollment with issued tokens is tracked in [#3](https://github.com/agentux-os/agentux-command/issues/3) and [#7](https://github.com/agentux-os/agentux-command/issues/7).

## Node enrollment

Every Command server has an **instance id** (`GET /v1/instance`), created once and kept in its database. Nodes join one server, and only that one:

1. An operator with `node.enroll` issues tokens: `POST /v1/enrollment-tokens` with `{ "organizationId", "count"?, "nodeName"?, "ttlMinutes"?, "requireApproval"? }`. A token (`auxe1_<instanceId>_<secret>`) names this server, is single use and expires (24 hours by default, 30 days at most). A token for a `nodeName` is for that node only. Tokens are shown once; only their hashes are stored.
2. On the node, `aux node enroll <url>` reads the token and calls `POST /v1/enroll` (`{ token, nodeName?, hostname? }`). It gets the node's own credential (`auxn1_<secret>`), its node id (unique on this server: a suffix is added when a host name is taken, never someone else's id), its organization and the instance id, which the node pins.
3. Nodes authenticate with that credential, which is bound to their node id and organization. A token of another Command server is refused (409), as is a message for another organization. With `requireApproval`, a node may only say hello until an operator approves it (`POST /v1/nodes/<id>/approve`); `POST /v1/nodes/<id>/revoke` cuts a node off and closes its connection. `GET /v1/enrollments` lists every enrolled node.

So two Command servers in one institution never take each other's nodes: tokens and credentials only work on the server that issued them, and nodes refuse commands from any other instance. Nodes are never discovered or joined automatically. The command connection answers with `X-AgentUX-Command-Instance`, `node.hello` with the `instanceId`, and every command carries `commandInstanceId`.

**Fleets of VMs.** Add `"provision": { "profile": "node", "user": "dev", "policy"?, "preferences"? }` to the token request, and each token comes with a ready provision file (`version`, `profile`, `user`, `command.url`, `command.enrollmentToken`, ...) for `aux provision` (agentux-core `docs/provision.md`): give each VM its file through QEMU fw_cfg, an `AGENTUX` disk or the kernel command line, and it comes up enrolled. The URL in it is `AGENTUX_COMMAND_PUBLIC_URL`, or the address the request came to.

Static tokens (`AGENTUX_COMMAND_NODE_TOKENS`) still work. Nodes that are neither enrolled nor listed are refused, except on the in-memory demo server or with `AGENTUX_COMMAND_INSECURE_OPEN_NODES=1`.

## Node commands

Nodes keep a command connection open: the gateway dials `GET /v1/protocol/connect?nodeId=<id>` (WebSocket, with its enrollment credential (or a static token)). Operators send a node a command with `POST /v1/nodes/<nodeId>/commands` and `{ "method", "params", "commandId"? }`, with the `node.command` capability (`Bearer demo-leader` in the demo). Methods: `run.start`, `run.cancel`, `run.pause`, `run.resume`, `run.snapshot`, `request.resolve`, `bus.post`, `harness.list`, `harness.test`, `limits.clear`, `preferences.set`. Command fills in the envelope and a `commandId` when none is given, and answers with the node's `result` (200), its error (409 when the node holds the command for local confirmation or its policy denies it, 502 otherwise), 503 when the node is not connected, or 504 when it does not answer within 150 seconds. A command sent again with the same `commandId` gets the first answer. `GET /v1/nodes` says which nodes are `connected`.

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

