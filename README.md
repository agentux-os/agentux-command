# AgentUX Command

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

The first executable gateway is available in `cmd/agentux-command`. It
exposes `/healthz`, a read-only `/v1/nodes` view and accepts validated AUXP v1
messages at `/v1/protocol/messages`. Node state is currently in memory; the
next increment moves it to PostgreSQL and adds authenticated outbound node
sessions.

Run it locally:

```sh
go run ./cmd/agentux-command
curl http://localhost:8080/healthz
```

This is an integration slice, not production deployment: authentication,
WebSocket/TLS transport, durable storage and event replay remain tracked in
the Command backlog.

The optional server bundle is documented in [`deploy/`](deploy/). It packages
PostgreSQL, Temporal, NATS, MinIO and Command as rootless Podman Quadlets with
health checks, persistent volumes and an explicit backup/upgrade policy. It is
an operational preview until durable storage, authentication and event replay
are implemented.

