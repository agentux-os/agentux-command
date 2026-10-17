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

Architecture scaffold. The remote node protocol is intentionally not extracted into a separate repository until the first integration has stabilized.

