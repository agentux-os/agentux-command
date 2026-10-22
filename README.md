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

- Rust HTTP/WebSocket service (axum, tokio-postgres)
- PostgreSQL for remote product state, and for durable organization-level
  workflows: explicit state machines with a transactional outbox
  ([ADR 0020](https://github.com/agentux-os/agentux/blob/main/docs/adr/0020-command-workflows-without-temporal.md))
- NATS for delivery only: waking workers, live views, routing commands between replicas
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

Events are applied once per node sequence (duplicates are ignored), and a node Command does not know yet (after a restart) is registered by its first event. The registry is kept in PostgreSQL behind every change and read back at startup, so a restarted Command still knows its nodes; each node is listed with a `presence` (`online` when heard from within 90 seconds, `degraded` within 5 minutes, else `offline`). With `AGENTUX_COMMAND_NODE_TOKENS=node-1=token,...` each node must send its own `Authorization: Bearer` token; without it, protocol messages are not authenticated (development only). Enrollment with issued tokens is tracked in [#3](https://github.com/agentux-os/agentux-command/issues/3) and [#7](https://github.com/agentux-os/agentux-command/issues/7).

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

## Operator access

Every operator request resolves to a principal holding capabilities **per
organization**: `organization.read`, `organization.edit`,
`organization.publish`, `node.command`, `node.enroll`, `approval.decide` and
`auth.admin` (ADR 0015's vocabulary and how it maps:
[ADR 0015, Acceptance](https://github.com/agentux-os/agentux/blob/main/docs/adr/0015-organization-command-office-contract.md#acceptance)). A route acts on a record (a node, an
enrollment, an approval) only with the capability in **that record's
organization**; another organization's nodes and enrollments are not listed,
and are answered as unknown. No credential, or one Command does not accept,
is 401; a missing capability is 403. Without credentials every operator route
fails closed; `/healthz` and `/v1/instance` stay public and carry no tenant
data.

**Service tokens** (`auxs1_…`) are for automation and the first
administrator. Only their hashes are stored; the secret is shown once.

```sh
# The first administrator, on the server (straight in the database):
podman exec agentux-command agentux-command token issue \
  --organization acme --name admin \
  --capabilities auth.admin,organization.read,node.enroll,node.command,approval.decide

# Then, with auth.admin in that organization:
POST /v1/service-tokens        { "name", "organizationId", "capabilities": [...], "days"? }
GET  /v1/service-tokens?organizationId=acme
POST /v1/service-tokens/<id>/revoke
```

**Signing people in (OIDC).** With an OpenID Connect provider configured
(Ferriskey, Keycloak, Entra ID, Google, or GitHub through a provider that
brokers it), `GET /v1/auth/login?returnTo=/console` sends the browser to the
provider (authorization code with PKCE S256 and a nonce), and
`/v1/auth/callback` accepts the ID token only when its signature checks
against the provider's published keys and its issuer, audience, expiry and
nonce are right. The token's groups map to capabilities per organization
through a role map; a person whose groups map to nothing is not signed in
(403). The session is an `HttpOnly`, `SameSite=Lax` cookie (`Secure` over
HTTPS) lasting 8 hours, stored hashed in PostgreSQL; group changes at the
provider apply at the next sign-in. A state-changing request carried by the
cookie alone must come from this server's own origin (CSRF). `GET
/v1/auth/me` says who is signed in and what they may do; `POST
/v1/auth/logout` ends the session.

```sh
AGENTUX_COMMAND_PUBLIC_URL=https://command.acme.org      # the callback is <this>/v1/auth/callback
AGENTUX_COMMAND_OIDC_ISSUER=https://id.acme.org/realms/acme
AGENTUX_COMMAND_OIDC_CLIENT_ID=agentux-command
AGENTUX_COMMAND_OIDC_CLIENT_SECRET=…                     # from a Podman secret
AGENTUX_COMMAND_OIDC_ROLES=/var/lib/agentux/command/roles.json
AGENTUX_COMMAND_OIDC_GROUPS_CLAIM=groups                 # default
AGENTUX_COMMAND_OIDC_SCOPES="openid profile email groups"  # default
```

```json
{ "groups": {
    "agentux-leads":    { "acme": ["organization.read", "node.command", "node.enroll", "approval.decide"] },
    "agentux-admins":   { "acme": ["auth.admin", "organization.read"] },
    "agentux-auditors": { "acme": ["organization.read"], "beta": ["organization.read"] } } }
```
The demo tokens (`demo-leader`: every capability but `auth.admin` in every
organization; `demo-observer`: `organization.read`) work on the in-memory demo
server, and on a real one only with `AGENTUX_COMMAND_INSECURE_DEMO_TOKENS=1`.

## Organization definitions

An organization's areas, roles, missions, workflows, gates, agents and nodes
are one versioned document, the
[organization definition v1 contract](https://github.com/agentux-os/agentux/blob/main/docs/contracts/organization-definition-v1.md)
(ADR 0015), kept field for field; fields this version does not know are kept.

```text
GET  /v1/organizations/<org>/definitions                    organization.read  (versions, newest first)
POST /v1/organizations/<org>/definitions                    organization.edit  (a new draft)
GET  /v1/organizations/<org>/definitions/<version>          organization.read
PUT  /v1/organizations/<org>/definitions/<version>          organization.edit  (a draft only; 409 once published)
POST /v1/organizations/<org>/definitions/<version>/publish  organization.publish
GET  /v1/organizations/<org>/audit                          organization.read  (drafted, replaced, published; actor and time)
```

A draft is validated every time it is saved and keeps its problems (missing
or duplicate ids, references to areas, roles, missions, gates or steps that do
not exist, a step going outside its workflow, unknown permissions, a
`human-required` gate no human role can pass, a `schema_version` other than
1.x). Only a draft without problems is published (422 otherwise); a published
version never changes again, and the next change is a new draft with the next
number. Publishing records `organization.definition.published` in the Office
event log, and the Office projection takes the organization, its areas,
missions and agents, and the definition version from it.

## Office projection slice

The current gateway exposes a storage-independent first slice for AgentUX Office:

```text
GET  /v1/office/projection?organizationId=org-demo
POST /v1/approvals/{approvalId}/resolve
```

Operators authenticate as described in [Operator access](#operator-access);
the demo server also accepts `Bearer demo-leader` and `Bearer demo-observer`.

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

Build and run the tests locally (Rust stable; the PostgreSQL tests run when
`AGENTUX_COMMAND_TEST_DATABASE_URL` points at a test database, and are
skipped otherwise):

```sh
cargo build --release      # target/release/agentux-command, agentux-command-storage-report
cargo test
cargo clippy --all-targets -- -D warnings
```

The image the Quadlet runs, `ghcr.io/agentux-os/agentux-command`, is built
from `Containerfile` for amd64 and arm64 by `.github/workflows/image.yml`:
`:main` and `:sha-<12>` from `main`, `:X.Y.Z` and `:latest` from a `vX.Y.Z`
tag that matches `Cargo.toml`. Locally: `podman build -t agentux-command -f Containerfile .`

**Releases.** A pull request bumps `version` in `Cargo.toml`, `Cargo.lock` and
the image the Quadlet pins (`deploy/quadlet/agentux-command.container`); CI
checks they agree. Pushing the matching `vX.Y.Z` tag publishes the image
`:X.Y.Z` and the GitHub release with both binaries for x86_64 and aarch64, the
deploy bundle and the migrations (`.github/workflows/release.yml`).


