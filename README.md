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

Command must not manipulate a node's SQLite database or local Unix socket directly. The node gateway is implemented in agentux-core and connects through the versioned, authenticated AUXP contract for enrollment, capabilities, commands and events.

That contract is defined in [AgentUX Protocol](https://github.com/agentux-os/agentux-protocol).

## Initial stack

- Rust HTTP/WebSocket service (axum, tokio-postgres)
- PostgreSQL for remote product state, and for durable organization-level
  workflows: explicit state machines with a transactional outbox
  ([ADR 0020](https://github.com/agentux-os/agentux/blob/main/docs/adr/0020-command-workflows-without-temporal.md))
- AUXP over HTTP and WebSocket for authenticated node enrollment, telemetry and remote commands
- The deployment bundle also includes NATS and an object store for future integrations; the current Command service does not depend on them

## Implemented capabilities

- Versioned organization definitions with validation, review, publication and audit history
- Durable mission runs with agent, approval-gate and recorded-action steps; pause, resume, cancel and recovery after restart
- Node enrollment, approval/revocation, health and harness telemetry, plus authenticated remote commands
- Organization-scoped event history, replay, live streams, audit export and Office projections for runs, approvals, artifacts and node status
- OIDC operator sessions and capability-checked Console and Office access

## Planned capabilities

- **Capability-aware placement:** runs may name a node; otherwise Command currently picks the lexicographically first online, connected node in the organization. It does not yet match mission requirements to advertised node capabilities. See [#67](https://github.com/agentux-os/agentux-command/issues/67).
- **Aggregated incident and cost analytics:** node events can carry run-level cost and incident data, but Command does not yet provide cross-run or organization-level analytics and reporting. See [#68](https://github.com/agentux-os/agentux-command/issues/68).
- **Scheduled and recurring missions** ([#58](https://github.com/agentux-os/agentux-command/issues/58)), **approval decisions through messaging channels** ([#59](https://github.com/agentux-os/agentux-command/issues/59)), and **organization-shared tool catalogs and policies** ([#57](https://github.com/agentux-os/agentux-command/issues/57)).

## Non-goals

- Replacing `agentux-core`'s local execution engine
- Owning worktrees or ACP sessions directly
- Making the 2D Office view the source of truth
- Requiring AgentUX OS for every remote node

## Status

The implementation on `main` is the source of truth for current Command and gateway behavior. Versioned remote compatibility belongs to [AgentUX Protocol](https://github.com/agentux-os/agentux-protocol); this README does not assign release-specific protocol versions. Production availability, capacity and recovery remain unqualified until the evidence in the [production profile](docs/production-readiness.md) is collected.

## Production profile

The initial production-readiness target is a single host running one Command
process and one PostgreSQL instance with the Podman bundle. It is not highly
available. Availability, recovery objectives, and numeric capacity ranges are
qualification targets; unmeasured values remain unqualified. See the versioned
[production readiness profile](docs/production-readiness.md) and its
[deployment runbook](deploy/README.md). This repository makes no broader
production availability or capacity claim.

## Node telemetry

AgentUX nodes report themselves with the AgentUX node gateway (`aux node enroll`, then `aux node gateway`, in agentux-core). They POST AUXP messages to `/v1/protocol/messages`: `node.hello`, `node.heartbeat`, and events (`harness.status`, `harness.limits_changed`, `approval.requested`, `run.updated`, `run.completed`, `run.failed`, ...). `GET /v1/nodes` (operators with `organization.read`) lists each node with its version, last heartbeat and capacity, its agent CLIs (installed, signed in, version, spend, plan usage), its usage limits, its latest run states and its pending approvals.

Events are applied once per node sequence (duplicates are ignored), and an enrolled node not yet known after a restart is registered by its first event. The registry is kept in PostgreSQL behind every change and read back at startup, so a restarted Command still knows its nodes; each node is listed with a `presence` (`online` when heard from within 90 seconds, `degraded` within 5 minutes, else `offline`). Enrolled nodes use their issued credential; static `AGENTUX_COMMAND_NODE_TOKENS=node-1=token,...` entries remain a compatibility option. The PostgreSQL production handler refuses unauthenticated nodes.

## Node enrollment

Every Command server has an **instance id** (`GET /v1/instance`), created once and kept in its database. Nodes join one server, and only that one:

1. An operator with `node.enroll` issues tokens: `POST /v1/enrollment-tokens` with `{ "organizationId", "count"?, "nodeName"?, "ttlMinutes"?, "requireApproval"? }`. A token (`auxe1_<instanceId>_<secret>`) names this server, is single use and expires (24 hours by default, 30 days at most). A token for a `nodeName` is for that node only. Tokens are shown once; only their hashes are stored.
2. On the node, `aux node enroll <url>` reads the token and calls `POST /v1/enroll` (`{ token, nodeName?, hostname? }`). It gets the node's own credential (`auxn1_<secret>`), its node id (unique on this server: a suffix is added when a host name is taken, never someone else's id), its organization and the instance id, which the node pins.
3. Nodes authenticate with that credential, which is bound to their node id and organization. A token of another Command server is refused (409), as is a message for another organization. With `requireApproval`, a node may only say hello until an operator approves it (`POST /v1/nodes/<id>/approve`); `POST /v1/nodes/<id>/revoke` cuts a node off and closes its connection. `GET /v1/enrollments` lists every enrolled node.

So two Command servers in one institution never take each other's nodes: tokens and credentials only work on the server that issued them, and nodes refuse commands from any other instance. Nodes are never discovered or joined automatically. The command connection answers with `X-AgentUX-Command-Instance`, `node.hello` with the `instanceId`, and every command carries `commandInstanceId`.

**Fleets of VMs.** Add `"provision": { "profile": "node", "user": "dev", "policy"?, "preferences"? }` to the token request, and each token comes with a ready provision file (`version`, `profile`, `user`, `command.url`, `command.enrollmentToken`, ...) for `aux provision` (agentux-core `docs/provision.md`): give each VM its file through QEMU fw_cfg, an `AGENTUX` disk or the kernel command line, and it comes up enrolled. The URL in it is `AGENTUX_COMMAND_PUBLIC_URL`, or the address the request came to.

Static tokens (`AGENTUX_COMMAND_NODE_TOKENS`) still work. Nodes that are neither enrolled nor listed are refused, except on the in-memory demo server or in the explicitly constrained local development mode described below.

## Node commands

Nodes keep a command connection open: the gateway dials `GET /v1/protocol/connect?nodeId=<id>` (WebSocket, with its enrollment credential (or a static token)). Operators send a node a command with `POST /v1/nodes/<nodeId>/commands` and `{ "method", "params", "commandId"? }`, with the `node.command` capability (`Bearer demo-leader` in the demo). Methods: `run.start`, `run.cancel`, `run.pause`, `run.resume`, `run.snapshot`, `request.resolve`, `bus.post`, `harness.list`, `harness.test`, `limits.clear`, `preferences.set`. Command fills in the envelope and a `commandId` when none is given, and answers with the node's `result` (200), its error (409 when the node holds the command for local confirmation or its policy denies it, 502 otherwise), 503 when the node is not connected, or 504 when it does not answer within 150 seconds. A command sent again with the same `commandId` gets the first answer. `GET /v1/nodes` says which nodes are `connected`.

## Operator access

Every operator request resolves to a principal holding capabilities **per
organization**: `organization.read`, `organization.edit`,
`organization.publish`, `node.command`, `node.enroll`, `approval.decide`,
`audit.read` and `auth.admin` (ADR 0015's vocabulary and how it maps:
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
brokers it), `GET /v1/auth/login?returnTo=/console/` sends the browser to the
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
server. The PostgreSQL server refuses the insecure demo switches unless
`AGENTUX_COMMAND_DEVELOPMENT_MODE=1` is also set, it binds to a literal
loopback address, and neither a public URL nor OIDC is configured. Never use
that development mode in a deployed server.

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
POST /v1/organizations/<org>/definitions?from=<version>     organization.edit  (a new draft from a version: the next change, or a rollback)
POST /v1/organizations/<org>/definitions/<version>/reviews  approval.decide    {"decision": "approved" | "changes_requested", "comment"}
POST /v1/organizations/<org>/definitions/<version>/publish  organization.publish
GET  /v1/organizations/<org>/audit                          organization.read  (drafted, replaced, published; actor and time)
```

A draft is validated every time it is saved and keeps its problems (missing
or duplicate ids, references to areas, roles, missions, gates or steps that do
not exist, a step going outside its workflow, unknown permissions, a
`human-required` gate no human role can pass, a `schema_version` other than
1.x). Only a draft without problems is published (422 otherwise); a published
version never changes again, and the next change is a new draft with the next
number.

Editors work on revisions: a draft's `revision` goes up with every save, and
responses carry it as `ETag`. A save or publication sent with `If-Match:
"<revision>"` is refused with 409 `stale_draft` (and `currentRevision`) if
someone saved the draft since; nobody overwrites changes they did not see.
Reviews approve a revision or ask for changes, and nobody reviews their own
edits (403). An organization whose definition says `"publication_policy":
"review-required"` publishes only a revision someone other than its editor
approved (409 `review_required` otherwise; a later save needs a new approval).
Two publications of the same draft: the second gets 409. Publishing records `organization.definition.published` in the Office
event log, and the Office projection takes the organization, its areas,
missions and agents, and the definition version from it.

## The whole workflow, end to end

`tests/end_to_end.rs` runs an organization from definition to finished
mission with nothing stubbed but the agents, and no model credentials. It
needs Linux or macOS and git, and runs in CI on every change:

```sh
cargo test --test end_to_end
```

1. A leader drafts the organization. Validation catches a broken step, the
   leader fixes it and publishes version 1.
2. A node (agentux-core's gateway in front of an agentuxd with scripted
   agents and a git project) enrolls with a token from this Command, pins it,
   connects and advertises its capabilities.
3. A mission run starts. Its agent step goes to the node as `run.start`.
4. The node's own run asks to approve its plan. The approval appears in the
   Office projection, a leader decides it, and the decision goes back to the
   node (`request.resolve`).
5. The node's run completes, and the mission's human-required gate asks for
   approval. A service token is refused (403), a person approves, and the
   action step finishes the mission.
6. The event log holds every step (definition, run, node and approval events),
   each with its actor and `definitionVersion`, and the Office projection
   shows the mission run and the node's run as completed, with the pull
   request the node opened as the run's artifact.

To try it by hand, run Command with demo tokens and Office against it:

```sh
AGENTUX_COMMAND_DATABASE_URL=postgres://... AGENTUX_COMMAND_ADDR=127.0.0.1:8080 \
  AGENTUX_COMMAND_DEVELOPMENT_MODE=1 AGENTUX_COMMAND_INSECURE_DEMO_TOKENS=1 cargo run
# in agentux-apps:
COMMAND_URL=http://127.0.0.1:8080 COMMAND_TOKEN=demo-leader COMMAND_ORGANIZATION=acme npm run serve
```

Then edit and publish the organization in Office's editor
(`http://127.0.0.1:4173/editor.html`), enroll an AgentUX machine
(`aux node enroll <token>`, with a token from `POST /v1/enrollment-tokens`),
start a mission (`POST /v1/organizations/acme/runs`) and follow it on the
floor.

## Missions

A mission runs as its workflow from the organization's newest published
definition, and stays pinned to that version
([ADR 0020](https://github.com/agentux-os/agentux/blob/main/docs/adr/0020-command-workflows-without-temporal.md):
a state machine in PostgreSQL, no Temporal).

```text
POST /v1/organizations/<org>/runs                    node.command        {"missionId", "repository", "prompt", "title"?, "nodeId"?}
GET  /v1/organizations/<org>/runs                    organization.read   (newest first)
GET  /v1/organizations/<org>/runs/<run>              organization.read
POST /v1/organizations/<org>/runs/<run>/pause        node.command
POST /v1/organizations/<org>/runs/<run>/resume       node.command        (a failed run retries its step)
POST /v1/organizations/<org>/runs/<run>/cancel       node.command
```

- **agent** steps go to a node of the organization (the run's `nodeId`, or
  one that is connected and online) as AUXP `run.start`, with command id
  `<run>:<step>:<attempt>`, so a resend after a crash is the same command.
  The node's `run.completed` or `run.failed` ends the step. If Command
  missed those, it asks the node (`run.snapshot`) every minute. A step
  running longer than 24 h fails.
- **gate** steps request an approval in the Office (`approval.requested`, id
  `<run>-<step>-<attempt>`). A `human-required` gate is decided by a person
  (a signed-in operator) with `approval.decide`; a service token gets 403.
  Rejected, the run fails; resumed, the gate asks again.
- **action** steps are recorded as done (Command does not execute them).
- **Held at the node**: a node's policy may hold `run.start` for someone there
  to confirm (the default). The run waits (`held`) without retrying, and the
  node's `command.completed` starts the step, or fails it if it was rejected.
- **No node**: the run `waiting` with the reason, and it tries again every 30 s.
  **A node refusing** (its policy) or failing the step: the run `failed`,
  and resume retries the step as a new attempt.
- Every transition is saved with a revision check (two Command instances
  cannot both move a run) and goes into the event log as `mission.run.*`,
  with its actor and `definitionVersion`; the log is the run's audit trail.
  The Office projection shows each run with `kind: "mission"`. Due runs move
  on their own every 2 s, including after a restart.

## Events

Every event Command takes (a node's reports, an approval decision, a
published definition) goes into the organization's event log in the same
transaction as the state it changes, deduplicated by `eventId`. Events carry
ADR 0015's envelope: the organization, `correlationId`, `actor` (`{"kind":
"user" | "service" | "operator" | "node", "id": …}`; a node's own events are
the node's) and, once the organization has a published definition,
`definitionVersion`.

```text
GET /v1/organizations/<org>/events?after=<cursor>&since=<time>&until=<time>&limit=<n>   organization.read
```

replays the log oldest first: after a cursor, within a time range (RFC 3339,
`since` inclusive, `until` exclusive), at most `limit` (100, up to 1000).
Each entry is `{"cursor": n, "event": {...}}`; `next` is the cursor to go on
from.

```text
GET /v1/organizations/<org>/events/stream?after=<cursor>   organization.read   (text/event-stream)
```

is the same log, live: what comes after the cursor (`after`, or the
`Last-Event-ID` a reconnecting `EventSource` sends by itself), then each event
as Command takes it. Each message is one event (`id:` its cursor,
`event: office.event`, `data:` the event). Delivery is at least once: dedupe
by `eventId`. `event: unavailable` means the log cannot be read at the moment;
the stream keeps trying. A slow reader is sent only as fast as it reads, and a
comment every 15 s keeps proxies from closing the stream. Signed-in operators'
browsers authenticate with their session cookie, which `EventSource` sends.
With event retention disabled, cursors remain replayable because the log is
kept. When retention is enabled, older events are archived and pruned from the
live event table; API replay covers retained events, while older history must
be recovered from the operator-managed archive ([Event retention](deploy/README.md#event-retention)).

With `AGENTUX_COMMAND_EVENTS_URL` set, Command also POSTs each event as JSON
to that URL (`Idempotency-Key: <eventId>`, and `Authorization: Bearer
<AGENTUX_COMMAND_EVENTS_TOKEN>` when that is set), from an outbox written in
the same transaction (ADR 0020): at least once, in order per organization (an
organization's next event waits while an earlier one fails), retried with a
backoff from 5 s to 10 min that survives restarts. Delivered events leave the
outbox after a day; one the sink has not taken in 7 days is dropped from it,
and stays in the event log for replay until event retention prunes it.

## Audit

The organization's event log is its audit trail. Besides what nodes report
and what runs do, it records, with the operator as `actor`:

| Event | When |
|---|---|
| `organization.definition.drafted`, `.replaced`, `.published` | a definition changes |
| `approval.resolved` | someone decides an approval (the decision, the reason) |
| `mission.run.*` | a run starts, is paused, resumed or cancelled (and every step it takes) |
| `node.command.sent` | an operator sends a node a command: the method, the command id and the outcome; never its params |
| `node.enrollment_tokens.issued`, `node.approved`, `node.revoked` | nodes join or leave |
| `auth.service_token.issued`, `.revoked` | service tokens change (never their secrets) |
| `audit.exported` | someone exports the trail |

```text
GET /v1/organizations/<org>/audit/export?since=<time>&until=<time>   audit.read   (application/x-ndjson)
```

exports it, oldest first. Each line is `{"cursor", "event", "prev", "hash"}`,
where `hash` is the hex sha256 of `prev`, a newline, and the line's compact
JSON without `hash`. `prev` is the line before's `hash`, empty for the first.
The last line is `{"organizationId", "count", "digest", "since", "until",
"exportedAt"}`, where `digest` is the last hash. Keep the digest apart from the
file: a line changed, dropped or reordered no longer chains to it. Free text
in events (`prompt`, `detail`, `message`, `body`, `answer`, `reason`) is
exported as `[redacted]`. One export carries at most 100,000 events
(`"truncated": true` beyond that), so export by time range.

To check a file in Python:

```python
import hashlib, json
lines = [json.loads(l) for l in open("audit.ndjson")]
prev = ""
for record in lines[:-1]:
    h = record.pop("hash")
    assert record["prev"] == prev
    assert h == hashlib.sha256((prev + "\n" + json.dumps(record, separators=(",", ":"), ensure_ascii=False)).encode()).hexdigest()
    prev = h
assert lines[-1]["digest"] == prev
```

## Office projection

**AgentUX Office is served at `/office/`** when `AGENTUX_COMMAND_OFFICE_DIR`
points at its versioned bundle (the image pins it by version and checksum).
The published image also pins [Console 0.1.1](https://github.com/agentux-os/agentux-apps/releases/tag/console-v0.1.1)
and serves it at `/console/` via `AGENTUX_COMMAND_CONSOLE_DIR`. Both pages share
Command's origin and OIDC session. Their browser requests use the operator's
`HttpOnly` session cookie; each API route checks that operator's organization
capabilities. No shared service token is placed in browser configuration or
forwarded on visitors' behalf. The Office config includes only the selected
organization and whether sign-in is configured. The Console config contains
only same-origin and sign-in flags. Only bundle web files are served, with a
strict content security policy. OIDC must be configured for interactive web
access; service tokens remain available for automation and CLI use.

```text
GET  /v1/office/projection?organizationId=<org>      organization.read in <org>
POST /v1/approvals/{approvalId}/resolve               approval.decide in the body's organization
```

The projection is `office-projection.v1` from
[agentux-protocol](https://github.com/agentux-os/agentux-protocol), and
`tests/contract.rs` checks what Command serves against that schema (through
`auxp::schemas`, at the revision Command pins). What fills it:

- **Nodes' events.** A node's `run.*` events keep one entry per run in
  `runs` (its title, step, cost, branch, pull request, `status` and
  `nodeId`); `approval.requested` adds a pending approval (its `requestId` is
  the id, its `kind` the action); `artifact.available` adds to `artifacts`.
  `gate.completed`, `node.status_changed`, `node.alert` and `command.*` go to
  the timeline. Harness status and session traffic stay in the node registry
  (`GET /v1/nodes`).
- **Published definitions**: the organization, its areas, missions and
  agents, and `definitionVersion` ([Organization definitions](#organization-definitions)).
- **Nodes and freshness, when read.** `nodes` lists the organization's nodes
  as Command sees them at that moment (`presence` `online`, `degraded` or
  `offline`, `lastHeartbeatAt`, `connected`), and `freshness` follows them:
  `fresh` while every node reports, `degraded` while some do not, `stale`
  when none does. `projectedAt` is when the last event was taken.
- **Timeline.** `events` holds the latest 100; the whole log, from any
  cursor or time, is [replayed](#events) from
  `/v1/organizations/<org>/events`.

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
Events are kept indefinitely by default; an operator can turn on retention,
which archives old events and keeps projections verifiable through checkpoints
([Event retention](deploy/README.md#event-retention)).
Approval decisions are also appended to the event log, so the read model can be
rebuilt and checked against replay after restart. The Office JSON contract stays
independent of the storage implementation. Live updates depend on authenticated
organization sessions; Office keeps current by loading the projection once
and then following [the live stream](#events) from its `lastEventId`'s cursor.

The PostgreSQL-backed service requires `AGENTUX_COMMAND_DATABASE_URL`, connects to the
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


