# AgentUX Command security review

**Review date:** 2026-11-04

**Scope:** PostgreSQL-backed single-host profile and the HTTP/WebSocket server
on `main` at `4e793c8`.

This review records the assets, boundaries, controls, confirmed findings and
remaining risks for the initial production profile. It is an engineering
assessment of the current code and deployment bundle, not a penetration test
or an independent certification.

## Assets

- Operator identities, session credentials, service-token secrets and grants.
- Node enrollment tokens, enrolled node credentials and commands sent to
  nodes.
- Organization definitions, mission state, approvals, audit events and node
  telemetry in PostgreSQL.
- OIDC client credentials, database credentials, backup credentials and
  encryption recovery material held by the operator.
- Integrity and availability of the Command process, database and event
  archives.

## Trust boundaries and attacker capabilities

1. **Internet/client to ingress and Command.** Unauthenticated clients can
   send HTTP and WebSocket upgrade requests, malformed bodies and repeated
   requests. A TLS terminator is outside this repository's trust boundary.
2. **Browser to operator API.** A malicious website may cause a signed-in
   browser to make requests. A compromised browser or stolen session can act
   within that operator's grants.
3. **Node to Command.** A node has one organization-bound credential and may
   submit protocol messages or hold a WebSocket. A compromised node can
   falsify its own telemetry and consume its authorized connection; it must
   not speak for another node or organization.
4. **OIDC provider to callback.** Command relies on the configured issuer,
   signing keys, client registration, group claims and role map. A compromised
   provider or overly broad role map can grant unauthorized access.
5. **Command to PostgreSQL and host.** Database and container credentials,
   persistent volumes, backups and host root are operator-controlled. Host
   compromise is outside the application authorization boundary.
6. **Organization boundary.** A caller may attempt to enumerate or mutate
   another organization's nodes, runs, definitions, events, approvals or
   tokens. Authorization must be checked against the resource's organization.

## Controls verified in code and tests

- OIDC uses authorization code flow with PKCE S256, state, nonce, issuer,
  audience, expiry and JWKS signature verification. Role maps translate
  provider groups into organization-scoped capabilities.
- Browser sessions and service tokens are stored by hash, expire or can be
  revoked, and receive organization-scoped grants. Node enrollment tokens are
  single-use and server-instance-bound; enrolled nodes authenticate
  individually. Revoking a node closes its active channel.
- Protected routes check capabilities in the resource's organization and
  conceal foreign-organization resources as unknown. Existing regression
  tests cover tenant boundaries, revoked credentials, invalid/expired OIDC
  tokens and replayed OIDC state.
- Session-cookie mutations require an exact same-origin `Origin`; reads and
  explicit bearer-token clients do not depend on cookies. Cookies are
  `HttpOnly` and `SameSite=Lax`; `Secure` is set when the canonical origin is
  HTTPS. Command now ignores client-supplied `X-Forwarded-*` values in the
  CSRF comparison. TLS deployments must configure
  `AGENTUX_COMMAND_PUBLIC_URL` to their canonical HTTPS origin.
- HTTP request bodies and WebSocket messages are limited to 4 MiB. Metrics are
  disabled unless configured and then require a dedicated bearer token of at
  least 32 bytes. Static UI assets set CSP, `nosniff`, same-origin referrer
  policy and `no-cache`.
- The production constructor disables demo tokens and unregistered nodes.
  Those development switches are now rejected unless an operator explicitly
  enables development mode, binds to a literal loopback address, and leaves
  public URL and OIDC unset. The restore drill uses that constrained mode.
- The single-host bundle puts PostgreSQL and Command on a private Podman
  network and publishes only the Command port. The operator must restrict
  that port to the TLS ingress and protect environment files, Podman secrets,
  host, volumes and backups.

## Confirmed findings addressed in this review

| Severity | Finding | Resolution |
|---|---|---|
| High | `AGENTUX_COMMAND_INSECURE_DEMO_TOKENS=1` and `AGENTUX_COMMAND_INSECURE_OPEN_NODES=1` could re-enable development behavior in the PostgreSQL server, including a non-loopback deployment. | Require a separate development-mode opt-in, literal loopback binding, no public URL and no OIDC; fail before opening the database otherwise. Add startup-configuration regression tests and constrain the CI restore drill. |
| Medium | Without a configured public URL, the CSRF origin comparison trusted client-supplied `X-Forwarded-Host` and `X-Forwarded-Proto`. A cross-origin request could make those values match its own `Origin`, subject to browser cookie behavior. | Use the configured public URL when present; otherwise compare against the request `Host` with an HTTP scheme. Never use forwarded headers for this security decision. Add a forged-forwarded-header regression test. |

## Production profile review checklist

- [x] Public HTTP and WebSocket surfaces: production operator authentication,
  per-node authentication, 4 MiB body/message limits and readiness/metrics
  exposure checked. No request-rate limiter is present; ingress throttling is
  a residual risk.
- [x] Node enrollment and command channel: single-use instance-bound enrollment,
  node identity binding, command authorization, revocation and channel close
  behavior checked against the implementation and existing tests.
- [x] OIDC and sessions: PKCE, state, nonce, token signature/claims, role map,
  session hash/expiry, cookie flags and CSRF handling checked. New regressions
  cover forged forwarding headers and production refusal of development
  switches.
- [x] Organization isolation: authorization is applied to the resource's
  organization; foreign resources are concealed. Existing tests cover
  cross-organization node, token, approval and audit access.
- [x] Audit and export: organization-scoped audit capability checks and secret
  redaction are exercised in `tests/audit.rs`.
- [x] Proxy and TLS assumptions: application TLS termination is out of scope;
  canonical HTTPS public URL, ingress port restriction and forwarding-header
  handling are documented as operator requirements.
- [x] Secrets and insecure modes: token/session hashing, secret environment
  handling, node credential rotation/revocation limits, and development-mode
  guard are recorded with operator actions.
- [ ] Abuse controls: add and qualify rate limits, connection limits and
  overload behavior in the ingress/staging profile; no application limiter is
  claimed by this review.
- [ ] Independent penetration testing, fuzzing and a real proxy configuration
  audit remain release qualification tasks.

## Residual risks and operator responsibilities

- There is no application-level per-IP or per-credential rate limiter. The
  ingress should enforce connection/request rates and request timeouts;
  deployment-specific limits and abuse tests remain open work.
- Command does not terminate TLS. The operator must provide HTTPS, restrict
  direct container access, set the canonical public URL, and configure the
  ingress to overwrite forwarding headers. HSTS and edge security headers
  belong at that ingress.
- A node credential is a bearer secret. Protect it on the node, revoke it
  promptly after compromise, and use least-privilege access at the host.
  Static node tokens are long-lived until rotated by the operator.
- OIDC group-to-capability mappings are deployment policy. Review them for
  least privilege and test role changes with the identity provider. Session
  revocation is per-session; account disablement propagation depends on the
  provider and configured session lifetime.
- Service and node secrets supplied in environment variables can be exposed
  by an incorrectly permissioned host or process-inspection policy. Use the
  documented operator-managed secret mechanisms and restrict the container
  environment file.
- Database SQL access and host root can read or alter all organizations.
  Application-level tenant checks do not defend against a compromised host,
  database administrator or backup key.
- No independent penetration test, fuzz campaign, proxy configuration audit,
  or production abuse test has been performed. These remain release
  qualification work; see issue #54 for deployed staging and fault scenarios.

## Regression evidence

The automated suite includes organization-isolation and revocation cases in
`tests/auth.rs`, node credential/revocation and protocol-boundary cases in
`tests/server.rs`, OIDC signature/claim/state/session/CSRF cases in
`tests/oidc.rs`, and the production startup-mode guard in the binary's unit
tests. CI runs the Rust test suite and the restore drill on every pull request.

Revisit this model whenever authentication, proxy assumptions, node protocol,
deployment topology, or secret handling changes.
