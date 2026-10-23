//! Command's HTTP API: node messages and command connections, enrollment,
//! operator commands, and the Office projection.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::ws::rejection::WebSocketUpgradeRejection;
use axum::extract::{Path, Query, Request, State, WebSocketUpgrade};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use subtle::ConstantTimeEq;

use crate::auth::{self, DemoTokens, Principal, ServiceTokens, Sessions};
use crate::channel::{Hub, SendError};
use crate::definitions::{self, Actor};
use crate::oidc::{self, Oidc};
use auxp::enrollment;
use auxp::provision::{CommandJoin, Provision};

use crate::enroll::{self, TokenRequest};
use crate::missions;
use crate::nodes::{self, Registry};
use crate::office::{self, OfficeRepository, ResolutionRequest};
use crate::protocol;
use crate::random_hex;

/// How long an operator's command waits for the node's answer.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(150);
/// The largest node message and command connection message.
const MESSAGE_LIMIT: usize = 4 << 20;

struct Inner {
    registry: Registry,
    office: Arc<dyn OfficeRepository>,
    hub: Hub,
    /// Node id to a static bearer token (AGENTUX_COMMAND_NODE_TOKENS).
    node_tokens: HashMap<String, String>,
    /// Enrolled nodes and this server's instance id.
    enroll: Arc<dyn enroll::Store>,
    instance: String,
    /// Accept protocol messages from nodes nobody enrolled (the demo only).
    open_nodes: bool,
    /// The address nodes reach this server at, for provision files.
    public_url: String,
    /// Operators' service tokens (and, later, sessions).
    service_tokens: Arc<dyn ServiceTokens>,
    /// Accept the demo tokens (the demo server; development only).
    demo_tokens: bool,
    /// Signed-in operators' sessions.
    sessions: Arc<dyn Sessions>,
    /// Sign-in with an OIDC provider, when configured.
    oidc: Option<Arc<Oidc>>,
    /// Organization definitions and their audit log.
    definitions: Arc<dyn definitions::Store>,
    /// The database, when Command keeps its state in one: event delivery
    /// reads its outbox.
    database: Option<deadpool_postgres::Pool>,
    /// Woken whenever this server takes an event, for live streams.
    changed: tokio::sync::Notify,
    /// Missions' runs.
    runs: Arc<dyn missions::Store>,
}

/// A Command server, configured with the `with_*` methods, served by
/// [`Handler::router`].
pub struct Handler {
    inner: Inner,
}

/// Reads `node-1=token1,node-2=token2`.
pub fn parse_node_tokens(text: &str) -> HashMap<String, String> {
    text.split(',')
        .filter_map(|entry| {
            let (node, token) = entry.trim().split_once('=')?;
            let (node, token) = (node.trim(), token.trim());
            (!node.is_empty() && !token.is_empty()).then(|| (node.to_string(), token.to_string()))
        })
        .collect()
}

impl Default for Handler {
    fn default() -> Self {
        Self::new()
    }
}

impl Handler {
    /// The demo server: memory stores, open to nodes nobody enrolled, and
    /// the demo tokens.
    pub fn new() -> Self {
        Self::with_office(Arc::new(office::Store::new()))
    }

    pub fn with_office(office: Arc<dyn OfficeRepository>) -> Self {
        let memory = enroll::Memory::new("");
        let instance = memory.instance().to_string();
        Self {
            inner: Inner {
                registry: Registry::new(),
                office,
                hub: Hub::new(),
                node_tokens: HashMap::new(),
                enroll: Arc::new(memory),
                instance,
                open_nodes: true,
                public_url: String::new(),
                service_tokens: Arc::new(auth::MemoryTokens::default()),
                demo_tokens: true,
                sessions: Arc::new(auth::MemorySessions::default()),
                oidc: None,
                definitions: Arc::new(definitions::Memory::default()),
                database: None,
                changed: tokio::sync::Notify::new(),
                runs: Arc::new(missions::Memory::default()),
            },
        }
    }

    /// The production server: PostgreSQL for the read model, enrollment and
    /// service tokens; closed to nodes nobody enrolled and to the demo
    /// tokens.
    pub async fn postgres(database_url: &str) -> Result<Self, String> {
        let pool = office::open_postgres(database_url)
            .await
            .map_err(|e| e.to_string())?;
        let mut handler = Self::with_office(Arc::new(office::PostgresStore::new(pool.clone())));
        handler.inner.database = Some(pool.clone());
        let handler = handler
            .with_service_tokens(Arc::new(auth::PostgresTokens::new(pool.clone())))
            .with_sessions(Arc::new(auth::PostgresSessions::new(pool.clone())))
            .with_definitions(Arc::new(definitions::Postgres::new(pool.clone())))
            .with_runs(Arc::new(missions::Postgres::new(pool.clone())))
            .with_stored_nodes(pool.clone())
            .await?
            .with_enroll_store(Arc::new(enroll::Postgres::new(pool)))
            .await
            .map_err(|e| e.to_string())?;
        Ok(handler.with_open_nodes(false).with_demo_tokens(false))
    }

    /// Reads the nodes stored before a restart from `pool`, and stores every
    /// change from now on.
    pub async fn with_stored_nodes(self, pool: deadpool_postgres::Pool) -> Result<Self, String> {
        self.inner.registry.load(nodes::persist::load(&pool).await?);
        self.inner.registry.store_to(nodes::persist::writer(pool));
        Ok(self)
    }

    /// Delivers the events Command takes to `sink`, from the database's
    /// outbox ([`outbox`](crate::outbox)); without a sink the outbox is only
    /// kept bounded. Nothing to do without a database.
    pub fn deliver_events(self, sink: Option<Arc<dyn crate::outbox::Sink>>) -> Self {
        if let Some(pool) = &self.inner.database {
            crate::outbox::Delivery::new(pool.clone(), sink).spawn(Duration::from_secs(2));
        }
        self
    }

    /// Keeps missions' runs in `store`.
    pub fn with_runs(mut self, store: Arc<dyn missions::Store>) -> Self {
        self.inner.runs = store;
        self
    }

    /// Keeps organization definitions and their audit log in `store`.
    pub fn with_definitions(mut self, store: Arc<dyn definitions::Store>) -> Self {
        self.inner.definitions = store;
        self
    }

    /// Keeps signed-in operators' sessions in `store`.
    pub fn with_sessions(mut self, store: Arc<dyn Sessions>) -> Self {
        self.inner.sessions = store;
        self
    }

    /// Signs operators in with an OIDC provider (`/v1/auth/login`).
    pub fn with_oidc(mut self, provider: Oidc) -> Self {
        self.inner.oidc = Some(Arc::new(provider));
        self
    }

    /// Keeps operators' service tokens in `store`.
    pub fn with_service_tokens(mut self, store: Arc<dyn ServiceTokens>) -> Self {
        self.inner.service_tokens = store;
        self
    }

    /// Whether the demo tokens (`demo-leader`, `demo-observer`) are
    /// accepted (AGENTUX_COMMAND_INSECURE_DEMO_TOKENS=1; development only).
    pub fn with_demo_tokens(mut self, accept: bool) -> Self {
        self.inner.demo_tokens = accept;
        self
    }

    /// The operator token store, for issuing the first administrator token.
    pub fn service_tokens(&self) -> Arc<dyn ServiceTokens> {
        self.inner.service_tokens.clone()
    }

    /// Keeps enrolled nodes in `store` (PostgreSQL in production).
    pub async fn with_enroll_store(
        mut self,
        store: Arc<dyn enroll::Store>,
    ) -> Result<Self, enroll::Error> {
        self.inner.instance = store.instance_id().await?;
        self.inner.enroll = store;
        Ok(self)
    }

    /// Whether nodes that are neither enrolled nor listed in the static
    /// tokens may report (development only).
    pub fn with_open_nodes(mut self, open: bool) -> Self {
        self.inner.open_nodes = open;
        self
    }

    /// Requires each listed node's protocol messages to carry its token
    /// (`Authorization: Bearer <token>`).
    pub fn with_node_tokens(mut self, tokens: HashMap<String, String>) -> Self {
        self.inner.node_tokens = tokens;
        self
    }

    /// The address nodes use (AGENTUX_COMMAND_PUBLIC_URL); without it,
    /// provision files use the address the operator's request came to.
    pub fn with_public_url(mut self, url: &str) -> Self {
        self.inner.public_url = url.trim().trim_end_matches('/').to_string();
        self
    }

    /// This Command server's identity, which nodes pin.
    pub fn instance_id(&self) -> &str {
        &self.inner.instance
    }

    pub fn router(self) -> Router {
        let state = Arc::new(self.inner);
        // Runs that are due move on their own (a retry, a check on a node);
        // the loop ends with the server.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let weak = Arc::downgrade(&state);
            runtime.spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    let Some(s) = weak.upgrade() else { break };
                    s.engine().tick(&NodesVia(&s)).await;
                }
            });
        }
        Router::new()
            .route("/healthz", get(healthz))
            .route("/livez", get(livez))
            .route("/readyz", get(healthz))
            .route("/v1/instance", get(instance))
            .route("/v1/nodes", get(list_nodes))
            .route("/v1/enroll", post(redeem))
            .route("/v1/enrollment-tokens", post(issue_tokens))
            .route("/v1/enrollments", get(list_enrollments))
            .route("/v1/nodes/{node}/approve", post(approve_node))
            .route("/v1/nodes/{node}/revoke", post(revoke_node))
            .route("/v1/nodes/{node}/commands", post(command))
            .route("/v1/protocol/connect", get(connect))
            .route("/v1/protocol/messages", post(message))
            .route("/v1/office/projection", get(projection))
            .route("/v1/approvals/{approval}/resolve", post(resolve_approval))
            .route(
                "/v1/service-tokens",
                get(list_service_tokens).post(issue_service_token),
            )
            .route(
                "/v1/service-tokens/{token}/revoke",
                post(revoke_service_token),
            )
            .route(
                "/v1/organizations/{org}/definitions",
                get(list_definitions).post(draft_definition),
            )
            .route(
                "/v1/organizations/{org}/definitions/{version}",
                get(get_definition).put(replace_definition),
            )
            .route(
                "/v1/organizations/{org}/definitions/{version}/publish",
                post(publish_definition),
            )
            .route(
                "/v1/organizations/{org}/definitions/{version}/reviews",
                post(review_definition),
            )
            .route("/v1/organizations/{org}/audit", get(organization_audit))
            .route("/v1/organizations/{org}/audit/export", get(export_audit))
            .route("/v1/organizations/{org}/events", get(organization_events))
            .route(
                "/v1/organizations/{org}/runs",
                get(list_runs).post(start_run),
            )
            .route("/v1/organizations/{org}/runs/{run}", get(get_run))
            .route(
                "/v1/organizations/{org}/runs/{run}/{action}",
                post(control_run),
            )
            .route("/v1/organizations/{org}/events/stream", get(stream_events))
            .route("/v1/auth/login", get(login))
            .route("/v1/auth/callback", get(callback))
            .route("/v1/auth/logout", post(logout))
            .route("/v1/auth/me", get(me))
            .fallback(not_found)
            .method_not_allowed_fallback(not_found)
            .layer(axum::extract::DefaultBodyLimit::max(MESSAGE_LIMIT))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                same_origin,
            ))
            .with_state(state)
    }
}

type AppState = State<Arc<Inner>>;

fn reply(status: StatusCode, body: Value) -> Response {
    (status, axum::Json(body)).into_response()
}

fn error(status: StatusCode, message: &str) -> Response {
    reply(status, json!({"error": message}))
}

async fn not_found() -> Response {
    error(StatusCode::NOT_FOUND, "not found")
}

fn bearer(headers: &HeaderMap) -> &str {
    let value = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    value.strip_prefix("Bearer ").unwrap_or(value)
}

/// 403 for a capability the caller lacks in an organization.
fn forbidden(capability: &str, organization: &str) -> Response {
    error(
        StatusCode::FORBIDDEN,
        &format!("{capability} capability required in organization {organization}"),
    )
}

/// The session cookie a sign-in sets.
const SESSION_COOKIE: &str = "agentux_session";

/// The session secret in the request's cookie, if any.
fn session_cookie(headers: &HeaderMap) -> Option<&str> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|pair| pair.trim().split_once('='))
        .find(|(name, _)| *name == SESSION_COOKIE)
        .map(|(_, value)| value)
}

/// Refuses a state-changing request that a browser sent on the strength of
/// the session cookie alone from another origin (CSRF). Requests with an
/// Authorization header, and reads, are not concerned.
async fn same_origin(State(s): AppState, request: Request, next: Next) -> Response {
    let reads = matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    );
    let headers = request.headers();
    if !reads && !headers.contains_key(header::AUTHORIZATION) && session_cookie(headers).is_some() {
        let expected = s.base_url(headers);
        let origin = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok());
        if origin != Some(expected.as_str()) {
            return error(
                StatusCode::FORBIDDEN,
                "a signed-in request must come from this server's own pages",
            );
        }
    }
    next.run(request).await
}

/// 401: no operator credential, or one Command does not accept.
fn unauthenticated() -> Response {
    error(StatusCode::UNAUTHORIZED, "invalid credentials")
}

/// Parses a JSON body of at most `limit` bytes.
fn parse<T: serde::de::DeserializeOwned>(body: &[u8], limit: usize) -> Option<T> {
    if body.len() > limit {
        return None;
    }
    serde_json::from_slice(body).ok()
}

/// What a request's node credential allows.
#[derive(Debug, PartialEq, Eq)]
enum Access {
    None,
    /// Enrolled, waiting for an operator's approval.
    Pending,
    Full,
}

impl Inner {
    /// The operator behind a request: its service token, or a demo token
    /// where those are accepted. `Err` is the 401 to answer.
    async fn operator(&self, headers: &HeaderMap) -> Result<Principal, Box<Response>> {
        let bearer = match bearer(headers) {
            "" => session_cookie(headers).unwrap_or(""),
            given => given,
        };
        if bearer.is_empty() {
            return Err(Box::new(unauthenticated()));
        }
        if bearer.starts_with(auth::SESSION_PREFIX) {
            return self
                .sessions
                .resolve(bearer, Utc::now())
                .await
                .ok_or_else(|| Box::new(unauthenticated()));
        }
        if self.demo_tokens
            && let Some(p) = auth::Authenticator::authenticate(&DemoTokens, bearer).await
        {
            return Ok(p);
        }
        self.service_tokens
            .authenticate(bearer)
            .await
            .ok_or_else(|| Box::new(unauthenticated()))
    }

    /// The organization an enrolled node belongs to.
    async fn node_organization(&self, node_id: &str) -> Option<String> {
        if let Some(node) = self.registry.get(node_id) {
            return Some(node.organization_id);
        }
        self.enroll
            .credentials()
            .await
            .ok()?
            .into_iter()
            .find(|c| c.node_id == node_id)
            .map(|c| c.organization_id)
    }

    /// Checks the node's bearer token: its enrollment credential (bound to
    /// its organization), a static token, or, on the demo server only, none.
    async fn node_access(
        &self,
        headers: &HeaderMap,
        node_id: &str,
        organization_id: &str,
    ) -> Access {
        let got = bearer(headers);
        if enrollment::is_credential(got) {
            return match self.enroll.authenticate(node_id, got).await {
                Ok(c) if !organization_id.is_empty() && c.organization_id != organization_id => {
                    Access::None
                }
                Ok(c) if c.active() => Access::Full,
                Ok(_) => Access::Pending,
                Err(_) => Access::None,
            };
        }
        if let Some(want) = self.node_tokens.get(node_id) {
            return if bool::from(got.as_bytes().ct_eq(want.as_bytes())) {
                Access::Full
            } else {
                Access::None
            };
        }
        if self.open_nodes && self.node_tokens.is_empty() {
            Access::Full
        } else {
            Access::None
        }
    }

    /// Records a valid node message in the registry.
    fn apply(&self, body: &[u8]) {
        let Ok(message) = protocol::read(body) else {
            return;
        };
        let raw = serde_json::from_slice::<Value>(body)
            .ok()
            .and_then(|v| v.get("params").cloned());
        let p = message.params;
        match message.method.as_str() {
            "node.hello" => self.registry.hello(nodes::Node {
                id: p.node_id,
                organization_id: p.organization_id,
                capabilities: Some(p.capabilities),
                agentux_version: p.agentux_version,
                last_seen_at: p.occurred_at,
                ..nodes::Node::default()
            }),
            "node.heartbeat" => {
                self.registry
                    .heartbeat_with(&p.node_id, &p.status, p.capacity, &p.occurred_at)
            }
            method if protocol::is_event(method) => {
                self.registry.apply(nodes::Event {
                    node_id: p.node_id,
                    organization_id: p.organization_id,
                    method: method.to_string(),
                    sequence: p.sequence,
                    occurred_at: p.occurred_at,
                    run_id: p.run_id,
                    request_id: p.request_id,
                    state: p.state,
                    harnesses: p.harnesses,
                    limits: p.limits,
                    params: raw,
                });
            }
            _ => {}
        }
    }

    /// Takes a node's event into the Office log, when the Office shows that
    /// kind. A replayed event is a no-op (by event id); one the log refuses
    /// (out of order, malformed) is reported, and the node's message still
    /// counts as received: the registry has it.
    async fn project(&self, method: &str, body: &[u8]) {
        let Some(params) = serde_json::from_slice::<Value>(body)
            .ok()
            .and_then(|v| v.get("params").cloned())
        else {
            return;
        };
        let Some(event) = office::from_node_event(method, &params) else {
            return;
        };
        let (org, node, sequence) = (
            event.organization_id.clone(),
            event.node_id.clone(),
            event.sequence,
        );
        let data = event.data.clone().unwrap_or(Value::Null);
        let (node_run, error) = (
            event.run_id.clone(),
            data["error"]
                .as_str()
                .or_else(|| data["error"]["message"].as_str())
                .unwrap_or_default()
                .to_string(),
        );
        match self.office.apply_event(event).await {
            Ok(()) => {
                self.changed.notify_waiters();
                let engine = self.engine();
                let nodes = NodesVia(self);
                let moved = if method == "command.completed" {
                    engine
                        .command_completed(
                            &org,
                            data["commandId"].as_str().unwrap_or_default(),
                            data["ok"].as_bool().unwrap_or(false),
                            &data["result"],
                            &error,
                            &nodes,
                        )
                        .await
                } else {
                    engine
                        .node_event(&org, &node_run, method, &error, &nodes)
                        .await
                };
                if let Err(e) = moved {
                    eprintln!("agentux-command: missions: {e}");
                }
            }
            Err(e) => eprintln!(
                "agentux-command: the Office did not take {method} {sequence} from {node} in {org}: {e}"
            ),
        }
    }

    /// Records what an operator did in the organization's event log, which
    /// is its audit trail: what, who, and what it touched (never secrets or
    /// prompts).
    async fn audit(&self, org: &str, event_type: &str, operator: &Principal, data: Value) {
        let event_id = format!("audit-{}", random_hex(12));
        let event = office::Event {
            event_id: event_id.clone(),
            event_type: event_type.into(),
            organization_id: org.into(),
            node_id: "command.audit".into(),
            occurred_at: crate::now_rfc3339(),
            correlation_id: event_id,
            actor: serde_json::to_value(Actor::of(&operator.subject)).ok(),
            data: Some(data),
            ..office::Event::default()
        };
        match self.office.apply_event(event).await {
            Ok(()) => self.changed.notify_waiters(),
            Err(e) => eprintln!("agentux-command: audit {event_type} in {org}: {e}"),
        }
    }

    /// The mission engine over this server's stores.
    fn engine(&self) -> missions::Engine {
        missions::Engine::new(
            self.runs.clone(),
            self.definitions.clone(),
            self.office.clone(),
        )
    }

    /// Where nodes reach this server.
    fn base_url(&self, headers: &HeaderMap) -> String {
        if !self.public_url.is_empty() {
            return self.public_url.clone();
        }
        let header = |name: &str| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
        };
        let scheme = if header("x-forwarded-proto") == "https" {
            "https"
        } else {
            "http"
        };
        let host = match header("x-forwarded-host") {
            "" => header("host"),
            host => host,
        };
        format!("{scheme}://{host}")
    }
}

/// Readiness (`/readyz`, and `/healthz` as before): the process and its
/// database. 503 while the database does not answer.
async fn healthz(State(s): AppState) -> Response {
    match s.office.ping().await {
        Ok(()) => reply(StatusCode::OK, json!({"status": "ok", "database": "ok"})),
        Err(_) => reply(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"status": "unavailable", "database": "unreachable"}),
        ),
    }
}

/// Liveness: the process answers, whatever its dependencies do. A container
/// manager restarts Command when this fails, not when its database is down.
async fn livez() -> Response {
    reply(StatusCode::OK, json!({"status": "alive"}))
}

async fn instance(State(s): AppState) -> Response {
    reply(
        StatusCode::OK,
        serde_json::to_value(enrollment::Instance::new(&s.instance)).unwrap_or_default(),
    )
}

async fn list_nodes(State(s): AppState, headers: HeaderMap) -> Response {
    // Node telemetry names repositories, agents and spend: operators only,
    // and only their organizations' nodes.
    let operator = match s.operator(&headers).await {
        Ok(p) => p,
        Err(denied) => return *denied,
    };
    if !operator.can_somewhere(auth::ORGANIZATION_READ) {
        return error(
            StatusCode::FORBIDDEN,
            "organization.read capability required",
        );
    }
    let mut list = s.registry.list();
    list.retain(|node| operator.can(&node.organization_id, auth::ORGANIZATION_READ));
    for node in &mut list {
        node.connected = s.hub.connected(&node.id);
    }
    reply(
        StatusCode::OK,
        serde_json::to_value(list).unwrap_or_default(),
    )
}

async fn message(State(s): AppState, headers: HeaderMap, body: Bytes) -> Response {
    if body.len() > MESSAGE_LIMIT {
        return error(StatusCode::BAD_REQUEST, "the message is too large");
    }
    let message = match protocol::read(&body) {
        Ok(message) => message,
        Err(e) => return error(StatusCode::BAD_REQUEST, &e),
    };
    let p = &message.params;
    match s
        .node_access(&headers, &p.node_id, &p.organization_id)
        .await
    {
        Access::None => return error(StatusCode::UNAUTHORIZED, "unknown node or token"),
        Access::Pending => {
            // Until an operator approves it, a node may only say it exists.
            if message.method != "node.hello" && message.method != "node.heartbeat" {
                return reply(
                    StatusCode::FORBIDDEN,
                    json!({"error": "the node waits for an operator's approval", "instanceId": s.instance}),
                );
            }
            s.apply(&body);
            s.registry
                .heartbeat_with(&p.node_id, "pending_approval", None, &p.occurred_at);
        }
        Access::Full => {
            s.apply(&body);
            s.project(&message.method, &body).await;
        }
    }
    reply(
        StatusCode::ACCEPTED,
        json!({"jsonrpc": "2.0", "id": message.id, "result": {"accepted": "true", "instanceId": s.instance}}),
    )
}

#[derive(Deserialize)]
struct ConnectQuery {
    #[serde(rename = "nodeId", default)]
    node_id: String,
}

/// Accepts a node's command connection.
async fn connect(
    State(s): AppState,
    Query(query): Query<ConnectQuery>,
    headers: HeaderMap,
    upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Response {
    let node_id = query.node_id;
    if node_id.is_empty() {
        return error(StatusCode::BAD_REQUEST, "nodeId is required");
    }
    match s.node_access(&headers, &node_id, "").await {
        Access::None => return error(StatusCode::UNAUTHORIZED, "unknown node or token"),
        Access::Pending => {
            return error(
                StatusCode::FORBIDDEN,
                "the node waits for an operator's approval",
            );
        }
        Access::Full => {}
    }
    let upgrade = match upgrade {
        Ok(upgrade) => upgrade,
        Err(rejection) => return rejection.into_response(),
    };
    let state = s.clone();
    let mut response = upgrade
        .max_message_size(MESSAGE_LIMIT)
        .on_upgrade(move |ws| async move {
            let on_message = |node: &str, raw: &[u8]| {
                // A node's own connection only speaks for that node.
                let speaks_for = serde_json::from_slice::<Value>(raw).ok().and_then(|v| {
                    v.pointer("/params/nodeId")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                });
                if speaks_for.as_deref() == Some(node) {
                    state.apply(raw);
                }
            };
            state.hub.serve(ws, &node_id, on_message).await;
        });
    // Nodes check they reached the Command server they enrolled with.
    if let Ok(value) = HeaderValue::from_str(&s.instance) {
        response.headers_mut().insert(auxp::INSTANCE_HEADER, value);
    }
    response
}

#[derive(Deserialize)]
struct CommandRequest {
    #[serde(default, deserialize_with = "crate::nullable")]
    method: String,
    #[serde(rename = "commandId", default, deserialize_with = "crate::nullable")]
    command_id: String,
    #[serde(default)]
    params: Option<Map<String, Value>>,
}

/// Sends an operator's command to a node and returns its answer.
async fn command(
    State(s): AppState,
    Path(node_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let operator = match s.operator(&headers).await {
        Ok(p) => p,
        Err(denied) => return *denied,
    };
    if !operator.can_somewhere(auth::NODE_COMMAND) {
        return error(StatusCode::FORBIDDEN, "node.command capability required");
    }
    // Another organization's node is answered as unknown: its existence is
    // not the caller's to learn.
    let visible = |org: &str| {
        operator.can(org, auth::ORGANIZATION_READ) || operator.can(org, auth::NODE_COMMAND)
    };
    let Some(node) = s
        .registry
        .get(&node_id)
        .filter(|n| visible(&n.organization_id))
    else {
        return error(StatusCode::NOT_FOUND, "unknown node");
    };
    if !operator.can(&node.organization_id, auth::NODE_COMMAND) {
        return forbidden(auth::NODE_COMMAND, &node.organization_id);
    }
    let request =
        parse::<CommandRequest>(&body, 1 << 20).filter(|r| auxp::methods::is_command(&r.method));
    let Some(request) = request else {
        return error(
            StatusCode::BAD_REQUEST,
            "a known node command method and JSON params are required",
        );
    };
    let mut params = request.params.unwrap_or_default();
    if params.contains_key("apiKey") || params.contains_key("providerToken") {
        return error(
            StatusCode::BAD_REQUEST,
            "provider credentials are not protocol payloads",
        );
    }
    let command_id = if request.command_id.is_empty() {
        format!("cmd-{}", random_hex(12))
    } else {
        request.command_id
    };
    let org = node.organization_id.clone();
    for (key, value) in [
        ("protocolVersion", "v1".to_string()),
        ("organizationId", node.organization_id),
        ("nodeId", node_id.clone()),
        ("messageId", command_id.clone()),
        ("commandId", command_id.clone()),
        (
            "occurredAt",
            Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        ),
        ("commandInstanceId", s.instance.clone()),
    ] {
        params.insert(key.into(), Value::String(value));
    }
    let answer = tokio::time::timeout(
        COMMAND_TIMEOUT,
        s.hub.send(&node_id, &request.method, params),
    )
    .await;
    let outcome = match &answer {
        Err(_) => "timeout",
        Ok(Err(SendError::NotConnected)) => "unreachable",
        Ok(Err(_)) => "disconnected",
        Ok(Ok(a)) if a.error.is_some() => "refused",
        Ok(Ok(_)) => "ok",
    };
    s.audit(
        &org,
        "node.command.sent",
        &operator,
        json!({"nodeId": node_id, "method": request.method, "commandId": command_id, "outcome": outcome}),
    )
    .await;
    match answer {
        Err(_) => reply(
            StatusCode::GATEWAY_TIMEOUT,
            json!({"error": "the node did not answer in time", "commandId": command_id}),
        ),
        Ok(Err(SendError::NotConnected)) => reply(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error": "the node is not connected", "commandId": command_id}),
        ),
        Ok(Err(e)) => reply(
            StatusCode::GATEWAY_TIMEOUT,
            json!({"error": e.to_string(), "commandId": command_id}),
        ),
        Ok(Ok(answer)) => match answer.error {
            Some(e) => {
                // Held for confirmation at the node, or denied by its policy.
                let status = if auxp::codes::is_policy(e.code) {
                    StatusCode::CONFLICT
                } else {
                    StatusCode::BAD_GATEWAY
                };
                reply(status, json!({"commandId": command_id, "error": e}))
            }
            None => reply(
                StatusCode::OK,
                json!({"commandId": command_id, "result": answer.result}),
            ),
        },
    }
}

/// Trades an enrollment token for the node's credential.
async fn redeem(State(s): AppState, body: Bytes) -> Response {
    // The protocol's redemption: nodeName and hostname may be null.
    let Some(request) = parse::<enrollment::Redemption>(&body, 16 << 10) else {
        return error(StatusCode::BAD_REQUEST, "invalid JSON");
    };
    let node_name = request.node_name.unwrap_or_default();
    let hostname = request.hostname.unwrap_or_default();
    match s
        .enroll
        .redeem(&request.token, &node_name, &hostname, Utc::now())
        .await
    {
        Ok(r) => {
            let enrolled = enrollment::Enrolled {
                instance_id: s.instance.clone(),
                organization_id: r.credential.organization_id,
                node_id: r.credential.node_id,
                node_secret: r.secret,
                approved: r.credential.approved,
            };
            reply(
                StatusCode::OK,
                serde_json::to_value(enrolled).unwrap_or_default(),
            )
        }
        Err(e @ enroll::Error::OtherInstance) => {
            let refused = enrollment::Refused {
                error: e.to_string(),
                instance_id: Some(s.instance.clone()),
            };
            reply(
                StatusCode::CONFLICT,
                serde_json::to_value(refused).unwrap_or_default(),
            )
        }
        Err(
            e @ (enroll::Error::InvalidToken
            | enroll::Error::UsedToken
            | enroll::Error::ExpiredToken),
        ) => error(StatusCode::UNAUTHORIZED, &e.to_string()),
        Err(e) => error(StatusCode::CONFLICT, &e.to_string()),
    }
}

/// What an operator wants in each machine's provision file.
#[derive(Deserialize)]
struct ProvisionTemplate {
    #[serde(default, deserialize_with = "crate::nullable")]
    profile: String,
    #[serde(default, deserialize_with = "crate::nullable")]
    user: String,
    #[serde(default)]
    policy: Option<Value>,
    #[serde(default)]
    preferences: Option<Value>,
}

impl ProvisionTemplate {
    /// The provision file for one token: auxp's type, the one `aux provision`
    /// reads.
    fn file(&self, url: &str, token: &str, node_name: &str) -> Provision {
        let node_name = (!node_name.is_empty()).then(|| node_name.to_string());
        Provision {
            version: auxp::provision::VERSION,
            profile: self.profile.clone(),
            hostname: node_name.clone(),
            user: Some(self.user.clone()),
            command: Some(CommandJoin {
                url: url.to_string(),
                enrollment_token: token.to_string(),
                node_name,
            }),
            policy: self.policy.clone(),
            preferences: self.preferences.clone(),
        }
    }
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct TokensRequest {
    #[serde(deserialize_with = "crate::nullable")]
    organization_id: String,
    #[serde(deserialize_with = "crate::nullable")]
    node_name: String,
    #[serde(deserialize_with = "crate::nullable")]
    count: i64,
    #[serde(deserialize_with = "crate::nullable")]
    ttl_minutes: i64,
    #[serde(deserialize_with = "crate::nullable")]
    require_approval: bool,
    /// With it, each token comes with a ready provision file.
    provision: Option<ProvisionTemplate>,
}

/// Creates enrollment tokens (operators with node.enroll).
async fn issue_tokens(State(s): AppState, headers: HeaderMap, body: Bytes) -> Response {
    let operator = match s.operator(&headers).await {
        Ok(p) => p,
        Err(denied) => return *denied,
    };
    if !operator.can_somewhere(auth::NODE_ENROLL) {
        return error(StatusCode::FORBIDDEN, "node.enroll capability required");
    }
    let Some(request) = parse::<TokensRequest>(&body, 64 << 10) else {
        return error(StatusCode::BAD_REQUEST, "invalid JSON");
    };
    if !operator.can(&request.organization_id, auth::NODE_ENROLL) {
        return forbidden(auth::NODE_ENROLL, &request.organization_id);
    }
    let base = s.base_url(&headers);
    let (org, require_approval) = (request.organization_id.clone(), request.require_approval);
    if let Some(template) = &request.provision {
        if template.user.trim().is_empty() {
            return error(
                StatusCode::BAD_REQUEST,
                "provision.user is required: the account whose agents run on the machine",
            );
        }
        // Checked before any token exists, with a token of the right shape.
        let sample = enrollment::token(&s.instance, &"0".repeat(enrollment::SECRET_HEX_LEN));
        let node_name = request.node_name.trim().to_lowercase();
        if let Err(e) = template.file(&base, &sample, &node_name).validate() {
            return error(StatusCode::BAD_REQUEST, &format!("provision: {e}"));
        }
    }
    let tokens = s
        .enroll
        .issue_tokens(
            TokenRequest {
                organization_id: request.organization_id,
                node_name: request.node_name,
                count: request.count,
                ttl: chrono::Duration::minutes(request.ttl_minutes),
                require_approval: request.require_approval,
                created_by: operator.subject.clone(),
            },
            Utc::now(),
        )
        .await;
    let tokens = match tokens {
        Ok(tokens) => tokens,
        Err(e) => return error(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    let names: Vec<&str> = tokens.iter().map(|t| t.node_name.as_str()).collect();
    s.audit(
        &org,
        "node.enrollment_tokens.issued",
        &operator,
        json!({"count": tokens.len(), "nodeNames": names, "requireApproval": require_approval}),
    )
    .await;
    let Some(template) = request.provision else {
        return reply(
            StatusCode::CREATED,
            json!({"instanceId": s.instance, "tokens": tokens}),
        );
    };
    let out: Vec<Value> = tokens
        .iter()
        .map(|token| {
            let file = template.file(&base, &token.token, &token.node_name);
            json!({"token": token.token, "expiresAt": token.expires_at, "nodeName": token.node_name, "provision": file})
        })
        .collect();
    reply(
        StatusCode::CREATED,
        json!({"instanceId": s.instance, "tokens": out}),
    )
}

/// Every enrolled node and its state.
async fn list_enrollments(State(s): AppState, headers: HeaderMap) -> Response {
    let operator = match s.operator(&headers).await {
        Ok(p) => p,
        Err(denied) => return *denied,
    };
    if !operator.can_somewhere(auth::ORGANIZATION_READ) {
        return error(
            StatusCode::FORBIDDEN,
            "organization.read capability required",
        );
    }
    match s.enroll.credentials().await {
        Ok(mut credentials) => {
            credentials.retain(|c| operator.can(&c.organization_id, auth::ORGANIZATION_READ));
            reply(
                StatusCode::OK,
                json!({"instanceId": s.instance, "nodes": credentials}),
            )
        }
        Err(_) => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "enrollment store unavailable",
        ),
    }
}

async fn approve_node(
    state: AppState,
    Path(node_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    decide_node(state, node_id, headers, "approve").await
}

async fn revoke_node(state: AppState, Path(node_id): Path<String>, headers: HeaderMap) -> Response {
    decide_node(state, node_id, headers, "revoke").await
}

/// Approves a node waiting for approval, or revokes a node.
async fn decide_node(
    State(s): AppState,
    node_id: String,
    headers: HeaderMap,
    action: &str,
) -> Response {
    let operator = match s.operator(&headers).await {
        Ok(p) => p,
        Err(denied) => return *denied,
    };
    if !operator.can_somewhere(auth::NODE_ENROLL) {
        return error(StatusCode::FORBIDDEN, "node.enroll capability required");
    }
    let org = match s.node_organization(&node_id).await {
        Some(org) if operator.can(&org, auth::NODE_ENROLL) => org,
        Some(org) if operator.can(&org, auth::ORGANIZATION_READ) => {
            return forbidden(auth::NODE_ENROLL, &org);
        }
        _ => return error(StatusCode::NOT_FOUND, "unknown node"),
    };
    let result = if action == "approve" {
        s.enroll.approve(&node_id, Utc::now()).await
    } else {
        let result = s.enroll.revoke(&node_id, Utc::now()).await;
        if result.is_ok() {
            s.hub.disconnect(&node_id);
        }
        result
    };
    if result.is_ok() {
        let event_type = if action == "approve" {
            "node.approved"
        } else {
            "node.revoked"
        };
        s.audit(&org, event_type, &operator, json!({"nodeId": node_id}))
            .await;
    }
    match result {
        Ok(()) => reply(
            StatusCode::OK,
            json!({"nodeId": node_id, "status": format!("{action}d")}),
        ),
        Err(enroll::Error::UnknownNode) => error(StatusCode::NOT_FOUND, "unknown node"),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
    }
}

#[derive(Deserialize)]
struct ProjectionQuery {
    #[serde(rename = "organizationId", default)]
    organization_id: String,
}

async fn projection(
    State(s): AppState,
    Query(query): Query<ProjectionQuery>,
    headers: HeaderMap,
) -> Response {
    let operator = match s.operator(&headers).await {
        Ok(p) => p,
        Err(denied) => return *denied,
    };
    // Without organizationId: the caller's one organization, or the demo's.
    let organization_id = match query.organization_id.as_str() {
        "" => match operator.organizations_with(auth::ORGANIZATION_READ) {
            None => "org-demo".to_string(),
            Some(orgs) if orgs.len() == 1 => orgs.into_iter().next().unwrap_or_default(),
            Some(_) => return error(StatusCode::BAD_REQUEST, "organizationId is required"),
        },
        id => id.to_string(),
    };
    if !operator.can(&organization_id, auth::ORGANIZATION_READ) {
        return forbidden(auth::ORGANIZATION_READ, &organization_id);
    }
    let organization_id = organization_id.as_str();
    match s.office.get_projection(organization_id).await {
        Ok(Some(mut projection)) => {
            let nodes = s
                .registry
                .list()
                .into_iter()
                .filter(|n| n.organization_id == organization_id)
                .filter_map(|n| {
                    let mut node = json!({
                        "id": n.id,
                        "status": n.status,
                        "presence": n.presence,
                        "connected": s.hub.connected(&n.id),
                        "agentuxVersion": n.agentux_version,
                    });
                    if !n.last_seen_at.is_empty() {
                        node["lastHeartbeatAt"] = Value::String(n.last_seen_at);
                    }
                    node.as_object().cloned()
                })
                .collect();
            office::with_nodes(&mut projection, nodes);
            reply(
                StatusCode::OK,
                serde_json::to_value(projection).unwrap_or_default(),
            )
        }
        Ok(None) => error(StatusCode::NOT_FOUND, "projection_not_found"),
        Err(_) => error(StatusCode::SERVICE_UNAVAILABLE, "projection_unavailable"),
    }
}

async fn resolve_approval(
    State(s): AppState,
    Path(approval_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let operator = match s.operator(&headers).await {
        Ok(p) => p,
        Err(denied) => return *denied,
    };
    if !operator.can_somewhere(auth::APPROVAL_DECIDE) {
        return error(StatusCode::FORBIDDEN, "approval.decide capability required");
    }
    let Some(mut request) = parse::<ResolutionRequest>(&body, 64 << 10) else {
        return error(StatusCode::BAD_REQUEST, "invalid JSON");
    };
    let actor = Actor::of(&operator.subject);
    request.actor = serde_json::to_value(&actor).ok();
    // A human-required gate of a mission is decided by a person.
    let engine = s.engine();
    let policy = engine
        .gate_policy(&request.organization_id, &approval_id)
        .await;
    if policy.as_deref() == Some("human-required") && actor.kind == "service" {
        return error(
            StatusCode::FORBIDDEN,
            "this gate is human-required: a person decides it, not a service token",
        );
    }
    let (org, decision, decider) = (
        request.organization_id.clone(),
        request.decision.clone(),
        request.actor.clone(),
    );
    // The approval is scoped to the request's organization, so deciding
    // needs the capability there.
    if !operator.can(&request.organization_id, auth::APPROVAL_DECIDE) {
        return forbidden(auth::APPROVAL_DECIDE, &request.organization_id);
    }
    // A node's own approval (a tool call, a plan) goes back to that node.
    let node_approval = s
        .office
        .get_projection(&org)
        .await
        .ok()
        .flatten()
        .and_then(|p| {
            p.approvals
                .into_iter()
                .find(|a| a.get("id").and_then(Value::as_str) == Some(approval_id.as_str()))
        })
        .filter(|a| {
            a.get("requestId").is_some()
                && a.get("nodeId")
                    .and_then(Value::as_str)
                    .is_some_and(|n| !n.starts_with("command"))
        });
    let reason = request.reason.clone();
    let key = request.idempotency_key.clone();
    let resolved = s.office.resolve_approval(&approval_id, request).await;
    if let Ok(response) = &resolved {
        if !response.idempotent_replay
            && let Err(e) = engine
                .decided(&org, &approval_id, &decision, decider, &NodesVia(&s))
                .await
        {
            eprintln!("agentux-command: missions: {e}");
        }
        s.changed.notify_waiters();
    }
    match resolved {
        Ok(response) => {
            let mut body = serde_json::to_value(&response).unwrap_or_default();
            if let Some(approval) = node_approval
                && !response.idempotent_replay
            {
                body["delivery"] = deliver(&s, &approval, &decision, &reason, &key).await;
            }
            reply(StatusCode::OK, body)
        }
        Err(office::Error::Conflict(conflict)) => reply(
            StatusCode::CONFLICT,
            json!({"ok": false, "conflict": conflict}),
        ),
        Err(_) => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "approval_store_unavailable",
        ),
    }
}

#[derive(Deserialize)]
struct ServiceTokensQuery {
    #[serde(rename = "organizationId", default)]
    organization_id: String,
}

/// Issues an operator service token (auth.admin in its organization). The
/// secret is in this answer only.
async fn issue_service_token(State(s): AppState, headers: HeaderMap, body: Bytes) -> Response {
    let operator = match s.operator(&headers).await {
        Ok(p) => p,
        Err(denied) => return *denied,
    };
    let Some(mut request) = parse::<auth::TokenRequest>(&body, 16 << 10) else {
        return error(StatusCode::BAD_REQUEST, "invalid JSON");
    };
    if !operator.can(&request.organization_id, auth::AUTH_ADMIN) {
        return forbidden(auth::AUTH_ADMIN, &request.organization_id);
    }
    request.created_by = operator.subject.clone();
    let org = request.organization_id.clone();
    match s.service_tokens.issue(request, Utc::now()).await {
        Ok(issued) => {
            s.audit(
                &org,
                "auth.service_token.issued",
                &operator,
                json!({"tokenId": issued.token.id, "name": issued.token.name, "capabilities": issued.token.capabilities}),
            )
            .await;
            reply(
                StatusCode::CREATED,
                serde_json::to_value(issued).unwrap_or_default(),
            )
        }
        Err(auth::Error::Invalid(e)) => error(StatusCode::BAD_REQUEST, &e),
        Err(_) => error(StatusCode::SERVICE_UNAVAILABLE, "token store unavailable"),
    }
}

/// An organization's service tokens, without their secrets.
async fn list_service_tokens(
    State(s): AppState,
    Query(query): Query<ServiceTokensQuery>,
    headers: HeaderMap,
) -> Response {
    let operator = match s.operator(&headers).await {
        Ok(p) => p,
        Err(denied) => return *denied,
    };
    if !operator.can(&query.organization_id, auth::AUTH_ADMIN) {
        return forbidden(auth::AUTH_ADMIN, &query.organization_id);
    }
    match s.service_tokens.list(&query.organization_id).await {
        Ok(tokens) => reply(StatusCode::OK, json!({"tokens": tokens})),
        Err(_) => error(StatusCode::SERVICE_UNAVAILABLE, "token store unavailable"),
    }
}

/// Revokes a service token at once.
async fn revoke_service_token(
    State(s): AppState,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let operator = match s.operator(&headers).await {
        Ok(p) => p,
        Err(denied) => return *denied,
    };
    let token = match s.service_tokens.get(&id).await {
        Ok(token) if operator.can(&token.organization_id, auth::AUTH_ADMIN) => token,
        Ok(_) | Err(auth::Error::NotFound) => {
            return error(StatusCode::NOT_FOUND, "unknown service token");
        }
        Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, "token store unavailable"),
    };
    match s.service_tokens.revoke(&token.id, Utc::now()).await {
        Ok(()) => {
            s.audit(
                &token.organization_id,
                "auth.service_token.revoked",
                &operator,
                json!({"tokenId": token.id, "name": token.name}),
            )
            .await;
            reply(StatusCode::OK, json!({"id": token.id, "status": "revoked"}))
        }
        Err(_) => error(StatusCode::SERVICE_UNAVAILABLE, "token store unavailable"),
    }
}

#[derive(Deserialize)]
struct LoginQuery {
    #[serde(rename = "returnTo", default)]
    return_to: String,
}

/// Starts a sign-in: sends the browser to the identity provider.
async fn login(State(s): AppState, Query(query): Query<LoginQuery>) -> Response {
    let Some(provider) = &s.oidc else {
        return error(
            StatusCode::NOT_FOUND,
            "sign-in is not configured on this server",
        );
    };
    match provider.login_url(&query.return_to).await {
        Ok(url) => redirect(&url, None),
        Err(e) => error(StatusCode::BAD_GATEWAY, &e.to_string()),
    }
}

#[derive(Deserialize)]
struct CallbackQuery {
    #[serde(default)]
    code: String,
    #[serde(default)]
    state: String,
    #[serde(default)]
    error: String,
}

/// The provider sends the browser back here: checks the sign-in, starts a
/// session and sets its cookie.
async fn callback(
    State(s): AppState,
    Query(query): Query<CallbackQuery>,
    headers: HeaderMap,
) -> Response {
    let Some(provider) = &s.oidc else {
        return error(
            StatusCode::NOT_FOUND,
            "sign-in is not configured on this server",
        );
    };
    if !query.error.is_empty() {
        return error(
            StatusCode::UNAUTHORIZED,
            &format!("the identity provider refused the sign-in: {}", query.error),
        );
    }
    let signed_in = match provider.callback(&query.code, &query.state).await {
        Ok(signed_in) => signed_in,
        Err(e @ oidc::Error::UnknownState) => {
            return error(StatusCode::BAD_REQUEST, &e.to_string());
        }
        Err(e @ oidc::Error::InvalidToken(_)) => {
            return error(StatusCode::UNAUTHORIZED, &e.to_string());
        }
        Err(e @ oidc::Error::NoAccess(_)) => return error(StatusCode::FORBIDDEN, &e.to_string()),
        Err(e) => return error(StatusCode::BAD_GATEWAY, &e.to_string()),
    };
    let secret = match s.sessions.start(&signed_in.principal, Utc::now()).await {
        Ok(secret) => secret,
        Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, "session store unavailable"),
    };
    let secure = if s.base_url(&headers).starts_with("https://") {
        "; Secure"
    } else {
        ""
    };
    let cookie = format!(
        "{SESSION_COOKIE}={secret}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}{secure}",
        auth::SESSION_HOURS * 3600
    );
    redirect(&signed_in.return_to, Some(cookie))
}

/// Ends the caller's session and clears its cookie.
async fn logout(State(s): AppState, headers: HeaderMap) -> Response {
    let secret = match bearer(&headers) {
        "" => session_cookie(&headers).unwrap_or(""),
        given => given,
    };
    if secret.starts_with(auth::SESSION_PREFIX) {
        let _ = s.sessions.end(secret).await;
    }
    let mut response = reply(StatusCode::OK, json!({"status": "signed out"}));
    if let Ok(value) = HeaderValue::from_str(&format!(
        "{SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0"
    )) {
        response.headers_mut().insert(header::SET_COOKIE, value);
    }
    response
}

/// Who the caller is and what it may do, for the console and the Office.
async fn me(State(s): AppState, headers: HeaderMap) -> Response {
    let operator = match s.operator(&headers).await {
        Ok(p) => p,
        Err(denied) => return *denied,
    };
    reply(
        StatusCode::OK,
        json!({"subject": operator.subject, "organizations": operator.grants(), "signIn": s.oidc.is_some()}),
    )
}

/// A 303 to `location`, optionally setting a cookie.
fn redirect(location: &str, cookie: Option<String>) -> Response {
    let mut response = StatusCode::SEE_OTHER.into_response();
    if let Ok(value) = HeaderValue::from_str(location) {
        response.headers_mut().insert(header::LOCATION, value);
    }
    if let Some(value) = cookie.and_then(|c| HeaderValue::from_str(&c).ok()) {
        response.headers_mut().insert(header::SET_COOKIE, value);
    }
    response
}

/// The 4xx/5xx answer for a definition store error.
fn definition_error(e: definitions::Error) -> Response {
    match e {
        definitions::Error::NotFound => error(StatusCode::NOT_FOUND, &e.to_string()),
        definitions::Error::Published => error(StatusCode::CONFLICT, &e.to_string()),
        definitions::Error::Invalid(problems) => reply(
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({"error": "the definition does not validate", "problems": problems}),
        ),
        definitions::Error::Stale { current } => reply(
            StatusCode::CONFLICT,
            json!({"error": "stale_draft", "message": e.to_string(), "currentRevision": current}),
        ),
        definitions::Error::ReviewRequired => reply(
            StatusCode::CONFLICT,
            json!({"error": "review_required", "message": e.to_string()}),
        ),
        definitions::Error::OwnReview => error(StatusCode::FORBIDDEN, &e.to_string()),
        definitions::Error::Storage(_) => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "definition store unavailable",
        ),
    }
}

/// The revision an `If-Match` header names (`"3"`, `3` or `W/"3"`).
fn if_match(headers: &HeaderMap) -> Option<i64> {
    let value = headers.get(header::IF_MATCH)?.to_str().ok()?.trim();
    value
        .trim_start_matches("W/")
        .trim_matches('"')
        .parse()
        .ok()
}

/// A definition, with its revision as the `ETag` editors send back in
/// `If-Match`.
fn definition_reply(status: StatusCode, definition: &definitions::Definition) -> Response {
    let mut response = reply(status, serde_json::to_value(definition).unwrap_or_default());
    if let Ok(tag) = HeaderValue::from_str(&format!("\"{}\"", definition.revision)) {
        response.headers_mut().insert(header::ETAG, tag);
    }
    response
}

/// The operator, when it holds `capability` in `org`; else the answer.
async fn operator_in(
    s: &Inner,
    headers: &HeaderMap,
    org: &str,
    capability: &str,
) -> Result<Principal, Box<Response>> {
    let operator = s.operator(headers).await?;
    if !operator.can(org, capability) {
        return Err(Box::new(forbidden(capability, org)));
    }
    Ok(operator)
}

/// Every version of an organization's definition (without documents).
async fn list_definitions(
    State(s): AppState,
    Path(org): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(denied) = operator_in(&s, &headers, &org, auth::ORGANIZATION_READ).await {
        return *denied;
    }
    match s.definitions.list(&org).await {
        Ok(versions) => reply(
            StatusCode::OK,
            json!({"organizationId": org, "definitions": versions}),
        ),
        Err(e) => definition_error(e),
    }
}

/// One version, with its document and validation result.
async fn get_definition(
    State(s): AppState,
    Path((org, version)): Path<(String, i64)>,
    headers: HeaderMap,
) -> Response {
    if let Err(denied) = operator_in(&s, &headers, &org, auth::ORGANIZATION_READ).await {
        return *denied;
    }
    match s.definitions.get(&org, version).await {
        Ok(definition) => definition_reply(StatusCode::OK, &definition),
        Err(e) => definition_error(e),
    }
}

/// A new draft, validated (the problems come back; it is kept either way).
#[derive(Deserialize)]
struct DraftQuery {
    /// Start from this version's document (a rollback, or the next change
    /// to the published one) instead of the body.
    #[serde(default)]
    from: Option<i64>,
}

async fn draft_definition(
    State(s): AppState,
    Path(org): Path<String>,
    Query(query): Query<DraftQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let operator = match operator_in(&s, &headers, &org, auth::ORGANIZATION_EDIT).await {
        Ok(p) => p,
        Err(denied) => return *denied,
    };
    let document = match query.from {
        Some(version) => match s.definitions.get(&org, version).await {
            Ok(definition) => definition.document,
            Err(e) => return definition_error(e),
        },
        None => match parse::<Value>(&body, 1 << 20) {
            Some(document) => document,
            None => return error(StatusCode::BAD_REQUEST, "invalid JSON"),
        },
    };
    match s
        .definitions
        .draft(&org, document, &Actor::of(&operator.subject), Utc::now())
        .await
    {
        Ok(definition) => {
            s.audit(
                &org,
                "organization.definition.drafted",
                &operator,
                json!({"definitionVersion": definition.definition_version, "problems": definition.problems.len()}),
            )
            .await;
            definition_reply(StatusCode::CREATED, &definition)
        }
        Err(e) => definition_error(e),
    }
}

/// Replaces a draft; a published version answers 409.
async fn replace_definition(
    State(s): AppState,
    Path((org, version)): Path<(String, i64)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let operator = match operator_in(&s, &headers, &org, auth::ORGANIZATION_EDIT).await {
        Ok(p) => p,
        Err(denied) => return *denied,
    };
    let Some(document) = parse::<Value>(&body, 1 << 20) else {
        return error(StatusCode::BAD_REQUEST, "invalid JSON");
    };
    match s
        .definitions
        .replace_at(
            &org,
            version,
            document,
            &Actor::of(&operator.subject),
            Utc::now(),
            if_match(&headers),
        )
        .await
    {
        Ok(definition) => {
            s.audit(
                &org,
                "organization.definition.replaced",
                &operator,
                json!({"definitionVersion": definition.definition_version, "problems": definition.problems.len()}),
            )
            .await;
            definition_reply(StatusCode::OK, &definition)
        }
        Err(e) => definition_error(e),
    }
}

/// Publishes a draft that validates, and tells the Office projection.
async fn publish_definition(
    State(s): AppState,
    Path((org, version)): Path<(String, i64)>,
    headers: HeaderMap,
) -> Response {
    let operator = match operator_in(&s, &headers, &org, auth::ORGANIZATION_PUBLISH).await {
        Ok(p) => p,
        Err(denied) => return *denied,
    };
    let published = match s
        .definitions
        .publish_at(
            &org,
            version,
            &Actor::of(&operator.subject),
            Utc::now(),
            if_match(&headers),
        )
        .await
    {
        Ok(definition) => definition,
        Err(e) => return definition_error(e),
    };
    // The PostgreSQL store took the event with the publication; taking it
    // again is a no-op there, and the memory store takes it here.
    let event = definitions::published_event(&published);
    if let Err(e) = s.office.apply_event(event).await {
        eprintln!(
            "agentux-command: definition {org} v{version} published, but the Office projection did not take it: {e}"
        );
    }
    s.changed.notify_waiters();
    reply(
        StatusCode::OK,
        serde_json::to_value(published).unwrap_or_default(),
    )
}

#[derive(Deserialize)]
struct AuditQuery {
    #[serde(default)]
    limit: Option<i64>,
}

/// The organization's audit log, newest first.
async fn organization_audit(
    State(s): AppState,
    Path(org): Path<String>,
    Query(query): Query<AuditQuery>,
    headers: HeaderMap,
) -> Response {
    if let Err(denied) = operator_in(&s, &headers, &org, auth::ORGANIZATION_READ).await {
        return *denied;
    }
    let limit = query.limit.unwrap_or(100).clamp(1, 1000);
    match s.definitions.audit(&org, limit).await {
        Ok(entries) => reply(
            StatusCode::OK,
            json!({"organizationId": org, "entries": entries}),
        ),
        Err(e) => definition_error(e),
    }
}

#[derive(Deserialize)]
struct EventsQuery {
    #[serde(default)]
    after: Option<i64>,
    #[serde(default)]
    since: Option<String>,
    #[serde(default)]
    until: Option<String>,
    #[serde(default)]
    limit: Option<i64>,
}

/// Replays the organization's event log: after a cursor, within a time range
/// (`since` inclusive, `until` exclusive, RFC 3339), at most `limit` (100 by
/// default, 1000 at most). `next` is the cursor to continue from.
async fn organization_events(
    State(s): AppState,
    Path(org): Path<String>,
    Query(query): Query<EventsQuery>,
    headers: HeaderMap,
) -> Response {
    if let Err(denied) = operator_in(&s, &headers, &org, auth::ORGANIZATION_READ).await {
        return *denied;
    }
    let time = |value: &Option<String>| match value.as_deref() {
        None | Some("") => Ok(None),
        Some(text) => chrono::DateTime::parse_from_rfc3339(text)
            .map(|t| Some(t.with_timezone(&Utc)))
            .map_err(|_| ()),
    };
    let (Ok(since), Ok(until)) = (time(&query.since), time(&query.until)) else {
        return error(
            StatusCode::BAD_REQUEST,
            "since and until are RFC 3339 times",
        );
    };
    let replay = office::Replay {
        after: query.after.unwrap_or(0).max(0),
        since,
        until,
        limit: query.limit.unwrap_or(100).clamp(1, 1000),
    };
    match s.office.events(&org, &replay).await {
        Ok(events) => {
            let next = events.last().map_or(replay.after, |e| e.cursor);
            reply(
                StatusCode::OK,
                json!({"organizationId": org, "events": events, "next": next}),
            )
        }
        Err(_) => error(StatusCode::SERVICE_UNAVAILABLE, "event log unavailable"),
    }
}

#[derive(Deserialize)]
struct StreamQuery {
    #[serde(default)]
    after: Option<i64>,
}

/// Events taken from the log per read.
const STREAM_BATCH: i64 = 500;
/// How long a stream waits for a wake-up before it looks anyway: events
/// another Command instance took wake nobody here.
const STREAM_POLL: Duration = Duration::from_secs(5);

/// The organization's events, live, as server-sent events: the log after a
/// cursor (`after`, or the `Last-Event-ID` a reconnecting `EventSource`
/// sends), then each event as Command takes it. Each message is one event:
/// `id` its cursor, `event: office.event`, `data` the event (clients
/// deduplicate by `eventId`; delivery is at least once). `event: unavailable`
/// says the log cannot be read right now; the stream goes on trying. A slow
/// reader is never sent more than it reads.
async fn stream_events(
    State(s): AppState,
    Path(org): Path<String>,
    Query(query): Query<StreamQuery>,
    headers: HeaderMap,
) -> Response {
    use axum::response::sse::{Event as Sse, KeepAlive};
    use std::collections::VecDeque;
    if let Err(denied) = operator_in(&s, &headers, &org, auth::ORGANIZATION_READ).await {
        return *denied;
    }
    let after = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<i64>().ok())
        .or(query.after)
        .unwrap_or(0)
        .max(0);
    let start = (s, org, after, VecDeque::<Sse>::new());
    let stream =
        futures_util::stream::unfold(start, |(s, org, mut after, mut pending)| async move {
            loop {
                if let Some(next) = pending.pop_front() {
                    return Some((
                        Ok::<_, std::convert::Infallible>(next),
                        (s, org, after, pending),
                    ));
                }
                let unavailable = {
                    // Listen before reading, so an event taken in between
                    // wakes us.
                    let changed = s.changed.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    let replay = office::Replay {
                        after,
                        limit: STREAM_BATCH,
                        ..office::Replay::default()
                    };
                    match s.office.events(&org, &replay).await {
                        Ok(batch) if !batch.is_empty() => {
                            for logged in batch {
                                after = logged.cursor;
                                let message = Sse::default()
                                    .id(logged.cursor.to_string())
                                    .event("office.event")
                                    .json_data(&logged.event)
                                    .unwrap_or_default();
                                pending.push_back(message);
                            }
                            false
                        }
                        Ok(_) => {
                            tokio::select! {
                                _ = changed => {}
                                _ = tokio::time::sleep(STREAM_POLL) => {}
                            }
                            false
                        }
                        Err(_) => true,
                    }
                };
                if unavailable {
                    tokio::time::sleep(STREAM_POLL).await;
                    let message = Sse::default()
                        .event("unavailable")
                        .data("the event log is unavailable");
                    return Some((Ok(message), (s, org, after, pending)));
                }
            }
        });
    axum::response::sse::Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

/// The engine's way to nodes: the command channel, with the AUXP envelope
/// every command carries.
struct NodesVia<'a>(&'a Inner);

/// How long the engine waits for a node to answer a command.
const ENGINE_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

#[async_trait::async_trait]
impl missions::Nodes for NodesVia<'_> {
    fn pick(&self, organization_id: &str) -> Option<String> {
        let mut nodes: Vec<String> = self
            .0
            .registry
            .list()
            .into_iter()
            .filter(|n| n.organization_id == organization_id && n.presence == "online")
            .map(|n| n.id)
            .filter(|id| self.0.hub.connected(id))
            .collect();
        nodes.sort();
        nodes.into_iter().next()
    }

    async fn command(
        &self,
        node_id: &str,
        method: &str,
        mut params: Map<String, Value>,
    ) -> Result<Value, missions::NodeError> {
        use missions::NodeError;
        let s = self.0;
        let organization = s
            .registry
            .get(node_id)
            .map(|n| n.organization_id)
            .unwrap_or_default();
        let command_id = params
            .get("commandId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        for (key, value) in [
            ("protocolVersion", "v1".to_string()),
            ("organizationId", organization),
            ("nodeId", node_id.to_string()),
            ("messageId", command_id),
            (
                "occurredAt",
                Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            ),
            ("commandInstanceId", s.instance.clone()),
        ] {
            params.insert(key.into(), Value::String(value));
        }
        match tokio::time::timeout(ENGINE_COMMAND_TIMEOUT, s.hub.send(node_id, method, params))
            .await
        {
            Err(_) => Err(NodeError::Unreachable(format!(
                "node {node_id} did not answer in time"
            ))),
            Ok(Err(e)) => Err(NodeError::Unreachable(format!("node {node_id}: {e}"))),
            Ok(Ok(answer)) => match answer.error {
                Some(e) if e.code == auxp::codes::HELD => Err(NodeError::Held(e.message)),
                Some(e) => Err(NodeError::Refused(e.message)),
                None => Ok(answer.result.unwrap_or(Value::Null)),
            },
        }
    }
}

/// The 4xx/5xx answer for a mission engine error.
fn mission_error(e: missions::Error) -> Response {
    match e {
        missions::Error::NotFound => error(StatusCode::NOT_FOUND, "run not found"),
        missions::Error::Invalid(m) => error(StatusCode::UNPROCESSABLE_ENTITY, &m),
        missions::Error::Conflict => {
            error(StatusCode::CONFLICT, "the run changed meanwhile; try again")
        }
        missions::Error::Storage(_) => {
            error(StatusCode::SERVICE_UNAVAILABLE, "run store unavailable")
        }
    }
}

/// Starts a mission (`node.command`, which covers `run.start` in ADR 0015's
/// mapping) under the organization's newest published definition.
async fn start_run(
    State(s): AppState,
    Path(org): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let operator = match operator_in(&s, &headers, &org, auth::NODE_COMMAND).await {
        Ok(p) => p,
        Err(denied) => return *denied,
    };
    let Some(input) = parse::<missions::Input>(&body, 1 << 20) else {
        return error(StatusCode::BAD_REQUEST, "invalid JSON");
    };
    match s
        .engine()
        .start(&org, input, &Actor::of(&operator.subject), &NodesVia(&s))
        .await
    {
        Ok(run) => reply(
            StatusCode::CREATED,
            serde_json::to_value(run).unwrap_or_default(),
        ),
        Err(e) => mission_error(e),
    }
}

/// The organization's runs, newest first.
async fn list_runs(State(s): AppState, Path(org): Path<String>, headers: HeaderMap) -> Response {
    if let Err(denied) = operator_in(&s, &headers, &org, auth::ORGANIZATION_READ).await {
        return *denied;
    }
    match s.runs.list(&org).await {
        Ok(runs) => reply(StatusCode::OK, json!({"organizationId": org, "runs": runs})),
        Err(e) => mission_error(e),
    }
}

async fn get_run(
    State(s): AppState,
    Path((org, run)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if let Err(denied) = operator_in(&s, &headers, &org, auth::ORGANIZATION_READ).await {
        return *denied;
    }
    match s.runs.get(&org, &run).await {
        Ok(run) => reply(
            StatusCode::OK,
            serde_json::to_value(run).unwrap_or_default(),
        ),
        Err(e) => mission_error(e),
    }
}

/// `pause`, `resume` (a failed run retries its step) or `cancel`.
async fn control_run(
    State(s): AppState,
    Path((org, run, action)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    let control = match action.as_str() {
        "pause" => missions::Control::Pause,
        "resume" => missions::Control::Resume,
        "cancel" => missions::Control::Cancel,
        _ => return error(StatusCode::NOT_FOUND, "unknown action"),
    };
    let operator = match operator_in(&s, &headers, &org, auth::NODE_COMMAND).await {
        Ok(p) => p,
        Err(denied) => return *denied,
    };
    match s
        .engine()
        .control(
            &org,
            &run,
            control,
            &Actor::of(&operator.subject),
            &NodesVia(&s),
        )
        .await
    {
        Ok(run) => reply(
            StatusCode::OK,
            serde_json::to_value(run).unwrap_or_default(),
        ),
        Err(missions::Error::Invalid(m)) => error(StatusCode::CONFLICT, &m),
        Err(e) => mission_error(e),
    }
}

/// Fields of an event's data the audit export leaves out: what people and
/// agents wrote, which an audit needs to know happened, not to read.
const REDACTED: &[&str] = &["prompt", "detail", "message", "body", "answer", "reason"];

/// `event` as the audit export shows it.
fn redacted(mut event: office::Event) -> office::Event {
    if let Some(Value::Object(data)) = &mut event.data {
        for key in REDACTED {
            if let Some(value) = data.get_mut(*key)
                && !value.is_null()
            {
                *value = Value::String("[redacted]".into());
            }
        }
    }
    event
}

/// sha256 of `prev` and the compact JSON of `record`, in hex.
pub fn chain_hash(prev: &str, record: &Value) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(prev.as_bytes());
    hasher.update(b"\n");
    hasher.update(record.to_string().as_bytes());
    hex::encode(hasher.finalize())
}

#[derive(Deserialize)]
struct ExportQuery {
    #[serde(default)]
    since: Option<String>,
    #[serde(default)]
    until: Option<String>,
}

/// The most events one export carries.
const EXPORT_LIMIT: usize = 100_000;

/// The organization's audit trail as NDJSON, tamper-evident: each line is
/// `{"cursor", "event", "prev", "hash"}` with `hash` = sha256(`prev` + "\n" +
/// the line's compact JSON without `hash`), `prev` the previous line's hash
/// (empty for the first); the last line is `{"organizationId", "count",
/// "digest", ...}`, `digest` the last hash. Prompts and other free text are
/// redacted. Needs `audit.read`; the export itself is audited.
async fn export_audit(
    State(s): AppState,
    Path(org): Path<String>,
    Query(query): Query<ExportQuery>,
    headers: HeaderMap,
) -> Response {
    let operator = match operator_in(&s, &headers, &org, auth::AUDIT_READ).await {
        Ok(p) => p,
        Err(denied) => return *denied,
    };
    let time = |value: &Option<String>| match value.as_deref() {
        None | Some("") => Ok(None),
        Some(text) => chrono::DateTime::parse_from_rfc3339(text)
            .map(|t| Some(t.with_timezone(&Utc)))
            .map_err(|_| ()),
    };
    let (Ok(since), Ok(until)) = (time(&query.since), time(&query.until)) else {
        return error(
            StatusCode::BAD_REQUEST,
            "since and until are RFC 3339 times",
        );
    };
    let mut out = String::new();
    let (mut prev, mut count, mut after) = (String::new(), 0usize, 0i64);
    loop {
        let replay = office::Replay {
            after,
            since,
            until,
            limit: 1000,
        };
        let batch = match s.office.events(&org, &replay).await {
            Ok(batch) => batch,
            Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, "event log unavailable"),
        };
        if batch.is_empty() || count >= EXPORT_LIMIT {
            break;
        }
        for logged in batch {
            after = logged.cursor;
            let mut record =
                json!({"cursor": logged.cursor, "event": redacted(logged.event), "prev": prev});
            let hash = chain_hash(&prev, &record);
            record["hash"] = Value::String(hash.clone());
            out.push_str(&record.to_string());
            out.push('\n');
            prev = hash;
            count += 1;
        }
    }
    let summary = json!({
        "organizationId": org,
        "count": count,
        "digest": prev,
        "truncated": count >= EXPORT_LIMIT,
        "since": query.since,
        "until": query.until,
        "exportedAt": crate::now_rfc3339(),
    });
    out.push_str(&summary.to_string());
    out.push('\n');
    s.audit(
        &org,
        "audit.exported",
        &operator,
        json!({"count": count, "digest": prev, "since": query.since, "until": query.until}),
    )
    .await;
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/x-ndjson")],
        out,
    )
        .into_response()
}

#[derive(Deserialize)]
struct ReviewRequest {
    #[serde(default)]
    decision: String,
    #[serde(default)]
    comment: String,
}

/// Reviews a draft's current revision (`approval.decide`): `approved` or
/// `changes_requested`. Nobody reviews their own changes; `If-Match` makes
/// sure the reviewer saw the revision they decide on.
async fn review_definition(
    State(s): AppState,
    Path((org, version)): Path<(String, i64)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let operator = match operator_in(&s, &headers, &org, auth::APPROVAL_DECIDE).await {
        Ok(p) => p,
        Err(denied) => return *denied,
    };
    let Some(request) = parse::<ReviewRequest>(&body, 64 << 10) else {
        return error(StatusCode::BAD_REQUEST, "invalid JSON");
    };
    let review = definitions::Review {
        reviewer: Actor::of(&operator.subject),
        decision: request.decision,
        comment: request.comment,
        revision: 0,
        reviewed_at: Utc::now(),
    };
    match s
        .definitions
        .review(&org, version, review, if_match(&headers))
        .await
    {
        Ok(definition) => {
            let last = definition.reviews.last();
            s.audit(
                &org,
                "organization.definition.reviewed",
                &operator,
                json!({
                    "definitionVersion": definition.definition_version,
                    "revision": definition.revision,
                    "decision": last.map(|r| r.decision.as_str()),
                    "comment": last.map(|r| r.comment.as_str()),
                }),
            )
            .await;
            definition_reply(StatusCode::OK, &definition)
        }
        Err(e) => definition_error(e),
    }
}

/// Sends a decision on a node's approval to that node (`request.resolve`):
/// what the delivery came to, for the decider.
async fn deliver(
    s: &Inner,
    approval: &Map<String, Value>,
    decision: &str,
    reason: &str,
    key: &str,
) -> Value {
    use missions::{NodeError, Nodes};
    let text = |k: &str| {
        approval
            .get(k)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let node = text("nodeId");
    let mut params = Map::new();
    for (k, v) in [
        ("commandId", format!("resolve-{}-{key}", text("id"))),
        ("runId", text("runId")),
        ("requestId", text("requestId")),
        (
            "resolution",
            if decision == "approved" {
                "approve"
            } else {
                "deny"
            }
            .to_string(),
        ),
    ] {
        params.insert(k.into(), Value::String(v));
    }
    if !reason.is_empty() {
        params.insert("answer".into(), Value::String(reason.into()));
    }
    match NodesVia(s).command(&node, "request.resolve", params).await {
        Ok(_) => json!({"nodeId": node, "delivered": true}),
        Err(NodeError::Unreachable(e) | NodeError::Held(e) | NodeError::Refused(e)) => {
            json!({"nodeId": node, "delivered": false, "error": e})
        }
    }
}
