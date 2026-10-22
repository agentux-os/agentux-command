//! Command's HTTP API: node messages and command connections, enrollment,
//! operator commands, and the Office projection.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::ws::rejection::WebSocketUpgradeRejection;
use axum::extract::{Path, Query, State, WebSocketUpgrade};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use subtle::ConstantTimeEq;

use crate::channel::{Hub, SendError};
use auxp::enrollment;
use auxp::provision::{CommandJoin, Provision};

use crate::enroll::{self, TokenRequest};
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
    /// The demo server: memory stores, open to nodes nobody enrolled.
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
            },
        }
    }

    /// The production server: PostgreSQL for the read model and enrollment,
    /// closed to nodes nobody enrolled.
    pub async fn postgres(database_url: &str) -> Result<Self, String> {
        let pool = office::open_postgres(database_url)
            .await
            .map_err(|e| e.to_string())?;
        let handler = Self::with_office(Arc::new(office::PostgresStore::new(pool.clone())))
            .with_enroll_store(Arc::new(enroll::Postgres::new(pool)))
            .await
            .map_err(|e| e.to_string())?;
        Ok(handler.with_open_nodes(false))
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
        Router::new()
            .route("/healthz", get(healthz))
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
            .fallback(not_found)
            .method_not_allowed_fallback(not_found)
            .layer(axum::extract::DefaultBodyLimit::max(MESSAGE_LIMIT))
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

/// What an operator's token allows. Only the demo tokens exist until
/// organization sessions (agentux-command#13).
fn has_capability(headers: &HeaderMap, capability: &str) -> bool {
    match headers.get("authorization").and_then(|v| v.to_str().ok()) {
        Some("Bearer demo-leader") => matches!(
            capability,
            "organization.read" | "approval.decide" | "node.command" | "node.enroll"
        ),
        Some("Bearer demo-observer") => capability == "organization.read",
        _ => false,
    }
}

fn authorized(headers: &HeaderMap) -> bool {
    matches!(
        headers.get("authorization").and_then(|v| v.to_str().ok()),
        Some("Bearer demo-leader" | "Bearer demo-observer")
    )
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

async fn healthz(State(s): AppState) -> Response {
    match s.office.ping().await {
        Ok(()) => reply(StatusCode::OK, json!({"status": "ok"})),
        Err(_) => reply(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"status": "unavailable"}),
        ),
    }
}

async fn instance(State(s): AppState) -> Response {
    reply(
        StatusCode::OK,
        json!({"product": "agentux-command", "instanceId": s.instance, "protocolVersions": ["v1"]}),
    )
}

async fn list_nodes(State(s): AppState, headers: HeaderMap) -> Response {
    // Node telemetry names repositories, agents and spend: operators only.
    if !has_capability(&headers, "organization.read") {
        return error(
            StatusCode::FORBIDDEN,
            "organization.read capability required",
        );
    }
    let mut list = s.registry.list();
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
        Access::Full => s.apply(&body),
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
    if !authorized(&headers) {
        return error(StatusCode::UNAUTHORIZED, "invalid credentials");
    }
    if !has_capability(&headers, "node.command") {
        return error(StatusCode::FORBIDDEN, "node.command capability required");
    }
    let Some(node) = s.registry.get(&node_id) else {
        return error(StatusCode::NOT_FOUND, "unknown node");
    };
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

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct RedeemRequest {
    #[serde(deserialize_with = "crate::nullable")]
    token: String,
    /// `null` from a gateway enrolling without a name.
    #[serde(deserialize_with = "crate::nullable")]
    node_name: String,
    #[serde(deserialize_with = "crate::nullable")]
    hostname: String,
}

/// Trades an enrollment token for the node's credential.
async fn redeem(State(s): AppState, body: Bytes) -> Response {
    let Some(request) = parse::<RedeemRequest>(&body, 16 << 10) else {
        return error(StatusCode::BAD_REQUEST, "invalid JSON");
    };
    match s
        .enroll
        .redeem(
            &request.token,
            &request.node_name,
            &request.hostname,
            Utc::now(),
        )
        .await
    {
        Ok(r) => reply(
            StatusCode::OK,
            json!({
                "instanceId": s.instance,
                "organizationId": r.credential.organization_id,
                "nodeId": r.credential.node_id,
                "nodeSecret": r.secret,
                "approved": r.credential.approved,
            }),
        ),
        Err(e @ enroll::Error::OtherInstance) => reply(
            StatusCode::CONFLICT,
            json!({"error": e.to_string(), "instanceId": s.instance}),
        ),
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
    if !authorized(&headers) || !has_capability(&headers, "node.enroll") {
        return error(StatusCode::FORBIDDEN, "node.enroll capability required");
    }
    let Some(request) = parse::<TokensRequest>(&body, 64 << 10) else {
        return error(StatusCode::BAD_REQUEST, "invalid JSON");
    };
    let base = s.base_url(&headers);
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
                created_by: String::new(),
            },
            Utc::now(),
        )
        .await;
    let tokens = match tokens {
        Ok(tokens) => tokens,
        Err(e) => return error(StatusCode::BAD_REQUEST, &e.to_string()),
    };
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
    if !authorized(&headers) || !has_capability(&headers, "organization.read") {
        return error(
            StatusCode::FORBIDDEN,
            "organization.read capability required",
        );
    }
    match s.enroll.credentials().await {
        Ok(credentials) => reply(
            StatusCode::OK,
            json!({"instanceId": s.instance, "nodes": credentials}),
        ),
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
    if !authorized(&headers) || !has_capability(&headers, "node.enroll") {
        return error(StatusCode::FORBIDDEN, "node.enroll capability required");
    }
    let result = if action == "approve" {
        s.enroll.approve(&node_id, Utc::now()).await
    } else {
        let result = s.enroll.revoke(&node_id, Utc::now()).await;
        if result.is_ok() {
            s.hub.disconnect(&node_id);
        }
        result
    };
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
    if !authorized(&headers) {
        return error(StatusCode::UNAUTHORIZED, "invalid credentials");
    }
    if !has_capability(&headers, "organization.read") {
        return error(
            StatusCode::FORBIDDEN,
            "organization.read capability required",
        );
    }
    let organization_id = match query.organization_id.as_str() {
        "" => "org-demo",
        id => id,
    };
    match s.office.get_projection(organization_id).await {
        Ok(Some(projection)) => reply(
            StatusCode::OK,
            serde_json::to_value(projection).unwrap_or_default(),
        ),
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
    if !authorized(&headers) {
        return error(StatusCode::UNAUTHORIZED, "invalid credentials");
    }
    if !has_capability(&headers, "approval.decide") {
        return error(StatusCode::FORBIDDEN, "approval.decide capability required");
    }
    let Some(request) = parse::<ResolutionRequest>(&body, 64 << 10) else {
        return error(StatusCode::BAD_REQUEST, "invalid JSON");
    };
    match s.office.resolve_approval(&approval_id, request).await {
        Ok(response) => reply(
            StatusCode::OK,
            serde_json::to_value(response).unwrap_or_default(),
        ),
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
