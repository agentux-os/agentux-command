//! The Office read model: an organization's projection, built from its event
//! log, and approval decisions.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Mutex, MutexGuard};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::now_rfc3339;

mod growth;
mod migrations;
mod postgres;

pub use growth::{
    DetailedGrowth, GrowthReport, OrganizationGrowth, PartitionGrowth, collect_growth_report,
};
pub use migrations::{migrate, migration_versions};
pub use postgres::{PostgresStore, append_event, open_postgres, pool, with_env_password};

/// Events kept in a projection; older ones stay in the log only.
const PROJECTION_EVENT_LIMIT: usize = 100;

#[async_trait]
pub trait OfficeRepository: Send + Sync {
    async fn ping(&self) -> Result<(), Error>;
    async fn get_projection(&self, organization_id: &str) -> Result<Option<Projection>, Error>;
    async fn apply_event(&self, event: Event) -> Result<(), Error>;
    /// Part of an organization's event log, oldest first.
    async fn events(&self, organization_id: &str, replay: &Replay) -> Result<Vec<Logged>, Error>;
    async fn resolve_approval(
        &self,
        approval_id: &str,
        request: ResolutionRequest,
    ) -> Result<ResolutionResponse, Error>;
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    #[serde(default)]
    pub event_id: String,
    #[serde(default)]
    pub event_type: String,
    #[serde(default)]
    pub organization_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub node_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub run_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub occurred_at: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub received_at: String,
    #[serde(default)]
    pub sequence: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub correlation_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub causation_id: String,
    /// Who caused it (ADR 0015): `{"kind": "user" | "service" | "operator" |
    /// "node", "id": …}`. A node's events without one are the node's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<Value>,
    /// The organization definition the event happened under, when known
    /// (`definition-<n>`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub definition_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// An event as the log keeps it: its place in the organization's log, for
/// replay from a cursor.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Logged {
    pub cursor: i64,
    pub event: Event,
}

/// Which part of an organization's log to replay: after a cursor, within a
/// time range (when the event was taken), at most `limit` events.
#[derive(Debug, Clone, Default)]
pub struct Replay {
    pub after: i64,
    pub since: Option<chrono::DateTime<chrono::Utc>>,
    pub until: Option<chrono::DateTime<chrono::Utc>>,
    pub limit: i64,
}

impl Replay {
    fn takes(&self, cursor: i64, event: &Event) -> bool {
        let at = chrono::DateTime::parse_from_rfc3339(&event.received_at).ok();
        cursor > self.after
            && self.since.is_none_or(|s| at.is_some_and(|at| at >= s))
            && self.until.is_none_or(|u| at.is_some_and(|at| at < u))
    }
}

/// An event without a definition version happened under the one the
/// organization's projection has, when it has one.
pub(crate) fn stamp_definition(event: &mut Event, projection: &Projection) {
    if event.definition_version.is_empty()
        && !projection.definition_version.is_empty()
        && projection.definition_version != "definition-unknown"
    {
        event.definition_version = projection.definition_version.clone();
    }
}

/// The organization's nodes as Command sees them now (each with `id` and
/// `presence`: `online`, `degraded` or `offline`) replace the projected ones,
/// and set the freshness: `fresh` while every node reports, `degraded` while
/// some do not, `stale` when none does, since then nothing can bring the
/// projection up to date. Without nodes the stored freshness stands.
pub fn with_nodes(projection: &mut Projection, nodes: Vec<Object>) {
    if nodes.is_empty() {
        return;
    }
    let online = nodes
        .iter()
        .filter(|n| n.get("presence").and_then(Value::as_str) == Some("online"))
        .count();
    projection.freshness = match online {
        n if n == nodes.len() => "fresh",
        0 => "stale",
        _ => "degraded",
    }
    .into();
    projection.nodes = nodes;
}

/// The envelope fields of an AUXP message's params; the rest is the event's
/// data.
const ENVELOPE: &[&str] = &[
    "protocolVersion",
    "organizationId",
    "nodeId",
    "messageId",
    "occurredAt",
    "eventId",
    "sequence",
    "correlationId",
    "causationId",
];

/// The node events the Office log takes: what runs, approvals, gates,
/// artifacts, alerts and commands do. Harness status and session traffic
/// stay in the node registry.
const NODE_EVENTS: &[&str] = &[
    "node.status_changed",
    "node.alert",
    "run.accepted",
    "run.started",
    "run.updated",
    "run.completed",
    "run.failed",
    "approval.requested",
    "gate.completed",
    "artifact.available",
    "command.denied",
    "command.completed",
];

/// The Office event for a node's AUXP event (`params` as the node sent
/// them), when the Office shows that kind. A node's approval is known by its
/// `requestId`, and its `kind` is the approval's action.
pub fn from_node_event(method: &str, params: &Value) -> Option<Event> {
    if !NODE_EVENTS.contains(&method) {
        return None;
    }
    let text = |key: &str| {
        params
            .get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let mut data: Object = params.as_object()?.clone();
    data.retain(|k, _| !ENVELOPE.contains(&k.as_str()));
    if method == "approval.requested" {
        if !data.contains_key("id")
            && let Some(id) = data.get("requestId").cloned()
        {
            data.insert("id".into(), id);
        }
        if !data.contains_key("action")
            && let Some(kind) = data.get("kind").cloned()
        {
            data.insert("action".into(), kind);
        }
    }
    Some(Event {
        event_id: text("eventId"),
        event_type: method.into(),
        organization_id: text("organizationId"),
        node_id: text("nodeId"),
        run_id: text("runId"),
        occurred_at: text("occurredAt"),
        sequence: params.get("sequence").and_then(Value::as_i64).unwrap_or(0),
        correlation_id: text("correlationId"),
        causation_id: text("causationId"),
        data: Some(Value::Object(data)),
        ..Event::default()
    })
}

/// Inserts `item` into `list`, or merges it into the one with the same `id`.
fn upsert(list: &mut Vec<Object>, item: Object) {
    match list.iter_mut().find(|o| o.get("id") == item.get("id")) {
        Some(existing) => existing.extend(item),
        None => list.push(item),
    }
}

/// Stamps what the log needs on an event it takes: when, and who for a
/// node's own events.
pub(crate) fn prepare(event: &mut Event) {
    if event.received_at.is_empty() {
        event.received_at = now_rfc3339();
    }
    if event.actor.is_none() && !event.node_id.is_empty() && !event.node_id.starts_with("command") {
        event.actor = Some(json!({"kind": "node", "id": event.node_id}));
    }
}

pub type Object = Map<String, Value>;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Projection {
    pub organization_id: String,
    pub schema_version: String,
    pub definition_version: String,
    pub projected_at: String,
    pub source: String,
    pub freshness: String,
    pub last_event_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub last_node_id: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub last_sequence: i64,
    #[serde(default)]
    pub cursors: BTreeMap<String, i64>,
    #[serde(default)]
    pub organization: Object,
    #[serde(default)]
    pub areas: Vec<Object>,
    #[serde(default)]
    pub missions: Vec<Object>,
    #[serde(default)]
    pub agents: Vec<Object>,
    #[serde(default)]
    pub nodes: Vec<Object>,
    #[serde(default)]
    pub runs: Vec<Object>,
    #[serde(default)]
    pub approvals: Vec<Object>,
    #[serde(default)]
    pub artifacts: Vec<Object>,
    #[serde(default)]
    pub events: Vec<Event>,
}

fn is_zero(n: &i64) -> bool {
    *n == 0
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ResolutionRequest {
    #[serde(deserialize_with = "crate::nullable")]
    pub organization_id: String,
    #[serde(deserialize_with = "crate::nullable")]
    pub decision: String,
    #[serde(
        skip_serializing_if = "String::is_empty",
        deserialize_with = "crate::nullable"
    )]
    pub reason: String,
    #[serde(deserialize_with = "crate::nullable")]
    pub idempotency_key: String,
    /// Who decides: set by the server from the caller, never from the body.
    #[serde(skip)]
    pub actor: Option<Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolutionResponse {
    pub ok: bool,
    pub organization_id: String,
    pub approval_id: String,
    pub decision: String,
    pub idempotency_key: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub idempotent_replay: bool,
}

/// A request the read model refuses, shaped as agentux-protocol's
/// approval-resolution `conflict`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Conflict {
    pub code: String,
    pub message: String,
}

#[derive(Debug)]
pub enum Error {
    Conflict(Conflict),
    Invalid(String),
    Storage(String),
}

impl Error {
    fn conflict(code: &str, message: impl Into<String>) -> Self {
        Error::Conflict(Conflict {
            code: code.into(),
            message: message.into(),
        })
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Conflict(c) => f.write_str(&c.message),
            Error::Invalid(message) | Error::Storage(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {}

/// An in-memory read model with a demo organization: the demo server and
/// tests.
pub struct Store {
    state: Mutex<MemoryState>,
}

struct MemoryState {
    projections: HashMap<String, Projection>,
    /// Approval status by organization and approval.
    approvals: HashMap<(String, String), String>,
    /// Decisions by organization, approval and idempotency key.
    decisions: HashMap<(String, String, String), ResolutionResponse>,
    events: HashSet<(String, String)>,
    /// The last sequence by organization and node.
    sequences: HashMap<(String, String), i64>,
    /// Every event taken, in order, with its cursor.
    log: Vec<Logged>,
}

fn objects(value: Value) -> Vec<Object> {
    serde_json::from_value(value).unwrap_or_default()
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

impl Store {
    pub fn new() -> Self {
        let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let expires = (chrono::Utc::now() + chrono::Duration::hours(1))
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let demo = Projection {
            organization_id: "org-demo".into(),
            schema_version: "office-projection.v1".into(),
            definition_version: "definition-3".into(),
            projected_at: now.clone(),
            source: "command-memory".into(),
            freshness: "fresh".into(),
            last_event_id: "evt-107".into(),
            last_sequence: 7,
            cursors: BTreeMap::from([("node-01".into(), 7)]),
            organization: serde_json::from_value(
                json!({"id": "org-demo", "name": "Acme Robotics"}),
            )
            .unwrap(),
            areas: objects(
                json!([{"id": "area-engineering", "name": "Engineering"}, {"id": "area-operations", "name": "Operations"}]),
            ),
            missions: objects(
                json!([{"id": "mission-release", "areaId": "area-engineering", "name": "Ship release", "status": "active"}]),
            ),
            agents: objects(
                json!([{"id": "agent-atlas", "name": "Atlas", "status": "active", "missionId": "mission-release"}]),
            ),
            nodes: objects(json!([{"id": "node-01", "status": "online", "lastHeartbeatAt": now}])),
            runs: objects(
                json!([{"id": "run-01", "missionId": "mission-release", "status": "waiting", "nodeId": "node-01"}]),
            ),
            approvals: objects(
                json!([{"id": "approval-01", "runId": "run-01", "status": "pending", "action": "publish", "expiresAt": expires}]),
            ),
            artifacts: Vec::new(),
            events: vec![Event {
                event_id: "evt-107".into(),
                event_type: "approval.requested".into(),
                organization_id: "org-demo".into(),
                node_id: "node-01".into(),
                run_id: "run-01".into(),
                occurred_at: now.clone(),
                received_at: now,
                sequence: 7,
                correlation_id: "corr-01".into(),
                ..Event::default()
            }],
            ..Projection::default()
        };
        Self {
            state: Mutex::new(MemoryState {
                projections: HashMap::from([("org-demo".into(), demo)]),
                approvals: HashMap::from([(
                    ("org-demo".into(), "approval-01".into()),
                    "pending".into(),
                )]),
                decisions: HashMap::new(),
                events: HashSet::from([("org-demo".into(), "evt-107".into())]),
                sequences: HashMap::from([(("org-demo".into(), "node-01".into()), 7)]),
                log: Vec::new(),
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, MemoryState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[async_trait]
impl OfficeRepository for Store {
    async fn ping(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn get_projection(&self, organization_id: &str) -> Result<Option<Projection>, Error> {
        Ok(self.lock().projections.get(organization_id).cloned())
    }

    async fn apply_event(&self, mut event: Event) -> Result<(), Error> {
        validate_event(&event)?;
        prepare(&mut event);
        let mut state = self.lock();
        let event_key = (event.organization_id.clone(), event.event_id.clone());
        if state.events.contains(&event_key) {
            return Ok(());
        }
        let stream = (event.organization_id.clone(), event.node_id.clone());
        let last = state.sequences.get(&stream).copied().unwrap_or(0);
        if event.sequence == 0 {
            event.sequence = last + 1;
        }
        if event.sequence <= last {
            return Err(Error::Invalid(format!(
                "event sequence {} is not after {last} for {}/{}",
                event.sequence, stream.0, stream.1
            )));
        }
        let mut projection = state
            .projections
            .get(&event.organization_id)
            .cloned()
            .unwrap_or_else(|| Projection {
                source: "command-memory".into(),
                ..empty_projection(&event.organization_id)
            });
        stamp_definition(&mut event, &projection);
        apply_projection_event(&mut projection, &event)?;
        state.events.insert(event_key);
        state.sequences.insert(stream, event.sequence);
        let data = event.data.as_ref();
        if event.event_type == "approval.requested" {
            let id = data
                .and_then(|d| d.get("id"))
                .and_then(Value::as_str)
                .unwrap_or("");
            state
                .approvals
                .insert((event.organization_id.clone(), id.into()), "pending".into());
        } else if event.event_type == "approval.resolved" {
            let id = data
                .and_then(|d| d.get("approvalId"))
                .and_then(Value::as_str)
                .unwrap_or("");
            let decision = data
                .and_then(|d| d.get("decision"))
                .and_then(Value::as_str)
                .unwrap_or("");
            state
                .approvals
                .insert((event.organization_id.clone(), id.into()), decision.into());
        }
        state
            .projections
            .insert(event.organization_id.clone(), projection);
        let cursor = state.log.len() as i64 + 1;
        state.log.push(Logged { cursor, event });
        Ok(())
    }

    async fn events(&self, organization_id: &str, replay: &Replay) -> Result<Vec<Logged>, Error> {
        Ok(self
            .lock()
            .log
            .iter()
            .filter(|l| {
                l.event.organization_id == organization_id && replay.takes(l.cursor, &l.event)
            })
            .take(replay.limit.max(0) as usize)
            .cloned()
            .collect())
    }

    async fn resolve_approval(
        &self,
        approval_id: &str,
        request: ResolutionRequest,
    ) -> Result<ResolutionResponse, Error> {
        validate_resolution(&request)?;
        let mut state = self.lock();
        let key = (
            request.organization_id.clone(),
            approval_id.to_string(),
            request.idempotency_key.clone(),
        );
        if let Some(previous) = state.decisions.get(&key) {
            return Ok(ResolutionResponse {
                idempotent_replay: true,
                ..previous.clone()
            });
        }
        let approval_key = (request.organization_id.clone(), approval_id.to_string());
        match state.approvals.get(&approval_key).map(String::as_str) {
            None => return Err(Error::conflict("not_found", "approval not found")),
            Some("pending") => {}
            Some(_) => {
                return Err(Error::conflict(
                    "already_decided",
                    "approval already decided",
                ));
            }
        }
        let response = ResolutionResponse {
            ok: true,
            organization_id: request.organization_id.clone(),
            approval_id: approval_id.to_string(),
            decision: request.decision.clone(),
            idempotency_key: request.idempotency_key.clone(),
            idempotent_replay: false,
        };
        state.decisions.insert(key, response.clone());
        state
            .approvals
            .insert(approval_key, request.decision.clone());
        if let Some(projection) = state.projections.get_mut(&request.organization_id) {
            for approval in &mut projection.approvals {
                if approval.get("id").and_then(Value::as_str) == Some(approval_id) {
                    approval.insert("status".into(), Value::String(request.decision.clone()));
                }
            }
        }
        Ok(response)
    }
}

pub(crate) fn validate_event(event: &Event) -> Result<(), Error> {
    // Command's own streams (`command.*`) may leave the sequence to the log.
    let numbered =
        event.sequence >= 1 || (event.sequence == 0 && event.node_id.starts_with("command."));
    if event.event_id.is_empty() || event.organization_id.is_empty() || !numbered {
        return Err(Error::Invalid(
            "event requires eventId, organizationId and positive sequence".into(),
        ));
    }
    let resolved = event.event_type == "approval.resolved";
    if event.event_type == "approval.requested" || resolved {
        let Some(data) = event.data.as_ref().and_then(Value::as_object) else {
            return Err(Error::Invalid(format!(
                "decode {} data: not an object",
                event.event_type
            )));
        };
        let key = if resolved { "approvalId" } else { "id" };
        if data
            .get(key)
            .and_then(Value::as_str)
            .unwrap_or("")
            .is_empty()
        {
            return Err(Error::Invalid(format!(
                "{} data requires {key}",
                event.event_type
            )));
        }
        if resolved
            && !matches!(
                data.get("decision").and_then(Value::as_str),
                Some("approved" | "rejected")
            )
        {
            return Err(Error::Invalid(
                "approval.resolved data has invalid decision".into(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_resolution(request: &ResolutionRequest) -> Result<(), Error> {
    if request.organization_id.is_empty() {
        return Err(Error::conflict(
            "stale_version",
            "organizationId is required",
        ));
    }
    if request.decision != "approved" && request.decision != "rejected" {
        return Err(Error::conflict(
            "denied",
            "decision must be approved or rejected",
        ));
    }
    if request.idempotency_key.is_empty() {
        return Err(Error::conflict(
            "stale_version",
            "idempotencyKey is required",
        ));
    }
    Ok(())
}

pub(crate) fn empty_projection(organization_id: &str) -> Projection {
    Projection {
        organization_id: organization_id.into(),
        schema_version: "office-projection.v1".into(),
        definition_version: "definition-unknown".into(),
        projected_at: now_rfc3339(),
        source: "command-postgres".into(),
        freshness: "fresh".into(),
        organization: serde_json::from_value(json!({"id": organization_id})).unwrap(),
        ..Projection::default()
    }
}

pub(crate) fn apply_projection_event(
    projection: &mut Projection,
    event: &Event,
) -> Result<(), Error> {
    projection.organization_id = event.organization_id.clone();
    projection.schema_version = "office-projection.v1".into();
    if projection.definition_version.is_empty() {
        projection.definition_version = "definition-unknown".into();
    }
    projection.projected_at = event.received_at.clone();
    if projection.source.is_empty() {
        projection.source = "command-postgres".into();
    }
    projection.freshness = "fresh".into();
    projection.last_event_id = event.event_id.clone();
    projection.last_node_id = event.node_id.clone();
    projection.last_sequence = event.sequence;
    let cursor = projection.cursors.entry(event.node_id.clone()).or_insert(0);
    if event.sequence > *cursor {
        *cursor = event.sequence;
    }
    projection.events.push(event.clone());
    if projection.events.len() > PROJECTION_EVENT_LIMIT {
        let excess = projection.events.len() - PROJECTION_EVENT_LIMIT;
        projection.events.drain(..excess);
    }
    let data = || event.data.clone().unwrap_or(Value::Null);
    if event.event_type == "approval.requested" {
        let Value::Object(mut approval) = data() else {
            return Err(Error::Invalid(
                "approval.requested data is not an object".into(),
            ));
        };
        approval.insert("status".into(), Value::String("pending".into()));
        projection.approvals.push(approval);
    }
    if event.event_type == "organization.definition.published"
        && let Some(Value::Object(data)) = &event.data
    {
        if let Some(version) = data.get("definition_version").and_then(Value::as_i64) {
            projection.definition_version = format!("definition-{version}");
        }
        if let Some(Value::Object(organization)) = data.get("organization") {
            projection.organization = organization.clone();
        }
        let objects = |key: &str| -> Option<Vec<Object>> {
            data.get(key)
                .and_then(|v| serde_json::from_value(v.clone()).ok())
        };
        if let Some(areas) = objects("areas") {
            projection.areas = areas;
        }
        if let Some(missions) = objects("missions") {
            projection.missions = missions;
        }
        if let Some(agents) = objects("agents") {
            projection.agents = agents;
        }
    }
    let run_status = match event.event_type.as_str() {
        "run.accepted" => Some("accepted"),
        "run.started" | "run.updated" => Some("running"),
        "run.completed" => Some("completed"),
        "run.failed" => Some("failed"),
        _ => None,
    };
    if let Some(status) = run_status {
        let mut run = data().as_object().cloned().unwrap_or_default();
        let id = run
            .remove("runId")
            .or_else(|| (!event.run_id.is_empty()).then(|| Value::String(event.run_id.clone())));
        if let Some(id) = id {
            // A node's own word for the state (`waiting`, `paused`, ...) wins
            // over the method's while the run goes on.
            let state = run.remove("state");
            let status = match (status, state) {
                ("running", Some(Value::String(s))) if !s.is_empty() => s,
                _ => status.to_string(),
            };
            run.insert("id".into(), id);
            run.insert("status".into(), Value::String(status));
            if !event.node_id.is_empty() {
                run.insert("nodeId".into(), Value::String(event.node_id.clone()));
            }
            run.insert("updatedAt".into(), Value::String(event.occurred_at.clone()));
            upsert(&mut projection.runs, run);
        }
    }
    if event.event_type.starts_with("mission.run.")
        && let Some(Value::Object(data)) = &event.data
    {
        let mut run = Object::new();
        for (from, to) in [
            ("runId", "id"),
            ("missionId", "missionId"),
            ("workflowId", "workflowId"),
            ("status", "status"),
            ("step", "step"),
            ("error", "error"),
        ] {
            if let Some(value) = data.get(from) {
                run.insert(to.into(), value.clone());
            }
        }
        run.insert("kind".into(), Value::String("mission".into()));
        run.insert(
            "definitionVersion".into(),
            Value::String(event.definition_version.clone()),
        );
        run.insert("updatedAt".into(), Value::String(event.occurred_at.clone()));
        if run.contains_key("id") {
            upsert(&mut projection.runs, run);
        }
    }
    if event.event_type == "artifact.available" {
        let mut artifact = data().as_object().cloned().unwrap_or_default();
        let id = artifact
            .get("artifactId")
            .or_else(|| artifact.get("id"))
            .cloned()
            .unwrap_or_else(|| Value::String(event.event_id.clone()));
        artifact.insert("id".into(), id);
        if !event.node_id.is_empty() {
            artifact.insert("nodeId".into(), Value::String(event.node_id.clone()));
        }
        if !event.run_id.is_empty() {
            artifact.insert("runId".into(), Value::String(event.run_id.clone()));
        }
        upsert(&mut projection.artifacts, artifact);
    }
    if event.event_type == "approval.resolved" {
        let Value::Object(resolution) = data() else {
            return Err(Error::Invalid(
                "approval.resolved data is not an object".into(),
            ));
        };
        for approval in &mut projection.approvals {
            if approval.get("id") == resolution.get("approvalId") {
                approval.insert(
                    "status".into(),
                    resolution.get("decision").cloned().unwrap_or(Value::Null),
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(id: &str, org: &str, node: &str, sequence: i64) -> Event {
        Event {
            event_id: id.into(),
            event_type: "run.updated".into(),
            organization_id: org.into(),
            node_id: node.into(),
            sequence,
            ..Event::default()
        }
    }

    #[tokio::test]
    async fn deduplicates_events_and_keeps_the_last_sequence() {
        let store = Store::new();
        let e = event("evt-1", "org-demo", "node-01", 8);
        store.apply_event(e.clone()).await.unwrap();
        store.apply_event(e).await.unwrap();
        let projection = store.get_projection("org-demo").await.unwrap().unwrap();
        assert_eq!(projection.last_event_id, "evt-1");
        assert_eq!(projection.last_sequence, 8);
    }

    #[test]
    fn freshness_follows_the_organizations_nodes() {
        let mut projection = empty_projection("acme");
        let nodes = |states: &[(&str, &str)]| -> Vec<Object> {
            states
                .iter()
                .map(|(n, p)| objects(json!([{"id": n, "presence": p}])).remove(0))
                .collect()
        };
        with_nodes(&mut projection, Vec::new());
        assert_eq!(projection.freshness, "fresh", "no nodes: as stored");
        with_nodes(
            &mut projection,
            nodes(&[("n1", "online"), ("n2", "offline")]),
        );
        assert_eq!(projection.freshness, "degraded");
        assert_eq!(projection.nodes.len(), 2);
        with_nodes(&mut projection, nodes(&[("n1", "degraded")]));
        assert_eq!(projection.freshness, "stale");
        with_nodes(&mut projection, nodes(&[("n1", "online")]));
        assert_eq!(projection.freshness, "fresh");
    }

    fn node_event(method: &str, sequence: i64, extra: Value) -> Event {
        let mut params = json!({
            "protocolVersion": "v1", "organizationId": "acme", "nodeId": "node-1",
            "messageId": format!("m-{sequence}"), "occurredAt": "2026-10-22T12:00:00Z",
            "eventId": format!("e-{sequence}"), "sequence": sequence,
        });
        params
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        from_node_event(method, &params).unwrap()
    }

    #[tokio::test]
    async fn node_events_project_runs_approvals_and_artifacts() {
        assert!(from_node_event("harness.status", &json!({})).is_none());
        let store = Store::new();
        for event in [
            node_event(
                "run.updated",
                1,
                json!({"runId": "r1", "state": "waiting", "title": "Fix", "costUsd": 0.5}),
            ),
            node_event(
                "approval.requested",
                2,
                json!({"runId": "r1", "requestId": "q1", "kind": "push", "title": "Push?"}),
            ),
            node_event(
                "artifact.available",
                3,
                json!({"runId": "r1", "artifactId": "a1", "name": "report.md"}),
            ),
            node_event(
                "run.completed",
                4,
                json!({"runId": "r1", "state": "done", "costUsd": 0.9}),
            ),
        ] {
            store.apply_event(event).await.unwrap();
        }
        let p = store.get_projection("acme").await.unwrap().unwrap();
        assert_eq!(p.runs.len(), 1, "one run, updated in place");
        let run = &p.runs[0];
        assert_eq!(
            (run["id"].as_str(), run["status"].as_str()),
            (Some("r1"), Some("completed"))
        );
        assert_eq!(
            (run["title"].as_str(), run["costUsd"].as_f64()),
            (Some("Fix"), Some(0.9))
        );
        assert_eq!(run["nodeId"], "node-1");
        let approval = &p.approvals[0];
        assert_eq!(
            (
                approval["id"].as_str(),
                approval["action"].as_str(),
                approval["status"].as_str()
            ),
            (Some("q1"), Some("push"), Some("pending"))
        );
        assert_eq!(p.artifacts[0]["id"], "a1");
        assert_eq!(p.events.len(), 4);
        assert_eq!(
            p.events[0].actor,
            Some(json!({"kind": "node", "id": "node-1"}))
        );
        assert!(
            p.events[0]
                .data
                .as_ref()
                .unwrap()
                .get("protocolVersion")
                .is_none(),
            "the envelope is the event's, not its data"
        );
    }

    #[tokio::test]
    async fn returns_the_original_approval_for_an_idempotent_retry() {
        let store = Store::new();
        let request = ResolutionRequest {
            organization_id: "org-demo".into(),
            decision: "approved".into(),
            reason: "reviewed".into(),
            idempotency_key: "idem-01".into(),
            ..ResolutionRequest::default()
        };
        let first = store
            .resolve_approval("approval-01", request.clone())
            .await
            .unwrap();
        assert!(!first.idempotent_replay);
        let replay = store
            .resolve_approval("approval-01", request)
            .await
            .unwrap();
        assert!(replay.idempotent_replay);
        assert_eq!(replay.decision, "approved");
    }

    #[tokio::test]
    async fn keeps_node_sequences_and_event_ids_per_organization() {
        let store = Store::new();
        for org in ["org-a", "org-b"] {
            store
                .apply_event(event("evt-shared", org, "node-01", 1))
                .await
                .unwrap();
        }
        for org in ["org-a", "org-b"] {
            let projection = store.get_projection(org).await.unwrap().unwrap();
            assert_eq!(projection.last_sequence, 1);
            assert_eq!(projection.organization_id, org);
            assert_eq!(projection.last_event_id, "evt-shared");
        }
    }

    #[tokio::test]
    async fn scopes_approval_idempotency_by_organization_and_approval() {
        let store = Store::new();
        let mut request = ResolutionRequest {
            organization_id: "org-demo".into(),
            decision: "approved".into(),
            idempotency_key: "shared-key".into(),
            ..ResolutionRequest::default()
        };
        let first = store
            .resolve_approval("approval-01", request.clone())
            .await
            .unwrap();
        assert!(!first.idempotent_replay);
        request.organization_id = "org-other".into();
        match store.resolve_approval("approval-01", request).await {
            Err(Error::Conflict(c)) => assert_eq!(c.code, "not_found"),
            other => panic!("expected not_found, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn keeps_independent_node_cursors() {
        let store = Store::new();
        store
            .apply_event(event("evt-node-a", "org-demo", "node-a", 9))
            .await
            .unwrap();
        store
            .apply_event(event("evt-node-b", "org-demo", "node-b", 3))
            .await
            .unwrap();
        let projection = store.get_projection("org-demo").await.unwrap().unwrap();
        assert_eq!(projection.cursors["node-a"], 9);
        assert_eq!(projection.cursors["node-b"], 3);
    }

    #[tokio::test]
    async fn seeds_the_demo_node_cursor_from_its_projection() {
        let store = Store::new();
        assert!(
            store
                .apply_event(event("evt-old-demo", "org-demo", "node-01", 6))
                .await
                .is_err()
        );
    }

    #[test]
    fn conflicts_use_the_protocol_shape() {
        let conflict = Conflict {
            code: "already_decided".into(),
            message: "approval already decided".into(),
        };
        assert_eq!(
            serde_json::to_value(conflict).unwrap(),
            json!({"code": "already_decided", "message": "approval already decided"})
        );
    }
}
