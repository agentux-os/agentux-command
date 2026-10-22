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
pub use postgres::{PostgresStore, open_postgres, pool};

/// Events kept in a projection; older ones stay in the log only.
const PROJECTION_EVENT_LIMIT: usize = 100;

#[async_trait]
pub trait OfficeRepository: Send + Sync {
    async fn ping(&self) -> Result<(), Error>;
    async fn get_projection(&self, organization_id: &str) -> Result<Option<Projection>, Error>;
    async fn apply_event(&self, event: Event) -> Result<(), Error>;
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
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
    pub organization_id: String,
    pub decision: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub reason: String,
    pub idempotency_key: String,
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
        if event.received_at.is_empty() {
            event.received_at = now_rfc3339();
        }
        let mut state = self.lock();
        let event_key = (event.organization_id.clone(), event.event_id.clone());
        if state.events.contains(&event_key) {
            return Ok(());
        }
        let stream = (event.organization_id.clone(), event.node_id.clone());
        let last = state.sequences.get(&stream).copied().unwrap_or(0);
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
        Ok(())
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
    if event.event_id.is_empty() || event.organization_id.is_empty() || event.sequence < 1 {
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

    #[tokio::test]
    async fn returns_the_original_approval_for_an_idempotent_retry() {
        let store = Store::new();
        let request = ResolutionRequest {
            organization_id: "org-demo".into(),
            decision: "approved".into(),
            reason: "reviewed".into(),
            idempotency_key: "idem-01".into(),
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
