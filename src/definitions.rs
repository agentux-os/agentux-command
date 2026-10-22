//! Organization definitions (ADR 0015, agentux `docs/contracts/
//! organization-definition-v1.md`): an organization's areas, roles,
//! missions, workflows, gates, agents and nodes, as a versioned document.
//!
//! A definition starts as a **draft**, which may be replaced while it stays
//! a draft and carries the result of its last validation. **Publishing** a
//! draft that validates freezes it: a published version is never changed
//! again, and the next change is a new draft with the next version number.
//! Every creation, replacement and publication is recorded in the
//! organization's audit log with its actor.
//!
//! The document is the contract's, field for field (snake_case). Fields this
//! version does not know are kept as they are.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Mutex, MutexGuard};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The contract major version this server reads.
pub const SCHEMA_MAJOR: &str = "1";

/// The permission vocabulary of ADR 0015 (a role's `permissions`).
pub const PERMISSIONS: &[&str] = &[
    "organization.read",
    "organization.edit",
    "organization.publish",
    "run.read",
    "run.start",
    "run.pause",
    "run.cancel",
    "approval.decide",
    "artifact.read",
    "node.manage",
];

fn object(value: &Value) -> Option<&Map<String, Value>> {
    value.as_object()
}

fn text<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or("")
}

fn list<'a>(document: &'a Value, key: &str) -> &'a [Value] {
    document
        .get(key)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

/// The problems that keep a definition from being published; empty when it
/// may be.
pub fn validate(document: &Value, organization_id: &str) -> Vec<String> {
    let mut problems = Vec::new();
    let Some(_) = object(document) else {
        return vec!["a definition is a JSON object".into()];
    };
    let schema = text(document, "schema_version");
    if schema.split('.').next() != Some(SCHEMA_MAJOR) || !schema.contains('.') {
        problems.push(format!(
            "schema_version must be {SCHEMA_MAJOR}.x (found {schema:?})"
        ));
    }
    let org = document.get("organization").unwrap_or(&Value::Null);
    if object(org).is_none() {
        problems.push("organization is required".into());
    } else {
        if text(org, "id") != organization_id {
            problems.push(format!("organization.id must be {organization_id:?}"));
        }
        if text(org, "name").trim().is_empty() {
            problems.push("organization.name is required".into());
        }
    }

    // Ids per kind, unique within the definition.
    let mut ids: HashMap<&str, HashSet<String>> = HashMap::new();
    for kind in [
        "areas",
        "roles",
        "missions",
        "workflows",
        "gates",
        "agents",
        "nodes",
    ] {
        let mut seen = HashSet::new();
        for (i, item) in list(document, kind).iter().enumerate() {
            let id = text(item, "id");
            if id.is_empty() {
                problems.push(format!("{kind}[{i}] has no id"));
            } else if !seen.insert(id.to_string()) {
                problems.push(format!("{kind}: id {id:?} appears twice"));
            }
        }
        ids.insert(kind, seen);
    }
    let refers = |problems: &mut Vec<String>,
                  kind: &str,
                  owner: &str,
                  field: &str,
                  target: &str,
                  value: &str| {
        if !value.is_empty() && !ids[target].contains(value) {
            problems.push(format!(
                "{kind} {owner:?}: {field} {value:?} is not in {target}"
            ));
        }
    };
    for area in list(document, "areas") {
        refers(
            &mut problems,
            "area",
            text(area, "id"),
            "owner_role_id",
            "roles",
            text(area, "owner_role_id"),
        );
    }
    for mission in list(document, "missions") {
        refers(
            &mut problems,
            "mission",
            text(mission, "id"),
            "area_id",
            "areas",
            text(mission, "area_id"),
        );
        refers(
            &mut problems,
            "mission",
            text(mission, "id"),
            "owner_role_id",
            "roles",
            text(mission, "owner_role_id"),
        );
    }
    for agent in list(document, "agents") {
        refers(
            &mut problems,
            "agent",
            text(agent, "id"),
            "role_id",
            "roles",
            text(agent, "role_id"),
        );
    }
    for workflow in list(document, "workflows") {
        let wid = text(workflow, "id");
        refers(
            &mut problems,
            "workflow",
            wid,
            "mission_id",
            "missions",
            text(workflow, "mission_id"),
        );
        let steps: HashSet<&str> = list(workflow, "steps")
            .iter()
            .map(|s| text(s, "id"))
            .collect();
        for step in list(workflow, "steps") {
            let sid = text(step, "id");
            refers(
                &mut problems,
                "step",
                sid,
                "role_id",
                "roles",
                text(step, "role_id"),
            );
            refers(
                &mut problems,
                "step",
                sid,
                "gate_id",
                "gates",
                text(step, "gate_id"),
            );
            for next in list(step, "next") {
                let next = next.as_str().unwrap_or("");
                if !steps.contains(next) {
                    problems.push(format!(
                        "workflow {wid:?}: step {sid:?} goes to {next:?}, which is not a step of this workflow"
                    ));
                }
            }
        }
    }
    for gate in list(document, "gates") {
        for permission in list(gate, "required_permissions") {
            let p = permission.as_str().unwrap_or("");
            if !PERMISSIONS.contains(&p) {
                problems.push(format!(
                    "gate {:?}: unknown permission {p:?}",
                    text(gate, "id")
                ));
            }
        }
    }
    let mut known_permission = |role: &Value| {
        for permission in list(role, "permissions") {
            let p = permission.as_str().unwrap_or("");
            if !PERMISSIONS.contains(&p) {
                problems.push(format!(
                    "role {:?}: unknown permission {p:?}",
                    text(role, "id")
                ));
            }
        }
    };
    for role in list(document, "roles") {
        known_permission(role);
    }
    // A human-required gate cannot be passed by an agent: some human role
    // must hold the permissions it requires.
    let human_permissions: HashSet<&str> = list(document, "roles")
        .iter()
        .filter(|r| text(r, "principal_kind") == "human")
        .flat_map(|r| list(r, "permissions").iter().filter_map(Value::as_str))
        .collect();
    for gate in list(document, "gates")
        .iter()
        .filter(|g| text(g, "policy") == "human-required")
    {
        for permission in list(gate, "required_permissions")
            .iter()
            .filter_map(Value::as_str)
        {
            if !human_permissions.contains(permission) {
                problems.push(format!(
                    "gate {:?} is human-required, but no human role holds {permission:?}",
                    text(gate, "id")
                ));
            }
        }
    }
    problems
}

/// Who did something: `{ "kind": "user" | "service" | …, "id": … }`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Actor {
    pub kind: String,
    pub id: String,
}

impl Actor {
    /// The actor for an authenticated subject (`oidc:…`, `service-token:…`,
    /// a demo token).
    pub fn of(subject: &str) -> Self {
        let kind = match subject.split_once(':').map(|(k, _)| k) {
            Some("oidc") => "user",
            Some("service-token") => "service",
            _ => "operator",
        };
        Self {
            kind: kind.into(),
            id: subject.into(),
        }
    }
}

/// One version of an organization's definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Definition {
    pub organization_id: String,
    pub definition_version: i64,
    /// `draft` or `published`.
    pub status: String,
    pub document: Value,
    /// The problems its last validation found; empty: it may be published.
    pub problems: Vec<String>,
    pub created_by: Actor,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub published_by: Option<Actor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub published_at: Option<DateTime<Utc>>,
}

/// One entry of an organization's audit log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditEntry {
    pub organization_id: String,
    /// `definition.drafted`, `definition.replaced`, `definition.published`.
    pub action: String,
    pub actor: Actor,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub definition_version: Option<i64>,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug)]
pub enum Error {
    NotFound,
    /// A published version cannot change.
    Published,
    /// It does not validate; the problems.
    Invalid(Vec<String>),
    Storage(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotFound => f.write_str("no such definition version"),
            Error::Published => {
                f.write_str("a published definition never changes; draft a new version")
            }
            Error::Invalid(problems) => write!(
                f,
                "the definition does not validate: {}",
                problems.join("; ")
            ),
            Error::Storage(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for Error {}

impl From<tokio_postgres::Error> for Error {
    fn from(e: tokio_postgres::Error) -> Self {
        Error::Storage(e.to_string())
    }
}

impl From<deadpool_postgres::PoolError> for Error {
    fn from(e: deadpool_postgres::PoolError) -> Self {
        Error::Storage(e.to_string())
    }
}

/// Keeps definitions and the audit log.
#[async_trait]
pub trait Store: Send + Sync {
    /// A new draft with the next version number.
    async fn draft(
        &self,
        organization_id: &str,
        document: Value,
        actor: &Actor,
        now: DateTime<Utc>,
    ) -> Result<Definition, Error>;
    /// Replaces a draft's document.
    async fn replace(
        &self,
        organization_id: &str,
        version: i64,
        document: Value,
        actor: &Actor,
        now: DateTime<Utc>,
    ) -> Result<Definition, Error>;
    /// Publishes a draft that validates.
    async fn publish(
        &self,
        organization_id: &str,
        version: i64,
        actor: &Actor,
        now: DateTime<Utc>,
    ) -> Result<Definition, Error>;
    async fn get(&self, organization_id: &str, version: i64) -> Result<Definition, Error>;
    /// Every version, newest first (documents left out).
    async fn list(&self, organization_id: &str) -> Result<Vec<Definition>, Error>;
    async fn audit(&self, organization_id: &str, limit: i64) -> Result<Vec<AuditEntry>, Error>;
}

/// Versions by organization and number, and the audit log.
type MemoryState = (BTreeMap<(String, i64), Definition>, Vec<AuditEntry>);

/// Definitions in memory: the demo server and tests.
#[derive(Default)]
pub struct Memory {
    state: Mutex<MemoryState>,
}

impl Memory {
    fn lock(&self) -> MutexGuard<'_, MemoryState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

fn audit_entry(
    org: &str,
    action: &str,
    actor: &Actor,
    version: i64,
    now: DateTime<Utc>,
) -> AuditEntry {
    AuditEntry {
        organization_id: org.into(),
        action: action.into(),
        actor: actor.clone(),
        definition_version: Some(version),
        occurred_at: now,
    }
}

fn summary(definition: &Definition) -> Definition {
    Definition {
        document: Value::Null,
        ..definition.clone()
    }
}

#[async_trait]
impl Store for Memory {
    async fn draft(
        &self,
        org: &str,
        document: Value,
        actor: &Actor,
        now: DateTime<Utc>,
    ) -> Result<Definition, Error> {
        let mut state = self.lock();
        let version = state
            .0
            .keys()
            .filter(|(o, _)| o == org)
            .map(|(_, v)| *v)
            .max()
            .unwrap_or(0)
            + 1;
        let definition = Definition {
            organization_id: org.into(),
            definition_version: version,
            status: "draft".into(),
            problems: validate(&document, org),
            document,
            created_by: actor.clone(),
            created_at: now,
            published_by: None,
            published_at: None,
        };
        state.0.insert((org.into(), version), definition.clone());
        state
            .1
            .push(audit_entry(org, "definition.drafted", actor, version, now));
        Ok(definition)
    }

    async fn replace(
        &self,
        org: &str,
        version: i64,
        document: Value,
        actor: &Actor,
        now: DateTime<Utc>,
    ) -> Result<Definition, Error> {
        let mut state = self.lock();
        let definition = state
            .0
            .get_mut(&(org.into(), version))
            .ok_or(Error::NotFound)?;
        if definition.status != "draft" {
            return Err(Error::Published);
        }
        definition.problems = validate(&document, org);
        definition.document = document;
        let out = definition.clone();
        state
            .1
            .push(audit_entry(org, "definition.replaced", actor, version, now));
        Ok(out)
    }

    async fn publish(
        &self,
        org: &str,
        version: i64,
        actor: &Actor,
        now: DateTime<Utc>,
    ) -> Result<Definition, Error> {
        let mut state = self.lock();
        let definition = state
            .0
            .get_mut(&(org.into(), version))
            .ok_or(Error::NotFound)?;
        if definition.status != "draft" {
            return Err(Error::Published);
        }
        let problems = validate(&definition.document, org);
        if !problems.is_empty() {
            definition.problems = problems.clone();
            return Err(Error::Invalid(problems));
        }
        definition.status = "published".into();
        definition.published_by = Some(actor.clone());
        definition.published_at = Some(now);
        let out = definition.clone();
        state.1.push(audit_entry(
            org,
            "definition.published",
            actor,
            version,
            now,
        ));
        Ok(out)
    }

    async fn get(&self, org: &str, version: i64) -> Result<Definition, Error> {
        self.lock()
            .0
            .get(&(org.into(), version))
            .cloned()
            .ok_or(Error::NotFound)
    }

    async fn list(&self, org: &str) -> Result<Vec<Definition>, Error> {
        let state = self.lock();
        let mut out: Vec<Definition> = state
            .0
            .values()
            .filter(|d| d.organization_id == org)
            .map(summary)
            .collect();
        out.sort_by_key(|d| std::cmp::Reverse(d.definition_version));
        Ok(out)
    }

    async fn audit(&self, org: &str, limit: i64) -> Result<Vec<AuditEntry>, Error> {
        let state = self.lock();
        Ok(state
            .1
            .iter()
            .rev()
            .filter(|e| e.organization_id == org)
            .take(limit.max(0) as usize)
            .cloned()
            .collect())
    }
}

/// The Office event a publication records: the organization, its areas,
/// missions and agents, the new definition version and who published it.
pub fn published_event(definition: &Definition) -> crate::office::Event {
    let org = &definition.organization_id;
    let version = definition.definition_version;
    let document = &definition.document;
    crate::office::Event {
        event_id: format!("definition-{org}-{version}"),
        event_type: "organization.definition.published".into(),
        organization_id: org.clone(),
        node_id: "command.definitions".into(),
        sequence: version,
        occurred_at: definition
            .published_at
            .map(|at| at.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
            .unwrap_or_else(crate::now_rfc3339),
        correlation_id: format!("definition-{org}-{version}"),
        actor: definition
            .published_by
            .as_ref()
            .and_then(|a| serde_json::to_value(a).ok()),
        definition_version: format!("definition-{version}"),
        data: Some(serde_json::json!({
            "definition_version": version,
            "organization": document.get("organization"),
            "areas": document.get("areas"),
            "missions": document.get("missions"),
            "agents": document.get("agents"),
        })),
        ..crate::office::Event::default()
    }
}

/// Definitions in Command's database (migration 006_organization_definitions).
pub struct Postgres {
    pool: deadpool_postgres::Pool,
}

impl Postgres {
    pub fn new(pool: deadpool_postgres::Pool) -> Self {
        Self { pool }
    }
}

use tokio_postgres::types::Json;

const COLUMNS: &str = "organization_id, definition_version, status, document, problems, created_by, created_at, published_by, published_at";

fn row_definition(row: &tokio_postgres::Row) -> Definition {
    Definition {
        organization_id: row.get(0),
        definition_version: row.get(1),
        status: row.get(2),
        document: row
            .get::<_, Option<Json<Value>>>(3)
            .map(|j| j.0)
            .unwrap_or(Value::Null),
        problems: row.get::<_, Json<Vec<String>>>(4).0,
        created_by: row.get::<_, Json<Actor>>(5).0,
        created_at: row.get(6),
        published_by: row.get::<_, Option<Json<Actor>>>(7).map(|j| j.0),
        published_at: row.get(8),
    }
}

async fn record(
    tx: &deadpool_postgres::Transaction<'_>,
    org: &str,
    action: &str,
    actor: &Actor,
    version: i64,
    now: DateTime<Utc>,
) -> Result<(), Error> {
    tx.execute(
        "INSERT INTO organization_audit (organization_id, action, actor, definition_version, occurred_at) VALUES ($1, $2, $3, $4, $5)",
        &[&org, &action, &Json(actor), &version, &now],
    )
    .await?;
    Ok(())
}

#[async_trait]
impl Store for Postgres {
    async fn draft(
        &self,
        org: &str,
        document: Value,
        actor: &Actor,
        now: DateTime<Utc>,
    ) -> Result<Definition, Error> {
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        // One new version at a time per organization.
        tx.execute(
            "SELECT pg_advisory_xact_lock(842121, hashtext($1))",
            &[&org],
        )
        .await?;
        let version: i64 = tx
            .query_one(
                "SELECT COALESCE(MAX(definition_version), 0) + 1 FROM organization_definitions WHERE organization_id = $1",
                &[&org],
            )
            .await?
            .get(0);
        let problems = validate(&document, org);
        let row = tx
            .query_one(
                &format!(
                    "INSERT INTO organization_definitions (organization_id, definition_version, status, document, problems, created_by, created_at)
                     VALUES ($1, $2, 'draft', $3, $4, $5, $6) RETURNING {COLUMNS}"
                ),
                &[&org, &version, &Json(&document), &Json(&problems), &Json(actor), &now],
            )
            .await?;
        record(&tx, org, "definition.drafted", actor, version, now).await?;
        tx.commit().await?;
        Ok(row_definition(&row))
    }

    async fn replace(
        &self,
        org: &str,
        version: i64,
        document: Value,
        actor: &Actor,
        now: DateTime<Utc>,
    ) -> Result<Definition, Error> {
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        let problems = validate(&document, org);
        let row = tx
            .query_opt(
                &format!(
                    "UPDATE organization_definitions SET document = $3, problems = $4
                     WHERE organization_id = $1 AND definition_version = $2 AND status = 'draft' RETURNING {COLUMNS}"
                ),
                &[&org, &version, &Json(&document), &Json(&problems)],
            )
            .await?;
        let Some(row) = row else {
            drop(tx);
            return Err(match self.get(org, version).await {
                Ok(_) => Error::Published,
                Err(e) => e,
            });
        };
        record(&tx, org, "definition.replaced", actor, version, now).await?;
        tx.commit().await?;
        Ok(row_definition(&row))
    }

    async fn publish(
        &self,
        org: &str,
        version: i64,
        actor: &Actor,
        now: DateTime<Utc>,
    ) -> Result<Definition, Error> {
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        let row = tx
            .query_opt(
                &format!("SELECT {COLUMNS} FROM organization_definitions WHERE organization_id = $1 AND definition_version = $2 FOR UPDATE"),
                &[&org, &version],
            )
            .await?
            .ok_or(Error::NotFound)?;
        let current = row_definition(&row);
        if current.status != "draft" {
            return Err(Error::Published);
        }
        let problems = validate(&current.document, org);
        if !problems.is_empty() {
            return Err(Error::Invalid(problems));
        }
        let row = tx
            .query_one(
                &format!(
                    "UPDATE organization_definitions SET status = 'published', problems = '[]', published_by = $3, published_at = $4
                     WHERE organization_id = $1 AND definition_version = $2 RETURNING {COLUMNS}"
                ),
                &[&org, &version, &Json(actor), &now],
            )
            .await?;
        let published = row_definition(&row);
        // The Office event commits with the publication, or neither does.
        crate::office::append_event(&tx, published_event(&published))
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        record(&tx, org, "definition.published", actor, version, now).await?;
        tx.commit().await?;
        Ok(published)
    }

    async fn get(&self, org: &str, version: i64) -> Result<Definition, Error> {
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                &format!("SELECT {COLUMNS} FROM organization_definitions WHERE organization_id = $1 AND definition_version = $2"),
                &[&org, &version],
            )
            .await?
            .ok_or(Error::NotFound)?;
        Ok(row_definition(&row))
    }

    async fn list(&self, org: &str) -> Result<Vec<Definition>, Error> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT organization_id, definition_version, status, NULL::jsonb, problems, created_by, created_at, published_by, published_at
                 FROM organization_definitions WHERE organization_id = $1 ORDER BY definition_version DESC",
                &[&org],
            )
            .await?;
        Ok(rows.iter().map(row_definition).collect())
    }

    async fn audit(&self, org: &str, limit: i64) -> Result<Vec<AuditEntry>, Error> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT organization_id, action, actor, definition_version, occurred_at FROM organization_audit
                 WHERE organization_id = $1 ORDER BY audit_id DESC LIMIT $2",
                &[&org, &limit],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|row| AuditEntry {
                organization_id: row.get(0),
                action: row.get(1),
                actor: row.get::<_, Json<Actor>>(2).0,
                definition_version: row.get(3),
                occurred_at: row.get(4),
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The contract's own example.
    pub(crate) fn example() -> Value {
        json!({
            "schema_version": "1.0",
            "organization": { "id": "acme", "name": "Acme Robotics", "timezone": "America/Sao_Paulo", "default_approval_policy": "human-required" },
            "areas": [{ "id": "engineering", "name": "Engineering", "owner_role_id": "cto" }],
            "roles": [
                { "id": "cto", "name": "CTO", "principal_kind": "human", "permissions": ["organization.read", "organization.publish", "run.read", "approval.decide"] },
                { "id": "reviewer", "name": "Reviewer", "principal_kind": "agent", "permissions": ["run.read", "artifact.read"] }
            ],
            "missions": [{ "id": "ship-release", "area_id": "engineering", "name": "Ship a release", "owner_role_id": "cto", "success_criteria": ["tests_green"] }],
            "workflows": [{ "id": "release-v1", "mission_id": "ship-release", "steps": [
                { "id": "implement", "kind": "agent", "role_id": "reviewer", "next": ["review"] },
                { "id": "review", "kind": "gate", "gate_id": "human-release", "next": ["publish"] },
                { "id": "publish", "kind": "action", "next": [] }
            ]}],
            "gates": [{ "id": "human-release", "kind": "approval", "required_permissions": ["approval.decide"], "policy": "human-required" }],
            "agents": [{ "id": "reviewer-1", "role_id": "reviewer", "capabilities": ["code.review"] }],
            "nodes": [{ "id": "node-sao-paulo", "kind": "workstation", "trust": "registered" }],
            "future_optional_field": { "kept": true }
        })
    }

    #[test]
    fn the_contracts_example_validates() {
        assert_eq!(validate(&example(), "acme"), Vec::<String>::new());
    }

    #[test]
    fn every_invariant_is_checked() {
        let broken = |path: &str, value: Value| {
            let mut doc = example();
            *doc.pointer_mut(path).unwrap() = value;
            validate(&doc, "acme")
        };
        let has = |problems: Vec<String>, needle: &str| {
            assert!(
                problems.iter().any(|p| p.contains(needle)),
                "{needle:?} not in {problems:?}"
            );
        };
        has(broken("/schema_version", json!("2.0")), "schema_version");
        has(
            broken("/organization/id", json!("other")),
            "organization.id",
        );
        has(
            broken("/areas/0/owner_role_id", json!("ghost")),
            "owner_role_id \"ghost\"",
        );
        has(broken("/missions/0/area_id", json!("ghost")), "area_id");
        has(
            broken("/workflows/0/steps/0/next", json!(["elsewhere"])),
            "not a step of this workflow",
        );
        has(
            broken("/workflows/0/steps/1/gate_id", json!("ghost")),
            "gate_id",
        );
        has(broken("/agents/0/role_id", json!("ghost")), "role_id");
        has(
            broken("/roles/0/permissions", json!(["root"])),
            "unknown permission",
        );
        has(
            broken("/roles/0/principal_kind", json!("agent")),
            "human-required",
        );
        has(broken("/agents/0/id", json!("")), "has no id");
        let mut twice = example();
        twice["roles"]
            .as_array_mut()
            .unwrap()
            .push(json!({"id": "cto", "principal_kind": "human"}));
        has(validate(&twice, "acme"), "appears twice");
    }

    #[tokio::test]
    async fn drafts_change_until_published_and_published_versions_never_do() {
        let store = Memory::default();
        let ana = Actor::of("oidc:ana");
        let now = Utc::now();
        let first = store
            .draft("acme", json!({"schema_version": "1.0"}), &ana, now)
            .await
            .unwrap();
        assert_eq!(
            (first.definition_version, first.status.as_str()),
            (1, "draft")
        );
        assert!(!first.problems.is_empty());
        assert!(matches!(
            store.publish("acme", 1, &ana, now).await,
            Err(Error::Invalid(_))
        ));
        let fixed = store
            .replace("acme", 1, example(), &ana, now)
            .await
            .unwrap();
        assert!(fixed.problems.is_empty());
        let published = store.publish("acme", 1, &ana, now).await.unwrap();
        assert_eq!(published.status, "published");
        assert!(matches!(
            store.replace("acme", 1, example(), &ana, now).await,
            Err(Error::Published)
        ));
        assert!(matches!(
            store.publish("acme", 1, &ana, now).await,
            Err(Error::Published)
        ));
        let next = store.draft("acme", example(), &ana, now).await.unwrap();
        assert_eq!(next.definition_version, 2);
        assert_eq!(
            store.get("acme", 1).await.unwrap().document["future_optional_field"]["kept"],
            true
        );
        let actions: Vec<String> = store
            .audit("acme", 10)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.action)
            .collect();
        assert_eq!(
            actions,
            [
                "definition.drafted",
                "definition.published",
                "definition.replaced",
                "definition.drafted"
            ]
        );
        assert!(store.list("other").await.unwrap().is_empty());
    }
}
