//! Missions run as workflows (ADR 0015, ADR 0020): a run is pinned to the
//! published definition version it started under and walks its workflow's
//! steps as an explicit state machine kept in the database.
//!
//! - **agent** steps go to a node as AUXP `run.start`, with a command id
//!   derived from the run, the step and the attempt, so a resend after a
//!   crash is the same command. The step finishes with the node's
//!   `run.completed` or `run.failed`; if Command missed those, it asks the
//!   node (`run.snapshot`) when the step is due.
//! - **gate** steps request an approval in the Office; a `human-required`
//!   gate is decided only by a human with `approval.decide`.
//! - **action** steps (and kinds Command does not execute) are recorded as
//!   done.
//!
//! Every transition is saved with a revision check and announced as an
//! Office event (`mission.run.*`, the run's actor, its definition version),
//! so the event log is the run's audit trail. A run waits, rather than
//! fails, while no node is reachable; a failed run can be resumed, which
//! retries its step.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::definitions::{self, Actor};
use crate::office::{self, OfficeRepository};

/// How often a waiting agent step asks its node, when no event came.
const RECONCILE_AFTER: i64 = 60;
/// How long a run waits for a node of its organization before trying again.
const RETRY_NODE_AFTER: i64 = 30;
/// How long an agent step may run before it is given up.
const STEP_TIMEOUT_HOURS: i64 = 24;

pub const RUNNING: &str = "running";
pub const WAITING: &str = "waiting";
pub const PAUSED: &str = "paused";
pub const COMPLETED: &str = "completed";
pub const FAILED: &str = "failed";
pub const CANCELLED: &str = "cancelled";

/// What a run is asked to do: the repository and prompt its agent steps
/// start from, and optionally the node to run on.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Input {
    pub mission_id: String,
    pub repository: String,
    pub prompt: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub title: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub node_id: String,
}

/// One step's progress.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Step {
    pub kind: String,
    /// `dispatching`, `held` (at the node, for someone to confirm),
    /// `waiting`, `done`, `failed` or `rejected`.
    pub status: String,
    pub attempt: i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub command_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub node_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub node_run_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub approval_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decided_by: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub error: String,
}

/// A mission's run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Run {
    pub run_id: String,
    pub organization_id: String,
    pub definition_version: i64,
    pub mission_id: String,
    pub workflow_id: String,
    pub status: String,
    /// The current step; empty once the run is over.
    pub step: String,
    pub input: Input,
    pub steps: BTreeMap<String, Step>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    /// When the engine looks at the run next, if it has to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_at: Option<DateTime<Utc>>,
    pub created_by: Actor,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Bumped by every save; also the sequence of the run's events.
    pub revision: i64,
}

impl Run {
    pub fn finished(&self) -> bool {
        matches!(self.status.as_str(), COMPLETED | CANCELLED)
    }
}

#[derive(Debug)]
pub enum Error {
    NotFound,
    /// The request does not fit the run or the definition.
    Invalid(String),
    /// Someone else changed the run first.
    Conflict,
    Storage(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotFound => f.write_str("run not found"),
            Error::Invalid(m) | Error::Storage(m) => f.write_str(m),
            Error::Conflict => f.write_str("the run changed meanwhile"),
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

/// Where runs are kept.
#[async_trait]
pub trait Store: Send + Sync {
    async fn insert(&self, run: &Run) -> Result<(), Error>;
    async fn get(&self, organization_id: &str, run_id: &str) -> Result<Run, Error>;
    /// The organization's runs, newest first.
    async fn list(&self, organization_id: &str) -> Result<Vec<Run>, Error>;
    /// Saves `run` if it is still at revision `run.revision - 1`.
    async fn save(&self, run: &Run) -> Result<(), Error>;
    /// Runs the engine still drives (running or waiting), in an
    /// organization or in all.
    async fn active(&self, organization_id: Option<&str>) -> Result<Vec<Run>, Error>;
}

/// Runs in memory: the demo server and tests.
#[derive(Default)]
pub struct Memory {
    runs: Mutex<BTreeMap<String, Run>>,
}

impl Memory {
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Run>> {
        self.runs.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[async_trait]
impl Store for Memory {
    async fn insert(&self, run: &Run) -> Result<(), Error> {
        self.lock().insert(run.run_id.clone(), run.clone());
        Ok(())
    }

    async fn get(&self, org: &str, run_id: &str) -> Result<Run, Error> {
        self.lock()
            .get(run_id)
            .filter(|r| r.organization_id == org)
            .cloned()
            .ok_or(Error::NotFound)
    }

    async fn list(&self, org: &str) -> Result<Vec<Run>, Error> {
        let mut runs: Vec<Run> = self
            .lock()
            .values()
            .filter(|r| r.organization_id == org)
            .cloned()
            .collect();
        runs.sort_by_key(|r| std::cmp::Reverse(r.created_at));
        Ok(runs)
    }

    async fn save(&self, run: &Run) -> Result<(), Error> {
        let mut runs = self.lock();
        match runs.get(&run.run_id) {
            Some(stored) if stored.revision == run.revision - 1 => {
                runs.insert(run.run_id.clone(), run.clone());
                Ok(())
            }
            Some(_) => Err(Error::Conflict),
            None => Err(Error::NotFound),
        }
    }

    async fn active(&self, org: Option<&str>) -> Result<Vec<Run>, Error> {
        Ok(self
            .lock()
            .values()
            .filter(|r| org.is_none_or(|o| r.organization_id == o))
            .filter(|r| matches!(r.status.as_str(), RUNNING | WAITING))
            .cloned()
            .collect())
    }
}

/// Runs in Command's database (migration 008_mission_runs).
pub struct Postgres {
    pool: deadpool_postgres::Pool,
}

impl Postgres {
    pub fn new(pool: deadpool_postgres::Pool) -> Self {
        Self { pool }
    }
}

use tokio_postgres::types::Json;

#[async_trait]
impl Store for Postgres {
    async fn insert(&self, run: &Run) -> Result<(), Error> {
        let client = self.pool.get().await?;
        client
            .execute(
                "INSERT INTO mission_runs (run_id, organization_id, status, run_at, revision, doc, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                &[&run.run_id, &run.organization_id, &run.status, &run.run_at, &run.revision, &Json(run), &run.created_at],
            )
            .await?;
        Ok(())
    }

    async fn get(&self, org: &str, run_id: &str) -> Result<Run, Error> {
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                "SELECT doc FROM mission_runs WHERE organization_id = $1 AND run_id = $2",
                &[&org, &run_id],
            )
            .await?
            .ok_or(Error::NotFound)?;
        Ok(row.get::<_, Json<Run>>(0).0)
    }

    async fn list(&self, org: &str) -> Result<Vec<Run>, Error> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT doc FROM mission_runs WHERE organization_id = $1 ORDER BY created_at DESC LIMIT 200",
                &[&org],
            )
            .await?;
        Ok(rows.iter().map(|r| r.get::<_, Json<Run>>(0).0).collect())
    }

    async fn save(&self, run: &Run) -> Result<(), Error> {
        let client = self.pool.get().await?;
        let changed = client
            .execute(
                "UPDATE mission_runs SET status = $3, run_at = $4, revision = $5, doc = $6
                 WHERE organization_id = $1 AND run_id = $2 AND revision = $7",
                &[
                    &run.organization_id,
                    &run.run_id,
                    &run.status,
                    &run.run_at,
                    &run.revision,
                    &Json(run),
                    &(run.revision - 1),
                ],
            )
            .await?;
        if changed == 1 {
            Ok(())
        } else {
            Err(Error::Conflict)
        }
    }

    async fn active(&self, org: Option<&str>) -> Result<Vec<Run>, Error> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT doc FROM mission_runs WHERE status IN ('running', 'waiting') AND ($1::text IS NULL OR organization_id = $1)",
                &[&org],
            )
            .await?;
        Ok(rows.iter().map(|r| r.get::<_, Json<Run>>(0).0).collect())
    }
}

/// A node's answer to a command that did not succeed.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeError {
    /// Not connected, or it went away: try again later.
    Unreachable(String),
    /// Its policy holds the command for someone at the node to confirm; the
    /// node reports the outcome later (`command.completed`).
    Held(String),
    /// It answered with an error (its policy, or the command failed).
    Refused(String),
}

/// The nodes the engine talks to: the server's command channel.
#[async_trait]
pub trait Nodes: Send + Sync {
    /// A node of the organization that can take a run now.
    fn pick(&self, organization_id: &str) -> Option<String>;
    /// Sends a command (its `params` carry `commandId`); the node's result.
    async fn command(
        &self,
        node_id: &str,
        method: &str,
        params: Map<String, Value>,
    ) -> Result<Value, NodeError>;
}

/// What a request to change a run asks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Control {
    Pause,
    Resume,
    Cancel,
}

/// The engine: starts runs and moves them along.
pub struct Engine {
    store: Arc<dyn Store>,
    definitions: Arc<dyn definitions::Store>,
    office: Arc<dyn OfficeRepository>,
}

/// The engine's own actor, for transitions nobody asked for.
fn engine_actor() -> Value {
    json!({"kind": "service", "id": "command.missions"})
}

fn step_of<'a>(workflow: &'a Value, id: &str) -> Option<&'a Value> {
    workflow["steps"]
        .as_array()?
        .iter()
        .find(|s| s["id"].as_str() == Some(id))
}

impl Engine {
    pub fn new(
        store: Arc<dyn Store>,
        definitions: Arc<dyn definitions::Store>,
        office: Arc<dyn OfficeRepository>,
    ) -> Self {
        Self {
            store,
            definitions,
            office,
        }
    }

    pub fn store(&self) -> &Arc<dyn Store> {
        &self.store
    }

    /// The definition document a run follows.
    async fn document(&self, run: &Run) -> Result<Value, Error> {
        self.definitions
            .get(&run.organization_id, run.definition_version)
            .await
            .map(|d| d.document)
            .map_err(|e| Error::Storage(e.to_string()))
    }

    fn workflow<'a>(document: &'a Value, run: &Run) -> Option<&'a Value> {
        document["workflows"]
            .as_array()?
            .iter()
            .find(|w| w["id"].as_str() == Some(run.workflow_id.as_str()))
    }

    /// Starts `input.mission_id` under the organization's newest published
    /// definition.
    pub async fn start(
        &self,
        org: &str,
        input: Input,
        actor: &Actor,
        nodes: &dyn Nodes,
    ) -> Result<Run, Error> {
        if input.repository.trim().is_empty() || input.prompt.trim().is_empty() {
            return Err(Error::Invalid("repository and prompt are required".into()));
        }
        let published = self
            .definitions
            .list(org)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
            .into_iter()
            .find(|d| d.status == "published")
            .ok_or_else(|| Error::Invalid("the organization has no published definition".into()))?;
        let document = self
            .definitions
            .get(org, published.definition_version)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
            .document;
        let workflow = document["workflows"]
            .as_array()
            .and_then(|ws| {
                ws.iter()
                    .find(|w| w["mission_id"].as_str() == Some(input.mission_id.as_str()))
            })
            .ok_or_else(|| {
                Error::Invalid(format!(
                    "mission {:?} has no workflow in definition {}",
                    input.mission_id, published.definition_version
                ))
            })?;
        let first = workflow["steps"][0]["id"]
            .as_str()
            .ok_or_else(|| Error::Invalid("the mission's workflow has no steps".into()))?;
        let now = Utc::now();
        let run = Run {
            run_id: format!("run-{}", crate::random_hex(8)),
            organization_id: org.into(),
            definition_version: published.definition_version,
            mission_id: input.mission_id.clone(),
            workflow_id: workflow["id"].as_str().unwrap_or_default().into(),
            status: RUNNING.into(),
            step: first.into(),
            input,
            steps: BTreeMap::new(),
            error: String::new(),
            run_at: None,
            created_by: actor.clone(),
            created_at: now,
            updated_at: now,
            revision: 1,
        };
        self.store.insert(&run).await?;
        let actor = serde_json::to_value(actor).ok();
        self.announce(&run, "mission.run.started", actor).await;
        self.advance(run, nodes).await
    }

    /// Saves the next revision of `run` and announces it as `event_type`.
    async fn commit(
        &self,
        mut run: Run,
        event_type: &str,
        actor: Option<Value>,
    ) -> Result<Run, Error> {
        run.revision += 1;
        run.updated_at = Utc::now();
        self.store.save(&run).await?;
        self.announce(&run, event_type, actor).await;
        Ok(run)
    }

    /// The Office event for the run as it is now.
    async fn announce(&self, run: &Run, event_type: &str, actor: Option<Value>) {
        let event = office::Event {
            event_id: format!("{}-{}", run.run_id, run.revision),
            event_type: event_type.into(),
            organization_id: run.organization_id.clone(),
            node_id: format!("command.run.{}", run.run_id),
            run_id: run.run_id.clone(),
            occurred_at: crate::now_rfc3339(),
            sequence: run.revision,
            correlation_id: run.run_id.clone(),
            actor: actor.or_else(|| Some(engine_actor())),
            definition_version: format!("definition-{}", run.definition_version),
            data: Some(json!({
                "runId": run.run_id,
                "missionId": run.mission_id,
                "workflowId": run.workflow_id,
                "status": run.status,
                "step": run.step,
                "error": run.error,
            })),
            ..office::Event::default()
        };
        if let Err(e) = self.office.apply_event(event).await {
            eprintln!(
                "agentux-command: run {} {event_type}: the Office did not take it: {e}",
                run.run_id
            );
        }
    }

    /// Moves `run` as far as it goes now, saving each transition.
    pub async fn advance(&self, mut run: Run, nodes: &dyn Nodes) -> Result<Run, Error> {
        let document = self.document(&run).await?;
        let Some(workflow) = Self::workflow(&document, &run).cloned() else {
            run.status = FAILED.into();
            run.error = format!("workflow {} is not in its definition", run.workflow_id);
            return self.commit(run, "mission.run.failed", None).await;
        };
        // Each pass handles one step; a finished step hands on to the next.
        for _ in 0..64 {
            if !matches!(run.status.as_str(), RUNNING | WAITING) {
                return Ok(run);
            }
            let Some(spec) = step_of(&workflow, &run.step).cloned() else {
                run.status = FAILED.into();
                run.error = format!("step {} is not in workflow {}", run.step, run.workflow_id);
                return self.commit(run, "mission.run.failed", None).await;
            };
            let state = run.steps.get(&run.step).cloned().unwrap_or_default();
            match state.status.as_str() {
                "done" => {
                    let next = spec["next"][0].as_str().map(str::to_string);
                    run = match next {
                        Some(next) => {
                            run.step = next;
                            run.status = RUNNING.into();
                            run.run_at = None;
                            self.commit(run, "mission.run.step_started", None).await?
                        }
                        None => {
                            run.step = String::new();
                            run.status = COMPLETED.into();
                            run.run_at = None;
                            return self.commit(run, "mission.run.completed", None).await;
                        }
                    };
                }
                "failed" | "rejected" => {
                    run.status = FAILED.into();
                    run.error = state.error.clone();
                    run.run_at = None;
                    return self.commit(run, "mission.run.failed", None).await;
                }
                _ => {
                    let kind = spec["kind"].as_str().unwrap_or_default().to_string();
                    let before = run.revision;
                    run = match kind.as_str() {
                        "agent" => self.agent_step(run, state, nodes).await?,
                        "gate" => self.gate_step(run, state, &document, &spec).await?,
                        _ => {
                            let now = Utc::now();
                            let step = Step {
                                kind,
                                status: "done".into(),
                                started_at: Some(now),
                                finished_at: Some(now),
                                ..Step::default()
                            };
                            run.steps.insert(run.step.clone(), step);
                            self.commit(run, "mission.run.step_completed", None).await?
                        }
                    };
                    let state = run.steps.get(&run.step).map(|s| s.status.as_str());
                    if run.revision == before
                        || !matches!(state, Some("done" | "failed" | "rejected"))
                    {
                        // Waiting on a node, a person or a retry.
                        return Ok(run);
                    }
                }
            }
        }
        Ok(run)
    }

    /// An agent step: dispatch it, or see whether its node run ended.
    async fn agent_step(
        &self,
        mut run: Run,
        mut state: Step,
        nodes: &dyn Nodes,
    ) -> Result<Run, Error> {
        let now = Utc::now();
        if state.status == "held" {
            // The node answers with command.completed once someone decides.
            return Ok(run);
        }
        if state.status == "waiting" {
            if run.run_at.is_some_and(|at| at > now) {
                return Ok(run);
            }
            if state
                .started_at
                .is_some_and(|at| now - at > Duration::hours(STEP_TIMEOUT_HOURS))
            {
                state.status = "failed".into();
                state.error = format!("step {} ran longer than {STEP_TIMEOUT_HOURS} h", run.step);
                state.finished_at = Some(now);
                run.steps.insert(run.step.clone(), state);
                return self.commit(run, "mission.run.step_failed", None).await;
            }
            // No event came: ask the node.
            let mut params = Map::new();
            params.insert(
                "commandId".into(),
                Value::String(format!(
                    "{}:{}:snapshot:{}",
                    run.run_id, run.step, run.revision
                )),
            );
            params.insert("runId".into(), Value::String(state.node_run_id.clone()));
            return match nodes.command(&state.node_id, "run.snapshot", params).await {
                Ok(snapshot) => match snapshot["status"].as_str() {
                    Some("done" | "completed") => self.finish_agent(run, "run.completed", "").await,
                    Some("failed" | "cancelled") => {
                        let error = snapshot["error"]
                            .as_str()
                            .unwrap_or("the node's run failed");
                        self.finish_agent(run, "run.failed", error).await
                    }
                    _ => {
                        run.run_at = Some(now + Duration::seconds(RECONCILE_AFTER));
                        self.commit(run, "mission.run.checked", None).await
                    }
                },
                Err(_) => {
                    run.run_at = Some(now + Duration::seconds(RECONCILE_AFTER));
                    self.commit(run, "mission.run.checked", None).await
                }
            };
        }
        // Not dispatched yet, or dispatched without an answer saved: send
        // (again) the same command.
        let node = if !run.input.node_id.is_empty() {
            Some(run.input.node_id.clone())
        } else if !state.node_id.is_empty() {
            Some(state.node_id.clone())
        } else {
            nodes.pick(&run.organization_id)
        };
        let Some(node) = node else {
            return self
                .wait_for_node(run, "no node of the organization is connected")
                .await;
        };
        if state.status.is_empty() {
            state = Step {
                kind: "agent".into(),
                status: "dispatching".into(),
                attempt: state.attempt.max(0) + 1,
                node_id: node.clone(),
                started_at: Some(now),
                ..Step::default()
            };
            state.command_id = format!("{}:{}:{}", run.run_id, run.step, state.attempt);
            run.steps.insert(run.step.clone(), state.clone());
            run.status = RUNNING.into();
            run = self.commit(run, "mission.run.step_started", None).await?;
        }
        let mut params = Map::new();
        for (key, value) in [
            ("commandId", state.command_id.clone()),
            ("missionId", run.mission_id.clone()),
            ("runId", run.run_id.clone()),
            ("repository", run.input.repository.clone()),
            ("prompt", run.input.prompt.clone()),
            ("title", run.input.title.clone()),
        ] {
            params.insert(key.into(), Value::String(value));
        }
        match nodes.command(&node, "run.start", params).await {
            Ok(result) => {
                state.status = "waiting".into();
                state.node_id = node;
                state.node_run_id = node_run_id(&result);
                run.steps.insert(run.step.clone(), state);
                run.status = RUNNING.into();
                run.error = String::new();
                run.run_at = Some(now + Duration::seconds(RECONCILE_AFTER));
                self.commit(run, "mission.run.dispatched", None).await
            }
            Err(NodeError::Unreachable(why)) => self.wait_for_node(run, &why).await,
            Err(NodeError::Held(why)) => {
                state.status = "held".into();
                state.node_id = node.clone();
                run.steps.insert(run.step.clone(), state);
                run.status = WAITING.into();
                run.error =
                    format!("waiting for someone at node {node} to confirm the step ({why})");
                run.run_at = None;
                self.commit(run, "mission.run.held", None).await
            }
            Err(NodeError::Refused(why)) => {
                state.status = "failed".into();
                state.error = format!("node {node} refused the step: {why}");
                state.finished_at = Some(now);
                run.steps.insert(run.step.clone(), state);
                self.commit(run, "mission.run.step_failed", None).await
            }
        }
    }

    async fn wait_for_node(&self, mut run: Run, why: &str) -> Result<Run, Error> {
        if run.status == WAITING && run.error == why && run.run_at.is_some_and(|at| at > Utc::now())
        {
            return Ok(run);
        }
        run.status = WAITING.into();
        run.error = why.into();
        run.run_at = Some(Utc::now() + Duration::seconds(RETRY_NODE_AFTER));
        self.commit(run, "mission.run.waiting", None).await
    }

    async fn finish_agent(&self, mut run: Run, how: &str, error: &str) -> Result<Run, Error> {
        let Some(state) = run.steps.get_mut(&run.step) else {
            return Ok(run);
        };
        state.finished_at = Some(Utc::now());
        let event = if how == "run.completed" {
            state.status = "done".into();
            "mission.run.step_completed"
        } else {
            state.status = "failed".into();
            state.error = if error.is_empty() {
                "the node's run failed".into()
            } else {
                error.into()
            };
            "mission.run.step_failed"
        };
        run.run_at = None;
        self.commit(run, event, None).await
    }

    /// A gate step: request its approval once, then wait for the decision.
    async fn gate_step(
        &self,
        mut run: Run,
        state: Step,
        document: &Value,
        spec: &Value,
    ) -> Result<Run, Error> {
        if state.status == "waiting" {
            return Ok(run);
        }
        let gate_id = spec["gate_id"].as_str().unwrap_or_default();
        let gate = document["gates"]
            .as_array()
            .and_then(|gs| gs.iter().find(|g| g["id"].as_str() == Some(gate_id)))
            .cloned()
            .unwrap_or(Value::Null);
        // A retried gate asks again, under a new approval.
        let attempt = state.attempt.max(0) + 1;
        let approval_id = format!("{}-{}-{attempt}", run.run_id, run.step);
        let mission = document["missions"]
            .as_array()
            .and_then(|ms| {
                ms.iter()
                    .find(|m| m["id"].as_str() == Some(run.mission_id.as_str()))
            })
            .and_then(|m| m["name"].as_str())
            .unwrap_or(run.mission_id.as_str())
            .to_string();
        run.steps.insert(
            run.step.clone(),
            Step {
                kind: "gate".into(),
                status: "waiting".into(),
                attempt,
                approval_id: approval_id.clone(),
                started_at: Some(Utc::now()),
                ..Step::default()
            },
        );
        run.status = WAITING.into();
        run.run_at = None;
        let run = self.commit(run, "mission.run.gate_waiting", None).await?;
        // The approval itself, on the run's own stream.
        let event = office::Event {
            event_id: format!("{approval_id}-requested"),
            event_type: "approval.requested".into(),
            organization_id: run.organization_id.clone(),
            node_id: format!("command.gate.{}", run.run_id),
            run_id: run.run_id.clone(),
            occurred_at: crate::now_rfc3339(),
            sequence: run.revision,
            correlation_id: run.run_id.clone(),
            actor: Some(engine_actor()),
            definition_version: format!("definition-{}", run.definition_version),
            data: Some(json!({
                "id": approval_id,
                "runId": run.run_id,
                "missionId": run.mission_id,
                "gateId": gate_id,
                "action": gate["kind"].as_str().unwrap_or("approval"),
                "title": format!("{mission}: {}", run.step),
                "policy": gate["policy"],
                "requiredPermissions": gate["required_permissions"],
            })),
            ..office::Event::default()
        };
        if let Err(e) = self.office.apply_event(event).await {
            eprintln!(
                "agentux-command: run {}: the approval was not recorded: {e}",
                run.run_id
            );
        }
        Ok(run)
    }

    /// The policy of the gate `approval_id` stands for, if it is one of a
    /// run's gates (`human-required`, ...).
    pub async fn gate_policy(&self, org: &str, approval_id: &str) -> Option<String> {
        let run = self.run_with_approval(org, approval_id).await?;
        let document = self.document(&run).await.ok()?;
        let workflow = Self::workflow(&document, &run)?;
        let gate_id = step_of(workflow, &run.step)?["gate_id"]
            .as_str()?
            .to_string();
        let gates = document["gates"].as_array()?;
        let gate = gates
            .iter()
            .find(|g| g["id"].as_str() == Some(gate_id.as_str()))?;
        Some(gate["policy"].as_str().unwrap_or_default().to_string())
    }

    async fn run_with_approval(&self, org: &str, approval_id: &str) -> Option<Run> {
        self.store
            .active(Some(org))
            .await
            .ok()?
            .into_iter()
            .find(|r| {
                r.steps
                    .get(&r.step)
                    .is_some_and(|s| s.approval_id == approval_id)
            })
    }

    /// An approval was decided: the gate it stands for goes on or fails.
    pub async fn decided(
        &self,
        org: &str,
        approval_id: &str,
        decision: &str,
        actor: Option<Value>,
        nodes: &dyn Nodes,
    ) -> Result<Option<Run>, Error> {
        let Some(mut run) = self.run_with_approval(org, approval_id).await else {
            return Ok(None);
        };
        let step = run.step.clone();
        let Some(state) = run.steps.get_mut(&step) else {
            return Ok(None);
        };
        if state.status != "waiting" {
            return Ok(Some(run));
        }
        state.finished_at = Some(Utc::now());
        state.decided_by = actor.clone();
        if decision == "approved" {
            state.status = "done".into();
        } else {
            state.status = "rejected".into();
            state.error = format!("gate {step} was rejected");
        }
        run.status = RUNNING.into();
        let run = self.commit(run, "mission.run.gate_decided", actor).await?;
        self.advance(run, nodes).await.map(Some)
    }

    /// A node's run event: the agent step waiting on that node run goes on.
    pub async fn node_event(
        &self,
        org: &str,
        node_run_id: &str,
        event_type: &str,
        error: &str,
        nodes: &dyn Nodes,
    ) -> Result<Option<Run>, Error> {
        if node_run_id.is_empty() || !matches!(event_type, "run.completed" | "run.failed") {
            return Ok(None);
        }
        let run = self.store.active(Some(org)).await?.into_iter().find(|r| {
            r.steps
                .get(&r.step)
                .is_some_and(|s| s.status == "waiting" && s.node_run_id == node_run_id)
        });
        let Some(run) = run else {
            return Ok(None);
        };
        let run = self.finish_agent(run, event_type, error).await?;
        self.advance(run, nodes).await.map(Some)
    }

    /// A command the node held was decided there (`command.completed`): the
    /// held step starts, or fails if it was rejected or did not start.
    pub async fn command_completed(
        &self,
        org: &str,
        command_id: &str,
        ok: bool,
        result: &Value,
        error: &str,
        nodes: &dyn Nodes,
    ) -> Result<Option<Run>, Error> {
        if command_id.is_empty() {
            return Ok(None);
        }
        let run = self.store.active(Some(org)).await?.into_iter().find(|r| {
            r.steps
                .get(&r.step)
                .is_some_and(|s| s.status == "held" && s.command_id == command_id)
        });
        let Some(mut run) = run else {
            return Ok(None);
        };
        let step = run.step.clone();
        let state = run.steps.get_mut(&step).expect("the held step");
        let now = Utc::now();
        let run_id = node_run_id(result);
        if ok && !run_id.is_empty() {
            state.status = "waiting".into();
            state.node_run_id = run_id;
            run.status = RUNNING.into();
            run.error = String::new();
            run.run_at = Some(now + Duration::seconds(RECONCILE_AFTER));
            return self
                .commit(run, "mission.run.dispatched", None)
                .await
                .map(Some);
        }
        state.status = "failed".into();
        state.finished_at = Some(now);
        state.error = if error.is_empty() {
            format!("node {} did not start the step", state.node_id)
        } else {
            format!("node {}: {error}", state.node_id)
        };
        run.status = RUNNING.into();
        let run = self.commit(run, "mission.run.step_failed", None).await?;
        self.advance(run, nodes).await.map(Some)
    }

    /// Pauses, resumes or cancels a run. Resuming a failed run retries its
    /// step.
    pub async fn control(
        &self,
        org: &str,
        run_id: &str,
        control: Control,
        actor: &Actor,
        nodes: &dyn Nodes,
    ) -> Result<Run, Error> {
        let mut run = self.store.get(org, run_id).await?;
        let actor = serde_json::to_value(actor).ok();
        match control {
            Control::Pause => {
                if !matches!(run.status.as_str(), RUNNING | WAITING) {
                    return Err(Error::Invalid(format!(
                        "a {} run cannot be paused",
                        run.status
                    )));
                }
                run.status = PAUSED.into();
                self.commit(run, "mission.run.paused", actor).await
            }
            Control::Resume => {
                match run.status.as_str() {
                    PAUSED => {}
                    FAILED if !run.step.is_empty() => {
                        // Retry the step that failed.
                        let attempt = run.steps.get(&run.step).map_or(0, |s| s.attempt);
                        run.steps.insert(
                            run.step.clone(),
                            Step {
                                attempt,
                                ..Step::default()
                            },
                        );
                        run.error = String::new();
                    }
                    other => {
                        return Err(Error::Invalid(format!("a {other} run cannot be resumed")));
                    }
                }
                run.status = RUNNING.into();
                run.run_at = None;
                let run = self.commit(run, "mission.run.resumed", actor).await?;
                self.advance(run, nodes).await
            }
            Control::Cancel => {
                if run.finished() {
                    return Err(Error::Invalid(format!(
                        "a {} run cannot be cancelled",
                        run.status
                    )));
                }
                if let Some(state) = run.steps.get(&run.step)
                    && state.status == "waiting"
                    && !state.node_run_id.is_empty()
                {
                    let mut params = Map::new();
                    params.insert(
                        "commandId".into(),
                        Value::String(format!("{}:{}:cancel", run.run_id, run.step)),
                    );
                    params.insert("runId".into(), Value::String(state.node_run_id.clone()));
                    // Best effort: the node may be gone; the run ends here either way.
                    let _ = nodes.command(&state.node_id, "run.cancel", params).await;
                }
                run.status = CANCELLED.into();
                run.run_at = None;
                self.commit(run, "mission.run.cancelled", actor).await
            }
        }
    }

    /// Moves every run that is due. Returns how many were looked at.
    pub async fn tick(&self, nodes: &dyn Nodes) -> usize {
        let Ok(runs) = self.store.active(None).await else {
            return 0;
        };
        let now = Utc::now();
        let mut moved = 0;
        for run in runs {
            let due = run.run_at.is_none_or(|at| at <= now);
            let idle_gate = run
                .steps
                .get(&run.step)
                // Gates wait for people, held steps for someone at the node.
                .is_some_and(|s| (s.kind == "gate" && s.status == "waiting") || s.status == "held");
            if !due || idle_gate {
                continue;
            }
            moved += 1;
            match self.advance(run, nodes).await {
                Ok(_) | Err(Error::Conflict) => {}
                Err(e) => eprintln!("agentux-command: missions: {e}"),
            }
        }
        moved
    }
}

/// The node's own id for a run it started: `run.start` answers
/// `{"localRunId", "run": {"id", ...}}` (agentux-core's gateway).
fn node_run_id(result: &Value) -> String {
    [
        &result["localRunId"],
        &result["run"]["id"],
        &result["id"],
        &result["runId"],
    ]
    .iter()
    .find_map(|v| v.as_str())
    .unwrap_or_default()
    .to_string()
}
