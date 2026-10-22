//! What Command knows of each node, from its messages. Kept in memory for
//! every request, and, on a production server, written to PostgreSQL behind
//! each change ([`persist`]) and read back at startup, so a restarted Command
//! still knows its nodes, their agents, runs and approvals.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, MutexGuard};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;

/// A node heard from within this many seconds is online...
const ONLINE_FOR: i64 = 90;
/// ...within this many, degraded (heartbeats come every 30 seconds).
const DEGRADED_FOR: i64 = 300;

/// What Command knows of one AgentUX node: what it said in node.hello and
/// heartbeats, and the latest of its agent, limit, run and approval events.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Node {
    #[serde(rename = "nodeId")]
    pub id: String,
    pub organization_id: String,
    pub capabilities: Option<Vec<String>>,
    pub status: String,
    #[serde(rename = "agentuxVersion", skip_serializing_if = "String::is_empty")]
    pub agentux_version: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub last_seen_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capacity: Option<Value>,
    /// The node's agent CLIs and usage limits, as its last harness.status
    /// and harness.limits_changed reported them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub harnesses: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limits: Option<Value>,
    /// The latest event of each run, and approvals still waiting, by id.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub runs: BTreeMap<String, Value>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub pending_approvals: BTreeMap<String, Value>,
    pub last_sequence: i64,
    /// Whether the node has its command connection open (set by the server).
    pub connected: bool,
    /// `online`, `degraded` or `offline`, from how long ago it was last
    /// heard from (set when listed).
    #[serde(skip_deserializing)]
    pub presence: String,
}

impl Node {
    /// Online, degraded or offline at `now`, from when it was last heard.
    pub fn presence_at(&self, now: DateTime<Utc>) -> &'static str {
        let Ok(seen) = DateTime::parse_from_rfc3339(&self.last_seen_at) else {
            return "offline";
        };
        match (now - seen.with_timezone(&Utc)).num_seconds() {
            age if age <= ONLINE_FOR => "online",
            age if age <= DEGRADED_FOR => "degraded",
            _ => "offline",
        }
    }
}

/// One node event as the registry applies it.
#[derive(Debug, Clone, Default)]
pub struct Event {
    pub node_id: String,
    pub organization_id: String,
    pub method: String,
    pub sequence: i64,
    pub occurred_at: String,
    pub run_id: String,
    pub request_id: String,
    pub state: String,
    pub harnesses: Option<Value>,
    pub limits: Option<Value>,
    /// The whole params object, kept for runs and approvals.
    pub params: Option<Value>,
}

/// Bounds the runs kept per node; the oldest finished ones go first.
const MAX_RUNS: usize = 200;

#[derive(Default)]
pub struct Registry {
    nodes: Mutex<HashMap<String, Node>>,
    /// Where changed nodes go to be stored, when they are.
    store: Mutex<Option<mpsc::UnboundedSender<Node>>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Node>> {
        self.nodes.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Sends every change from now on to `store` ([`persist::writer`]).
    pub fn store_to(&self, store: mpsc::UnboundedSender<Node>) {
        *self.store.lock().unwrap_or_else(|e| e.into_inner()) = Some(store);
    }

    /// Takes nodes stored before a restart, as they were.
    pub fn load(&self, stored: Vec<Node>) {
        let mut nodes = self.lock();
        for mut node in stored {
            node.connected = false;
            nodes.insert(node.id.clone(), node);
        }
    }

    fn stored(&self, node: &Node) {
        if let Some(store) = self
            .store
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            let _ = store.send(node.clone());
        }
    }

    pub fn hello(&self, mut node: Node) {
        let mut nodes = self.lock();
        // A node saying hello again keeps what it reported before.
        if let Some(known) = nodes.get(&node.id) {
            node.harnesses = known.harnesses.clone();
            node.limits = known.limits.clone();
            node.runs = known.runs.clone();
            node.pending_approvals = known.pending_approvals.clone();
            node.last_sequence = known.last_sequence;
        }
        node.status = "online".into();
        self.stored(&node);
        nodes.insert(node.id.clone(), node);
    }

    pub fn heartbeat(&self, id: &str, status: &str) {
        self.heartbeat_with(id, status, None, "");
    }

    /// Records a heartbeat with the node's capacity and time.
    pub fn heartbeat_with(&self, id: &str, status: &str, capacity: Option<Value>, at: &str) {
        let mut nodes = self.lock();
        let Some(node) = nodes.get_mut(id) else {
            return;
        };
        node.status = status.into();
        if capacity.is_some() {
            node.capacity = capacity;
        }
        if !at.is_empty() {
            node.last_seen_at = at.into();
        }
        self.stored(node);
    }

    /// Records a node event. Events are at least once: one whose sequence is
    /// not above the node's last is a duplicate and changes nothing. Events
    /// from a node Command does not know yet (Command restarted) register
    /// it. Returns whether the event was new.
    pub fn apply(&self, event: Event) -> bool {
        let mut nodes = self.lock();
        let node = nodes.entry(event.node_id.clone()).or_insert_with(|| Node {
            id: event.node_id.clone(),
            organization_id: event.organization_id.clone(),
            status: "online".into(),
            ..Node::default()
        });
        if event.sequence <= node.last_sequence {
            return false;
        }
        node.last_sequence = event.sequence;
        if !event.occurred_at.is_empty() {
            node.last_seen_at = event.occurred_at.clone();
        }
        let params = event.params.unwrap_or(Value::Null);
        match event.method.as_str() {
            "harness.status" => node.harnesses = event.harnesses,
            "harness.limits_changed" => node.limits = event.limits,
            "approval.requested" => {
                if !event.request_id.is_empty() {
                    node.pending_approvals.insert(event.request_id, params);
                }
            }
            "run.accepted" | "run.started" | "run.updated" | "run.completed" | "run.failed"
                if !event.run_id.is_empty() =>
            {
                node.runs.insert(event.run_id.clone(), params);
                if node.runs.len() > MAX_RUNS {
                    trim_runs(&mut node.runs);
                }
                // A run that is not waiting has no approval pending any more.
                if event.state != "waiting" {
                    node.pending_approvals.retain(|_, approval| {
                        approval.get("runId").and_then(Value::as_str) != Some(&event.run_id)
                    });
                }
            }
            _ => {}
        }
        self.stored(node);
        true
    }

    pub fn get(&self, id: &str) -> Option<Node> {
        self.lock().get(id).cloned()
    }

    /// Every node, by id, with its presence now.
    pub fn list(&self) -> Vec<Node> {
        let now = Utc::now();
        let mut list: Vec<Node> = self.lock().values().cloned().collect();
        for node in &mut list {
            node.presence = node.presence_at(now).to_string();
        }
        list.sort_by(|a, b| a.id.cmp(&b.id));
        list
    }
}

/// The registry in PostgreSQL (migration 005_node_state).
pub mod persist {
    use std::collections::HashMap;

    use tokio::sync::mpsc;
    use tokio_postgres::types::Json;

    use super::Node;

    /// Every stored node.
    pub async fn load(pool: &deadpool_postgres::Pool) -> Result<Vec<Node>, String> {
        let client = pool.get().await.map_err(|e| e.to_string())?;
        let rows = client
            .query("SELECT payload FROM node_state ORDER BY node_id", &[])
            .await
            .map_err(|e| format!("read node state: {e}"))?;
        Ok(rows
            .iter()
            .map(|row| row.get::<_, Json<Node>>(0).0)
            .collect())
    }

    /// Starts the task that stores changed nodes; send it each change. A
    /// burst of changes is written once per node, the latest.
    pub fn writer(pool: deadpool_postgres::Pool) -> mpsc::UnboundedSender<Node> {
        let (tx, mut rx) = mpsc::unbounded_channel::<Node>();
        tokio::spawn(async move {
            while let Some(first) = rx.recv().await {
                let mut latest = HashMap::from([(first.id.clone(), first)]);
                while let Ok(more) = rx.try_recv() {
                    latest.insert(more.id.clone(), more);
                }
                if let Err(e) = write(&pool, latest.into_values()).await {
                    eprintln!("agentux-command: cannot store node state: {e}");
                }
            }
        });
        tx
    }

    async fn write(
        pool: &deadpool_postgres::Pool,
        nodes: impl Iterator<Item = Node>,
    ) -> Result<(), String> {
        let client = pool.get().await.map_err(|e| e.to_string())?;
        for mut node in nodes {
            node.connected = false;
            node.presence = String::new();
            client
                .execute(
                    "INSERT INTO node_state (node_id, organization_id, payload, updated_at) VALUES ($1, $2, $3, now())
                     ON CONFLICT (node_id) DO UPDATE SET organization_id = EXCLUDED.organization_id, payload = EXCLUDED.payload, updated_at = now()",
                    &[&node.id, &node.organization_id, &Json(&node)],
                )
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}

/// Drops finished runs, oldest started first, down to MAX_RUNS.
fn trim_runs(runs: &mut BTreeMap<String, Value>) {
    let mut done: Vec<(String, String)> = runs
        .iter()
        .filter(|(_, run)| {
            matches!(
                run.get("state").and_then(Value::as_str),
                Some("done" | "failed" | "cancelled")
            )
        })
        .map(|(id, run)| {
            let started = run.get("startedAt").and_then(Value::as_str).unwrap_or("");
            (id.clone(), started.to_string())
        })
        .collect();
    done.sort_by(|a, b| a.1.cmp(&b.1));
    for (id, _) in done {
        if runs.len() <= MAX_RUNS {
            return;
        }
        runs.remove(&id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn presence_follows_the_last_time_a_node_was_heard() {
        let now = Utc::now();
        let seen = |secs: i64| Node {
            last_seen_at: (now - chrono::Duration::seconds(secs)).to_rfc3339(),
            ..Node::default()
        };
        assert_eq!(seen(10).presence_at(now), "online");
        assert_eq!(seen(200).presence_at(now), "degraded");
        assert_eq!(seen(3600).presence_at(now), "offline");
        assert_eq!(
            Node::default().presence_at(now),
            "offline",
            "never heard from"
        );
    }

    #[tokio::test]
    async fn changes_go_to_the_store_and_load_back() {
        let registry = Registry::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        registry.store_to(tx);
        registry.hello(Node {
            id: "node-1".into(),
            organization_id: "org-1".into(),
            ..Node::default()
        });
        registry.heartbeat("node-1", "ready");
        assert_eq!(rx.recv().await.unwrap().status, "online");
        let latest = rx.recv().await.unwrap();
        assert_eq!(latest.status, "ready");

        let restarted = Registry::new();
        restarted.load(vec![Node {
            connected: true,
            ..latest
        }]);
        let node = restarted.get("node-1").unwrap();
        assert_eq!(node.status, "ready");
        assert!(!node.connected, "no connection survives a restart");
    }

    #[test]
    fn tracks_hello_and_heartbeat() {
        let registry = Registry::new();
        registry.hello(Node {
            id: "node-1".into(),
            organization_id: "org-1".into(),
            capabilities: Some(vec!["run.start".into()]),
            ..Node::default()
        });
        registry.heartbeat("node-1", "ready");
        let node = registry.get("node-1").unwrap();
        assert_eq!(node.status, "ready");
        assert_eq!(node.organization_id, "org-1");
    }

    #[test]
    fn ignores_heartbeats_from_unknown_nodes() {
        let registry = Registry::new();
        registry.heartbeat("unknown", "ready");
        assert!(registry.get("unknown").is_none());
    }

    #[test]
    fn applies_node_events_once_and_in_order() {
        let registry = Registry::new();
        assert!(registry.apply(Event {
            node_id: "node-1".into(),
            organization_id: "org-1".into(),
            method: "harness.status".into(),
            sequence: 1,
            harnesses: Some(json!([{"id": "codex"}])),
            ..Event::default()
        }));
        assert!(!registry.apply(Event {
            node_id: "node-1".into(),
            method: "harness.status".into(),
            sequence: 1,
            harnesses: Some(json!([])),
            ..Event::default()
        }));
        registry.apply(Event {
            node_id: "node-1".into(),
            method: "approval.requested".into(),
            sequence: 2,
            request_id: "q1".into(),
            params: Some(json!({"runId": "r1", "requestId": "q1"})),
            ..Event::default()
        });
        registry.apply(Event {
            node_id: "node-1".into(),
            method: "run.updated".into(),
            sequence: 3,
            run_id: "r1".into(),
            state: "waiting".into(),
            params: Some(json!({"runId": "r1", "state": "waiting"})),
            ..Event::default()
        });
        let node = registry.get("node-1").unwrap();
        assert_eq!(node.harnesses, Some(json!([{"id": "codex"}])));
        assert_eq!(node.pending_approvals.len(), 1);
        assert_eq!(node.last_sequence, 3);
        assert_eq!(node.organization_id, "org-1");

        registry.apply(Event {
            node_id: "node-1".into(),
            method: "run.updated".into(),
            sequence: 4,
            run_id: "r1".into(),
            state: "running".into(),
            params: Some(json!({"runId": "r1", "state": "running"})),
            ..Event::default()
        });
        let node = registry.get("node-1").unwrap();
        assert!(
            node.pending_approvals.is_empty(),
            "the approval goes once the run goes on"
        );
        assert_eq!(node.runs.len(), 1);

        // Hello again keeps what the node reported.
        registry.hello(Node {
            id: "node-1".into(),
            organization_id: "org-1".into(),
            ..Node::default()
        });
        let node = registry.get("node-1").unwrap();
        assert_eq!(node.last_sequence, 4);
        assert!(node.harnesses.is_some());
    }

    #[test]
    fn keeps_at_most_max_runs_dropping_the_oldest_finished() {
        let registry = Registry::new();
        for i in 0..=MAX_RUNS as i64 {
            let state = if i == 0 { "running" } else { "done" };
            registry.apply(Event {
                node_id: "n".into(),
                method: "run.updated".into(),
                sequence: i + 1,
                run_id: format!("r{i}"),
                state: state.into(),
                params: Some(json!({"state": state, "startedAt": format!("2026-10-21T00:{:02}:{:02}Z", i / 60, i % 60)})),
                ..Event::default()
            });
        }
        let node = registry.get("n").unwrap();
        assert_eq!(node.runs.len(), MAX_RUNS);
        assert!(
            node.runs.contains_key("r0"),
            "a running run is never dropped"
        );
        assert!(
            !node.runs.contains_key("r1"),
            "the oldest finished run goes first"
        );
    }
}
