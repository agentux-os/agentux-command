//! What Command knows of each node, from its messages. In memory: a restarted
//! Command learns its nodes again from their next hello and events.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, MutexGuard};

use serde::Serialize;
use serde_json::Value;

/// What Command knows of one AgentUX node: what it said in node.hello and
/// heartbeats, and the latest of its agent, limit, run and approval events.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
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
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Node>> {
        self.nodes.lock().unwrap_or_else(|e| e.into_inner())
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
        true
    }

    pub fn get(&self, id: &str) -> Option<Node> {
        self.lock().get(id).cloned()
    }

    /// Every node, by id.
    pub fn list(&self) -> Vec<Node> {
        let mut list: Vec<Node> = self.lock().values().cloned().collect();
        list.sort_by(|a, b| a.id.cmp(&b.id));
        list
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
