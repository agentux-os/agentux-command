//! Missions run as workflows: the engine against a scripted node, and a
//! whole mission through the API with a node on the command channel.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use agentux_command::definitions::{self, Actor, Store as _};
use agentux_command::missions::{self, Control, Engine, Input, NodeError, Nodes};
use agentux_command::office::{self, OfficeRepository};
use agentux_command::server::Handler;
use async_trait::async_trait;
use axum::http::StatusCode;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Map, Value, json};
use tokio_tungstenite::tungstenite::{self, client::IntoClientRequest};

use common::{call, hello};

/// An organization whose mission is: an agent implements, a human approves,
/// then it ships.
fn definition(org: &str) -> Value {
    json!({
        "schema_version": "1.0",
        "organization": { "id": org, "name": "Acme Robotics" },
        "areas": [{ "id": "engineering", "name": "Engineering", "owner_role_id": "cto" }],
        "roles": [
            { "id": "cto", "name": "CTO", "principal_kind": "human", "permissions": ["organization.read", "approval.decide"] },
            { "id": "dev", "name": "Developer", "principal_kind": "agent", "permissions": ["run.read"] }
        ],
        "missions": [{ "id": "ship", "area_id": "engineering", "name": "Ship a release", "owner_role_id": "cto" }],
        "workflows": [{ "id": "ship-v1", "mission_id": "ship", "steps": [
            { "id": "implement", "kind": "agent", "role_id": "dev", "next": ["review"] },
            { "id": "review", "kind": "gate", "gate_id": "release", "next": ["publish"] },
            { "id": "publish", "kind": "action", "next": [] }
        ]}],
        "gates": [{ "id": "release", "kind": "approval", "required_permissions": ["approval.decide"], "policy": "human-required" }],
        "agents": [{ "id": "dev-1", "role_id": "dev" }],
        "nodes": []
    })
}

/// A node that answers as told.
#[derive(Default)]
struct Scripted {
    online: Mutex<bool>,
    /// The node's policy holds run.start for someone there.
    hold: Mutex<bool>,
    refuse: Mutex<Option<String>>,
    snapshot: Mutex<Value>,
    sent: Mutex<Vec<(String, Map<String, Value>)>>,
}

impl Scripted {
    fn online() -> Self {
        let node = Self::default();
        *node.online.lock().unwrap() = true;
        node
    }

    fn sent(&self, method: &str) -> Vec<Map<String, Value>> {
        self.sent
            .lock()
            .unwrap()
            .iter()
            .filter(|(m, _)| m == method)
            .map(|(_, p)| p.clone())
            .collect()
    }
}

#[async_trait]
impl Nodes for Scripted {
    fn pick(&self, _: &str) -> Option<String> {
        self.online.lock().unwrap().then(|| "node-1".to_string())
    }

    async fn command(
        &self,
        _: &str,
        method: &str,
        params: Map<String, Value>,
    ) -> Result<Value, NodeError> {
        if !*self.online.lock().unwrap() {
            return Err(NodeError::Unreachable("the node is not connected".into()));
        }
        self.sent
            .lock()
            .unwrap()
            .push((method.to_string(), params.clone()));
        if let Some(why) = self.refuse.lock().unwrap().clone() {
            return Err(NodeError::Refused(why));
        }
        if method == "run.start" && *self.hold.lock().unwrap() {
            return Err(NodeError::Held("awaiting_local_confirmation".into()));
        }
        Ok(match method {
            "run.start" => json!({"id": format!("node-{}", params["commandId"].as_str().unwrap())}),
            "run.snapshot" => self.snapshot.lock().unwrap().clone(),
            _ => Value::Null,
        })
    }
}

struct World {
    engine: Engine,
    office: Arc<office::Store>,
    runs: Arc<dyn missions::Store>,
}

async fn world() -> World {
    let defs = Arc::new(definitions::Memory::default());
    let ana = Actor::of("oidc:ana");
    defs.draft("acme", definition("acme"), &ana, Utc::now())
        .await
        .unwrap();
    defs.publish("acme", 1, &ana, Utc::now()).await.unwrap();
    let office = Arc::new(office::Store::new());
    let runs: Arc<dyn missions::Store> = Arc::new(missions::Memory::default());
    World {
        engine: Engine::new(runs.clone(), defs, office.clone()),
        office,
        runs,
    }
}

fn input() -> Input {
    Input {
        mission_id: "ship".into(),
        repository: "https://github.com/acme/app".into(),
        prompt: "Ship it".into(),
        ..Input::default()
    }
}

fn ana() -> Actor {
    Actor::of("oidc:ana")
}

#[tokio::test]
async fn a_mission_runs_its_agent_step_its_human_gate_and_its_action() {
    let w = world().await;
    let node = Scripted::online();
    let run = w
        .engine
        .start("acme", input(), &ana(), &node)
        .await
        .unwrap();
    assert_eq!(run.definition_version, 1, "pinned to the published version");
    assert_eq!(
        (run.status.as_str(), run.step.as_str()),
        ("running", "implement")
    );
    let step = &run.steps["implement"];
    assert_eq!(step.status, "waiting");
    let starts = node.sent("run.start");
    assert_eq!(starts.len(), 1);
    assert_eq!(
        starts[0]["commandId"],
        format!("{}:implement:1", run.run_id)
    );
    assert_eq!(starts[0]["repository"], "https://github.com/acme/app");

    // The node's run ends: the gate asks a human.
    let run = w
        .engine
        .node_event("acme", &step.node_run_id, "run.completed", "", &node)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (run.status.as_str(), run.step.as_str()),
        ("waiting", "review")
    );
    let approval = run.steps["review"].approval_id.clone();
    assert_eq!(
        w.engine.gate_policy("acme", &approval).await.as_deref(),
        Some("human-required")
    );
    let projection = w.office.get_projection("acme").await.unwrap().unwrap();
    let pending = projection
        .approvals
        .iter()
        .find(|a| a["id"] == approval.as_str())
        .unwrap();
    assert_eq!(pending["status"], "pending");
    assert_eq!(pending["gateId"], "release");

    // Approved: the action, and done.
    let run = w
        .engine
        .decided(
            "acme",
            &approval,
            "approved",
            Some(json!({"kind": "user", "id": "oidc:ana"})),
            &node,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.status, "completed");
    assert_eq!(
        run.steps["review"].decided_by.as_ref().unwrap()["id"],
        "oidc:ana"
    );
    assert_eq!(run.steps["publish"].status, "done");

    // The Office shows the run, and every transition is in the log.
    let projection = w.office.get_projection("acme").await.unwrap().unwrap();
    let shown = projection
        .runs
        .iter()
        .find(|r| r["id"] == run.run_id.as_str())
        .unwrap();
    assert_eq!(shown["status"], "completed");
    assert_eq!(shown["definitionVersion"], "definition-1");
    let log = w
        .office
        .events(
            "acme",
            &office::Replay {
                limit: 1000,
                ..office::Replay::default()
            },
        )
        .await
        .unwrap();
    let types: Vec<&str> = log
        .iter()
        .filter(|l| l.event.run_id == run.run_id)
        .map(|l| l.event.event_type.as_str())
        .collect();
    assert_eq!(types.first(), Some(&"mission.run.started"));
    assert_eq!(types.last(), Some(&"mission.run.completed"));
    assert!(types.contains(&"mission.run.gate_decided"));
    let started = log
        .iter()
        .find(|l| l.event.event_type == "mission.run.started")
        .unwrap();
    assert_eq!(started.event.actor.as_ref().unwrap()["id"], "oidc:ana");
}

#[tokio::test]
async fn a_run_waits_for_a_node_and_retries_a_refused_step() {
    let w = world().await;
    let node = Scripted::default();
    let run = w
        .engine
        .start("acme", input(), &ana(), &node)
        .await
        .unwrap();
    assert_eq!(run.status, "waiting", "no node: waiting, not failed");
    assert!(run.error.contains("no node"), "{}", run.error);
    assert!(run.run_at.is_some(), "it looks again later");

    // The node is there, but refuses (its policy).
    *node.online.lock().unwrap() = true;
    *node.refuse.lock().unwrap() = Some("denied_by_policy".into());
    let run = w
        .engine
        .advance(w.runs.get("acme", &run.run_id).await.unwrap(), &node)
        .await
        .unwrap();
    assert_eq!(run.status, "failed");
    assert!(run.error.contains("denied_by_policy"), "{}", run.error);

    // Resuming a failed run retries its step, as a new attempt.
    *node.refuse.lock().unwrap() = None;
    let run = w
        .engine
        .control("acme", &run.run_id, Control::Resume, &ana(), &node)
        .await
        .unwrap();
    assert_eq!(run.status, "running");
    assert_eq!(run.steps["implement"].attempt, 2);
    assert_eq!(run.steps["implement"].status, "waiting");
    let last = node.sent("run.start").pop().unwrap();
    assert_eq!(last["commandId"], format!("{}:implement:2", run.run_id));
}

#[tokio::test]
async fn a_missed_node_event_is_caught_by_asking_the_node() {
    let w = world().await;
    let node = Scripted::online();
    let run = w
        .engine
        .start("acme", input(), &ana(), &node)
        .await
        .unwrap();
    // A restart lost the node's event; the step is due for a check.
    let mut due = w.runs.get("acme", &run.run_id).await.unwrap();
    due.run_at = Some(Utc::now() - chrono::Duration::seconds(1));
    due.revision += 1;
    w.runs.save(&due).await.unwrap();
    *node.snapshot.lock().unwrap() = json!({"status": "running"});
    assert_eq!(w.engine.tick(&node).await, 1);
    assert_eq!(
        w.runs.get("acme", &run.run_id).await.unwrap().step,
        "implement",
        "still running at the node"
    );
    let mut due = w.runs.get("acme", &run.run_id).await.unwrap();
    due.run_at = Some(Utc::now() - chrono::Duration::seconds(1));
    due.revision += 1;
    w.runs.save(&due).await.unwrap();
    *node.snapshot.lock().unwrap() = json!({"status": "done"});
    w.engine.tick(&node).await;
    let run = w.runs.get("acme", &run.run_id).await.unwrap();
    assert_eq!(
        (run.status.as_str(), run.step.as_str()),
        ("waiting", "review")
    );
    assert_eq!(node.sent("run.snapshot").len(), 2);
}

#[tokio::test]
async fn runs_pause_resume_and_cancel_and_a_rejected_gate_fails_the_run() {
    let w = world().await;
    let node = Scripted::online();
    let run = w
        .engine
        .start("acme", input(), &ana(), &node)
        .await
        .unwrap();
    let paused = w
        .engine
        .control("acme", &run.run_id, Control::Pause, &ana(), &node)
        .await
        .unwrap();
    assert_eq!(paused.status, "paused");
    assert!(
        w.engine
            .node_event(
                "acme",
                &run.steps["implement"].node_run_id,
                "run.completed",
                "",
                &node
            )
            .await
            .unwrap()
            .is_none(),
        "a paused run does not move"
    );
    assert!(matches!(
        w.engine
            .control("acme", &run.run_id, Control::Pause, &ana(), &node)
            .await,
        Err(missions::Error::Invalid(_))
    ));
    let resumed = w
        .engine
        .control("acme", &run.run_id, Control::Resume, &ana(), &node)
        .await
        .unwrap();
    assert_eq!(resumed.status, "running");
    let cancelled = w
        .engine
        .control("acme", &run.run_id, Control::Cancel, &ana(), &node)
        .await
        .unwrap();
    assert_eq!(cancelled.status, "cancelled");
    assert_eq!(
        node.sent("run.cancel").len(),
        1,
        "the node's run is cancelled too"
    );
    assert!(matches!(
        w.engine
            .control("acme", &run.run_id, Control::Resume, &ana(), &node)
            .await,
        Err(missions::Error::Invalid(_))
    ));

    let run = w
        .engine
        .start("acme", input(), &ana(), &node)
        .await
        .unwrap();
    let run = w
        .engine
        .node_event(
            "acme",
            &run.steps["implement"].node_run_id,
            "run.completed",
            "",
            &node,
        )
        .await
        .unwrap()
        .unwrap();
    let run = w
        .engine
        .decided(
            "acme",
            &run.steps["review"].approval_id,
            "rejected",
            None,
            &node,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.status, "failed");
    assert!(run.error.contains("rejected"), "{}", run.error);
    // Resumed, the gate asks again under a new approval.
    let run = w
        .engine
        .control("acme", &run.run_id, Control::Resume, &ana(), &node)
        .await
        .unwrap();
    assert_eq!(run.status, "waiting");
    assert!(run.steps["review"].approval_id.ends_with("-2"));
}

#[tokio::test]
async fn without_a_published_definition_nothing_starts() {
    let w = world().await;
    let node = Scripted::online();
    for (org, mission) in [("other", "ship"), ("acme", "unknown")] {
        let input = Input {
            mission_id: mission.into(),
            ..input()
        };
        assert!(matches!(
            w.engine.start(org, input, &ana(), &node).await,
            Err(missions::Error::Invalid(_))
        ));
    }
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn dial(address: &str, node: &str) -> Socket {
    let mut request = format!("ws://{address}/v1/protocol/connect?nodeId={node}")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", "Bearer any".parse().unwrap());
    tokio_tungstenite::connect_async(request).await.unwrap().0
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mission_completes_through_the_api_with_a_node_and_a_human() {
    let handler = Handler::new();
    let robot = handler
        .service_tokens()
        .issue(
            agentux_command::auth::TokenRequest {
                name: "robot".into(),
                organization_id: "acme".into(),
                capabilities: vec!["approval.decide".into(), "organization.read".into()],
                ..Default::default()
            },
            Utc::now(),
        )
        .await
        .unwrap()
        .secret;
    let app = handler.router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    tokio::spawn(axum::serve(listener, app.clone()).into_future());

    // The organization's definition, published.
    let (status, _) = call(
        &app,
        "POST",
        "/v1/organizations/acme/definitions",
        "demo-leader",
        Some(definition("acme")),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = call(
        &app,
        "POST",
        "/v1/organizations/acme/definitions/1/publish",
        "demo-leader",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // A node: it says hello, connects, and starts what it is told to.
    let mut greeting = hello("acme", "node-1");
    greeting["params"]["occurredAt"] = json!(Utc::now().to_rfc3339());
    call(&app, "POST", "/v1/protocol/messages", "", Some(greeting)).await;
    let (mut sink, mut stream) = dial(&address, "node-1").await.split();
    let started = Arc::new(Mutex::new(Vec::<Value>::new()));
    let seen = started.clone();
    tokio::spawn(async move {
        while let Some(Ok(tungstenite::Message::Text(text))) = stream.next().await {
            let request: Value = serde_json::from_str(&text).unwrap();
            seen.lock().unwrap().push(request.clone());
            let answer = json!({"jsonrpc": "2.0", "id": request["id"], "result": {"id": "node-run-1", "status": "running"}});
            if sink
                .send(tungstenite::Message::Text(answer.to_string().into()))
                .await
                .is_err()
            {
                return;
            }
        }
    });
    for _ in 0..200 {
        let (_, nodes) = call(&app, "GET", "/v1/nodes", "demo-observer", None).await;
        if nodes[0]["connected"] == true {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let start = json!({"missionId": "ship", "repository": "https://github.com/acme/app", "prompt": "Ship it"});
    let (status, _) = call(
        &app,
        "POST",
        "/v1/organizations/acme/runs",
        "demo-observer",
        Some(start.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "starting needs node.command");
    let (status, run) = call(
        &app,
        "POST",
        "/v1/organizations/acme/runs",
        "demo-leader",
        Some(start),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{run}");
    let run_id = run["runId"].as_str().unwrap().to_string();
    assert_eq!(run["steps"]["implement"]["status"], "waiting");
    assert_eq!(started.lock().unwrap()[0]["method"], "run.start");

    // The node reports its run done.
    let done = json!({"jsonrpc": "2.0", "method": "run.completed", "params": common::params("acme", "node-1", json!({
        "eventId": "node-1-1", "sequence": 1, "runId": "node-run-1", "state": "done",
    }))});
    let (status, _) = call(&app, "POST", "/v1/protocol/messages", "", Some(done)).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let (_, run) = call(
        &app,
        "GET",
        &format!("/v1/organizations/acme/runs/{run_id}"),
        "demo-observer",
        None,
    )
    .await;
    assert_eq!(
        (run["status"].as_str(), run["step"].as_str()),
        (Some("waiting"), Some("review")),
        "{run}"
    );
    let approval = run["steps"]["review"]["approvalId"]
        .as_str()
        .unwrap()
        .to_string();

    // A human-required gate: a service token cannot pass it, a person can.
    let decide = json!({"organizationId": "acme", "decision": "approved", "idempotencyKey": "k1"});
    let path = format!("/v1/approvals/{approval}/resolve");
    let (status, _) = call(&app, "POST", &path, &robot, Some(decide.clone())).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, body) = call(&app, "POST", &path, "demo-leader", Some(decide)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, run) = call(
        &app,
        "GET",
        &format!("/v1/organizations/acme/runs/{run_id}"),
        "demo-observer",
        None,
    )
    .await;
    assert_eq!(run["status"], "completed", "{run}");

    let (_, listed) = call(
        &app,
        "GET",
        "/v1/organizations/acme/runs",
        "demo-observer",
        None,
    )
    .await;
    assert_eq!(listed["runs"][0]["runId"], run_id.as_str());
    let (_, projection) = call(
        &app,
        "GET",
        "/v1/office/projection?organizationId=acme",
        "demo-observer",
        None,
    )
    .await;
    let shown = projection["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["id"] == run_id.as_str())
        .cloned()
        .unwrap();
    assert_eq!(shown["status"], "completed");
}

#[tokio::test]
async fn a_step_the_node_holds_waits_for_someone_there() {
    let w = world().await;
    let node = Scripted::online();
    *node.hold.lock().unwrap() = true;
    let run = w
        .engine
        .start("acme", input(), &ana(), &node)
        .await
        .unwrap();
    assert_eq!(run.status, "waiting");
    assert_eq!(run.steps["implement"].status, "held");
    assert!(run.error.contains("to confirm"), "{}", run.error);
    let command = run.steps["implement"].command_id.clone();
    assert_eq!(
        w.engine.tick(&node).await,
        0,
        "nothing to do until the node answers"
    );

    // Someone at the node accepted: the gateway's answer names its run.
    let run = w
        .engine
        .command_completed(
            "acme",
            &command,
            true,
            &json!({"localRunId": "local-7", "run": {"id": "local-7"}}),
            "",
            &node,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.status, "running");
    assert_eq!(run.steps["implement"].node_run_id, "local-7");
    let run = w
        .engine
        .node_event("acme", "local-7", "run.completed", "", &node)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.step, "review");

    // Rejected at the node: the step fails, and resuming asks again.
    let held = w
        .engine
        .start("acme", input(), &ana(), &node)
        .await
        .unwrap();
    let command = held.steps["implement"].command_id.clone();
    let run = w
        .engine
        .command_completed(
            "acme",
            &command,
            false,
            &Value::Null,
            "rejected at the node",
            &node,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.status, "failed");
    assert!(run.error.contains("rejected at the node"), "{}", run.error);
    assert!(
        w.engine
            .command_completed("acme", "someone-else", true, &Value::Null, "", &node)
            .await
            .unwrap()
            .is_none()
    );
}
