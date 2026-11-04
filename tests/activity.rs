//! Typed execution activity ingestion and mission-workspace replay.

mod common;

use agentux_command::auth::{ORGANIZATION_READ, TokenRequest};
use agentux_command::missions::Store as _;
use agentux_command::server::Handler;
use agentux_command::{definitions::Actor, missions, office};
use axum::http::StatusCode;
use chrono::Utc;
use futures_util::SinkExt;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use tokio_tungstenite::tungstenite::{Message as WebSocketMessage, client::IntoClientRequest};

use common::{call, hello, params};

async fn seed_run(store: &missions::Memory, run_id: &str, node_id: &str) {
    let mission_id = "mission-1".to_string();
    store
        .insert(&missions::Run {
            run_id: run_id.into(),
            organization_id: "acme".into(),
            definition_version: 1,
            mission_id: mission_id.clone(),
            workflow_id: "workflow-1".into(),
            status: missions::RUNNING.into(),
            step: "agent".into(),
            input: missions::Input {
                mission_id,
                ..missions::Input::default()
            },
            schedule_id: None,
            scheduled_for: None,
            steps: BTreeMap::from([(
                "agent".into(),
                missions::Step {
                    kind: "agent".into(),
                    node_id: node_id.into(),
                    ..missions::Step::default()
                },
            )]),
            error: String::new(),
            run_at: None,
            created_by: Actor::of("operator:test"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            revision: 1,
        })
        .await
        .unwrap();
}

fn activity_message(org: &str, node: &str, event_id: &str, sequence: i64, run_id: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "execution.activity",
        "params": params(org, node, json!({
            "eventId": event_id,
            "sequence": sequence,
            "activity": {
                "schemaVersion": "v1",
                "activityId": format!("activity-{event_id}"),
                "missionId": "mission-1",
                "runId": run_id,
                "agentId": "agent-worker",
                "roleId": "worker",
                "taskId": "task-1",
                "parentTaskId": "task-root",
                "attempt": 2,
                "causationId": "activity-parent",
                "delegationId": "delegation-1",
                "toolCallId": "tool-call-1",
                "type": "tool_completed",
                "toolName": "search",
                "summary": "Found the requested documentation.",
            }
        }))
    })
}

async fn reader(handler: &Handler, org: &str) -> String {
    handler
        .service_tokens()
        .issue(
            TokenRequest {
                name: "activity-reader".into(),
                organization_id: org.into(),
                capabilities: vec![ORGANIZATION_READ.into()],
                ..TokenRequest::default()
            },
            Utc::now(),
        )
        .await
        .unwrap()
        .secret
}

async fn negotiate_activity(app: &axum::Router, org: &str, node: &str) {
    let mut hello = hello(org, node);
    hello["params"]["capabilities"] = json!(["harness.list", "execution.activity.v1"]);
    let (status, result) = call(app, "POST", "/v1/protocol/messages", "", Some(hello)).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{result}");
    assert_eq!(
        result["result"]["acceptedCapabilities"],
        json!(["execution.activity.v1"])
    );
}

#[tokio::test]
async fn accepts_typed_activity_once_and_replays_it_by_run_cursor() {
    let store = Arc::new(office::Store::new());
    let runs = Arc::new(missions::Memory::default());
    seed_run(&runs, "run-1", "node-1").await;
    seed_run(&runs, "run-2", "node-1").await;
    let handler = Handler::with_office(store.clone()).with_runs(runs);
    let reader = reader(&handler, "acme").await;
    let app = handler.router();
    negotiate_activity(&app, "acme", "node-1").await;
    let message = activity_message("acme", "node-1", "event-1", 7, "run-1");

    for _ in 0..2 {
        let (status, body) = call(
            &app,
            "POST",
            "/v1/protocol/messages",
            "",
            Some(message.clone()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    }
    let (status, _) = call(
        &app,
        "POST",
        "/v1/protocol/messages",
        "",
        Some(activity_message("acme", "node-1", "event-2", 8, "run-2")),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    let (status, page) = call(
        &app,
        "GET",
        "/v1/organizations/acme/runs/run-1/activity?limit=1",
        &reader,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page["activities"].as_array().unwrap().len(), 1);
    let item = &page["activities"][0];
    assert_eq!(item["eventId"], "event-1");
    assert_eq!(item["sequence"], 7, "per-node gaps remain visible as-is");
    assert_eq!(item["activity"]["missionId"], "mission-1");
    assert_eq!(item["activity"]["parentTaskId"], "task-root");
    assert_eq!(item["activity"]["attempt"], 2);
    assert_eq!(item["activity"]["causationId"], "activity-parent");
    assert_eq!(item["activity"]["delegationId"], "delegation-1");
    assert_eq!(item["activity"]["toolCallId"], "tool-call-1");
    assert_eq!(item["activity"]["type"], "tool_completed");
    assert_eq!(page["next"], item["cursor"]);
    assert_eq!(page["nodePresence"], "offline");
    assert_eq!(page["freshness"], "stale");

    let (status, done) = call(
        &app,
        "GET",
        &format!(
            "/v1/organizations/acme/runs/run-1/activity?after={}",
            page["next"]
        ),
        &reader,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{done}");
    assert!(done["activities"].as_array().unwrap().is_empty());
    assert_eq!(done["next"], page["next"]);
}

#[tokio::test]
async fn activity_replay_is_tenant_scoped_and_schema_invalid_payloads_are_refused() {
    let runs = Arc::new(missions::Memory::default());
    seed_run(&runs, "run-shared", "node-1").await;
    let handler = Handler::with_office(Arc::new(office::Store::new())).with_runs(runs);
    let acme = reader(&handler, "acme").await;
    let beta = reader(&handler, "beta").await;
    let app = handler.router();
    negotiate_activity(&app, "acme", "node-1").await;
    let event = activity_message("acme", "node-1", "event-1", 1, "run-shared");
    let (status, _) = call(&app, "POST", "/v1/protocol/messages", "", Some(event)).await;
    assert_eq!(status, StatusCode::ACCEPTED);

    let (status, _) = call(
        &app,
        "GET",
        "/v1/organizations/beta/runs/run-shared/activity",
        &acme,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, beta_page) = call(
        &app,
        "GET",
        "/v1/organizations/beta/runs/run-shared/activity",
        &beta,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{beta_page}");
    assert!(beta_page["activities"].as_array().unwrap().is_empty());

    let mut invalid = activity_message("acme", "node-1", "event-2", 2, "run-shared");
    invalid["params"]["activity"]["privateReasoning"] = json!("do not persist");
    let (status, _) = call(&app, "POST", "/v1/protocol/messages", "", Some(invalid)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let mut unknown = activity_message("acme", "node-1", "event-3", 2, "run-shared");
    unknown["params"]["activity"]["unrecognizedPrivatePayload"] = json!("do not persist");
    let (status, _) = call(&app, "POST", "/v1/protocol/messages", "", Some(unknown)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn execution_activity_rejects_unknown_runs_mission_mismatch_and_wrong_node() {
    let runs = Arc::new(missions::Memory::default());
    seed_run(&runs, "run-1", "node-1").await;
    let handler = Handler::with_office(Arc::new(office::Store::new())).with_runs(runs);
    let app = handler.router();
    negotiate_activity(&app, "acme", "node-1").await;
    negotiate_activity(&app, "acme", "node-2").await;

    for (node, event, run, mission) in [
        ("node-1", "unknown", "missing-run", "mission-1"),
        ("node-1", "wrong-mission", "run-1", "another-mission"),
        ("node-2", "wrong-node", "run-1", "mission-1"),
    ] {
        let mut message = activity_message("acme", node, event, 1, run);
        message["params"]["activity"]["missionId"] = json!(mission);
        let (status, _) = call(&app, "POST", "/v1/protocol/messages", "", Some(message)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{event}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn websocket_execution_activity_is_authorized_and_projected_for_replay() {
    let runs = Arc::new(missions::Memory::default());
    seed_run(&runs, "run-1", "node-1").await;
    let handler = Handler::with_office(Arc::new(office::Store::new())).with_runs(runs);
    let reader = reader(&handler, "acme").await;
    let app = handler.router();
    negotiate_activity(&app, "acme", "node-1").await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(axum::serve(listener, app.clone()).into_future());
    let request = format!("ws://{address}/v1/protocol/connect?nodeId=node-1")
        .into_client_request()
        .unwrap();
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    let mut forged = activity_message("acme", "node-1", "ws-forged", 3, "run-1");
    forged["params"]["activity"]["missionId"] = json!("another-mission");
    socket
        .send(WebSocketMessage::Text(forged.to_string().into()))
        .await
        .unwrap();
    socket
        .send(WebSocketMessage::Text(
            activity_message("acme", "node-1", "ws-event-1", 4, "run-1")
                .to_string()
                .into(),
        ))
        .await
        .unwrap();

    let mut page = Value::Null;
    for _ in 0..50 {
        let (status, response) = call(
            &app,
            "GET",
            "/v1/organizations/acme/runs/run-1/activity",
            &reader,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{response}");
        page = response;
        if !page["activities"].as_array().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(page["activities"][0]["eventId"], "ws-event-1");
    assert_eq!(page["activities"][0]["sequence"], 4);
    assert_eq!(page["activities"].as_array().unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn websocket_hello_cannot_change_node_organization_or_negotiate_capabilities() {
    let runs = Arc::new(missions::Memory::default());
    seed_run(&runs, "run-1", "node-1").await;
    let handler = Handler::with_office(Arc::new(office::Store::new())).with_runs(runs);
    let reader = reader(&handler, "acme").await;
    let app = handler.router();
    let (status, _) = call(
        &app,
        "POST",
        "/v1/protocol/messages",
        "",
        Some(hello("acme", "node-1")),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(axum::serve(listener, app.clone()).into_future());
    let request = format!("ws://{address}/v1/protocol/connect?nodeId=node-1")
        .into_client_request()
        .unwrap();
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();

    let mut spoofed_hello = hello("beta", "node-1");
    spoofed_hello["params"]["capabilities"] = json!(["execution.activity.v1"]);
    socket
        .send(WebSocketMessage::Text(spoofed_hello.to_string().into()))
        .await
        .unwrap();
    socket
        .send(WebSocketMessage::Text(
            activity_message("acme", "node-1", "ws-unnegotiated", 1, "run-1")
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    socket
        .send(WebSocketMessage::Text(
            activity_message("beta", "node-1", "ws-foreign-org", 1, "run-1")
                .to_string()
                .into(),
        ))
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let (status, page) = call(
        &app,
        "GET",
        "/v1/organizations/acme/runs/run-1/activity",
        &reader,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert!(page["activities"].as_array().unwrap().is_empty());
    let (status, nodes) = call(&app, "GET", "/v1/nodes", "demo-observer", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(nodes[0]["organizationId"], "acme");
}

#[tokio::test]
async fn execution_activity_is_refused_until_the_node_negotiates_its_capability() {
    let handler = Handler::with_office(Arc::new(office::Store::new()));
    let reader = reader(&handler, "acme").await;
    let app = handler.router();
    let (status, result) = call(
        &app,
        "POST",
        "/v1/protocol/messages",
        "",
        Some(hello("acme", "node-1")),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{result}");
    assert_eq!(result["result"]["acceptedCapabilities"], json!([]));

    let (status, _) = call(
        &app,
        "POST",
        "/v1/protocol/messages",
        "",
        Some(activity_message("acme", "node-1", "event-1", 1, "run-1")),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, page) = call(
        &app,
        "GET",
        "/v1/organizations/acme/runs/run-1/activity",
        &reader,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert!(page["activities"].as_array().unwrap().is_empty());
}
