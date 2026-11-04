//! Typed execution activity ingestion and mission-workspace replay.

mod common;

use agentux_command::auth::{ORGANIZATION_READ, TokenRequest};
use agentux_command::office;
use agentux_command::server::Handler;
use axum::http::StatusCode;
use chrono::Utc;
use serde_json::{Value, json};
use std::sync::Arc;

use common::{call, params};

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

#[tokio::test]
async fn accepts_typed_activity_once_and_replays_it_by_run_cursor() {
    let store = Arc::new(office::Store::new());
    let handler = Handler::with_office(store.clone());
    let reader = reader(&handler, "acme").await;
    let app = handler.router();
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
    let handler = Handler::with_office(Arc::new(office::Store::new()));
    let acme = reader(&handler, "acme").await;
    let beta = reader(&handler, "beta").await;
    let app = handler.router();
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
}
