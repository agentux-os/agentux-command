//! Replaying an organization's event log through the API.

mod common;

use std::sync::Arc;

use agentux_command::auth::{ORGANIZATION_READ, TokenRequest};
use agentux_command::office::{self, Event, OfficeRepository};
use agentux_command::server::Handler;
use axum::http::StatusCode;
use chrono::Utc;
use serde_json::Value;

use common::call;

fn event(id: &str, org: &str, sequence: i64) -> Event {
    Event {
        event_id: id.into(),
        event_type: "run.started".into(),
        organization_id: org.into(),
        node_id: "node-01".into(),
        sequence,
        ..Event::default()
    }
}

fn ids(body: &Value) -> Vec<String> {
    body["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["event"]["eventId"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn the_event_log_replays_per_organization_from_a_cursor() {
    let store = Arc::new(office::Store::new());
    for (id, org, sequence) in [
        ("a1", "acme", 1),
        ("b1", "beta", 1),
        ("a2", "acme", 2),
        ("a3", "acme", 3),
    ] {
        store.apply_event(event(id, org, sequence)).await.unwrap();
    }
    let handler = Handler::with_office(store);
    let reader = handler
        .service_tokens()
        .issue(
            TokenRequest {
                name: "reader".into(),
                organization_id: "acme".into(),
                capabilities: vec![ORGANIZATION_READ.into()],
                ..TokenRequest::default()
            },
            Utc::now(),
        )
        .await
        .unwrap()
        .secret;
    let app = handler.router();

    let (status, page) = call(
        &app,
        "GET",
        "/v1/organizations/acme/events?limit=2",
        &reader,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(ids(&page), ["a1", "a2"]);
    assert_eq!(page["events"][0]["event"]["actor"]["kind"], "node");
    let next = page["next"].as_i64().unwrap();
    let (_, rest) = call(
        &app,
        "GET",
        &format!("/v1/organizations/acme/events?after={next}"),
        &reader,
        None,
    )
    .await;
    assert_eq!(ids(&rest), ["a3"]);
    let (_, done) = call(
        &app,
        "GET",
        &format!("/v1/organizations/acme/events?after={}", rest["next"]),
        &reader,
        None,
    )
    .await;
    assert!(ids(&done).is_empty());
    assert_eq!(done["next"], rest["next"], "the cursor stays where it was");

    let (_, later) = call(
        &app,
        "GET",
        "/v1/organizations/acme/events?since=2999-01-01T00:00:00Z",
        &reader,
        None,
    )
    .await;
    assert!(ids(&later).is_empty());
    let (status, _) = call(
        &app,
        "GET",
        "/v1/organizations/acme/events?since=yesterday",
        &reader,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = call(&app, "GET", "/v1/organizations/beta/events", &reader, None).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "another organization's log");
    let (status, _) = call(&app, "GET", "/v1/organizations/acme/events", "", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}
