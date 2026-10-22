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

/// Reads `body` until its text contains `want`, or fails after a few seconds.
async fn read_until(body: &mut axum::body::Body, seen: &mut String, want: &str) {
    use http_body_util::BodyExt;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(4);
    while !seen.contains(want) {
        let frame = tokio::time::timeout_at(deadline, body.frame())
            .await
            .unwrap_or_else(|_| panic!("no {want:?} in the stream: {seen}"))
            .expect("the stream goes on")
            .unwrap();
        if let Ok(data) = frame.into_data() {
            seen.push_str(&String::from_utf8_lossy(&data));
        }
    }
}

#[tokio::test]
async fn the_live_stream_resumes_from_a_cursor_and_follows_new_events() {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    let store = Arc::new(office::Store::new());
    for (id, sequence) in [("a1", 1), ("a2", 2)] {
        store
            .apply_event(event(id, "acme", sequence))
            .await
            .unwrap();
    }
    let handler = Handler::with_office(store);
    let issue = |org: &str| TokenRequest {
        name: "reader".into(),
        organization_id: org.into(),
        capabilities: vec![ORGANIZATION_READ.into()],
        ..TokenRequest::default()
    };
    let tokens = handler.service_tokens();
    let reader = tokens
        .issue(issue("acme"), Utc::now())
        .await
        .unwrap()
        .secret;
    let outsider = tokens
        .issue(issue("beta"), Utc::now())
        .await
        .unwrap()
        .secret;
    let app = handler.router();

    let (status, _) = call(
        &app,
        "GET",
        "/v1/organizations/acme/events/stream",
        &outsider,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // A reconnecting EventSource: everything after the cursor it had.
    let response = app
        .clone()
        .oneshot(
            Request::get("/v1/organizations/acme/events/stream")
                .header("authorization", format!("Bearer {reader}"))
                .header("last-event-id", "1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    let mut body = response.into_body();
    let mut seen = String::new();
    read_until(&mut body, &mut seen, "\"a2\"").await;
    assert!(!seen.contains("\"a1\""), "not what the client had: {seen}");
    assert!(seen.contains("id: 2"), "{seen}");
    assert!(seen.contains("event: office.event"), "{seen}");

    // A node's new event arrives without waiting for a poll.
    let message = serde_json::json!({"jsonrpc": "2.0", "method": "run.started", "params": common::params("acme", "node-01", serde_json::json!({
        "eventId": "a3", "sequence": 3, "runId": "r1",
    }))});
    let (status, _) = call(&app, "POST", "/v1/protocol/messages", "", Some(message)).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    read_until(&mut body, &mut seen, "\"a3\"").await;
}
