//! The Office projection, built from real node messages, against
//! agentux-protocol's schema (auxp::schemas), per organization.

mod common;

use agentux_command::auth::{ORGANIZATION_READ, TokenRequest};
use agentux_command::server::Handler;
use axum::http::StatusCode;
use chrono::Utc;
use serde_json::{Value, json};

use common::{call, hello, params};

fn conforms(projection: &Value) {
    let schema: Value = serde_json::from_str(auxp::schemas::OFFICE_PROJECTION).unwrap();
    let validator = jsonschema::validator_for(&schema).unwrap();
    let errors: Vec<String> = validator
        .iter_errors(projection)
        .map(|e| format!("{} at {}", e, e.instance_path))
        .collect();
    assert!(errors.is_empty(), "{errors:?}\n{projection:#}");
}

fn event(org: &str, node: &str, method: &str, sequence: i64, extra: Value) -> Value {
    json!({"jsonrpc": "2.0", "method": method, "params": params(org, node, common::merge(
        json!({"eventId": format!("{node}-{sequence}"), "sequence": sequence}),
        extra,
    ))})
}

async fn reader(handler: &Handler, org: &str) -> String {
    handler
        .service_tokens()
        .issue(
            TokenRequest {
                name: "reader".into(),
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
async fn the_projection_from_node_messages_follows_the_schema_per_organization() {
    let handler = Handler::new();
    let (acme, beta) = (
        reader(&handler, "acme").await,
        reader(&handler, "beta").await,
    );
    let app = handler.router();
    let send = |message: Value| {
        let app = app.clone();
        async move {
            let (status, body) = call(
                &app,
                "POST",
                "/v1/protocol/messages",
                "",
                Some(now(message)),
            )
            .await;
            assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        }
    };

    // The demo projection as it is.
    let (_, demo) = call(
        &app,
        "GET",
        "/v1/office/projection?organizationId=org-demo",
        "demo-leader",
        None,
    )
    .await;
    conforms(&demo);

    send(hello("acme", "node-a")).await;
    send(hello("beta", "node-b")).await;
    let messages = [
        event(
            "acme",
            "node-a",
            "run.started",
            1,
            json!({"runId": "r1", "title": "Fix the build", "state": "running"}),
        ),
        event(
            "acme",
            "node-a",
            "approval.requested",
            2,
            json!({"runId": "r1", "requestId": "q1", "kind": "push", "title": "Push to main?"}),
        ),
        event(
            "acme",
            "node-a",
            "artifact.available",
            3,
            json!({"runId": "r1", "artifactId": "a1", "name": "report.md"}),
        ),
        event(
            "acme",
            "node-a",
            "harness.status",
            4,
            json!({"harnesses": []}),
        ),
        event(
            "beta",
            "node-b",
            "run.started",
            1,
            json!({"runId": "secret-run", "state": "running"}),
        ),
    ];
    for message in messages.clone() {
        send(message).await;
    }
    // A node resending what it sent (a reconnect) changes nothing.
    send(messages[0].clone()).await;

    let (status, projection) = call(
        &app,
        "GET",
        "/v1/office/projection?organizationId=acme",
        &acme,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{projection}");
    conforms(&projection);
    assert_eq!(projection["runs"].as_array().unwrap().len(), 1);
    assert_eq!(projection["runs"][0]["id"], "r1");
    assert_eq!(projection["approvals"][0]["id"], "q1");
    assert_eq!(projection["approvals"][0]["status"], "pending");
    assert_eq!(projection["artifacts"][0]["id"], "a1");
    assert_eq!(
        projection["events"].as_array().unwrap().len(),
        3,
        "runs, approvals and artifacts; not harness status; once each"
    );
    assert_eq!(projection["nodes"][0]["id"], "node-a");
    assert_eq!(projection["nodes"][0]["presence"], "online");
    assert_eq!(projection["freshness"], "fresh", "its one node reports");
    assert!(
        !projection.to_string().contains("secret-run"),
        "nothing of another organization"
    );

    let (status, _) = call(
        &app,
        "GET",
        "/v1/office/projection?organizationId=acme",
        &beta,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (_, theirs) = call(
        &app,
        "GET",
        "/v1/office/projection?organizationId=beta",
        &beta,
        None,
    )
    .await;
    conforms(&theirs);
    assert_eq!(theirs["runs"][0]["id"], "secret-run");
}

/// `message`, as sent now: the node was just heard from.
fn now(mut message: Value) -> Value {
    message["params"]["occurredAt"] = json!(Utc::now().to_rfc3339());
    message
}
