//! The audit trail: what operators do is recorded with who did it, and the
//! export is redacted, ordered and tamper-evident.

mod common;

use agentux_command::auth::{
    AUDIT_READ, AUTH_ADMIN, NODE_COMMAND, ORGANIZATION_EDIT, ORGANIZATION_READ, TokenRequest,
};
use agentux_command::server::{Handler, chain_hash};
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

use common::{call, hello, params};

async fn export(app: &Router, org: &str, token: &str) -> (StatusCode, String, Vec<Value>) {
    let response = app
        .clone()
        .oneshot(
            Request::get(format!("/v1/organizations/{org}/audit/export"))
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let kind = response
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    let text = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    let lines = text
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();
    (status, kind, lines)
}

/// Whether every line's hash follows from the previous one and the summary
/// carries the last.
fn verifies(lines: &[Value]) -> bool {
    let (records, summary) = lines.split_at(lines.len() - 1);
    let mut prev = String::new();
    for line in records {
        let mut record = line.clone();
        let hash = record.as_object_mut().unwrap().remove("hash").unwrap();
        if record["prev"] != prev.as_str() || hash != chain_hash(&prev, &record).as_str() {
            return false;
        }
        prev = hash.as_str().unwrap().to_string();
    }
    summary[0]["digest"] == prev.as_str() && summary[0]["count"] == records.len()
}

#[tokio::test]
async fn operators_actions_are_recorded_and_exported_tamper_evident() {
    let handler = Handler::new();
    let tokens = handler.service_tokens();
    let issue = |org: &str, caps: &[&str]| TokenRequest {
        name: "t".into(),
        organization_id: org.into(),
        capabilities: caps.iter().map(|c| c.to_string()).collect(),
        ..TokenRequest::default()
    };
    let admin = tokens
        .issue(
            issue(
                "acme",
                &[
                    AUTH_ADMIN,
                    ORGANIZATION_EDIT,
                    NODE_COMMAND,
                    ORGANIZATION_READ,
                ],
            ),
            Utc::now(),
        )
        .await
        .unwrap();
    let auditor = tokens
        .issue(issue("acme", &[AUDIT_READ]), Utc::now())
        .await
        .unwrap()
        .secret;
    let reader = tokens
        .issue(issue("acme", &[ORGANIZATION_READ]), Utc::now())
        .await
        .unwrap()
        .secret;
    let outsider = tokens
        .issue(issue("beta", &[AUDIT_READ]), Utc::now())
        .await
        .unwrap()
        .secret;
    let app = handler.router();
    let admin_id = format!("service-token:{}", admin.token.id);

    // Who may export.
    assert_eq!(export(&app, "acme", "").await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        export(&app, "acme", &reader).await.0,
        StatusCode::FORBIDDEN,
        "organization.read is not audit.read"
    );
    assert_eq!(
        export(&app, "acme", &outsider).await.0,
        StatusCode::FORBIDDEN,
        "another organization"
    );

    // Things an operator does.
    let (status, issued) = call(
        &app,
        "POST",
        "/v1/service-tokens",
        &admin.secret,
        Some(
            json!({"organizationId": "acme", "name": "ci", "capabilities": ["organization.read"]}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{issued}");
    let ci = issued["id"].as_str().unwrap().to_string();
    let (status, _) = call(
        &app,
        "POST",
        &format!("/v1/service-tokens/{ci}/revoke"),
        &admin.secret,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        &app,
        "POST",
        "/v1/organizations/acme/definitions",
        &admin.secret,
        Some(json!({"schema_version": "1.0"})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    call(
        &app,
        "POST",
        "/v1/protocol/messages",
        "",
        Some(hello("acme", "node-1")),
    )
    .await;
    let (status, _) = call(
        &app,
        "POST",
        "/v1/nodes/node-1/commands",
        &admin.secret,
        Some(json!({"method": "run.start", "params": {"prompt": "the secret plan", "repository": "r"}})),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "the node is not connected"
    );
    // A node's approval, with free text the export must not carry.
    let approval = json!({"jsonrpc": "2.0", "method": "approval.requested", "params": params("acme", "node-1", json!({
        "eventId": "e1", "sequence": 1, "runId": "r1", "requestId": "q1", "kind": "push", "detail": "password=hunter2",
    }))});
    call(&app, "POST", "/v1/protocol/messages", "", Some(approval)).await;

    let (status, kind, lines) = export(&app, "acme", &auditor).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(kind, "application/x-ndjson");
    assert!(verifies(&lines), "the chain holds");
    let records = &lines[..lines.len() - 1];
    let of = |kind: &str| {
        records
            .iter()
            .find(|l| l["event"]["eventType"] == kind)
            .unwrap_or_else(|| panic!("no {kind} in {records:#?}"))
            .clone()
    };
    for kind in [
        "auth.service_token.issued",
        "auth.service_token.revoked",
        "organization.definition.drafted",
        "node.command.sent",
    ] {
        assert_eq!(
            of(kind)["event"]["actor"]["id"],
            admin_id.as_str(),
            "{kind}"
        );
    }
    let command = of("node.command.sent");
    assert_eq!(command["event"]["data"]["outcome"], "unreachable");
    assert_eq!(command["event"]["data"]["method"], "run.start");
    let text = serde_json::to_string(&lines).unwrap();
    assert!(
        !text.contains("the secret plan"),
        "command params are not recorded"
    );
    assert!(!text.contains("hunter2"), "free text is redacted");
    assert_eq!(
        of("approval.requested")["event"]["data"]["detail"],
        "[redacted]"
    );
    let cursors: Vec<i64> = records
        .iter()
        .map(|l| l["cursor"].as_i64().unwrap())
        .collect();
    assert!(cursors.windows(2).all(|w| w[0] < w[1]), "in log order");

    // Changing any line breaks the chain.
    let mut tampered = lines.clone();
    tampered[1]["event"]["actor"]["id"] = json!("someone-else");
    assert!(!verifies(&tampered));
    let mut dropped = lines.clone();
    dropped.remove(1);
    assert!(!verifies(&dropped));

    // The export is itself on the record.
    let (_, _, again) = export(&app, "acme", &auditor).await;
    assert!(
        again
            .iter()
            .any(|l| l["event"]["eventType"] == "audit.exported")
    );
    let (_, _, theirs) = export(&app, "beta", &outsider).await;
    assert!(
        !serde_json::to_string(&theirs)
            .unwrap()
            .contains("service_token"),
        "nothing of another organization"
    );
}
