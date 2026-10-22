//! Operators see and act on their own organizations only, with the
//! capabilities their token holds; everything else fails closed.

mod common;

use agentux_command::auth::{
    APPROVAL_DECIDE, AUTH_ADMIN, NODE_COMMAND, NODE_ENROLL, ORGANIZATION_READ, TokenRequest,
};
use agentux_command::server::Handler;
use axum::Router;
use axum::http::StatusCode;
use chrono::Utc;
use serde_json::{Value, json};

use common::{call, hello};

/// A server with two organizations' nodes, and a token for each operator.
struct Fleet {
    app: Router,
    /// org-a: every capability.
    admin_a: String,
    /// org-b: read only.
    reader_b: String,
}

async fn fleet(demo_tokens: bool) -> Fleet {
    let handler = Handler::new().with_demo_tokens(demo_tokens);
    let tokens = handler.service_tokens();
    let issue = |org: &str, caps: &[&str]| TokenRequest {
        name: format!("{org} operator"),
        organization_id: org.into(),
        capabilities: caps.iter().map(|c| c.to_string()).collect(),
        ..TokenRequest::default()
    };
    let admin_a = tokens
        .issue(
            issue(
                "org-a",
                &[
                    ORGANIZATION_READ,
                    NODE_COMMAND,
                    NODE_ENROLL,
                    APPROVAL_DECIDE,
                    AUTH_ADMIN,
                ],
            ),
            Utc::now(),
        )
        .await
        .unwrap()
        .secret;
    let reader_b = tokens
        .issue(issue("org-b", &[ORGANIZATION_READ]), Utc::now())
        .await
        .unwrap()
        .secret;
    let app = handler.router();
    // The demo server accepts nodes nobody enrolled.
    for (org, node) in [("org-a", "node-a"), ("org-b", "node-b")] {
        let (status, _) = call(
            &app,
            "POST",
            "/v1/protocol/messages",
            "",
            Some(hello(org, node)),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
    }
    Fleet {
        app,
        admin_a,
        reader_b,
    }
}

fn node_ids(list: &Value) -> Vec<String> {
    list.as_array()
        .unwrap()
        .iter()
        .map(|n| n["nodeId"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn operators_only_see_their_organizations() {
    let f = fleet(true).await;
    let (status, list) = call(&f.app, "GET", "/v1/nodes", &f.admin_a, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(node_ids(&list), ["node-a"]);
    let (_, list) = call(&f.app, "GET", "/v1/nodes", &f.reader_b, None).await;
    assert_eq!(node_ids(&list), ["node-b"]);
    // The demo token still sees every organization on the demo server.
    let (_, list) = call(&f.app, "GET", "/v1/nodes", "demo-observer", None).await;
    assert_eq!(node_ids(&list), ["node-a", "node-b"]);

    let (_, issued) = call(
        &f.app,
        "POST",
        "/v1/enrollment-tokens",
        &f.admin_a,
        Some(json!({"organizationId": "org-a"})),
    )
    .await;
    let token = issued["tokens"][0]["token"].as_str().unwrap().to_string();
    let (status, _) = call(
        &f.app,
        "POST",
        "/v1/enroll",
        "",
        Some(json!({"token": token, "hostname": "box"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, enrollments) = call(&f.app, "GET", "/v1/enrollments", &f.reader_b, None).await;
    assert_eq!(
        enrollments["nodes"],
        json!([]),
        "org-b sees none of org-a's enrollments"
    );
    let (_, enrollments) = call(&f.app, "GET", "/v1/enrollments", &f.admin_a, None).await;
    assert_eq!(enrollments["nodes"][0]["organizationId"], "org-a");
}

#[tokio::test]
async fn acting_needs_the_capability_in_the_records_organization() {
    let f = fleet(true).await;
    let command = json!({"method": "harness.list", "params": {}});
    // Another organization's node does not exist for the caller...
    let (status, _) = call(
        &f.app,
        "POST",
        "/v1/nodes/node-b/commands",
        &f.admin_a,
        Some(command.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    // ...and seeing a node is not commanding it.
    let (status, _) = call(
        &f.app,
        "POST",
        "/v1/nodes/node-b/commands",
        &f.reader_b,
        Some(command.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // Its own node: allowed (503 only because no node is connected here).
    let (status, _) = call(
        &f.app,
        "POST",
        "/v1/nodes/node-a/commands",
        &f.admin_a,
        Some(command),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

    let (status, _) = call(
        &f.app,
        "POST",
        "/v1/enrollment-tokens",
        &f.admin_a,
        Some(json!({"organizationId": "org-b"})),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "no enrollment tokens for another organization"
    );
    let (status, _) = call(
        &f.app,
        "POST",
        "/v1/enrollment-tokens",
        &f.reader_b,
        Some(json!({"organizationId": "org-b"})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "reading is not enrolling");
    let (status, _) = call(&f.app, "POST", "/v1/nodes/node-b/revoke", &f.admin_a, None).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "another organization's node is unknown"
    );

    let (status, _) = call(
        &f.app,
        "GET",
        "/v1/office/projection?organizationId=org-demo",
        &f.admin_a,
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "another organization's Office"
    );
    let decision =
        json!({"organizationId": "org-demo", "decision": "approved", "idempotencyKey": "k"});
    let (status, _) = call(
        &f.app,
        "POST",
        "/v1/approvals/approval-01/resolve",
        &f.admin_a,
        Some(decision),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "another organization's approval"
    );
}

#[tokio::test]
async fn service_tokens_are_issued_listed_and_revoked_by_their_organizations_admins() {
    let f = fleet(true).await;
    let request = |org: &str| json!({"name": "ci", "organizationId": org, "capabilities": [ORGANIZATION_READ], "days": 30});
    let (status, _) = call(
        &f.app,
        "POST",
        "/v1/service-tokens",
        &f.admin_a,
        Some(request("org-b")),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = call(
        &f.app,
        "POST",
        "/v1/service-tokens",
        &f.reader_b,
        Some(request("org-b")),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "reading is not administering"
    );
    let (status, _) = call(
        &f.app,
        "POST",
        "/v1/service-tokens",
        "demo-leader",
        Some(request("org-a")),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "the demo leader does not administer"
    );

    let (status, issued) = call(
        &f.app,
        "POST",
        "/v1/service-tokens",
        &f.admin_a,
        Some(request("org-a")),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{issued}");
    let secret = issued["secret"].as_str().unwrap().to_string();
    let id = issued["id"].as_str().unwrap().to_string();
    assert!(issued["expiresAt"].is_string());
    let (status, _) = call(&f.app, "GET", "/v1/nodes", &secret, None).await;
    assert_eq!(status, StatusCode::OK, "the new token works");

    let (_, listed) = call(
        &f.app,
        "GET",
        "/v1/service-tokens?organizationId=org-a",
        &f.admin_a,
        None,
    )
    .await;
    let tokens = listed["tokens"].as_array().unwrap();
    assert_eq!(tokens.len(), 2);
    assert!(
        tokens.iter().all(|t| t.get("secret").is_none()),
        "secrets are never listed"
    );
    let (status, _) = call(
        &f.app,
        "GET",
        "/v1/service-tokens?organizationId=org-b",
        &f.admin_a,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (status, _) = call(
        &f.app,
        "POST",
        &format!("/v1/service-tokens/{id}/revoke"),
        &f.reader_b,
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "another organization's token is unknown"
    );
    let (status, _) = call(
        &f.app,
        "POST",
        &format!("/v1/service-tokens/{id}/revoke"),
        &f.admin_a,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(&f.app, "GET", "/v1/nodes", &secret, None).await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "a revoked token is refused"
    );
}

#[tokio::test]
async fn without_credentials_everything_fails_closed() {
    let f = fleet(false).await;
    for (method, path) in [
        ("GET", "/v1/nodes"),
        ("GET", "/v1/enrollments"),
        ("POST", "/v1/enrollment-tokens"),
        ("POST", "/v1/nodes/node-a/commands"),
        ("POST", "/v1/nodes/node-a/revoke"),
        ("GET", "/v1/office/projection?organizationId=org-a"),
        ("POST", "/v1/approvals/x/resolve"),
        ("GET", "/v1/service-tokens?organizationId=org-a"),
        ("POST", "/v1/service-tokens"),
    ] {
        for token in ["", "nonsense", "demo-leader", "auxs1_0000"] {
            let (status, _) = call(&f.app, method, path, token, Some(json!({}))).await;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "{method} {path} with {token:?}"
            );
        }
    }
    // Health and the instance stay public, with no tenant data.
    let (status, health) = call(&f.app, "GET", "/healthz", "", None).await;
    assert_eq!(
        (status, health),
        (StatusCode::OK, json!({"status": "ok", "database": "ok"}))
    );
    let (status, live) = call(&f.app, "GET", "/livez", "", None).await;
    assert_eq!((status, live), (StatusCode::OK, json!({"status": "alive"})));
    let (status, _) = call(&f.app, "GET", "/v1/instance", "", None).await;
    assert_eq!(status, StatusCode::OK);
}
