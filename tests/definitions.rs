//! Organization definitions through the API: drafted, validated, published
//! once and then frozen, per organization, with an audit log.

mod common;

use agentux_command::auth::{
    ORGANIZATION_EDIT, ORGANIZATION_PUBLISH, ORGANIZATION_READ, TokenRequest,
};
use agentux_command::server::Handler;
use axum::http::StatusCode;
use chrono::Utc;
use serde_json::{Value, json};

use common::call;

pub fn example() -> Value {
    json!({
        "schema_version": "1.0",
        "organization": { "id": "acme", "name": "Acme Robotics" },
        "areas": [{ "id": "engineering", "name": "Engineering", "owner_role_id": "cto" }],
        "roles": [
            { "id": "cto", "name": "CTO", "principal_kind": "human", "permissions": ["organization.read", "approval.decide"] },
            { "id": "reviewer", "name": "Reviewer", "principal_kind": "agent", "permissions": ["run.read"] }
        ],
        "missions": [{ "id": "ship-release", "area_id": "engineering", "name": "Ship a release", "owner_role_id": "cto" }],
        "workflows": [{ "id": "release-v1", "mission_id": "ship-release", "steps": [
            { "id": "implement", "kind": "agent", "role_id": "reviewer", "next": ["review"] },
            { "id": "review", "kind": "gate", "gate_id": "human-release", "next": [] }
        ]}],
        "gates": [{ "id": "human-release", "kind": "approval", "required_permissions": ["approval.decide"], "policy": "human-required" }],
        "agents": [{ "id": "reviewer-1", "role_id": "reviewer" }],
        "nodes": []
    })
}

#[tokio::test]
async fn a_definition_is_drafted_validated_published_and_then_frozen() {
    let handler = Handler::new();
    let tokens = handler.service_tokens();
    let issue = |org: &str, caps: &[&str]| TokenRequest {
        name: "t".into(),
        organization_id: org.into(),
        capabilities: caps.iter().map(|c| c.to_string()).collect(),
        ..TokenRequest::default()
    };
    let editor = tokens
        .issue(
            issue("acme", &[ORGANIZATION_READ, ORGANIZATION_EDIT]),
            Utc::now(),
        )
        .await
        .unwrap()
        .secret;
    let publisher = tokens
        .issue(
            issue("acme", &[ORGANIZATION_READ, ORGANIZATION_PUBLISH]),
            Utc::now(),
        )
        .await
        .unwrap()
        .secret;
    let outsider = tokens
        .issue(
            issue(
                "beta",
                &[ORGANIZATION_READ, ORGANIZATION_EDIT, ORGANIZATION_PUBLISH],
            ),
            Utc::now(),
        )
        .await
        .unwrap()
        .secret;
    let app = handler.router();
    let base = "/v1/organizations/acme/definitions";

    // A draft is kept with its problems.
    let (status, draft) = call(
        &app,
        "POST",
        base,
        &editor,
        Some(json!({"schema_version": "1.0"})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{draft}");
    assert_eq!(
        (
            draft["definition_version"].as_i64(),
            draft["status"].as_str()
        ),
        (Some(1), Some("draft"))
    );
    assert!(!draft["problems"].as_array().unwrap().is_empty());
    assert_eq!(draft["created_by"]["kind"], "service");

    // Only acme's operators, and only with the right capability.
    let (status, _) = call(&app, "GET", base, &outsider, None).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "another organization");
    let (status, _) = call(&app, "POST", base, &publisher, Some(example())).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "publishing is not editing");
    let (status, _) = call(&app, "POST", &format!("{base}/1/publish"), &editor, None).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "editing is not publishing");

    let (status, body) = call(&app, "POST", &format!("{base}/1/publish"), &publisher, None).await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "an invalid draft is not published"
    );
    assert!(!body["problems"].as_array().unwrap().is_empty());

    let (status, fixed) = call(&app, "PUT", &format!("{base}/1"), &editor, Some(example())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fixed["problems"], json!([]));
    let (status, published) =
        call(&app, "POST", &format!("{base}/1/publish"), &publisher, None).await;
    assert_eq!(status, StatusCode::OK, "{published}");
    assert_eq!(published["status"], "published");

    // Frozen: the next change is a new version.
    let (status, _) = call(
        &app,
        "PUT",
        &format!("{base}/1"),
        &editor,
        Some(json!({"schema_version": "1.0"})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (_, next) = call(&app, "POST", base, &editor, Some(example())).await;
    assert_eq!(next["definition_version"], 2);
    let (_, listed) = call(&app, "GET", base, &editor, None).await;
    let versions: Vec<(i64, String)> = listed["definitions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| {
            (
                d["definition_version"].as_i64().unwrap(),
                d["status"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(versions, [(2, "draft".into()), (1, "published".into())]);
    let (_, one) = call(&app, "GET", &format!("{base}/1"), &editor, None).await;
    assert_eq!(one["document"]["organization"]["name"], "Acme Robotics");

    // The Office shows the published organization.
    let (status, projection) = call(
        &app,
        "GET",
        "/v1/office/projection?organizationId=acme",
        &editor,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{projection}");
    assert_eq!(projection["organization"]["name"], "Acme Robotics");
    assert_eq!(projection["definitionVersion"], "definition-1");
    assert_eq!(projection["areas"][0]["id"], "engineering");

    // Every change, with its actor, newest first.
    let (_, audit) = call(&app, "GET", "/v1/organizations/acme/audit", &editor, None).await;
    let actions: Vec<&str> = audit["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["action"].as_str().unwrap())
        .collect();
    assert_eq!(
        actions,
        [
            "definition.drafted",
            "definition.published",
            "definition.replaced",
            "definition.drafted"
        ]
    );
    let (status, _) = call(&app, "GET", "/v1/organizations/acme/audit", &outsider, None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}
