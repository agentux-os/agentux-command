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

/// A PUT with `If-Match: "<revision>"`; the status, the body and the ETag.
async fn put_if(
    app: &axum::Router,
    path: &str,
    token: &str,
    revision: i64,
    body: Value,
) -> (StatusCode, Value, Option<String>) {
    use axum::body::Body;
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::put(path)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .header("if-match", format!("\"{revision}\""))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let etag = response
        .headers()
        .get("etag")
        .map(|v| v.to_str().unwrap().to_string());
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        etag,
    )
}

#[tokio::test]
async fn drafts_are_reviewed_by_someone_else_and_stale_edits_are_refused() {
    use agentux_command::auth::APPROVAL_DECIDE;
    let handler = Handler::new();
    let tokens = handler.service_tokens();
    let caps = [
        ORGANIZATION_READ,
        ORGANIZATION_EDIT,
        ORGANIZATION_PUBLISH,
        APPROVAL_DECIDE,
    ];
    let person = |name: &str| TokenRequest {
        name: name.into(),
        organization_id: "acme".into(),
        capabilities: caps.iter().map(|c| c.to_string()).collect(),
        ..TokenRequest::default()
    };
    let ana = tokens
        .issue(person("ana"), Utc::now())
        .await
        .unwrap()
        .secret;
    let bob = tokens
        .issue(person("bob"), Utc::now())
        .await
        .unwrap()
        .secret;
    let app = handler.router();
    let base = "/v1/organizations/acme/definitions";
    // This organization publishes only reviewed definitions.
    let mut reviewed = example();
    reviewed["organization"]["publication_policy"] = json!("review-required");

    let (status, draft) = call(&app, "POST", base, &ana, Some(reviewed.clone())).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(draft["revision"], 1);

    // Two editors from revision 1: the second is told, not overwritten.
    let mut first = reviewed.clone();
    first["organization"]["name"] = json!("Acme One");
    let (status, saved, etag) = put_if(&app, &format!("{base}/1"), &ana, 1, first).await;
    assert_eq!(status, StatusCode::OK, "{saved}");
    assert_eq!(saved["revision"], 2);
    assert_eq!(etag.as_deref(), Some("\"2\""));
    let (status, stale, _) = put_if(&app, &format!("{base}/1"), &bob, 1, reviewed.clone()).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(stale["error"], "stale_draft");
    assert_eq!(stale["currentRevision"], 2);

    // Not without review; never by its own editor.
    let (status, body) = call(&app, "POST", &format!("{base}/1/publish"), &ana, None).await;
    assert_eq!(
        (status, body["error"].as_str()),
        (StatusCode::CONFLICT, Some("review_required"))
    );
    let approve = json!({"decision": "approved", "comment": "looks right"});
    let (status, _) = call(
        &app,
        "POST",
        &format!("{base}/1/reviews"),
        &ana,
        Some(approve.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "ana edited revision 2");
    let (status, body) = call(
        &app,
        "POST",
        &format!("{base}/1/reviews"),
        &bob,
        Some(json!({"decision": "maybe"})),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    let (status, body) = call(
        &app,
        "POST",
        &format!("{base}/1/reviews"),
        &bob,
        Some(approve.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["reviews"][0]["revision"], 2);

    // A change after the approval needs a new one.
    let mut second = reviewed.clone();
    second["organization"]["name"] = json!("Acme Two");
    let (status, _, _) = put_if(&app, &format!("{base}/1"), &ana, 2, second).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(&app, "POST", &format!("{base}/1/publish"), &ana, None).await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "the approval was of revision 2"
    );
    let (status, _) = call(
        &app,
        "POST",
        &format!("{base}/1/reviews"),
        &bob,
        Some(approve),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, published) = call(&app, "POST", &format!("{base}/1/publish"), &ana, None).await;
    assert_eq!(status, StatusCode::OK, "{published}");
    // Publishing twice (two leaders at once): the second is told.
    let (status, _) = call(&app, "POST", &format!("{base}/1/publish"), &bob, None).await;
    assert_eq!(status, StatusCode::CONFLICT);

    // A rollback, or the next change: a new draft from a version.
    let (status, next) = call(&app, "POST", &format!("{base}?from=1"), &ana, None).await;
    assert_eq!(status, StatusCode::CREATED, "{next}");
    assert_eq!(next["definition_version"], 2);
    assert_eq!(next["document"]["organization"]["name"], "Acme Two");
    let (status, _) = call(&app, "POST", &format!("{base}?from=9"), &ana, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (_, audit) = call(&app, "GET", "/v1/organizations/acme/audit", &ana, None).await;
    let reviews = audit["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["action"] == "definition.reviewed")
        .count();
    assert_eq!(reviews, 2);
}
