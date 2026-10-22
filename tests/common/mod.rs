//! Helpers shared by the integration tests.
#![allow(dead_code)]

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Map, Value, json};
use tower::ServiceExt;

/// Sends one request to `app`; the status and the JSON body (Null when
/// there is none).
pub async fn call(
    app: &Router,
    method: &str,
    path: &str,
    token: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut request = Request::builder().method(method).uri(path);
    if !token.is_empty() {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let body = match body {
        Some(body) => Body::from(body.to_string()),
        None => Body::empty(),
    };
    let response = app
        .clone()
        .oneshot(
            request
                .header("content-type", "application/json")
                .body(body)
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// Sends a raw body.
pub async fn call_raw(
    app: &Router,
    method: &str,
    path: &str,
    token: &str,
    body: &str,
) -> (StatusCode, Value) {
    let mut request = Request::builder().method(method).uri(path);
    if !token.is_empty() {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

pub fn merge(a: Value, b: Value) -> Value {
    let mut out: Map<String, Value> = a.as_object().cloned().unwrap_or_default();
    for (k, v) in b.as_object().cloned().unwrap_or_default() {
        out.insert(k, v);
    }
    Value::Object(out)
}

/// A node message's common params for `node` in `org`.
pub fn params(org: &str, node: &str, extra: Value) -> Value {
    merge(
        json!({"protocolVersion": "v1", "organizationId": org, "nodeId": node, "messageId": "m", "occurredAt": "2026-10-21T12:00:00Z"}),
        extra,
    )
}

pub fn hello(org: &str, node: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": "h", "method": "node.hello", "params": params(org, node, json!({
        "capabilities": ["harness.list"], "agentuxVersion": "0.4.9", "supportedProtocolVersions": ["v1"],
    }))})
}

/// The test database, when AGENTUX_COMMAND_TEST_DATABASE_URL names one.
pub fn database_url() -> Option<String> {
    std::env::var("AGENTUX_COMMAND_TEST_DATABASE_URL")
        .ok()
        .filter(|u| !u.is_empty())
}

/// A fresh schema in the test database, dropped at the end of the test
/// process at the latest by the next run's cleanup; its connection config.
pub async fn schema(prefix: &str) -> Option<(tokio_postgres::Config, String)> {
    let url = database_url()?;
    let config: tokio_postgres::Config = url.parse().unwrap();
    let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
    tokio::spawn(connection);
    let mut random = [0u8; 8];
    getrandom::fill(&mut random).unwrap();
    let name = format!("{prefix}_{}", hex::encode(random));
    client
        .batch_execute(&format!("CREATE SCHEMA {name}"))
        .await
        .unwrap();
    let mut scoped = config.clone();
    scoped.options(format!("-c search_path={name}"));
    Some((scoped, name))
}
