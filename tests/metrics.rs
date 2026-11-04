use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use agentux_command::definitions::Actor;
use agentux_command::metrics::Metrics;
use agentux_command::missions::{self, Input, Run, Store};
use agentux_command::server::Handler;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

mod common;

#[test]
fn http_metrics_use_bounded_labels_for_dynamic_inputs() {
    let metrics = Metrics::default();
    metrics.record_request(
        "GET",
        "/v1/organizations/customer-secret/runs/run-private",
        500,
        Duration::from_millis(150),
    );
    metrics.record_request(
        "X-CUSTOM-METHOD",
        "/v1/organizations/another-tenant/audit",
        403,
        Duration::from_millis(25),
    );

    let output = metrics.render_http();
    assert!(output.contains(
        "command_http_requests_total{method=\"GET\",route=\"runs_api\",status_class=\"5xx\"} 1"
    ));
    assert!(output.contains(
        "command_http_errors_total{method=\"GET\",route=\"runs_api\",status_class=\"5xx\"} 1"
    ));
    assert!(output.contains("method=\"OTHER\",route=\"office_api\",status_class=\"4xx\""));
    assert!(output.contains("command_http_request_duration_seconds_bucket"));
    assert!(!output.contains("customer-secret"));
    assert!(!output.contains("run-private"));
    assert!(!output.contains("another-tenant"));
    assert!(!output.contains("X-CUSTOM-METHOD"));
}

#[test]
fn http_latency_histogram_uses_seconds_and_fixed_buckets() {
    let metrics = Metrics::default();
    metrics.record_request("POST", "/healthz", 204, Duration::from_millis(20));

    let output = metrics.render_http();
    assert!(output.contains("le=\"0.025\"} 1"));
    assert!(output.contains(
        "command_http_request_duration_seconds_sum{method=\"POST\",route=\"health\"} 0.02"
    ));
    assert!(output.contains(
        "command_http_request_duration_seconds_count{method=\"POST\",route=\"health\"} 1"
    ));
}

#[tokio::test]
async fn metrics_endpoint_is_disabled_and_requires_its_own_bearer_token() {
    let disabled = Handler::new().router();
    let (status, _) = common::call(&disabled, "GET", "/metrics", "", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let token = "0123456789abcdef0123456789abcdef";
    let protected = Handler::new().with_metrics_token(token).unwrap().router();
    let (status, _) = common::call(&protected, "GET", "/metrics", "wrong", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(Handler::new().with_metrics_token("too-short").is_err());
}

#[tokio::test]
async fn authorized_metrics_scrape_exposes_process_and_service_metrics_without_secrets() {
    let runs = Arc::new(missions::Memory::default());
    let now = chrono::Utc::now();
    for (id, state) in [
        ("private-running-run", missions::RUNNING),
        ("private-waiting-run", missions::WAITING),
    ] {
        runs.insert(&Run {
            run_id: id.into(),
            organization_id: "private-organization".into(),
            definition_version: 1,
            mission_id: "private-mission".into(),
            workflow_id: "workflow".into(),
            status: state.into(),
            step: String::new(),
            input: Input::default(),
            steps: BTreeMap::new(),
            error: String::new(),
            run_at: None,
            created_by: Actor::of("service-token:private-actor"),
            created_at: now,
            updated_at: now - chrono::Duration::minutes(20),
            revision: 1,
        })
        .await
        .unwrap();
    }
    let app = Handler::new()
        .with_metrics_token("0123456789abcdef0123456789abcdef")
        .unwrap()
        .with_runs(runs)
        .router();
    let (status, _) = common::call(&app, "GET", "/healthz", "", None).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = common::call(
        &app,
        "POST",
        "/v1/protocol/messages",
        "",
        Some(common::hello(
            "private-node-organization",
            "private-node-id",
        )),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .header("authorization", "Bearer 0123456789abcdef0123456789abcdef")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/plain; version=0.0.4")
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let output = String::from_utf8(body.to_vec()).unwrap();
    assert!(output.contains("command_database_configured 0"));
    assert!(output.contains("command_database_ready 0"));
    assert!(output.contains("command_event_sink_configured 0"));
    assert!(output.contains("command_event_retention_enabled 0"));
    assert!(output.contains("command_mission_runs{state=\"running\"} 1"));
    assert!(output.contains("command_mission_runs{state=\"waiting\"} 1"));
    let wait_age = output
        .lines()
        .find_map(|line| line.strip_prefix("command_mission_waiting_oldest_age_seconds "))
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert!(wait_age >= 1200);
    // The shared fixture hello timestamp is 2026-10-21, so this node is
    // correctly offline when the metrics test runs on 2026-11-04.
    assert!(output.contains("command_nodes{presence=\"offline\"} 1"));
    assert!(output.contains("command_node_connections{state=\"disconnected\"} 1"));
    assert!(output.contains(
        "command_http_requests_total{method=\"GET\",route=\"health\",status_class=\"2xx\"} 1"
    ));
    assert!(!output.contains("0123456789abcdef0123456789abcdef"));
    assert!(!output.contains("organizationId"));
    assert!(!output.contains("nodeId"));
    assert!(!output.contains("private-organization"));
    assert!(!output.contains("private-running-run"));
    assert!(!output.contains("private-waiting-run"));
    assert!(!output.contains("private-node-id"));
    assert!(!output.contains("private-actor"));
}
