//! Command against the real node side: agentux-core's gateway enrolls with
//! a token Command issued, reports the node, keeps the command connection
//! and answers commands from an agentuxd with scripted agents. Nothing on
//! either side is a stand-in, so a protocol change one side makes and the
//! other does not fails here.
#![cfg(unix)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use agentux_command::server::Handler;
use agentux_gateway::channel::CommandOptions;
use agentux_gateway::policy::Policy;
use agentux_gateway::{Http, NodeConfig};
use agentux_store::Store;
use agentuxd::{Engine, FakeExecutor};
use axum::Router;
use axum::http::StatusCode;
use serde_json::{Value, json};

use common::call;

/// Polls the node list until `check` holds for the node.
async fn wait_for_node(
    app: &Router,
    node: &str,
    what: &str,
    check: impl Fn(&Value) -> bool,
) -> Value {
    for _ in 0..600 {
        let (_, list) = call(app, "GET", "/v1/nodes", "demo-observer", None).await;
        if let Some(found) = list
            .as_array()
            .and_then(|l| l.iter().find(|n| n["nodeId"] == node))
            && check(found)
        {
            return found.clone();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_real_node_enrolls_reports_and_takes_commands() {
    // Command, on a real port: the gateway speaks HTTP and WebSocket to it.
    let handler = Handler::new().with_open_nodes(false);
    let instance = handler.instance_id().to_string();
    let app = handler.router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(axum::serve(listener, app.clone()).into_future());

    // The node's agentuxd, with scripted agents.
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("agentuxd.sock");
    let engine = Engine::new(
        Store::open(&dir.path().join("agentuxd.db")).unwrap(),
        Arc::new(FakeExecutor::default()),
    );
    let (stop_daemon, daemon_stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn({
        let socket = socket.clone();
        async move {
            agentuxd::server::serve(engine, &socket, async {
                let _ = daemon_stopped.await;
            })
            .await
        }
    });
    while !socket.exists() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // A token from another Command never leaves the node: the gateway sees
    // the server is not the one the token names.
    let other = Handler::new().router();
    let (_, foreign) = call(
        &other,
        "POST",
        "/v1/enrollment-tokens",
        "demo-leader",
        Some(json!({"organizationId": "acme"})),
    )
    .await;
    let foreign = foreign["tokens"][0]["token"].as_str().unwrap().to_string();
    let refused = agentux_gateway::enroll(&url, &foreign, None, "host").await;
    assert!(
        refused.is_err(),
        "a token of another Command must be refused"
    );

    // Enroll with a token this Command issued.
    let (status, issued) = call(
        &app,
        "POST",
        "/v1/enrollment-tokens",
        "demo-leader",
        Some(json!({"organizationId": "acme", "nodeName": "e2e-node"})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let token = issued["tokens"][0]["token"].as_str().unwrap().to_string();
    let (config, waiting): (NodeConfig, bool) = agentux_gateway::enroll(&url, &token, None, "host")
        .await
        .unwrap();
    assert!(!waiting);
    assert_eq!(config.node_id, "e2e-node");
    assert_eq!(config.organization_id, "acme");
    assert_eq!(
        config.command_instance_id.as_deref(),
        Some(instance.as_str()),
        "the node pins this Command"
    );

    // Run the gateway: reports over HTTP, commands over the WebSocket.
    let (stop_gateway, gateway_stopped) = tokio::sync::oneshot::channel::<()>();
    let gateway = tokio::spawn({
        let config = config.clone();
        let socket = socket.clone();
        let state = dir.path().join("gateway.json");
        let pending = dir.path().join("pending.json");
        async move {
            agentux_gateway::run(
                config.clone(),
                socket,
                Http::new(&config).unwrap(),
                Some(state),
                "0.0.0-e2e",
                Some(CommandOptions {
                    policy: Policy::default(),
                    pending,
                }),
                |_| {},
                async {
                    let _ = gateway_stopped.await;
                },
            )
            .await
        }
    });

    let node = wait_for_node(
        &app,
        "e2e-node",
        "the node's hello and its command connection",
        |n| n["agentuxVersion"] == "0.0.0-e2e" && n["connected"] == true,
    )
    .await;
    assert_eq!(node["organizationId"], "acme");

    // A command the node's policy allows runs against agentuxd.
    let command = |body: Value| {
        let app = app.clone();
        async move {
            call(
                &app,
                "POST",
                "/v1/nodes/e2e-node/commands",
                "demo-leader",
                Some(body),
            )
            .await
        }
    };
    let (status, answer) = command(json!({"method": "harness.list", "params": {}})).await;
    assert_eq!(status, StatusCode::OK, "{answer}");
    assert!(answer["result"].is_array(), "the node's agents: {answer}");

    // One its policy holds waits for someone at the node.
    let (status, answer) =
        command(json!({"method": "preferences.set", "params": {"preferences": {}}})).await;
    assert_eq!(status, StatusCode::CONFLICT, "{answer}");
    assert_eq!(answer["error"]["code"], auxp::codes::HELD);
    assert_eq!(
        answer["error"]["message"],
        auxp::policy::REASON_AWAITING_CONFIRMATION
    );
    // ...and the node reports it held it.
    wait_for_node(&app, "e2e-node", "the command.denied event", |n| {
        n["lastSequence"].as_i64().unwrap_or(0) > 0
    })
    .await;

    // Revoked, the node is cut off.
    let (status, _) = call(
        &app,
        "POST",
        "/v1/nodes/e2e-node/revoke",
        "demo-leader",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    wait_for_node(&app, "e2e-node", "the disconnect", |n| {
        n["connected"] == false
    })
    .await;
    let (status, _) = command(json!({"method": "harness.list", "params": {}})).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a revoked node takes no commands"
    );

    let _ = stop_gateway.send(());
    let _ = tokio::time::timeout(Duration::from_secs(10), gateway).await;
    let _ = stop_daemon.send(());
}
