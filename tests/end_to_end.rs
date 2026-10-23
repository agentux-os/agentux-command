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

/// Polls a mission run until `check` holds.
async fn wait_for_run(
    app: &Router,
    run: &str,
    what: &str,
    check: impl Fn(&Value) -> bool,
) -> Value {
    let mut last = Value::Null;
    for _ in 0..600 {
        let (_, found) = call(
            app,
            "GET",
            &format!("/v1/organizations/acme/runs/{run}"),
            "demo-observer",
            None,
        )
        .await;
        if check(&found) {
            return found;
        }
        last = found;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (_, nodes) = call(app, "GET", "/v1/nodes", "demo-observer", None).await;
    let (_, log) = call(
        app,
        "GET",
        "/v1/organizations/acme/events?limit=1000",
        "demo-observer",
        None,
    )
    .await;
    panic!(
        "timed out waiting for {what}: {last:#}
nodes: {nodes:#}
log: {log:#}"
    );
}

/// A git repository with an origin, as a node's project.
fn repository(dir: &std::path::Path) -> std::path::PathBuf {
    let repo = dir.join("app");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(repo.join("README.md"), "# app\n").unwrap();
    let git = |args: &[&str]| {
        let status = std::process::Command::new("git")
            .args([
                "-c",
                "user.name=e2e",
                "-c",
                "user.email=e2e@example.invalid",
                "-c",
                "init.defaultBranch=main",
            ])
            .args(args)
            .current_dir(&repo)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?}");
    };
    git(&["init", "--quiet"]);
    git(&["add", "README.md"]);
    git(&["commit", "--quiet", "-m", "start"]);
    git(&[
        "remote",
        "add",
        "origin",
        "https://example.invalid/acme/app.git",
    ]);
    repo
}

/// The whole organization workflow, nothing stubbed but the agents (and no
/// model credentials): a leader drafts, edits and publishes an organization;
/// a node enrolls and connects; a mission runs its agent step on the node's
/// agentuxd, waits at a human-required gate a service token cannot pass, is
/// approved by a person, and finishes; the event log and the Office
/// projection show every step with the definition version.
#[tokio::test(flavor = "multi_thread")]
async fn an_organization_runs_a_mission_on_a_real_node_through_a_human_gate() {
    let handler = Handler::new().with_open_nodes(false);
    let robot = handler
        .service_tokens()
        .issue(
            agentux_command::auth::TokenRequest {
                name: "robot".into(),
                organization_id: "acme".into(),
                capabilities: vec!["approval.decide".into(), "organization.read".into()],
                ..Default::default()
            },
            chrono::Utc::now(),
        )
        .await
        .unwrap()
        .secret;
    let app = handler.router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(axum::serve(listener, app.clone()).into_future());

    // 1. The leader drafts the organization, fixes it, and publishes it.
    let mut definition = json!({
        "schema_version": "1.0",
        "organization": {"id": "acme", "name": "Acme Robotics"},
        "areas": [{"id": "engineering", "name": "Engineering", "owner_role_id": "cto"}],
        "roles": [
            {"id": "cto", "name": "CTO", "principal_kind": "human", "permissions": ["organization.read", "approval.decide"]},
            {"id": "dev", "name": "Developer", "principal_kind": "agent", "permissions": ["run.read"]}
        ],
        "missions": [{"id": "ship", "area_id": "engineering", "name": "Ship a release", "owner_role_id": "cto"}],
        "workflows": [{"id": "ship-v1", "mission_id": "ship", "steps": [
            {"id": "implement", "kind": "agent", "role_id": "dev", "next": ["review"]},
            {"id": "review", "kind": "gate", "gate_id": "release", "next": ["announce"]},
            {"id": "announce", "kind": "action", "next": []}
        ]}],
        "gates": [{"id": "release", "kind": "approval", "required_permissions": ["approval.decide"], "policy": "human-required"}],
        "agents": [{"id": "dev-1", "role_id": "dev"}],
        "nodes": []
    });
    definition["workflows"][0]["steps"][0]["next"] = json!(["nowhere"]);
    let base = "/v1/organizations/acme/definitions";
    let (status, draft) = call(&app, "POST", base, "demo-leader", Some(definition.clone())).await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(
        !draft["problems"].as_array().unwrap().is_empty(),
        "the broken step is caught"
    );
    definition["workflows"][0]["steps"][0]["next"] = json!(["review"]);
    let (status, fixed) = call(
        &app,
        "PUT",
        &format!("{base}/1"),
        "demo-leader",
        Some(definition),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fixed["problems"], json!([]));
    let (status, _) = call(
        &app,
        "POST",
        &format!("{base}/1/publish"),
        "demo-leader",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // 2. A node: agentuxd with scripted agents and the project, enrolled
    //    with a token from this Command, its policy letting Command start runs.
    let dir = tempfile::tempdir().unwrap();
    let repo = repository(dir.path());
    let socket = dir.path().join("agentuxd.sock");
    let engine = Engine::new(
        Store::open(&dir.path().join("agentuxd.db")).unwrap(),
        Arc::new(FakeExecutor::default()),
    );
    engine
        .register_project(repo.to_str().unwrap())
        .await
        .unwrap();
    let (_stop_daemon, daemon_stopped) = tokio::sync::oneshot::channel::<()>();
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
    let (_, issued) = call(
        &app,
        "POST",
        "/v1/enrollment-tokens",
        "demo-leader",
        Some(json!({"organizationId": "acme", "nodeName": "org-node"})),
    )
    .await;
    let token = issued["tokens"][0]["token"].as_str().unwrap().to_string();
    let (config, _) = agentux_gateway::enroll(&url, &token, None, "host")
        .await
        .unwrap();
    let (_stop_gateway, gateway_stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn({
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
                    policy: Policy::parse("commands:\n  run.start: allow\n").unwrap(),
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
    let node = wait_for_node(&app, "org-node", "the node online and connected", |n| {
        n["connected"] == true
    })
    .await;
    assert!(
        node["capabilities"]
            .as_array()
            .is_some_and(|c| !c.is_empty()),
        "the node advertises what it can do: {node}"
    );

    // 3. The mission: its agent step runs on the node.
    let (status, run) = call(
        &app,
        "POST",
        "/v1/organizations/acme/runs",
        "demo-leader",
        Some(json!({"missionId": "ship", "repository": "https://example.invalid/acme/app", "prompt": "Ship the release", "nodeId": "org-node"})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{run}");
    let run_id = run["runId"].as_str().unwrap().to_string();
    assert_eq!(run["definitionVersion"], 1);
    let step = &run["steps"]["implement"];
    assert_eq!(step["status"], "waiting", "{run}");
    assert!(
        !step["nodeRunId"].as_str().unwrap_or_default().is_empty(),
        "the node's own run id is known: {run}"
    );

    // ...the node's own approvals (its plan) are decided from Office and go
    // back to the node; then its agents finish, and the gate asks a person.
    let mut decided = std::collections::HashSet::new();
    loop {
        let (_, run) = call(
            &app,
            "GET",
            &format!("/v1/organizations/acme/runs/{run_id}"),
            "demo-observer",
            None,
        )
        .await;
        if run["step"] == "review" {
            break;
        }
        let (_, projection) = call(
            &app,
            "GET",
            "/v1/office/projection?organizationId=acme",
            "demo-observer",
            None,
        )
        .await;
        for approval in projection["approvals"].as_array().unwrap() {
            let id = approval["id"].as_str().unwrap().to_string();
            if approval["status"] == "pending"
                && approval["nodeId"] == "org-node"
                && decided.insert(id.clone())
            {
                let (status, body) = call(
                    &app,
                    "POST",
                    &format!("/v1/approvals/{id}/resolve"),
                    "demo-leader",
                    Some(json!({"organizationId": "acme", "decision": "approved", "idempotencyKey": format!("e2e-{id}")})),
                )
                .await;
                assert_eq!(status, StatusCode::OK, "{body}");
                assert_eq!(
                    body["delivery"]["delivered"], true,
                    "the node got the decision: {body}"
                );
            }
        }
        assert!(decided.len() < 20, "the node keeps asking: {run:#}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!decided.is_empty(), "the node asked to approve its plan");
    let run = wait_for_run(&app, &run_id, "the human gate", |r| r["step"] == "review").await;
    assert_eq!(run["status"], "waiting");
    assert_eq!(run["steps"]["implement"]["status"], "done");
    let approval = run["steps"]["review"]["approvalId"]
        .as_str()
        .unwrap()
        .to_string();

    // 4. A service token cannot pass a human-required gate; a person can.
    let decide = json!({"organizationId": "acme", "decision": "approved", "idempotencyKey": "e2e"});
    let path = format!("/v1/approvals/{approval}/resolve");
    let (status, _) = call(&app, "POST", &path, &robot, Some(decide.clone())).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, body) = call(&app, "POST", &path, "demo-leader", Some(decide)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let run = wait_for_run(&app, &run_id, "the end of the mission", |r| {
        r["status"] == "completed"
    })
    .await;
    assert_eq!(run["steps"]["announce"]["status"], "done");

    // 5. Durable, versioned events, and Office's view of them.
    let (_, log) = call(
        &app,
        "GET",
        "/v1/organizations/acme/events?limit=1000",
        "demo-observer",
        None,
    )
    .await;
    let events: Vec<&Value> = log["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| &e["event"])
        .collect();
    let types: Vec<&str> = events
        .iter()
        .map(|e| e["eventType"].as_str().unwrap())
        .collect();
    for expected in [
        "organization.definition.drafted",
        "organization.definition.published",
        "mission.run.started",
        "mission.run.dispatched",
        "run.completed",
        "approval.requested",
        "approval.resolved",
        "mission.run.completed",
    ] {
        assert!(types.contains(&expected), "{expected} in {types:?}");
    }
    let completed = events
        .iter()
        .find(|e| e["eventType"] == "mission.run.completed")
        .unwrap();
    assert_eq!(completed["definitionVersion"], "definition-1");
    assert_eq!(completed["correlationId"], run_id.as_str());
    let resolved = events
        .iter()
        .find(|e| e["eventType"] == "approval.resolved")
        .unwrap();
    assert_eq!(resolved["actor"]["id"], "demo-leader");

    let (_, projection) = call(
        &app,
        "GET",
        "/v1/office/projection?organizationId=acme",
        "demo-observer",
        None,
    )
    .await;
    assert_eq!(projection["organization"]["name"], "Acme Robotics");
    assert_eq!(projection["definitionVersion"], "definition-1");
    let runs = projection["runs"].as_array().unwrap();
    let mission = runs.iter().find(|r| r["id"] == run_id.as_str()).unwrap();
    assert_eq!(mission["status"], "completed");
    let node_run = runs
        .iter()
        .find(|r| r["id"] == step["nodeRunId"])
        .expect("the node's run, as the node reported it");
    assert_eq!(node_run["status"], "completed");
    // ...and what it produced: the node's pull request, as an artifact.
    let artifact = projection["artifacts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["runId"] == step["nodeRunId"])
        .unwrap_or_else(|| panic!("the run's pull request: {projection:#}"));
    assert_eq!(artifact["kind"], "pull_request");
    assert_eq!(projection["nodes"][0]["id"], "org-node");
    let pending: Vec<&Value> = projection["approvals"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| a["status"] == "pending")
        .collect();
    assert!(pending.is_empty(), "the gate was decided: {pending:?}");
}
