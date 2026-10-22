mod common;

use std::sync::Arc;
use std::time::Duration;

use agentux_command::office::{
    self, OfficeRepository, Projection, ResolutionRequest, ResolutionResponse,
};
use agentux_command::server::{Handler, parse_node_tokens};
use async_trait::async_trait;
use axum::http::StatusCode;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::{self, client::IntoClientRequest};

use common::{call, call_raw, hello, params};

#[tokio::test]
async fn the_gateway_accepts_hello_and_lists_the_node() {
    let app = Handler::new().router();
    let message = json!({"jsonrpc": "2.0", "id": "hello-1", "method": "node.hello", "params": {
        "protocolVersion": "v1", "organizationId": "org-1", "nodeId": "node-1", "messageId": "msg-1", "occurredAt": "2026-10-15T12:00:00Z",
        "capabilities": ["run.start"], "agentuxVersion": "0.5.0", "supportedProtocolVersions": ["v1"],
    }});
    let (status, _) = call(&app, "POST", "/v1/protocol/messages", "", Some(message)).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let (status, list) = call(&app, "GET", "/v1/nodes", "demo-observer", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list[0]["nodeId"], "node-1");
}

#[tokio::test]
async fn the_office_projection_needs_a_capability_and_carries_replay_metadata() {
    let app = Handler::new().router();
    let (status, projection) = call(
        &app,
        "GET",
        "/v1/office/projection?organizationId=org-demo",
        "demo-leader",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    for field in [
        "organizationId",
        "definitionVersion",
        "projectedAt",
        "freshness",
        "lastEventId",
        "events",
    ] {
        assert!(
            !projection[field].is_null(),
            "projection missing {field}: {projection}"
        );
    }
    let (status, _) = call(&app, "GET", "/v1/office/projection", "", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = call(
        &app,
        "GET",
        "/v1/office/projection?organizationId=org-missing",
        "demo-leader",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = call(&app, "GET", "/v1/office/projection", "invalid", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn office_approval_resolution_is_idempotent() {
    let app = Handler::new().router();
    let path = "/v1/approvals/approval-01/resolve";
    let body = json!({"organizationId": "org-demo", "decision": "approved", "reason": "release reviewed", "idempotencyKey": "idem-01"});
    let (status, _) = call(&app, "POST", path, "demo-leader", Some(body.clone())).await;
    assert_eq!(status, StatusCode::OK);
    let (status, replay) = call(&app, "POST", path, "demo-leader", Some(body)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay["idempotentReplay"], true);
    let (status, decided) = call(&app, "POST", path, "demo-leader", Some(json!({"organizationId": "org-demo", "decision": "rejected", "reason": "changed", "idempotencyKey": "idem-02"}))).await;
    assert_eq!(status, StatusCode::CONFLICT);
    // The protocol's conflict shape: lowercase code and message.
    assert_eq!(decided["conflict"]["code"], "already_decided", "{decided}");
    assert_eq!(decided["ok"], false);
    let (status, _) = call(&app, "POST", path, "demo-observer", Some(json!({"organizationId": "org-demo", "decision": "approved", "idempotencyKey": "idem-03"}))).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = call_raw(&app, "POST", path, "demo-leader", "{not json").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

struct Failing;

#[async_trait]
impl OfficeRepository for Failing {
    async fn ping(&self) -> Result<(), office::Error> {
        Err(office::Error::Storage("database unavailable".into()))
    }
    async fn get_projection(&self, _: &str) -> Result<Option<Projection>, office::Error> {
        Err(office::Error::Storage("database unavailable".into()))
    }
    async fn apply_event(&self, _: office::Event) -> Result<(), office::Error> {
        Err(office::Error::Storage("database unavailable".into()))
    }
    async fn resolve_approval(
        &self,
        _: &str,
        _: ResolutionRequest,
    ) -> Result<ResolutionResponse, office::Error> {
        Err(office::Error::Storage("database unavailable".into()))
    }
}

#[tokio::test]
async fn storage_failures_are_unavailable() {
    let app = Handler::with_office(Arc::new(Failing)).router();
    let (status, _) = call(
        &app,
        "GET",
        "/v1/office/projection?organizationId=org-demo",
        "demo-leader",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let (status, _) = call(&app, "GET", "/healthz", "", None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn the_production_server_needs_a_database_url() {
    assert!(Handler::postgres("").await.is_err());
    assert!(Handler::postgres("not-a-postgres-url").await.is_err());
}

#[tokio::test]
async fn node_events_show_in_the_node_list() {
    let app = Handler::new()
        .with_node_tokens(parse_node_tokens("node-1=s3cret, node-2=other"))
        .router();
    let message = |method: &str, extra: Value| json!({"jsonrpc": "2.0", "method": method, "params": params("org-1", "node-1", extra)});
    let hello = json!({"jsonrpc": "2.0", "id": "h", "method": "node.hello", "params": params("org-1", "node-1", json!({
        "capabilities": ["harness.status"], "agentuxVersion": "0.4.8", "supportedProtocolVersions": ["v1"],
    }))});
    let (status, _) = call(
        &app,
        "POST",
        "/v1/protocol/messages",
        "wrong",
        Some(hello.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = call(&app, "POST", "/v1/protocol/messages", "s3cret", Some(hello)).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let status_event = message(
        "harness.status",
        json!({"eventId": "node-1-1", "sequence": 1, "harnesses": [{"id": "codex", "installed": true, "signedIn": true}]}),
    );
    let (status, _) = call(
        &app,
        "POST",
        "/v1/protocol/messages",
        "s3cret",
        Some(status_event),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let limits = message(
        "harness.limits_changed",
        json!({"eventId": "node-1-2", "sequence": 2, "limits": [{"harness": "codex", "kind": "weekly"}]}),
    );
    call(
        &app,
        "POST",
        "/v1/protocol/messages",
        "s3cret",
        Some(limits),
    )
    .await;

    let (_, list) = call(&app, "GET", "/v1/nodes", "demo-observer", None).await;
    let list = list.as_array().unwrap();
    assert_eq!(list.len(), 1);
    let node = &list[0];
    assert_eq!(node["agentuxVersion"], "0.4.8");
    assert_eq!(node["lastSequence"], 2);
    assert_eq!(node["limits"][0]["kind"], "weekly");
    assert_eq!(node["harnesses"][0]["signedIn"], true);
}

#[tokio::test]
async fn invalid_messages_are_refused() {
    let app = Handler::new().router();
    let (status, _) = call_raw(&app, "POST", "/v1/protocol/messages", "", "{").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let bad = json!({"jsonrpc": "2.0", "method": "node.hello", "params": params("org-1", "node-1", json!({}))});
    let (status, body) = call(&app, "POST", "/v1/protocol/messages", "", Some(bad)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("node.hello requires")
    );
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn dial(
    address: &str,
    node: &str,
    token: &str,
) -> Result<(Socket, tungstenite::http::Response<Option<Vec<u8>>>), Box<tungstenite::Error>> {
    let mut request = format!("ws://{address}/v1/protocol/connect?nodeId={node}")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    tokio_tungstenite::connect_async(request)
        .await
        .map_err(Box::new)
}

#[tokio::test(flavor = "multi_thread")]
async fn operators_send_commands_to_connected_nodes() {
    let handler = Handler::new().with_node_tokens(parse_node_tokens("node-1=s3cret"));
    let instance = handler.instance_id().to_string();
    let app = handler.router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    tokio::spawn(axum::serve(listener, app.clone()).into_future());
    call(
        &app,
        "POST",
        "/v1/protocol/messages",
        "s3cret",
        Some(hello("org-1", "node-1")),
    )
    .await;

    let command = |token: &'static str, body: Value| {
        let app = app.clone();
        async move { call(&app, "POST", "/v1/nodes/node-1/commands", token, Some(body)).await }
    };
    let (status, _) = command(
        "demo-observer",
        json!({"method": "harness.list", "params": {}}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "observers must not send commands"
    );
    let (status, _) = command(
        "demo-leader",
        json!({"method": "harness.list", "params": {}}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "before the node connects"
    );
    let (status, _) = command("demo-leader", json!({"method": "shell.exec", "params": {}})).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "unknown methods are refused"
    );
    let (status, _) = command(
        "demo-leader",
        json!({"method": "run.start", "params": {"apiKey": "x"}}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "credentials are refused");

    assert!(
        dial(&address, "node-1", "wrong").await.is_err(),
        "a wrong node token must not connect"
    );
    let (socket, response) = dial(&address, "node-1", "s3cret").await.unwrap();
    assert_eq!(
        response.headers()["x-agentux-command-instance"],
        instance.as_str()
    );
    let (mut sink, mut stream) = socket.split();
    tokio::spawn(async move {
        while let Some(Ok(tungstenite::Message::Text(text))) = stream.next().await {
            let request: Value = serde_json::from_str(&text).unwrap();
            let answer = if request["method"] == "preferences.set" {
                json!({"jsonrpc": "2.0", "id": request["id"], "error": {"code": -32010, "message": "awaiting_local_confirmation"}})
            } else {
                json!({"jsonrpc": "2.0", "id": request["id"], "result": {"method": request["method"], "nodeId": request["params"]["nodeId"], "commandInstanceId": request["params"]["commandInstanceId"]}})
            };
            if sink
                .send(tungstenite::Message::Text(answer.to_string().into()))
                .await
                .is_err()
            {
                return;
            }
        }
    });
    for _ in 0..200 {
        let (_, list) = call(&app, "GET", "/v1/nodes", "demo-observer", None).await;
        if list[0]["connected"] == true {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let (status, answer) = command(
        "demo-leader",
        json!({"method": "harness.list", "commandId": "cmd-1", "params": {}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{answer}");
    assert_eq!(answer["result"]["nodeId"], "node-1");
    assert_eq!(answer["result"]["commandInstanceId"], instance.as_str());
    let (status, answer) = command(
        "demo-leader",
        json!({"method": "preferences.set", "params": {"preferences": {}}}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(answer["error"]["message"], "awaiting_local_confirmation");

    let (_, list) = call(&app, "GET", "/v1/nodes", "demo-observer", None).await;
    assert_eq!(list[0]["connected"], true);
}

#[tokio::test]
async fn nodes_enroll_with_one_command_only() {
    let ours_handler = Handler::new().with_open_nodes(false);
    let theirs_handler = Handler::new().with_open_nodes(false);
    let ours_id = ours_handler.instance_id().to_string();
    assert_ne!(
        ours_id,
        theirs_handler.instance_id(),
        "each Command server has its own instance id"
    );
    let (ours, theirs) = (ours_handler.router(), theirs_handler.router());
    let (_, instance) = call(&ours, "GET", "/v1/instance", "", None).await;
    assert_eq!(instance["instanceId"], ours_id.as_str());

    let (status, _) = call(
        &ours,
        "POST",
        "/v1/enrollment-tokens",
        "demo-observer",
        Some(json!({"organizationId": "acme"})),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "observers must not issue tokens"
    );
    let (status, issued) = call(
        &ours,
        "POST",
        "/v1/enrollment-tokens",
        "demo-leader",
        Some(json!({"organizationId": "acme", "count": 2})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let token = issued["tokens"][0]["token"].as_str().unwrap().to_string();
    let (_, foreign) = call(
        &theirs,
        "POST",
        "/v1/enrollment-tokens",
        "demo-leader",
        Some(json!({"organizationId": "acme"})),
    )
    .await;
    let foreign_token = foreign["tokens"][0]["token"].as_str().unwrap().to_string();

    // Another Command's token cannot capture this node, nor ours theirs.
    let (status, _) = call(
        &ours,
        "POST",
        "/v1/enroll",
        "",
        Some(json!({"token": foreign_token, "hostname": "box"})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, enrolled) = call(
        &ours,
        "POST",
        "/v1/enroll",
        "",
        Some(json!({"token": token, "hostname": "box"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(enrolled["instanceId"], ours_id.as_str());
    assert_eq!(enrolled["organizationId"], "acme");
    let node = enrolled["nodeId"].as_str().unwrap().to_string();
    let secret = enrolled["nodeSecret"].as_str().unwrap().to_string();
    let (status, _) = call(
        &ours,
        "POST",
        "/v1/enroll",
        "",
        Some(json!({"token": token})),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "a token is used once");

    let (status, accepted) = call(
        &ours,
        "POST",
        "/v1/protocol/messages",
        &secret,
        Some(hello("acme", &node)),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(accepted["result"]["instanceId"], ours_id.as_str());
    // The credential is bound to its organization and to this server.
    let (status, _) = call(
        &ours,
        "POST",
        "/v1/protocol/messages",
        &secret,
        Some(hello("other-org", &node)),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = call(
        &theirs,
        "POST",
        "/v1/protocol/messages",
        &secret,
        Some(hello("acme", &node)),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (_, enrollments) = call(&ours, "GET", "/v1/enrollments", "demo-observer", None).await;
    assert_eq!(enrollments["nodes"][0]["nodeId"], node.as_str());

    let (status, _) = call(
        &ours,
        "POST",
        &format!("/v1/nodes/{node}/revoke"),
        "demo-leader",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        &ours,
        "POST",
        "/v1/protocol/messages",
        &secret,
        Some(hello("acme", &node)),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "a revoked node is refused"
    );
    let (status, _) = call(
        &ours,
        "POST",
        "/v1/nodes/nobody/approve",
        "demo-leader",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn nodes_waiting_for_approval_only_say_hello() {
    let app = Handler::new().with_open_nodes(false).router();
    let (_, issued) = call(
        &app,
        "POST",
        "/v1/enrollment-tokens",
        "demo-leader",
        Some(json!({"organizationId": "acme", "nodeName": "gpu-1", "requireApproval": true})),
    )
    .await;
    let token = issued["tokens"][0]["token"].as_str().unwrap();
    let (_, enrolled) = call(
        &app,
        "POST",
        "/v1/enroll",
        "",
        Some(json!({"token": token})),
    )
    .await;
    assert_eq!(enrolled["nodeId"], "gpu-1");
    assert_eq!(enrolled["approved"], false);
    let secret = enrolled["nodeSecret"].as_str().unwrap().to_string();
    let event = json!({"jsonrpc": "2.0", "method": "harness.limits_changed", "params": params("acme", "gpu-1", json!({"eventId": "e1", "sequence": 1, "limits": []}))});
    let (status, _) = call(
        &app,
        "POST",
        "/v1/protocol/messages",
        &secret,
        Some(event.clone()),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a node waiting for approval must not send events"
    );
    let (status, _) = call(
        &app,
        "POST",
        "/v1/protocol/messages",
        &secret,
        Some(hello("acme", "gpu-1")),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "hello is accepted while waiting"
    );
    let (_, list) = call(&app, "GET", "/v1/nodes", "demo-observer", None).await;
    assert_eq!(list[0]["status"], "pending_approval");
    call(&app, "POST", "/v1/nodes/gpu-1/approve", "demo-leader", None).await;
    let (status, _) = call(&app, "POST", "/v1/protocol/messages", &secret, Some(event)).await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "an approved node sends events"
    );
}

#[tokio::test]
async fn tokens_come_with_provision_files() {
    let app = Handler::new()
        .with_public_url("https://command.acme.org/")
        .router();
    let (status, issued) = call(&app, "POST", "/v1/enrollment-tokens", "demo-leader", Some(json!({
        "organizationId": "acme", "count": 2,
        "provision": {"profile": "node", "user": "dev", "policy": {"commands": {"run.start": "allow"}}},
    }))).await;
    assert_eq!(status, StatusCode::CREATED);
    let token = &issued["tokens"][1];
    let file = &token["provision"];
    assert_eq!(file["profile"], "node");
    assert_eq!(file["user"], "dev");
    assert_eq!(file["command"]["url"], "https://command.acme.org");
    assert_eq!(file["command"]["enrollmentToken"], token["token"]);
    assert_eq!(file["policy"]["commands"]["run.start"], "allow");
    let (status, _) = call(
        &app,
        "POST",
        "/v1/enrollment-tokens",
        "demo-leader",
        Some(json!({
            "organizationId": "acme", "provision": {"profile": "executive", "user": "dev"},
        })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "an Executive runs no node to provision"
    );
}

#[tokio::test]
async fn the_node_list_needs_an_operator() {
    let app = Handler::new().router();
    for token in ["", "not-a-token"] {
        let (status, _) = call(&app, "GET", "/v1/nodes", token, None).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "listing nodes with {token:?}"
        );
    }
    let (status, _) = call(&app, "GET", "/v1/nodes", "demo-observer", None).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(&app, "GET", "/nowhere", "", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn fields_sent_as_null_read_as_unset() {
    // The node gateway sends "nodeName": null when the node has no name.
    let app = Handler::new().router();
    let (_, issued) = call(
        &app,
        "POST",
        "/v1/enrollment-tokens",
        "demo-leader",
        Some(json!({"organizationId": "acme", "nodeName": null, "count": null})),
    )
    .await;
    let token = issued["tokens"][0]["token"].as_str().unwrap();
    let (status, enrolled) = call(
        &app,
        "POST",
        "/v1/enroll",
        "",
        Some(json!({"token": token, "nodeName": null, "hostname": "box"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{enrolled}");
    assert_eq!(enrolled["nodeId"], "node-box");
    let (status, _) = call(&app, "POST", "/v1/approvals/approval-01/resolve", "demo-leader", Some(json!({"organizationId": "org-demo", "decision": "approved", "reason": null, "idempotencyKey": "k"}))).await;
    assert_eq!(status, StatusCode::OK);
}
