use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use agentux_command::channel::{Hub, SendError};
use axum::Router;
use axum::extract::{Query, State, WebSocketUpgrade};
use axum::response::Response;
use axum::routing::get;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio_tungstenite::tungstenite::Message;

#[derive(Deserialize)]
struct Node {
    #[serde(rename = "nodeId")]
    node_id: String,
}

struct Shared {
    hub: Hub,
    messages: AtomicUsize,
}

async fn serve(
    State(shared): State<Arc<Shared>>,
    Query(node): Query<Node>,
    upgrade: WebSocketUpgrade,
) -> Response {
    upgrade.on_upgrade(move |ws| async move {
        let counter = shared.clone();
        shared
            .hub
            .serve(ws, &node.node_id, move |id, raw| {
                if id == "node-1" && String::from_utf8_lossy(raw).contains("node.heartbeat") {
                    counter.messages.fetch_add(1, Ordering::SeqCst);
                }
            })
            .await;
    })
}

fn command(id: &str) -> Map<String, Value> {
    json!({"commandId": id}).as_object().cloned().unwrap()
}

async fn wait_for(what: &str, check: impl Fn() -> bool) {
    for _ in 0..500 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test(flavor = "multi_thread")]
async fn commands_reach_the_node_once_and_messages_reach_the_callback() {
    let shared = Arc::new(Shared {
        hub: Hub::new(),
        messages: AtomicUsize::new(0),
    });
    let app = Router::new()
        .route("/", get(serve))
        .with_state(shared.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(axum::serve(listener, app).into_future());

    assert_eq!(
        shared
            .hub
            .send("node-1", "harness.list", command("c1"))
            .await,
        Err(SendError::NotConnected)
    );

    let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/?nodeId=node-1"))
        .await
        .unwrap();
    let (mut sink, mut stream) = socket.split();
    sink.send(Message::Text(
        r#"{"jsonrpc":"2.0","method":"node.heartbeat","params":{}}"#.into(),
    ))
    .await
    .unwrap();
    let got = Arc::new(AtomicUsize::new(0));
    let counted = got.clone();
    tokio::spawn(async move {
        while let Some(Ok(Message::Text(text))) = stream.next().await {
            let request: Value = serde_json::from_str(&text).unwrap();
            counted.fetch_add(1, Ordering::SeqCst);
            let answer = json!({"jsonrpc": "2.0", "id": request["id"], "result": {"ran": request["method"]}});
            if sink
                .send(Message::Text(answer.to_string().into()))
                .await
                .is_err()
            {
                return;
            }
        }
    });
    wait_for("the connection", || shared.hub.connected("node-1")).await;

    let answer = shared
        .hub
        .send("node-1", "harness.list", command("c1"))
        .await
        .unwrap();
    assert_eq!(answer.result, Some(json!({"ran": "harness.list"})));
    // The same commandId again: the first answer, without the node.
    let again = shared
        .hub
        .send("node-1", "harness.list", command("c1"))
        .await
        .unwrap();
    assert_eq!(again, answer);
    assert_eq!(got.load(Ordering::SeqCst), 1);
    assert_eq!(
        shared.hub.send("node-1", "harness.list", Map::new()).await,
        Err(SendError::NoCommandId)
    );
    wait_for("the node's own message", || {
        shared.messages.load(Ordering::SeqCst) > 0
    })
    .await;

    shared.hub.disconnect("node-1");
    wait_for("the disconnect", || !shared.hub.connected("node-1")).await;
}
