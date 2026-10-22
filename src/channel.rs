//! The command connections AgentUX nodes keep open to Command (AUXP over
//! WebSocket, opened by the node), and sending them commands.
//!
//! A node connects to /v1/protocol/connect. Command writes JSON-RPC requests
//! to it; the node answers each with a response carrying the same id (the
//! commandId). Anything else the node writes (events, heartbeats) goes to
//! the message callback, as if it had been POSTed.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use axum::extract::ws::{CloseFrame, Message, WebSocket, close_code};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::sync::{mpsc, oneshot};

/// A node's answer to one command.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Response {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SendError {
    /// The node has no command connection open.
    NotConnected,
    NoCommandId,
    /// The node went away before answering.
    Disconnected,
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            SendError::NotConnected => "the node is not connected",
            SendError::NoCommandId => "a command needs a commandId",
            SendError::Disconnected => "the node disconnected before answering",
        })
    }
}

impl std::error::Error for SendError {}

enum Outgoing {
    Text(String),
    Close(&'static str),
}

struct Conn {
    id: u64,
    out: mpsc::UnboundedSender<Outgoing>,
    waiting: Mutex<HashMap<String, oneshot::Sender<Response>>>,
}

#[derive(Default)]
struct State {
    conns: HashMap<String, Arc<Conn>>,
    /// Answers by node and commandId: a command sent again gets the first
    /// answer without reaching the node twice.
    answers: HashMap<String, HashMap<String, Response>>,
}

/// The set of connected nodes.
#[derive(Default)]
pub struct Hub {
    state: Mutex<State>,
    next: AtomicU64,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Hub {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether a node has a command connection.
    pub fn connected(&self, node_id: &str) -> bool {
        lock(&self.state).conns.contains_key(node_id)
    }

    /// Serves a node's connection until it closes. A node connecting again
    /// replaces its previous connection. `on_message` gets every message
    /// that is not an answer to a command.
    pub async fn serve(&self, ws: WebSocket, node_id: &str, on_message: impl Fn(&str, &[u8])) {
        let (mut sink, mut stream) = ws.split();
        let (out, mut outgoing) = mpsc::unbounded_channel::<Outgoing>();
        let conn = Arc::new(Conn {
            id: self.next.fetch_add(1, Ordering::Relaxed),
            out,
            waiting: Mutex::default(),
        });
        if let Some(previous) = lock(&self.state)
            .conns
            .insert(node_id.to_string(), conn.clone())
        {
            let _ = previous
                .out
                .send(Outgoing::Close("replaced by a new connection"));
        }
        let writer = tokio::spawn(async move {
            while let Some(message) = outgoing.recv().await {
                let frame = match message {
                    Outgoing::Text(text) => Message::Text(text.into()),
                    Outgoing::Close(reason) => {
                        let _ = sink
                            .send(Message::Close(Some(CloseFrame {
                                code: close_code::POLICY,
                                reason: reason.into(),
                            })))
                            .await;
                        return;
                    }
                };
                if sink.send(frame).await.is_err() {
                    return;
                }
            }
        });

        let mut writer = writer;
        loop {
            let message = tokio::select! {
                message = stream.next() => message,
                // The writer ends when the connection was replaced or revoked.
                _ = &mut writer => None,
            };
            let data = match message {
                Some(Ok(Message::Text(text))) => text.as_bytes().to_vec(),
                Some(Ok(Message::Binary(bytes))) => bytes.to_vec(),
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => continue,
            };
            let Ok(envelope) = serde_json::from_slice::<Value>(&data) else {
                continue;
            };
            let method = envelope.get("method").and_then(Value::as_str).unwrap_or("");
            if method.is_empty()
                && let Some(id) = envelope.get("id")
            {
                let Some(id) = id.as_str() else { continue };
                let answer = Response {
                    result: envelope.get("result").cloned(),
                    error: envelope
                        .get("error")
                        .and_then(|e| serde_json::from_value(e.clone()).ok()),
                };
                if let Some(waiter) = lock(&conn.waiting).remove(id) {
                    let _ = waiter.send(answer);
                }
                continue;
            }
            on_message(node_id, &data);
        }

        {
            let mut state = lock(&self.state);
            if state.conns.get(node_id).is_some_and(|c| c.id == conn.id) {
                state.conns.remove(node_id);
            }
        }
        // Dropping the waiters tells each Send the node went away.
        lock(&conn.waiting).clear();
        writer.abort();
    }

    /// Closes a node's command connection (a revoked node).
    pub fn disconnect(&self, node_id: &str) {
        if let Some(conn) = lock(&self.state).conns.get(node_id) {
            let _ = conn.out.send(Outgoing::Close("revoked"));
        }
    }

    /// Writes a command to a node and waits for its answer. `params` must
    /// carry the commandId, which is also the request id. Callers bound the
    /// wait with a timeout.
    pub async fn send(
        &self,
        node_id: &str,
        method: &str,
        params: Map<String, Value>,
    ) -> Result<Response, SendError> {
        let command_id = params
            .get("commandId")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or(SendError::NoCommandId)?
            .to_string();
        let conn = {
            let state = lock(&self.state);
            if let Some(answer) = state.answers.get(node_id).and_then(|a| a.get(&command_id)) {
                return Ok(answer.clone());
            }
            state
                .conns
                .get(node_id)
                .cloned()
                .ok_or(SendError::NotConnected)?
        };

        let (tx, rx) = oneshot::channel();
        lock(&conn.waiting).insert(command_id.clone(), tx);
        let request =
            json!({"jsonrpc": "2.0", "id": command_id, "method": method, "params": params});
        if conn.out.send(Outgoing::Text(request.to_string())).is_err() {
            lock(&conn.waiting).remove(&command_id);
            return Err(SendError::Disconnected);
        }
        // Whatever ends the wait (an answer, a timeout dropping this future),
        // the waiter goes.
        struct Forget<'a>(&'a Conn, &'a str);
        impl Drop for Forget<'_> {
            fn drop(&mut self) {
                lock(&self.0.waiting).remove(self.1);
            }
        }
        let _forget = Forget(&conn, &command_id);
        let answer = rx.await.map_err(|_| SendError::Disconnected)?;

        let mut state = lock(&self.state);
        let answers = state.answers.entry(node_id.to_string()).or_default();
        if answers.len() > 10_000 {
            answers.clear();
        }
        answers.insert(command_id.clone(), answer.clone());
        Ok(answer)
    }
}
