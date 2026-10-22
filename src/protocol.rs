//! AUXP messages from nodes. The contract, its checks and the params Command
//! reads come from the shared `auxp` crate (agentux-protocol), which the
//! node gateway uses too.

use serde::Deserialize;
use serde_json::Value;

pub use auxp::methods::is_event;
pub use auxp::{PROTOCOL_VERSION, Params};

/// A node's message as Command reads it.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Message {
    #[serde(default)]
    pub jsonrpc: String,
    #[serde(default)]
    pub id: Option<Value>,
    #[serde(default)]
    pub method: String,
    #[serde(default)]
    pub params: Params,
}

/// Checks a raw message against AUXP and reads it.
pub fn read(raw: &[u8]) -> Result<Message, String> {
    let value: Value = serde_json::from_slice(raw).map_err(|_| "invalid JSON".to_string())?;
    auxp::validate(&value)?;
    serde_json::from_value(value).map_err(|e| format!("invalid message: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reads_valid_messages_and_refuses_the_rest() {
        let hello = json!({"jsonrpc": "2.0", "id": "h", "method": "node.hello", "params": {
            "protocolVersion": "v1", "organizationId": "org-1", "nodeId": "node-1", "messageId": "m",
            "occurredAt": "2026-10-15T12:00:00Z", "capabilities": ["run.start"], "agentuxVersion": "0.5.0",
            "supportedProtocolVersions": ["v1"],
        }});
        let message = read(hello.to_string().as_bytes()).unwrap();
        assert_eq!(message.method, "node.hello");
        assert_eq!(message.params.node_id, "node-1");
        assert_eq!(message.id, Some(json!("h")));
        assert_eq!(read(b"{").unwrap_err(), "invalid JSON");
        assert!(
            read(
                json!({"jsonrpc": "2.0", "method": "node.secret", "params": {}})
                    .to_string()
                    .as_bytes()
            )
            .is_err()
        );
    }
}
