//! AUXP messages from nodes, and their validation.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Message {
    #[serde(default)]
    pub jsonrpc: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
    #[serde(default)]
    pub method: String,
    #[serde(default)]
    pub params: Params,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Params {
    pub protocol_version: String,
    pub organization_id: String,
    pub node_id: String,
    pub message_id: String,
    pub occurred_at: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub command_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub event_id: String,
    #[serde(skip_serializing_if = "is_zero")]
    pub sequence: i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub mission_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub run_id: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
    #[serde(rename = "agentuxVersion", skip_serializing_if = "String::is_empty")]
    pub agentux_version: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub supported_protocol_versions: Vec<String>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capacity: Option<Value>,
    #[serde(skip_serializing_if = "is_zero")]
    pub last_acknowledged_sequence: i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub provider_token: String,
    /// Event payloads Command keeps as the node sent them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub harnesses: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limits: Option<Value>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub request_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub state: String,
}

fn is_zero(n: &i64) -> bool {
    *n == 0
}

/// The node → command notifications that carry an event identity (eventId,
/// sequence).
pub const EVENT_METHODS: &[&str] = &[
    "node.status_changed",
    "run.accepted",
    "run.started",
    "run.updated",
    "session.event",
    "approval.requested",
    "gate.completed",
    "run.completed",
    "run.failed",
    "artifact.available",
    "node.alert",
    "harness.status",
    "harness.limits_changed",
    "command.denied",
    "command.completed",
];

pub fn is_event(method: &str) -> bool {
    EVENT_METHODS.contains(&method)
}

pub fn validate(message: &Message) -> Result<(), String> {
    if message.jsonrpc != "2.0" {
        return Err("jsonrpc must be 2.0".into());
    }
    if message.method.is_empty() {
        return Err("method is required".into());
    }
    let p = &message.params;
    for (name, value) in [
        ("protocolVersion", &p.protocol_version),
        ("organizationId", &p.organization_id),
        ("nodeId", &p.node_id),
        ("messageId", &p.message_id),
        ("occurredAt", &p.occurred_at),
    ] {
        if value.is_empty() {
            return Err(format!("{name} is required"));
        }
    }
    if p.protocol_version != "v1" {
        return Err(format!(
            "unsupported protocol version {:?}",
            p.protocol_version
        ));
    }
    if let Err(e) = chrono::DateTime::parse_from_rfc3339(&p.occurred_at) {
        return Err(format!("occurredAt must be RFC3339: {e}"));
    }
    if !p.provider_token.is_empty() {
        return Err("provider credentials are not protocol payloads".into());
    }
    match message.method.as_str() {
        "node.hello" => {
            if p.capabilities.is_empty()
                || p.agentux_version.is_empty()
                || p.supported_protocol_versions.is_empty()
            {
                return Err("node.hello requires capabilities, agentuxVersion and supportedProtocolVersions".into());
            }
        }
        "node.heartbeat" => {
            if p.status.is_empty() || p.capacity.is_none() {
                return Err("node.heartbeat requires status and capacity".into());
            }
        }
        "node.resume" => {
            if p.last_acknowledged_sequence < 0 {
                return Err("lastAcknowledgedSequence cannot be negative".into());
            }
        }
        "run.start" => {
            for (name, value) in [
                ("commandId", &p.command_id),
                ("missionId", &p.mission_id),
                ("runId", &p.run_id),
            ] {
                if value.is_empty() {
                    return Err(format!("run.start requires {name}"));
                }
            }
        }
        method => {
            if !is_event(method) {
                return Err(format!("unsupported AUXP method {method:?}"));
            }
            if p.event_id.is_empty() || p.sequence < 1 {
                return Err(format!("{method} requires eventId and a positive sequence"));
            }
            if method == "harness.status" && p.harnesses.is_none() {
                return Err("harness.status requires harnesses".into());
            }
            if method == "harness.limits_changed" && p.limits.is_none() {
                return Err("harness.limits_changed requires limits".into());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn base() -> Params {
        Params {
            protocol_version: "v1".into(),
            organization_id: "org-1".into(),
            node_id: "node-1".into(),
            message_id: "msg-1".into(),
            occurred_at: "2026-10-15T12:00:00Z".into(),
            ..Params::default()
        }
    }

    fn message(method: &str, params: Params) -> Message {
        Message {
            jsonrpc: "2.0".into(),
            id: None,
            method: method.into(),
            params,
        }
    }

    fn event(sequence: i64) -> Params {
        Params {
            event_id: "node-1-1".into(),
            sequence,
            ..base()
        }
    }

    #[test]
    fn accepts_node_hello() {
        let params = Params {
            capabilities: vec!["run.start".into()],
            agentux_version: "0.5.0".into(),
            supported_protocol_versions: vec!["v1".into()],
            ..base()
        };
        validate(&message("node.hello", params)).unwrap();
    }

    #[test]
    fn rejects_provider_credentials() {
        let params = Params {
            provider_token: "must-not-be-here".into(),
            ..base()
        };
        assert!(validate(&message("run.start", params)).is_err());
    }

    #[test]
    fn rejects_unknown_methods() {
        assert!(validate(&message("node.secret", base())).is_err());
    }

    #[test]
    fn accepts_node_events() {
        let status = Params {
            harnesses: Some(json!([{"id": "codex", "installed": true}])),
            ..event(1)
        };
        let limits = Params {
            limits: Some(json!([])),
            ..event(2)
        };
        for (method, params) in [
            ("harness.status", status),
            ("harness.limits_changed", limits),
            ("run.updated", event(3)),
        ] {
            validate(&message(method, params)).unwrap();
        }
    }

    #[test]
    fn rejects_events_without_identity_or_payload() {
        assert!(validate(&message("run.updated", event(0))).is_err());
        assert!(validate(&message("harness.status", event(1))).is_err());
    }
}
