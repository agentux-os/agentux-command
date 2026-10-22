//! AgentUX Command: the remote control plane AgentUX nodes report to and
//! take commands from, over AUXP (agentux-protocol).
//!
//! Command never runs agents and never touches a node's database or socket:
//! each node's agentuxd stays the authority for its runs, and the node's
//! policy decides which commands it accepts.

pub mod auth;
pub mod channel;
pub mod enroll;
pub mod nodes;
pub mod office;
pub mod protocol;
pub mod report;
pub mod server;

/// Now, as RFC 3339 with the fraction it has (Go's RFC3339Nano).
pub(crate) fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
}

/// `n` random bytes as hex.
pub(crate) fn random_hex(n: usize) -> String {
    let mut bytes = vec![0u8; n];
    getrandom::fill(&mut bytes).expect("the system's random source");
    hex::encode(bytes)
}

/// A field that may come as `null`, read as its default, as the Go server
/// did: clients send `null` for what they do not set (the node gateway's
/// `nodeName` when the node has none).
pub(crate) fn nullable<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Default + serde::Deserialize<'de>,
{
    Ok(<Option<T> as serde::Deserialize>::deserialize(deserializer)?.unwrap_or_default())
}
