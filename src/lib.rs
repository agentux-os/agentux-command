//! AgentUX Command: the remote control plane AgentUX nodes report to and
//! take commands from, over AUXP (agentux-protocol).
//!
//! Command never runs agents and never touches a node's database or socket:
//! each node's agentuxd stays the authority for its runs, and the node's
//! policy decides which commands it accepts.

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
