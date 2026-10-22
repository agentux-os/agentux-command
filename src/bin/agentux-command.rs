//! The AgentUX Command server.

use std::process::ExitCode;

use agentux_command::server::{Handler, parse_node_tokens};

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("agentux-command: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), String> {
    let env = |name: &str| std::env::var(name).unwrap_or_default();
    let address = match env("AGENTUX_COMMAND_ADDR") {
        a if a.is_empty() => "0.0.0.0:8080".to_string(),
        // ":8080": every interface.
        a if a.starts_with(':') => format!("0.0.0.0{a}"),
        a => a,
    };
    let database_url = env("AGENTUX_COMMAND_DATABASE_URL");
    let startup = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        Handler::postgres(&database_url),
    );
    let mut handler = startup
        .await
        .map_err(|_| "the database did not answer within 30 s".to_string())??;
    // Each node authenticates with its own token: node-1=token,node-2=token.
    let tokens = parse_node_tokens(&env("AGENTUX_COMMAND_NODE_TOKENS"));
    if !tokens.is_empty() {
        handler = handler.with_node_tokens(tokens);
    }
    if env("AGENTUX_COMMAND_INSECURE_OPEN_NODES") == "1" {
        eprintln!("AGENTUX_COMMAND_INSECURE_OPEN_NODES=1: nodes that never enrolled may report");
        handler = handler.with_open_nodes(true);
    }
    let public_url = env("AGENTUX_COMMAND_PUBLIC_URL");
    if !public_url.is_empty() {
        handler = handler.with_public_url(&public_url);
    }
    eprintln!("agentux-command instance {}", handler.instance_id());

    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .map_err(|e| format!("listen on {address}: {e}"))?;
    eprintln!("agentux-command listening on {address}");
    axum::serve(listener, handler.router())
        .with_graceful_shutdown(shutdown())
        .await
        .map_err(|e| e.to_string())
}

/// Ctrl-C, or SIGTERM from systemd or Podman.
async fn shutdown() {
    let interrupt = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("a SIGTERM handler");
        tokio::select! {
            _ = interrupt => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = interrupt.await;
}
