//! The AgentUX Command server.
//!
//! `agentux-command` serves the API. `agentux-command token issue …` issues
//! an operator service token straight in the database, for the first
//! administrator (later ones come from the API):
//!
//!   agentux-command token issue --organization acme --name admin \
//!       --capabilities auth.admin,organization.read,node.enroll,node.command,approval.decide

use std::process::ExitCode;

use agentux_command::server::{Handler, parse_node_tokens};

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("token") => token(&args[1..]).await,
        Some(other) => Err(format!("unknown command {other:?} (try: token issue)")),
        None => run().await,
    };
    match result {
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
    if env("AGENTUX_COMMAND_INSECURE_DEMO_TOKENS") == "1" {
        eprintln!(
            "AGENTUX_COMMAND_INSECURE_DEMO_TOKENS=1: demo-leader and demo-observer are accepted"
        );
        handler = handler.with_demo_tokens(true);
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

/// `token issue --organization ORG --name NAME --capabilities A,B [--days N]`:
/// issues a service token in the database and prints it once.
async fn token(args: &[String]) -> Result<(), String> {
    let usage = "usage: agentux-command token issue --organization ORG --name NAME --capabilities A,B [--days N]";
    if args.first().map(String::as_str) != Some("issue") {
        return Err(usage.into());
    }
    let mut request = agentux_command::auth::TokenRequest {
        created_by: "agentux-command token issue".into(),
        ..Default::default()
    };
    let mut rest = args[1..].iter();
    while let Some(flag) = rest.next() {
        let value = rest.next().ok_or(usage)?;
        match flag.as_str() {
            "--organization" => request.organization_id = value.clone(),
            "--name" => request.name = value.clone(),
            "--capabilities" => {
                request.capabilities = value.split(',').map(|c| c.trim().to_string()).collect()
            }
            "--days" => request.days = Some(value.parse().map_err(|_| usage)?),
            _ => return Err(usage.into()),
        }
    }
    let database_url = std::env::var("AGENTUX_COMMAND_DATABASE_URL").unwrap_or_default();
    let handler = Handler::postgres(&database_url).await?;
    let issued = handler
        .service_tokens()
        .issue(request, chrono::Utc::now())
        .await
        .map_err(|e| e.to_string())?;
    eprintln!(
        "issued {} ({}) in {} with {}; it is shown once:",
        issued.token.id,
        issued.token.name,
        issued.token.organization_id,
        issued.token.capabilities.join(", ")
    );
    println!("{}", issued.secret);
    Ok(())
}
