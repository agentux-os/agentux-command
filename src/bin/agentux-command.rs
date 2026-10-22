//! The AgentUX Command server.
//!
//! `agentux-command` serves the API. `agentux-command token issue …` issues
//! an operator service token straight in the database, for the first
//! administrator (later ones come from the API):
//!
//!   agentux-command token issue --organization acme --name admin \
//!       --capabilities auth.admin,organization.read,node.enroll,node.command,approval.decide

use std::process::ExitCode;
use std::sync::Arc;

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
    if let Some(provider) = oidc_from_env(&public_url)? {
        eprintln!(
            "operators sign in with {}",
            env("AGENTUX_COMMAND_OIDC_ISSUER")
        );
        handler = handler.with_oidc(provider);
    }
    // Events go to a webhook when one is set; the outbox is kept either way.
    let events_url = env("AGENTUX_COMMAND_EVENTS_URL");
    let sink: Option<Arc<dyn agentux_command::outbox::Sink>> = if events_url.is_empty() {
        None
    } else {
        eprintln!("events are delivered to {events_url}");
        Some(Arc::new(agentux_command::outbox::Webhook::new(
            &events_url,
            std::env::var("AGENTUX_COMMAND_EVENTS_TOKEN").ok(),
        )?))
    };
    handler = handler.deliver_events(sink);
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

/// OIDC sign-in from the environment, when AGENTUX_COMMAND_OIDC_ISSUER is
/// set: the client (ID, secret), the role map (a JSON file), the groups
/// claim and the scopes. The callback is <public URL>/v1/auth/callback.
fn oidc_from_env(public_url: &str) -> Result<Option<agentux_command::oidc::Oidc>, String> {
    use agentux_command::oidc::{Config, Oidc, RoleMap};
    let env = |name: &str| std::env::var(name).unwrap_or_default();
    let issuer = env("AGENTUX_COMMAND_OIDC_ISSUER");
    if issuer.is_empty() {
        return Ok(None);
    }
    if public_url.is_empty() {
        return Err(
            "AGENTUX_COMMAND_OIDC_ISSUER needs AGENTUX_COMMAND_PUBLIC_URL (the callback address)"
                .into(),
        );
    }
    let roles_file = env("AGENTUX_COMMAND_OIDC_ROLES");
    if roles_file.is_empty() {
        return Err(
            "AGENTUX_COMMAND_OIDC_ROLES must name the role map (groups to capabilities)".into(),
        );
    }
    let roles = std::fs::read_to_string(&roles_file).map_err(|e| format!("{roles_file}: {e}"))?;
    let roles = RoleMap::parse(&roles).map_err(|e| format!("{roles_file}: {e}"))?;
    let scopes = match env("AGENTUX_COMMAND_OIDC_SCOPES") {
        s if s.is_empty() => "openid profile email groups".to_string(),
        s => s,
    };
    let groups_claim = match env("AGENTUX_COMMAND_OIDC_GROUPS_CLAIM") {
        c if c.is_empty() => "groups".to_string(),
        c => c,
    };
    let config = Config {
        issuer,
        client_id: env("AGENTUX_COMMAND_OIDC_CLIENT_ID"),
        client_secret: env("AGENTUX_COMMAND_OIDC_CLIENT_SECRET"),
        redirect_url: format!("{}/v1/auth/callback", public_url.trim_end_matches('/')),
        scopes: scopes.split_whitespace().map(str::to_string).collect(),
        groups_claim,
        roles,
    };
    Oidc::new(config).map(Some).map_err(|e| e.to_string())
}
