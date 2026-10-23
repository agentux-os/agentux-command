//! The AgentUX Command server.
//!
//! `agentux-command` serves the API. `agentux-command token issue …` issues
//! an operator service token straight in the database, for the first
//! administrator (later ones come from the API):
//!
//!   agentux-command token issue --organization acme --name admin \
//!       --capabilities auth.admin,organization.read,node.enroll,node.command,approval.decide
//!
//! `agentux-command check` verifies the database (after a restore): every
//! organization's projection against its event log, and what it holds.

use std::process::ExitCode;
use std::sync::Arc;

use agentux_command::server::{Handler, parse_node_tokens};

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("token") => token(&args[1..]).await,
        Some("check") => check().await,
        Some("prune-events") => prune_events(&args[1..]).await,
        Some(other) => Err(format!(
            "unknown command {other:?} (try: token issue, check, prune-events)"
        )),
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
    // AgentUX Office at /office/, from an agentux-office bundle (the image
    // ships one).
    let office_dir = env("AGENTUX_COMMAND_OFFICE_DIR");
    if !office_dir.is_empty() {
        let dir = std::path::PathBuf::from(&office_dir);
        if !dir.join("index.html").is_file() {
            return Err(format!(
                "AGENTUX_COMMAND_OFFICE_DIR {office_dir} has no index.html"
            ));
        }
        eprintln!("AgentUX Office is served at /office/ from {office_dir}");
        handler = handler.with_office_dir(dir, &env("AGENTUX_COMMAND_OFFICE_ORGANIZATION"));
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
    // Event retention, when an operator turned it on: once a day, archive and
    // prune what is older than the retention (ADR-free default: keep all).
    let retention = env("AGENTUX_COMMAND_EVENT_RETENTION_DAYS");
    if !retention.is_empty() {
        let days: i64 = retention
            .parse()
            .map_err(|_| "AGENTUX_COMMAND_EVENT_RETENTION_DAYS is a number of days".to_string())?;
        let archive = env("AGENTUX_COMMAND_EVENT_ARCHIVE_DIR");
        if archive.is_empty() {
            return Err("AGENTUX_COMMAND_EVENT_RETENTION_DAYS needs AGENTUX_COMMAND_EVENT_ARCHIVE_DIR: pruned events are archived first".into());
        }
        if days < agentux_command::office::retention::MINIMUM_RETENTION_DAYS {
            return Err(format!(
                "AGENTUX_COMMAND_EVENT_RETENTION_DAYS must be at least {}",
                agentux_command::office::retention::MINIMUM_RETENTION_DAYS
            ));
        }
        let pool = agentux_command::office::open_postgres(&database_url)
            .await
            .map_err(|e| e.to_string())?;
        eprintln!("events older than {days} days are archived to {archive} and pruned daily");
        tokio::spawn(async move {
            loop {
                let cutoff = chrono::Utc::now() - chrono::Duration::days(days);
                match agentux_command::office::retention::prune(
                    &pool,
                    cutoff,
                    std::path::Path::new(&archive),
                    false,
                )
                .await
                {
                    Ok(report) => {
                        let events: u64 = report.iter().map(|r| r.events).sum();
                        if events > 0 {
                            eprintln!(
                                "agentux-command: archived and pruned {events} events older than {cutoff}"
                            );
                        }
                    }
                    Err(e) => eprintln!("agentux-command: event retention: {e}"),
                }
                tokio::time::sleep(std::time::Duration::from_secs(24 * 3600)).await;
            }
        });
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

/// `check`: connects to the database (migrating it if needed), rebuilds
/// every organization's Office projection from its event log and compares
/// it with the stored one, and prints what the database holds as JSON. Exits
/// non-zero if a projection differs: run it after a restore.
async fn check() -> Result<(), String> {
    use agentux_command::office::{self, PostgresStore};
    let database_url = std::env::var("AGENTUX_COMMAND_DATABASE_URL").unwrap_or_default();
    let pool = office::open_postgres(&database_url)
        .await
        .map_err(|e| e.to_string())?;
    let client = pool.get().await.map_err(|e| e.to_string())?;
    let count = |table: &'static str| {
        let client = &client;
        async move {
            client
                .query_one(&format!("SELECT count(*) FROM {table}"), &[])
                .await
                .map(|row| row.get::<_, i64>(0))
                .map_err(|e| format!("count {table}: {e}"))
        }
    };
    let organizations: Vec<String> = client
        .query(
            "SELECT organization_id FROM office_projections ORDER BY organization_id",
            &[],
        )
        .await
        .map_err(|e| e.to_string())?
        .iter()
        .map(|row| row.get(0))
        .collect();
    let store = PostgresStore::new(pool.clone());
    let mut differing = Vec::new();
    for organization in &organizations {
        if let Err(e) = store.verify_projection(organization).await {
            differing.push(format!("{organization}: {e}"));
        }
    }
    let migration: i32 = client
        .query_one(
            "SELECT COALESCE(MAX(version), 0) FROM command_schema_migrations",
            &[],
        )
        .await
        .map_err(|e| e.to_string())?
        .get(0);
    let report = serde_json::json!({
        "migration": migration,
        "organizations": organizations.len(),
        "events": count("office_events").await?,
        "definitions": count("organization_definitions").await?,
        "runs": count("mission_runs").await?,
        "nodes": count("node_state").await?,
        "enrolledNodes": count("node_credentials").await?,
        "serviceTokens": count("service_tokens").await?,
        "projectionsVerified": differing.is_empty(),
    });
    println!("{report}");
    if differing.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "projections differ from their event log: {}",
            differing.join("; ")
        ))
    }
}

/// `prune-events --older-than-days N --archive DIR [--dry-run]`: archives
/// and deletes the events every organization took more than N days ago
/// (at least 30), keeping each projection verifiable through its checkpoint.
/// Prints what it did as JSON. Nothing is pruned unless this runs.
async fn prune_events(args: &[String]) -> Result<(), String> {
    let usage = "usage: agentux-command prune-events --older-than-days N --archive DIR [--dry-run]";
    let (mut days, mut archive, mut dry_run) = (None::<i64>, None::<String>, false);
    let mut rest = args.iter();
    while let Some(flag) = rest.next() {
        match flag.as_str() {
            "--older-than-days" => {
                days = Some(rest.next().and_then(|v| v.parse().ok()).ok_or(usage)?)
            }
            "--archive" => archive = Some(rest.next().ok_or(usage)?.clone()),
            "--dry-run" => dry_run = true,
            _ => return Err(usage.into()),
        }
    }
    let (Some(days), Some(archive)) = (days, archive) else {
        return Err(usage.into());
    };
    let database_url = std::env::var("AGENTUX_COMMAND_DATABASE_URL").unwrap_or_default();
    let pool = agentux_command::office::open_postgres(&database_url)
        .await
        .map_err(|e| e.to_string())?;
    let cutoff = chrono::Utc::now() - chrono::Duration::days(days);
    let report = agentux_command::office::retention::prune(
        &pool,
        cutoff,
        std::path::Path::new(&archive),
        dry_run,
    )
    .await
    .map_err(|e| e.to_string())?;
    println!(
        "{}",
        serde_json::json!({"cutoff": cutoff, "dryRun": dry_run, "organizations": report})
    );
    Ok(())
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
