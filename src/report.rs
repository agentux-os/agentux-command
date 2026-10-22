//! `agentux-command-storage-report`: the event log's size and growth, as one
//! JSON object on stdout. Read-only.

use std::io::Write;
use std::str::FromStr;

use tokio_postgres::NoTls;

use crate::office::collect_growth_report;

const USAGE: &str = "usage: agentux-command-storage-report [--detailed]\n  --detailed  include exact time bounds and per-organization aggregates (may scan the event log)\n";

/// Runs the report; the exit status. Nothing reaches `stdout` unless the
/// whole report does, and errors never repeat the database URL.
pub async fn run(
    args: &[String],
    database_url: &str,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> i32 {
    let mut detailed = false;
    for arg in args {
        match arg.as_str() {
            "--detailed" | "-detailed" => detailed = true,
            "-h" | "--help" | "-help" => {
                let _ = stderr.write_all(USAGE.as_bytes());
                return 2;
            }
            other if other.starts_with('-') => {
                let _ = writeln!(stderr, "flag provided but not defined: {other}");
                let _ = stderr.write_all(USAGE.as_bytes());
                return 2;
            }
            _ => {
                let _ = writeln!(stderr, "unexpected arguments: {args:?}");
                return 2;
            }
        }
    }
    if database_url.trim().is_empty() {
        let _ = writeln!(stderr, "AGENTUX_COMMAND_DATABASE_URL is required");
        return 2;
    }
    let valid = url::Url::parse(database_url).ok().filter(|u| {
        matches!(u.scheme(), "postgres" | "postgresql")
            && u.host_str().is_some_and(|h| !h.is_empty())
    });
    let config = valid.and_then(|_| tokio_postgres::Config::from_str(database_url).ok());
    let Some(config) = config else {
        let _ = writeln!(stderr, "AGENTUX_COMMAND_DATABASE_URL is invalid");
        return 2;
    };
    let (mut client, connection) = match config.connect(NoTls).await {
        Ok(pair) => pair,
        Err(e) => {
            let _ = writeln!(stderr, "connect to PostgreSQL: {}", detail(&e));
            return 1;
        }
    };
    let connection = tokio::spawn(connection);
    let report = collect_growth_report(&mut client, detailed).await;
    drop(client);
    let _ = connection.await;
    let report = match report {
        Ok(report) => report,
        Err(e) => {
            let _ = writeln!(stderr, "collect Office event growth report: {e}");
            return 1;
        }
    };
    let encoded = match serde_json::to_string(&report) {
        Ok(encoded) => encoded,
        Err(e) => {
            let _ = writeln!(stderr, "encode Office event growth report: {e}");
            return 1;
        }
    };
    if let Err(e) = writeln!(stdout, "{encoded}") {
        let _ = writeln!(stderr, "write Office event growth report: {e}");
        return 1;
    }
    0
}

/// The server's own message when there is one ("db error" says little).
fn detail(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{e}: {}", db.message()),
        None => e.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn report(args: &[&str], url: &str) -> (i32, String, String) {
        let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let code = run(&args, url, &mut out, &mut err).await;
        (
            code,
            String::from_utf8(out).unwrap(),
            String::from_utf8(err).unwrap(),
        )
    }

    #[tokio::test]
    async fn needs_a_database_url() {
        let (code, out, err) = report(&[], "").await;
        assert_ne!(code, 0);
        assert!(
            out.is_empty() && err.contains("AGENTUX_COMMAND_DATABASE_URL"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn refuses_a_malformed_url_without_repeating_it() {
        let (code, out, err) = report(&[], "postgres://private-user:private-password@[").await;
        assert_ne!(code, 0);
        assert!(out.is_empty() && !err.contains("private-password"), "{err}");
    }

    #[tokio::test]
    async fn refuses_a_keyword_dsn_before_connecting() {
        let (code, out, err) = report(
            &[],
            "host=localhost user=private-user password=private-password",
        )
        .await;
        assert_eq!(code, 2, "{err}");
        assert!(out.is_empty() && !err.contains("private-password"), "{err}");
    }

    #[tokio::test]
    async fn refuses_unknown_flags() {
        let (code, out, _) = report(&["--everything"], "postgres://localhost/x").await;
        assert_eq!(code, 2);
        assert!(out.is_empty());
    }
}
