//! Versioned PostgreSQL migrations, applied in order at startup.

use tokio_postgres::Client;

use super::Error;

/// The migrations, embedded: `<version>_<name>.sql`.
const FILES: &[(&str, &str)] = &[
    (
        "001_office_store.sql",
        include_str!("../../migrations/001_office_store.sql"),
    ),
    (
        "002_node_enrollment.sql",
        include_str!("../../migrations/002_node_enrollment.sql"),
    ),
    (
        "003_service_tokens.sql",
        include_str!("../../migrations/003_service_tokens.sql"),
    ),
    (
        "004_operator_sessions.sql",
        include_str!("../../migrations/004_operator_sessions.sql"),
    ),
    (
        "005_node_state.sql",
        include_str!("../../migrations/005_node_state.sql"),
    ),
    (
        "006_organization_definitions.sql",
        include_str!("../../migrations/006_organization_definitions.sql"),
    ),
    (
        "007_event_outbox.sql",
        include_str!("../../migrations/007_event_outbox.sql"),
    ),
    (
        "008_mission_runs.sql",
        include_str!("../../migrations/008_mission_runs.sql"),
    ),
    (
        "009_definition_reviews.sql",
        include_str!("../../migrations/009_definition_reviews.sql"),
    ),
    (
        "010_event_retention.sql",
        include_str!("../../migrations/010_event_retention.sql"),
    ),
    (
        "011_mission_schedules.sql",
        include_str!("../../migrations/011_mission_schedules.sql"),
    ),
    (
        "012_execution_activity_replay.sql",
        include_str!("../../migrations/012_execution_activity_replay.sql"),
    ),
    (
        "013_node_event_replay.sql",
        include_str!("../../migrations/013_node_event_replay.sql"),
    ),
    (
        "014_safe_legacy_node_event_cursor.sql",
        include_str!("../../migrations/014_safe_legacy_node_event_cursor.sql"),
    ),
];

pub(crate) struct Migration {
    pub version: i32,
    pub name: &'static str,
    pub sql: &'static str,
}

pub(crate) fn load() -> Result<Vec<Migration>, String> {
    let mut migrations = Vec::new();
    for (name, sql) in FILES {
        let (prefix, _) = name.split_once('_').ok_or_else(|| {
            format!("migration filename {name:?} must start with a numeric version")
        })?;
        let version: i32 = prefix
            .parse()
            .ok()
            .filter(|v| *v >= 1)
            .ok_or_else(|| format!("migration filename {name:?} has an invalid version"))?;
        migrations.push(Migration { version, name, sql });
    }
    migrations.sort_by_key(|m| m.version);
    if let Some(pair) = migrations.windows(2).find(|w| w[0].version == w[1].version) {
        return Err(format!("duplicate migration version {}", pair[0].version));
    }
    Ok(migrations)
}

pub fn migration_versions() -> Vec<i32> {
    load()
        .map(|m| m.iter().map(|m| m.version).collect())
        .unwrap_or_default()
}

/// Applies the migrations not applied yet, each in its own transaction.
pub async fn migrate(client: &mut Client) -> Result<(), Error> {
    let migrations = load().map_err(|e| Error::Storage(format!("load migrations: {e}")))?;
    client
        .execute(
            "CREATE TABLE IF NOT EXISTS command_schema_migrations (version integer PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())",
            &[],
        )
        .await
        .map_err(|e| Error::Storage(format!("create migration table: {e}")))?;
    for item in migrations {
        let fail = |what: &str, e: tokio_postgres::Error| {
            let diagnostic = e
                .as_db_error()
                .map(|db| format!("{}: {}", db.code().code(), db.message()))
                .unwrap_or_else(|| e.to_string());
            Error::Storage(format!(
                "{what} migration {} ({}): {diagnostic}",
                item.version, item.name
            ))
        };
        let tx = client.transaction().await.map_err(|e| fail("begin", e))?;
        tx.execute("SELECT pg_advisory_xact_lock(842119)", &[])
            .await
            .map_err(|e| fail("lock", e))?;
        let applied: bool = tx
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM command_schema_migrations WHERE version = $1)",
                &[&item.version],
            )
            .await
            .map_err(|e| fail("read", e))?
            .get(0);
        if !applied {
            for statement in item.sql.split(';').map(str::trim).filter(|s| !s.is_empty()) {
                tx.execute(statement, &[])
                    .await
                    .map_err(|e| fail("execute", e))?;
            }
            tx.execute(
                "INSERT INTO command_schema_migrations (version, name) VALUES ($1, $2)",
                &[&item.version, &item.name],
            )
            .await
            .map_err(|e| fail("record", e))?;
        }
        tx.commit().await.map_err(|e| fail("commit", e))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_are_ordered_and_start_at_one() {
        assert_eq!(
            migration_versions(),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
        );
    }

    #[test]
    fn legacy_replay_cursors_preserve_prior_ack_and_record_unverified_history() {
        let migrations = load().unwrap();
        let cursor_safety = migrations
            .iter()
            .find(|migration| migration.version == 14)
            .unwrap();
        assert!(
            cursor_safety
                .sql
                .contains("verified_sequence = prefix.verified_prefix")
        );
        assert!(!cursor_safety.sql.contains("SET contiguous_sequence"));
        assert!(cursor_safety.sql.contains("history_gap_through_sequence"));
    }

    #[test]
    fn activity_replay_has_a_tenant_run_cursor_index() {
        let migrations = load().unwrap();
        let activity = migrations
            .iter()
            .find(|migration| migration.version == 12)
            .unwrap();
        assert!(activity.sql.contains("office_events_activity_run_idx"));
        assert!(
            activity
                .sql
                .contains("organization_id, (payload->>'runId'), ingest_id")
        );
        assert!(
            activity
                .sql
                .contains("payload->>'eventType' = 'execution.activity'")
        );
        let statements = activity
            .sql
            .split(';')
            .map(str::trim)
            .filter(|statement| !statement.is_empty())
            .collect::<Vec<_>>();
        assert_eq!(statements.len(), 1, "migration runner splits on semicolons");
        let sql = statements[0]
            .lines()
            .filter(|line| !line.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(sql.trim_start().starts_with("CREATE INDEX"));
    }

    #[test]
    fn node_event_replay_has_a_durable_identity_ledger_and_cursor() {
        let migrations = load().unwrap();
        let replay = migrations
            .iter()
            .find(|migration| migration.version == 13)
            .unwrap();
        assert!(replay.sql.contains("command_node_event_cursors"));
        assert!(replay.sql.contains("command_node_events"));
        assert!(replay.sql.contains("command_node_capabilities"));
        assert!(replay.sql.contains("payload_sha256"));
        assert!(
            replay
                .sql
                .contains("UNIQUE (organization_id, node_id, event_id)")
        );
        assert!(
            replay
                .sql
                .lines()
                .all(|line| { !line.trim_start().starts_with("--") || !line.contains(';') }),
            "migration runner splits SQL on semicolons, including comment text"
        );
    }

    #[test]
    fn the_event_log_is_partitioned_by_organization_and_indexed_by_time() {
        let migrations = load().unwrap();
        assert!(
            migrations[0]
                .sql
                .contains("PARTITION BY HASH (organization_id)")
        );
        assert!(migrations[0].sql.contains("office_events_org_received_idx"));
    }
}
