//! Keeping the event log bounded without losing what audit and replay need.
//!
//! Nothing is pruned unless an operator asks: the policy is to keep every
//! event, and pruning is opt-in (`agentux-command prune-events`, or
//! `AGENTUX_COMMAND_EVENT_RETENTION_DAYS` with an archive directory). When it
//! runs, for each organization and in one transaction:
//!
//! 1. the events taken before the cutoff are written to an archive file,
//!    tamper-evident like the audit export (each line chained to the one
//!    before by sha256), and flushed to disk;
//! 2. they are applied to the organization's **checkpoint**, a copy of its
//!    projection as of the last pruned event, so the projection can still be
//!    rebuilt and verified (checkpoint + the events after it);
//! 3. only then are they deleted.
//!
//! If the archive cannot be written, nothing is deleted. A node resending a
//! pruned event is still refused by its per-node sequence cursor, which
//! pruning never touches.

use std::io::Write;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use serde::Serialize;
use serde_json::{Value, json};
use tokio_postgres::types::Json;

use super::{Error, Event, Projection, apply_projection_event, empty_projection};

/// The fewest days an operator may keep: shorter than this is refused, so a
/// typo cannot empty the log.
pub const MINIMUM_RETENTION_DAYS: i64 = 30;

/// How many events one transaction prunes.
const BATCH: i64 = 10_000;

fn storage(what: &'static str) -> impl Fn(tokio_postgres::Error) -> Error {
    move |e| Error::Storage(format!("{what}: {e}"))
}

/// What a pruning pass did, per organization.
#[derive(Debug, Default, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Pruned {
    pub organization_id: String,
    pub events: u64,
    /// The archive file the events went to (none on a dry run).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archive: Option<PathBuf>,
    /// The archive's last hash: what proves the file was not changed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    /// The checkpoint now covers events up to this cursor.
    pub checkpoint: i64,
}

/// The checkpoint of `organization_id`: its projection as of the last
/// pruned event, and that event's cursor. `None` when nothing was pruned.
pub async fn checkpoint(
    client: &impl deadpool_postgres::GenericClient,
    organization_id: &str,
) -> Result<Option<(Projection, i64)>, Error> {
    let row = client
        .query_opt(
            "SELECT projection, last_ingest_id FROM office_projection_checkpoints WHERE organization_id = $1",
            &[&organization_id],
        )
        .await
        .map_err(storage("read checkpoint"))?;
    Ok(row.map(|r| (r.get::<_, Json<Projection>>(0).0, r.get(1))))
}

/// sha256 of `prev`, a newline and the compact JSON of `record`, in hex: the
/// audit export's chain.
fn chain(prev: &str, record: &Value) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(prev.as_bytes());
    hasher.update(b"\n");
    hasher.update(record.to_string().as_bytes());
    hex::encode(hasher.finalize())
}

/// Prunes the events every organization took before `cutoff`, archiving them
/// under `archive_dir/<organization>/`. With `dry_run`, only counts.
pub async fn prune(
    pool: &Pool,
    cutoff: DateTime<Utc>,
    archive_dir: &Path,
    dry_run: bool,
) -> Result<Vec<Pruned>, Error> {
    if cutoff > Utc::now() - chrono::Duration::days(MINIMUM_RETENTION_DAYS) {
        return Err(Error::Invalid(format!(
            "events are kept at least {MINIMUM_RETENTION_DAYS} days"
        )));
    }
    let client = pool.get().await?;
    let organizations: Vec<String> = client
        .query(
            "SELECT DISTINCT organization_id FROM office_events WHERE received_at < $1 ORDER BY 1",
            &[&cutoff],
        )
        .await
        .map_err(storage("list organizations to prune"))?
        .iter()
        .map(|r| r.get(0))
        .collect();
    drop(client);
    let mut report = Vec::new();
    for organization in organizations {
        let mut total = Pruned {
            organization_id: organization.clone(),
            ..Pruned::default()
        };
        loop {
            let pass = prune_batch(pool, &organization, cutoff, archive_dir, dry_run).await?;
            total.events += pass.events;
            total.checkpoint = pass.checkpoint.max(total.checkpoint);
            if pass.archive.is_some() {
                total.archive = pass.archive;
                total.digest = pass.digest;
            }
            if pass.events < BATCH as u64 || dry_run {
                break;
            }
        }
        report.push(total);
    }
    Ok(report)
}

async fn prune_batch(
    pool: &Pool,
    organization: &str,
    cutoff: DateTime<Utc>,
    archive_dir: &Path,
    dry_run: bool,
) -> Result<Pruned, Error> {
    let mut client = pool.get().await?;
    let tx = client
        .transaction()
        .await
        .map_err(storage("begin pruning"))?;
    // Appends for this organization wait: they lock the same projection row.
    tx.query_opt(
        "SELECT 1 FROM office_projections WHERE organization_id = $1 FOR UPDATE",
        &[&organization],
    )
    .await
    .map_err(storage("lock the projection"))?;
    let (mut projection, mut last) = match checkpoint(&tx, organization).await? {
        Some(found) => found,
        None => (empty_projection(organization), 0),
    };
    let rows = tx
        .query(
            "SELECT ingest_id, payload FROM office_events
             WHERE organization_id = $1 AND ingest_id > $2 AND received_at < $3
             ORDER BY ingest_id LIMIT $4",
            &[&organization, &last, &cutoff, &BATCH],
        )
        .await
        .map_err(storage("read events to prune"))?;
    let mut pruned = Pruned {
        organization_id: organization.into(),
        events: rows.len() as u64,
        checkpoint: last,
        ..Pruned::default()
    };
    if rows.is_empty() || dry_run {
        return Ok(pruned);
    }
    // Only a prefix of the log: an event after the cutoff that came before
    // one of these (a late write) stops the batch there, so the checkpoint is
    // always "everything up to N".
    let mut events: Vec<(i64, Event)> = Vec::new();
    let first: i64 = rows[0].get(0);
    let gap: Option<i64> = tx
        .query_one(
            "SELECT MIN(ingest_id) FROM office_events WHERE organization_id = $1 AND ingest_id > $2 AND received_at >= $3",
            &[&organization, &last, &cutoff],
        )
        .await
        .map_err(storage("find the first kept event"))?
        .get(0);
    for row in &rows {
        let cursor: i64 = row.get(0);
        if gap.is_some_and(|g| cursor > g) {
            break;
        }
        events.push((cursor, row.get::<_, Json<Event>>(1).0));
    }
    if events.is_empty() {
        pruned.events = 0;
        return Ok(pruned);
    }
    let up_to = events.last().map(|(c, _)| *c).unwrap_or(last);

    // 1. The archive, on disk before anything is deleted.
    let dir = archive_dir.join(sanitize(organization));
    std::fs::create_dir_all(&dir)
        .map_err(|e| Error::Storage(format!("archive directory {}: {e}", dir.display())))?;
    let path = dir.join(format!("events-{first:012}-{up_to:012}.ndjson"));
    let mut text = String::new();
    let mut prev = String::new();
    for (cursor, event) in &events {
        let mut record = json!({"cursor": cursor, "event": event, "prev": prev});
        let hash = chain(&prev, &record);
        record["hash"] = Value::String(hash.clone());
        text.push_str(&record.to_string());
        text.push('\n');
        prev = hash;
    }
    text.push_str(
        &json!({"organizationId": organization, "count": events.len(), "digest": prev, "from": first, "to": up_to, "archivedAt": Utc::now()})
            .to_string(),
    );
    text.push('\n');
    let written = (|| -> std::io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()
    })();
    written.map_err(|e| Error::Storage(format!("archive {}: {e}", path.display())))?;

    // 2. The checkpoint takes them.
    for (_, event) in &events {
        apply_projection_event(&mut projection, event)?;
    }
    last = up_to;
    tx.execute(
        "INSERT INTO office_projection_checkpoints (organization_id, last_ingest_id, projection, updated_at)
         VALUES ($1, $2, $3, now())
         ON CONFLICT (organization_id) DO UPDATE SET last_ingest_id = EXCLUDED.last_ingest_id,
           projection = EXCLUDED.projection, updated_at = now()",
        &[&organization, &last, &Json(&projection)],
    )
    .await
    .map_err(storage("save the checkpoint"))?;
    tx.execute(
        "INSERT INTO office_event_archives (organization_id, from_ingest_id, to_ingest_id, events, path, digest)
         VALUES ($1, $2, $3, $4, $5, $6)",
        &[
            &organization,
            &first,
            &up_to,
            &(events.len() as i64),
            &path.display().to_string(),
            &prev,
        ],
    )
    .await
    .map_err(storage("record the archive"))?;

    // 3. Only then, the events.
    tx.execute(
        "DELETE FROM office_events WHERE organization_id = $1 AND ingest_id >= $2 AND ingest_id <= $3",
        &[&organization, &first, &up_to],
    )
    .await
    .map_err(storage("delete pruned events"))?;
    if let Err(e) = tx.commit().await {
        // The archive stays: it holds events that were not deleted.
        return Err(Error::Storage(format!("commit pruning: {e}")));
    }
    pruned.events = events.len() as u64;
    pruned.checkpoint = last;
    pruned.archive = Some(path);
    pruned.digest = Some(prev);
    Ok(pruned)
}

/// An organization id as a directory name.
fn sanitize(organization: &str) -> String {
    organization
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || "-_.".contains(c) {
                c
            } else {
                '_'
            }
        })
        .collect()
}
