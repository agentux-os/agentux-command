//! Delivers the events Command takes to a sink, from the outbox the event
//! log writes in the same transaction (ADR 0020, migration 007).
//!
//! At least once: an event is marked delivered only after the sink took it,
//! so a crash between the two sends it again, and consumers deduplicate by
//! `eventId` (the webhook carries it as `Idempotency-Key`). In order per
//! organization: an organization's next event waits while an earlier one is
//! failing. A failed delivery is retried with a backoff (5 s doubling up to
//! 10 min) kept in the database, so retries survive a restart. Delivered
//! events leave the outbox after a day; one no sink took within 7 days is
//! dropped from it (it stays in the event log and in replay).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use deadpool_postgres::Pool;
use serde_json::Value;
use tokio_postgres::types::Json;

/// Where events go.
#[async_trait]
pub trait Sink: Send + Sync {
    /// Takes one event; an error leaves it for a retry.
    async fn deliver(&self, event: &Value) -> Result<(), String>;
}

/// POSTs each event as JSON to a URL; any 2xx is delivered.
pub struct Webhook {
    client: reqwest::Client,
    url: String,
    token: Option<String>,
}

impl Webhook {
    /// `token`, when set, goes as `Authorization: Bearer`.
    pub fn new(url: &str, token: Option<String>) -> Result<Self, String> {
        let parsed = url::Url::parse(url).map_err(|e| format!("events URL {url:?}: {e}"))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(format!("events URL {url:?} is not http(s)"));
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            client,
            url: url.to_string(),
            token: token.filter(|t| !t.is_empty()),
        })
    }
}

#[async_trait]
impl Sink for Webhook {
    async fn deliver(&self, event: &Value) -> Result<(), String> {
        let mut request = self.client.post(&self.url).json(event);
        if let Some(id) = event.get("eventId").and_then(Value::as_str) {
            request = request.header("Idempotency-Key", id);
        }
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.map_err(|e| e.to_string())?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(format!("the sink answered {}", response.status()))
        }
    }
}

/// What one pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Pass {
    pub delivered: usize,
    pub failed: usize,
}

/// Sends due outbox rows to the sink.
pub struct Delivery {
    pool: Pool,
    sink: Option<Arc<dyn Sink>>,
    batch: i64,
}

impl Delivery {
    /// Without a sink nothing is sent, and the outbox is only kept bounded.
    pub fn new(pool: Pool, sink: Option<Arc<dyn Sink>>) -> Self {
        Self {
            pool,
            sink,
            batch: 50,
        }
    }

    /// One pass: prunes, then sends what is due, oldest first. One Command
    /// instance at a time does it (an advisory lock per schema); another
    /// finds the lock taken and does nothing.
    pub async fn run_once(&self) -> Result<Pass, String> {
        let mut client = self.pool.get().await.map_err(|e| e.to_string())?;
        let tx = client.transaction().await.map_err(|e| e.to_string())?;
        let locked: bool = tx
            .query_one(
                "SELECT pg_try_advisory_xact_lock(842122, hashtext(current_schema()))",
                &[],
            )
            .await
            .map_err(|e| e.to_string())?
            .get(0);
        if !locked {
            return Ok(Pass::default());
        }
        tx.execute(
            "DELETE FROM office_outbox WHERE delivered_at < now() - interval '1 day' OR created_at < now() - interval '7 days'",
            &[],
        )
        .await
        .map_err(|e| e.to_string())?;
        let mut pass = Pass::default();
        if let Some(sink) = &self.sink {
            // Due rows whose organization has no earlier row still waiting.
            let rows = tx
                .query(
                    "SELECT outbox_id, organization_id, payload FROM office_outbox o
                     WHERE delivered_at IS NULL AND next_attempt_at <= now()
                       AND NOT EXISTS (
                         SELECT 1 FROM office_outbox e
                         WHERE e.organization_id = o.organization_id AND e.delivered_at IS NULL
                           AND e.outbox_id < o.outbox_id AND e.next_attempt_at > now())
                     ORDER BY outbox_id LIMIT $1",
                    &[&self.batch],
                )
                .await
                .map_err(|e| e.to_string())?;
            let mut stopped = HashSet::new();
            for row in rows {
                let id: i64 = row.get(0);
                let organization: String = row.get(1);
                if stopped.contains(&organization) {
                    continue;
                }
                let Json(event): Json<Value> = row.get(2);
                match sink.deliver(&event).await {
                    Ok(()) => {
                        tx.execute(
                            "UPDATE office_outbox SET delivered_at = now(), attempts = attempts + 1, last_error = NULL WHERE outbox_id = $1",
                            &[&id],
                        )
                        .await
                        .map_err(|e| e.to_string())?;
                        pass.delivered += 1;
                    }
                    Err(error) => {
                        tx.execute(
                            "UPDATE office_outbox SET attempts = attempts + 1, last_error = $2,
                               next_attempt_at = now() + make_interval(secs => LEAST(5 * power(2, LEAST(attempts, 10)), 600))
                             WHERE outbox_id = $1",
                            &[&id, &error],
                        )
                        .await
                        .map_err(|e| e.to_string())?;
                        pass.failed += 1;
                        stopped.insert(organization);
                    }
                }
            }
        }
        tx.commit().await.map_err(|e| e.to_string())?;
        Ok(pass)
    }

    /// Runs a pass every `every` until the process ends.
    pub fn spawn(self, every: Duration) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut failing = false;
            loop {
                match self.run_once().await {
                    Ok(_) => failing = false,
                    Err(e) if !failing => {
                        eprintln!("agentux-command: event delivery: {e}");
                        failing = true;
                    }
                    Err(_) => {}
                }
                tokio::time::sleep(every).await;
            }
        })
    }
}
