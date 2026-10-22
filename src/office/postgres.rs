//! The read model in PostgreSQL: an append-only event log, per-node cursors,
//! the materialized projection and approval idempotency, updated together.

use std::str::FromStr;

use async_trait::async_trait;
use deadpool_postgres::GenericClient;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio_postgres::NoTls;
use tokio_postgres::types::Json;

use super::{
    Error, Event, OfficeRepository, Projection, ResolutionRequest, ResolutionResponse,
    apply_projection_event, empty_projection, migrate, validate_event, validate_resolution,
};
use crate::now_rfc3339;

fn storage(what: &str) -> impl Fn(tokio_postgres::Error) -> Error + '_ {
    move |e| Error::Storage(format!("{what}: {e}"))
}

impl From<deadpool_postgres::PoolError> for Error {
    fn from(e: deadpool_postgres::PoolError) -> Self {
        Error::Storage(format!("database connection: {e}"))
    }
}

/// A connection pool for `config` (no TLS: Command reaches its database on
/// the pod's own network).
pub fn pool(config: tokio_postgres::Config) -> Pool {
    let manager = Manager::from_config(
        config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    Pool::builder(manager)
        .max_size(20)
        .build()
        .expect("a pool without a runtime-specific setting")
}

/// Connects to `database_url` and applies the migrations.
pub async fn open_postgres(database_url: &str) -> Result<Pool, Error> {
    if database_url.trim().is_empty() {
        return Err(Error::Invalid(
            "AGENTUX_COMMAND_DATABASE_URL is required".into(),
        ));
    }
    let config = tokio_postgres::Config::from_str(database_url).map_err(|_| {
        Error::Invalid("AGENTUX_COMMAND_DATABASE_URL is not a PostgreSQL URL".into())
    })?;
    let pool = pool(config);
    let mut client = pool
        .get()
        .await
        .map_err(|e| Error::Storage(format!("connect to PostgreSQL: {e}")))?;
    migrate(&mut client)
        .await
        .map_err(|e| Error::Storage(format!("migrate PostgreSQL database: {e}")))?;
    drop(client);
    Ok(pool)
}

pub struct PostgresStore {
    pool: Pool,
}

impl PostgresStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    /// Rebuilds an organization's projection from its event log and checks
    /// it matches the stored one.
    pub async fn verify_projection(&self, organization_id: &str) -> Result<(), Error> {
        let stored = self
            .get_projection(organization_id)
            .await?
            .ok_or_else(|| Error::conflict("not_found", "projection not found"))?;
        let mut rebuilt = empty_projection(organization_id);
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT payload FROM office_events WHERE organization_id=$1 ORDER BY ingest_id",
                &[&organization_id],
            )
            .await
            .map_err(storage("read event log for replay"))?;
        for row in rows {
            let Json(event): Json<Event> = row.get(0);
            apply_projection_event(&mut rebuilt, &event)?;
        }
        if stored != rebuilt {
            return Err(Error::Invalid(format!(
                "materialized projection for organization {organization_id} differs from event replay"
            )));
        }
        Ok(())
    }
}

async fn save_projection(
    client: &impl GenericClient,
    projection: &Projection,
) -> Result<(), Error> {
    client
        .execute(
            "UPDATE office_projections SET schema_version=$2,definition_version=$3,projected_at=$4::text::timestamptz,source=$5,freshness=$6,last_event_id=$7,last_node_id=$8,last_sequence=$9,payload=$10 WHERE organization_id=$1",
            &[
                &projection.organization_id,
                &projection.schema_version,
                &projection.definition_version,
                &projection.projected_at,
                &projection.source,
                &projection.freshness,
                &projection.last_event_id,
                &projection.last_node_id,
                &projection.last_sequence,
                &Json(projection),
            ],
        )
        .await
        .map_err(storage("save office projection"))?;
    Ok(())
}

#[async_trait]
impl OfficeRepository for PostgresStore {
    async fn ping(&self) -> Result<(), Error> {
        let client = self.pool.get().await?;
        client
            .simple_query("SELECT 1")
            .await
            .map_err(storage("ping"))?;
        Ok(())
    }

    async fn get_projection(&self, organization_id: &str) -> Result<Option<Projection>, Error> {
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                "SELECT payload FROM office_projections WHERE organization_id = $1",
                &[&organization_id],
            )
            .await
            .map_err(storage("load office projection"))?;
        Ok(row.map(|row| row.get::<_, Json<Projection>>(0).0))
    }

    async fn apply_event(&self, mut event: Event) -> Result<(), Error> {
        validate_event(&event)?;
        if event.received_at.is_empty() {
            event.received_at = now_rfc3339();
        }
        let mut client = self.pool.get().await?;
        let tx = client
            .transaction()
            .await
            .map_err(storage("begin event transaction"))?;
        let initial = empty_projection(&event.organization_id);
        tx.execute(
            "INSERT INTO office_projections (organization_id, schema_version, definition_version, projected_at, source, freshness, last_event_id, last_node_id, last_sequence, payload) VALUES ($1,$2,$3,$4::text::timestamptz,$5,$6,$7,$8,$9,$10) ON CONFLICT (organization_id) DO NOTHING",
            &[
                &initial.organization_id,
                &initial.schema_version,
                &initial.definition_version,
                &event.received_at,
                &initial.source,
                &initial.freshness,
                &initial.last_event_id,
                &initial.last_node_id,
                &initial.last_sequence,
                &Json(&initial),
            ],
        )
        .await
        .map_err(storage("initialize office projection"))?;
        let Json(mut projection): Json<Projection> = tx
            .query_one(
                "SELECT payload FROM office_projections WHERE organization_id = $1 FOR UPDATE",
                &[&event.organization_id],
            )
            .await
            .map_err(storage("lock office projection"))?
            .get(0);
        let exists: bool = tx
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM office_events WHERE organization_id = $1 AND event_id = $2)",
                &[&event.organization_id, &event.event_id],
            )
            .await
            .map_err(storage("check duplicate office event"))?
            .get(0);
        if exists {
            tx.commit().await.map_err(storage("commit"))?;
            return Ok(());
        }
        let last: i64 = tx
            .query_opt(
                "SELECT sequence FROM office_event_cursors WHERE organization_id = $1 AND node_id = $2 FOR UPDATE",
                &[&event.organization_id, &event.node_id],
            )
            .await
            .map_err(storage("read office event cursor"))?
            .map(|row| row.get(0))
            .unwrap_or(0);
        if event.sequence <= last {
            return Err(Error::conflict(
                "stale_version",
                format!(
                    "event sequence {} is not after {last} for organization {} node {}",
                    event.sequence, event.organization_id, event.node_id
                ),
            ));
        }
        tx.execute(
            "INSERT INTO office_events (organization_id, event_id, node_id, sequence, payload) VALUES ($1,$2,$3,$4,$5)",
            &[&event.organization_id, &event.event_id, &event.node_id, &event.sequence, &Json(&event)],
        )
        .await
        .map_err(storage("append office event"))?;
        tx.execute(
            "INSERT INTO office_event_cursors (organization_id, node_id, sequence, last_event_id) VALUES ($1,$2,$3,$4) ON CONFLICT (organization_id,node_id) DO UPDATE SET sequence=EXCLUDED.sequence, last_event_id=EXCLUDED.last_event_id, updated_at=now()",
            &[&event.organization_id, &event.node_id, &event.sequence, &event.event_id],
        )
        .await
        .map_err(storage("advance office event cursor"))?;
        apply_projection_event(&mut projection, &event)?;
        if event.event_type == "approval.requested" {
            let approval = event.data.clone().unwrap_or(Value::Null);
            let approval_id = approval
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            tx.execute(
                "INSERT INTO office_approval_state (organization_id, approval_id, status, payload) VALUES ($1,$2,'pending',$3) ON CONFLICT (organization_id,approval_id) DO NOTHING",
                &[&event.organization_id, &approval_id, &Json(&approval)],
            )
            .await
            .map_err(storage("record requested approval"))?;
        }
        save_projection(&tx, &projection).await?;
        tx.commit().await.map_err(storage("commit office event"))?;
        Ok(())
    }

    async fn resolve_approval(
        &self,
        approval_id: &str,
        request: ResolutionRequest,
    ) -> Result<ResolutionResponse, Error> {
        validate_resolution(&request)?;
        let org = &request.organization_id;
        let mut client = self.pool.get().await?;
        let tx = client
            .transaction()
            .await
            .map_err(storage("begin approval transaction"))?;
        let Json(mut projection): Json<Projection> = tx
            .query_opt(
                "SELECT payload FROM office_projections WHERE organization_id=$1 FOR UPDATE",
                &[org],
            )
            .await
            .map_err(storage("lock approval projection"))?
            .ok_or_else(|| Error::conflict("not_found", "approval not found"))?
            .get(0);
        let state = tx
            .query_opt(
                "SELECT status,payload FROM office_approval_state WHERE organization_id=$1 AND approval_id=$2 FOR UPDATE",
                &[org, &approval_id],
            )
            .await
            .map_err(storage("lock approval state"))?
            .ok_or_else(|| Error::conflict("not_found", "approval not found"))?;
        let status: String = state.get(0);
        let Json(mut approval): Json<Value> = state.get(1);
        let previous = tx
            .query_opt(
                "SELECT response FROM office_approval_idempotency WHERE organization_id=$1 AND approval_id=$2 AND idempotency_key=$3",
                &[org, &approval_id, &request.idempotency_key],
            )
            .await
            .map_err(storage("read approval idempotency record"))?;
        if let Some(row) = previous {
            let Json(mut response): Json<ResolutionResponse> = row.get(0);
            response.idempotent_replay = true;
            tx.commit()
                .await
                .map_err(storage("finish approval replay"))?;
            return Ok(response);
        }
        if status != "pending" {
            return Err(Error::conflict(
                "already_decided",
                "approval already decided",
            ));
        }
        let response = ResolutionResponse {
            ok: true,
            organization_id: org.clone(),
            approval_id: approval_id.to_string(),
            decision: request.decision.clone(),
            idempotency_key: request.idempotency_key.clone(),
            idempotent_replay: false,
        };
        tx.execute(
            "INSERT INTO office_approval_idempotency (organization_id,approval_id,idempotency_key,response) VALUES ($1,$2,$3,$4)",
            &[org, &approval_id, &request.idempotency_key, &Json(&response)],
        )
        .await
        .map_err(storage("persist approval idempotency response"))?;
        let decision = json!({"decision": request.decision, "reason": request.reason, "idempotencyKey": request.idempotency_key});
        tx.execute(
            "UPDATE office_approval_state SET status=$3, decision=$4, updated_at=now() WHERE organization_id=$1 AND approval_id=$2",
            &[org, &approval_id, &request.decision, &Json(&decision)],
        )
        .await
        .map_err(storage("persist approval decision"))?;
        let command_sequence: i64 = tx
            .query_opt(
                "SELECT sequence FROM office_event_cursors WHERE organization_id=$1 AND node_id='command' FOR UPDATE",
                &[org],
            )
            .await
            .map_err(storage("read command event cursor"))?
            .map(|row| row.get(0))
            .unwrap_or(0)
            + 1;
        let identity =
            Sha256::digest(format!("{org}\0{approval_id}\0{}", request.idempotency_key).as_bytes());
        let now = now_rfc3339();
        let resolution = Event {
            event_id: format!("approval-resolved-{}", hex::encode(identity)),
            event_type: "approval.resolved".into(),
            organization_id: org.clone(),
            node_id: "command".into(),
            occurred_at: now.clone(),
            received_at: now,
            sequence: command_sequence,
            data: Some(
                json!({"approvalId": approval_id, "decision": request.decision, "reason": request.reason, "idempotencyKey": request.idempotency_key}),
            ),
            ..Event::default()
        };
        apply_projection_event(&mut projection, &resolution)?;
        tx.execute(
            "INSERT INTO office_events (organization_id,event_id,node_id,sequence,payload) VALUES ($1,$2,$3,$4,$5)",
            &[org, &resolution.event_id, &resolution.node_id, &resolution.sequence, &Json(&resolution)],
        )
        .await
        .map_err(storage("append approval resolution event"))?;
        tx.execute(
            "INSERT INTO office_event_cursors (organization_id,node_id,sequence,last_event_id) VALUES ($1,$2,$3,$4) ON CONFLICT (organization_id,node_id) DO UPDATE SET sequence=EXCLUDED.sequence,last_event_id=EXCLUDED.last_event_id,updated_at=now()",
            &[org, &resolution.node_id, &resolution.sequence, &resolution.event_id],
        )
        .await
        .map_err(storage("advance approval event cursor"))?;
        if let Some(object) = approval.as_object_mut() {
            object.insert("status".into(), Value::String(request.decision.clone()));
        }
        tx.execute(
            "UPDATE office_approval_state SET payload=$3 WHERE organization_id=$1 AND approval_id=$2",
            &[org, &approval_id, &Json(&approval)],
        )
        .await
        .map_err(storage("update approval payload"))?;
        save_projection(&tx, &projection).await?;
        tx.commit()
            .await
            .map_err(storage("commit approval decision"))?;
        Ok(response)
    }
}
