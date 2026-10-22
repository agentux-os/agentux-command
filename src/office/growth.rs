//! A read-only report of the event log's size and growth.

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio_postgres::{Client, IsolationLevel};

use super::Error;

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GrowthReport {
    pub database: String,
    pub schema: String,
    pub collected_at: DateTime<Utc>,
    pub total_event_bytes: i64,
    pub estimated_event_rows: i64,
    pub partitions: Vec<PartitionGrowth>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detailed: Option<DetailedGrowth>,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionGrowth {
    pub name: String,
    pub bytes: i64,
    pub estimated_rows: i64,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DetailedGrowth {
    pub oldest_received_at: Option<DateTime<Utc>>,
    pub newest_received_at: Option<DateTime<Utc>>,
    pub organizations: Vec<OrganizationGrowth>,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrganizationGrowth {
    pub organization_id: String,
    pub event_count: i64,
    pub oldest_received_at: Option<DateTime<Utc>>,
    pub newest_received_at: Option<DateTime<Utc>>,
}

/// Reports the event storage without changing it. `detailed` aggregates
/// over the whole append-only event table.
pub async fn collect_growth_report(
    client: &mut Client,
    detailed: bool,
) -> Result<GrowthReport, Error> {
    let fail =
        |what: &'static str| move |e: tokio_postgres::Error| Error::Storage(format!("{what}: {e}"));
    let tx = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await
        .map_err(fail("begin read-only growth report transaction"))?;
    let summary = tx
        .query_one(
            "SELECT current_database()::text, current_schema()::text, clock_timestamp()",
            &[],
        )
        .await
        .map_err(fail("read event storage summary"))?;
    // current_schema() is NULL when the search path names no schema.
    let mut report = GrowthReport {
        database: summary.try_get(0).map_err(fail("read the database name"))?,
        schema: summary
            .try_get::<_, Option<String>>(1)
            .map_err(fail("read the schema"))?
            .ok_or_else(|| {
                Error::Storage("read the schema: the search path names no schema".into())
            })?,
        collected_at: summary.try_get(2).map_err(fail("read the time"))?,
        ..GrowthReport::default()
    };
    let rows = tx
        .query(
            "SELECT child.relname::text, pg_total_relation_size(child.oid), COALESCE(stats.n_live_tup, 0)::bigint
             FROM pg_partition_tree('office_events'::regclass) AS partition_tree
             JOIN pg_class AS child ON child.oid = partition_tree.relid
             LEFT JOIN pg_stat_user_tables AS stats ON stats.relid = child.oid
             WHERE partition_tree.isleaf
             ORDER BY child.relname",
            &[],
        )
        .await
        .map_err(fail("read event partition statistics"))?;
    for row in rows {
        let partition = PartitionGrowth {
            name: row.get(0),
            bytes: row.get(1),
            estimated_rows: row.get(2),
        };
        report.total_event_bytes += partition.bytes;
        report.estimated_event_rows += partition.estimated_rows;
        report.partitions.push(partition);
    }
    if detailed {
        let rows = tx
            .query(
                "SELECT organization_id, count(*)::bigint, min(received_at), max(received_at)
                 FROM office_events GROUP BY organization_id ORDER BY organization_id",
                &[],
            )
            .await
            .map_err(fail("read detailed organization event aggregates"))?;
        let mut growth = DetailedGrowth::default();
        for row in rows {
            let organization = OrganizationGrowth {
                organization_id: row.get(0),
                event_count: row.get(1),
                oldest_received_at: row.get(2),
                newest_received_at: row.get(3),
            };
            if let Some(oldest) = organization.oldest_received_at
                && growth.oldest_received_at.is_none_or(|o| oldest < o)
            {
                growth.oldest_received_at = Some(oldest);
            }
            if let Some(newest) = organization.newest_received_at
                && growth.newest_received_at.is_none_or(|n| newest > n)
            {
                growth.newest_received_at = Some(newest);
            }
            growth.organizations.push(organization);
        }
        report.detailed = Some(growth);
    }
    tx.commit()
        .await
        .map_err(fail("finish read-only growth report transaction"))?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_summary_omits_detailed_data_unless_asked() {
        let mut report = GrowthReport {
            database: "agentux".into(),
            total_event_bytes: 4096,
            estimated_event_rows: 7,
            partitions: vec![PartitionGrowth {
                name: "office_events_p0".into(),
                bytes: 1024,
                estimated_rows: 1,
            }],
            ..GrowthReport::default()
        };
        let text = serde_json::to_string(&report).unwrap();
        assert!(
            !text.contains("organizations") && !text.contains("oldestReceivedAt"),
            "{text}"
        );
        assert!(
            text.contains(r#""database":"agentux""#) && text.contains(r#""totalEventBytes":4096"#),
            "{text}"
        );

        report.detailed = Some(DetailedGrowth {
            organizations: vec![OrganizationGrowth {
                organization_id: "org-a".into(),
                event_count: 7,
                ..OrganizationGrowth::default()
            }],
            ..DetailedGrowth::default()
        });
        let text = serde_json::to_string(&report).unwrap();
        assert!(text.contains(r#""organizationId":"org-a""#), "{text}");
    }
}
