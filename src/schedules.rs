//! Organization-scoped mission schedules and durable firing occurrences.

use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cron_lite::Schedule as CronSchedule;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::missions::Input;

pub const ACTIVE: &str = "active";
pub const PAUSED: &str = "paused";
pub const DELETED: &str = "deleted";
pub const COMPLETED: &str = "completed";
pub const PENDING: &str = "pending";
pub const DISPATCHING: &str = "dispatching";
pub const DISPATCHED: &str = "dispatched";
pub const SKIPPED: &str = "skipped";
pub const FAILED: &str = "failed";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissedPolicy {
    Skip,
    RunOnce,
    RunAll,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConcurrencyPolicy {
    Skip,
    Queue,
    Allow,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Create {
    pub name: String,
    pub input: Input,
    #[serde(default)]
    pub cron: Option<String>,
    #[serde(default)]
    pub timezone: Option<String>,
    #[serde(default)]
    pub run_at: Option<DateTime<Utc>>,
    pub missed_policy: MissedPolicy,
    pub concurrency_policy: ConcurrencyPolicy,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Schedule {
    pub schedule_id: String,
    pub organization_id: String,
    pub name: String,
    pub input: Input,
    pub cron: Option<String>,
    pub timezone: Option<String>,
    pub run_at: Option<DateTime<Utc>>,
    pub missed_policy: MissedPolicy,
    pub concurrency_policy: ConcurrencyPolicy,
    pub status: String,
    pub next_run_at: Option<DateTime<Utc>>,
    pub last_run_at: Option<DateTime<Utc>>,
    pub last_run_id: Option<String>,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub revision: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Occurrence {
    pub schedule_id: String,
    pub organization_id: String,
    pub scheduled_for: DateTime<Utc>,
    pub run_id: String,
    pub status: String,
    pub error: String,
    pub created_at: DateTime<Utc>,
    pub run: Option<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    NotFound,
    Conflict,
    Invalid(String),
    Storage(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => f.write_str("schedule not found"),
            Self::Conflict => f.write_str("schedule changed meanwhile"),
            Self::Invalid(message) | Self::Storage(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {}

impl From<tokio_postgres::Error> for Error {
    fn from(error: tokio_postgres::Error) -> Self {
        Self::Storage(error.to_string())
    }
}

impl From<deadpool_postgres::PoolError> for Error {
    fn from(error: deadpool_postgres::PoolError) -> Self {
        Self::Storage(error.to_string())
    }
}

pub fn validate(input: &Create) -> Result<Option<DateTime<Utc>>, Error> {
    if input.name.trim().is_empty() || input.name.len() > 200 {
        return Err(Error::Invalid(
            "name must contain 1 to 200 characters".into(),
        ));
    }
    if input.input.repository.trim().is_empty() || input.input.prompt.trim().is_empty() {
        return Err(Error::Invalid("repository and prompt are required".into()));
    }
    match (&input.cron, &input.timezone, input.run_at) {
        (Some(cron), Some(timezone), None) => {
            let expression = format!("TZ={timezone} {cron}");
            let schedule = CronSchedule::new(expression)
                .map_err(|error| Error::Invalid(format!("invalid cron schedule: {error}")))?;
            schedule
                .upcoming(&Utc::now())
                .map(Some)
                .ok_or_else(|| Error::Invalid("cron schedule has no upcoming occurrence".into()))
        }
        (None, None, Some(run_at)) => Ok(Some(run_at)),
        _ => Err(Error::Invalid(
            "provide either cron and timezone, or runAt, exclusively".into(),
        )),
    }
}

fn next_after(schedule: &Schedule, after: DateTime<Utc>) -> Result<Option<DateTime<Utc>>, Error> {
    match (&schedule.cron, &schedule.timezone) {
        (Some(cron), Some(timezone)) => CronSchedule::new(format!("TZ={timezone} {cron}"))
            .map_err(|error| Error::Invalid(format!("invalid cron schedule: {error}")))
            .map(|parsed| parsed.upcoming(&after)),
        _ => Ok(None),
    }
}

fn occurrence_id(schedule_id: &str, scheduled_for: DateTime<Utc>) -> String {
    let mut digest = Sha256::new();
    digest.update(schedule_id.as_bytes());
    digest.update([0]);
    digest.update(scheduled_for.to_rfc3339().as_bytes());
    format!("scheduled-{}", hex::encode(digest.finalize()))
}

type DueSlots = (Vec<(DateTime<Utc>, &'static str)>, Option<DateTime<Utc>>);

fn due_slots(schedule: &Schedule, now: DateTime<Utc>) -> Result<DueSlots, Error> {
    let Some(mut due) = schedule.next_run_at else {
        return Ok((Vec::new(), None));
    };
    if schedule.cron.is_none() {
        let status = if schedule.missed_policy == MissedPolicy::Skip
            && now.signed_duration_since(due).num_seconds() > 60
        {
            SKIPPED
        } else {
            PENDING
        };
        return Ok((vec![(due, status)], None));
    }
    let mut all = Vec::new();
    while due <= now && all.len() < 100 {
        all.push(due);
        let Some(next) = next_after(schedule, due + chrono::Duration::seconds(1))? else {
            break;
        };
        due = next;
    }
    if all.is_empty() {
        return Ok((Vec::new(), Some(due)));
    }
    let selected: Vec<DateTime<Utc>> = match schedule.missed_policy {
        MissedPolicy::Skip
            if all
                .last()
                .is_some_and(|last| now.signed_duration_since(*last).num_seconds() > 60) =>
        {
            vec![*all.last().unwrap()]
        }
        MissedPolicy::Skip => vec![all[0]],
        MissedPolicy::RunOnce => vec![*all.last().unwrap()],
        MissedPolicy::RunAll => all.clone(),
    };
    let next = Some(due);
    Ok((
        selected
            .into_iter()
            .map(|time| {
                let skipped = schedule.missed_policy == MissedPolicy::Skip
                    && now.signed_duration_since(time).num_seconds() > 60;
                (time, if skipped { SKIPPED } else { PENDING })
            })
            .collect(),
        next,
    ))
}

#[async_trait]
pub trait Store: Send + Sync {
    async fn create(&self, schedule: &Schedule) -> Result<(), Error>;
    async fn get(&self, org: &str, id: &str) -> Result<Schedule, Error>;
    async fn list(&self, org: &str) -> Result<Vec<Schedule>, Error>;
    async fn save(&self, schedule: &Schedule) -> Result<(), Error>;
    async fn claim_due(&self, now: DateTime<Utc>, limit: i64) -> Result<Vec<Occurrence>, Error>;
    async fn claim_pending(&self, now: DateTime<Utc>, limit: i64)
    -> Result<Vec<Occurrence>, Error>;
    async fn set_occurrence(&self, occurrence: &Occurrence) -> Result<(), Error>;
    async fn record_occurrence(&self, occurrence: &Occurrence) -> Result<(), Error>;
    async fn occurrences(&self, org: &str, schedule: &str) -> Result<Vec<Occurrence>, Error>;
}

#[derive(Default)]
pub struct Memory {
    schedules: Mutex<std::collections::BTreeMap<String, Schedule>>,
    occurrences: Mutex<std::collections::BTreeMap<(String, DateTime<Utc>), Occurrence>>,
}

#[async_trait]
impl Store for Memory {
    async fn create(&self, schedule: &Schedule) -> Result<(), Error> {
        self.schedules
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(schedule.schedule_id.clone(), schedule.clone());
        Ok(())
    }

    async fn get(&self, org: &str, id: &str) -> Result<Schedule, Error> {
        self.schedules
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(id)
            .filter(|schedule| schedule.organization_id == org && schedule.status != DELETED)
            .cloned()
            .ok_or(Error::NotFound)
    }

    async fn list(&self, org: &str) -> Result<Vec<Schedule>, Error> {
        Ok(self
            .schedules
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|schedule| schedule.organization_id == org && schedule.status != DELETED)
            .cloned()
            .collect())
    }

    async fn save(&self, schedule: &Schedule) -> Result<(), Error> {
        let mut schedules = self.schedules.lock().unwrap_or_else(|e| e.into_inner());
        let Some(current) = schedules.get(&schedule.schedule_id) else {
            return Err(Error::NotFound);
        };
        if current.revision + 1 != schedule.revision
            || current.organization_id != schedule.organization_id
        {
            return Err(Error::Conflict);
        }
        schedules.insert(schedule.schedule_id.clone(), schedule.clone());
        Ok(())
    }

    async fn claim_due(&self, now: DateTime<Utc>, limit: i64) -> Result<Vec<Occurrence>, Error> {
        let mut schedules = self.schedules.lock().unwrap_or_else(|e| e.into_inner());
        let mut occurrences = self.occurrences.lock().unwrap_or_else(|e| e.into_inner());
        let ids: Vec<String> = schedules
            .values()
            .filter(|s| s.status == ACTIVE && s.next_run_at.is_some_and(|time| time <= now))
            .take(limit.max(0) as usize)
            .map(|s| s.schedule_id.clone())
            .collect();
        let mut claimed = Vec::new();
        for id in ids {
            let Some(schedule) = schedules.get_mut(&id) else {
                continue;
            };
            let (slots, next) = due_slots(schedule, now)?;
            for (due, status) in slots {
                let occurrence = Occurrence {
                    schedule_id: schedule.schedule_id.clone(),
                    organization_id: schedule.organization_id.clone(),
                    scheduled_for: due,
                    run_id: occurrence_id(&schedule.schedule_id, due),
                    status: status.into(),
                    error: String::new(),
                    created_at: now,
                    run: None,
                };
                occurrences
                    .entry((id.clone(), due))
                    .or_insert_with(|| occurrence.clone());
                claimed.push(occurrence.clone());
                schedule.last_run_at = Some(due);
                schedule.last_run_id = Some(occurrence.run_id);
            }
            schedule.next_run_at = next;
            if schedule.cron.is_none() {
                schedule.status = COMPLETED.into();
            }
            schedule.revision += 1;
            schedule.updated_at = now;
        }
        Ok(claimed)
    }

    async fn claim_pending(
        &self,
        _now: DateTime<Utc>,
        limit: i64,
    ) -> Result<Vec<Occurrence>, Error> {
        let mut occurrences = self.occurrences.lock().unwrap_or_else(|e| e.into_inner());
        let mut schedules = std::collections::BTreeSet::new();
        let mut claimed = Vec::new();
        for occurrence in occurrences.values_mut() {
            if occurrence.status != PENDING || !schedules.insert(occurrence.schedule_id.clone()) {
                continue;
            }
            occurrence.status = DISPATCHING.into();
            claimed.push(occurrence.clone());
            if claimed.len() >= limit.max(0) as usize {
                break;
            }
        }
        Ok(claimed)
    }

    async fn set_occurrence(&self, occurrence: &Occurrence) -> Result<(), Error> {
        let mut entries = self.occurrences.lock().unwrap_or_else(|e| e.into_inner());
        let key = (occurrence.schedule_id.clone(), occurrence.scheduled_for);
        if !entries.contains_key(&key) {
            return Err(Error::NotFound);
        }
        entries.insert(key, occurrence.clone());
        Ok(())
    }

    async fn record_occurrence(&self, occurrence: &Occurrence) -> Result<(), Error> {
        self.occurrences
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry((occurrence.schedule_id.clone(), occurrence.scheduled_for))
            .or_insert_with(|| occurrence.clone());
        Ok(())
    }

    async fn occurrences(&self, org: &str, schedule: &str) -> Result<Vec<Occurrence>, Error> {
        Ok(self
            .occurrences
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|item| item.organization_id == org && item.schedule_id == schedule)
            .cloned()
            .collect())
    }
}

pub struct Postgres {
    pool: deadpool_postgres::Pool,
}

impl Postgres {
    pub fn new(pool: deadpool_postgres::Pool) -> Self {
        Self { pool }
    }
}

use tokio_postgres::types::Json;

#[async_trait]
impl Store for Postgres {
    async fn create(&self, schedule: &Schedule) -> Result<(), Error> {
        let client = self.pool.get().await?;
        client.execute("INSERT INTO mission_schedules (schedule_id, organization_id, status, next_run_at, revision, doc) VALUES ($1,$2,$3,$4,$5,$6)",
            &[&schedule.schedule_id, &schedule.organization_id, &schedule.status, &schedule.next_run_at, &schedule.revision, &Json(schedule)]).await?;
        Ok(())
    }

    async fn get(&self, org: &str, id: &str) -> Result<Schedule, Error> {
        let client = self.pool.get().await?;
        let row = client.query_opt("SELECT doc FROM mission_schedules WHERE organization_id=$1 AND schedule_id=$2 AND status <> 'deleted'", &[&org, &id]).await?.ok_or(Error::NotFound)?;
        Ok(row.get::<_, Json<Schedule>>(0).0)
    }

    async fn list(&self, org: &str) -> Result<Vec<Schedule>, Error> {
        let client = self.pool.get().await?;
        let rows = client.query("SELECT doc FROM mission_schedules WHERE organization_id=$1 AND status <> 'deleted' ORDER BY next_run_at NULLS LAST, created_at DESC", &[&org]).await?;
        Ok(rows
            .iter()
            .map(|row| row.get::<_, Json<Schedule>>(0).0)
            .collect())
    }

    async fn save(&self, schedule: &Schedule) -> Result<(), Error> {
        let client = self.pool.get().await?;
        let updated = client.execute("UPDATE mission_schedules SET status=$3,next_run_at=$4,revision=$5,doc=$6,updated_at=$7 WHERE organization_id=$1 AND schedule_id=$2 AND revision=$8",
            &[&schedule.organization_id, &schedule.schedule_id, &schedule.status, &schedule.next_run_at, &schedule.revision, &Json(schedule), &schedule.updated_at, &(schedule.revision - 1)]).await?;
        if updated == 1 {
            Ok(())
        } else {
            Err(Error::Conflict)
        }
    }

    async fn claim_due(&self, now: DateTime<Utc>, limit: i64) -> Result<Vec<Occurrence>, Error> {
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        let rows = tx.query("SELECT doc FROM mission_schedules WHERE status='active' AND next_run_at <= $1 ORDER BY next_run_at LIMIT $2 FOR UPDATE SKIP LOCKED", &[&now, &limit]).await?;
        let mut claimed = Vec::new();
        for row in rows {
            let mut schedule: Schedule = row.get::<_, Json<Schedule>>(0).0;
            let (slots, next) = due_slots(&schedule, now)?;
            for (due, status) in slots {
                let occurrence = Occurrence {
                    schedule_id: schedule.schedule_id.clone(),
                    organization_id: schedule.organization_id.clone(),
                    scheduled_for: due,
                    run_id: occurrence_id(&schedule.schedule_id, due),
                    status: status.into(),
                    error: String::new(),
                    created_at: now,
                    run: None,
                };
                tx.execute("INSERT INTO mission_schedule_occurrences (schedule_id, organization_id, scheduled_for, run_id, status, error, created_at, doc) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (schedule_id, scheduled_for) DO NOTHING",
                    &[&occurrence.schedule_id, &occurrence.organization_id, &occurrence.scheduled_for, &occurrence.run_id, &occurrence.status, &occurrence.error, &occurrence.created_at, &Json(&occurrence)]).await?;
                if status == PENDING {
                    claimed.push(occurrence.clone());
                }
                schedule.last_run_at = Some(due);
                schedule.last_run_id = Some(occurrence.run_id);
            }
            schedule.next_run_at = next;
            if schedule.cron.is_none() {
                schedule.status = COMPLETED.into();
            }
            schedule.revision += 1;
            schedule.updated_at = now;
            tx.execute("UPDATE mission_schedules SET status=$2,next_run_at=$3,revision=$4,updated_at=$5,doc=$6 WHERE schedule_id=$1",
                &[&schedule.schedule_id, &schedule.status, &schedule.next_run_at, &schedule.revision, &schedule.updated_at, &Json(&schedule)]).await?;
        }
        tx.commit().await?;
        Ok(claimed)
    }

    async fn claim_pending(
        &self,
        now: DateTime<Utc>,
        limit: i64,
    ) -> Result<Vec<Occurrence>, Error> {
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        let rows = tx.query("SELECT o.doc FROM mission_schedule_occurrences o WHERE (o.status='pending' OR (o.status='dispatching' AND o.claimed_at < $1 - interval '5 minutes')) AND NOT EXISTS (SELECT 1 FROM mission_schedule_occurrences d WHERE d.schedule_id=o.schedule_id AND d.run_id<>o.run_id AND d.status='dispatching' AND d.claimed_at >= $1 - interval '5 minutes') ORDER BY o.scheduled_for LIMIT $2 FOR UPDATE OF o SKIP LOCKED", &[&now, &limit]).await?;
        let mut claimed = Vec::new();
        let mut schedules = std::collections::BTreeSet::new();
        for row in rows {
            let mut occurrence: Occurrence = row.get::<_, Json<Occurrence>>(0).0;
            if !schedules.insert(occurrence.schedule_id.clone()) {
                continue;
            }
            occurrence.status = DISPATCHING.into();
            tx.execute("UPDATE mission_schedule_occurrences SET status='dispatching',claimed_at=$3,doc=$4 WHERE organization_id=$1 AND schedule_id=$2 AND scheduled_for=$5",
                &[&occurrence.organization_id, &occurrence.schedule_id, &now, &Json(&occurrence), &occurrence.scheduled_for]).await?;
            claimed.push(occurrence);
        }
        tx.commit().await?;
        Ok(claimed)
    }

    async fn set_occurrence(&self, occurrence: &Occurrence) -> Result<(), Error> {
        let client = self.pool.get().await?;
        let claimed_at: Option<DateTime<Utc>> = None;
        let changed = client.execute("UPDATE mission_schedule_occurrences SET status=$4,error=$5,doc=$6,claimed_at=$7 WHERE organization_id=$1 AND schedule_id=$2 AND scheduled_for=$3",
            &[&occurrence.organization_id, &occurrence.schedule_id, &occurrence.scheduled_for, &occurrence.status, &occurrence.error, &Json(occurrence), &claimed_at]).await?;
        if changed == 1 {
            Ok(())
        } else {
            Err(Error::NotFound)
        }
    }

    async fn record_occurrence(&self, occurrence: &Occurrence) -> Result<(), Error> {
        let client = self.pool.get().await?;
        client.execute("INSERT INTO mission_schedule_occurrences (schedule_id, organization_id, scheduled_for, run_id, status, error, created_at, doc) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (schedule_id, scheduled_for) DO NOTHING",
            &[&occurrence.schedule_id, &occurrence.organization_id, &occurrence.scheduled_for, &occurrence.run_id, &occurrence.status, &occurrence.error, &occurrence.created_at, &Json(occurrence)]).await?;
        Ok(())
    }

    async fn occurrences(&self, org: &str, schedule: &str) -> Result<Vec<Occurrence>, Error> {
        let client = self.pool.get().await?;
        let rows = client.query("SELECT doc FROM mission_schedule_occurrences WHERE organization_id=$1 AND schedule_id=$2 ORDER BY scheduled_for DESC LIMIT 100", &[&org, &schedule]).await?;
        Ok(rows
            .iter()
            .map(|row| row.get::<_, Json<Occurrence>>(0).0)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn schedule(missed_policy: MissedPolicy, first: DateTime<Utc>) -> Schedule {
        Schedule {
            schedule_id: "schedule-test".into(),
            organization_id: "org-test".into(),
            name: "daily review".into(),
            input: Input {
                mission_id: "review".into(),
                repository: "org/repo".into(),
                prompt: "Review dependencies".into(),
                ..Input::default()
            },
            cron: Some("0 9 * * *".into()),
            timezone: Some("America/New_York".into()),
            run_at: None,
            missed_policy,
            concurrency_policy: ConcurrencyPolicy::Allow,
            status: ACTIVE.into(),
            next_run_at: Some(first),
            last_run_at: None,
            last_run_id: None,
            created_by: "operator".into(),
            created_at: first,
            updated_at: first,
            revision: 1,
        }
    }

    #[test]
    fn cron_keeps_local_wall_clock_across_daylight_saving_time() {
        let cron = CronSchedule::new("TZ=America/New_York 0 9 * * *").unwrap();
        let before_dst = Utc.with_ymd_and_hms(2026, 3, 7, 14, 0, 0).unwrap();
        let after_dst = cron.upcoming(&before_dst).unwrap();
        assert_eq!(
            after_dst,
            Utc.with_ymd_and_hms(2026, 3, 7, 14, 0, 0).unwrap()
        );
        let next = cron
            .upcoming(&(after_dst + chrono::Duration::seconds(1)))
            .unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 3, 8, 13, 0, 0).unwrap());
    }

    #[test]
    fn missed_run_once_collapses_backlog_to_latest_slot() {
        let schedule = schedule(
            MissedPolicy::RunOnce,
            Utc.with_ymd_and_hms(2026, 10, 1, 13, 0, 0).unwrap(),
        );
        let now = Utc.with_ymd_and_hms(2026, 10, 3, 16, 0, 0).unwrap();
        let (slots, next) = due_slots(&schedule, now).unwrap();
        assert_eq!(slots.len(), 1);
        assert_eq!(
            slots[0].0,
            Utc.with_ymd_and_hms(2026, 10, 3, 13, 0, 0).unwrap()
        );
        assert!(next.is_some_and(|time| time > now));
    }

    #[test]
    fn missed_skip_records_skipped_fire_and_advances_schedule() {
        let schedule = schedule(
            MissedPolicy::Skip,
            Utc.with_ymd_and_hms(2026, 10, 1, 13, 0, 0).unwrap(),
        );
        let now = Utc.with_ymd_and_hms(2026, 10, 3, 16, 0, 0).unwrap();
        let (slots, next) = due_slots(&schedule, now).unwrap();
        assert_eq!(
            slots,
            vec![(
                Utc.with_ymd_and_hms(2026, 10, 3, 13, 0, 0).unwrap(),
                SKIPPED
            )]
        );
        assert!(next.is_some_and(|time| time > now));
    }

    #[test]
    fn occurrence_run_id_is_stable_for_same_schedule_instant() {
        let instant = Utc.with_ymd_and_hms(2026, 11, 4, 12, 0, 0).unwrap();
        assert_eq!(
            occurrence_id("schedule-a", instant),
            occurrence_id("schedule-a", instant)
        );
        assert_ne!(
            occurrence_id("schedule-a", instant),
            occurrence_id("schedule-b", instant)
        );
    }

    #[tokio::test]
    async fn claiming_a_one_time_occurrence_advances_it_exactly_once() {
        let store = Memory::default();
        let due = Utc.with_ymd_and_hms(2026, 11, 4, 12, 0, 0).unwrap();
        let schedule = Schedule {
            schedule_id: "schedule-once".into(),
            organization_id: "org-test".into(),
            name: "one-time".into(),
            input: Input::default(),
            cron: None,
            timezone: None,
            run_at: Some(due),
            missed_policy: MissedPolicy::Skip,
            concurrency_policy: ConcurrencyPolicy::Allow,
            status: ACTIVE.into(),
            next_run_at: Some(due),
            last_run_at: None,
            last_run_id: None,
            created_by: "operator".into(),
            created_at: due,
            updated_at: due,
            revision: 1,
        };
        store.create(&schedule).await.unwrap();
        let now = due + chrono::Duration::seconds(2);
        let first = store.claim_due(now, 10).await.unwrap();
        let second = store.claim_due(now, 10).await.unwrap();
        assert_eq!(first.len(), 1);
        assert!(second.is_empty());
        assert_eq!(first[0].status, PENDING);
        assert_eq!(first[0].run_id, occurrence_id("schedule-once", due));
    }
}
