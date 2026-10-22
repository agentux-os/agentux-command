//! The PostgreSQL read model and storage report; skipped without
//! AGENTUX_COMMAND_TEST_DATABASE_URL.

mod common;

use agentux_command::office::{self, Event, OfficeRepository, PostgresStore, ResolutionRequest};
use serde_json::json;

async fn store() -> Option<(
    PostgresStore,
    deadpool_postgres::Pool,
    tokio_postgres::Config,
    String,
)> {
    let (config, schema) = common::schema("office_test").await?;
    let pool = office::pool(config.clone());
    office::migrate(&mut pool.get().await.unwrap())
        .await
        .unwrap();
    Some((PostgresStore::new(pool.clone()), pool, config, schema))
}

fn event(id: &str, org: &str, sequence: i64) -> Event {
    Event {
        event_id: id.into(),
        event_type: "run.started".into(),
        organization_id: org.into(),
        node_id: "node-01".into(),
        sequence,
        ..Event::default()
    }
}

#[tokio::test]
async fn recovers_and_deduplicates_events_across_connections() {
    let Some((store, _, config, _)) = store().await else {
        return;
    };
    let e = event("evt-restart", "org-a", 11);
    store.apply_event(e.clone()).await.unwrap();
    let restarted = PostgresStore::new(office::pool(config));
    restarted
        .apply_event(e)
        .await
        .expect("a duplicate replay is idempotent");
    let loaded = restarted.get_projection("org-a").await.unwrap().unwrap();
    assert_eq!(loaded.last_event_id, "evt-restart");
    assert_eq!(loaded.last_sequence, 11);
    assert_eq!(loaded.events.len(), 1);
    restarted.verify_projection("org-a").await.unwrap();
}

#[tokio::test]
async fn refuses_out_of_order_node_events() {
    let Some((store, ..)) = store().await else {
        return;
    };
    store
        .apply_event(event("evt-12", "org-a", 12))
        .await
        .unwrap();
    match store.apply_event(event("evt-10", "org-a", 10)).await {
        Err(office::Error::Conflict(c)) => assert_eq!(c.code, "stale_version"),
        other => panic!("expected a stale cursor conflict, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn scopes_approval_idempotency_and_serializes_concurrent_retries() {
    let Some((store, ..)) = store().await else {
        return;
    };
    let store = std::sync::Arc::new(store);
    for org in ["org-a", "org-b"] {
        store
            .apply_event(Event {
                event_type: "approval.requested".into(),
                data: Some(json!({"id": "approval-1", "action": "publish", "expiresAt": "2099-01-01T00:00:00Z"})),
                ..event(&format!("requested-{org}"), org, 1)
            })
            .await
            .unwrap();
    }
    let request = ResolutionRequest {
        organization_id: "org-a".into(),
        decision: "approved".into(),
        idempotency_key: "shared-key".into(),
        ..ResolutionRequest::default()
    };
    let retries: Vec<_> = (0..8)
        .map(|_| {
            let (store, request) = (store.clone(), request.clone());
            tokio::spawn(async move { store.resolve_approval("approval-1", request).await })
        })
        .collect();
    for retry in retries {
        let response = retry.await.unwrap().unwrap();
        assert_eq!(response.decision, "approved");
        assert_eq!(response.organization_id, "org-a");
    }
    let other = store
        .resolve_approval(
            "approval-1",
            ResolutionRequest {
                organization_id: "org-b".into(),
                decision: "rejected".into(),
                ..request
            },
        )
        .await
        .unwrap();
    assert!(
        !other.idempotent_replay,
        "the same key in another organization is independent"
    );
    assert_eq!(other.decision, "rejected");
    store.verify_projection("org-a").await.unwrap();
}

#[tokio::test]
async fn replay_verification_detects_projection_drift() {
    let Some((store, pool, ..)) = store().await else {
        return;
    };
    store
        .apply_event(event("evt-drift", "org-a", 1))
        .await
        .unwrap();
    pool.get()
        .await
        .unwrap()
        .execute(
            "UPDATE office_projections SET payload = jsonb_set(payload, '{freshness}', '\"stale\"') WHERE organization_id = $1",
            &[&"org-a"],
        )
        .await
        .unwrap();
    assert!(store.verify_projection("org-a").await.is_err());
}

#[tokio::test]
async fn migrations_apply_twice_safely() {
    let Some((_, pool, ..)) = store().await else {
        return;
    };
    office::migrate(&mut pool.get().await.unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn the_growth_report_summarizes_partitions_and_optionally_organizations() {
    let Some((store, pool, ..)) = store().await else {
        return;
    };
    for (i, org) in ["org-growth-a", "org-growth-b"].iter().enumerate() {
        store
            .apply_event(event(&format!("growth-{org}"), org, i as i64 + 1))
            .await
            .unwrap();
    }
    let mut client = pool.get().await.unwrap();
    let summary = office::collect_growth_report(&mut client, false)
        .await
        .unwrap();
    assert!(!summary.database.is_empty() && summary.total_event_bytes > 0);
    assert_eq!(summary.partitions.len(), 8);
    assert!(summary.detailed.is_none());
    let detailed = office::collect_growth_report(&mut client, true)
        .await
        .unwrap();
    let organizations = detailed.detailed.unwrap().organizations;
    assert_eq!(organizations.len(), 2);
    for organization in organizations {
        assert_eq!(organization.event_count, 1);
        assert!(
            organization.oldest_received_at.is_some() && organization.newest_received_at.is_some()
        );
    }
}

#[tokio::test]
async fn the_growth_report_handles_an_empty_log() {
    let Some((_, pool, ..)) = store().await else {
        return;
    };
    let report = office::collect_growth_report(&mut pool.get().await.unwrap(), true)
        .await
        .unwrap();
    let detailed = report.detailed.unwrap();
    assert!(detailed.oldest_received_at.is_none() && detailed.newest_received_at.is_none());
    assert!(detailed.organizations.is_empty());
}

/// The test database's URL with `options` setting the search path.
fn url_with_schema(schema: &str) -> String {
    let url = common::database_url().unwrap();
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}options=-c%20search_path%3D{schema}")
}

#[tokio::test]
async fn the_storage_report_writes_summary_and_detailed_json() {
    let Some((store, _, _, schema)) = store().await else {
        return;
    };
    store
        .apply_event(event("report-event", "org-report", 1))
        .await
        .unwrap();
    let url = url_with_schema(&schema);
    for (args, detailed) in [(vec![], false), (vec!["--detailed".to_string()], true)] {
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let code = agentux_command::report::run(&args, &url, &mut out, &mut err).await;
        assert_eq!(code, 0, "{}", String::from_utf8_lossy(&err));
        let report: serde_json::Value = serde_json::from_slice(&out).expect("one JSON report");
        assert_eq!(report.get("detailed").is_some(), detailed);
        assert!(report.get("partitions").is_some());
    }
}

#[tokio::test]
async fn a_failed_storage_report_writes_nothing_to_stdout() {
    if common::database_url().is_none() {
        return;
    }
    let url = url_with_schema("report_missing_schema");
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let code = agentux_command::report::run(&[], &url, &mut out, &mut err).await;
    assert_ne!(code, 0);
    assert!(out.is_empty() && !err.is_empty());
}

#[tokio::test]
async fn service_tokens_live_in_the_database() {
    use agentux_command::auth::{
        Authenticator, NODE_COMMAND, PostgresTokens, ServiceTokens, TokenRequest,
    };
    let Some((_, pool, ..)) = store().await else {
        return;
    };
    let tokens = PostgresTokens::new(pool);
    let issued = tokens
        .issue(
            TokenRequest {
                name: "ci".into(),
                organization_id: "acme".into(),
                capabilities: vec![NODE_COMMAND.into()],
                days: Some(7),
                ..TokenRequest::default()
            },
            chrono::Utc::now(),
        )
        .await
        .unwrap();
    let principal = tokens
        .authenticate(&issued.secret)
        .await
        .expect("the token authenticates");
    assert!(principal.can("acme", NODE_COMMAND) && !principal.can("other", NODE_COMMAND));
    let listed = tokens.list("acme").await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].capabilities, [NODE_COMMAND]);
    tokens
        .revoke(&issued.token.id, chrono::Utc::now())
        .await
        .unwrap();
    assert!(
        tokens.authenticate(&issued.secret).await.is_none(),
        "revoked"
    );
    assert!(
        tokens
            .get(&issued.token.id)
            .await
            .unwrap()
            .revoked_at
            .is_some()
    );
}

#[tokio::test]
async fn sessions_live_in_the_database() {
    use agentux_command::auth::{NODE_COMMAND, PostgresSessions, Principal, Sessions};
    let Some((_, pool, ..)) = store().await else {
        return;
    };
    let sessions = PostgresSessions::new(pool);
    let principal = Principal::new("oidc:ana").grant("acme", [NODE_COMMAND]);
    let now = chrono::Utc::now();
    let secret = sessions.start(&principal, now).await.unwrap();
    assert_eq!(sessions.resolve(&secret, now).await, Some(principal));
    assert!(
        sessions
            .resolve(&secret, now + chrono::Duration::hours(9))
            .await
            .is_none(),
        "expired"
    );
    assert!(sessions.resolve("auxo1_wrong", now).await.is_none());
    sessions.end(&secret).await.unwrap();
    assert!(sessions.resolve(&secret, now).await.is_none(), "ended");
}

#[tokio::test(flavor = "multi_thread")]
async fn nodes_are_known_after_a_restart() {
    use agentux_command::auth::{ORGANIZATION_READ, TokenRequest};
    use agentux_command::server::{Handler, parse_node_tokens};
    use axum::http::StatusCode;
    let Some((_, pool, _, schema)) = store().await else {
        return;
    };
    let url = url_with_schema(&schema);
    let handler = Handler::postgres(&url)
        .await
        .unwrap()
        .with_node_tokens(parse_node_tokens("node-1=s3cret"));
    let reader = handler
        .service_tokens()
        .issue(
            TokenRequest {
                name: "reader".into(),
                organization_id: "org-1".into(),
                capabilities: vec![ORGANIZATION_READ.into()],
                ..TokenRequest::default()
            },
            chrono::Utc::now(),
        )
        .await
        .unwrap()
        .secret;
    let app = handler.router();
    let mut hello = common::hello("org-1", "node-1");
    hello["params"]["occurredAt"] = json!(chrono::Utc::now().to_rfc3339());
    let (status, _) =
        common::call(&app, "POST", "/v1/protocol/messages", "s3cret", Some(hello)).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let harnesses = json!({"jsonrpc": "2.0", "method": "harness.status", "params": common::params("org-1", "node-1", json!({
        "eventId": "node-1-1", "sequence": 1, "harnesses": [{"id": "codex", "installed": true}],
    }))});
    let mut harnesses = harnesses;
    harnesses["params"]["occurredAt"] = json!(chrono::Utc::now().to_rfc3339());
    let (status, _) = common::call(
        &app,
        "POST",
        "/v1/protocol/messages",
        "s3cret",
        Some(harnesses),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    // The writer stores behind the request; wait for it.
    let client = pool.get().await.unwrap();
    for _ in 0..200 {
        let stored = client
            .query_opt(
                "SELECT payload FROM node_state WHERE node_id = 'node-1'",
                &[],
            )
            .await
            .unwrap();
        if stored.is_some_and(|row| row.get::<_, serde_json::Value>(0)["lastSequence"] == 1) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let restarted = Handler::postgres(&url).await.unwrap().router();
    let (status, list) = common::call(&restarted, "GET", "/v1/nodes", &reader, None).await;
    assert_eq!(status, StatusCode::OK);
    let node = &list[0];
    assert_eq!(node["nodeId"], "node-1");
    assert_eq!(
        node["harnesses"][0]["id"], "codex",
        "what it reported survives"
    );
    assert_eq!(
        node["lastSequence"], 1,
        "so its next event is not taken for a duplicate"
    );
    assert_eq!(node["connected"], false);
    assert_eq!(node["presence"], "online");
}

#[tokio::test]
async fn definitions_live_in_the_database() {
    use agentux_command::definitions::{self, Actor, Store};
    let Some((_, pool, ..)) = store().await else {
        return;
    };
    let store = definitions::Postgres::new(pool.clone());
    let ana = Actor::of("oidc:ana");
    let now = chrono::Utc::now();
    let mut doc = json!({"schema_version": "1.0", "organization": {"id": "acme", "name": "Acme"}, "areas": [], "roles": [], "missions": [], "workflows": [], "gates": [], "agents": [], "nodes": []});
    let first = store.draft("acme", json!({}), &ana, now).await.unwrap();
    assert_eq!(first.definition_version, 1);
    assert!(matches!(
        store.publish("acme", 1, &ana, now).await,
        Err(definitions::Error::Invalid(_))
    ));
    store
        .replace("acme", 1, doc.clone(), &ana, now)
        .await
        .unwrap();
    let published = store.publish("acme", 1, &ana, now).await.unwrap();
    assert_eq!(published.published_by, Some(ana.clone()));
    doc["organization"]["name"] = json!("Changed");
    assert!(matches!(
        store.replace("acme", 1, doc.clone(), &ana, now).await,
        Err(definitions::Error::Published)
    ));
    assert_eq!(
        store.get("acme", 1).await.unwrap().document["organization"]["name"],
        "Acme",
        "published stays as published"
    );
    assert_eq!(
        store
            .draft("acme", doc, &ana, now)
            .await
            .unwrap()
            .definition_version,
        2
    );
    assert_eq!(store.list("acme").await.unwrap().len(), 2);
    assert_eq!(store.audit("acme", 10).await.unwrap().len(), 4);
    // The publication took its Office event in the same transaction.
    let office = PostgresStore::new(pool.clone());
    let projection = office.get_projection("acme").await.unwrap().unwrap();
    assert_eq!(projection.definition_version, "definition-1");
    assert_eq!(projection.organization["name"], "Acme");
    let queued: i64 = pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT count(*) FROM office_outbox WHERE event_id = 'definition-acme-1'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(queued, 1);
    assert!(matches!(
        store.get("other", 1).await,
        Err(definitions::Error::NotFound)
    ));
}

#[tokio::test]
async fn events_are_queued_with_their_state_and_replayed_by_organization() {
    use office::Replay;
    let Some((store, pool, ..)) = store().await else {
        return;
    };
    for (id, org, sequence) in [("a1", "org-a", 1), ("b1", "org-b", 1), ("a2", "org-a", 2)] {
        store.apply_event(event(id, org, sequence)).await.unwrap();
    }
    store.apply_event(event("a1", "org-a", 1)).await.unwrap();
    let client = pool.get().await.unwrap();
    let queued: Vec<String> = client
        .query("SELECT event_id FROM office_outbox ORDER BY outbox_id", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(queued, ["a1", "b1", "a2"], "once per event, in order");

    let all = |after| Replay {
        after,
        limit: 100,
        ..Replay::default()
    };
    let replayed = store.events("org-a", &all(0)).await.unwrap();
    let ids: Vec<&str> = replayed.iter().map(|l| l.event.event_id.as_str()).collect();
    assert_eq!(ids, ["a1", "a2"], "only the organization's events");
    assert_eq!(
        replayed[0].event.actor,
        Some(json!({"kind": "node", "id": "node-01"})),
        "a node's events are the node's"
    );
    let rest = store
        .events("org-a", &all(replayed[0].cursor))
        .await
        .unwrap();
    assert_eq!(rest.len(), 1);
    assert_eq!(rest[0].event.event_id, "a2");
    let one = Replay { limit: 1, ..all(0) };
    assert_eq!(store.events("org-a", &one).await.unwrap().len(), 1);
    let future = Replay {
        since: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
        ..all(0)
    };
    assert!(store.events("org-a", &future).await.unwrap().is_empty());
    let past = Replay {
        until: Some(chrono::Utc::now() - chrono::Duration::hours(1)),
        ..all(0)
    };
    assert!(store.events("org-a", &past).await.unwrap().is_empty());
}

#[tokio::test]
async fn an_approval_decision_is_queued_with_its_actor() {
    let Some((store, pool, ..)) = store().await else {
        return;
    };
    store
        .apply_event(Event {
            event_type: "approval.requested".into(),
            data: Some(json!({"id": "approval-1", "runId": "run-7", "correlationId": "corr-7"})),
            ..event("requested", "org-a", 1)
        })
        .await
        .unwrap();
    store
        .resolve_approval(
            "approval-1",
            ResolutionRequest {
                organization_id: "org-a".into(),
                decision: "approved".into(),
                idempotency_key: "k".into(),
                actor: Some(json!({"kind": "user", "id": "oidc:ana"})),
                ..ResolutionRequest::default()
            },
        )
        .await
        .unwrap();
    let row = pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT payload FROM office_outbox WHERE payload->>'eventType' = 'approval.resolved'",
            &[],
        )
        .await
        .unwrap();
    let tokio_postgres::types::Json(event): tokio_postgres::types::Json<serde_json::Value> =
        row.get(0);
    assert_eq!(event["actor"]["id"], "oidc:ana");
    assert_eq!(event["runId"], "run-7");
    assert_eq!(event["correlationId"], "corr-7");
}

/// A sink that records what it takes and refuses the events named in `fail`.
#[derive(Default)]
struct Recorder {
    taken: std::sync::Mutex<Vec<String>>,
    fail: std::sync::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl agentux_command::outbox::Sink for Recorder {
    async fn deliver(&self, event: &serde_json::Value) -> Result<(), String> {
        let id = event["eventId"].as_str().unwrap_or_default().to_string();
        if self.fail.lock().unwrap().contains(&id) {
            return Err(format!("{id} refused"));
        }
        self.taken.lock().unwrap().push(id);
        Ok(())
    }
}

#[tokio::test]
async fn delivery_is_ordered_per_organization_retried_and_survives_a_restart() {
    use agentux_command::outbox::{Delivery, Pass};
    use std::sync::Arc;
    let Some((store, pool, config, _)) = store().await else {
        return;
    };
    for (id, org, sequence) in [
        ("a1", "org-a", 1),
        ("a2", "org-a", 2),
        ("b1", "org-b", 1),
        ("a3", "org-a", 3),
    ] {
        store.apply_event(event(id, org, sequence)).await.unwrap();
    }
    let sink = Arc::new(Recorder::default());
    sink.fail.lock().unwrap().push("a2".into());
    let delivery = Delivery::new(pool.clone(), Some(sink.clone()));
    assert_eq!(
        delivery.run_once().await.unwrap(),
        Pass {
            delivered: 2,
            failed: 1
        }
    );
    assert_eq!(
        *sink.taken.lock().unwrap(),
        ["a1", "b1"],
        "org-a stops at a2; org-b goes on"
    );
    let client = pool.get().await.unwrap();
    let row = client
        .query_one(
            "SELECT attempts, last_error, next_attempt_at > now() FROM office_outbox WHERE event_id = 'a2'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i32>(0), 1);
    assert_eq!(
        row.get::<_, Option<String>>(1).as_deref(),
        Some("a2 refused")
    );
    assert!(row.get::<_, bool>(2), "retried later");
    assert_eq!(
        delivery.run_once().await.unwrap(),
        Pass::default(),
        "a3 waits behind a2"
    );

    // A restart: a new pool and delivery, and the sink takes a2 now.
    drop(delivery);
    let restarted = Delivery::new(office::pool(config), Some(sink.clone()));
    sink.fail.lock().unwrap().clear();
    client
        .execute(
            "UPDATE office_outbox SET next_attempt_at = now() WHERE event_id = 'a2'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        restarted.run_once().await.unwrap(),
        Pass {
            delivered: 2,
            failed: 0
        }
    );
    assert_eq!(*sink.taken.lock().unwrap(), ["a1", "b1", "a2", "a3"]);
    assert_eq!(
        restarted.run_once().await.unwrap(),
        Pass::default(),
        "each once"
    );

    // Without a sink nothing is sent.
    store.apply_event(event("a4", "org-a", 4)).await.unwrap();
    assert_eq!(
        Delivery::new(pool.clone(), None).run_once().await.unwrap(),
        Pass::default()
    );
}

#[tokio::test]
async fn mission_runs_live_in_the_database_and_refuse_stale_writes() {
    use agentux_command::missions::{self, Input, Run, Store};
    let Some((_, pool, config, _)) = store().await else {
        return;
    };
    let runs = missions::Postgres::new(pool);
    let now = chrono::Utc::now();
    let mut run = Run {
        run_id: "run-1".into(),
        organization_id: "acme".into(),
        definition_version: 3,
        mission_id: "ship".into(),
        workflow_id: "ship-v1".into(),
        status: missions::RUNNING.into(),
        step: "implement".into(),
        input: Input::default(),
        steps: Default::default(),
        error: String::new(),
        run_at: None,
        created_by: agentux_command::definitions::Actor::of("oidc:ana"),
        created_at: now,
        updated_at: now,
        revision: 1,
    };
    runs.insert(&run).await.unwrap();
    run.revision = 2;
    run.status = missions::WAITING.into();
    runs.save(&run).await.unwrap();
    assert!(
        matches!(runs.save(&run).await, Err(missions::Error::Conflict)),
        "a second writer of revision 2 loses"
    );

    // After a restart: the same run, still driven.
    let restarted = missions::Postgres::new(office::pool(config));
    let loaded = restarted.get("acme", "run-1").await.unwrap();
    assert_eq!((loaded.revision, loaded.status.as_str()), (2, "waiting"));
    assert_eq!(restarted.active(None).await.unwrap().len(), 1);
    assert!(matches!(
        restarted.get("beta", "run-1").await,
        Err(missions::Error::NotFound)
    ));
    run.revision = 3;
    run.status = missions::COMPLETED.into();
    restarted.save(&run).await.unwrap();
    assert!(restarted.active(Some("acme")).await.unwrap().is_empty());
    assert_eq!(restarted.list("acme").await.unwrap().len(), 1);
}
