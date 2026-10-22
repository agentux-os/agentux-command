//! The same checks on every enrollment store: memory always, PostgreSQL
//! when AGENTUX_COMMAND_TEST_DATABASE_URL is set.

mod common;

use agentux_command::enroll::{self, Memory, Store, TOKEN_PREFIX, TokenRequest};
use agentux_command::office;
use chrono::{Duration, TimeZone, Utc};

async fn stores() -> Vec<(&'static str, Box<dyn Store>)> {
    let mut out: Vec<(&'static str, Box<dyn Store>)> = vec![("memory", Box::new(Memory::new("")))];
    if let Some((config, _)) = common::schema("enroll_test").await {
        let pool = office::pool(config);
        office::migrate(&mut pool.get().await.unwrap())
            .await
            .unwrap();
        out.push(("postgres", Box::new(enroll::Postgres::new(pool))));
    }
    out
}

fn request(org: &str) -> TokenRequest {
    TokenRequest {
        organization_id: org.into(),
        ..TokenRequest::default()
    }
}

#[tokio::test]
async fn tokens_are_bound_to_one_instance_used_once_and_expire() {
    let now = Utc.with_ymd_and_hms(2026, 10, 21, 12, 0, 0).unwrap();
    for (name, store) in stores().await {
        let instance = store.instance_id().await.unwrap();
        assert!(enroll::valid_instance_id(&instance), "{name}: {instance}");
        assert_eq!(
            store.instance_id().await.unwrap(),
            instance,
            "{name}: the instance id must not change"
        );
        let tokens = store
            .issue_tokens(
                TokenRequest {
                    count: 3,
                    ttl: Duration::hours(1),
                    ..request("acme")
                },
                now,
            )
            .await
            .unwrap();
        assert_eq!(tokens.len(), 3);
        assert_eq!(
            enroll::token_instance(&tokens[0].token),
            Some(instance.as_str())
        );

        let first = store
            .redeem(&tokens[0].token, "", "Build Box 1", now)
            .await
            .unwrap();
        assert_eq!(first.credential.node_id, "node-build-box-1", "{name}");
        assert!(first.credential.approved);
        assert_eq!(first.credential.organization_id, "acme");
        assert!(
            matches!(
                store.redeem(&tokens[0].token, "", "x", now).await,
                Err(enroll::Error::UsedToken)
            ),
            "{name}"
        );
        // Same host name: a unique id, never someone else's.
        let second = store
            .redeem(&tokens[1].token, "", "Build Box 1", now)
            .await
            .unwrap();
        assert_eq!(second.credential.node_id, "node-build-box-1-2", "{name}");
        assert!(matches!(
            store
                .redeem(&tokens[2].token, "", "x", now + Duration::hours(2))
                .await,
            Err(enroll::Error::ExpiredToken)
        ));

        // Another Command's token, or a forged one.
        let foreign = Memory::new("")
            .issue_tokens(request("acme"), now)
            .await
            .unwrap();
        assert!(matches!(
            store.redeem(&foreign[0].token, "", "x", now).await,
            Err(enroll::Error::OtherInstance)
        ));
        let forged = format!("{TOKEN_PREFIX}{instance}_{}", "0".repeat(64));
        assert!(matches!(
            store.redeem(&forged, "", "x", now).await,
            Err(enroll::Error::InvalidToken)
        ));

        // The credential authenticates its node only, until revoked.
        let c = store
            .authenticate(&first.credential.node_id, &first.secret)
            .await
            .unwrap();
        assert!(c.active());
        assert!(
            store
                .authenticate(&second.credential.node_id, &first.secret)
                .await
                .is_err()
        );
        store.revoke(&first.credential.node_id, now).await.unwrap();
        assert!(
            store
                .authenticate(&first.credential.node_id, &first.secret)
                .await
                .is_err(),
            "{name}"
        );
    }
}

#[tokio::test]
async fn named_tokens_and_approval() {
    let now = Utc.with_ymd_and_hms(2026, 10, 21, 12, 0, 0).unwrap();
    for (name, store) in stores().await {
        let named = |require_approval| TokenRequest {
            node_name: "gpu-1".into(),
            require_approval,
            ..request("acme")
        };
        assert!(
            store
                .issue_tokens(
                    TokenRequest {
                        count: 2,
                        ..named(false)
                    },
                    now
                )
                .await
                .is_err(),
            "a named token is issued alone"
        );
        assert!(
            store.issue_tokens(request("bad org"), now).await.is_err(),
            "organization ids are checked"
        );
        let tokens = store.issue_tokens(named(true), now).await.unwrap();
        let r = store
            .redeem(&tokens[0].token, "whatever", "host", now)
            .await
            .unwrap();
        assert_eq!(r.credential.node_id, "gpu-1", "{name}");
        assert!(!r.credential.approved);
        assert!(
            !store
                .authenticate("gpu-1", &r.secret)
                .await
                .unwrap()
                .active()
        );
        store.approve("gpu-1", now).await.unwrap();
        assert!(
            store
                .authenticate("gpu-1", &r.secret)
                .await
                .unwrap()
                .active()
        );
        // A second token for the same name waits until gpu-1 is revoked.
        let again = store.issue_tokens(named(false), now).await.unwrap();
        assert!(
            store.redeem(&again[0].token, "", "", now).await.is_err(),
            "{name}: an enrolled name must not be taken over"
        );
        store.revoke("gpu-1", now).await.unwrap();
        let again = store.issue_tokens(named(false), now).await.unwrap();
        let r = store.redeem(&again[0].token, "", "", now).await.unwrap();
        assert_eq!(
            r.credential.node_id, "gpu-1",
            "{name}: a revoked name may enroll again"
        );
        assert_eq!(store.credentials().await.unwrap().len(), 1);
    }
}
