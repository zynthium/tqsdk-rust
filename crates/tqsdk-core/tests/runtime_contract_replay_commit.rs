use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, ObjectKey, ProtocolDomain, ReplayEvent,
    ReplaySessionId, ReplayUniverseBatch, ReplayUniverseChange, Runtime, RuntimeHandle,
    RuntimeInput, StatePath,
};

#[test]
fn replay_steps_form_replay_scope_commits() {
    let handle = runtime_with_default_adapters();
    let log = handle.commit_log();

    let commit = handle
        .ingest(
            RuntimeInput::Replay(ReplayEvent {
                label: "step",
                session_id: Some(ReplaySessionId::new("rb-replay")),
                payload: Some(json!({
                    "cursor": {
                        "dt": 1713500000000_i64,
                        "state": "running",
                    }
                })),
            }),
            vec![],
            CommitScope::ReplayStep,
        )
        .unwrap()
        .unwrap();

    assert_eq!(commit.revision.get(), 1);
    assert_eq!(commit.scope, CommitScope::ReplayStep);
    assert_eq!(
        commit.changes.path_hits,
        vec![StatePath::new(["replay", "rb-replay", "cursor"])]
    );
    assert_eq!(
        commit.changes.object_hits,
        vec![ObjectKey::ReplayCursor {
            session_id: ReplaySessionId::new("rb-replay"),
        }]
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["replay", "rb-replay", "cursor", "dt"]),
        Some(&json!(1713500000000_i64))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["replay", "rb-replay", "cursor", "state"]),
        Some(&json!("running"))
    );

    let mut cursor = handle.cursor_from(tqsdk_core::Revision::new(1));
    assert_eq!(
        log.next(&mut cursor).map(|commit| commit.revision.get()),
        Some(1)
    );
    assert_eq!(log.next(&mut cursor), None);
}

#[test]
fn replay_universe_and_market_are_one_atomic_replay_commit() {
    let handle = runtime_with_default_adapters();
    let log = handle.commit_log();
    let session_id = ReplaySessionId::new("dynamic-universe");

    let commit = handle
        .ingest_batch(
            vec![
                ReplayUniverseBatch {
                    session_id: session_id.clone(),
                    effective_ns: 1_000,
                    changes: vec![ReplayUniverseChange {
                        instrument: "SHFE.au2406".to_string(),
                        active: true,
                        readiness: Some("warming_up".to_string()),
                        provenance: Some("catalog:fixture".to_string()),
                    }],
                }
                .into_runtime_input(),
                RuntimeInput::Io(IoEvent {
                    route: "market".to_string(),
                    domains: vec![ProtocolDomain::Market],
                    payload: InputPayload::Json(json!({
                        "aid": "rtn_data",
                        "data": [{
                            "quotes": {"SHFE.au2406": {"last_price": 100.0}}
                        }]
                    })),
                }),
            ],
            vec![],
            CommitScope::ReplayStep,
        )
        .unwrap()
        .unwrap();

    assert_eq!(commit.revision.get(), 1);
    assert_eq!(commit.scope, CommitScope::ReplayStep);
    assert!(commit.changes.path_hits.contains(&StatePath::new([
        "replay",
        "dynamic-universe",
        "universe"
    ])));
    assert!(
        commit
            .changes
            .path_hits
            .contains(&StatePath::new(["quotes", "SHFE.au2406"]))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["replay", "dynamic-universe", "universe", "changes",]),
        Some(&json!([{
            "instrument": "SHFE.au2406",
            "active": true,
            "readiness": "warming_up",
            "provenance": "catalog:fixture",
        }]))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["quotes", "SHFE.au2406", "last_price"]),
        Some(&json!(100.0))
    );

    let mut cursor = handle.cursor_from(tqsdk_core::Revision::new(1));
    assert_eq!(
        log.next(&mut cursor).map(|commit| commit.revision.get()),
        Some(1)
    );
    assert_eq!(log.next(&mut cursor), None);
}

#[test]
fn presorted_replay_step_accepts_only_replay_and_market_mutations() {
    let handle = runtime_with_default_adapters();
    let mutation = ReplayUniverseBatch {
        session_id: ReplaySessionId::new("dynamic-universe"),
        effective_ns: 1_000,
        changes: vec![],
    }
    .into_normalized_mutation();

    let commit = handle
        .ingest_presorted_replay_step_mutations(vec![mutation], vec![], CommitScope::ReplayStep)
        .unwrap()
        .unwrap();
    assert_eq!(commit.revision.get(), 1);

    let error = handle
        .ingest_presorted_replay_step_mutations(
            vec![tqsdk_core::NormalizedMutation {
                path: StatePath::new(["query", "forbidden"]),
                object: None,
                fields: vec![],
                source: tqsdk_core::MutationSource::QueryResult,
            }],
            vec![],
            CommitScope::ReplayStep,
        )
        .unwrap_err();
    assert!(error.to_string().contains("MarketDiff or ReplayStep"));
}

fn runtime_with_default_adapters() -> RuntimeHandle {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();
    RuntimeHandle::with_adapters(registry)
}
