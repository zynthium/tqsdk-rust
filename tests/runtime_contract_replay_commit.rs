use serde_json::json;
use tqsdk_runtime_contract::{
    AdapterRegistry, CommitScope, ObjectKey, ReplayEvent, ReplaySessionId, Runtime, RuntimeHandle, RuntimeInput,
    StatePath,
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
        handle.latest_snapshot().get(["replay", "rb-replay", "cursor", "dt"]),
        Some(&json!(1713500000000_i64))
    );
    assert_eq!(
        handle.latest_snapshot().get(["replay", "rb-replay", "cursor", "state"]),
        Some(&json!("running"))
    );

    let mut cursor = handle.cursor_from(tqsdk_runtime_contract::Revision::new(1));
    assert_eq!(log.next(&mut cursor).map(|commit| commit.revision.get()), Some(1));
    assert_eq!(log.next(&mut cursor), None);
}

fn runtime_with_default_adapters() -> RuntimeHandle {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();
    RuntimeHandle::with_adapters(registry)
}
