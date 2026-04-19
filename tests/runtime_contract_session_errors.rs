use serde_json::json;
use tqsdk_runtime_contract::{
    AdapterRegistry, CommitScope, InternalEvent, Runtime, RuntimeHandle, RuntimeInput, StatePath,
};

#[test]
fn session_errors_commit_under_system_namespace() {
    let handle = runtime_with_default_adapters();
    let log = handle.commit_log();

    let commit = handle
        .ingest(
            RuntimeInput::Internal(InternalEvent {
                label: "transport-error",
                payload: Some(json!({
                    "code": "ws_closed",
                    "message": "connection lost",
                })),
            }),
            vec![],
            CommitScope::SessionTransition,
        )
        .unwrap()
        .unwrap();

    assert_eq!(commit.revision.get(), 1);
    assert_eq!(commit.scope, CommitScope::SessionTransition);
    assert_eq!(
        commit.changes.path_hits,
        vec![StatePath::new(["system", "internal", "transport-error"])]
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "internal", "transport-error", "code"]),
        Some(&json!("ws_closed"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "internal", "transport-error", "message"]),
        Some(&json!("connection lost"))
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
