use serde_json::json;
use tqsdk_runtime_contract::{
    AdapterRegistry, CommitScope, IoEvent, ObjectKey, QueryId, Runtime, RuntimeHandle, RuntimeInput, SchemaId,
    StatePath,
};

#[test]
fn bootstrap_inputs_can_merge_into_one_initial_ready_commit() {
    let handle = runtime_with_default_adapters();
    let log = handle.commit_log();

    let commit = handle
        .ingest_batch(
            vec![
                RuntimeInput::Io(IoEvent {
                    route: "instrument-schema".to_string(),
                    domains: vec![tqsdk_runtime_contract::ProtocolDomain::Schema],
                    payload: tqsdk_runtime_contract::InputPayload::Json(json!({
                        "nodes": {
                            "quote": {
                                "fields": ["last_price", "ask_price1"]
                            }
                        }
                    })),
                }),
                RuntimeInput::Io(IoEvent {
                    route: "ins.query".to_string(),
                    domains: vec![tqsdk_runtime_contract::ProtocolDomain::Query],
                    payload: tqsdk_runtime_contract::InputPayload::Json(json!({
                        "aid": "rtn_data",
                        "data": [{
                            "symbols": {
                                "quotes-page-1": {
                                    "items": [{"instrument_id": "au2602"}],
                                    "has_more": false
                                }
                            }
                        }]
                    })),
                }),
            ],
            vec![],
            CommitScope::InitialReady,
        )
        .unwrap()
        .unwrap();

    assert_eq!(commit.revision.get(), 1);
    assert_eq!(commit.scope, CommitScope::InitialReady);
    assert_eq!(
        commit.changes.path_hits,
        vec![
            StatePath::new(["schema", "instrument-schema", "nodes", "quote"]),
            StatePath::new(["query", "quotes-page-1"]),
        ]
    );
    assert_eq!(
        commit.changes.object_hits,
        vec![
            ObjectKey::SchemaNode {
                schema_id: SchemaId::new("instrument-schema"),
            },
            ObjectKey::QueryResult {
                query_id: QueryId::new("quotes-page-1"),
            },
        ]
    );
    assert_eq!(
        handle.latest_snapshot().get(["schema", "instrument-schema", "nodes", "quote", "fields"]),
        Some(&json!(["last_price", "ask_price1"]))
    );
    assert_eq!(
        handle.latest_snapshot().get(["query", "quotes-page-1", "has_more"]),
        Some(&json!(false))
    );
    assert_eq!(
        handle.latest_snapshot().get(["query", "quotes-page-1", "items"]),
        Some(&json!([{ "instrument_id": "au2602" }]))
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
