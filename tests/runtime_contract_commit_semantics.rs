use serde_json::json;
use tqsdk_runtime_contract::{
    AdapterRegistry, ChangeHit, CommitScope, IoEvent, ObjectKey, ProtocolDomain, Revision, RuntimeHandle,
    RuntimeInput, StatePath, Symbol, Runtime,
};

#[test]
fn runtime_only_commits_visible_field_changes() {
    let handle = runtime_with_default_adapters();
    let log = handle.commit_log();

    let first = handle
        .ingest(market_quote_input(618.5, 619.0), vec![], CommitScope::RealtimeUpdate)
        .unwrap()
        .unwrap();
    assert_eq!(first.revision, Revision::new(1));

    let repeated = handle
        .ingest(market_quote_input(618.5, 619.0), vec![], CommitScope::RealtimeUpdate)
        .unwrap();
    assert_eq!(repeated, None);
    assert_eq!(handle.latest_snapshot().revision(), Revision::new(1));

    let changed = handle
        .ingest(market_quote_input(619.2, 619.0), vec![], CommitScope::RealtimeUpdate)
        .unwrap()
        .unwrap();
    assert_eq!(changed.revision, Revision::new(2));
    assert_eq!(
        changed.changes.field_hits,
        vec![ChangeHit::field(
            StatePath::new(["quotes", "SHFE.au2602"]),
            ObjectKey::Quote {
                symbol: Symbol::new("SHFE.au2602"),
            },
            "last_price",
        )]
    );
    assert_eq!(
        handle.latest_snapshot().get(["quotes", "SHFE.au2602", "last_price"]),
        Some(&json!(619.2))
    );
    assert_eq!(
        handle.latest_snapshot().get(["quotes", "SHFE.au2602", "ask_price1"]),
        Some(&json!(619.0))
    );

    let mut cursor = handle.cursor_from(Revision::new(1));
    assert_eq!(log.next(&mut cursor).map(|commit| commit.revision), Some(Revision::new(1)));
    assert_eq!(log.next(&mut cursor).map(|commit| commit.revision), Some(Revision::new(2)));
    assert_eq!(log.next(&mut cursor), None);
}

fn runtime_with_default_adapters() -> RuntimeHandle {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();
    RuntimeHandle::with_adapters(registry)
}

fn market_quote_input(last_price: f64, ask_price1: f64) -> RuntimeInput {
    RuntimeInput::Io(IoEvent {
        route: "market.shared".to_string(),
        domains: vec![ProtocolDomain::Market],
        payload: tqsdk_runtime_contract::InputPayload::Json(json!({
            "aid": "rtn_data",
            "data": [{
                "quotes": {
                    "SHFE.au2602": {
                        "last_price": last_price,
                        "ask_price1": ask_price1
                    }
                }
            }]
        })),
    })
}
