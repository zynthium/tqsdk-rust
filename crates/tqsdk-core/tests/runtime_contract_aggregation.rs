use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, AggregatedRuntimeReader, CommitScope, InputPayload, IoEvent, ProtocolDomain,
    RuntimeHandle, RuntimeInput, StateSourceId,
};

#[test]
fn aggregated_reader_keeps_two_source_snapshots_and_commits_separate() {
    let primary = runtime_with_default_adapters();
    let backup = runtime_with_default_adapters();

    ingest_quote(&primary, 601.0);
    ingest_quote(&backup, 701.0);

    let mut aggregate = AggregatedRuntimeReader::new();
    let primary_id = StateSourceId::new("primary");
    let backup_id = StateSourceId::new("backup");
    aggregate.insert_source(primary_id.clone(), primary.reader());
    aggregate.insert_source(backup_id.clone(), backup.reader());

    let read = aggregate.read();
    assert_eq!(read.revision(&primary_id).unwrap().get(), 1);
    assert_eq!(read.revision(&backup_id).unwrap().get(), 1);
    assert_eq!(
        read.get(&primary_id, ["quotes", "SHFE.au2602", "last_price"]),
        Some(&json!(601.0))
    );
    assert_eq!(
        read.get(&backup_id, ["quotes", "SHFE.au2602", "last_price"]),
        Some(&json!(701.0))
    );
    drop(read);

    let mut cursor = aggregate.cursor();
    ingest_quote(&primary, 602.0);
    ingest_quote(&backup, 702.0);

    let first = aggregate
        .next(&mut cursor)
        .expect("primary update should be visible through aggregate cursor");
    let second = aggregate
        .next(&mut cursor)
        .expect("backup update should be visible through aggregate cursor");
    assert_eq!(first.source_id.as_str(), "primary");
    assert_eq!(first.commit.revision.get(), 2);
    assert_eq!(second.source_id.as_str(), "backup");
    assert_eq!(second.commit.revision.get(), 2);
    assert!(
        aggregate.next(&mut cursor).is_none(),
        "aggregate cursor should advance each source independently"
    );

    let read = aggregate.read();
    assert_eq!(
        read.get(&primary_id, ["quotes", "SHFE.au2602", "last_price"]),
        Some(&json!(602.0))
    );
    assert_eq!(
        read.get(&backup_id, ["quotes", "SHFE.au2602", "last_price"]),
        Some(&json!(702.0))
    );
}

fn runtime_with_default_adapters() -> RuntimeHandle {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();
    RuntimeHandle::with_adapters(registry)
}

fn ingest_quote(handle: &RuntimeHandle, last_price: f64) {
    handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market.shared".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "quotes": {
                            "SHFE.au2602": {
                                "last_price": last_price
                            }
                        }
                    }]
                })),
            }),
            Vec::new(),
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("quote update should publish a commit");
}
