use serde_json::Value;
use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, ProtocolDomain, RuntimeHandle,
    RuntimeInput,
};
use tqsdk_session::{SessionClient, SessionFacadeConfig};
use tqsdk_stream::TqStream;

#[allow(dead_code)]
pub fn seeded_stream() -> TqStream {
    seeded_stream_with_capacity(1024)
}

#[allow(dead_code)]
pub fn seeded_stream_with_capacity(capacity: usize) -> TqStream {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();

    let handle = RuntimeHandle::with_adapters(adapters);
    let session =
        SessionClient::new_for_test_with_handle(handle.clone(), SessionFacadeConfig::default());

    TqStream::new_for_test_with_capacity(session, capacity)
}

#[allow(dead_code)]
pub fn seed_quote_commit(stream: &TqStream, symbol: &str, last_price: f64) {
    seed_quote_commit_with_scope(stream, symbol, last_price, CommitScope::RealtimeUpdate);
}

#[allow(dead_code)]
pub fn seed_quote_commit_with_scope(
    stream: &TqStream,
    symbol: &str,
    last_price: f64,
    scope: CommitScope,
) {
    seed_quote_fields_commit_with_scope(
        stream,
        symbol,
        json!({
            "instrument_id": symbol,
            "last_price": last_price
        }),
        scope,
    );
}

#[allow(dead_code)]
pub fn seed_quote_fields_commit_with_scope(
    stream: &TqStream,
    symbol: &str,
    quote_fields: Value,
    scope: CommitScope,
) {
    stream
        .handle_for_test()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "quotes": {
                            symbol: quote_fields
                        }
                    }]
                })),
            }),
            vec![],
            scope,
        )
        .unwrap()
        .expect("seed quote commit should produce a commit");
}
