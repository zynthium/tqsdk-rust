use serde_json::json;
use tqsdk_core::{AdapterRegistry, CommitScope, InputPayload, IoEvent, ProtocolDomain, RuntimeHandle, RuntimeInput};
use tqsdk_session::{SessionClient, SessionFacadeConfig};
use tqsdk_wait::TqApi;

pub fn seeded_api() -> TqApi {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();

    let handle = RuntimeHandle::with_adapters(adapters);
    let session =
        SessionClient::new_for_test_with_handle(handle.clone(), SessionFacadeConfig::default());

    TqApi::new_for_test(handle, session)
}

pub fn seed_quote_commit(api: &mut TqApi, symbol: &str, last_price: f64) {
    let commit = api
        .handle_for_test()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "quotes": {
                            symbol: {
                                "instrument_id": symbol,
                                "last_price": last_price
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("seed quote commit should produce a commit");

    api.push_deferred_commit_for_test(commit);
}
