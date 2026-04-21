use serde_json::json;
use tqsdk_core::{AdapterRegistry, CommitScope, InputPayload, IoEvent, ProtocolDomain, RuntimeHandle, RuntimeInput};
use tqsdk_session::{SessionClient, SessionFacadeConfig};
use tqsdk_wait::TqApi;

#[allow(dead_code)]
pub fn seeded_api() -> TqApi {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();

    let handle = RuntimeHandle::with_adapters(adapters);
    let session =
        SessionClient::new_for_test_with_handle(handle.clone(), SessionFacadeConfig::default());

    TqApi::new_for_test(handle, session)
}

#[allow(dead_code)]
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

#[allow(dead_code)]
pub fn seed_ready_kline_chart(api: &mut TqApi, symbol: &str, duration_ns: i64, view_width: usize) {
    let chart_id = format!("wait-kline-{symbol}-{duration_ns}-{view_width}");
    let commit = api
        .handle_for_test()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "charts": {
                            chart_id: {
                                "state": {
                                    "ins_list": symbol,
                                    "duration": duration_ns,
                                },
                                "left_id": 100,
                                "right_id": 101,
                                "more_data": false,
                                "ready": true,
                            }
                        },
                        "klines": {
                            symbol: {
                                duration_ns.to_string(): {
                                    "data": {
                                        "100": {
                                            "datetime": 1_713_660_000_000_000_000_i64,
                                            "open": 618.0,
                                            "high": 620.0,
                                            "low": 617.0,
                                            "close": 619.0,
                                            "volume": 12,
                                            "open_oi": 100,
                                            "close_oi": 101
                                        },
                                        "101": {
                                            "datetime": 1_713_660_060_000_000_000_i64,
                                            "open": 619.0,
                                            "high": 621.0,
                                            "low": 618.0,
                                            "close": 620.0,
                                            "volume": 15,
                                            "open_oi": 101,
                                            "close_oi": 103
                                        }
                                    }
                                }
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("seed ready kline chart should produce a commit");

    api.push_deferred_commit_for_test(commit);
}
