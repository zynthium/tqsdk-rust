use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, ProtocolDomain, RuntimeHandle,
    RuntimeInput,
};
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

#[allow(dead_code)]
pub fn seed_ready_tick_chart(api: &mut TqApi, symbol: &str, view_width: usize) {
    let chart_id = format!("wait-tick-{symbol}-{view_width}");
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
                                    "duration": 0,
                                },
                                "left_id": 200,
                                "right_id": 201,
                                "more_data": false,
                                "ready": true,
                            }
                        },
                        "ticks": {
                            symbol: {
                                "data": {
                                    "200": {
                                        "datetime": 1_713_660_000_000_000_000_i64,
                                        "last_price": 618.0,
                                        "average": 618.2,
                                        "highest": 619.0,
                                        "lowest": 617.5,
                                        "ask_price1": 618.2,
                                        "ask_volume1": 4,
                                        "bid_price1": 618.0,
                                        "bid_volume1": 5,
                                        "volume": 12,
                                        "amount": 7416.0,
                                        "open_interest": 101
                                    },
                                    "201": {
                                        "datetime": 1_713_660_000_500_000_000_i64,
                                        "last_price": 618.5,
                                        "average": 618.3,
                                        "highest": 619.2,
                                        "lowest": 617.5,
                                        "ask_price1": 618.6,
                                        "ask_volume1": 3,
                                        "bid_price1": 618.4,
                                        "bid_volume1": 6,
                                        "volume": 15,
                                        "amount": 9277.5,
                                        "open_interest": 102
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
        .expect("seed ready tick chart should produce a commit");

    api.push_deferred_commit_for_test(commit);
}

#[allow(dead_code)]
pub fn seed_trade_snapshot(api: &mut TqApi, account_id: &str, symbol: &str) {
    let commit = api
        .handle_for_test()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "trade".to_string(),
                domains: vec![ProtocolDomain::Trade],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "trade": {
                            account_id: {
                                "accounts": {
                                    "CNY": {
                                        "user_id": account_id,
                                        "currency": "CNY",
                                        "balance": 100000.0,
                                        "available": 80000.0
                                    }
                                },
                                "positions": {
                                    symbol: {
                                        "user_id": account_id,
                                        "exchange_id": "SHFE",
                                        "instrument_id": "ao2602",
                                        "pos_long_today": 1,
                                        "volume_long_today": 1,
                                        "volume_long": 1,
                                        "pos": 1,
                                        "pos_long": 1
                                    }
                                },
                                "orders": {
                                    "order-1": {
                                        "seqno": 1,
                                        "user_id": account_id,
                                        "order_id": "order-1",
                                        "exchange_order_id": "exchange-order-1",
                                        "exchange_id": "SHFE",
                                        "instrument_id": "ao2602",
                                        "direction": "BUY",
                                        "offset": "OPEN",
                                        "volume_orign": 1,
                                        "volume_left": 0,
                                        "limit_price": 618.0,
                                        "price_type": "LIMIT",
                                        "volume_condition": "ANY",
                                        "time_condition": "GFD",
                                        "insert_date_time": 1_713_660_000_000_000_000_i64,
                                        "status": "FINISHED",
                                        "trade_price": 618.0
                                    }
                                },
                                "trades": {
                                    "trade-1": {
                                        "seqno": 1,
                                        "user_id": account_id,
                                        "order_id": "order-1",
                                        "trade_id": "trade-1",
                                        "exchange_trade_id": "exchange-trade-1",
                                        "exchange_id": "SHFE",
                                        "instrument_id": "ao2602",
                                        "direction": "BUY",
                                        "offset": "OPEN",
                                        "price": 618.0,
                                        "volume": 1,
                                        "trade_date_time": 1_713_660_000_100_000_000_i64,
                                        "commission": 1.2
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
        .expect("seed trade snapshot should produce a commit");

    api.push_deferred_commit_for_test(commit);
}
