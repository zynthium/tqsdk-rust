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

#[allow(dead_code)]
pub fn seed_trade_extended_snapshot(api: &mut TqApi, account_id: &str, symbol: &str) {
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
                                "pre_insert_orders": {
                                    "pre-1": {
                                        "user_id": account_id,
                                        "order_id": "pre-1",
                                        "exchange_id": "SHFE",
                                        "instrument_id": "ao2602",
                                        "direction": "BUY",
                                        "pre_margin": 1234.5
                                    }
                                },
                                "risk_management_rule": {
                                    "SSE": {
                                        "user_id": account_id,
                                        "exchange_id": "SSE",
                                        "enable": true,
                                        "self_trade": {
                                            "count_limit": 3
                                        },
                                        "frequent_cancellation": {
                                            "insert_order_count_limit": 10,
                                            "cancel_order_count_limit": 2,
                                            "cancel_order_percent_limit": 5.0
                                        },
                                        "trade_position_ratio": {
                                            "trade_units_limit": 100,
                                            "trade_position_ratio_limit": 70.0
                                        }
                                    }
                                },
                                "risk_management_data": {
                                    symbol: {
                                        "user_id": account_id,
                                        "exchange_id": "SHFE",
                                        "instrument_id": "ao2602",
                                        "self_trade": {
                                            "highest_buy_price": 618.0,
                                            "lowest_sell_price": 617.0,
                                            "self_trade_count": 1,
                                            "rejected_count": 0
                                        },
                                        "frequent_cancellation": {
                                            "insert_order_count": 2,
                                            "cancel_order_count": 1,
                                            "cancel_order_percent": 50.0,
                                            "rejected_count": 0
                                        },
                                        "trade_position_ratio": {
                                            "trade_units": 12,
                                            "net_position_units": 4,
                                            "trade_position_ratio": 300.0,
                                            "rejected_count": 1
                                        }
                                    }
                                },
                                "his_settlements": {
                                    "20260420": {
                                        "content": "line-1\nline-2"
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
        .expect("seed extended trade snapshot should produce a commit");

    api.push_deferred_commit_for_test(commit);
}

#[allow(dead_code)]
pub fn seed_risk_management_rule_nested_update(
    api: &mut TqApi,
    account_id: &str,
    exchange_id: &str,
    count_limit: i64,
) {
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
                                "risk_management_rule": {
                                    exchange_id: {
                                        "self_trade": {
                                            "count_limit": count_limit
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
        .expect("seed risk management rule nested update should produce a commit");

    api.push_deferred_commit_for_test(commit);
}

#[allow(dead_code)]
pub fn seed_risk_management_data_nested_update(
    api: &mut TqApi,
    account_id: &str,
    symbol: &str,
    trade_units: i64,
) {
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
                                "risk_management_data": {
                                    symbol: {
                                        "trade_position_ratio": {
                                            "trade_units": trade_units
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
        .expect("seed risk management data nested update should produce a commit");

    api.push_deferred_commit_for_test(commit);
}

#[allow(dead_code)]
pub fn seed_notification_commit(api: &mut TqApi, notification_id: &str) {
    let commit = api
        .handle_for_test()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market.shared".to_string(),
                domains: vec![ProtocolDomain::System],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "notify": {
                            notification_id: {
                                "code": "INFO",
                                "level": "INFO",
                                "type": "MESSAGE",
                                "content": "connected",
                                "bid": "system",
                                "user_id": "sim"
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("seed notification commit should produce a commit");

    api.push_deferred_commit_for_test(commit);
}

#[allow(dead_code)]
pub fn seed_security_trade_snapshot(api: &mut TqApi, account_id: &str, symbol: &str) {
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
                                        "market_value": 12345.0,
                                        "asset": 20000.0,
                                        "available": 15000.0
                                    }
                                },
                                "positions": {
                                    symbol: {
                                        "user_id": account_id,
                                        "exchange_id": "SSE",
                                        "instrument_id": "600000",
                                        "create_date": "20260422",
                                        "volume": 100,
                                        "volume_his": 100,
                                        "market_value": 12345.0
                                    }
                                },
                                "orders": {
                                    "stock-order-1": {
                                        "user_id": account_id,
                                        "order_id": "stock-order-1",
                                        "exchange_order_id": "stock-exchange-order-1",
                                        "exchange_id": "SSE",
                                        "instrument_id": "600000",
                                        "direction": "BUY",
                                        "volume_orign": 100,
                                        "volume_left": 0,
                                        "price_type": "LIMIT",
                                        "limit_price": 123.45,
                                        "frozen_fee": 2.0,
                                        "insert_date_time": 1_713_660_000_000_000_000_i64,
                                        "status": "FINISHED",
                                        "last_msg": "accepted"
                                    }
                                },
                                "trades": {
                                    "stock-trade-1": {
                                        "user_id": account_id,
                                        "trade_id": "stock-trade-1",
                                        "exchange_id": "SSE",
                                        "instrument_id": "600000",
                                        "order_id": "stock-order-1",
                                        "exchange_order_id": "stock-exchange-order-1",
                                        "direction": "BUY",
                                        "volume": 100,
                                        "price": 123.45,
                                        "balance": 12345.0,
                                        "fee": 2.0,
                                        "trade_date_time": 1_713_660_000_100_000_000_i64
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
        .expect("seed security trade snapshot should produce a commit");

    api.push_deferred_commit_for_test(commit);
}
