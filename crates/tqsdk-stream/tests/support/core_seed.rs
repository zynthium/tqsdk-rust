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
    seed_quote_fields_commit_on_domains_with_scope(
        stream,
        symbol,
        quote_fields,
        vec![ProtocolDomain::Market],
        scope,
    );
}

#[allow(dead_code)]
pub fn seed_quote_fields_commit_on_domains_with_scope(
    stream: &TqStream,
    symbol: &str,
    quote_fields: Value,
    domains: Vec<ProtocolDomain>,
    scope: CommitScope,
) {
    stream
        .handle_for_test()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains,
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

#[allow(dead_code)]
pub fn seed_trading_status_commit(stream: &TqStream, symbol: &str, trade_status: &str) {
    stream
        .handle_for_test()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "trading_status": {
                            symbol: {
                                "symbol": symbol,
                                "trade_status": trade_status
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("seed trading status commit should produce a commit");
}

#[allow(dead_code)]
pub fn seed_trade_snapshot(stream: &TqStream, account_id: &str, symbol: &str) {
    stream
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
                                        "insert_date_time": 1713660000000000000_i64,
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
                                        "trade_date_time": 1713660000100000000_i64,
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
}
