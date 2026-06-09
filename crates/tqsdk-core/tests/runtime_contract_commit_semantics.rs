use serde_json::json;
use tqsdk_core::{
    AccountId, AdapterRegistry, ChangeHit, CommitScope, IoEvent, ObjectKey, ProtocolDomain,
    Revision, Runtime, RuntimeHandle, RuntimeInput, StatePath, Symbol,
};

#[test]
fn runtime_only_commits_visible_field_changes() {
    let handle = runtime_with_default_adapters();
    let log = handle.commit_log();

    let first = handle
        .ingest(
            market_quote_input(618.5, 619.0),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .unwrap();
    assert_eq!(first.revision, Revision::new(1));

    let repeated = handle
        .ingest(
            market_quote_input(618.5, 619.0),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap();
    assert_eq!(repeated, None);
    assert_eq!(handle.latest_snapshot().revision(), Revision::new(1));

    let changed = handle
        .ingest(
            market_quote_input(619.2, 619.0),
            vec![],
            CommitScope::RealtimeUpdate,
        )
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
        handle
            .latest_snapshot()
            .get(["quotes", "SHFE.au2602", "last_price"]),
        Some(&json!(619.2))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["quotes", "SHFE.au2602", "ask_price1"]),
        Some(&json!(619.0))
    );

    let mut cursor = handle.cursor_from(Revision::new(1));
    assert_eq!(
        log.next(&mut cursor).map(|commit| commit.revision),
        Some(Revision::new(1))
    );
    assert_eq!(
        log.next(&mut cursor).map(|commit| commit.revision),
        Some(Revision::new(2))
    );
    assert_eq!(log.next(&mut cursor), None);
}

#[test]
fn runtime_change_metadata_is_built_from_applied_fields_only() {
    let handle = runtime_with_default_adapters();
    let log = handle.commit_log();

    handle
        .ingest(
            market_quote_input(618.5, 619.0),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .unwrap();

    let changed = handle
        .ingest(
            market_quote_input(618.5, 620.0),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .unwrap();

    let quote_path = StatePath::new(["quotes", "SHFE.au2602"]);
    let quote_object = ObjectKey::Quote {
        symbol: Symbol::new("SHFE.au2602"),
    };
    assert_eq!(changed.revision, Revision::new(2));
    assert_eq!(changed.changes.path_hits, vec![quote_path.clone()]);
    assert_eq!(changed.changes.object_hits, vec![quote_object.clone()]);
    assert_eq!(
        changed.changes.field_hits,
        vec![ChangeHit::field(quote_path, quote_object, "ask_price1")]
    );

    let mut cursor = handle.cursor_from(changed.revision);
    let logged = log
        .next(&mut cursor)
        .expect("commit log should expose the changed commit");
    assert_eq!(
        logged, changed,
        "write-side returned commit and cursor-visible commit should share metadata"
    );
}

#[test]
fn runtime_change_metadata_order_is_stable_for_market_and_trade_payloads() {
    let handle = runtime_with_default_adapters();

    let commit = handle
        .ingest_batch(
            vec![market_serial_input(), trade_account_input()],
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .unwrap();

    let quote_path = StatePath::new(["quotes", "SHFE.au2602"]);
    let kline_parent_path = StatePath::new(["klines", "SHFE.au2602", "60000000000"]);
    let kline_path = StatePath::new(["klines", "SHFE.au2602", "60000000000", "data", "42"]);
    let tick_parent_path = StatePath::new(["ticks", "SHFE.au2602"]);
    let tick_path = StatePath::new(["ticks", "SHFE.au2602", "data", "17"]);
    let account_path = StatePath::new(["trade", "simnow", "accounts", "CNY"]);

    let quote_object = ObjectKey::Quote {
        symbol: Symbol::new("SHFE.au2602"),
    };
    let kline_object = ObjectKey::Kline {
        series: tqsdk_core::SeriesKey {
            primary: Symbol::new("SHFE.au2602"),
            secondary: vec![],
            duration_ns: 60_000_000_000,
            view_width: 0,
            right_id: None,
        },
        bar_id: 42,
    };
    let tick_object = ObjectKey::Tick {
        symbol: Symbol::new("SHFE.au2602"),
        tick_id: 17,
    };
    let account_object = ObjectKey::Account {
        account_id: AccountId::new("simnow"),
    };

    assert_eq!(
        commit.changes.path_hits,
        vec![
            quote_path.clone(),
            kline_parent_path,
            kline_path.clone(),
            tick_parent_path,
            tick_path.clone(),
            account_path.clone(),
        ]
    );
    assert_eq!(
        commit.changes.object_hits,
        vec![
            quote_object.clone(),
            kline_object.clone(),
            tick_object.clone(),
            account_object.clone(),
        ]
    );
    assert_eq!(
        commit.changes.field_hits,
        vec![
            ChangeHit::field(quote_path.clone(), quote_object.clone(), "ask_price1"),
            ChangeHit::field(quote_path, quote_object, "last_price"),
            ChangeHit::field(kline_path.clone(), kline_object.clone(), "close"),
            ChangeHit::field(kline_path.clone(), kline_object.clone(), "id"),
            ChangeHit::field(kline_path, kline_object, "open"),
            ChangeHit::field(tick_path.clone(), tick_object.clone(), "id"),
            ChangeHit::field(tick_path.clone(), tick_object.clone(), "last_price"),
            ChangeHit::field(tick_path, tick_object, "volume"),
            ChangeHit::field(account_path.clone(), account_object.clone(), "available"),
            ChangeHit::field(account_path, account_object, "balance"),
        ]
    );
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
        payload: tqsdk_core::InputPayload::Json(json!({
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

fn market_serial_input() -> RuntimeInput {
    RuntimeInput::Io(IoEvent {
        route: "market.shared".to_string(),
        domains: vec![ProtocolDomain::Market],
        payload: tqsdk_core::InputPayload::Json(json!({
            "aid": "rtn_data",
            "data": [
                {
                    "quotes": {
                        "SHFE.au2602": {
                            "last_price": 618.5,
                            "ask_price1": 619.0
                        }
                    }
                },
                {
                    "klines": {
                        "SHFE.au2602": {
                            "60000000000": {
                                "last_id": 42,
                                "data": {
                                    "42": {
                                        "open": 610.0,
                                        "close": 618.5
                                    }
                                }
                            }
                        }
                    }
                },
                {
                    "ticks": {
                        "SHFE.au2602": {
                            "last_id": 17,
                            "data": {
                                "17": {
                                    "last_price": 618.5,
                                    "volume": 200
                                }
                            }
                        }
                    }
                }
            ]
        })),
    })
}

fn trade_account_input() -> RuntimeInput {
    RuntimeInput::Io(IoEvent {
        route: "trade.simnow".to_string(),
        domains: vec![ProtocolDomain::Trade],
        payload: tqsdk_core::InputPayload::Json(json!({
            "aid": "rtn_data",
            "data": [{
                "trade": {
                    "simnow": {
                        "accounts": {
                            "CNY": {
                                "balance": 100000.0,
                                "available": 80000.0
                            }
                        }
                    }
                }
            }]
        })),
    })
}
