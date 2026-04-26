use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, ProtocolDomain, RuntimeHandle,
    RuntimeInput,
};
use tqsdk_session::{SessionClient, StartupRecoverySpec};

fn seeded_session() -> SessionClient {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    let handle = RuntimeHandle::with_adapters(adapters);
    SessionClient::new_for_test_with_handle(handle)
}

fn seed_quote(session: &SessionClient, symbol: &str) {
    session
        .handle()
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
                                "datetime": "2026-04-26 09:00:00.000000",
                                "last_price": 618.0
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("quote seed should commit");
}

fn seed_trade_account(session: &SessionClient, account_id: &str, trade_more_data: bool) {
    session
        .handle()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "trade".to_string(),
                domains: vec![ProtocolDomain::Trade],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "trade": {
                            account_id: {
                                "trade_more_data": trade_more_data,
                                "accounts": {
                                    "CNY": {
                                        "user_id": account_id,
                                        "currency": "CNY",
                                        "balance": 100000.0,
                                        "available": 80000.0
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
        .expect("trade account seed should commit");
}

#[test]
fn startup_recovery_status_requires_quotes_and_trade_sync_marker() {
    let session = seeded_session();
    let spec = StartupRecoverySpec::new()
        .with_quote_symbols(["SHFE.au2602"])
        .with_trade_accounts(["sim"]);

    let status = session.startup_recovery_status(&spec).unwrap();
    assert!(!status.is_ready());
    assert_eq!(status.missing_quotes, vec!["SHFE.au2602"]);
    assert_eq!(status.pending_trade_accounts, vec!["sim"]);

    seed_quote(&session, "SHFE.au2602");
    seed_trade_account(&session, "sim", true);
    let status = session.startup_recovery_status(&spec).unwrap();
    assert!(status.market_ready);
    assert!(!status.trade_ready);
    assert!(status.missing_quotes.is_empty());
    assert_eq!(status.pending_trade_accounts, vec!["sim"]);

    seed_trade_account(&session, "sim", false);
    let status = session.startup_recovery_status(&spec).unwrap();
    assert!(status.is_ready());
    assert!(status.missing_quotes.is_empty());
    assert!(status.pending_trade_accounts.is_empty());
}
