use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, ProtocolDomain, RuntimeHandle,
    RuntimeInput,
};
use tqsdk_session::SessionClient;
use tqsdk_task::{StrategyHost, TaskError, TaskHost, TaskKind};
use tqsdk_wait::TqApi;

fn seeded_host() -> TaskHost {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    let handle = RuntimeHandle::with_adapters(adapters);
    let session = SessionClient::new_for_test_with_handle(handle);
    TaskHost::new(TqApi::new(session))
}

fn seed_account_position_quote(
    host: &TaskHost,
    account_id: &str,
    symbol: &str,
    available: f64,
    net_position: i64,
    last_price: f64,
) {
    let _ = host
        .api()
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
                                "datetime": "2026-04-27 09:30:00.000000",
                                "last_price": last_price
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap();

    let (exchange_id, instrument_id) = symbol
        .split_once('.')
        .expect("test symbol should contain an exchange prefix");
    host.api()
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
                                        "balance": available,
                                        "available": available
                                    }
                                },
                                "positions": {
                                    symbol: {
                                        "user_id": account_id,
                                        "exchange_id": exchange_id,
                                        "instrument_id": instrument_id,
                                        "volume_long": net_position.max(0),
                                        "volume_short": (-net_position).max(0),
                                        "pos_long": net_position.max(0),
                                        "pos_short": (-net_position).max(0),
                                        "pos": net_position
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
        .expect("seed account/position commit should produce a commit");
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_context_reads_quote_account_and_position() {
    let host = seeded_host();
    seed_account_position_quote(&host, "sim", "SHFE.rb2601", 80_000.0, 2, 3_678.0);

    let mut strategy = StrategyHost::builder(host)
        .account("sim")
        .quote("SHFE.rb2601")
        .build()
        .await
        .unwrap();
    let ctx = strategy.next_once().await.unwrap();

    let quote = ctx.quote("SHFE.rb2601").unwrap();
    let account = ctx.account("sim").unwrap();
    let position = ctx.position("sim", "SHFE.rb2601").unwrap();

    assert_eq!(quote.last_price, 3_678.0);
    assert_eq!(account.available, 80_000.0);
    assert_eq!(position.pos_long, 2);
    assert!(ctx.risk().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_context_orders_delegate_to_task_order_builder() {
    let host = seeded_host();
    seed_account_position_quote(&host, "sim", "SHFE.rb2601", 80_000.0, 0, 3_678.0);

    let mut strategy = host
        .strategy()
        .account("sim")
        .quote("SHFE.rb2601")
        .build()
        .await
        .unwrap();
    let mut ctx = strategy.next_once().await.unwrap();

    let ticket = ctx
        .orders("sim")
        .buy_open("SHFE.rb2601", 1)
        .limit(3_678.0)
        .send_once("strategy-entry-1")
        .await
        .unwrap();

    assert!(ticket.was_submitted());
    assert_eq!(ticket.client_order_id(), "strategy-entry-1");
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_context_target_pos_delegates_to_task_host_ownership() {
    let host = seeded_host();
    seed_account_position_quote(&host, "sim", "SHFE.rb2601", 80_000.0, 0, 3_678.0);

    let mut strategy = StrategyHost::builder(host)
        .account("sim")
        .quote("SHFE.rb2601")
        .build()
        .await
        .unwrap();
    let mut ctx = strategy.next_once().await.unwrap();

    let task = ctx.target_pos("sim", "SHFE.rb2601").build().unwrap();
    task.set_target_volume(1).unwrap();

    let err = ctx
        .orders("sim")
        .buy_open("SHFE.rb2601", 1)
        .limit(3_678.0)
        .send_once("blocked-by-target-pos")
        .await
        .unwrap_err();
    assert_eq!(
        err,
        TaskError::ManualOrderBlocked {
            account_id: "sim".to_string(),
            symbol: "SHFE.rb2601".to_string(),
            active_task_kind: TaskKind::TargetPos,
        }
    );
}
