use std::time::Duration;

use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, ProtocolDomain, RuntimeHandle,
    RuntimeInput,
};
use tqsdk_session::SessionClient;
use tqsdk_task::{RiskEngine, RiskRejection, StrategyHost, TaskError, TaskHost, TaskKind};
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

fn seed_ready_kline_and_tick(host: &TaskHost, symbol: &str) {
    host.api()
        .handle_for_test()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "charts": {
                            "wait-kline-SHFE.rb2601-60000000000-16": {
                                "state": {
                                    "ins_list": symbol,
                                    "duration": 60_000_000_000_i64
                                },
                                "left_id": 1,
                                "right_id": 1,
                                "more_data": false,
                                "ready": true
                            },
                            "wait-tick-SHFE.rb2601-16": {
                                "state": {
                                    "ins_list": symbol,
                                    "duration": 0
                                },
                                "left_id": 1,
                                "right_id": 1,
                                "more_data": false,
                                "ready": true
                            }
                        },
                        "klines": {
                            symbol: {
                                "60000000000": {
                                    "data": {
                                        "1": {
                                            "id": 1,
                                            "datetime": 1_000_i64,
                                            "open": 3670.0,
                                            "high": 3680.0,
                                            "low": 3660.0,
                                            "close": 3678.0
                                        }
                                    }
                                }
                            }
                        },
                        "ticks": {
                            symbol: {
                                "data": {
                                    "1": {
                                        "id": 1,
                                        "datetime": 1_100_i64,
                                        "last_price": 3679.0
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
        .expect("seed serial market commit should produce a commit");
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
async fn strategy_context_orders_apply_risk_gate_before_dispatch() {
    let host = seeded_host().with_risk(RiskEngine::new().max_order_volume(1));
    seed_account_position_quote(&host, "sim", "SHFE.rb2601", 80_000.0, 0, 3_678.0);

    let mut strategy = StrategyHost::builder(host)
        .account("sim")
        .quote("SHFE.rb2601")
        .build()
        .await
        .unwrap();
    let mut ctx = strategy.next_once().await.unwrap();
    ctx.task_host()
        .api()
        .handle_for_test()
        .drain_dispatches()
        .unwrap();

    let err = ctx
        .orders("sim")
        .buy_open("SHFE.rb2601", 2)
        .limit(3_678.0)
        .send_once("strategy-risk-rejected")
        .await
        .unwrap_err();

    assert_eq!(
        err,
        TaskError::RiskRejected(RiskRejection::MaxOrderVolumeExceeded {
            account_id: "sim".to_string(),
            symbol: "SHFE.rb2601".to_string(),
            requested: 2,
            max: 1,
        })
    );
    assert!(
        ctx.task_host()
            .api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
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

#[tokio::test(flavor = "current_thread")]
async fn strategy_context_reads_kline_and_tick_windows() {
    let host = seeded_host();
    seed_account_position_quote(&host, "sim", "SHFE.rb2601", 80_000.0, 0, 3_678.0);
    seed_ready_kline_and_tick(&host, "SHFE.rb2601");

    let mut strategy = StrategyHost::builder(host)
        .account("sim")
        .quote("SHFE.rb2601")
        .kline("SHFE.rb2601", Duration::from_secs(60), 16)
        .tick("SHFE.rb2601", 16)
        .build()
        .await
        .unwrap();
    let ctx = strategy.next_once().await.unwrap();

    let klines = ctx.kline("SHFE.rb2601", Duration::from_secs(60)).unwrap();
    let ticks = ctx.tick("SHFE.rb2601").unwrap();

    assert_eq!(klines.len(), 1);
    assert_eq!(klines.last().unwrap().close, 3_678.0);
    assert_eq!(ticks.len(), 1);
    assert_eq!(ticks.last().unwrap().last_price, 3_679.0);
}
