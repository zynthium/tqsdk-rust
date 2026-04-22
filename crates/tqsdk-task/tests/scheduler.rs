use std::time::Duration;

use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, OutboundFrame, OutboundRequest,
    ProtocolDomain, RuntimeHandle, RuntimeInput, TradeDirection, TradeOffset,
};
use tqsdk_session::{SessionClient, SessionFacadeConfig};
use tqsdk_task::{
    OffsetPriority, PriceMode, TargetPosExecutionReport, TargetPosExecutionStep,
    TargetPosScheduleStep, TargetPosScheduler, TargetPosSchedulerConfig, TaskError, TaskHost,
    TaskKind, VolumeSplitPolicy,
};
use tqsdk_wait::TqApi;

fn seeded_host() -> TaskHost {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    let handle = RuntimeHandle::with_adapters(adapters);
    let session = SessionClient::new_for_test_with_handle(handle, SessionFacadeConfig::default());
    TaskHost::new(TqApi::new(session))
}

fn seed_quote_commit(host: &TaskHost, symbol: &str, last_price: f64) {
    seed_quote_book_commit(host, symbol, last_price, last_price, last_price);
}

fn seed_quote_book_commit(
    host: &TaskHost,
    symbol: &str,
    ask_price1: f64,
    bid_price1: f64,
    last_price: f64,
) {
    host.api()
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
                                "ask_price1": ask_price1,
                                "bid_price1": bid_price1,
                                "last_price": last_price,
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
}

fn seed_position_commit(host: &TaskHost, account_id: &str, symbol: &str, pos: i64) {
    let (pos_long, pos_short) = if pos >= 0 { (pos, 0) } else { (0, -pos) };
    let (exchange_id, instrument_id) = symbol
        .split_once('.')
        .expect("symbol should contain exchange");
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
                                "positions": {
                                    symbol: {
                                        "user_id": account_id,
                                        "exchange_id": exchange_id,
                                        "instrument_id": instrument_id,
                                        "pos": pos,
                                        "pos_long": pos_long,
                                        "pos_short": pos_short,
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
        .expect("seed position commit should produce a commit");
}

fn seed_order_status_commit(
    host: &TaskHost,
    account_id: &str,
    symbol: &str,
    order_id: &str,
    status: &str,
    volume_orign: i64,
    volume_left: i64,
) {
    let (exchange_id, instrument_id) = symbol
        .split_once('.')
        .expect("symbol should contain exchange");
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
                                "orders": {
                                    order_id: {
                                        "seqno": 1,
                                        "user_id": account_id,
                                        "order_id": order_id,
                                        "exchange_order_id": "exchange-order-1",
                                        "exchange_id": exchange_id,
                                        "instrument_id": instrument_id,
                                        "direction": "BUY",
                                        "offset": "OPEN",
                                        "volume_orign": volume_orign,
                                        "volume_left": volume_left,
                                        "limit_price": 3678.0,
                                        "price_type": "LIMIT",
                                        "volume_condition": "ANY",
                                        "time_condition": "GFD",
                                        "insert_date_time": 1_713_660_000_000_000_000_i64,
                                        "status": status,
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
        .expect("seed order status commit should produce a commit");
}

fn seed_wait_order_finished_commit(
    host: &TaskHost,
    account_id: &str,
    symbol: &str,
    order_seq: u64,
    volume_orign: i64,
) {
    let order_id = format!("wait-order-{order_seq}");
    seed_order_status_commit(
        host,
        account_id,
        symbol,
        &order_id,
        "FINISHED",
        volume_orign,
        0,
    );
}

fn transport_payload(request: &OutboundRequest) -> serde_json::Value {
    match request {
        OutboundRequest::Transport(OutboundFrame::Text(text)) => {
            serde_json::from_str(text).expect("transport frame should contain valid json payload")
        }
        OutboundRequest::Transport(OutboundFrame::Binary(bytes)) => serde_json::from_slice(bytes)
            .expect("transport frame should contain valid json payload"),
        other => panic!("expected transport request, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn empty_scheduler_finishes_immediately_and_releases_ownership() {
    let mut host = seeded_host();

    let scheduler: TargetPosScheduler = host
        .target_pos_scheduler("sim", "SHFE.rb2601")
        .steps(Vec::new())
        .build()
        .unwrap();

    scheduler.wait_finished().await.unwrap();
    assert!(scheduler.is_finished());
    assert_eq!(
        scheduler.execution_report(),
        TargetPosExecutionReport {
            applied_steps: vec![]
        }
    );
    host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
        .expect("ownership should be released immediately for empty schedulers");
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_advances_steps_via_host_wait_updates() {
    let mut host = seeded_host();
    let scheduler = host
        .target_pos_scheduler("sim", "SHFE.rb2601")
        .steps(vec![
            TargetPosScheduleStep::target(Duration::from_millis(20), 3, PriceMode::Active),
            TargetPosScheduleStep::target(Duration::from_millis(20), 0, PriceMode::Active),
        ])
        .build()
        .unwrap();

    assert_eq!(
        scheduler.execution_report(),
        TargetPosExecutionReport {
            applied_steps: vec![]
        }
    );

    seed_quote_commit(&host, "SHFE.rb2601", 3678.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    assert_eq!(
        scheduler.execution_report(),
        TargetPosExecutionReport {
            applied_steps: vec![TargetPosExecutionStep {
                step_index: 0,
                target_volume: 3,
            }],
        }
    );
    assert!(!scheduler.is_finished());
    assert_eq!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .len(),
        1
    );

    tokio::time::sleep(Duration::from_millis(25)).await;
    seed_order_status_commit(&host, "sim", "SHFE.rb2601", "wait-order-1", "ALIVE", 3, 3);
    seed_quote_commit(&host, "SHFE.rb2601", 3679.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    assert!(!scheduler.is_finished());

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "cancel_order");
    assert_eq!(payload["order_id"], "wait-order-1");
    assert_eq!(scheduler.execution_report().applied_steps.len(), 1);

    seed_quote_commit(&host, "SHFE.rb2601", 3679.1);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    assert!(!scheduler.is_finished());
    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
    assert_eq!(scheduler.execution_report().applied_steps.len(), 1);

    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 1, 3);
    seed_quote_commit(&host, "SHFE.rb2601", 3680.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    scheduler.wait_finished().await.unwrap();
    assert_eq!(
        scheduler.execution_report(),
        TargetPosExecutionReport {
            applied_steps: vec![
                TargetPosExecutionStep {
                    step_index: 0,
                    target_volume: 3,
                },
                TargetPosExecutionStep {
                    step_index: 1,
                    target_volume: 0,
                },
            ],
        }
    );
    assert!(scheduler.is_finished());
    host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
        .expect("ownership should be released after the last scheduler step");
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_drives_internal_target_task_until_last_step_reaches_target() {
    let mut host = seeded_host();
    let scheduler = host
        .target_pos_scheduler("sim", "SHFE.rb2601")
        .steps(vec![TargetPosScheduleStep::target(
            Duration::from_secs(60),
            2,
            PriceMode::Active,
        )])
        .build()
        .unwrap();

    seed_quote_commit(&host, "SHFE.rb2601", 3678.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    assert!(!scheduler.is_finished());
    assert_eq!(
        scheduler.execution_report(),
        TargetPosExecutionReport {
            applied_steps: vec![TargetPosExecutionStep {
                step_index: 0,
                target_volume: 2,
            }],
        }
    );

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "insert_order");
    assert_eq!(payload["user_id"], "sim");
    assert_eq!(payload["direction"], "BUY");
    assert_eq!(payload["offset"], "OPEN");
    assert_eq!(payload["volume"], 2);
    assert_eq!(payload["limit_price"], 3678.0);

    seed_position_commit(&host, "sim", "SHFE.rb2601", 2);
    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 1, 2);
    seed_quote_commit(&host, "SHFE.rb2601", 3679.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    scheduler.wait_finished().await.unwrap();
    assert!(scheduler.is_finished());
    host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
        .expect("ownership should be released once the last step reaches target");
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_uses_step_passive_price_mode_for_internal_target_task() {
    let mut host = seeded_host();
    let scheduler = host
        .target_pos_scheduler("sim", "SHFE.rb2601")
        .steps(vec![TargetPosScheduleStep::target(
            Duration::from_secs(60),
            1,
            PriceMode::Passive,
        )])
        .build()
        .unwrap();

    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["limit_price"], 3677.0);

    seed_position_commit(&host, "sim", "SHFE.rb2601", 1);
    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 1, 1);
    seed_quote_commit(&host, "SHFE.rb2601", 3679.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    scheduler.wait_finished().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_pause_step_waits_interval_then_advances_without_orders() {
    let mut host = seeded_host();
    let scheduler = host
        .target_pos_scheduler("sim", "SHFE.rb2601")
        .steps(vec![
            TargetPosScheduleStep::pause(Duration::from_millis(20)),
            TargetPosScheduleStep::target(Duration::from_secs(60), 1, PriceMode::Active),
        ])
        .build()
        .unwrap();

    seed_quote_commit(&host, "SHFE.rb2601", 3678.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        scheduler.execution_report(),
        TargetPosExecutionReport {
            applied_steps: vec![TargetPosExecutionStep {
                step_index: 0,
                target_volume: 0,
            }],
        }
    );

    seed_quote_commit(&host, "SHFE.rb2601", 3678.1);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
    assert_eq!(scheduler.execution_report().applied_steps.len(), 1);

    tokio::time::sleep(Duration::from_millis(25)).await;
    seed_quote_commit(&host, "SHFE.rb2601", 3679.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["direction"], "BUY");
    assert_eq!(payload["offset"], "OPEN");
    assert_eq!(payload["volume"], 1);
    assert_eq!(
        scheduler.execution_report(),
        TargetPosExecutionReport {
            applied_steps: vec![
                TargetPosExecutionStep {
                    step_index: 0,
                    target_volume: 0,
                },
                TargetPosExecutionStep {
                    step_index: 1,
                    target_volume: 1,
                },
            ],
        }
    );

    seed_position_commit(&host, "sim", "SHFE.rb2601", 1);
    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 1, 1);
    seed_quote_commit(&host, "SHFE.rb2601", 3680.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    scheduler.wait_finished().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_last_pause_step_finishes_without_submitting_orders() {
    let mut host = seeded_host();
    let scheduler = host
        .target_pos_scheduler("sim", "SHFE.rb2601")
        .steps(vec![TargetPosScheduleStep::pause(Duration::from_secs(60))])
        .build()
        .unwrap();

    seed_quote_commit(&host, "SHFE.rb2601", 3678.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    scheduler.wait_finished().await.unwrap();
    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        scheduler.execution_report(),
        TargetPosExecutionReport {
            applied_steps: vec![TargetPosExecutionStep {
                step_index: 0,
                target_volume: 0,
            }],
        }
    );
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_blocks_guarded_manual_orders_while_active() {
    let mut host = seeded_host();
    let _scheduler = host
        .target_pos_scheduler("sim", "SHFE.rb2601")
        .steps(vec![TargetPosScheduleStep::target(
            Duration::from_secs(60),
            1,
            PriceMode::Active,
        )])
        .build()
        .unwrap();

    let err = host
        .insert_order_guarded(
            "sim",
            "SHFE.rb2601",
            TradeDirection::Buy,
            Some(TradeOffset::Open),
            1,
            Some(json!(3678.0)),
        )
        .await
        .expect_err("manual order should be blocked while scheduler owns the symbol");

    assert_eq!(
        err,
        TaskError::ManualOrderBlocked {
            account_id: "sim".to_string(),
            symbol: "SHFE.rb2601".to_string(),
            active_task_kind: TaskKind::Scheduler,
        }
    );
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_cancel_releases_ownership_and_wait_finished() {
    let mut host = seeded_host();
    let scheduler = host
        .target_pos_scheduler("sim", "SHFE.rb2601")
        .steps(vec![TargetPosScheduleStep::target(
            Duration::from_secs(60),
            1,
            PriceMode::Active,
        )])
        .build()
        .unwrap();

    scheduler.cancel().await.unwrap();
    seed_quote_commit(&host, "SHFE.rb2601", 3678.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    scheduler.wait_finished().await.unwrap();
    assert!(scheduler.is_finished());

    host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
        .expect("ownership should be released after scheduler cancellation");
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_cancel_waits_for_live_order_to_finish_before_releasing_ownership() {
    let mut host = seeded_host();
    let scheduler = host
        .target_pos_scheduler("sim", "SHFE.rb2601")
        .steps(vec![TargetPosScheduleStep::target(
            Duration::from_secs(60),
            1,
            PriceMode::Active,
        )])
        .build()
        .unwrap();

    seed_quote_commit(&host, "SHFE.rb2601", 3678.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    assert_eq!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .len(),
        1
    );

    scheduler.cancel().await.unwrap();
    let pending = tokio::time::timeout(Duration::from_millis(10), scheduler.wait_finished()).await;
    assert!(pending.is_err());

    seed_order_status_commit(&host, "sim", "SHFE.rb2601", "wait-order-1", "ALIVE", 1, 1);
    seed_quote_commit(&host, "SHFE.rb2601", 3679.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    assert!(!scheduler.is_finished());
    assert!(
        host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
            .is_err()
    );

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "cancel_order");
    assert_eq!(payload["order_id"], "wait-order-1");

    seed_quote_commit(&host, "SHFE.rb2601", 3680.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    assert!(!scheduler.is_finished());
    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );

    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 1, 1);
    seed_quote_commit(&host, "SHFE.rb2601", 3681.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    scheduler.wait_finished().await.unwrap();
    assert!(scheduler.is_finished());
    host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
        .expect("ownership should be released after scheduler live order finishes");
}

#[test]
fn scheduler_builder_preserves_explicit_config() {
    let mut host = seeded_host();
    let scheduler = host
        .target_pos_scheduler("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::YesterdayThenOpen)
        .split_policy(VolumeSplitPolicy {
            min_volume: 1,
            max_volume: 4,
        })
        .build()
        .unwrap();

    assert_eq!(
        scheduler.config(),
        &TargetPosSchedulerConfig {
            offset_priority: OffsetPriority::YesterdayThenOpen,
            split_policy: Some(VolumeSplitPolicy {
                min_volume: 1,
                max_volume: 4,
            }),
        }
    );
}

#[test]
fn scheduler_builder_rejects_invalid_split_policy() {
    let mut host = seeded_host();
    let err = host
        .target_pos_scheduler("sim", "SHFE.rb2601")
        .steps(vec![TargetPosScheduleStep::target(
            Duration::from_secs(1),
            1,
            PriceMode::Active,
        )])
        .split_policy(VolumeSplitPolicy {
            min_volume: 5,
            max_volume: 4,
        })
        .build()
        .err()
        .expect("invalid split policy should be rejected");

    assert_eq!(
        err,
        TaskError::Unsupported("split policy min_volume must not exceed max_volume")
    );
}
