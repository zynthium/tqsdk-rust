use std::time::Duration;

use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, CommitScope, ContractError, InputPayload, IoEvent, MarketAdapter,
    NormalizedMutation, OutboundFrame, OutboundRequest, ProtocolAdapter, ProtocolDomain,
    RuntimeCommand, RuntimeHandle, RuntimeInput, TradeAdapter, TradeCommand, TradeDirection,
    TradeOffset,
};
use tqsdk_session::SessionClient;
use tqsdk_task::{
    OffsetPriority, PriceMode, TargetPosExecutionReport, TargetPosExecutionStep,
    TargetPosScheduleStep, TargetPosScheduler, TargetPosSchedulerConfig,
    TargetPosSchedulerExecutionEvent, TargetPosTaskExecutionEvent, TaskError, TaskHost, TaskKind,
    VolumeSplitPolicy,
};
use tqsdk_wait::TqApi;

fn seeded_host() -> TaskHost {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    let handle = RuntimeHandle::with_adapters(adapters);
    let session = SessionClient::new_for_test_with_handle(handle);
    TaskHost::new(TqApi::new(session))
}

fn market_only_host() -> TaskHost {
    let mut adapters = AdapterRegistry::new();
    adapters.register_adapter(MarketAdapter::default());
    let handle = RuntimeHandle::with_adapters(adapters);
    let session = SessionClient::new_for_test_with_handle(handle);
    TaskHost::new(TqApi::new(session))
}

fn host_with_trade_adapter<A>(trade_adapter: A) -> TaskHost
where
    A: ProtocolAdapter + 'static,
{
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    adapters.register_adapter(trade_adapter);
    let handle = RuntimeHandle::with_adapters(adapters);
    let session = SessionClient::new_for_test_with_handle(handle);
    TaskHost::new(TqApi::new(session))
}

#[derive(Debug, Default)]
struct FailNthTradeInsertAdapter {
    inner: TradeAdapter,
    fail_on_insert: usize,
    seen_insert_orders: usize,
}

impl FailNthTradeInsertAdapter {
    fn new(fail_on_insert: usize) -> Self {
        Self {
            inner: TradeAdapter,
            fail_on_insert,
            seen_insert_orders: 0,
        }
    }
}

impl ProtocolAdapter for FailNthTradeInsertAdapter {
    fn domain(&self) -> ProtocolDomain {
        ProtocolDomain::Trade
    }

    fn accepts_command(&self, cmd: &RuntimeCommand) -> bool {
        self.inner.accepts_command(cmd)
    }

    fn encode(&mut self, cmd: &RuntimeCommand) -> tqsdk_core::Result<Vec<OutboundRequest>> {
        if matches!(cmd, RuntimeCommand::Trade(TradeCommand::InsertOrder(_))) {
            self.seen_insert_orders += 1;
            if self.seen_insert_orders == self.fail_on_insert {
                return Err(ContractError::validation(format!(
                    "injected trade insert failure at batch order {}",
                    self.seen_insert_orders
                )));
            }
        }

        self.inner.encode(cmd)
    }

    fn accepts_input(&self, input: &RuntimeInput) -> bool {
        self.inner.accepts_input(input)
    }

    fn decode(&mut self, input: &RuntimeInput) -> tqsdk_core::Result<Vec<NormalizedMutation>> {
        self.inner.decode(input)
    }
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

fn seed_position_detail_commit(
    host: &TaskHost,
    account_id: &str,
    symbol: &str,
    pos_long_today: i64,
    pos_long_his: i64,
    pos_short_today: i64,
    pos_short_his: i64,
) {
    let pos_long = pos_long_today + pos_long_his;
    let pos_short = pos_short_today + pos_short_his;
    let pos = pos_long - pos_short;
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
                                        "pos_long_today": pos_long_today,
                                        "pos_long_his": pos_long_his,
                                        "pos_short_today": pos_short_today,
                                        "pos_short_his": pos_short_his,
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
        .expect("seed detailed position commit should produce a commit");
}

#[derive(Clone, Copy)]
struct OrderStatusSeed<'a> {
    direction: &'a str,
    offset: &'a str,
    limit_price: f64,
    status: &'a str,
    volume_orign: i64,
    volume_left: i64,
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
    seed_order_status_commit_with_seed(
        host,
        account_id,
        symbol,
        order_id,
        OrderStatusSeed {
            direction: "BUY",
            offset: "OPEN",
            limit_price: 3678.0,
            status,
            volume_orign,
            volume_left,
        },
    );
}

fn seed_order_status_commit_with_seed(
    host: &TaskHost,
    account_id: &str,
    symbol: &str,
    order_id: &str,
    seed: OrderStatusSeed<'_>,
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
                                        "direction": seed.direction,
                                        "offset": seed.offset,
                                        "volume_orign": seed.volume_orign,
                                        "volume_left": seed.volume_left,
                                        "limit_price": seed.limit_price,
                                        "price_type": "LIMIT",
                                        "volume_condition": "ANY",
                                        "time_condition": "GFD",
                                        "insert_date_time": 1_713_660_000_000_000_000_i64,
                                        "status": seed.status,
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

fn seed_trade_commit(
    host: &TaskHost,
    account_id: &str,
    symbol: &str,
    order_id: &str,
    trade_id: &str,
    volume: i64,
    price: f64,
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
                                "trades": {
                                    trade_id: {
                                        "seqno": 1,
                                        "user_id": account_id,
                                        "order_id": order_id,
                                        "trade_id": trade_id,
                                        "exchange_trade_id": "exchange-trade-1",
                                        "exchange_id": exchange_id,
                                        "instrument_id": instrument_id,
                                        "direction": "BUY",
                                        "offset": "OPEN",
                                        "price": price,
                                        "volume": volume,
                                        "trade_date_time": 1_713_660_000_000_000_000_i64,
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
        .expect("seed trade commit should produce a commit");
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
async fn scheduler_execution_events_include_internal_task_commands() {
    let mut host = seeded_host();
    let scheduler = host
        .target_pos_scheduler("sim", "SHFE.rb2601")
        .steps(vec![
            TargetPosScheduleStep::target(Duration::from_millis(20), 3, PriceMode::Active),
            TargetPosScheduleStep::pause(Duration::from_secs(60)),
        ])
        .build()
        .unwrap();

    seed_quote_commit(&host, "SHFE.rb2601", 3678.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    host.api().handle_for_test().drain_dispatches().unwrap();

    assert_eq!(
        scheduler.execution_events(),
        vec![TargetPosSchedulerExecutionEvent {
            step_index: 0,
            event: TargetPosTaskExecutionEvent::InsertOrder {
                request_seq: 1,
                order_id: "wait-order-1".to_string(),
                direction: TradeDirection::Buy,
                offset: TradeOffset::Open,
                volume: 3,
                limit_price: 3678.0,
            },
        }]
    );

    tokio::time::sleep(Duration::from_millis(25)).await;
    seed_order_status_commit(&host, "sim", "SHFE.rb2601", "wait-order-1", "ALIVE", 3, 3);
    seed_quote_commit(&host, "SHFE.rb2601", 3679.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    assert_eq!(
        scheduler.execution_events(),
        vec![
            TargetPosSchedulerExecutionEvent {
                step_index: 0,
                event: TargetPosTaskExecutionEvent::InsertOrder {
                    request_seq: 1,
                    order_id: "wait-order-1".to_string(),
                    direction: TradeDirection::Buy,
                    offset: TradeOffset::Open,
                    volume: 3,
                    limit_price: 3678.0,
                },
            },
            TargetPosSchedulerExecutionEvent {
                step_index: 0,
                event: TargetPosTaskExecutionEvent::CancelOrder {
                    order_id: "wait-order-1".to_string(),
                },
            },
        ]
    );

    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 1, 3);
    seed_quote_commit(&host, "SHFE.rb2601", 3680.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    scheduler.wait_finished().await.unwrap();

    assert_eq!(
        scheduler.execution_events(),
        vec![
            TargetPosSchedulerExecutionEvent {
                step_index: 0,
                event: TargetPosTaskExecutionEvent::InsertOrder {
                    request_seq: 1,
                    order_id: "wait-order-1".to_string(),
                    direction: TradeDirection::Buy,
                    offset: TradeOffset::Open,
                    volume: 3,
                    limit_price: 3678.0,
                },
            },
            TargetPosSchedulerExecutionEvent {
                step_index: 0,
                event: TargetPosTaskExecutionEvent::CancelOrder {
                    order_id: "wait-order-1".to_string(),
                },
            },
            TargetPosSchedulerExecutionEvent {
                step_index: 0,
                event: TargetPosTaskExecutionEvent::OrderFinished {
                    order_id: "wait-order-1".to_string(),
                    status: "FINISHED".to_string(),
                    filled_volume: 3,
                    remaining_volume: 0,
                    last_msg: String::new(),
                },
            },
        ]
    );
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

    seed_trade_commit(
        &host,
        "sim",
        "SHFE.rb2601",
        "wait-order-1",
        "trade-1",
        2,
        3678.0,
    );
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    seed_position_commit(&host, "sim", "SHFE.rb2601", 2);
    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 1, 2);
    seed_quote_commit(&host, "SHFE.rb2601", 3679.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    scheduler.wait_finished().await.unwrap();
    assert!(scheduler.is_finished());
    assert_eq!(
        scheduler.execution_events(),
        vec![
            TargetPosSchedulerExecutionEvent {
                step_index: 0,
                event: TargetPosTaskExecutionEvent::InsertOrder {
                    request_seq: 1,
                    order_id: "wait-order-1".to_string(),
                    direction: TradeDirection::Buy,
                    offset: TradeOffset::Open,
                    volume: 2,
                    limit_price: 3678.0,
                },
            },
            TargetPosSchedulerExecutionEvent {
                step_index: 0,
                event: TargetPosTaskExecutionEvent::Trade {
                    trade_id: "trade-1".to_string(),
                    order_id: "wait-order-1".to_string(),
                    direction: "BUY".to_string(),
                    offset: "OPEN".to_string(),
                    volume: 2,
                    price: 3678.0,
                    trade_date_time: 1_713_660_000_000_000_000_i64,
                },
            },
            TargetPosSchedulerExecutionEvent {
                step_index: 0,
                event: TargetPosTaskExecutionEvent::OrderFinished {
                    order_id: "wait-order-1".to_string(),
                    status: "FINISHED".to_string(),
                    filled_volume: 2,
                    remaining_volume: 0,
                    last_msg: String::new(),
                },
            },
            TargetPosSchedulerExecutionEvent {
                step_index: 0,
                event: TargetPosTaskExecutionEvent::TargetReached {
                    request_seq: 1,
                    target_volume: 2,
                },
            },
        ]
    );
    host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
        .expect("ownership should be released once the last step reaches target");
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_reprices_stale_live_order_via_internal_target_task() {
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

    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "insert_order");
    assert_eq!(payload["order_id"], "wait-order-1");
    assert_eq!(payload["limit_price"], 3678.0);

    seed_order_status_commit(&host, "sim", "SHFE.rb2601", "wait-order-1", "ALIVE", 2, 2);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3679.0, 3678.0, 3678.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "cancel_order");
    assert_eq!(payload["order_id"], "wait-order-1");

    seed_quote_book_commit(&host, "SHFE.rb2601", 3680.0, 3679.0, 3679.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );

    seed_order_status_commit(
        &host,
        "sim",
        "SHFE.rb2601",
        "wait-order-1",
        "FINISHED",
        2,
        2,
    );
    seed_quote_book_commit(&host, "SHFE.rb2601", 3681.0, 3680.0, 3680.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "insert_order");
    assert_eq!(payload["order_id"], "wait-order-2");
    assert_eq!(payload["limit_price"], 3681.0);

    assert_eq!(
        scheduler.execution_events(),
        vec![
            TargetPosSchedulerExecutionEvent {
                step_index: 0,
                event: TargetPosTaskExecutionEvent::InsertOrder {
                    request_seq: 1,
                    order_id: "wait-order-1".to_string(),
                    direction: TradeDirection::Buy,
                    offset: TradeOffset::Open,
                    volume: 2,
                    limit_price: 3678.0,
                },
            },
            TargetPosSchedulerExecutionEvent {
                step_index: 0,
                event: TargetPosTaskExecutionEvent::CancelOrder {
                    order_id: "wait-order-1".to_string(),
                },
            },
            TargetPosSchedulerExecutionEvent {
                step_index: 0,
                event: TargetPosTaskExecutionEvent::OrderFinished {
                    order_id: "wait-order-1".to_string(),
                    status: "FINISHED".to_string(),
                    filled_volume: 0,
                    remaining_volume: 2,
                    last_msg: String::new(),
                },
            },
            TargetPosSchedulerExecutionEvent {
                step_index: 0,
                event: TargetPosTaskExecutionEvent::InsertOrder {
                    request_seq: 1,
                    order_id: "wait-order-2".to_string(),
                    direction: TradeDirection::Buy,
                    offset: TradeOffset::Open,
                    volume: 2,
                    limit_price: 3681.0,
                },
            },
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_reprices_remaining_batch_order_after_partial_fill() {
    let mut host = seeded_host();
    let scheduler = host
        .target_pos_scheduler("sim", "SHFE.rb2601")
        .steps(vec![TargetPosScheduleStep::target(
            Duration::from_secs(60),
            -1,
            PriceMode::Active,
        )])
        .build()
        .unwrap();

    seed_position_detail_commit(&host, "sim", "SHFE.rb2601", 1, 1, 0, 0);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 2);
    assert_eq!(
        transport_payload(&dispatches[0].request)["order_id"],
        "wait-order-1"
    );
    assert_eq!(
        transport_payload(&dispatches[1].request)["order_id"],
        "wait-order-2"
    );

    seed_position_detail_commit(&host, "sim", "SHFE.rb2601", 0, 1, 0, 0);
    seed_order_status_commit_with_seed(
        &host,
        "sim",
        "SHFE.rb2601",
        "wait-order-1",
        OrderStatusSeed {
            direction: "SELL",
            offset: "CLOSETODAY",
            limit_price: 3677.0,
            status: "FINISHED",
            volume_orign: 1,
            volume_left: 0,
        },
    );
    seed_order_status_commit_with_seed(
        &host,
        "sim",
        "SHFE.rb2601",
        "wait-order-2",
        OrderStatusSeed {
            direction: "SELL",
            offset: "CLOSE",
            limit_price: 3677.0,
            status: "ALIVE",
            volume_orign: 1,
            volume_left: 1,
        },
    );
    seed_quote_book_commit(&host, "SHFE.rb2601", 3677.0, 3676.0, 3676.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "cancel_order");
    assert_eq!(payload["order_id"], "wait-order-2");

    let events = scheduler.execution_events();
    assert_eq!(events.len(), 4);
    assert_eq!(
        events[0],
        TargetPosSchedulerExecutionEvent {
            step_index: 0,
            event: TargetPosTaskExecutionEvent::InsertOrder {
                request_seq: 1,
                order_id: "wait-order-1".to_string(),
                direction: TradeDirection::Sell,
                offset: TradeOffset::CloseToday,
                volume: 1,
                limit_price: 3677.0,
            },
        }
    );
    assert_eq!(
        events[1],
        TargetPosSchedulerExecutionEvent {
            step_index: 0,
            event: TargetPosTaskExecutionEvent::InsertOrder {
                request_seq: 1,
                order_id: "wait-order-2".to_string(),
                direction: TradeDirection::Sell,
                offset: TradeOffset::Close,
                volume: 1,
                limit_price: 3677.0,
            },
        }
    );
    assert_eq!(
        events[2],
        TargetPosSchedulerExecutionEvent {
            step_index: 0,
            event: TargetPosTaskExecutionEvent::OrderFinished {
                order_id: "wait-order-1".to_string(),
                status: "FINISHED".to_string(),
                filled_volume: 1,
                remaining_volume: 0,
                last_msg: String::new(),
            },
        }
    );
    assert_eq!(
        events[3],
        TargetPosSchedulerExecutionEvent {
            step_index: 0,
            event: TargetPosTaskExecutionEvent::CancelOrder {
                order_id: "wait-order-2".to_string(),
            },
        }
    );

    seed_quote_book_commit(&host, "SHFE.rb2601", 3676.0, 3675.0, 3675.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );

    seed_order_status_commit_with_seed(
        &host,
        "sim",
        "SHFE.rb2601",
        "wait-order-2",
        OrderStatusSeed {
            direction: "SELL",
            offset: "CLOSE",
            limit_price: 3677.0,
            status: "FINISHED",
            volume_orign: 1,
            volume_left: 1,
        },
    );
    seed_quote_book_commit(&host, "SHFE.rb2601", 3675.0, 3674.0, 3674.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "insert_order");
    assert_eq!(payload["order_id"], "wait-order-3");
    assert_eq!(payload["direction"], "SELL");
    assert_eq!(payload["offset"], "CLOSE");
    assert_eq!(payload["volume"], 1);
    assert_eq!(payload["limit_price"], 3674.0);

    let events = scheduler.execution_events();
    assert_eq!(events.len(), 6);
    assert_eq!(
        events[4],
        TargetPosSchedulerExecutionEvent {
            step_index: 0,
            event: TargetPosTaskExecutionEvent::OrderFinished {
                order_id: "wait-order-2".to_string(),
                status: "FINISHED".to_string(),
                filled_volume: 0,
                remaining_volume: 1,
                last_msg: String::new(),
            },
        }
    );
    assert_eq!(
        events[5],
        TargetPosSchedulerExecutionEvent {
            step_index: 0,
            event: TargetPosTaskExecutionEvent::InsertOrder {
                request_seq: 1,
                order_id: "wait-order-3".to_string(),
                direction: TradeDirection::Sell,
                offset: TradeOffset::Close,
                volume: 1,
                limit_price: 3674.0,
            },
        }
    );

    let pending = tokio::time::timeout(Duration::from_millis(10), scheduler.wait_finished()).await;
    assert!(pending.is_err());
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
async fn scheduler_pause_step_can_advance_on_timeout_without_new_commit() {
    let mut host = seeded_host();
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

    let scheduler = host
        .target_pos_scheduler("sim", "SHFE.rb2601")
        .steps(vec![
            TargetPosScheduleStep::pause(Duration::from_millis(20)),
            TargetPosScheduleStep::target(Duration::from_secs(60), 1, PriceMode::Active),
        ])
        .build()
        .unwrap();

    let updated = host
        .wait_update(Some(
            tokio::time::Instant::now() + Duration::from_millis(10),
        ))
        .await
        .unwrap();
    assert!(!updated);
    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );

    tokio::time::sleep(Duration::from_millis(25)).await;
    let updated = host
        .wait_update(Some(
            tokio::time::Instant::now() + Duration::from_millis(10),
        ))
        .await
        .unwrap();
    assert!(!updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "insert_order");
    assert_eq!(payload["direction"], "BUY");
    assert_eq!(payload["offset"], "OPEN");
    assert_eq!(payload["volume"], 1);
    assert_eq!(payload["limit_price"], 3678.0);
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

#[tokio::test(flavor = "current_thread")]
async fn scheduler_wait_finished_returns_error_when_step_insert_order_submission_fails() {
    let mut host = market_only_host();
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

    assert!(scheduler.is_finished());
    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
    assert!(matches!(scheduler.last_error(), Some(TaskError::Wait(_))));
    assert!(matches!(
        scheduler.wait_finished().await,
        Err(TaskError::Wait(_))
    ));
    host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
        .expect("ownership should be released after scheduler submit failure");
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_wait_finished_returns_error_when_step_batch_submission_partially_fails() {
    let mut host = host_with_trade_adapter(FailNthTradeInsertAdapter::new(2));
    let scheduler = host
        .target_pos_scheduler("sim", "SHFE.rb2601")
        .steps(vec![TargetPosScheduleStep::target(
            Duration::from_secs(60),
            -1,
            PriceMode::Active,
        )])
        .build()
        .unwrap();

    seed_position_detail_commit(&host, "sim", "SHFE.rb2601", 1, 1, 0, 0);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    assert!(!scheduler.is_finished());
    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["direction"], "SELL");
    assert_eq!(payload["offset"], "CLOSETODAY");
    assert_eq!(payload["order_id"], "wait-order-1");
    assert_eq!(
        scheduler.execution_events(),
        vec![TargetPosSchedulerExecutionEvent {
            step_index: 0,
            event: TargetPosTaskExecutionEvent::InsertOrder {
                request_seq: 1,
                order_id: "wait-order-1".to_string(),
                direction: TradeDirection::Sell,
                offset: TradeOffset::CloseToday,
                volume: 1,
                limit_price: 3677.0,
            },
        }]
    );

    seed_quote_book_commit(&host, "SHFE.rb2601", 3679.0, 3678.0, 3678.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    assert!(!scheduler.is_finished());
    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "cancel_order");
    assert_eq!(payload["order_id"], "wait-order-1");
    assert_eq!(
        scheduler.execution_events(),
        vec![
            TargetPosSchedulerExecutionEvent {
                step_index: 0,
                event: TargetPosTaskExecutionEvent::InsertOrder {
                    request_seq: 1,
                    order_id: "wait-order-1".to_string(),
                    direction: TradeDirection::Sell,
                    offset: TradeOffset::CloseToday,
                    volume: 1,
                    limit_price: 3677.0,
                },
            },
            TargetPosSchedulerExecutionEvent {
                step_index: 0,
                event: TargetPosTaskExecutionEvent::CancelOrder {
                    order_id: "wait-order-1".to_string(),
                },
            },
        ]
    );

    seed_order_status_commit(
        &host,
        "sim",
        "SHFE.rb2601",
        "wait-order-1",
        "FINISHED",
        1,
        1,
    );
    seed_quote_book_commit(&host, "SHFE.rb2601", 3680.0, 3679.0, 3679.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    assert!(scheduler.is_finished());
    assert!(matches!(scheduler.last_error(), Some(TaskError::Wait(_))));
    assert!(matches!(
        scheduler.wait_finished().await,
        Err(TaskError::Wait(_))
    ));
    host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
        .expect("ownership should be released after scheduler partial batch submission failure");
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
