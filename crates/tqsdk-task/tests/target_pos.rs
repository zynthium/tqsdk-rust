use std::time::Duration;

use serde_json::json;
use tqsdk_core::adapter::{MarketAdapter, TradeAdapter};
use tqsdk_core::{
    AdapterRegistry, CommitScope, ContractError, InputPayload, IoEvent, NormalizedMutation,
    OutboundFrame, OutboundRequest, ProtocolAdapter, ProtocolDomain, RuntimeCommand, RuntimeHandle,
    RuntimeInput, TradeCommand, TradeDirection, TradeOffset,
};
use tqsdk_session::SessionClient;
use tqsdk_task::{
    OffsetPriority, PriceMode, TargetPosConfig, TargetPosTaskExecutionEvent,
    TargetPosTaskOrderReport, TargetPosTaskReachedTarget, TargetPosTaskTradeFill, TaskError,
    TaskHost, TaskKind, VolumeSplitPolicy,
};
use tqsdk_wait::TqApi;

#[test]
fn target_pos_task_inner_uses_dedicated_runtime_state_wrapper() {
    let source = include_str!("../src/target_pos.rs");
    let inner = source
        .split("struct TargetPosTaskInner {")
        .nth(1)
        .and_then(|tail| tail.split("impl TargetPosBuilder").next())
        .expect("TargetPosTaskInner source block should be present");

    let direct_mutex_fields = inner
        .lines()
        .filter(|line| line.trim_start().contains(": Mutex<"))
        .count();

    assert_eq!(
        direct_mutex_fields, 0,
        "TargetPosTaskInner should keep mutable task runtime state behind a dedicated state wrapper"
    );
    assert!(
        !source.contains("fn state(&self) -> std::sync::MutexGuard"),
        "TargetPosTaskInner should not expose raw MutexGuard access"
    );
    assert!(
        !inner.contains("Arc<Mutex"),
        "TargetPosTaskInner should access shared state through wrappers"
    );
}

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

fn seed_quote_commit(host: &TaskHost, symbol: &str, last_price: f64) {
    seed_quote_book_commit(host, symbol, last_price + 1.0, last_price - 1.0, last_price);
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
                                        "exchange_id": symbol.split_once('.').expect("symbol should contain exchange").0,
                                        "instrument_id": symbol.split_once('.').expect("symbol should contain exchange").1,
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

#[tokio::test(flavor = "current_thread")]
async fn target_pos_task_owns_symbol_until_cancelled() {
    let mut host = seeded_host();
    let task = host.target_pos("sim", "SHFE.rb2601").build().unwrap();

    let err = host
        .register_scheduler_owner_for_test("sim", "SHFE.rb2601")
        .expect_err("scheduler should not take ownership while target task is active");
    assert_eq!(
        err,
        TaskError::OwnershipConflict {
            account_id: "sim".to_string(),
            symbol: "SHFE.rb2601".to_string(),
            active_task_kind: TaskKind::TargetPos,
        }
    );

    task.cancel().await.unwrap();
    seed_quote_commit(&host, "SHFE.rb2601", 3678.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
        .expect("manual order should be allowed after target task cancellation");
}

#[test]
fn target_pos_task_tracks_latest_requested_target_volume() {
    let mut host = seeded_host();
    let task = host.target_pos("sim", "SHFE.rb2601").build().unwrap();

    assert_eq!(task.current_target_volume(), None);

    task.set_target_volume(5).unwrap();
    assert_eq!(task.current_target_volume(), Some(5));

    task.set_target_volume(8).unwrap();
    assert_eq!(task.current_target_volume(), Some(8));
}

#[test]
fn dropping_target_pos_task_releases_ownership() {
    let mut host = seeded_host();

    {
        let _task = host.target_pos("sim", "SHFE.rb2601").build().unwrap();
        assert!(
            host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
                .is_err()
        );
    }

    host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
        .expect("ownership should be released after the last task handle drops");
}

#[tokio::test(flavor = "current_thread")]
async fn target_pos_task_reaches_target_only_after_host_wait_update() {
    let mut host = seeded_host();
    let task = host.target_pos("sim", "SHFE.rb2601").build().unwrap();

    task.set_target_volume(5).unwrap();

    let pending = tokio::time::timeout(Duration::from_millis(10), task.wait_target_reached()).await;
    assert!(pending.is_err());
    assert_eq!(task.applied_target_volume_for_test(), None);

    seed_quote_commit(&host, "SHFE.rb2601", 3678.0);

    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "insert_order");
    assert_eq!(payload["direction"], "BUY");
    assert_eq!(payload["offset"], "OPEN");
    assert_eq!(payload["volume"], 5);

    let pending = tokio::time::timeout(Duration::from_millis(10), task.wait_target_reached()).await;
    assert!(pending.is_err());

    seed_position_commit(&host, "sim", "SHFE.rb2601", 5);
    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 1, 5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    task.wait_target_reached().await.unwrap();
    assert_eq!(task.applied_target_volume_for_test(), Some(5));
}

#[tokio::test(flavor = "current_thread")]
async fn target_pos_wait_update_subscribes_quote_before_pricing_when_quote_missing() {
    let mut host = seeded_host();
    let task = host.target_pos("sim", "SHFE.rb2601").build().unwrap();

    task.set_target_volume(2).unwrap();

    let updated = host
        .wait_update(Some(
            tokio::time::Instant::now() + Duration::from_millis(10),
        ))
        .await
        .unwrap();
    assert!(!updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 2);
    let subscribe = transport_payload(&dispatches[0].request);
    assert_eq!(subscribe["aid"], "subscribe_quote");
    assert_eq!(subscribe["ins_list"], "SHFE.rb2601");
    let peek = transport_payload(&dispatches[1].request);
    assert_eq!(peek["aid"], "peek_message");
    assert_eq!(task.execution_report().submitted_order_count, 0);

    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "insert_order");
    assert_eq!(payload["direction"], "BUY");
    assert_eq!(payload["offset"], "OPEN");
    assert_eq!(payload["volume"], 2);
    assert_eq!(payload["limit_price"], 3678.0);
}

#[tokio::test(flavor = "current_thread")]
async fn host_wait_update_timeout_still_advances_target_pos_with_existing_quote() {
    let mut host = seeded_host();
    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );

    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::OpenOnly)
        .build()
        .unwrap();
    task.set_target_volume(2).unwrap();

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
    assert_eq!(payload["volume"], 2);
    assert_eq!(payload["limit_price"], 3678.0);
}

#[tokio::test(flavor = "current_thread")]
async fn host_wait_update_applies_latest_target_request_only() {
    let mut host = seeded_host();
    let task = host.target_pos("sim", "SHFE.rb2601").build().unwrap();

    task.set_target_volume(5).unwrap();
    task.set_target_volume(8).unwrap();
    seed_quote_commit(&host, "SHFE.rb2601", 3679.0);

    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "insert_order");
    assert_eq!(payload["direction"], "BUY");
    assert_eq!(payload["offset"], "OPEN");
    assert_eq!(payload["volume"], 8);

    let pending = tokio::time::timeout(Duration::from_millis(10), task.wait_target_reached()).await;
    assert!(pending.is_err());

    seed_position_commit(&host, "sim", "SHFE.rb2601", 8);
    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 1, 8);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    task.wait_target_reached().await.unwrap();
    assert_eq!(task.applied_target_volume_for_test(), Some(8));
    assert_eq!(task.current_target_volume(), Some(8));
}

#[tokio::test(flavor = "current_thread")]
async fn target_pos_task_wait_finished_resolves_after_cancel() {
    let mut host = seeded_host();
    let task = host.target_pos("sim", "SHFE.rb2601").build().unwrap();

    let pending = tokio::time::timeout(Duration::from_millis(10), task.wait_finished()).await;
    assert!(pending.is_err());

    task.cancel().await.unwrap();
    let pending = tokio::time::timeout(Duration::from_millis(10), task.wait_finished()).await;
    assert!(pending.is_err());

    seed_quote_commit(&host, "SHFE.rb2601", 3678.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    task.wait_finished().await.unwrap();
    assert!(task.is_finished());

    host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
        .expect("ownership should be released after cancellation");
}

#[test]
fn target_pos_builder_preserves_explicit_config() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .price_mode(PriceMode::Passive)
        .offset_priority(OffsetPriority::OpenOnly)
        .split_policy(VolumeSplitPolicy {
            min_volume: 2,
            max_volume: 10,
        })
        .build()
        .unwrap();

    assert_eq!(
        task.config(),
        &TargetPosConfig {
            price_mode: PriceMode::Passive,
            offset_priority: OffsetPriority::OpenOnly,
            split_policy: Some(VolumeSplitPolicy {
                min_volume: 2,
                max_volume: 10,
            }),
        }
    );
}

#[test]
fn target_pos_builder_rejects_invalid_split_policy() {
    let mut host = seeded_host();
    let err = host
        .target_pos("sim", "SHFE.rb2601")
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

#[tokio::test(flavor = "current_thread")]
async fn open_only_target_pos_submits_buy_open_order_with_active_price() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::OpenOnly)
        .build()
        .unwrap();
    task.set_target_volume(2).unwrap();

    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "insert_order");
    assert_eq!(payload["user_id"], "sim");
    assert_eq!(payload["direction"], "BUY");
    assert_eq!(payload["offset"], "OPEN");
    assert_eq!(payload["volume"], 2);
    assert_eq!(payload["limit_price"], 3678.0);

    let pending = tokio::time::timeout(Duration::from_millis(10), task.wait_target_reached()).await;
    assert!(pending.is_err());

    seed_position_commit(&host, "sim", "SHFE.rb2601", 2);
    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 1, 2);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    task.wait_target_reached().await.unwrap();
    assert_eq!(task.applied_target_volume_for_test(), Some(2));
}

#[tokio::test(flavor = "current_thread")]
async fn open_only_target_pos_uses_passive_price_for_buy_orders() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .price_mode(PriceMode::Passive)
        .offset_priority(OffsetPriority::OpenOnly)
        .build()
        .unwrap();
    task.set_target_volume(1).unwrap();

    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["limit_price"], 3677.0);
}

#[tokio::test(flavor = "current_thread")]
async fn open_only_target_pos_splits_large_orders_by_split_policy() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::OpenOnly)
        .split_policy(VolumeSplitPolicy {
            min_volume: 5,
            max_volume: 10,
        })
        .build()
        .unwrap();
    task.set_target_volume(11).unwrap();

    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["direction"], "BUY");
    assert_eq!(payload["offset"], "OPEN");
    assert_eq!(payload["volume"], 6);

    seed_position_commit(&host, "sim", "SHFE.rb2601", 6);
    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 1, 6);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3679.0, 3678.0, 3678.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["direction"], "BUY");
    assert_eq!(payload["offset"], "OPEN");
    assert_eq!(payload["volume"], 5);

    seed_position_commit(&host, "sim", "SHFE.rb2601", 11);
    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 2, 5);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3680.0, 3679.0, 3679.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    task.wait_target_reached().await.unwrap();
    assert_eq!(task.applied_target_volume_for_test(), Some(11));
}

#[tokio::test(flavor = "current_thread")]
async fn open_only_target_pos_does_not_submit_order_when_position_already_matches_target() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::OpenOnly)
        .build()
        .unwrap();
    task.set_target_volume(2).unwrap();

    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    seed_position_commit(&host, "sim", "SHFE.rb2601", 2);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
    task.wait_target_reached().await.unwrap();
    assert_eq!(task.applied_target_volume_for_test(), Some(2));
}

#[tokio::test(flavor = "current_thread")]
async fn open_only_target_pos_does_not_resubmit_same_request_on_later_updates() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::OpenOnly)
        .build()
        .unwrap();
    task.set_target_volume(2).unwrap();

    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
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

    seed_quote_book_commit(&host, "SHFE.rb2601", 3679.0, 3678.0, 3678.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );

    let pending = tokio::time::timeout(Duration::from_millis(10), task.wait_target_reached()).await;
    assert!(pending.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn open_only_target_pos_waits_for_live_order_to_finish_before_resubmitting() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::OpenOnly)
        .build()
        .unwrap();
    task.set_target_volume(2).unwrap();

    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["volume"], 2);

    seed_position_commit(&host, "sim", "SHFE.rb2601", 1);
    seed_order_status_commit(&host, "sim", "SHFE.rb2601", "wait-order-1", "ALIVE", 2, 1);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3676.0, 3677.0);
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
        0,
    );
    seed_quote_book_commit(&host, "SHFE.rb2601", 3680.0, 3679.0, 3679.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["direction"], "BUY");
    assert_eq!(payload["offset"], "OPEN");
    assert_eq!(payload["volume"], 1);
}

#[tokio::test(flavor = "current_thread")]
async fn open_only_target_pos_cancels_stale_live_order_before_repricing() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::OpenOnly)
        .build()
        .unwrap();
    task.set_target_volume(2).unwrap();

    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
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
}

#[tokio::test(flavor = "current_thread")]
async fn open_only_target_pos_resubmits_after_terminal_order_without_position_change() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::OpenOnly)
        .build()
        .unwrap();
    task.set_target_volume(2).unwrap();

    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["direction"], "BUY");
    assert_eq!(payload["offset"], "OPEN");
    assert_eq!(payload["volume"], 2);

    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 1, 2);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3679.0, 3678.0, 3678.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["direction"], "BUY");
    assert_eq!(payload["offset"], "OPEN");
    assert_eq!(payload["volume"], 2);

    let pending = tokio::time::timeout(Duration::from_millis(10), task.wait_target_reached()).await;
    assert!(pending.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn open_only_target_pos_retargets_to_current_position_by_canceling_live_order() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::OpenOnly)
        .build()
        .unwrap();
    task.set_target_volume(2).unwrap();

    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    host.api().handle_for_test().drain_dispatches().unwrap();

    seed_position_commit(&host, "sim", "SHFE.rb2601", 1);
    seed_order_status_commit(&host, "sim", "SHFE.rb2601", "wait-order-1", "ALIVE", 2, 1);
    task.set_target_volume(1).unwrap();
    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3676.0, 3677.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "cancel_order");
    assert_eq!(payload["order_id"], "wait-order-1");

    let pending = tokio::time::timeout(Duration::from_millis(10), task.wait_target_reached()).await;
    assert!(pending.is_err());

    seed_order_status_commit(
        &host,
        "sim",
        "SHFE.rb2601",
        "wait-order-1",
        "FINISHED",
        2,
        1,
    );
    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3675.0, 3676.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );

    task.wait_target_reached().await.unwrap();
    assert_eq!(task.applied_target_volume_for_test(), Some(1));
}

#[tokio::test(flavor = "current_thread")]
async fn open_only_target_pos_retarget_cancels_unmaterialized_live_order_before_reaching_target() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::OpenOnly)
        .build()
        .unwrap();
    task.set_target_volume(2).unwrap();

    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    assert_eq!(
        transport_payload(&dispatches[0].request)["order_id"],
        "wait-order-1"
    );

    task.set_target_volume(0).unwrap();
    seed_quote_book_commit(&host, "SHFE.rb2601", 3679.0, 3678.0, 3678.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "cancel_order");
    assert_eq!(payload["order_id"], "wait-order-1");

    let pending = tokio::time::timeout(Duration::from_millis(10), task.wait_target_reached()).await;
    assert!(pending.is_err());
    assert_eq!(task.applied_target_volume_for_test(), None);

    seed_order_status_commit(
        &host,
        "sim",
        "SHFE.rb2601",
        "wait-order-1",
        "FINISHED",
        2,
        2,
    );
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
    task.wait_target_reached().await.unwrap();
    assert_eq!(task.applied_target_volume_for_test(), Some(0));
    assert_eq!(task.execution_report().cancel_request_count, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn open_only_target_pos_repeated_same_target_does_not_duplicate_unmaterialized_order() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::OpenOnly)
        .build()
        .unwrap();
    task.set_target_volume(2).unwrap();

    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    assert_eq!(
        transport_payload(&dispatches[0].request)["order_id"],
        "wait-order-1"
    );

    task.set_target_volume(2).unwrap();
    seed_quote_book_commit(&host, "SHFE.rb2601", 3679.0, 3678.0, 3678.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
    assert_eq!(task.execution_report().submitted_order_count, 1);
    assert_eq!(task.execution_report().cancel_request_count, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn target_pos_cancel_waits_for_live_order_to_finish_before_releasing_ownership() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::OpenOnly)
        .build()
        .unwrap();
    task.set_target_volume(2).unwrap();

    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
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

    task.cancel().await.unwrap();
    let pending = tokio::time::timeout(Duration::from_millis(10), task.wait_finished()).await;
    assert!(pending.is_err());

    seed_order_status_commit(&host, "sim", "SHFE.rb2601", "wait-order-1", "ALIVE", 2, 2);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3679.0, 3678.0, 3678.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    assert!(!task.is_finished());
    assert!(
        host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
            .is_err()
    );

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "cancel_order");
    assert_eq!(payload["order_id"], "wait-order-1");

    seed_quote_book_commit(&host, "SHFE.rb2601", 3680.0, 3679.0, 3679.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    assert!(!task.is_finished());
    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );

    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 1, 2);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3681.0, 3680.0, 3680.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    task.wait_finished().await.unwrap();
    assert!(task.is_finished());
    host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
        .expect("ownership should be released after live order finishes");
}

#[tokio::test(flavor = "current_thread")]
async fn target_pos_execution_report_records_insert_and_cancel_requests() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::OpenOnly)
        .build()
        .unwrap();
    task.set_target_volume(2).unwrap();

    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    host.api().handle_for_test().drain_dispatches().unwrap();

    assert_eq!(
        task.execution_report().events,
        vec![TargetPosTaskExecutionEvent::InsertOrder {
            request_seq: 1,
            order_id: "wait-order-1".to_string(),
            direction: TradeDirection::Buy,
            offset: TradeOffset::Open,
            volume: 2,
            limit_price: 3678.0,
        }]
    );

    task.cancel().await.unwrap();
    seed_order_status_commit(&host, "sim", "SHFE.rb2601", "wait-order-1", "ALIVE", 2, 2);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3679.0, 3678.0, 3678.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    host.api().handle_for_test().drain_dispatches().unwrap();

    assert_eq!(
        task.execution_report().events,
        vec![
            TargetPosTaskExecutionEvent::InsertOrder {
                request_seq: 1,
                order_id: "wait-order-1".to_string(),
                direction: TradeDirection::Buy,
                offset: TradeOffset::Open,
                volume: 2,
                limit_price: 3678.0,
            },
            TargetPosTaskExecutionEvent::CancelOrder {
                order_id: "wait-order-1".to_string(),
            },
        ]
    );

    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 1, 2);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3680.0, 3679.0, 3679.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    task.wait_finished().await.unwrap();

    assert_eq!(
        task.execution_report().events,
        vec![
            TargetPosTaskExecutionEvent::InsertOrder {
                request_seq: 1,
                order_id: "wait-order-1".to_string(),
                direction: TradeDirection::Buy,
                offset: TradeOffset::Open,
                volume: 2,
                limit_price: 3678.0,
            },
            TargetPosTaskExecutionEvent::CancelOrder {
                order_id: "wait-order-1".to_string(),
            },
            TargetPosTaskExecutionEvent::OrderFinished {
                order_id: "wait-order-1".to_string(),
                status: "FINISHED".to_string(),
                filled_volume: 2,
                remaining_volume: 0,
                last_msg: String::new(),
            },
        ]
    );
    assert_eq!(
        task.execution_report().orders,
        vec![TargetPosTaskOrderReport {
            request_seq: 1,
            order_id: "wait-order-1".to_string(),
            direction: TradeDirection::Buy,
            offset: TradeOffset::Open,
            requested_volume: 2,
            limit_price: 3678.0,
            cancel_requested: true,
            status: Some("FINISHED".to_string()),
            filled_volume: 2,
            remaining_volume: 0,
            last_msg: Some(String::new()),
            trade_count: 0,
            filled_turnover: 0.0,
        }]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn target_pos_execution_report_records_terminal_order_and_target_reached() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::OpenOnly)
        .build()
        .unwrap();
    task.set_target_volume(2).unwrap();

    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    host.api().handle_for_test().drain_dispatches().unwrap();

    seed_position_commit(&host, "sim", "SHFE.rb2601", 2);
    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 1, 2);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3679.0, 3678.0, 3678.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    task.wait_target_reached().await.unwrap();

    assert_eq!(
        task.execution_report().events,
        vec![
            TargetPosTaskExecutionEvent::InsertOrder {
                request_seq: 1,
                order_id: "wait-order-1".to_string(),
                direction: TradeDirection::Buy,
                offset: TradeOffset::Open,
                volume: 2,
                limit_price: 3678.0,
            },
            TargetPosTaskExecutionEvent::OrderFinished {
                order_id: "wait-order-1".to_string(),
                status: "FINISHED".to_string(),
                filled_volume: 2,
                remaining_volume: 0,
                last_msg: String::new(),
            },
            TargetPosTaskExecutionEvent::TargetReached {
                request_seq: 1,
                target_volume: 2,
            },
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn target_pos_execution_report_records_trade_events_from_commit_deltas() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::OpenOnly)
        .build()
        .unwrap();
    task.set_target_volume(2).unwrap();

    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    host.api().handle_for_test().drain_dispatches().unwrap();

    seed_trade_commit(
        &host,
        "sim",
        "SHFE.rb2601",
        "wait-order-1",
        "trade-1",
        1,
        3678.0,
    );
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    assert_eq!(
        task.execution_report().events,
        vec![
            TargetPosTaskExecutionEvent::InsertOrder {
                request_seq: 1,
                order_id: "wait-order-1".to_string(),
                direction: TradeDirection::Buy,
                offset: TradeOffset::Open,
                volume: 2,
                limit_price: 3678.0,
            },
            TargetPosTaskExecutionEvent::Trade {
                trade_id: "trade-1".to_string(),
                order_id: "wait-order-1".to_string(),
                direction: "BUY".to_string(),
                offset: "OPEN".to_string(),
                volume: 1,
                price: 3678.0,
                trade_date_time: 1_713_660_000_000_000_000_i64,
            },
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn target_pos_execution_report_accumulates_trade_buffer_and_summary() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::OpenOnly)
        .build()
        .unwrap();
    task.set_target_volume(2).unwrap();

    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    host.api().handle_for_test().drain_dispatches().unwrap();

    seed_trade_commit(
        &host,
        "sim",
        "SHFE.rb2601",
        "wait-order-1",
        "trade-1",
        1,
        3678.0,
    );
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    seed_trade_commit(
        &host,
        "sim",
        "SHFE.rb2601",
        "wait-order-1",
        "trade-2",
        1,
        3679.0,
    );
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    seed_position_commit(&host, "sim", "SHFE.rb2601", 2);
    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 1, 2);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3680.0, 3679.0, 3679.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    task.wait_target_reached().await.unwrap();

    let report = task.execution_report();
    assert_eq!(report.submitted_order_count, 1);
    assert_eq!(report.cancel_request_count, 0);
    assert_eq!(report.finished_order_count, 1);
    assert_eq!(report.filled_volume, 2);
    assert_eq!(report.filled_turnover, 7357.0);
    assert_eq!(
        report.last_reached_target,
        Some(TargetPosTaskReachedTarget {
            request_seq: 1,
            target_volume: 2,
        })
    );
    assert_eq!(
        report.trades,
        vec![
            TargetPosTaskTradeFill {
                trade_id: "trade-1".to_string(),
                order_id: "wait-order-1".to_string(),
                direction: "BUY".to_string(),
                offset: "OPEN".to_string(),
                volume: 1,
                price: 3678.0,
                trade_date_time: 1_713_660_000_000_000_000_i64,
            },
            TargetPosTaskTradeFill {
                trade_id: "trade-2".to_string(),
                order_id: "wait-order-1".to_string(),
                direction: "BUY".to_string(),
                offset: "OPEN".to_string(),
                volume: 1,
                price: 3679.0,
                trade_date_time: 1_713_660_000_000_000_000_i64,
            },
        ]
    );
    assert_eq!(
        report.orders,
        vec![TargetPosTaskOrderReport {
            request_seq: 1,
            order_id: "wait-order-1".to_string(),
            direction: TradeDirection::Buy,
            offset: TradeOffset::Open,
            requested_volume: 2,
            limit_price: 3678.0,
            cancel_requested: false,
            status: Some("FINISHED".to_string()),
            filled_volume: 2,
            remaining_volume: 0,
            last_msg: Some(String::new()),
            trade_count: 2,
            filled_turnover: 7357.0,
        }]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn target_pos_execution_cursor_reads_only_new_events_and_trades() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::OpenOnly)
        .build()
        .unwrap();
    task.set_target_volume(2).unwrap();

    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    host.api().handle_for_test().drain_dispatches().unwrap();

    let (event_cursor, initial_events) = task.execution_events_since(0);
    assert_eq!(event_cursor, 1);
    assert_eq!(initial_events.len(), 1);

    let (trade_cursor, initial_trades) = task.execution_trades_since(0);
    assert_eq!(trade_cursor, 0);
    assert!(initial_trades.is_empty());

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

    let (next_event_cursor, new_events) = task.execution_events_since(event_cursor);
    assert_eq!(next_event_cursor, 2);
    assert_eq!(
        new_events,
        vec![TargetPosTaskExecutionEvent::Trade {
            trade_id: "trade-1".to_string(),
            order_id: "wait-order-1".to_string(),
            direction: "BUY".to_string(),
            offset: "OPEN".to_string(),
            volume: 2,
            price: 3678.0,
            trade_date_time: 1_713_660_000_000_000_000_i64,
        }]
    );

    let (next_trade_cursor, new_trades) = task.execution_trades_since(trade_cursor);
    assert_eq!(next_trade_cursor, 1);
    assert_eq!(
        new_trades,
        vec![TargetPosTaskTradeFill {
            trade_id: "trade-1".to_string(),
            order_id: "wait-order-1".to_string(),
            direction: "BUY".to_string(),
            offset: "OPEN".to_string(),
            volume: 2,
            price: 3678.0,
            trade_date_time: 1_713_660_000_000_000_000_i64,
        }]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn target_pos_wait_target_reached_returns_error_when_insert_order_submission_fails() {
    let mut host = market_only_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::OpenOnly)
        .build()
        .unwrap();
    task.set_target_volume(1).unwrap();

    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    assert!(task.is_finished());
    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
    assert!(matches!(task.last_error(), Some(TaskError::Wait(_))));
    assert!(matches!(
        task.wait_target_reached().await,
        Err(TaskError::Wait(_))
    ));
    host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
        .expect("ownership should be released after task submit failure");
}

#[tokio::test(flavor = "current_thread")]
async fn target_pos_cancels_inserted_orders_when_later_batch_submission_fails() {
    let mut host = host_with_trade_adapter(FailNthTradeInsertAdapter::new(2));
    let task = host.target_pos("sim", "SHFE.rb2601").build().unwrap();
    task.set_target_volume(-1).unwrap();

    seed_position_detail_commit(&host, "sim", "SHFE.rb2601", 1, 1, 0, 0);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    assert!(!task.is_finished());
    assert!(matches!(task.last_error(), Some(TaskError::Wait(_))));
    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["direction"], "SELL");
    assert_eq!(payload["offset"], "CLOSETODAY");
    assert_eq!(payload["volume"], 1);
    assert_eq!(payload["order_id"], "wait-order-1");

    seed_quote_book_commit(&host, "SHFE.rb2601", 3679.0, 3678.0, 3678.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    assert!(!task.is_finished());
    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "cancel_order");
    assert_eq!(payload["order_id"], "wait-order-1");

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

    assert!(task.is_finished());
    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        task.wait_finished().await,
        Err(TaskError::Wait(_))
    ));
    host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
        .expect("ownership should be released after partial batch submission failure cleanup");
}

#[tokio::test(flavor = "current_thread")]
async fn open_only_target_pos_uses_opposite_open_order_to_reduce_net_position() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::OpenOnly)
        .build()
        .unwrap();
    task.set_target_volume(1).unwrap();

    seed_position_commit(&host, "sim", "SHFE.rb2601", 2);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["direction"], "SELL");
    assert_eq!(payload["offset"], "OPEN");
    assert_eq!(payload["volume"], 1);
    assert_eq!(payload["limit_price"], 3677.0);
}

#[tokio::test(flavor = "current_thread")]
async fn default_target_pos_advances_shfe_close_today_then_close_then_open() {
    let mut host = seeded_host();
    let task = host.target_pos("sim", "SHFE.rb2601").build().unwrap();
    task.set_target_volume(-1).unwrap();

    seed_position_detail_commit(&host, "sim", "SHFE.rb2601", 1, 1, 0, 0);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 2);
    let close_today = transport_payload(&dispatches[0].request);
    assert_eq!(close_today["direction"], "SELL");
    assert_eq!(close_today["offset"], "CLOSETODAY");
    assert_eq!(close_today["volume"], 1);
    assert_eq!(close_today["limit_price"], 3677.0);
    let close_yesterday = transport_payload(&dispatches[1].request);
    assert_eq!(close_yesterday["direction"], "SELL");
    assert_eq!(close_yesterday["offset"], "CLOSE");
    assert_eq!(close_yesterday["volume"], 1);
    assert_eq!(close_yesterday["limit_price"], 3677.0);

    seed_quote_book_commit(&host, "SHFE.rb2601", 3679.0, 3678.0, 3678.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );

    seed_position_detail_commit(&host, "sim", "SHFE.rb2601", 0, 0, 0, 0);
    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 1, 1);
    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 2, 1);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3680.0, 3679.0, 3679.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["direction"], "SELL");
    assert_eq!(payload["offset"], "OPEN");
    assert_eq!(payload["volume"], 1);
    assert_eq!(payload["order_id"], "wait-order-3");

    seed_position_detail_commit(&host, "sim", "SHFE.rb2601", 0, 0, 0, 1);
    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 3, 1);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3682.0, 3681.0, 3681.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    task.wait_target_reached().await.unwrap();
    assert_eq!(task.applied_target_volume_for_test(), Some(-1));
}

#[tokio::test(flavor = "current_thread")]
async fn default_target_pos_reprices_remaining_batch_order_after_partial_fill() {
    let mut host = seeded_host();
    let task = host.target_pos("sim", "SHFE.rb2601").build().unwrap();
    task.set_target_volume(-1).unwrap();

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

    let pending = tokio::time::timeout(Duration::from_millis(10), task.wait_target_reached()).await;
    assert!(pending.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn default_target_pos_replan_cancels_only_stale_subset_of_live_batch() {
    let mut host = seeded_host();
    let task = host.target_pos("sim", "SHFE.rb2601").build().unwrap();
    task.set_target_volume(-1).unwrap();

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

    task.set_target_volume(1).unwrap();
    seed_order_status_commit_with_seed(
        &host,
        "sim",
        "SHFE.rb2601",
        "wait-order-1",
        OrderStatusSeed {
            direction: "SELL",
            offset: "CLOSETODAY",
            limit_price: 3677.0,
            status: "ALIVE",
            volume_orign: 1,
            volume_left: 1,
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
    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.1, 3677.0, 3677.6);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "cancel_order");
    assert_eq!(payload["order_id"], "wait-order-2");

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
    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.2, 3677.0, 3677.7);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
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
    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.3, 3677.0, 3677.8);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    task.wait_target_reached().await.unwrap();

    let report = task.execution_report();
    assert_eq!(report.cancel_request_count, 1);
    assert_eq!(
        report.last_reached_target,
        Some(TargetPosTaskReachedTarget {
            request_seq: 2,
            target_volume: 1,
        })
    );
    assert_eq!(
        report
            .events
            .iter()
            .filter(|event| matches!(event, TargetPosTaskExecutionEvent::CancelOrder { .. }))
            .count(),
        1
    );
    assert!(report.events.iter().all(|event| {
        !matches!(
            event,
            TargetPosTaskExecutionEvent::CancelOrder { order_id } if order_id == "wait-order-1"
        )
    }));
}

#[tokio::test(flavor = "current_thread")]
async fn default_target_pos_replan_keeps_live_orders_after_stale_subset_finishes() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::TodayYesterdayThenOpen)
        .build()
        .unwrap();
    task.set_target_volume(-1).unwrap();

    seed_position_detail_commit(&host, "sim", "SHFE.rb2601", 1, 1, 0, 0);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.1, 3677.0, 3677.6);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 3);
    assert_eq!(
        transport_payload(&dispatches[0].request)["order_id"],
        "wait-order-1"
    );
    assert_eq!(
        transport_payload(&dispatches[1].request)["order_id"],
        "wait-order-2"
    );
    assert_eq!(
        transport_payload(&dispatches[2].request)["order_id"],
        "wait-order-3"
    );

    task.set_target_volume(-2).unwrap();
    seed_order_status_commit_with_seed(
        &host,
        "sim",
        "SHFE.rb2601",
        "wait-order-1",
        OrderStatusSeed {
            direction: "SELL",
            offset: "CLOSETODAY",
            limit_price: 3676.0,
            status: "ALIVE",
            volume_orign: 1,
            volume_left: 1,
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
    seed_order_status_commit_with_seed(
        &host,
        "sim",
        "SHFE.rb2601",
        "wait-order-3",
        OrderStatusSeed {
            direction: "SELL",
            offset: "OPEN",
            limit_price: 3677.0,
            status: "ALIVE",
            volume_orign: 1,
            volume_left: 1,
        },
    );
    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.2, 3677.0, 3677.7);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let cancel = transport_payload(&dispatches[0].request);
    assert_eq!(cancel["aid"], "cancel_order");
    assert_eq!(cancel["order_id"], "wait-order-1");

    seed_order_status_commit_with_seed(
        &host,
        "sim",
        "SHFE.rb2601",
        "wait-order-1",
        OrderStatusSeed {
            direction: "SELL",
            offset: "CLOSETODAY",
            limit_price: 3676.0,
            status: "FINISHED",
            volume_orign: 1,
            volume_left: 1,
        },
    );
    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 2);
    let close_today = transport_payload(&dispatches[0].request);
    assert_eq!(close_today["aid"], "insert_order");
    assert_eq!(close_today["direction"], "SELL");
    assert_eq!(close_today["offset"], "CLOSETODAY");
    assert_eq!(close_today["volume"], 1);
    let open = transport_payload(&dispatches[1].request);
    assert_eq!(open["aid"], "insert_order");
    assert_eq!(open["direction"], "SELL");
    assert_eq!(open["offset"], "OPEN");
    assert_eq!(open["volume"], 1);

    let report = task.execution_report();
    assert_eq!(report.cancel_request_count, 1);
    assert!(report.events.iter().all(|event| {
        !matches!(
            event,
            TargetPosTaskExecutionEvent::CancelOrder { order_id }
                if order_id == "wait-order-2" || order_id == "wait-order-3"
        )
    }));
}

#[tokio::test(flavor = "current_thread")]
async fn open_only_target_pos_retarget_keeps_matching_live_order_and_submits_missing_volume() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::OpenOnly)
        .build()
        .unwrap();
    task.set_target_volume(1).unwrap();

    seed_position_commit(&host, "sim", "SHFE.rb2601", 0);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    assert_eq!(
        transport_payload(&dispatches[0].request)["order_id"],
        "wait-order-1"
    );

    task.set_target_volume(2).unwrap();
    seed_order_status_commit_with_seed(
        &host,
        "sim",
        "SHFE.rb2601",
        "wait-order-1",
        OrderStatusSeed {
            direction: "BUY",
            offset: "OPEN",
            limit_price: 3678.0,
            status: "ALIVE",
            volume_orign: 1,
            volume_left: 1,
        },
    );
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "insert_order");
    assert_eq!(payload["order_id"], "wait-order-2");
    assert_eq!(payload["direction"], "BUY");
    assert_eq!(payload["offset"], "OPEN");
    assert_eq!(payload["volume"], 1);
    assert_eq!(payload["limit_price"], 3678.0);
    assert_eq!(task.execution_report().cancel_request_count, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn default_target_pos_uses_non_shfe_close_then_open() {
    let mut host = seeded_host();
    let task = host.target_pos("sim", "CFFEX.IF2606").build().unwrap();
    task.set_target_volume(-1).unwrap();

    seed_position_detail_commit(&host, "sim", "CFFEX.IF2606", 1, 1, 0, 0);
    seed_quote_book_commit(&host, "CFFEX.IF2606", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 2);
    let first_close = transport_payload(&dispatches[0].request);
    assert_eq!(first_close["direction"], "SELL");
    assert_eq!(first_close["offset"], "CLOSE");
    assert_eq!(first_close["volume"], 1);
    assert_eq!(first_close["limit_price"], 3677.0);
    let second_close = transport_payload(&dispatches[1].request);
    assert_eq!(second_close["direction"], "SELL");
    assert_eq!(second_close["offset"], "CLOSE");
    assert_eq!(second_close["volume"], 1);
    assert_eq!(second_close["limit_price"], 3677.0);

    seed_quote_book_commit(&host, "CFFEX.IF2606", 3679.0, 3678.0, 3678.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );

    seed_position_detail_commit(&host, "sim", "CFFEX.IF2606", 0, 0, 0, 0);
    seed_wait_order_finished_commit(&host, "sim", "CFFEX.IF2606", 1, 1);
    seed_wait_order_finished_commit(&host, "sim", "CFFEX.IF2606", 2, 1);
    seed_quote_book_commit(&host, "CFFEX.IF2606", 3680.0, 3679.0, 3679.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["direction"], "SELL");
    assert_eq!(payload["offset"], "OPEN");
    assert_eq!(payload["volume"], 1);
    assert_eq!(payload["order_id"], "wait-order-3");

    seed_position_detail_commit(&host, "sim", "CFFEX.IF2606", 0, 0, 0, 1);
    seed_wait_order_finished_commit(&host, "sim", "CFFEX.IF2606", 3, 1);
    seed_quote_book_commit(&host, "CFFEX.IF2606", 3682.0, 3681.0, 3681.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    task.wait_target_reached().await.unwrap();
    assert_eq!(task.applied_target_volume_for_test(), Some(-1));
}

#[tokio::test(flavor = "current_thread")]
async fn today_yesterday_then_open_target_pos_submits_open_in_same_batch() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::TodayYesterdayThenOpen)
        .build()
        .unwrap();
    task.set_target_volume(-1).unwrap();

    seed_position_detail_commit(&host, "sim", "SHFE.rb2601", 1, 1, 0, 0);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 3);
    let close_today = transport_payload(&dispatches[0].request);
    assert_eq!(close_today["direction"], "SELL");
    assert_eq!(close_today["offset"], "CLOSETODAY");
    assert_eq!(close_today["volume"], 1);
    let close_yesterday = transport_payload(&dispatches[1].request);
    assert_eq!(close_yesterday["direction"], "SELL");
    assert_eq!(close_yesterday["offset"], "CLOSE");
    assert_eq!(close_yesterday["volume"], 1);
    let open_order = transport_payload(&dispatches[2].request);
    assert_eq!(open_order["direction"], "SELL");
    assert_eq!(open_order["offset"], "OPEN");
    assert_eq!(open_order["volume"], 1);

    let pending = tokio::time::timeout(Duration::from_millis(10), task.wait_target_reached()).await;
    assert!(pending.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn yesterday_then_open_target_pos_skips_today_position_until_open_needed() {
    let mut host = seeded_host();
    let task = host
        .target_pos("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::YesterdayThenOpen)
        .build()
        .unwrap();
    task.set_target_volume(0).unwrap();

    seed_position_detail_commit(&host, "sim", "SHFE.rb2601", 1, 2, 0, 0);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3678.0, 3677.0, 3677.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 2);
    let close_order = transport_payload(&dispatches[0].request);
    assert_eq!(close_order["direction"], "SELL");
    assert_eq!(close_order["offset"], "CLOSE");
    assert_eq!(close_order["volume"], 2);
    assert_eq!(close_order["limit_price"], 3677.0);
    let open_order = transport_payload(&dispatches[1].request);
    assert_eq!(open_order["direction"], "SELL");
    assert_eq!(open_order["offset"], "OPEN");
    assert_eq!(open_order["volume"], 1);
    assert_eq!(open_order["limit_price"], 3677.0);

    seed_quote_book_commit(&host, "SHFE.rb2601", 3679.0, 3678.0, 3678.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );

    seed_position_detail_commit(&host, "sim", "SHFE.rb2601", 1, 0, 1, 0);
    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 1, 2);
    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 2, 1);
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

    task.wait_target_reached().await.unwrap();
    assert_eq!(task.applied_target_volume_for_test(), Some(0));
}
