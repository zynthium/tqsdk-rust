use std::time::Duration;

use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, OutboundFrame, OutboundRequest,
    ProtocolDomain, RuntimeHandle, RuntimeInput,
};
use tqsdk_session::{SessionClient, SessionFacadeConfig};
use tqsdk_task::{
    OffsetPriority, PriceMode, TargetPosConfig, TaskError, TaskHost, TaskKind, VolumeSplitPolicy,
};
use tqsdk_wait::TqApi;

fn seeded_host() -> TaskHost {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    let handle = RuntimeHandle::with_adapters(adapters);
    let session = SessionClient::new_for_test_with_handle(handle, SessionFacadeConfig::default());
    TaskHost::new(TqApi::new(session))
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
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["direction"], "SELL");
    assert_eq!(payload["offset"], "CLOSETODAY");
    assert_eq!(payload["volume"], 1);
    assert_eq!(payload["limit_price"], 3677.0);

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

    seed_position_detail_commit(&host, "sim", "SHFE.rb2601", 0, 1, 0, 0);
    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 1, 1);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3680.0, 3679.0, 3679.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["direction"], "SELL");
    assert_eq!(payload["offset"], "CLOSE");
    assert_eq!(payload["volume"], 1);

    seed_position_detail_commit(&host, "sim", "SHFE.rb2601", 0, 0, 0, 0);
    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 2, 1);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3681.0, 3680.0, 3680.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["direction"], "SELL");
    assert_eq!(payload["offset"], "OPEN");
    assert_eq!(payload["volume"], 1);

    seed_position_detail_commit(&host, "sim", "SHFE.rb2601", 0, 0, 0, 1);
    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 3, 1);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3682.0, 3681.0, 3681.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    task.wait_target_reached().await.unwrap();
    assert_eq!(task.applied_target_volume_for_test(), Some(-1));
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
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["direction"], "SELL");
    assert_eq!(payload["offset"], "CLOSE");
    assert_eq!(payload["volume"], 1);
    assert_eq!(payload["limit_price"], 3677.0);

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

    seed_position_detail_commit(&host, "sim", "CFFEX.IF2606", 0, 1, 0, 0);
    seed_wait_order_finished_commit(&host, "sim", "CFFEX.IF2606", 1, 1);
    seed_quote_book_commit(&host, "CFFEX.IF2606", 3680.0, 3679.0, 3679.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["direction"], "SELL");
    assert_eq!(payload["offset"], "CLOSE");
    assert_eq!(payload["volume"], 1);

    seed_position_detail_commit(&host, "sim", "CFFEX.IF2606", 0, 0, 0, 0);
    seed_wait_order_finished_commit(&host, "sim", "CFFEX.IF2606", 2, 1);
    seed_quote_book_commit(&host, "CFFEX.IF2606", 3681.0, 3680.0, 3680.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["direction"], "SELL");
    assert_eq!(payload["offset"], "OPEN");
    assert_eq!(payload["volume"], 1);

    seed_position_detail_commit(&host, "sim", "CFFEX.IF2606", 0, 0, 0, 1);
    seed_wait_order_finished_commit(&host, "sim", "CFFEX.IF2606", 3, 1);
    seed_quote_book_commit(&host, "CFFEX.IF2606", 3682.0, 3681.0, 3681.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    task.wait_target_reached().await.unwrap();
    assert_eq!(task.applied_target_volume_for_test(), Some(-1));
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
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["direction"], "SELL");
    assert_eq!(payload["offset"], "CLOSE");
    assert_eq!(payload["volume"], 2);
    assert_eq!(payload["limit_price"], 3677.0);

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

    seed_position_detail_commit(&host, "sim", "SHFE.rb2601", 1, 0, 0, 0);
    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 1, 2);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3680.0, 3679.0, 3679.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["direction"], "SELL");
    assert_eq!(payload["offset"], "OPEN");
    assert_eq!(payload["volume"], 1);

    seed_position_detail_commit(&host, "sim", "SHFE.rb2601", 1, 0, 0, 1);
    seed_wait_order_finished_commit(&host, "sim", "SHFE.rb2601", 2, 1);
    seed_quote_book_commit(&host, "SHFE.rb2601", 3681.0, 3680.0, 3680.5);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    task.wait_target_reached().await.unwrap();
    assert_eq!(task.applied_target_volume_for_test(), Some(0));
}
