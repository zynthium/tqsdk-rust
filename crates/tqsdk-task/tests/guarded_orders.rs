use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, OutboundFrame, OutboundRequest,
    ProtocolDomain, RuntimeHandle, RuntimeInput, TradeDirection, TradeOffset,
};
use tqsdk_session::testing::ManualSession;
use tqsdk_task::{TaskError, TaskHost, TaskKind};
use tqsdk_wait::TqApi;

fn seeded_host() -> TaskHost {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    let handle = RuntimeHandle::with_adapters(adapters);
    let session = ManualSession::from_runtime(handle).into_client();
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

fn seed_order_commit(host: &TaskHost, account_id: &str, symbol: &str, order_id: &str) {
    let (exchange_id, instrument_id) = symbol
        .split_once('.')
        .expect("test symbol should contain an exchange prefix");
    host.api()
        .session()
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
                                        "volume_orign": 1,
                                        "volume_left": 1,
                                        "limit_price": 3678.0,
                                        "price_type": "LIMIT",
                                        "volume_condition": "ANY",
                                        "time_condition": "GFD",
                                        "insert_date_time": 1_713_660_000_000_000_000_i64,
                                        "status": "ALIVE",
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
        .expect("seed order commit should produce a commit");
}

#[tokio::test(flavor = "current_thread")]
async fn insert_order_guarded_blocks_symbol_owned_by_target_task() {
    let mut host = seeded_host();
    let _task = host.target_pos("sim", "SHFE.rb2601").build().unwrap();

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
        .expect_err("manual order should be blocked while task owns the symbol");

    assert_eq!(
        err,
        TaskError::ManualOrderBlocked {
            account_id: "sim".to_string(),
            symbol: "SHFE.rb2601".to_string(),
            active_task_kind: TaskKind::TargetPos,
        }
    );
    assert!(
        host.api()
            .session()
            .handle()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn insert_order_guarded_allows_unowned_symbol_and_delegates_to_wait_api() {
    let mut host = seeded_host();

    let order = host
        .insert_order_guarded(
            "sim",
            "SHFE.rb2601",
            TradeDirection::Buy,
            Some(TradeOffset::Open),
            2,
            Some(json!(3678.0)),
        )
        .await
        .unwrap();

    assert!(!order.is_ready().unwrap());

    let dispatches = host.api().session().handle().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    assert_eq!(dispatches[0].domain, ProtocolDomain::Trade);
    assert_eq!(
        dispatches[0].account_id.as_ref().map(|id| id.as_str()),
        Some("sim")
    );

    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "insert_order");
    assert_eq!(payload["user_id"], "sim");
    assert_eq!(payload["exchange_id"], "SHFE");
    assert_eq!(payload["instrument_id"], "rb2601");
    assert_eq!(payload["volume"], 2);
}

#[tokio::test(flavor = "current_thread")]
async fn insert_order_guarded_accepts_best_magic_string() {
    let mut host = seeded_host();

    host.insert_order_guarded(
        "sim",
        "SHFE.rb2601",
        TradeDirection::Buy,
        Some(TradeOffset::Open),
        2,
        Some(json!("BEST")),
    )
    .await
    .unwrap();

    let dispatches = host.api().session().handle().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);

    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["price_type"], "BEST");
    assert_eq!(payload["time_condition"], "IOC");
    assert!(payload["limit_price"].is_null());
}

#[tokio::test(flavor = "current_thread")]
async fn insert_order_guarded_accepts_fivelevel_magic_string() {
    let mut host = seeded_host();

    host.insert_order_guarded(
        "sim",
        "SHFE.rb2601",
        TradeDirection::Buy,
        Some(TradeOffset::Open),
        2,
        Some(json!("FIVELEVEL")),
    )
    .await
    .unwrap();

    let dispatches = host.api().session().handle().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);

    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["price_type"], "FIVELEVEL");
    assert_eq!(payload["time_condition"], "IOC");
    assert!(payload["limit_price"].is_null());
}

#[tokio::test(flavor = "current_thread")]
async fn insert_order_guarded_rejects_unknown_legacy_price_mode() {
    let mut host = seeded_host();

    let error = host
        .insert_order_guarded(
            "sim",
            "SHFE.rb2601",
            TradeDirection::Buy,
            Some(TradeOffset::Open),
            2,
            Some(json!("MID")),
        )
        .await
        .unwrap_err();

    assert_eq!(
        error,
        TaskError::InvalidState("limit price must be a number, BEST, or FIVELEVEL")
    );
    assert!(
        host.api()
            .session()
            .handle()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_order_guarded_blocks_ready_order_whose_symbol_is_owned() {
    let mut host = seeded_host();
    seed_order_commit(&host, "sim", "SHFE.rb2601", "order-1");
    let _task = host.target_pos("sim", "SHFE.rb2601").build().unwrap();

    let err = host
        .cancel_order_guarded("sim", "order-1")
        .await
        .expect_err("manual cancel should be blocked while task owns the order symbol");

    assert_eq!(
        err,
        TaskError::ManualOrderBlocked {
            account_id: "sim".to_string(),
            symbol: "SHFE.rb2601".to_string(),
            active_task_kind: TaskKind::TargetPos,
        }
    );
    assert!(
        host.api()
            .session()
            .handle()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_order_guarded_rejects_missing_order_snapshot() {
    let mut host = seeded_host();

    let err = host
        .cancel_order_guarded("sim", "missing-order")
        .await
        .expect_err("guarded cancel should reject missing local order snapshots");

    assert_eq!(
        err,
        TaskError::OrderNotReady {
            account_id: "sim".to_string(),
            order_id: "missing-order".to_string(),
        }
    );
    assert!(
        host.api()
            .session()
            .handle()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_order_guarded_allows_unowned_ready_order_and_delegates_to_wait_api() {
    let mut host = seeded_host();
    seed_order_commit(&host, "sim", "SHFE.rb2601", "order-1");

    host.cancel_order_guarded("sim", "order-1").await.unwrap();

    let dispatches = host.api().session().handle().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    assert_eq!(dispatches[0].domain, ProtocolDomain::Trade);

    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "cancel_order");
    assert_eq!(payload["user_id"], "sim");
    assert_eq!(payload["order_id"], "order-1");
}
