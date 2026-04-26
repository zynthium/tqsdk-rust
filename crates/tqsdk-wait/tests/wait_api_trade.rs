mod support;

use serde_json::json;
use tqsdk_core::{
    OrderLifecycle, OutboundFrame, OutboundRequest, ProtocolDomain, TradeDirection, TradeOffset,
};

fn compact_source(source: &str) -> String {
    source.split_whitespace().collect::<String>()
}

#[test]
fn trade_refs_read_trade_partition_instead_of_full_snapshot() {
    let trade_refs = include_str!("../src/refs/trade.rs");
    let security_refs = include_str!("../src/refs/security.rs");

    assert!(trade_refs.contains("read_trade_state()"));
    assert!(security_refs.contains("read_trade_state()"));
    assert!(!compact_source(trade_refs).contains("reader.read()"));
    assert!(!compact_source(security_refs).contains("reader.read()"));
}

fn transport_payload(request: &OutboundRequest) -> serde_json::Value {
    match request {
        OutboundRequest::Transport(OutboundFrame::Text(text)) => {
            serde_json::from_str(text).expect("transport frame should contain valid json payload")
        }
        OutboundRequest::Transport(OutboundFrame::Binary(bytes)) => serde_json::from_slice(bytes)
            .expect("transport frame should contain valid json payload"),
        other => panic!("expected binary transport request, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn insert_order_returns_order_ref_without_local_overlay() {
    let mut api = support::seeded_api();
    let order = api
        .insert_order(
            "sim",
            "SHFE.ao2602",
            TradeDirection::Buy,
            Some(TradeOffset::Open),
            1,
            Some(json!(618.0)),
        )
        .await
        .unwrap();

    assert!(!order.is_ready(&api).unwrap());

    let dispatches = api.handle_for_test().drain_dispatches().unwrap();
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
    assert_eq!(payload["instrument_id"], "ao2602");
    assert_eq!(payload["price_type"], "LIMIT");
    assert_eq!(payload["time_condition"], "GFD");
    assert_eq!(payload["limit_price"], 618.0);
}

#[tokio::test(flavor = "current_thread")]
async fn insert_limit_order_uses_typed_finite_price() {
    let mut api = support::seeded_api();
    let order = api
        .insert_limit_order(
            "sim",
            "SHFE.ao2602",
            TradeDirection::Buy,
            Some(TradeOffset::Open),
            2,
            619.5,
        )
        .await
        .unwrap();

    assert_eq!(order.account_id(), "sim");

    let dispatches = api.handle_for_test().drain_dispatches().unwrap();
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["price_type"], "LIMIT");
    assert_eq!(payload["time_condition"], "GFD");
    assert_eq!(payload["volume"], 2);
    assert_eq!(payload["limit_price"], 619.5);
}

#[tokio::test(flavor = "current_thread")]
async fn insert_limit_order_rejects_non_finite_price() {
    let mut api = support::seeded_api();

    let error = api
        .insert_limit_order(
            "sim",
            "SHFE.ao2602",
            TradeDirection::Buy,
            Some(TradeOffset::Open),
            1,
            f64::NAN,
        )
        .await
        .unwrap_err();

    assert_eq!(
        error,
        tqsdk_wait::WaitFacadeError::InvalidState("limit price must be finite")
    );
    assert!(api.handle_for_test().drain_dispatches().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn account_position_order_and_trade_refs_decode_from_state_tree() {
    let mut api = support::seeded_api();
    support::seed_trade_snapshot(&mut api, "sim", "SHFE.ao2602");

    assert_eq!(api.get_account("sim").load(&api).unwrap().currency, "CNY");
    assert_eq!(
        api.get_position("sim", "SHFE.ao2602")
            .load(&api)
            .unwrap()
            .instrument_id,
        "ao2602"
    );
    assert_eq!(
        api.get_order("sim", "order-1").load(&api).unwrap().status,
        "FINISHED"
    );
    assert_eq!(
        api.get_trade("sim", "trade-1").load(&api).unwrap().volume,
        1
    );
}

#[tokio::test(flavor = "current_thread")]
async fn extended_trade_refs_decode_from_state_tree() {
    let mut api = support::seeded_api();
    support::seed_trade_extended_snapshot(&mut api, "sim", "SHFE.ao2602");

    assert_eq!(
        api.get_pre_insert_order("sim", "pre-1")
            .load(&api)
            .unwrap()
            .pre_margin,
        1234.5
    );
    assert!(
        api.get_risk_management_rule("sim", "SSE")
            .load(&api)
            .unwrap()
            .enable
    );
    assert_eq!(
        api.get_risk_management_data("sim", "SHFE.ao2602")
            .load(&api)
            .unwrap()
            .trade_position_ratio
            .trade_units,
        12
    );
    assert_eq!(
        api.get_settlement_info("sim", "20260420")
            .load(&api)
            .unwrap()
            .content,
        "line-1\nline-2"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn notification_ref_decodes_from_state_tree() {
    let mut api = support::seeded_api();
    support::seed_notification_commit(&mut api, "notify-1");

    assert_eq!(
        api.get_notification("notify-1").load(&api).unwrap().content,
        "connected"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn security_trade_refs_decode_from_state_tree() {
    let mut api = support::seeded_api();
    support::seed_security_trade_snapshot(&mut api, "stock-sim", "SSE.600000");

    assert_eq!(
        api.get_security_account("stock-sim")
            .load(&api)
            .unwrap()
            .market_value,
        12345.0
    );
    assert_eq!(
        api.get_security_position("stock-sim", "SSE.600000")
            .load(&api)
            .unwrap()
            .volume,
        100
    );
    assert_eq!(
        api.get_security_order("stock-sim", "stock-order-1")
            .load(&api)
            .unwrap()
            .limit_price,
        123.45
    );
    assert_eq!(
        api.get_security_trade("stock-sim", "stock-trade-1")
            .load(&api)
            .unwrap()
            .balance,
        12345.0
    );
}

#[tokio::test(flavor = "current_thread")]
async fn insert_order_without_limit_price_uses_any_ioc_semantics() {
    let mut api = support::seeded_api();
    api.insert_order(
        "sim",
        "DCE.m2601",
        TradeDirection::Buy,
        Some(TradeOffset::Open),
        1,
        None,
    )
    .await
    .unwrap();

    let dispatches = api.handle_for_test().drain_dispatches().unwrap();
    let payload = transport_payload(&dispatches[0].request);

    assert_eq!(payload["price_type"], "ANY");
    assert_eq!(payload["time_condition"], "IOC");
    assert!(payload.get("limit_price").is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn insert_order_best_price_maps_to_best_ioc_semantics() {
    let mut api = support::seeded_api();
    api.insert_order(
        "sim",
        "CFFEX.IF2606",
        TradeDirection::Buy,
        Some(TradeOffset::Open),
        1,
        Some(json!("BEST")),
    )
    .await
    .unwrap();

    let dispatches = api.handle_for_test().drain_dispatches().unwrap();
    let payload = transport_payload(&dispatches[0].request);

    assert_eq!(payload["price_type"], "BEST");
    assert_eq!(payload["time_condition"], "IOC");
    assert!(payload.get("limit_price").is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_order_and_confirm_settlement_submit_trade_commands() {
    let mut api = support::seeded_api();
    api.cancel_order("sim", "order-1").await.unwrap();
    api.confirm_settlement("sim").await.unwrap();

    let dispatches = api.handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 2);

    let cancel_payload = transport_payload(&dispatches[0].request);
    assert_eq!(cancel_payload["aid"], "cancel_order");
    assert_eq!(cancel_payload["user_id"], "sim");
    assert_eq!(cancel_payload["order_id"], "order-1");

    let confirm_payload = transport_payload(&dispatches[1].request);
    assert_eq!(confirm_payload["aid"], "confirm_settlement");
}

#[tokio::test(flavor = "current_thread")]
async fn order_ref_helpers_wait_cancel_remaining_and_terminal_state() {
    let mut api = support::seeded_api();
    let order = api.get_order("sim", "order-1");

    support::seed_order_update(
        &mut api,
        "sim",
        "SHFE.ao2602",
        "order-1",
        3,
        1,
        "ALIVE",
        false,
    );

    let partial = order.wait_partially_filled(&mut api).await.unwrap();
    assert_eq!(partial.lifecycle, OrderLifecycle::PartiallyFilled);
    assert_eq!(partial.volume_left, 1);

    order.cancel_remaining(&mut api).await.unwrap();

    let dispatches = api.handle_for_test().drain_dispatches().unwrap();
    let cancel_payload = transport_payload(&dispatches[0].request);
    assert_eq!(cancel_payload["aid"], "cancel_order");
    assert_eq!(cancel_payload["user_id"], "sim");
    assert_eq!(cancel_payload["order_id"], "order-1");

    support::seed_order_update(
        &mut api,
        "sim",
        "SHFE.ao2602",
        "order-1",
        3,
        1,
        "FINISHED",
        true,
    );

    let terminal = order.wait_terminal(&mut api).await.unwrap();
    assert_eq!(terminal.lifecycle, OrderLifecycle::Cancelled);
    assert!(terminal.lifecycle.is_terminal());
}
