mod support;

use serde_json::json;
use tqsdk_core::{OutboundFrame, OutboundRequest, ProtocolDomain, TradeDirection, TradeOffset};

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
    assert_eq!(payload["limit_price"], 618.0);
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
