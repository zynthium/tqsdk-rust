mod support;

use serde_json::json;
use tqsdk_core::{
    CommandStatus, CommitScope, OrderLifecycle, OutboundFrame, OutboundRequest, ProtocolDomain,
    TradeAccountType, TradeDirection, TradeOffset,
};
use tqsdk_wait::OrderTicketState;

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
async fn login_trade_account_submits_typed_login_and_waits_for_account_ready() {
    let mut api = support::seeded_api();
    support::seed_trade_snapshot(&mut api, "sim", "SHFE.ao2602");

    let account = api
        .login_trade_account("9999", "sim", "secret", TradeAccountType::Future, None)
        .await
        .unwrap();

    assert_eq!(account.load(&api).unwrap().currency, "CNY");

    let dispatches = api.handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    assert_eq!(dispatches[0].domain, ProtocolDomain::Trade);
    assert_eq!(
        dispatches[0].account_id.as_ref().map(|id| id.as_str()),
        Some("sim")
    );

    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "req_login");
    assert_eq!(payload["bid"], "9999");
    assert_eq!(payload["user_name"], "sim");
    assert_eq!(payload["password"], "secret");
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
async fn limit_order_intent_uses_client_intent_as_order_id() {
    let mut api = support::seeded_api();
    let ticket = api
        .limit_order("sim", "SHFE.ao2602")
        .client_intent("strategy-a-open-001")
        .buy_open(1)
        .at(618.0)
        .send_once()
        .await
        .unwrap();

    assert!(ticket.was_submitted());
    assert_eq!(ticket.client_order_id(), "strategy-a-open-001");
    assert_eq!(ticket.order().account_id(), "sim");
    assert_eq!(ticket.order().order_id(), "strategy-a-open-001");
    assert!(ticket.command_id().is_some());

    let dispatches = api.handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "insert_order");
    assert_eq!(payload["user_id"], "sim");
    assert_eq!(payload["order_id"], "strategy-a-open-001");
    assert_eq!(payload["direction"], "BUY");
    assert_eq!(payload["offset"], "OPEN");
    assert_eq!(payload["volume"], 1);
    assert_eq!(payload["price_type"], "LIMIT");
    assert_eq!(payload["time_condition"], "GFD");
    assert_eq!(payload["limit_price"], 618.0);
}

#[tokio::test(flavor = "current_thread")]
async fn send_once_does_not_resubmit_same_local_intent() {
    let mut api = support::seeded_api();

    let first = api
        .limit_order("sim", "SHFE.ao2602")
        .client_intent("strategy-a-open-001")
        .buy_open(1)
        .at(618.0)
        .send_once()
        .await
        .unwrap();
    assert!(first.was_submitted());
    let first_command_id = first.command_id();
    assert_eq!(api.handle_for_test().drain_dispatches().unwrap().len(), 1);

    let second = api
        .limit_order("sim", "SHFE.ao2602")
        .client_intent("strategy-a-open-001")
        .buy_open(1)
        .at(618.0)
        .send_once()
        .await
        .unwrap();

    assert!(!second.was_submitted());
    assert_eq!(second.command_id(), first_command_id);
    assert!(api.handle_for_test().drain_dispatches().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn send_once_does_not_resubmit_same_session_intent_after_wait_rewrap() {
    let mut api = support::seeded_api();

    let first = api
        .limit_order("sim", "SHFE.ao2602")
        .client_intent("strategy-a-open-001")
        .buy_open(1)
        .at(618.0)
        .send_once()
        .await
        .unwrap();
    assert!(first.was_submitted());
    let first_command_id = first.command_id();
    assert_eq!(api.handle_for_test().drain_dispatches().unwrap().len(), 1);

    let session = api.into_session();
    let mut api = tqsdk_wait::TqApi::new(session);

    let second = api
        .limit_order("sim", "SHFE.ao2602")
        .client_intent("strategy-a-open-001")
        .buy_open(1)
        .at(618.0)
        .send_once()
        .await
        .unwrap();

    assert!(!second.was_submitted());
    assert_eq!(second.command_id(), first_command_id);
    assert!(api.handle_for_test().drain_dispatches().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn send_once_rejects_same_intent_with_different_fields() {
    let mut api = support::seeded_api();

    api.limit_order("sim", "SHFE.ao2602")
        .client_intent("strategy-a-open-001")
        .buy_open(1)
        .at(618.0)
        .send_once()
        .await
        .unwrap();
    assert_eq!(api.handle_for_test().drain_dispatches().unwrap().len(), 1);

    let error = api
        .limit_order("sim", "SHFE.ao2602")
        .client_intent("strategy-a-open-001")
        .buy_open(2)
        .at(619.0)
        .send_once()
        .await
        .unwrap_err();

    assert_eq!(
        error,
        tqsdk_wait::WaitFacadeError::InvalidState(
            "client order intent already registered with different order fields"
        )
    );
    assert!(api.handle_for_test().drain_dispatches().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn send_once_returns_existing_order_without_resubmit() {
    let mut api = support::seeded_api();
    support::seed_order_update(
        &mut api,
        support::OrderUpdateSeed {
            account_id: "sim",
            symbol: "SHFE.ao2602",
            order_id: "strategy-a-open-001",
            volume_orign: 1,
            volume_left: 1,
            status: "ALIVE",
            is_dead: false,
        },
    );

    let ticket = api
        .limit_order("sim", "SHFE.ao2602")
        .client_intent("strategy-a-open-001")
        .buy_open(1)
        .at(618.0)
        .send_once()
        .await
        .unwrap();

    assert!(!ticket.was_submitted());
    assert!(ticket.command_id().is_none());
    assert_eq!(
        ticket.order().snapshot(&api).unwrap().unwrap().order_id,
        "strategy-a-open-001"
    );
    assert!(api.handle_for_test().drain_dispatches().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn order_ticket_status_reports_command_pending_without_order() {
    let mut api = support::seeded_api();
    let ticket = api
        .limit_order("sim", "SHFE.ao2602")
        .client_intent("strategy-a-open-001")
        .buy_open(1)
        .at(618.0)
        .send_once()
        .await
        .unwrap();
    let command_id = ticket.command_id().unwrap();
    if let Some(commit) = api
        .handle_for_test()
        .record_command_status(
            command_id,
            CommandStatus::Sent,
            None,
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
    {
        api.push_deferred_commit_for_test(commit);
    }

    match ticket.status(&api).unwrap() {
        OrderTicketState::CommandPending {
            command_id: actual_command_id,
            status,
        } => {
            assert_eq!(actual_command_id, command_id);
            assert_eq!(status, CommandStatus::Sent);
        }
        other => panic!("expected command-pending ticket state, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn wait_reconnect_safe_terminal_returns_rejected_command_without_order() {
    let mut api = support::seeded_api();
    let ticket = api
        .limit_order("sim", "SHFE.ao2602")
        .client_intent("strategy-a-open-001")
        .buy_open(1)
        .at(618.0)
        .send_once()
        .await
        .unwrap();
    let command_id = ticket.command_id().unwrap();

    if let Some(commit) = api
        .handle_for_test()
        .record_command_status(
            command_id,
            CommandStatus::Rejected,
            None,
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
    {
        api.push_deferred_commit_for_test(commit);
    }

    match ticket
        .wait_reconnect_safe_terminal_until(
            &mut api,
            tokio::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .await
        .unwrap()
    {
        OrderTicketState::Rejected {
            command_id: Some(actual_command_id),
            order: None,
        } => assert_eq!(actual_command_id, command_id),
        other => panic!("expected rejected command terminal state, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn wait_reconnect_safe_terminal_returns_typed_order_terminal() {
    let mut api = support::seeded_api();
    let ticket = api
        .limit_order("sim", "SHFE.ao2602")
        .client_intent("strategy-a-open-001")
        .buy_open(1)
        .at(618.0)
        .send_once()
        .await
        .unwrap();

    support::seed_order_update(
        &mut api,
        support::OrderUpdateSeed {
            account_id: "sim",
            symbol: "SHFE.ao2602",
            order_id: "strategy-a-open-001",
            volume_orign: 1,
            volume_left: 0,
            status: "FINISHED",
            is_dead: true,
        },
    );

    match ticket.wait_reconnect_safe_terminal(&mut api).await.unwrap() {
        OrderTicketState::Filled { command_id, order } => {
            assert_eq!(command_id, ticket.command_id());
            assert_eq!(order.order_id, "strategy-a-open-001");
            assert_eq!(order.lifecycle, OrderLifecycle::Filled);
        }
        other => panic!("expected filled order terminal state, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn send_once_validates_required_intent_fields() {
    let mut api = support::seeded_api();

    let error = api
        .limit_order("sim", "SHFE.ao2602")
        .buy_open(1)
        .at(618.0)
        .send_once()
        .await
        .unwrap_err();
    assert_eq!(
        error,
        tqsdk_wait::WaitFacadeError::InvalidState("client intent id is required")
    );

    let error = api
        .limit_order("sim", "SHFE.ao2602")
        .client_intent("strategy-a-open-002")
        .buy_open(1)
        .at(f64::NAN)
        .send_once()
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
        support::OrderUpdateSeed {
            account_id: "sim",
            symbol: "SHFE.ao2602",
            order_id: "order-1",
            volume_orign: 3,
            volume_left: 1,
            status: "ALIVE",
            is_dead: false,
        },
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
        support::OrderUpdateSeed {
            account_id: "sim",
            symbol: "SHFE.ao2602",
            order_id: "order-1",
            volume_orign: 3,
            volume_left: 1,
            status: "FINISHED",
            is_dead: true,
        },
    );

    let terminal = order.wait_terminal(&mut api).await.unwrap();
    assert_eq!(terminal.lifecycle, OrderLifecycle::Cancelled);
    assert!(terminal.lifecycle.is_terminal());
}
