use std::time::Duration;

use futures::StreamExt;
use tqsdk_stream::TradeObjectEvent;

mod support;

async fn next_trade_object_event(
    events: &mut tqsdk_stream::TradeObjectEventStream,
) -> tqsdk_stream::TradeObjectEvent {
    tokio::time::timeout(Duration::from_millis(50), events.next())
        .await
        .expect("trade object event stream should not stall")
        .expect("trade object event stream should yield an item")
        .expect("trade object event stream should decode an event")
        .value
}

#[tokio::test(flavor = "current_thread")]
async fn trade_object_event_stream_emits_matching_futures_variants() {
    let stream = support::core_seed::seeded_stream();
    let mut events = stream.trade_object_event_stream("sim").unwrap();

    support::core_seed::seed_trade_snapshot(&stream, "paper", "SHFE.au2602");

    let idle = tokio::time::timeout(Duration::from_millis(10), events.next()).await;
    assert!(idle.is_err());

    support::core_seed::seed_trade_snapshot(&stream, "sim", "SHFE.au2602");
    support::core_seed::seed_trade_extended_snapshot(&stream, "sim", "SHFE.au2602");

    let mut saw_account = false;
    let mut saw_position = false;
    let mut saw_order = false;
    let mut saw_trade = false;
    let mut saw_pre_insert = false;
    let mut saw_rule = false;
    let mut saw_risk_data = false;
    let mut saw_settlement = false;

    for _ in 0..8 {
        match next_trade_object_event(&mut events).await {
            TradeObjectEvent::Account(account) => {
                saw_account = true;
                assert_eq!(account.user_id, "sim");
            }
            TradeObjectEvent::Position(position) => {
                saw_position = true;
                assert_eq!(position.instrument_id, "ao2602");
            }
            TradeObjectEvent::Order(order) => {
                saw_order = true;
                assert_eq!(order.order_id, "order-1");
            }
            TradeObjectEvent::Trade(trade) => {
                saw_trade = true;
                assert_eq!(trade.trade_id, "trade-1");
            }
            TradeObjectEvent::PreInsertOrder(pre_insert) => {
                saw_pre_insert = true;
                assert_eq!(pre_insert.order_id, "pre-1");
            }
            TradeObjectEvent::RiskManagementRule(rule) => {
                saw_rule = true;
                assert_eq!(rule.exchange_id, "SSE");
            }
            TradeObjectEvent::RiskManagementData(risk_data) => {
                saw_risk_data = true;
                assert_eq!(risk_data.instrument_id, "ao2602");
            }
            TradeObjectEvent::SettlementInfo(settlement) => {
                saw_settlement = true;
                assert_eq!(settlement.content, "line-1\nline-2");
            }
            other => panic!("unexpected futures trade object event variant: {other:?}"),
        }
    }

    assert!(saw_account);
    assert!(saw_position);
    assert!(saw_order);
    assert!(saw_trade);
    assert!(saw_pre_insert);
    assert!(saw_rule);
    assert!(saw_risk_data);
    assert!(saw_settlement);
}

#[tokio::test(flavor = "current_thread")]
async fn trade_object_event_stream_emits_matching_security_variants() {
    let stream = support::core_seed::seeded_stream();
    let mut events = stream.trade_object_event_stream("stock-sim").unwrap();

    support::core_seed::seed_security_trade_snapshot(&stream, "paper", "SSE.600000");

    let idle = tokio::time::timeout(Duration::from_millis(10), events.next()).await;
    assert!(idle.is_err());

    support::core_seed::seed_security_trade_snapshot(&stream, "stock-sim", "SSE.600000");

    let mut saw_account = false;
    let mut saw_position = false;
    let mut saw_order = false;
    let mut saw_trade = false;

    for _ in 0..4 {
        match next_trade_object_event(&mut events).await {
            TradeObjectEvent::SecurityAccount(account) => {
                saw_account = true;
                assert_eq!(account.user_id, "stock-sim");
            }
            TradeObjectEvent::SecurityPosition(position) => {
                saw_position = true;
                assert_eq!(position.instrument_id, "600000");
            }
            TradeObjectEvent::SecurityOrder(order) => {
                saw_order = true;
                assert_eq!(order.order_id, "stock-order-1");
            }
            TradeObjectEvent::SecurityTrade(trade) => {
                saw_trade = true;
                assert_eq!(trade.trade_id, "stock-trade-1");
            }
            other => panic!("unexpected security trade object event variant: {other:?}"),
        }
    }

    assert!(saw_account);
    assert!(saw_position);
    assert!(saw_order);
    assert!(saw_trade);
}
