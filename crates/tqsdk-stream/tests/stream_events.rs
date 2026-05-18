use std::time::Duration;

use futures::StreamExt;
use tqsdk_core::{
    Position, PreInsertOrder, RiskManagementData, RiskManagementRule, SecurityOrder,
    SecurityPosition, SecurityTrade, SettlementInfo,
};

mod support;

#[test]
fn trade_event_streams_read_trade_partition_instead_of_full_snapshot() {
    let source = include_str!("../src/event.rs");
    let collect_fn_signature = "type CollectFn<T, C> = for<'a> fn(\n    &SharedCommitResult,\n    &TradeStateReadGuard<'a>,";
    assert!(
        source.contains(collect_fn_signature),
        "trade event collectors should receive TradeStateReadGuard"
    );

    let collected_stream_impl = source
        .split("impl<T, C> Stream for CollectedEventStream")
        .nth(1)
        .and_then(|rest| rest.split("impl Stream for TradeSessionEventStream").next())
        .expect("CollectedEventStream implementation should remain in event.rs");
    assert!(
        collected_stream_impl.contains("read_trade_state()"),
        "CollectedEventStream should read the trade partition directly"
    );
    assert!(
        !collected_stream_impl.contains("reader.read()"),
        "CollectedEventStream should not materialize a full snapshot"
    );
}

#[test]
fn trade_session_event_stream_reads_full_snapshot_only_for_system_events() {
    let source = include_str!("../src/event.rs");
    let trade_session_impl = source
        .split("impl Stream for TradeSessionEventStream")
        .nth(1)
        .and_then(|rest| {
            rest.split("macro_rules! define_account_event_stream")
                .next()
        })
        .expect("TradeSessionEventStream implementation should remain in event.rs");
    let collect_session_events = source
        .split("fn collect_trade_session_commit_events(")
        .nth(1)
        .and_then(|rest| rest.split("fn push_trade_session_event(").next())
        .expect("trade session event collector should remain in event.rs");

    assert!(
        trade_session_impl.contains("read_trade_state()"),
        "trade session stream should always read the trade partition directly"
    );
    assert!(
        !trade_session_impl.contains("reader.read()"),
        "trade session stream should not eagerly materialize a full snapshot per commit"
    );
    assert!(
        collect_session_events.contains("commit_requires_session_snapshot(commit)"),
        "trade session collector should gate full snapshot reads behind system object hits"
    );
    assert!(
        collect_session_events.contains("read_session_snapshot"),
        "trade session collector should lazily request a snapshot only when system events need it"
    );
}

#[test]
fn market_row_streams_read_market_partitions_instead_of_full_snapshot() {
    let source = include_str!("../src/window.rs");
    let projected_stream_impl = source
        .split("impl<T, C> Stream for ProjectedValueStream")
        .nth(1)
        .and_then(|rest| {
            rest.split("/// Commit-driven stream of ready kline row batches.")
                .next()
        })
        .expect("ProjectedValueStream implementation should remain in window.rs");

    assert!(
        projected_stream_impl.contains("read_market_state()"),
        "market row streams should read market partitions directly"
    );
    assert!(
        !projected_stream_impl.contains("reader.read()"),
        "market row streams should not materialize a full snapshot"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn position_event_stream_emits_matching_account_positions_only() {
    let stream = support::core_seed::seeded_stream();
    let mut events = stream.position_event_stream("sim").unwrap();

    support::core_seed::seed_trade_snapshot(&stream, "paper", "SHFE.au2602");

    let idle = tokio::time::timeout(Duration::from_millis(10), events.next()).await;
    assert!(idle.is_err());

    support::core_seed::seed_trade_snapshot(&stream, "sim", "SHFE.au2602");

    let update = events
        .next()
        .await
        .expect("position event stream should yield an update")
        .expect("position event stream should decode the matching position");

    assert_eq!(update.value.user_id, "sim");
    assert_eq!(update.value.instrument_id, "ao2602");
    let _: Position = update.value;
}

#[tokio::test(flavor = "current_thread")]
async fn trade_extension_event_streams_decode_matching_account_updates() {
    let stream = support::core_seed::seeded_stream();
    let mut pre_inserts = stream.pre_insert_order_event_stream("sim").unwrap();
    let mut rules = stream.risk_management_rule_event_stream("sim").unwrap();
    let mut data = stream.risk_management_data_event_stream("sim").unwrap();
    let mut settlements = stream.settlement_info_event_stream("sim").unwrap();

    support::core_seed::seed_trade_extended_snapshot(&stream, "paper", "SHFE.au2602");

    let idle = tokio::time::timeout(Duration::from_millis(10), pre_inserts.next()).await;
    assert!(idle.is_err());

    support::core_seed::seed_trade_extended_snapshot(&stream, "sim", "SHFE.au2602");

    let pre_insert = pre_inserts
        .next()
        .await
        .expect("pre-insert event stream should yield an update")
        .expect("pre-insert event stream should decode the matching object");
    let rule = rules
        .next()
        .await
        .expect("risk rule event stream should yield an update")
        .expect("risk rule event stream should decode the matching object");
    let risk_data = data
        .next()
        .await
        .expect("risk data event stream should yield an update")
        .expect("risk data event stream should decode the matching object");
    let settlement = settlements
        .next()
        .await
        .expect("settlement event stream should yield an update")
        .expect("settlement event stream should decode the matching object");

    assert_eq!(pre_insert.value.order_id, "pre-1");
    let _: PreInsertOrder = pre_insert.value;
    assert_eq!(rule.value.exchange_id, "SSE");
    let _: RiskManagementRule = rule.value;
    assert_eq!(risk_data.value.instrument_id, "ao2602");
    let _: RiskManagementData = risk_data.value;
    assert_eq!(settlement.value.content, "line-1\nline-2");
    let _: SettlementInfo = settlement.value;
}

#[tokio::test(flavor = "current_thread")]
async fn security_trade_event_streams_decode_matching_account_updates() {
    let stream = support::core_seed::seeded_stream();
    let mut positions = stream.security_position_event_stream("stock-sim").unwrap();
    let mut orders = stream.security_order_event_stream("stock-sim").unwrap();
    let mut trades = stream.security_trade_event_stream("stock-sim").unwrap();

    support::core_seed::seed_security_trade_snapshot(&stream, "paper", "SSE.600000");

    let idle = tokio::time::timeout(Duration::from_millis(10), orders.next()).await;
    assert!(idle.is_err());

    support::core_seed::seed_security_trade_snapshot(&stream, "stock-sim", "SSE.600000");

    let position = positions
        .next()
        .await
        .expect("security position event stream should yield an update")
        .expect("security position event stream should decode the matching object");
    let order = orders
        .next()
        .await
        .expect("security order event stream should yield an update")
        .expect("security order event stream should decode the matching object");
    let trade = trades
        .next()
        .await
        .expect("security trade event stream should yield an update")
        .expect("security trade event stream should decode the matching object");

    assert_eq!(position.value.instrument_id, "600000");
    let _: SecurityPosition = position.value;
    assert_eq!(order.value.order_id, "stock-order-1");
    let _: SecurityOrder = order.value;
    assert_eq!(trade.value.trade_id, "stock-trade-1");
    let _: SecurityTrade = trade.value;
}
