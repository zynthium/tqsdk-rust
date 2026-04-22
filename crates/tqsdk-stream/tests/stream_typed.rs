use futures::StreamExt;
use tqsdk_core::{
    Account, Notification, Order, Position, PreInsertOrder, Quote, RiskManagementData,
    RiskManagementRule, SecurityAccount, SecurityOrder, SecurityPosition, SecurityTrade,
    SettlementInfo, Trade, TradingStatus,
};

mod support;

#[tokio::test(flavor = "current_thread")]
async fn quote_stream_decodes_matching_quote_and_skips_other_symbols() {
    let stream = support::core_seed::seeded_stream();
    let mut quotes = stream.quote_stream("SHFE.au2602").unwrap();

    support::core_seed::seed_quote_commit(&stream, "SHFE.ag2606", 5103.0);
    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 625.0);

    let update = quotes
        .next()
        .await
        .expect("quote stream should yield a matching update")
        .expect("quote stream should decode the matching quote");

    assert_eq!(update.value.instrument_id, "SHFE.au2602");
    assert_eq!(update.value.last_price, 625.0);
    assert_eq!(update.commit.revision, stream.reader().read().revision());
}

#[tokio::test(flavor = "current_thread")]
async fn path_stream_decodes_typed_value_for_selected_path() {
    let stream = support::core_seed::seeded_stream();
    let mut quotes = stream
        .path_stream::<Quote, _, _>(["quotes", "SHFE.au2602"])
        .unwrap();

    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 626.0);

    let update = quotes
        .next()
        .await
        .expect("path stream should yield a matching update")
        .expect("path stream should decode the requested value");

    assert_eq!(update.value.instrument_id, "SHFE.au2602");
    assert_eq!(update.value.last_price, 626.0);
    assert_eq!(update.commit.revision, stream.reader().read().revision());
}

#[tokio::test(flavor = "current_thread")]
async fn trading_status_stream_decodes_matching_status() {
    let stream = support::core_seed::seeded_stream();
    let mut updates = stream.trading_status_stream("SHFE.au2602").unwrap();

    support::core_seed::seed_trading_status_commit(&stream, "SHFE.au2602", "AUCTIONORDERING");

    let update = updates
        .next()
        .await
        .expect("trading status stream should yield a matching update")
        .expect("trading status stream should decode the status object");

    assert_eq!(update.value.symbol, "SHFE.au2602");
    assert_eq!(update.value.trade_status, "AUCTIONORDERING");
    let _: TradingStatus = update.value;
}

#[tokio::test(flavor = "current_thread")]
async fn trade_object_wrappers_decode_account_position_order_and_trade() {
    let stream = support::core_seed::seeded_stream();
    let mut accounts = stream.account_stream("sim").unwrap();
    let mut positions = stream.position_stream("sim", "SHFE.au2602").unwrap();
    let mut orders = stream.order_stream("sim", "order-1").unwrap();
    let mut trades = stream.trade_stream("sim", "trade-1").unwrap();

    support::core_seed::seed_trade_snapshot(&stream, "sim", "SHFE.au2602");

    let account = accounts
        .next()
        .await
        .expect("account stream should yield an update")
        .expect("account stream should decode account");
    let position = positions
        .next()
        .await
        .expect("position stream should yield an update")
        .expect("position stream should decode position");
    let order = orders
        .next()
        .await
        .expect("order stream should yield an update")
        .expect("order stream should decode order");
    let trade = trades
        .next()
        .await
        .expect("trade stream should yield an update")
        .expect("trade stream should decode trade");

    let _: Account = account.value;
    let _: Position = position.value;
    assert_eq!(order.value.order_id, "order-1");
    let _: Order = order.value.clone();
    assert_eq!(trade.value.trade_id, "trade-1");
    let _: Trade = trade.value;
}

#[tokio::test(flavor = "current_thread")]
async fn notification_stream_decodes_system_notification() {
    let stream = support::core_seed::seeded_stream();
    let mut notifications = stream.notification_stream("notify-1").unwrap();

    support::core_seed::seed_notification_commit(&stream, "notify-1");

    let update = notifications
        .next()
        .await
        .expect("notification stream should yield an update")
        .expect("notification stream should decode notification");

    assert_eq!(update.value.content, "connected");
    let _: Notification = update.value;
}

#[tokio::test(flavor = "current_thread")]
async fn risk_and_settlement_wrappers_decode_trade_extensions() {
    let stream = support::core_seed::seeded_stream();
    let mut pre_inserts = stream.pre_insert_order_stream("sim", "pre-1").unwrap();
    let mut rules = stream.risk_management_rule_stream("sim", "SSE").unwrap();
    let mut data = stream
        .risk_management_data_stream("sim", "SHFE.au2602")
        .unwrap();
    let mut settlements = stream.settlement_info_stream("sim", "20260420").unwrap();

    support::core_seed::seed_trade_extended_snapshot(&stream, "sim", "SHFE.au2602");

    let pre_insert = pre_inserts
        .next()
        .await
        .expect("pre-insert stream should yield an update")
        .expect("pre-insert stream should decode object");
    let rule = rules
        .next()
        .await
        .expect("risk rule stream should yield an update")
        .expect("risk rule stream should decode object");
    let risk_data = data
        .next()
        .await
        .expect("risk data stream should yield an update")
        .expect("risk data stream should decode object");
    let settlement = settlements
        .next()
        .await
        .expect("settlement stream should yield an update")
        .expect("settlement stream should decode object");

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
async fn security_wrappers_decode_security_trade_objects() {
    let stream = support::core_seed::seeded_stream();
    let mut accounts = stream.security_account_stream("stock-sim").unwrap();
    let mut positions = stream
        .security_position_stream("stock-sim", "SSE.600000")
        .unwrap();
    let mut orders = stream
        .security_order_stream("stock-sim", "stock-order-1")
        .unwrap();
    let mut trades = stream
        .security_trade_stream("stock-sim", "stock-trade-1")
        .unwrap();

    support::core_seed::seed_security_trade_snapshot(&stream, "stock-sim", "SSE.600000");

    let account = accounts
        .next()
        .await
        .expect("security account stream should yield an update")
        .expect("security account stream should decode object");
    let position = positions
        .next()
        .await
        .expect("security position stream should yield an update")
        .expect("security position stream should decode object");
    let order = orders
        .next()
        .await
        .expect("security order stream should yield an update")
        .expect("security order stream should decode object");
    let trade = trades
        .next()
        .await
        .expect("security trade stream should yield an update")
        .expect("security trade stream should decode object");

    assert_eq!(account.value.user_id, "stock-sim");
    let _: SecurityAccount = account.value;
    assert_eq!(position.value.instrument_id, "600000");
    let _: SecurityPosition = position.value;
    assert_eq!(order.value.order_id, "stock-order-1");
    let _: SecurityOrder = order.value;
    assert_eq!(trade.value.trade_id, "stock-trade-1");
    let _: SecurityTrade = trade.value;
}
