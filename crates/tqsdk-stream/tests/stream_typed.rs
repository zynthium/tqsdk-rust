use futures::StreamExt;
use tqsdk_core::{Account, Order, Position, Quote, Trade, TradingStatus};

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
