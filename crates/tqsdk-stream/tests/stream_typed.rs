use std::time::Duration;

use futures::StreamExt;
use serde_json::Value;
use tqsdk_core::{
    Account, Notification, Order, OutboundDispatch, OutboundFrame, OutboundRequest, Position,
    PreInsertOrder, ProtocolDomain, Quote, RiskManagementData, RiskManagementRule, SecurityAccount,
    SecurityOrder, SecurityPosition, SecurityTrade, SettlementInfo, Trade, TradingStatus,
};
use tqsdk_stream::{KlineWindow, MarketEvent, TickWindow, TqStream};

mod support;

fn transport_payload(request: &OutboundRequest) -> Value {
    match request {
        OutboundRequest::Transport(OutboundFrame::Text(text)) => {
            serde_json::from_str(text).expect("transport frame should contain valid json payload")
        }
        OutboundRequest::Transport(OutboundFrame::Binary(bytes)) => serde_json::from_slice(bytes)
            .expect("transport frame should contain valid json payload"),
        other => panic!("expected transport request, got {other:?}"),
    }
}

fn drain_dispatches(stream: &TqStream) -> Vec<OutboundDispatch> {
    stream.session().handle().drain_dispatches().unwrap()
}

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
async fn subscribe_and_unsubscribe_quotes_submit_market_requests_without_protocol_types() {
    let stream = support::core_seed::seeded_stream();

    stream
        .subscribe_quotes(["SHFE.au2602", "SHFE.ag2606"])
        .await
        .unwrap();

    let dispatches = drain_dispatches(&stream);
    let payload = dispatches
        .iter()
        .map(|dispatch| transport_payload(&dispatch.request))
        .find(|payload| payload["aid"] == "subscribe_quote")
        .expect("subscribe_quotes should submit subscribe_quote");
    assert_eq!(payload["ins_list"], "SHFE.ag2606,SHFE.au2602");

    stream.unsubscribe_quotes(["SHFE.ag2606"]).await.unwrap();

    let dispatches = drain_dispatches(&stream);
    let payload = dispatches
        .iter()
        .map(|dispatch| transport_payload(&dispatch.request))
        .find(|payload| payload["aid"] == "subscribe_quote")
        .expect("unsubscribe_quotes should submit updated subscribe_quote");
    assert_eq!(payload["ins_list"], "SHFE.au2602");
}

#[tokio::test(flavor = "current_thread")]
async fn quote_subscription_adds_removes_and_streams_current_symbols() {
    let stream = support::core_seed::seeded_stream();
    let mut quotes = stream.quotes(["SHFE.au2602"]).await.unwrap();

    assert_eq!(quotes.symbols().collect::<Vec<_>>(), vec!["SHFE.au2602"]);

    let dispatches = drain_dispatches(&stream);
    let payload = dispatches
        .iter()
        .map(|dispatch| transport_payload(&dispatch.request))
        .find(|payload| payload["aid"] == "subscribe_quote")
        .expect("quote subscription should submit subscribe_quote");
    assert_eq!(payload["ins_list"], "SHFE.au2602");

    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 625.0);
    let update = quotes
        .next()
        .await
        .expect("quote subscription should yield initial symbol updates")
        .expect("quote subscription should decode quote");
    assert_eq!(update.value.instrument_id, "SHFE.au2602");
    assert_eq!(update.value.last_price, 625.0);

    quotes.add("SHFE.ag2606").await.unwrap();
    assert_eq!(
        quotes.symbols().collect::<Vec<_>>(),
        vec!["SHFE.ag2606", "SHFE.au2602"]
    );

    let dispatches = drain_dispatches(&stream);
    let payload = dispatches
        .iter()
        .map(|dispatch| transport_payload(&dispatch.request))
        .find(|payload| payload["aid"] == "subscribe_quote")
        .expect("quote subscription add should submit subscribe_quote");
    assert_eq!(payload["ins_list"], "SHFE.ag2606,SHFE.au2602");

    quotes.remove("SHFE.au2602").await.unwrap();
    assert_eq!(quotes.symbols().collect::<Vec<_>>(), vec!["SHFE.ag2606"]);

    let dispatches = drain_dispatches(&stream);
    let payload = dispatches
        .iter()
        .map(|dispatch| transport_payload(&dispatch.request))
        .find(|payload| payload["aid"] == "subscribe_quote")
        .expect("quote subscription remove should submit subscribe_quote");
    assert_eq!(payload["ins_list"], "SHFE.ag2606");

    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 626.0);
    let idle = tokio::time::timeout(Duration::from_millis(10), quotes.next()).await;
    assert!(idle.is_err());

    support::core_seed::seed_quote_commit(&stream, "SHFE.ag2606", 5103.0);
    let update = quotes
        .next()
        .await
        .expect("quote subscription should yield added symbol updates")
        .expect("quote subscription should decode added quote");
    assert_eq!(update.value.instrument_id, "SHFE.ag2606");
    assert_eq!(update.value.last_price, 5103.0);
}

#[tokio::test(flavor = "current_thread")]
async fn quote_subscription_close_unsubscribes_current_symbols() {
    let stream = support::core_seed::seeded_stream();
    let quotes = stream.quotes(["SHFE.au2602", "SHFE.ag2606"]).await.unwrap();

    drain_dispatches(&stream);
    quotes.close().await.unwrap();

    let dispatches = drain_dispatches(&stream);
    let payload = dispatches
        .iter()
        .map(|dispatch| transport_payload(&dispatch.request))
        .find(|payload| payload["aid"] == "subscribe_quote")
        .expect("quote subscription close should submit unsubscribe");
    assert_eq!(payload["ins_list"], "");
}

#[tokio::test(flavor = "current_thread")]
async fn overlapping_quote_subscriptions_do_not_unsubscribe_other_owner() {
    let stream = support::core_seed::seeded_stream();
    let first = stream.quotes(["SHFE.au2602"]).await.unwrap();
    drain_dispatches(&stream);

    let second = stream.quotes(["SHFE.au2602"]).await.unwrap();
    assert!(
        drain_dispatches(&stream).is_empty(),
        "second overlapping quote subscription should not resubmit"
    );

    second.close().await.unwrap();
    assert!(
        drain_dispatches(&stream).is_empty(),
        "closing second overlapping subscription must keep first owner subscribed"
    );

    first.close().await.unwrap();
    let dispatches = drain_dispatches(&stream);
    let payload = dispatches
        .iter()
        .map(|dispatch| transport_payload(&dispatch.request))
        .find(|payload| payload["aid"] == "subscribe_quote")
        .expect("last quote owner should unsubscribe");
    assert_eq!(payload["ins_list"], "");
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

#[tokio::test(flavor = "current_thread")]
async fn kline_stream_submits_chart_request_and_decodes_ready_window() {
    let stream = support::core_seed::seeded_stream();
    let mut windows = stream
        .kline_stream("SHFE.au2602", Duration::from_secs(60), 64)
        .await
        .unwrap();

    let dispatches = drain_dispatches(&stream);
    assert_eq!(dispatches.len(), 2);
    assert!(
        dispatches
            .iter()
            .all(|dispatch| dispatch.domain == ProtocolDomain::Market)
    );

    let payload = dispatches
        .iter()
        .map(|dispatch| transport_payload(&dispatch.request))
        .find(|payload| payload["aid"] == "set_chart")
        .expect("kline stream should submit a set_chart request");
    assert_eq!(payload["aid"], "set_chart");
    assert_eq!(
        payload["chart_id"],
        "stream-kline-SHFE_au2602-60000000000-64"
    );
    assert_eq!(payload["ins_list"], "SHFE.au2602");
    assert_eq!(payload["duration"], 60_000_000_000_i64);
    assert_eq!(payload["view_width"], 64);

    support::core_seed::seed_ready_kline_chart(&stream, "SHFE.au2602", 60_000_000_000_i64, 64);

    let update = windows
        .next()
        .await
        .expect("kline window stream should yield an update")
        .expect("kline window stream should decode the ready chart");

    assert_eq!(update.value.symbol(), "SHFE.au2602");
    assert_eq!(update.value.duration_ns(), 60_000_000_000_i64);
    assert_eq!(update.value.view_width(), 64);
    assert_eq!(
        update.value.chart_id(),
        "stream-kline-SHFE_au2602-60000000000-64"
    );
    assert_eq!(update.value.len(), 2);
    assert_eq!(update.value.last().unwrap().close, 620.0);
    let _: KlineWindow = update.value;
}

#[tokio::test(flavor = "current_thread")]
async fn overlapping_kline_streams_do_not_cancel_other_owner() {
    let stream = support::core_seed::seeded_stream();
    let first = stream
        .kline_stream("SHFE.au2602", Duration::from_secs(60), 64)
        .await
        .unwrap();
    drain_dispatches(&stream);

    let second = stream
        .kline_stream("SHFE.au2602", Duration::from_secs(60), 64)
        .await
        .unwrap();
    assert!(
        drain_dispatches(&stream).is_empty(),
        "second overlapping kline stream should not resubmit set_chart"
    );

    second.close().await.unwrap();
    assert!(
        drain_dispatches(&stream).is_empty(),
        "closing second overlapping kline stream must keep first owner subscribed"
    );

    first.close().await.unwrap();
    let dispatches = drain_dispatches(&stream);
    let payload = dispatches
        .iter()
        .map(|dispatch| transport_payload(&dispatch.request))
        .find(|payload| payload["aid"] == "set_chart")
        .expect("last kline owner should cancel chart");
    assert_eq!(
        payload["chart_id"],
        "stream-kline-SHFE_au2602-60000000000-64"
    );
    assert_eq!(payload["ins_list"], "");
}

#[tokio::test(flavor = "current_thread")]
async fn tick_stream_submits_chart_request_and_decodes_ready_window() {
    let stream = support::core_seed::seeded_stream();
    let mut windows = stream.tick_stream("SHFE.au2602", 32).await.unwrap();

    let dispatches = drain_dispatches(&stream);
    assert_eq!(dispatches.len(), 2);
    assert!(
        dispatches
            .iter()
            .all(|dispatch| dispatch.domain == ProtocolDomain::Market)
    );

    let payload = dispatches
        .iter()
        .map(|dispatch| transport_payload(&dispatch.request))
        .find(|payload| payload["aid"] == "set_chart")
        .expect("tick stream should submit a set_chart request");
    assert_eq!(payload["aid"], "set_chart");
    assert_eq!(payload["chart_id"], "stream-tick-SHFE_au2602-32");
    assert_eq!(payload["ins_list"], "SHFE.au2602");
    assert_eq!(payload["duration"], 0);
    assert_eq!(payload["view_width"], 32);

    support::core_seed::seed_ready_tick_chart(&stream, "SHFE.au2602", 32);

    let update = windows
        .next()
        .await
        .expect("tick window stream should yield an update")
        .expect("tick window stream should decode the ready chart");

    assert_eq!(update.value.symbol(), "SHFE.au2602");
    assert_eq!(update.value.view_width(), 32);
    assert_eq!(update.value.chart_id(), "stream-tick-SHFE_au2602-32");
    assert_eq!(update.value.len(), 2);
    assert_eq!(update.value.last().unwrap().last_price, 618.5);
    let _: TickWindow = update.value;

    windows.close().await.unwrap();

    let dispatches = drain_dispatches(&stream);
    assert_eq!(dispatches.len(), 2);

    let payload = dispatches
        .iter()
        .map(|dispatch| transport_payload(&dispatch.request))
        .find(|payload| payload["aid"] == "set_chart")
        .expect("tick window close should submit a cancel_chart request");
    assert_eq!(payload["chart_id"], "stream-tick-SHFE_au2602-32");
    assert_eq!(payload["ins_list"], "");
    assert_eq!(payload["duration"], 0);
    assert_eq!(payload["view_width"], 32);
}

#[tokio::test(flavor = "current_thread")]
async fn market_event_stream_subscribes_mixed_market_data_and_yields_typed_events() {
    let stream = support::core_seed::seeded_stream();
    let mut events = stream
        .market_events()
        .quote("SHFE.au2602")
        .tick("SHFE.au2602", 32)
        .kline("SHFE.au2602", Duration::from_secs(60), 64)
        .build()
        .await
        .unwrap();

    let dispatches = drain_dispatches(&stream);
    let payloads = dispatches
        .iter()
        .map(|dispatch| transport_payload(&dispatch.request))
        .collect::<Vec<_>>();
    assert!(
        payloads
            .iter()
            .any(|payload| payload["aid"] == "subscribe_quote"
                && payload["ins_list"] == "SHFE.au2602")
    );
    assert!(payloads.iter().any(|payload| {
        payload["aid"] == "set_chart"
            && payload["chart_id"] == "stream-tick-SHFE_au2602-32"
            && payload["duration"] == 0
            && payload["view_width"] == 32
    }));
    assert!(payloads.iter().any(|payload| {
        payload["aid"] == "set_chart"
            && payload["chart_id"] == "stream-kline-SHFE_au2602-60000000000-64"
            && payload["duration"] == 60_000_000_000_i64
            && payload["view_width"] == 64
    }));

    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 625.0);
    let quote = events
        .next()
        .await
        .expect("market event stream should yield a quote event")
        .expect("market event stream should decode quote");
    match quote {
        MarketEvent::Quote(update) => {
            assert_eq!(update.value.instrument_id, "SHFE.au2602");
            assert_eq!(update.value.last_price, 625.0);
        }
        other => panic!("expected quote event, got {other:?}"),
    }

    support::core_seed::seed_ready_tick_chart(&stream, "SHFE.au2602", 32);
    let tick = events
        .next()
        .await
        .expect("market event stream should yield a tick event")
        .expect("market event stream should decode tick window");
    match tick {
        MarketEvent::TickWindow(update) => {
            assert_eq!(update.value.symbol(), "SHFE.au2602");
            assert_eq!(update.value.len(), 2);
            assert_eq!(update.value.last().unwrap().last_price, 618.5);
        }
        other => panic!("expected tick window event, got {other:?}"),
    }

    support::core_seed::seed_ready_kline_chart(&stream, "SHFE.au2602", 60_000_000_000_i64, 64);
    let kline = events
        .next()
        .await
        .expect("market event stream should yield a kline event")
        .expect("market event stream should decode kline window");
    match kline {
        MarketEvent::KlineWindow(update) => {
            assert_eq!(update.value.symbol(), "SHFE.au2602");
            assert_eq!(update.value.len(), 2);
            assert_eq!(update.value.last().unwrap().close, 620.0);
        }
        other => panic!("expected kline window event, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn market_event_stream_close_unsubscribes_quotes_and_cancels_charts() {
    let stream = support::core_seed::seeded_stream();
    let events = stream
        .market_events()
        .quote("SHFE.au2602")
        .tick("SHFE.au2602", 32)
        .kline("SHFE.au2602", Duration::from_secs(60), 64)
        .build()
        .await
        .unwrap();

    drain_dispatches(&stream);
    events.close().await.unwrap();

    let dispatches = drain_dispatches(&stream);
    let payloads = dispatches
        .iter()
        .map(|dispatch| transport_payload(&dispatch.request))
        .collect::<Vec<_>>();
    assert!(
        payloads
            .iter()
            .any(|payload| payload["aid"] == "subscribe_quote" && payload["ins_list"] == "")
    );
    assert!(payloads.iter().any(|payload| {
        payload["aid"] == "set_chart"
            && payload["chart_id"] == "stream-tick-SHFE_au2602-32"
            && payload["ins_list"] == ""
    }));
    assert!(payloads.iter().any(|payload| {
        payload["aid"] == "set_chart"
            && payload["chart_id"] == "stream-kline-SHFE_au2602-60000000000-64"
            && payload["ins_list"] == ""
    }));
}

#[tokio::test(flavor = "current_thread")]
async fn kline_window_close_submits_cancel_chart_request() {
    let stream = support::core_seed::seeded_stream();
    let windows = stream
        .kline_stream("SHFE.au2602", Duration::from_secs(60), 64)
        .await
        .unwrap();

    drain_dispatches(&stream);
    windows.close().await.unwrap();

    let dispatches = drain_dispatches(&stream);
    assert_eq!(dispatches.len(), 2);

    let payload = dispatches
        .iter()
        .map(|dispatch| transport_payload(&dispatch.request))
        .find(|payload| payload["aid"] == "set_chart")
        .expect("kline window close should submit a cancel_chart request");
    assert_eq!(
        payload["chart_id"],
        "stream-kline-SHFE_au2602-60000000000-64"
    );
    assert_eq!(payload["ins_list"], "");
    assert_eq!(payload["duration"], 60_000_000_000_i64);
    assert_eq!(payload["view_width"], 64);
}

#[tokio::test(flavor = "current_thread")]
async fn order_event_stream_emits_matching_account_updates_only() {
    let stream = support::core_seed::seeded_stream();
    let mut events = stream.order_event_stream("sim").unwrap();

    support::core_seed::seed_trade_snapshot(&stream, "paper", "SHFE.au2602");

    let idle = tokio::time::timeout(Duration::from_millis(10), events.next()).await;
    assert!(idle.is_err());

    support::core_seed::seed_trade_snapshot(&stream, "sim", "SHFE.au2602");

    let update = events
        .next()
        .await
        .expect("order event stream should yield an update")
        .expect("order event stream should decode the order object");

    assert_eq!(update.value.order_id, "order-1");
    assert_eq!(update.value.user_id, "sim");
}

#[tokio::test(flavor = "current_thread")]
async fn trade_event_stream_emits_matching_account_trades_only() {
    let stream = support::core_seed::seeded_stream();
    let mut events = stream.trade_event_stream("sim").unwrap();

    support::core_seed::seed_trade_snapshot(&stream, "paper", "SHFE.au2602");

    let idle = tokio::time::timeout(Duration::from_millis(10), events.next()).await;
    assert!(idle.is_err());

    support::core_seed::seed_trade_snapshot(&stream, "sim", "SHFE.au2602");

    let update = events
        .next()
        .await
        .expect("trade event stream should yield an update")
        .expect("trade event stream should decode the trade object");

    assert_eq!(update.value.trade_id, "trade-1");
    assert_eq!(update.value.user_id, "sim");
}
