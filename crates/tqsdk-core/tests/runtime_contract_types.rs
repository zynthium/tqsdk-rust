use serde_json::json;
use tqsdk_core::{
    Account, CategoryInfo, Chart, ChartInfo, EdbIndexData, FrequentCancellation,
    FrequentCancellationRule, Kline, Notification, Order, Position, PreInsertOrder, Quote,
    RiskManagementData, RiskManagementRule, SecurityAccount, SecurityOrder, SecurityPosition,
    SecurityTrade, SelfTrade, SelfTradeRule, SettlementInfo, SymbolRanking, SymbolSettlement, Tick,
    Trade, TradeDirection, TradeOffset, TradePositionRatio, TradePositionRatioRule, TradePriceType,
    TradingCalendarDay, TradingStatus, TradingTime,
};

#[test]
fn market_and_query_schema_types_deserialize_sparse_payloads() {
    let quote = serde_json::from_value::<Quote>(json!({
        "instrument_id": "SHFE.au2602",
        "datetime": "2026-04-20 09:00:00.000000",
        "ask_price1": 618.5,
        "ask_volume1": 3,
        "bid_price1": 618.4,
        "bid_volume1": 6,
        "close": "-",
        "settlement": null,
        "trading_time": {
            "day": [["09:00:00", "10:15:00"]],
            "night": [["21:00:00", "23:00:00"]]
        },
        "categories": [
            {"id": "metal", "name": "Metals"}
        ]
    }))
    .expect("quote schema should deserialize");
    assert_eq!(quote.instrument_id, "SHFE.au2602");
    assert!(quote.close.is_nan());
    assert!(quote.settlement.is_nan());
    assert_eq!(quote.trading_time.day.len(), 1);
    assert_eq!(quote.categories.len(), 1);

    let quote_python_string_floats = serde_json::from_value::<Quote>(json!({
        "instrument_id": "SHFE.rb2606",
        "last_price": "not-a-number",
        "highest": "",
        "lowest": "-"
    }))
    .expect("quote string float placeholders should deserialize like Python prototypes");
    assert!(quote_python_string_floats.last_price.is_nan());
    assert!(quote_python_string_floats.highest.is_nan());
    assert!(quote_python_string_floats.lowest.is_nan());

    let quote_null_last_price = serde_json::from_value::<Quote>(json!({
        "instrument_id": "SHFE.rb2606",
        "last_price": null
    }))
    .expect("quote null last_price should deserialize like Python default nan");
    assert!(quote_null_last_price.last_price.is_nan());

    let quote_numeric_floats = serde_json::from_value::<Quote>(json!({
        "instrument_id": "SSE.510300",
        "volume_multiple": 1000.0,
        "max_market_order_volume": 100.0,
        "min_limit_order_volume": 1.0,
        "expire_datetime": 1776844800000000000.0,
        "last_exercise_datetime": 1776844800000000000.0,
        "public_float_share_quantity": 1000000.0
    }))
    .expect("quote integer-compatible floats should deserialize");
    assert_eq!(quote_numeric_floats.volume_multiple, 1000);
    assert_eq!(quote_numeric_floats.max_market_order_volume, 100);
    assert_eq!(quote_numeric_floats.min_limit_order_volume, 1);
    assert_eq!(
        quote_numeric_floats.expire_datetime,
        Some(1776844800000000000_i64)
    );
    assert_eq!(
        quote_numeric_floats.last_exercise_datetime,
        Some(1776844800000000000_i64)
    );
    assert_eq!(quote_numeric_floats.public_float_share_quantity, 1_000_000);

    let quote_nullable_vectors = serde_json::from_value::<Quote>(json!({
        "instrument_id": "SSE.510300",
        "trading_time": {
            "day": null,
            "night": null
        },
        "stock_dividend_ratio": null,
        "cash_dividend_ratio": null,
        "categories": null
    }))
    .expect("quote null vectors should deserialize");
    assert!(quote_nullable_vectors.trading_time.day.is_empty());
    assert!(quote_nullable_vectors.trading_time.night.is_empty());
    assert!(quote_nullable_vectors.stock_dividend_ratio.is_empty());
    assert!(quote_nullable_vectors.cash_dividend_ratio.is_empty());
    assert!(quote_nullable_vectors.categories.is_empty());

    let tick = serde_json::from_value::<Tick>(json!({
        "datetime": 1776646800000000000_i64,
        "last_price": 618.5,
        "ask_price1": 618.6,
        "ask_volume1": 2,
        "ask_price2": null,
        "ask_volume2": null,
        "bid_price1": 618.4,
        "bid_volume1": 4
    }))
    .expect("tick schema should deserialize");
    assert!(tick.ask_price2.is_nan());
    assert_eq!(tick.ask_volume2, 0);

    let tick_python_string_last_price = serde_json::from_value::<Tick>(json!({
        "datetime": 1776646800000000000_i64,
        "last_price": "not-a-number"
    }))
    .expect("tick string last_price should deserialize like Python prototype default");
    assert!(tick_python_string_last_price.last_price.is_nan());

    let trading_status = serde_json::from_value::<TradingStatus>(json!({
        "symbol": "SHFE.au2602",
        "trade_status": "CONTINOUS"
    }))
    .expect("trading status schema should deserialize");
    assert_eq!(trading_status.trade_status, "CONTINOUS");

    let chart = serde_json::from_value::<Chart>(json!({
        "left_id": 10,
        "right_id": 42,
        "more_data": true,
        "ready": false,
        "state": {"ins_list": "SHFE.au2602"}
    }))
    .expect("chart schema should deserialize");
    assert_eq!(chart.left_id, 10);
    assert_eq!(chart.state.get("ins_list"), Some(&json!("SHFE.au2602")));

    let chart_info = serde_json::from_value::<ChartInfo>(json!({
        "chart_id": "chart-1",
        "left_id": 10,
        "right_id": 42,
        "more_data": true,
        "ready": false,
        "view_width": 128
    }))
    .expect("chart info schema should deserialize");
    assert_eq!(chart_info.chart_id, "chart-1");

    let settlement = serde_json::from_value::<SymbolSettlement>(json!({
        "datetime": "2026-04-20",
        "symbol": "SHFE.au2602",
        "settlement": 618.5
    }))
    .expect("symbol settlement schema should deserialize");
    assert_eq!(settlement.symbol, "SHFE.au2602");

    let ranking = serde_json::from_value::<SymbolRanking>(json!({
        "datetime": "2026-04-20",
        "symbol": "SHFE.au2602",
        "exchange_id": "SHFE",
        "instrument_id": "au2602",
        "broker": "demo",
        "volume": 10.0,
        "volume_change": 1.0,
        "volume_ranking": 2.0,
        "long_oi": 3.0,
        "long_change": 4.0,
        "long_ranking": 5.0,
        "short_oi": 6.0,
        "short_change": 7.0,
        "short_ranking": 8.0
    }))
    .expect("symbol ranking schema should deserialize");
    assert_eq!(ranking.exchange_id, "SHFE");

    let trading_calendar_day = serde_json::from_value::<TradingCalendarDay>(json!({
        "date": "2026-04-20",
        "trading": true
    }))
    .expect("trading calendar day schema should deserialize");
    assert!(trading_calendar_day.trading);

    let edb_index_data = serde_json::from_value::<EdbIndexData>(json!({
        "date": "2026-04-20",
        "values": {
            "1": 3.125,
            "2": 2.75
        }
    }))
    .expect("edb index data schema should deserialize");
    assert_eq!(edb_index_data.values.get(&1), Some(&3.125));

    let _surface_refs = (
        Option::<TradingTime>::None,
        Option::<CategoryInfo>::None,
        Option::<Kline>::None,
        Option::<ChartInfo>::None,
        Option::<TradingCalendarDay>::None,
        Option::<EdbIndexData>::None,
    );
}

#[test]
fn trading_and_risk_schema_types_deserialize_nested_payloads() {
    let account = serde_json::from_value::<Account>(json!({
        "user_id": "simnow",
        "currency": "CNY",
        "available": 100000.0,
        "balance": 101234.5
    }))
    .expect("account schema should deserialize");
    assert_eq!(account.user_id, "simnow");
    assert_eq!(account.clone(), account);

    let position = serde_json::from_value::<Position>(json!({
        "user_id": "simnow",
        "exchange_id": "SHFE",
        "instrument_id": "au2602",
        "pos_long": 2,
        "pos_short": 0
    }))
    .expect("position schema should deserialize");
    assert_eq!(position.pos_long, 2);
    assert_eq!(position.clone(), position);

    let order = serde_json::from_value::<Order>(json!({
        "user_id": "simnow",
        "order_id": "order-1",
        "exchange_id": "SHFE",
        "instrument_id": "au2602",
        "direction": "BUY",
        "offset": "OPEN",
        "volume_orign": 2,
        "volume_left": 1,
        "status": "ALIVE"
    }))
    .expect("order schema should deserialize");
    assert_eq!(order.order_id, "order-1");
    assert_eq!(order.volume_origin, 2);
    assert_eq!(order.clone(), order);

    let trade = serde_json::from_value::<Trade>(json!({
        "user_id": "simnow",
        "trade_id": "trade-1",
        "order_id": "order-1",
        "exchange_id": "SHFE",
        "instrument_id": "au2602",
        "direction": "BUY",
        "offset": "OPEN",
        "price": 618.5,
        "volume": 1
    }))
    .expect("trade schema should deserialize");
    assert_eq!(trade.trade_id, "trade-1");
    assert_eq!(trade.clone(), trade);

    let pre_insert_order = serde_json::from_value::<PreInsertOrder>(json!({
        "user_id": "simnow",
        "order_id": "pre-1",
        "exchange_id": "SHFE",
        "instrument_id": "au2602",
        "direction": "BUY",
        "pre_margin": 1234.5
    }))
    .expect("pre-insert order schema should deserialize");
    assert_eq!(pre_insert_order.order_id, "pre-1");

    let risk_rule = serde_json::from_value::<RiskManagementRule>(json!({
        "user_id": "simnow",
        "exchange_id": "SHFE",
        "enable": true,
        "self_trade": {"count_limit": 3},
        "frequent_cancellation": {
            "insert_order_count_limit": 20,
            "cancel_order_count_limit": 10,
            "cancel_order_percent_limit": 50.0
        },
        "trade_position_ratio": {
            "trade_units_limit": 100,
            "trade_position_ratio_limit": 70.0
        }
    }))
    .expect("risk management rule schema should deserialize");
    assert_eq!(risk_rule.self_trade.count_limit, 3);
    assert_eq!(risk_rule.frequent_cancellation.cancel_order_count_limit, 10);

    let risk_data = serde_json::from_value::<RiskManagementData>(json!({
        "user_id": "simnow",
        "exchange_id": "SHFE",
        "instrument_id": "au2602",
        "self_trade": {
            "highest_buy_price": 618.5,
            "lowest_sell_price": 618.6,
            "self_trade_count": 1,
            "rejected_count": 0
        },
        "frequent_cancellation": {
            "insert_order_count": 8,
            "cancel_order_count": 3,
            "cancel_order_percent": 37.5,
            "rejected_count": 0
        },
        "trade_position_ratio": {
            "trade_units": 12,
            "net_position_units": 4,
            "trade_position_ratio": 300.0,
            "rejected_count": 1
        }
    }))
    .expect("risk management data schema should deserialize");
    assert_eq!(risk_data.trade_position_ratio.trade_units, 12);

    let notification = serde_json::from_value::<Notification>(json!({
        "code": "INFO",
        "level": "INFO",
        "type": "MESSAGE",
        "content": "ready",
        "bid": "system",
        "user_id": "simnow"
    }))
    .expect("notification schema should deserialize");
    assert_eq!(notification.content, "ready");

    let settlement_info = serde_json::from_value::<SettlementInfo>(json!({
        "content": "line-1\nline-2"
    }))
    .expect("settlement info schema should deserialize");
    assert_eq!(settlement_info.content, "line-1\nline-2");

    let _surface_refs = (
        Option::<SelfTradeRule>::None,
        Option::<FrequentCancellationRule>::None,
        Option::<TradePositionRatioRule>::None,
        Option::<SelfTrade>::None,
        Option::<FrequentCancellation>::None,
        Option::<TradePositionRatio>::None,
        Option::<PreInsertOrder>::None,
        Option::<SettlementInfo>::None,
    );
}

#[test]
fn futures_order_and_trade_decode_typed_side_offset_and_price_type() {
    let order = serde_json::from_value::<Order>(json!({
        "user_id": "simnow",
        "order_id": "order-typed-1",
        "exchange_id": "SHFE",
        "instrument_id": "au2602",
        "direction": "BUY",
        "offset": "OPEN",
        "price_type": "LIMIT",
        "volume_orign": 2,
        "volume_left": 1,
        "status": "ALIVE"
    }))
    .expect("typed order schema should deserialize");

    assert_eq!(order.direction, Some(TradeDirection::Buy));
    assert_eq!(order.offset, Some(TradeOffset::Open));
    assert_eq!(order.price_type, Some(TradePriceType::Limit));

    let trade = serde_json::from_value::<Trade>(json!({
        "user_id": "simnow",
        "trade_id": "trade-typed-1",
        "order_id": "order-typed-1",
        "exchange_id": "SHFE",
        "instrument_id": "au2602",
        "direction": "SELL",
        "offset": "CLOSETODAY",
        "price": 618.5,
        "volume": 1
    }))
    .expect("typed trade schema should deserialize");

    assert_eq!(trade.direction, Some(TradeDirection::Sell));
    assert_eq!(trade.offset, Some(TradeOffset::CloseToday));
}

#[test]
fn futures_order_and_trade_optional_typed_fields_preserve_missing_field_tolerance() {
    let order = serde_json::from_value::<Order>(json!({
        "user_id": "simnow",
        "order_id": "order-missing-typed-fields"
    }))
    .expect("missing optional typed order fields should deserialize");

    assert_eq!(order.direction, None);
    assert_eq!(order.offset, None);
    assert_eq!(order.price_type, None);

    let unknown_order = serde_json::from_value::<Order>(json!({
        "order_id": "order-unknown-direction",
        "direction": "SIDEWAYS"
    }));
    assert!(unknown_order.is_err());
}

#[test]
fn security_schema_types_deserialize_payloads() {
    let account = serde_json::from_value::<SecurityAccount>(json!({
        "user_id": "stock-demo",
        "currency": "CNY",
        "asset": 200000.0,
        "available": 100000.0
    }))
    .expect("security account schema should deserialize");
    assert_eq!(account.user_id, "stock-demo");

    let position = serde_json::from_value::<SecurityPosition>(json!({
        "user_id": "stock-demo",
        "exchange_id": "SSE",
        "instrument_id": "600000",
        "volume": 100,
        "last_price": 12.5
    }))
    .expect("security position schema should deserialize");
    assert_eq!(position.instrument_id, "600000");

    let order = serde_json::from_value::<SecurityOrder>(json!({
        "user_id": "stock-demo",
        "order_id": "stock-order-1",
        "exchange_id": "SSE",
        "instrument_id": "600000",
        "direction": "BUY",
        "volume_orign": 100,
        "volume_left": 100,
        "price_type": "LIMIT",
        "limit_price": 12.5,
        "status": "ALIVE"
    }))
    .expect("security order schema should deserialize");
    assert_eq!(order.order_id, "stock-order-1");
    assert_eq!(order.volume_origin, 100);

    let trade = serde_json::from_value::<SecurityTrade>(json!({
        "user_id": "stock-demo",
        "trade_id": "stock-trade-1",
        "exchange_id": "SSE",
        "instrument_id": "600000",
        "order_id": "stock-order-1",
        "exchange_order_id": "ex-1",
        "direction": "BUY",
        "volume": 100,
        "price": 12.5,
        "balance": 1250.0,
        "fee": 1.2
    }))
    .expect("security trade schema should deserialize");
    assert_eq!(trade.trade_id, "stock-trade-1");
}
