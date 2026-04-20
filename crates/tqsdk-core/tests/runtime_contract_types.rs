use serde_json::json;
use tqsdk_core::{
    Account, CategoryInfo, Chart, ChartInfo, EdbIndexData, FrequentCancellation,
    FrequentCancellationRule, Kline, Notification, Order, Position, PreInsertOrder, Quote,
    RiskManagementData, RiskManagementRule, SecurityAccount, SecurityOrder, SecurityPosition,
    SecurityTrade, SelfTrade, SelfTradeRule, SettlementInfo, SymbolRanking, SymbolSettlement, Tick,
    Trade, TradePositionRatio, TradePositionRatioRule, TradingCalendarDay, TradingStatus,
    TradingTime,
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

    let position = serde_json::from_value::<Position>(json!({
        "user_id": "simnow",
        "exchange_id": "SHFE",
        "instrument_id": "au2602",
        "pos_long": 2,
        "pos_short": 0
    }))
    .expect("position schema should deserialize");
    assert_eq!(position.pos_long, 2);

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
