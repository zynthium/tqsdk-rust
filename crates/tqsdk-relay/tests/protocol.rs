use serde_json::json;
use tqsdk_relay::{
    DownstreamCommand, RelayKlineRow, RelayMarketFrame, RelayTickRow, SetChartCommand,
};

#[test]
fn parses_subscribe_quote_command() {
    let command = DownstreamCommand::from_value(json!({
        "aid": "subscribe_quote",
        "ins_list": "SHFE.au2602,DCE.m2609"
    }))
    .unwrap();

    assert_eq!(
        command,
        DownstreamCommand::SubscribeQuote {
            symbols: vec!["SHFE.au2602".to_string(), "DCE.m2609".to_string()]
        }
    );
}

#[test]
fn parses_subscribe_quote_empty_ins_list() {
    let command = DownstreamCommand::from_value(json!({
        "aid": "subscribe_quote",
        "ins_list": ""
    }))
    .unwrap();

    assert_eq!(
        command,
        DownstreamCommand::SubscribeQuote { symbols: vec![] }
    );
}

#[test]
fn rejects_subscribe_quote_missing_ins_list() {
    let err = DownstreamCommand::from_value(json!({
        "aid": "subscribe_quote"
    }))
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid relay protocol: market command missing ins_list"
    );
}

#[test]
fn parses_set_chart_command() {
    let command = DownstreamCommand::from_value(json!({
        "aid": "set_chart",
        "chart_id": "client-chart-1",
        "ins_list": "SHFE.au2602",
        "duration": 60000000000i64,
        "view_width": 64,
        "left_kline_id": 100
    }))
    .unwrap();

    assert_eq!(
        command,
        DownstreamCommand::SetChart(SetChartCommand {
            chart_id: "client-chart-1".to_string(),
            symbols: vec!["SHFE.au2602".to_string()],
            duration_ns: 60_000_000_000,
            view_width: 64,
            left_kline_id: Some(100),
            focus_datetime_ns: None,
            focus_position: None,
        })
    );
}

#[test]
fn rejects_set_chart_missing_ins_list() {
    let err = DownstreamCommand::from_value(json!({
        "aid": "set_chart",
        "chart_id": "client-chart-1",
        "duration": 60000000000i64,
        "view_width": 64
    }))
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid relay protocol: market command missing ins_list"
    );
}

#[test]
fn rejects_trade_command() {
    let err = DownstreamCommand::from_value(json!({
        "aid": "insert_order"
    }))
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        "unsupported relay market command: insert_order"
    );
}

#[test]
fn encodes_compatible_rtn_data_for_ticks_and_klines() {
    let frame = RelayMarketFrame::rtn_data(vec![
        RelayMarketFrame::tick_update(
            "SHFE.au2602",
            RelayTickRow {
                id: 17,
                datetime: 1_713_660_000_000_000_000,
                last_price: 618.5,
                volume: 200,
                open_interest: 1000,
            },
        ),
        RelayMarketFrame::kline_update(
            "SHFE.au2602",
            60_000_000_000,
            RelayKlineRow {
                id: 42,
                datetime: 1_713_660_000_000_000_000,
                open: 610.0,
                high: 620.0,
                low: 609.0,
                close: 618.5,
                volume: 200,
                open_oi: 900,
                close_oi: 1000,
            },
        ),
    ]);

    assert_eq!(
        frame.into_value(),
        json!({
            "aid": "rtn_data",
            "data": [
                {
                    "ticks": {
                        "SHFE.au2602": {
                            "last_id": 17,
                            "data": {
                                "17": {
                                    "id": 17,
                                    "datetime": 1713660000000000000i64,
                                    "last_price": 618.5,
                                    "volume": 200,
                                    "open_interest": 1000
                                }
                            }
                        }
                    }
                },
                {
                    "klines": {
                        "SHFE.au2602": {
                            "60000000000": {
                                "last_id": 42,
                                "data": {
                                    "42": {
                                        "id": 42,
                                        "datetime": 1713660000000000000i64,
                                        "open": 610.0,
                                        "high": 620.0,
                                        "low": 609.0,
                                        "close": 618.5,
                                        "volume": 200,
                                        "open_oi": 900,
                                        "close_oi": 1000
                                    }
                                }
                            }
                        }
                    }
                }
            ]
        })
    );
}

#[test]
fn rtn_data_flattens_multi_fragment_inner_frames() {
    let frame = RelayMarketFrame::rtn_data(vec![
        RelayMarketFrame::RtnData(vec![json!({ "a": 1 }), json!({ "b": 2 })]),
        RelayMarketFrame::RtnData(vec![json!({ "c": 3 })]),
    ]);

    assert_eq!(
        frame.into_value(),
        json!({
            "aid": "rtn_data",
            "data": [
                { "a": 1 },
                { "b": 2 },
                { "c": 3 }
            ]
        })
    );
}
