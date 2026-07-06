use serde_json::{Map, Number, Value, json};
use tqsdk_core::{
    CommitScope, InputPayload, IoEvent, Kline, ProtocolDomain, Quote, RuntimeInput, Tick,
};

use crate::replay::{ReplayMarketEvent, ReplayMarketPayload};
use crate::{Result, TaskHost};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplayKlineSpec {
    pub(crate) symbol: String,
    pub(crate) duration_ns: i64,
    pub(crate) view_width: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplayTickSpec {
    pub(crate) symbol: String,
    pub(crate) view_width: usize,
}

pub(crate) fn ingest_replay_market_event(
    host: &TaskHost,
    event: &ReplayMarketEvent,
    klines: &[ReplayKlineSpec],
    ticks: &[ReplayTickSpec],
) -> Result<()> {
    let body = match event.payload() {
        ReplayMarketPayload::Quote(quote) => {
            quote_update(event.symbol(), quote, event.underlying_symbol())
        }
        ReplayMarketPayload::Kline { duration_ns, row } => kline_update(
            event.symbol(),
            *duration_ns,
            row,
            klines,
            event.underlying_symbol(),
        ),
        ReplayMarketPayload::Tick(tick) => {
            tick_update(event.symbol(), tick, ticks, event.underlying_symbol())
        }
    };

    host.api().session().handle().ingest(
        RuntimeInput::Io(IoEvent {
            route: "market-replay".to_string(),
            domains: vec![ProtocolDomain::Market],
            payload: InputPayload::Json(json!({
                "aid": "rtn_data",
                "data": [body]
            })),
        }),
        vec![],
        CommitScope::ReplayStep,
    )?;
    Ok(())
}

pub(crate) fn seed_replay_serials(
    host: &TaskHost,
    klines: &[ReplayKlineSpec],
    ticks: &[ReplayTickSpec],
) -> Result<()> {
    for spec in klines {
        let chart_id = kline_chart_id(&spec.symbol, spec.duration_ns, spec.view_width);
        host.api().session().handle().ingest(
            RuntimeInput::Io(IoEvent {
                route: "market-replay-seed".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "charts": {
                            chart_id: {
                                "state": {
                                    "ins_list": spec.symbol,
                                    "duration": spec.duration_ns
                                },
                                "left_id": -1,
                                "right_id": -1,
                                "more_data": false,
                                "ready": true
                            }
                        },
                        "klines": {
                            spec.symbol.clone(): {
                                spec.duration_ns.to_string(): {
                                    "data": {
                                        "-1": {
                                            "id": -1,
                                            "datetime": -1
                                        }
                                    }
                                }
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::ReplayStep,
        )?;
    }

    for spec in ticks {
        let chart_id = tick_chart_id(&spec.symbol, spec.view_width);
        host.api().session().handle().ingest(
            RuntimeInput::Io(IoEvent {
                route: "market-replay-seed".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "charts": {
                            chart_id: {
                                "state": {
                                    "ins_list": spec.symbol,
                                    "duration": 0
                                },
                                "left_id": -1,
                                "right_id": -1,
                                "more_data": false,
                                "ready": true
                            }
                        },
                        "ticks": {
                            spec.symbol.clone(): {
                                "data": {
                                    "-1": {
                                        "id": -1,
                                        "datetime": -1
                                    }
                                }
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::ReplayStep,
        )?;
    }

    Ok(())
}

fn quote_update(symbol: &str, quote: &Quote, underlying_symbol: Option<&str>) -> Value {
    let mut quote_value = Map::new();
    insert_string_if_present(&mut quote_value, "datetime", &quote.datetime);
    insert_f64_if_finite(&mut quote_value, "last_price", quote.last_price);
    insert_f64_if_finite(&mut quote_value, "ask_price1", quote.ask_price1);
    insert_i64_if_nonzero(&mut quote_value, "ask_volume1", quote.ask_volume1);
    insert_f64_if_finite(&mut quote_value, "bid_price1", quote.bid_price1);
    insert_i64_if_nonzero(&mut quote_value, "bid_volume1", quote.bid_volume1);
    insert_string_if_present(
        &mut quote_value,
        "underlying_symbol",
        effective_underlying_symbol(quote, underlying_symbol),
    );

    json!({
        "quotes": {
            symbol: Value::Object(quote_value)
        }
    })
}

fn kline_update(
    symbol: &str,
    duration_ns: i64,
    row: &Kline,
    klines: &[ReplayKlineSpec],
    underlying_symbol: Option<&str>,
) -> Value {
    let row_id = row.id.to_string();
    let mut charts = Map::new();
    for spec in klines
        .iter()
        .filter(|spec| spec.symbol == symbol && spec.duration_ns == duration_ns)
    {
        let chart_id = kline_chart_id(symbol, duration_ns, spec.view_width);
        charts.insert(
            chart_id,
            json!({
                "state": {
                    "ins_list": symbol,
                    "duration": duration_ns
                },
                "left_id": row.id,
                "right_id": row.id,
                "more_data": false,
                "ready": true
            }),
        );
    }

    let mut body = json!({
        "charts": Value::Object(charts),
        "klines": {
            symbol: {
                duration_ns.to_string(): {
                    "data": {
                        row_id: kline_value(row)
                    }
                }
            }
        }
    });
    insert_quote_underlying_update(&mut body, symbol, underlying_symbol);
    body
}

fn tick_update(
    symbol: &str,
    tick: &Tick,
    ticks: &[ReplayTickSpec],
    underlying_symbol: Option<&str>,
) -> Value {
    let row_id = tick.id.to_string();
    let mut charts = Map::new();
    for spec in ticks.iter().filter(|spec| spec.symbol == symbol) {
        let chart_id = tick_chart_id(symbol, spec.view_width);
        charts.insert(
            chart_id,
            json!({
                "state": {
                    "ins_list": symbol,
                    "duration": 0
                },
                "left_id": tick.id,
                "right_id": tick.id,
                "more_data": false,
                "ready": true
            }),
        );
    }

    let mut body = json!({
        "charts": Value::Object(charts),
        "ticks": {
            symbol: {
                "data": {
                    row_id: tick_value(tick)
                }
            }
        }
    });
    insert_quote_underlying_update(&mut body, symbol, underlying_symbol);
    body
}

fn kline_value(row: &Kline) -> Value {
    let mut value = Map::new();
    value.insert("id".to_string(), Value::from(row.id));
    value.insert("datetime".to_string(), Value::from(row.datetime));
    insert_f64_if_finite(&mut value, "open", row.open);
    insert_f64_if_finite(&mut value, "high", row.high);
    insert_f64_if_finite(&mut value, "low", row.low);
    insert_f64_if_finite(&mut value, "close", row.close);
    insert_i64_if_nonzero(&mut value, "volume", row.volume);
    insert_i64_if_nonzero(&mut value, "open_oi", row.open_oi);
    insert_i64_if_nonzero(&mut value, "close_oi", row.close_oi);
    Value::Object(value)
}

fn tick_value(row: &Tick) -> Value {
    let mut value = Map::new();
    value.insert("id".to_string(), Value::from(row.id));
    value.insert("datetime".to_string(), Value::from(row.datetime));
    insert_f64_if_finite(&mut value, "last_price", row.last_price);
    insert_f64_if_finite(&mut value, "average", row.average);
    insert_f64_if_finite(&mut value, "highest", row.highest);
    insert_f64_if_finite(&mut value, "lowest", row.lowest);
    insert_f64_if_finite(&mut value, "ask_price1", row.ask_price1);
    insert_i64_if_nonzero(&mut value, "ask_volume1", row.ask_volume1);
    insert_f64_if_finite(&mut value, "bid_price1", row.bid_price1);
    insert_i64_if_nonzero(&mut value, "bid_volume1", row.bid_volume1);
    insert_i64_if_nonzero(&mut value, "volume", row.volume);
    insert_f64_if_finite(&mut value, "amount", row.amount);
    insert_i64_if_nonzero(&mut value, "open_interest", row.open_interest);
    Value::Object(value)
}

fn effective_underlying_symbol<'a>(
    quote: &'a Quote,
    event_underlying_symbol: Option<&'a str>,
) -> &'a str {
    if !quote.underlying_symbol.is_empty() {
        &quote.underlying_symbol
    } else {
        event_underlying_symbol.unwrap_or("")
    }
}

fn insert_quote_underlying_update(body: &mut Value, symbol: &str, underlying_symbol: Option<&str>) {
    let Some(underlying_symbol) = underlying_symbol else {
        return;
    };
    if underlying_symbol.is_empty() {
        return;
    }
    let Some(object) = body.as_object_mut() else {
        return;
    };
    object.insert(
        "quotes".to_string(),
        json!({
            symbol: {
                "underlying_symbol": underlying_symbol
            }
        }),
    );
}

fn insert_string_if_present(value: &mut Map<String, Value>, key: &str, field: &str) {
    if !field.is_empty() {
        value.insert(key.to_string(), Value::from(field));
    }
}

fn insert_f64_if_finite(value: &mut Map<String, Value>, key: &str, field: f64) {
    if let Some(number) = Number::from_f64(field) {
        value.insert(key.to_string(), Value::Number(number));
    }
}

fn insert_i64_if_nonzero(value: &mut Map<String, Value>, key: &str, field: i64) {
    if field != 0 {
        value.insert(key.to_string(), Value::from(field));
    }
}

fn kline_chart_id(symbol: &str, duration_ns: i64, view_width: usize) -> String {
    let symbol = sanitize_chart_token(symbol);
    format!("wait-kline-{symbol}-{duration_ns}-{view_width}")
}

fn tick_chart_id(symbol: &str, view_width: usize) -> String {
    let symbol = sanitize_chart_token(symbol);
    format!("wait-tick-{symbol}-{view_width}")
}

fn sanitize_chart_token(raw: &str) -> String {
    raw.chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}
