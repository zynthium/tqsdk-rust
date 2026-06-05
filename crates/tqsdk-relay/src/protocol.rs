#![cfg_attr(not(test), forbid(unsafe_code))]

use serde_json::{Value, json};

use crate::error::{RelayError, RelayResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownstreamCommand {
    SubscribeQuote { symbols: Vec<String> },
    SetChart(SetChartCommand),
    PeekMessage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetChartCommand {
    pub chart_id: String,
    pub symbols: Vec<String>,
    pub duration_ns: i64,
    pub view_width: usize,
    pub left_kline_id: Option<i64>,
    pub focus_datetime_ns: Option<i64>,
    pub focus_position: Option<usize>,
}

impl DownstreamCommand {
    pub fn from_value(value: Value) -> RelayResult<Self> {
        let aid = value
            .get("aid")
            .and_then(Value::as_str)
            .ok_or_else(|| RelayError::invalid_protocol("market command missing string aid"))?;
        match aid {
            "subscribe_quote" => Ok(Self::SubscribeQuote {
                symbols: split_symbols(required_string_field(&value, "ins_list")?),
            }),
            "set_chart" => Ok(Self::SetChart(SetChartCommand {
                chart_id: required_string(&value, "chart_id")?,
                symbols: split_symbols(required_string_field(&value, "ins_list")?),
                duration_ns: required_i64(&value, "duration")?,
                view_width: required_usize(&value, "view_width")?,
                left_kline_id: value.get("left_kline_id").and_then(Value::as_i64),
                focus_datetime_ns: value.get("focus_datetime").and_then(Value::as_i64),
                focus_position: value
                    .get("focus_position")
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok()),
            })),
            "peek_message" => Ok(Self::PeekMessage),
            other => Err(RelayError::unsupported_command(other)),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RelayMarketFrame {
    RtnData(Vec<Value>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelayTickRow {
    pub id: i64,
    pub datetime: i64,
    pub last_price: f64,
    pub volume: i64,
    pub open_interest: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelayKlineRow {
    pub id: i64,
    pub datetime: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: i64,
    pub open_oi: i64,
    pub close_oi: i64,
}

impl RelayMarketFrame {
    #[must_use]
    pub fn rtn_data(data: Vec<Self>) -> Self {
        let values = data.into_iter().map(Self::into_inner_value).collect();
        Self::RtnData(values)
    }

    #[must_use]
    pub fn tick_update(symbol: &str, row: RelayTickRow) -> Self {
        Self::RtnData(vec![json!({
            "ticks": {
                symbol: {
                    "last_id": row.id,
                    "data": {
                        row.id.to_string(): {
                            "id": row.id,
                            "datetime": row.datetime,
                            "last_price": row.last_price,
                            "volume": row.volume,
                            "open_interest": row.open_interest
                        }
                    }
                }
            }
        })])
    }

    #[must_use]
    pub fn kline_update(symbol: &str, duration_ns: i64, row: RelayKlineRow) -> Self {
        Self::RtnData(vec![json!({
            "klines": {
                symbol: {
                    duration_ns.to_string(): {
                        "last_id": row.id,
                        "data": {
                            row.id.to_string(): {
                                "id": row.id,
                                "datetime": row.datetime,
                                "open": row.open,
                                "high": row.high,
                                "low": row.low,
                                "close": row.close,
                                "volume": row.volume,
                                "open_oi": row.open_oi,
                                "close_oi": row.close_oi
                            }
                        }
                    }
                }
            }
        })])
    }

    #[must_use]
    pub fn into_value(self) -> Value {
        match self {
            Self::RtnData(data) => json!({
                "aid": "rtn_data",
                "data": data,
            }),
        }
    }

    fn into_inner_value(self) -> Value {
        match self {
            Self::RtnData(mut values) => {
                if values.len() == 1 {
                    values.remove(0)
                } else {
                    json!({ "data": values })
                }
            }
        }
    }
}

fn split_symbols(ins_list: &str) -> Vec<String> {
    ins_list
        .split(',')
        .map(str::trim)
        .filter(|symbol| !symbol.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn required_string_field<'a>(value: &'a Value, key: &'static str) -> RelayResult<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| RelayError::invalid_protocol(format!("market command missing {key}")))
}

fn required_string(value: &Value, key: &'static str) -> RelayResult<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| RelayError::invalid_protocol(format!("market command missing {key}")))
}

fn required_i64(value: &Value, key: &'static str) -> RelayResult<i64> {
    value
        .get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| RelayError::invalid_protocol(format!("market command missing {key}")))
}

fn required_usize(value: &Value, key: &'static str) -> RelayResult<usize> {
    let raw = value
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| RelayError::invalid_protocol(format!("market command missing {key}")))?;
    usize::try_from(raw)
        .map_err(|_| RelayError::invalid_protocol(format!("market command {key} is too large")))
}
