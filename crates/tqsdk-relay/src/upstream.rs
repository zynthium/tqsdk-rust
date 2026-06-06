#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::VecDeque;

use crate::error::{RelayError, RelayResult};
use crate::protocol::RelayTickRow;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct UpstreamTick {
    pub symbol: String,
    pub row: RelayTickRow,
}

pub trait UpstreamTickSource {
    fn next_tick(&mut self) -> impl std::future::Future<Output = Option<UpstreamTick>> + Send + '_;
}

pub fn decode_upstream_ticks(frame: Value) -> RelayResult<Vec<UpstreamTick>> {
    if frame.get("aid").and_then(Value::as_str) != Some("rtn_data") {
        return Ok(Vec::new());
    }
    let Some(data) = frame.get("data").and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    let mut ticks = Vec::new();
    for fragment in data {
        let Some(symbols) = fragment.get("ticks").and_then(Value::as_object) else {
            continue;
        };
        for (symbol, series) in symbols {
            let Some(rows) = series.get("data").and_then(Value::as_object) else {
                continue;
            };
            for (row_id, row) in rows {
                ticks.push(UpstreamTick {
                    symbol: symbol.clone(),
                    row: decode_tick_row(row_id, row)?,
                });
            }
        }
    }
    Ok(ticks)
}

fn decode_tick_row(row_id: &str, row: &Value) -> RelayResult<RelayTickRow> {
    Ok(RelayTickRow {
        id: row
            .get("id")
            .and_then(Value::as_i64)
            .or_else(|| row_id.parse::<i64>().ok())
            .ok_or_else(|| RelayError::invalid_protocol("upstream tick row missing id"))?,
        datetime: required_i64(row, "datetime")?,
        last_price: required_f64(row, "last_price")?,
        volume: required_i64(row, "volume")?,
        open_interest: required_i64(row, "open_interest")?,
    })
}

fn required_i64(row: &Value, field: &'static str) -> RelayResult<i64> {
    row.get(field)
        .and_then(Value::as_i64)
        .ok_or_else(|| RelayError::invalid_protocol(format!("upstream tick row missing {field}")))
}

fn required_f64(row: &Value, field: &'static str) -> RelayResult<f64> {
    row.get(field)
        .and_then(Value::as_f64)
        .ok_or_else(|| RelayError::invalid_protocol(format!("upstream tick row missing {field}")))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamTickChart {
    chart_id: String,
    symbols: Vec<String>,
    view_width: usize,
}

impl UpstreamTickChart {
    pub fn new<I, S>(
        chart_id: impl Into<String>,
        symbols: I,
        view_width: usize,
    ) -> RelayResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let chart_id = chart_id.into();
        if chart_id.trim().is_empty() {
            return Err(RelayError::invalid_config(
                "upstream tick chart_id must not be empty",
            ));
        }
        if view_width == 0 {
            return Err(RelayError::invalid_config(
                "upstream tick view_width must be greater than zero",
            ));
        }
        let mut symbols: Vec<String> = symbols
            .into_iter()
            .map(|symbol| symbol.as_ref().trim().to_string())
            .filter(|symbol| !symbol.is_empty())
            .collect();
        symbols.sort();
        symbols.dedup();
        if symbols.is_empty() {
            return Err(RelayError::invalid_config(
                "upstream tick chart requires at least one symbol",
            ));
        }
        Ok(Self {
            chart_id,
            symbols,
            view_width,
        })
    }

    #[must_use]
    pub fn chart_id(&self) -> &str {
        &self.chart_id
    }

    #[must_use]
    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }

    #[must_use]
    pub const fn duration_ns(&self) -> i64 {
        0
    }

    #[must_use]
    pub const fn view_width(&self) -> usize {
        self.view_width
    }
}

#[derive(Debug, Default)]
pub struct FakeUpstreamTickSource {
    ticks: VecDeque<UpstreamTick>,
}

impl FakeUpstreamTickSource {
    pub fn push(&mut self, tick: UpstreamTick) {
        self.ticks.push_back(tick);
    }
}

impl UpstreamTickSource for FakeUpstreamTickSource {
    async fn next_tick(&mut self) -> Option<UpstreamTick> {
        self.ticks.pop_front()
    }
}
