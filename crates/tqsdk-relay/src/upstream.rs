#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::VecDeque;

use crate::error::{RelayError, RelayResult};
use crate::protocol::RelayTickRow;
use serde_json::Value;
#[cfg(feature = "server")]
use tqsdk_core::internal::WebSocketTransport;
#[cfg(feature = "server")]
use tqsdk_core::{OutboundFrame, RawFrame, Transport};

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
    pub fn ins_list(&self) -> String {
        self.symbols.join(",")
    }

    #[must_use]
    pub fn ins_list_chars(&self) -> usize {
        self.ins_list().len()
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

#[cfg(feature = "server")]
pub struct WebSocketUpstreamTickSource {
    transport: WebSocketTransport,
    buffered: VecDeque<UpstreamTick>,
    closed: bool,
}

#[cfg(feature = "server")]
impl WebSocketUpstreamTickSource {
    pub async fn connect(url: impl Into<String>) -> RelayResult<Self> {
        let mut transport = WebSocketTransport::new(url);
        transport.connect().await.map_err(|err| {
            RelayError::Transport(format!("upstream websocket connect failed: {err}"))
        })?;
        Ok(Self {
            transport,
            buffered: VecDeque::new(),
            closed: false,
        })
    }

    pub async fn connect_with_tick_chart(
        url: impl Into<String>,
        chart: UpstreamTickChart,
    ) -> RelayResult<Self> {
        let mut source = Self::connect(url).await?;
        source.subscribe_tick_chart(&chart).await?;
        Ok(source)
    }

    async fn subscribe_tick_chart(&mut self, chart: &UpstreamTickChart) -> RelayResult<()> {
        self.send_json(serde_json::json!({
            "aid": "set_chart",
            "chart_id": chart.chart_id(),
            "ins_list": chart.ins_list(),
            "duration": chart.duration_ns(),
            "view_width": chart.view_width(),
        }))
        .await?;
        self.send_json(serde_json::json!({"aid": "peek_message"}))
            .await
    }

    async fn send_peek_message(&mut self) -> RelayResult<()> {
        self.send_json(serde_json::json!({"aid": "peek_message"}))
            .await
    }

    async fn send_json(&mut self, value: Value) -> RelayResult<()> {
        self.transport
            .send(OutboundFrame::Text(value.to_string()))
            .await
            .map_err(|err| RelayError::Transport(format!("upstream websocket send failed: {err}")))
    }

    async fn recv_ticks(&mut self) -> RelayResult<Option<Vec<UpstreamTick>>> {
        match self.transport.recv().await {
            Ok(RawFrame::Text(text)) => {
                let value = serde_json::from_str::<Value>(&text).map_err(|err| {
                    RelayError::invalid_protocol(format!("invalid upstream JSON frame: {err}"))
                })?;
                let ticks = decode_upstream_ticks(value)?;
                self.send_peek_message().await?;
                Ok(Some(ticks))
            }
            Ok(RawFrame::Binary(bytes)) => {
                let value = serde_json::from_slice::<Value>(&bytes).map_err(|err| {
                    RelayError::invalid_protocol(format!("invalid upstream JSON frame: {err}"))
                })?;
                let ticks = decode_upstream_ticks(value)?;
                self.send_peek_message().await?;
                Ok(Some(ticks))
            }
            Ok(RawFrame::Ping | RawFrame::Pong) => {
                self.send_peek_message().await?;
                Ok(Some(Vec::new()))
            }
            Ok(RawFrame::Close) => Ok(None),
            Err(err) => Err(RelayError::Transport(format!(
                "upstream websocket recv failed: {err}"
            ))),
        }
    }
}

#[cfg(feature = "server")]
impl UpstreamTickSource for WebSocketUpstreamTickSource {
    async fn next_tick(&mut self) -> Option<UpstreamTick> {
        loop {
            if let Some(tick) = self.buffered.pop_front() {
                return Some(tick);
            }
            if self.closed {
                return None;
            }
            match self.recv_ticks().await {
                Ok(Some(ticks)) => {
                    self.buffered.extend(ticks);
                }
                Ok(None) | Err(_) => {
                    self.closed = true;
                }
            }
        }
    }
}
