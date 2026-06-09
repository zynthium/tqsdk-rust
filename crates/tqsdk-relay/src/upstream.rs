#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::VecDeque;

use crate::error::{RelayError, RelayResult};
use crate::protocol::RelayTickRow;
use serde_json::Value;
use tqsdk_core::Quote;
#[cfg(feature = "server")]
use tqsdk_core::internal::WebSocketTransport;
#[cfg(feature = "server")]
use tqsdk_core::{OutboundFrame, RawFrame, Transport};

#[derive(Debug, Clone, PartialEq)]
pub struct UpstreamTick {
    pub symbol: String,
    pub row: RelayTickRow,
}

#[derive(Debug, Clone)]
pub struct UpstreamQuote {
    pub symbol: String,
    pub quote: Quote,
}

#[derive(Debug, Clone)]
pub enum UpstreamMarketEvent {
    Tick(UpstreamTick),
    Quote(Box<UpstreamQuote>),
}

#[derive(Debug, Clone)]
pub struct UpstreamMarketDecodeReport {
    ticks: Vec<UpstreamTick>,
    quotes: Vec<UpstreamQuote>,
    invalid_rows: u64,
    last_invalid_row_error: Option<String>,
}

impl UpstreamMarketDecodeReport {
    #[must_use]
    pub fn ticks(&self) -> &[UpstreamTick] {
        &self.ticks
    }

    #[must_use]
    pub fn quotes(&self) -> &[UpstreamQuote] {
        &self.quotes
    }

    #[must_use]
    pub fn invalid_rows(&self) -> u64 {
        self.invalid_rows
    }

    #[must_use]
    pub fn last_invalid_row_error(&self) -> Option<&str> {
        self.last_invalid_row_error.as_deref()
    }

    #[must_use]
    pub fn into_ticks(self) -> Vec<UpstreamTick> {
        self.ticks
    }

    #[must_use]
    pub fn into_events(self) -> Vec<UpstreamMarketEvent> {
        self.ticks
            .into_iter()
            .map(UpstreamMarketEvent::Tick)
            .chain(
                self.quotes
                    .into_iter()
                    .map(|quote| UpstreamMarketEvent::Quote(Box::new(quote))),
            )
            .collect()
    }
}

pub type UpstreamTickDecodeReport = UpstreamMarketDecodeReport;

pub trait UpstreamTickSource {
    fn next_tick(&mut self) -> impl std::future::Future<Output = Option<UpstreamTick>> + Send + '_;

    fn next_event(
        &mut self,
    ) -> impl std::future::Future<Output = Option<UpstreamMarketEvent>> + Send + '_
    where
        Self: Send,
    {
        async { self.next_tick().await.map(UpstreamMarketEvent::Tick) }
    }

    fn take_invalid_tick_rows(&mut self) -> u64 {
        0
    }

    fn take_last_invalid_tick_row_error(&mut self) -> Option<String> {
        None
    }
}

pub fn decode_upstream_ticks(frame: Value) -> RelayResult<Vec<UpstreamTick>> {
    decode_upstream_tick_report(frame).map(UpstreamMarketDecodeReport::into_ticks)
}

pub fn decode_upstream_tick_report(frame: Value) -> RelayResult<UpstreamTickDecodeReport> {
    decode_upstream_market_report(frame)
}

pub fn decode_upstream_market_report(frame: Value) -> RelayResult<UpstreamMarketDecodeReport> {
    if frame.get("aid").and_then(Value::as_str) != Some("rtn_data") {
        return Ok(UpstreamMarketDecodeReport {
            ticks: Vec::new(),
            quotes: Vec::new(),
            invalid_rows: 0,
            last_invalid_row_error: None,
        });
    }
    let Some(data) = frame.get("data").and_then(Value::as_array) else {
        return Ok(UpstreamMarketDecodeReport {
            ticks: Vec::new(),
            quotes: Vec::new(),
            invalid_rows: 0,
            last_invalid_row_error: None,
        });
    };
    let mut ticks = Vec::new();
    let mut quotes = Vec::new();
    let mut invalid_rows = 0_u64;
    let mut last_invalid_row_error = None;
    for fragment in data {
        if let Some(symbols) = fragment.get("ticks").and_then(Value::as_object) {
            for (symbol, series) in symbols {
                let Some(rows) = series.get("data").and_then(Value::as_object) else {
                    continue;
                };
                for (row_id, row) in rows {
                    match decode_tick_row(row_id, row) {
                        Ok(row) => ticks.push(UpstreamTick {
                            symbol: symbol.clone(),
                            row,
                        }),
                        Err(error) => {
                            invalid_rows = invalid_rows.saturating_add(1);
                            last_invalid_row_error =
                                Some(format!("{symbol} row {row_id}: {error}"));
                        }
                    }
                }
            }
        }
        if let Some(symbols) = fragment.get("quotes").and_then(Value::as_object) {
            for (symbol, quote) in symbols {
                let mut quote = serde_json::from_value::<Quote>(quote.clone()).map_err(|err| {
                    RelayError::invalid_protocol(format!("invalid upstream quote row: {err}"))
                })?;
                if quote.instrument_id.is_empty() {
                    quote.instrument_id = symbol.clone();
                }
                quotes.push(UpstreamQuote {
                    symbol: symbol.clone(),
                    quote,
                });
            }
        }
    }
    Ok(UpstreamMarketDecodeReport {
        ticks,
        quotes,
        invalid_rows,
        last_invalid_row_error,
    })
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
    events: VecDeque<UpstreamMarketEvent>,
}

impl FakeUpstreamTickSource {
    pub fn push(&mut self, tick: UpstreamTick) {
        self.events.push_back(UpstreamMarketEvent::Tick(tick));
    }

    pub fn push_quote(&mut self, quote: UpstreamQuote) {
        self.events
            .push_back(UpstreamMarketEvent::Quote(Box::new(quote)));
    }
}

impl UpstreamTickSource for FakeUpstreamTickSource {
    async fn next_tick(&mut self) -> Option<UpstreamTick> {
        while let Some(event) = self.events.pop_front() {
            if let UpstreamMarketEvent::Tick(tick) = event {
                return Some(tick);
            }
        }
        None
    }

    async fn next_event(&mut self) -> Option<UpstreamMarketEvent> {
        self.events.pop_front()
    }
}

#[cfg(feature = "server")]
pub struct WebSocketUpstreamTickSource {
    transport: WebSocketTransport,
    buffered: VecDeque<UpstreamMarketEvent>,
    closed: bool,
    invalid_tick_rows: u64,
    last_invalid_tick_row_error: Option<String>,
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
            invalid_tick_rows: 0,
            last_invalid_tick_row_error: None,
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
            "aid": "subscribe_quote",
            "ins_list": chart.ins_list(),
        }))
        .await?;
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

    async fn recv_events(&mut self) -> RelayResult<Option<Vec<UpstreamMarketEvent>>> {
        match self.transport.recv().await {
            Ok(RawFrame::Text(text)) => {
                let value = serde_json::from_str::<Value>(&text).map_err(|err| {
                    RelayError::invalid_protocol(format!("invalid upstream JSON frame: {err}"))
                })?;
                let report = decode_upstream_market_report(value)?;
                self.record_decode_report(&report);
                let events = report.into_events();
                self.send_peek_message().await?;
                Ok(Some(events))
            }
            Ok(RawFrame::Binary(bytes)) => {
                let value = serde_json::from_slice::<Value>(&bytes).map_err(|err| {
                    RelayError::invalid_protocol(format!("invalid upstream JSON frame: {err}"))
                })?;
                let report = decode_upstream_market_report(value)?;
                self.record_decode_report(&report);
                let events = report.into_events();
                self.send_peek_message().await?;
                Ok(Some(events))
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

    fn record_decode_report(&mut self, report: &UpstreamMarketDecodeReport) {
        self.invalid_tick_rows = self.invalid_tick_rows.saturating_add(report.invalid_rows());
        if let Some(error) = report.last_invalid_row_error() {
            self.last_invalid_tick_row_error = Some(error.to_owned());
        }
    }
}

#[cfg(feature = "server")]
impl UpstreamTickSource for WebSocketUpstreamTickSource {
    async fn next_tick(&mut self) -> Option<UpstreamTick> {
        while let Some(event) = self.next_event().await {
            if let UpstreamMarketEvent::Tick(tick) = event {
                return Some(tick);
            }
        }
        None
    }

    async fn next_event(&mut self) -> Option<UpstreamMarketEvent> {
        loop {
            if let Some(event) = self.buffered.pop_front() {
                return Some(event);
            }
            if self.closed {
                return None;
            }
            match self.recv_events().await {
                Ok(Some(events)) => {
                    self.buffered.extend(events);
                }
                Ok(None) | Err(_) => {
                    self.closed = true;
                }
            }
        }
    }

    fn take_invalid_tick_rows(&mut self) -> u64 {
        std::mem::take(&mut self.invalid_tick_rows)
    }

    fn take_last_invalid_tick_row_error(&mut self) -> Option<String> {
        self.last_invalid_tick_row_error.take()
    }
}
