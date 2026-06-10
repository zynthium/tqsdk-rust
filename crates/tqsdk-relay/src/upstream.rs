#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, VecDeque};
#[cfg(feature = "server")]
use std::time::{SystemTime, UNIX_EPOCH};

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
pub enum UpstreamSourceUpdate {
    Event(UpstreamMarketEvent),
    Progress,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UpstreamSourceProgress {
    pub transport_connected: bool,
    pub subscription_sent: bool,
    pub frames_received: u64,
    pub events_decoded: u64,
    pub unix_secs: u64,
}

impl UpstreamSourceProgress {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        !self.transport_connected
            && !self.subscription_sent
            && self.frames_received == 0
            && self.events_decoded == 0
    }
}

#[derive(Debug, Clone)]
pub struct UpstreamMarketDecodeReport {
    ticks: Vec<UpstreamTick>,
    quotes: Vec<UpstreamQuote>,
    invalid_rows: u64,
    invalid_rows_by_symbol: BTreeMap<String, u64>,
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
    pub fn invalid_rows_by_symbol(&self) -> &BTreeMap<String, u64> {
        &self.invalid_rows_by_symbol
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

type TickRowCache = BTreeMap<String, BTreeMap<i64, CachedTickRow>>;

#[derive(Debug, Clone, Copy, Default)]
struct CachedTickRow {
    datetime: Option<i64>,
    last_price: Option<f64>,
    volume: Option<i64>,
    open_interest: Option<i64>,
}

impl CachedTickRow {
    fn complete(self, id: i64) -> Option<RelayTickRow> {
        Some(RelayTickRow {
            id,
            datetime: self.datetime?,
            last_price: self.last_price?,
            volume: self.volume?,
            open_interest: self.open_interest?,
        })
    }
}

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

    fn next_update(
        &mut self,
    ) -> impl std::future::Future<Output = Option<UpstreamSourceUpdate>> + Send + '_
    where
        Self: Send,
    {
        async { self.next_event().await.map(UpstreamSourceUpdate::Event) }
    }

    fn take_invalid_tick_rows(&mut self) -> u64 {
        0
    }

    fn take_invalid_tick_rows_by_symbol(&mut self) -> BTreeMap<String, u64> {
        BTreeMap::new()
    }

    fn take_last_invalid_tick_row_error(&mut self) -> Option<String> {
        None
    }

    fn take_progress(&mut self) -> UpstreamSourceProgress {
        UpstreamSourceProgress::default()
    }
}

pub fn decode_upstream_ticks(frame: Value) -> RelayResult<Vec<UpstreamTick>> {
    decode_upstream_tick_report(frame).map(UpstreamMarketDecodeReport::into_ticks)
}

pub fn decode_upstream_tick_report(frame: Value) -> RelayResult<UpstreamTickDecodeReport> {
    decode_upstream_market_report(frame)
}

pub fn decode_upstream_market_report(frame: Value) -> RelayResult<UpstreamMarketDecodeReport> {
    decode_upstream_market_report_inner(frame, None)
}

fn decode_upstream_market_report_with_cache(
    frame: Value,
    tick_row_cache: &mut TickRowCache,
) -> RelayResult<UpstreamMarketDecodeReport> {
    decode_upstream_market_report_inner(frame, Some(tick_row_cache))
}

fn decode_upstream_market_report_inner(
    frame: Value,
    mut tick_row_cache: Option<&mut TickRowCache>,
) -> RelayResult<UpstreamMarketDecodeReport> {
    if frame.get("aid").and_then(Value::as_str) != Some("rtn_data") {
        return Ok(UpstreamMarketDecodeReport {
            ticks: Vec::new(),
            quotes: Vec::new(),
            invalid_rows: 0,
            invalid_rows_by_symbol: BTreeMap::new(),
            last_invalid_row_error: None,
        });
    }
    let Some(data) = frame.get("data").and_then(Value::as_array) else {
        return Ok(UpstreamMarketDecodeReport {
            ticks: Vec::new(),
            quotes: Vec::new(),
            invalid_rows: 0,
            invalid_rows_by_symbol: BTreeMap::new(),
            last_invalid_row_error: None,
        });
    };
    let mut ticks = Vec::new();
    let mut quotes = Vec::new();
    let mut invalid_rows = 0_u64;
    let mut invalid_rows_by_symbol = BTreeMap::new();
    let mut last_invalid_row_error = None;
    for fragment in data {
        if let Some(symbols) = fragment.get("ticks").and_then(Value::as_object) {
            for (symbol, series) in symbols {
                let Some(rows) = series.get("data").and_then(Value::as_object) else {
                    continue;
                };
                for (row_id, row) in rows {
                    let decoded = match &mut tick_row_cache {
                        Some(cache) => decode_tick_row_with_cache(cache, symbol, row_id, row),
                        None => decode_tick_row(row_id, row).map(Some),
                    };
                    match decoded {
                        Ok(Some(row)) => ticks.push(UpstreamTick {
                            symbol: symbol.clone(),
                            row,
                        }),
                        Ok(None) => {}
                        Err(error) => {
                            invalid_rows = invalid_rows.saturating_add(1);
                            *invalid_rows_by_symbol.entry(symbol.clone()).or_default() += 1;
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
        invalid_rows_by_symbol,
        last_invalid_row_error,
    })
}

fn decode_tick_row(row_id: &str, row: &Value) -> RelayResult<RelayTickRow> {
    let id = tick_row_id(row_id, row)?;
    Ok(RelayTickRow {
        id,
        datetime: required_i64(row, "datetime")?,
        last_price: required_f64(row, "last_price")?,
        volume: required_i64(row, "volume")?,
        open_interest: required_i64(row, "open_interest")?,
    })
}

fn decode_tick_row_with_cache(
    cache: &mut TickRowCache,
    symbol: &str,
    row_id: &str,
    row: &Value,
) -> RelayResult<Option<RelayTickRow>> {
    let id = tick_row_id(row_id, row)?;
    let symbol_cache = cache.entry(symbol.to_string()).or_default();
    let mut cached = symbol_cache.get(&id).copied().unwrap_or_default();
    merge_i64_patch(&mut cached.datetime, row, "datetime")?;
    merge_f64_patch(&mut cached.last_price, row, "last_price")?;
    merge_i64_patch(&mut cached.volume, row, "volume")?;
    merge_i64_patch(&mut cached.open_interest, row, "open_interest")?;
    symbol_cache.insert(id, cached);
    Ok(cached.complete(id))
}

fn tick_row_id(row_id: &str, row: &Value) -> RelayResult<i64> {
    row.get("id")
        .and_then(Value::as_i64)
        .or_else(|| row_id.parse::<i64>().ok())
        .ok_or_else(|| RelayError::invalid_protocol("upstream tick row missing id"))
}

fn merge_i64_patch(cached: &mut Option<i64>, row: &Value, field: &'static str) -> RelayResult<()> {
    if row.get(field).is_some() {
        *cached = Some(required_i64(row, field)?);
    }
    Ok(())
}

fn merge_f64_patch(cached: &mut Option<f64>, row: &Value, field: &'static str) -> RelayResult<()> {
    if row.get(field).is_some() {
        *cached = Some(required_f64(row, field)?);
    }
    Ok(())
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
        if symbols.len() != 1 {
            return Err(RelayError::invalid_config(
                "upstream tick chart requires exactly one symbol",
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
    pub fn symbol(&self) -> &str {
        self.symbols[0].as_str()
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
    tick_row_cache: TickRowCache,
    closed: bool,
    invalid_tick_rows: u64,
    invalid_tick_rows_by_symbol: BTreeMap<String, u64>,
    last_invalid_tick_row_error: Option<String>,
    progress: UpstreamSourceProgress,
}

#[cfg(feature = "server")]
impl WebSocketUpstreamTickSource {
    pub async fn connect(url: impl Into<String>) -> RelayResult<Self> {
        let mut transport = WebSocketTransport::new(url);
        transport.connect().await.map_err(|err| {
            RelayError::Transport(format!("upstream websocket connect failed: {err}"))
        })?;
        let mut source = Self {
            transport,
            buffered: VecDeque::new(),
            tick_row_cache: TickRowCache::default(),
            closed: false,
            invalid_tick_rows: 0,
            invalid_tick_rows_by_symbol: BTreeMap::new(),
            last_invalid_tick_row_error: None,
            progress: UpstreamSourceProgress::default(),
        };
        source.record_transport_connected();
        Ok(source)
    }

    pub async fn connect_with_tick_chart(
        url: impl Into<String>,
        chart: UpstreamTickChart,
    ) -> RelayResult<Self> {
        Self::connect_with_tick_charts(url, [chart]).await
    }

    pub async fn connect_with_tick_charts<I>(url: impl Into<String>, charts: I) -> RelayResult<Self>
    where
        I: IntoIterator<Item = UpstreamTickChart>,
    {
        let mut source = Self::connect(url).await?;
        let charts: Vec<UpstreamTickChart> = charts.into_iter().collect();
        source.subscribe_tick_charts(&charts).await?;
        Ok(source)
    }

    async fn subscribe_tick_charts(&mut self, charts: &[UpstreamTickChart]) -> RelayResult<()> {
        if charts.is_empty() {
            return Err(RelayError::invalid_config(
                "upstream tick charts require at least one chart",
            ));
        }
        for chart in charts {
            self.send_json(serde_json::json!({
                "aid": "set_chart",
                "chart_id": chart.chart_id(),
                "ins_list": chart.ins_list(),
                "duration": chart.duration_ns(),
                "view_width": chart.view_width(),
            }))
            .await?;
            self.send_peek_message().await?;
        }
        self.record_subscription_sent();
        Ok(())
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
                let report = self.decode_market_report(value)?;
                self.record_decode_report(&report);
                let events = report.into_events();
                self.record_frame_received(events.len());
                self.send_peek_message().await?;
                Ok(Some(events))
            }
            Ok(RawFrame::Binary(bytes)) => {
                let value = serde_json::from_slice::<Value>(&bytes).map_err(|err| {
                    RelayError::invalid_protocol(format!("invalid upstream JSON frame: {err}"))
                })?;
                let report = self.decode_market_report(value)?;
                self.record_decode_report(&report);
                let events = report.into_events();
                self.record_frame_received(events.len());
                self.send_peek_message().await?;
                Ok(Some(events))
            }
            Ok(RawFrame::Ping | RawFrame::Pong) => {
                self.record_frame_received(0);
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
        for (symbol, count) in report.invalid_rows_by_symbol() {
            let entry = self
                .invalid_tick_rows_by_symbol
                .entry(symbol.clone())
                .or_default();
            *entry = entry.saturating_add(*count);
        }
        if let Some(error) = report.last_invalid_row_error() {
            self.last_invalid_tick_row_error = Some(error.to_owned());
        }
    }

    fn decode_market_report(&mut self, value: Value) -> RelayResult<UpstreamMarketDecodeReport> {
        decode_upstream_market_report_with_cache(value, &mut self.tick_row_cache)
    }

    fn record_transport_connected(&mut self) {
        self.progress.transport_connected = true;
        self.progress.unix_secs = current_unix_secs();
    }

    fn record_subscription_sent(&mut self) {
        self.progress.subscription_sent = true;
        self.progress.unix_secs = current_unix_secs();
    }

    fn record_frame_received(&mut self, events_decoded: usize) {
        self.progress.frames_received = self.progress.frames_received.saturating_add(1);
        self.progress.events_decoded = self
            .progress
            .events_decoded
            .saturating_add(u64::try_from(events_decoded).unwrap_or(u64::MAX));
        self.progress.unix_secs = current_unix_secs();
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
        while let Some(update) = self.next_update().await {
            if let UpstreamSourceUpdate::Event(event) = update {
                return Some(event);
            }
        }
        None
    }

    async fn next_update(&mut self) -> Option<UpstreamSourceUpdate> {
        loop {
            if let Some(event) = self.buffered.pop_front() {
                return Some(UpstreamSourceUpdate::Event(event));
            }
            if self.closed {
                return None;
            }
            match self.recv_events().await {
                Ok(Some(events)) if events.is_empty() => {
                    return Some(UpstreamSourceUpdate::Progress);
                }
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

    fn take_invalid_tick_rows_by_symbol(&mut self) -> BTreeMap<String, u64> {
        std::mem::take(&mut self.invalid_tick_rows_by_symbol)
    }

    fn take_last_invalid_tick_row_error(&mut self) -> Option<String> {
        self.last_invalid_tick_row_error.take()
    }

    fn take_progress(&mut self) -> UpstreamSourceProgress {
        std::mem::take(&mut self.progress)
    }
}

#[cfg(feature = "server")]
fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}
