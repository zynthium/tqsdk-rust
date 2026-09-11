#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
#[cfg(feature = "server")]
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::error::{RelayError, RelayResult};
use crate::protocol::{RelayKlineRow, RelayTickRow};
use serde_json::Value;
#[cfg(feature = "server")]
use tqsdk_core::OutboundFrame;
#[cfg(feature = "server")]
use tqsdk_core::transport::{RawFrame, Transport, WebSocketTransport};
use tqsdk_core::{Kline, Quote, Tick, TradingStatus};

#[cfg(feature = "server")]
const UPSTREAM_IDLE_PEEK_INTERVAL: Duration = Duration::from_secs(1);

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

#[derive(Debug, Clone, PartialEq)]
pub struct UpstreamKline {
    pub symbol: String,
    pub duration_ns: i64,
    pub row: RelayKlineRow,
}

#[derive(Debug, Clone)]
pub struct UpstreamTradingStatus {
    pub symbol: String,
    pub trading_status: TradingStatus,
}

#[derive(Debug, Clone)]
pub enum UpstreamMarketEvent {
    Tick(UpstreamTick),
    Kline(UpstreamKline),
    Quote(Box<UpstreamQuote>),
    TradingStatus(Box<UpstreamTradingStatus>),
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
    pub last_peek_delay_ms: Option<u64>,
    pub last_decode_ms: Option<u64>,
}

impl UpstreamSourceProgress {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        !self.transport_connected
            && !self.subscription_sent
            && self.frames_received == 0
            && self.events_decoded == 0
            && self.last_peek_delay_ms.is_none()
            && self.last_decode_ms.is_none()
    }
}

#[derive(Debug, Clone)]
pub struct UpstreamMarketDecodeReport {
    ticks: Vec<UpstreamTick>,
    klines: Vec<UpstreamKline>,
    quotes: Vec<UpstreamQuote>,
    trading_statuses: Vec<UpstreamTradingStatus>,
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
    pub fn klines(&self) -> &[UpstreamKline] {
        &self.klines
    }

    #[must_use]
    pub fn trading_statuses(&self) -> &[UpstreamTradingStatus] {
        &self.trading_statuses
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
            .chain(self.klines.into_iter().map(UpstreamMarketEvent::Kline))
            .chain(
                self.quotes
                    .into_iter()
                    .map(|quote| UpstreamMarketEvent::Quote(Box::new(quote))),
            )
            .chain(
                self.trading_statuses
                    .into_iter()
                    .map(|status| UpstreamMarketEvent::TradingStatus(Box::new(status))),
            )
            .collect()
    }
}

pub type UpstreamTickDecodeReport = UpstreamMarketDecodeReport;

type TickRowCache = BTreeMap<String, BTreeMap<i64, CachedTickRow>>;
type KlineRowCache = BTreeMap<(String, i64), BTreeMap<i64, Value>>;
type QuoteCache = BTreeMap<String, Value>;
const LOSSLESS_TICK_CACHE_ROWS: usize = 10_000;
const LOSSLESS_TICK_DRAIN_CAPACITY: usize = 2_048;

#[derive(Debug, Clone)]
struct CachedTickRow {
    datetime: Option<i64>,
    last_price: Option<f64>,
    volume: Option<i64>,
    open_interest: Option<i64>,
    raw: Value,
}

impl Default for CachedTickRow {
    fn default() -> Self {
        Self {
            datetime: None,
            last_price: None,
            volume: None,
            open_interest: None,
            raw: Value::Object(serde_json::Map::new()),
        }
    }
}

impl CachedTickRow {
    fn complete(&self, id: i64) -> Option<RelayTickRow> {
        Some(RelayTickRow {
            id,
            datetime: self.datetime?,
            last_price: self.last_price?,
            volume: self.volume?,
            open_interest: self.open_interest?,
        })
    }

    fn lossless_tick(&self) -> Option<Tick> {
        serde_json::from_value(self.raw.clone()).ok()
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
    decode_upstream_market_report_inner(frame, None, None, None)
}

#[cfg(feature = "server")]
fn decode_upstream_market_report_with_cache(
    frame: Value,
    tick_row_cache: &mut TickRowCache,
    kline_row_cache: &mut KlineRowCache,
    quote_cache: &mut QuoteCache,
) -> RelayResult<UpstreamMarketDecodeReport> {
    decode_upstream_market_report_inner(
        frame,
        Some(tick_row_cache),
        Some(kline_row_cache),
        Some(quote_cache),
    )
}

fn decode_upstream_market_report_inner(
    frame: Value,
    mut tick_row_cache: Option<&mut TickRowCache>,
    mut kline_row_cache: Option<&mut KlineRowCache>,
    mut quote_cache: Option<&mut QuoteCache>,
) -> RelayResult<UpstreamMarketDecodeReport> {
    if frame.get("aid").and_then(Value::as_str) != Some("rtn_data") {
        return Ok(UpstreamMarketDecodeReport {
            ticks: Vec::new(),
            klines: Vec::new(),
            quotes: Vec::new(),
            trading_statuses: Vec::new(),
            invalid_rows: 0,
            invalid_rows_by_symbol: BTreeMap::new(),
            last_invalid_row_error: None,
        });
    }
    let Some(data) = frame.get("data").and_then(Value::as_array) else {
        return Ok(UpstreamMarketDecodeReport {
            ticks: Vec::new(),
            klines: Vec::new(),
            quotes: Vec::new(),
            trading_statuses: Vec::new(),
            invalid_rows: 0,
            invalid_rows_by_symbol: BTreeMap::new(),
            last_invalid_row_error: None,
        });
    };
    let mut ticks = Vec::new();
    let mut klines = Vec::new();
    let mut quotes = Vec::new();
    let mut trading_statuses = Vec::new();
    let mut invalid_rows = 0_u64;
    let mut invalid_rows_by_symbol = BTreeMap::new();
    let mut last_invalid_row_error = None;
    for fragment in data {
        if let Some(symbols) = fragment.get("ticks").and_then(Value::as_object) {
            for (symbol, series) in symbols {
                let Some(rows) = series.get("data").and_then(Value::as_object) else {
                    continue;
                };
                let mut sorted_rows: Vec<_> = rows.iter().collect();
                sorted_rows.sort_by_key(|(row_id, _)| row_id.parse::<i64>().unwrap_or(i64::MAX));
                for (row_id, row) in sorted_rows {
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
        if let Some(symbols) = fragment.get("klines").and_then(Value::as_object) {
            for (symbol, durations) in symbols {
                let Some(durations) = durations.as_object() else {
                    continue;
                };
                for (duration_text, series) in durations {
                    let Ok(duration_ns) = duration_text.parse::<i64>() else {
                        continue;
                    };
                    if duration_ns <= 0 {
                        continue;
                    }
                    let Some(rows) = series.get("data").and_then(Value::as_object) else {
                        continue;
                    };
                    let mut sorted_rows: Vec<_> = rows.iter().collect();
                    sorted_rows
                        .sort_by_key(|(row_id, _)| row_id.parse::<i64>().unwrap_or(i64::MAX));
                    for (row_id, row) in sorted_rows {
                        let decoded = match &mut kline_row_cache {
                            Some(cache) => {
                                decode_kline_row_with_cache(cache, symbol, duration_ns, row_id, row)
                            }
                            None => decode_kline_row(row_id, row).map(Some),
                        };
                        match decoded {
                            Ok(Some(row)) => klines.push(UpstreamKline {
                                symbol: symbol.clone(),
                                duration_ns,
                                row,
                            }),
                            Ok(None) => {}
                            Err(error) => {
                                invalid_rows = invalid_rows.saturating_add(1);
                                *invalid_rows_by_symbol.entry(symbol.clone()).or_default() += 1;
                                last_invalid_row_error = Some(format!(
                                    "{symbol} kline {duration_ns} row {row_id}: {error}"
                                ));
                            }
                        }
                    }
                }
            }
        }
        if let Some(symbols) = fragment.get("quotes").and_then(Value::as_object) {
            for (symbol, quote) in symbols {
                let decoded = match &mut quote_cache {
                    Some(cache) => decode_quote_with_cache(cache, symbol, quote)?,
                    None => Some(decode_quote(symbol, quote.clone())?),
                };
                if let Some(quote) = decoded {
                    quotes.push(quote);
                }
            }
        }
        if let Some(symbols) = fragment.get("trading_status").and_then(Value::as_object) {
            for (symbol, row) in symbols {
                let mut trading_status = serde_json::from_value::<TradingStatus>(row.clone())
                    .map_err(|err| {
                        RelayError::invalid_protocol(format!(
                            "invalid upstream trading status row: {err}"
                        ))
                    })?;
                if trading_status.symbol.trim().is_empty() {
                    trading_status.symbol = symbol.clone();
                }
                trading_statuses.push(UpstreamTradingStatus {
                    symbol: trading_status.symbol.clone(),
                    trading_status,
                });
            }
        }
    }
    Ok(UpstreamMarketDecodeReport {
        ticks,
        klines,
        quotes,
        trading_statuses,
        invalid_rows,
        invalid_rows_by_symbol,
        last_invalid_row_error,
    })
}

fn decode_quote_with_cache(
    cache: &mut QuoteCache,
    symbol: &str,
    patch: &Value,
) -> RelayResult<Option<UpstreamQuote>> {
    if patch.is_null() {
        cache.remove(symbol);
        return Ok(None);
    }
    if !patch.is_object() {
        return Err(RelayError::invalid_protocol(
            "upstream quote row must be an object or null",
        ));
    }

    let cached = cache
        .entry(symbol.to_string())
        .or_insert_with(|| Value::Object(serde_json::Map::new()));
    merge_diff(cached, patch);
    decode_quote(symbol, cached.clone()).map(Some)
}

fn decode_quote(symbol: &str, value: Value) -> RelayResult<UpstreamQuote> {
    let mut quote = serde_json::from_value::<Quote>(value).map_err(|err| {
        RelayError::invalid_protocol(format!("invalid upstream quote row: {err}"))
    })?;
    if quote.instrument_id.is_empty() {
        quote.instrument_id = symbol.to_string();
    }
    Ok(UpstreamQuote {
        symbol: symbol.to_string(),
        quote,
    })
}

fn merge_diff(target: &mut Value, patch: &Value) {
    let (Some(target_object), Some(patch_object)) = (target.as_object_mut(), patch.as_object())
    else {
        *target = patch.clone();
        return;
    };

    for (key, value) in patch_object {
        if value.is_null() {
            target_object.remove(key);
        } else if value.is_object() {
            let entry = target_object
                .entry(key.clone())
                .or_insert_with(|| Value::Object(serde_json::Map::new()));
            merge_diff(entry, value);
        } else {
            target_object.insert(key.clone(), value.clone());
        }
    }
}

fn decode_kline_row_with_cache(
    cache: &mut KlineRowCache,
    symbol: &str,
    duration_ns: i64,
    row_id: &str,
    patch: &Value,
) -> RelayResult<Option<RelayKlineRow>> {
    let id = tick_row_id(row_id, patch)?;
    if patch.is_null() {
        if let Some(rows) = cache.get_mut(&(symbol.to_owned(), duration_ns)) {
            rows.remove(&id);
        }
        return Ok(None);
    }
    if !patch.is_object() {
        return Err(crate::error::RelayError::invalid_protocol(
            "upstream Kline row must be an object or null",
        ));
    }
    let cached = cache
        .entry((symbol.to_owned(), duration_ns))
        .or_default()
        .entry(id)
        .or_insert_with(|| Value::Object(serde_json::Map::new()));
    merge_diff(cached, patch);
    cached
        .as_object_mut()
        .expect("cached Kline rows are objects")
        .entry("id".to_owned())
        .or_insert_with(|| Value::from(id));
    decode_kline_row(row_id, cached).map(Some)
}

fn decode_kline_row(row_id: &str, row: &Value) -> RelayResult<RelayKlineRow> {
    Ok(RelayKlineRow {
        id: tick_row_id(row_id, row)?,
        datetime: required_i64(row, "datetime")?,
        open: required_f64(row, "open")?,
        high: required_f64(row, "high")?,
        low: required_f64(row, "low")?,
        close: required_f64(row, "close")?,
        volume: required_i64(row, "volume")?,
        open_oi: required_i64(row, "open_oi")?,
        close_oi: required_i64(row, "close_oi")?,
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
    let mut cached = symbol_cache.get(&id).cloned().unwrap_or_default();
    merge_i64_patch(&mut cached.datetime, row, "datetime")?;
    merge_f64_patch(&mut cached.last_price, row, "last_price")?;
    merge_i64_patch(&mut cached.volume, row, "volume")?;
    merge_i64_patch(&mut cached.open_interest, row, "open_interest")?;
    merge_diff(&mut cached.raw, row);
    cached
        .raw
        .as_object_mut()
        .expect("cached tick rows are objects")
        .entry("id".to_owned())
        .or_insert_with(|| Value::from(id));
    symbol_cache.insert(id, cached);
    while symbol_cache.len() > LOSSLESS_TICK_CACHE_ROWS {
        let _ = symbol_cache.pop_first();
    }
    Ok(symbol_cache.get(&id).and_then(|cached| cached.complete(id)))
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
    match row.get(field) {
        Some(Value::Number(number)) => number.as_f64().ok_or_else(|| {
            RelayError::invalid_protocol(format!("upstream tick row invalid {field}"))
        }),
        Some(Value::String(_)) | Some(Value::Null) => Ok(f64::NAN),
        Some(_) | None => Err(RelayError::invalid_protocol(format!(
            "upstream tick row missing {field}"
        ))),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamTickChart {
    chart_id: String,
    symbols: Vec<String>,
    duration_ns: i64,
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
        Self::new_with_duration(chart_id, symbols, 0, view_width)
    }

    pub fn new_with_duration<I, S>(
        chart_id: impl Into<String>,
        symbols: I,
        duration_ns: i64,
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
        if duration_ns < 0 || view_width == 0 {
            return Err(RelayError::invalid_config(
                "upstream chart duration must be nonnegative and view_width must be greater than zero",
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
            duration_ns,
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
        self.duration_ns
    }

    #[must_use]
    pub const fn view_width(&self) -> usize {
        self.view_width
    }
}

pub(crate) fn upstream_subscription_ins_list_chars(charts: &[UpstreamTickChart]) -> usize {
    charts
        .iter()
        .map(UpstreamTickChart::ins_list_chars)
        .max()
        .unwrap_or(0)
        .max(join_chart_symbols(charts).len())
}

fn join_chart_symbols(charts: &[UpstreamTickChart]) -> String {
    let mut symbols = BTreeSet::new();
    for chart in charts {
        symbols.extend(chart.symbols().iter().cloned());
    }
    join_symbols(&symbols)
}

fn join_symbols(symbols: &BTreeSet<String>) -> String {
    symbols
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(",")
}

#[derive(Debug, Default)]
pub struct FakeUpstreamTickSource {
    events: VecDeque<UpstreamMarketEvent>,
}

impl FakeUpstreamTickSource {
    pub fn push(&mut self, tick: UpstreamTick) {
        self.events.push_back(UpstreamMarketEvent::Tick(tick));
    }

    pub fn push_kline(&mut self, kline: UpstreamKline) {
        self.events.push_back(UpstreamMarketEvent::Kline(kline));
    }

    pub fn push_quote(&mut self, quote: UpstreamQuote) {
        self.events
            .push_back(UpstreamMarketEvent::Quote(Box::new(quote)));
    }

    pub fn push_trading_status(&mut self, status: UpstreamTradingStatus) {
        self.events
            .push_back(UpstreamMarketEvent::TradingStatus(Box::new(status)));
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
    lossless_ticks: VecDeque<(String, Tick)>,
    official_klines: VecDeque<(String, i64, Kline)>,
    lossless_tick_overflow: bool,
    kline_row_cache: KlineRowCache,
    quote_cache: QuoteCache,
    /// Quote-only upstream interest configured at connection time.
    quote_symbols: BTreeSet<String>,
    /// Exact quote set successfully sent upstream, including active tick charts.
    subscribed_quote_symbols: BTreeSet<String>,
    /// Exact tick-chart set successfully sent upstream, keyed by upstream chart id.
    tick_charts: BTreeMap<String, UpstreamTickChart>,
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
            lossless_ticks: VecDeque::new(),
            official_klines: VecDeque::new(),
            lossless_tick_overflow: false,
            kline_row_cache: KlineRowCache::default(),
            quote_cache: QuoteCache::default(),
            quote_symbols: BTreeSet::new(),
            subscribed_quote_symbols: BTreeSet::new(),
            tick_charts: BTreeMap::new(),
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

    pub async fn connect_with_quote_symbols<I, S>(
        url: impl Into<String>,
        symbols: I,
    ) -> RelayResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut source = Self::connect(url).await?;
        source.subscribe_quote_symbols(symbols).await?;
        Ok(source)
    }

    pub async fn subscribe_quote_symbols<I, S>(&mut self, symbols: I) -> RelayResult<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let symbols = symbols
            .into_iter()
            .map(|symbol| symbol.as_ref().trim().to_string())
            .filter(|symbol| !symbol.is_empty())
            .collect::<Vec<_>>();
        if symbols.is_empty() {
            return Err(RelayError::invalid_config(
                "upstream quote subscriptions require at least one symbol",
            ));
        }
        self.quote_symbols.extend(symbols);
        let charts = self.tick_charts.values().cloned().collect::<Vec<_>>();
        self.reconcile_tick_charts(&charts).await
    }

    /// Returns complete, DIFF-merged upstream ticks since the previous drain.
    /// These rows are suitable for the relay rolling cache, unlike the public
    /// five-field downstream tick projection.
    pub fn drain_lossless_ticks(&mut self) -> Vec<(String, Tick)> {
        self.lossless_ticks.drain(..).collect()
    }

    #[must_use]
    pub fn take_lossless_tick_overflow(&mut self) -> bool {
        std::mem::take(&mut self.lossless_tick_overflow)
    }

    pub fn drain_official_klines(&mut self) -> Vec<(String, i64, Kline)> {
        self.official_klines.drain(..).collect()
    }

    pub async fn subscribe_tick_charts(&mut self, charts: &[UpstreamTickChart]) -> RelayResult<()> {
        if charts.is_empty() {
            return Err(RelayError::invalid_config(
                "upstream tick charts require at least one chart",
            ));
        }
        let mut desired = self.tick_charts.clone();
        for chart in charts {
            desired.insert(chart.chart_id().to_owned(), chart.clone());
        }
        let desired = desired.into_values().collect::<Vec<_>>();
        self.reconcile_tick_charts(&desired).await
    }

    /// Reconciles upstream tick charts to the supplied exact desired set.
    ///
    /// Sent-state advances only after every request succeeds. A retry after a
    /// partial transport failure therefore replays idempotent full quote and
    /// `set_chart` requests instead of treating a partial update as confirmed.
    pub(crate) async fn reconcile_tick_charts(
        &mut self,
        charts: &[UpstreamTickChart],
    ) -> RelayResult<()> {
        let mut desired_charts = BTreeMap::new();
        for chart in charts {
            if let Some(previous) =
                desired_charts.insert(chart.chart_id().to_owned(), chart.clone())
                && previous != *chart
            {
                return Err(RelayError::invalid_config(format!(
                    "upstream tick chart_id {} has conflicting definitions",
                    chart.chart_id()
                )));
            }
        }

        let mut desired_quote_symbols = self.quote_symbols.clone();
        for chart in desired_charts.values() {
            desired_quote_symbols.extend(chart.symbols().iter().cloned());
        }
        let quote_changed = desired_quote_symbols != self.subscribed_quote_symbols;
        let stale_charts = self
            .tick_charts
            .iter()
            .filter(|(chart_id, _)| !desired_charts.contains_key(*chart_id))
            .map(|(_, chart)| chart.clone())
            .collect::<Vec<_>>();
        let changed_charts = desired_charts
            .iter()
            .filter(|(chart_id, chart)| self.tick_charts.get(*chart_id) != Some(*chart))
            .map(|(_, chart)| chart.clone())
            .collect::<Vec<_>>();
        // Preserve the established upstream sequencing: a new or replacement
        // chart is preceded by the complete quote subscription, even when that
        // quote set is unchanged. This also keeps peers that gate `set_chart`
        // handling on the preceding quote request compatible.
        let should_send_quote = quote_changed || !changed_charts.is_empty();

        if !should_send_quote && stale_charts.is_empty() && changed_charts.is_empty() {
            return Ok(());
        }

        if should_send_quote {
            self.send_quote_subscription(&desired_quote_symbols).await?;
        }
        for chart in stale_charts {
            self.send_tick_chart_deletion(&chart).await?;
        }
        for chart in changed_charts {
            self.send_tick_chart_subscription(&chart).await?;
        }

        self.subscribed_quote_symbols = desired_quote_symbols;
        self.tick_charts = desired_charts;
        self.record_subscription_sent();
        Ok(())
    }

    async fn send_quote_subscription(&mut self, symbols: &BTreeSet<String>) -> RelayResult<()> {
        self.send_json(serde_json::json!({
            "aid": "subscribe_quote",
            "ins_list": join_symbols(symbols),
        }))
        .await?;
        self.send_peek_message().await?;
        Ok(())
    }

    async fn send_tick_chart_subscription(&mut self, chart: &UpstreamTickChart) -> RelayResult<()> {
        self.send_json(serde_json::json!({
            "aid": "set_chart",
            "chart_id": chart.chart_id(),
            "ins_list": chart.ins_list(),
            "duration": chart.duration_ns(),
            "view_width": chart.view_width(),
        }))
        .await?;
        self.send_peek_message().await
    }

    async fn send_tick_chart_deletion(&mut self, chart: &UpstreamTickChart) -> RelayResult<()> {
        self.send_json(serde_json::json!({
            "aid": "set_chart",
            "chart_id": chart.chart_id(),
            "ins_list": "",
            "duration": chart.duration_ns(),
            "view_width": chart.view_width(),
        }))
        .await?;
        self.send_peek_message().await
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
        let frame =
            match tokio::time::timeout(UPSTREAM_IDLE_PEEK_INTERVAL, self.transport.recv()).await {
                Ok(frame) => frame,
                Err(_) => {
                    let peek_started_at = Instant::now();
                    self.send_peek_message().await?;
                    self.record_peek_sent(millis_u64(peek_started_at.elapsed()));
                    return Ok(Some(Vec::new()));
                }
            };
        match frame {
            Ok(RawFrame::Text(text)) => {
                let frame_received_at = Instant::now();
                self.send_peek_message().await?;
                self.record_peek_sent(millis_u64(frame_received_at.elapsed()));
                let decode_started_at = Instant::now();
                let value = serde_json::from_str::<Value>(&text).map_err(|err| {
                    RelayError::invalid_protocol(format!("invalid upstream JSON frame: {err}"))
                })?;
                let report = self.decode_market_report(value)?;
                let decode_ms = millis_u64(decode_started_at.elapsed());
                self.record_decode_report(&report);
                let events = report.into_events();
                self.record_frame_received(events.len(), Some(decode_ms));
                Ok(Some(events))
            }
            Ok(RawFrame::Binary(bytes)) => {
                let frame_received_at = Instant::now();
                self.send_peek_message().await?;
                self.record_peek_sent(millis_u64(frame_received_at.elapsed()));
                let decode_started_at = Instant::now();
                let value = serde_json::from_slice::<Value>(&bytes).map_err(|err| {
                    RelayError::invalid_protocol(format!("invalid upstream JSON frame: {err}"))
                })?;
                let report = self.decode_market_report(value)?;
                let decode_ms = millis_u64(decode_started_at.elapsed());
                self.record_decode_report(&report);
                let events = report.into_events();
                self.record_frame_received(events.len(), Some(decode_ms));
                Ok(Some(events))
            }
            Ok(RawFrame::Ping | RawFrame::Pong) => {
                let frame_received_at = Instant::now();
                self.send_peek_message().await?;
                self.record_peek_sent(millis_u64(frame_received_at.elapsed()));
                self.record_frame_received(0, None);
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
        let report = decode_upstream_market_report_with_cache(
            value,
            &mut self.tick_row_cache,
            &mut self.kline_row_cache,
            &mut self.quote_cache,
        )?;
        for tick in report.ticks() {
            if let Some(lossless) = self
                .tick_row_cache
                .get(&tick.symbol)
                .and_then(|rows| rows.get(&tick.row.id))
                .and_then(CachedTickRow::lossless_tick)
            {
                if self.lossless_ticks.len() >= LOSSLESS_TICK_DRAIN_CAPACITY {
                    self.lossless_tick_overflow = true;
                } else {
                    self.lossless_ticks
                        .push_back((tick.symbol.clone(), lossless));
                }
            }
        }
        for kline in report.klines() {
            if self.official_klines.len() >= LOSSLESS_TICK_DRAIN_CAPACITY {
                self.lossless_tick_overflow = true;
                continue;
            }
            self.official_klines.push_back((
                kline.symbol.clone(),
                kline.duration_ns,
                Kline {
                    id: kline.row.id,
                    datetime: kline.row.datetime,
                    open: kline.row.open,
                    high: kline.row.high,
                    low: kline.row.low,
                    close: kline.row.close,
                    volume: kline.row.volume,
                    open_oi: kline.row.open_oi,
                    close_oi: kline.row.close_oi,
                    epoch: None,
                },
            ));
        }
        Ok(report)
    }

    fn record_transport_connected(&mut self) {
        self.progress.transport_connected = true;
        self.progress.unix_secs = current_unix_secs();
    }

    fn record_subscription_sent(&mut self) {
        self.progress.subscription_sent = true;
        self.progress.unix_secs = current_unix_secs();
    }

    fn record_peek_sent(&mut self, peek_delay_ms: u64) {
        self.progress.last_peek_delay_ms = Some(peek_delay_ms);
        self.progress.unix_secs = current_unix_secs();
    }

    fn record_frame_received(&mut self, events_decoded: usize, decode_ms: Option<u64>) {
        self.progress.frames_received = self.progress.frames_received.saturating_add(1);
        self.progress.events_decoded = self
            .progress
            .events_decoded
            .saturating_add(u64::try_from(events_decoded).unwrap_or(u64::MAX));
        self.progress.last_decode_ms = decode_ms;
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn cached_kline_patch_keeps_unchanged_official_fields() {
        let mut ticks = TickRowCache::default();
        let mut klines = KlineRowCache::default();
        let mut quotes = QuoteCache::default();
        let first = decode_upstream_market_report_with_cache(
            json!({"aid":"rtn_data","data":[{"klines":{"SHFE.au2602":{"60000000000":{"data":{"7":{
                "datetime": 100,
                "open": 1.0,
                "high": 3.0,
                "low": 0.5,
                "close": 2.0,
                "volume": 4,
                "open_oi": 5,
                "close_oi": 6
            }}}}}}]}),
            &mut ticks,
            &mut klines,
            &mut quotes,
        )
        .unwrap();
        assert_eq!(first.klines()[0].row.close, 2.0);

        let patch = decode_upstream_market_report_with_cache(
            json!({"aid":"rtn_data","data":[{"klines":{"SHFE.au2602":{"60000000000":{"data":{"7":{"close":2.5,"volume":9}}}}}}]}),
            &mut ticks,
            &mut klines,
            &mut quotes,
        )
        .unwrap();
        assert_eq!(patch.klines().len(), 1);
        assert_eq!(patch.klines()[0].row.open, 1.0);
        assert_eq!(patch.klines()[0].row.high, 3.0);
        assert_eq!(patch.klines()[0].row.close, 2.5);
        assert_eq!(patch.klines()[0].row.volume, 9);
    }

    #[test]
    fn cached_tick_retains_full_row_for_rolling_persistence() {
        let mut cache = TickRowCache::default();
        let row = json!({
            "datetime": 100,
            "last_price": 610.5,
            "average": 610.25,
            "volume": 7,
            "amount": 4273.5,
            "open_interest": 9,
            "bid_price1": 610.4,
            "ask_price1": 610.6
        });
        let projected = decode_tick_row_with_cache(&mut cache, "SHFE.au2602", "7", &row)
            .unwrap()
            .unwrap();
        assert_eq!(projected.last_price, 610.5);
        let tick = cache["SHFE.au2602"][&7].lossless_tick().unwrap();
        assert_eq!(tick.id, 7);
        assert_eq!(tick.average, 610.25);
        assert_eq!(tick.amount, 4273.5);
        assert_eq!(tick.bid_price1, 610.4);
    }
}

#[cfg(feature = "server")]
fn millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
