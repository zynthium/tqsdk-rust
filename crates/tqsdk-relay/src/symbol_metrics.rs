#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use chrono::{NaiveTime, TimeZone, Timelike};
use serde::Serialize;
use tqsdk_core::{
    Quote, TradingSessionPhase, TradingSessionSchedule, TradingSessionSegment, TradingTime,
};

use crate::protocol::RelayTickRow;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SymbolStatus {
    Live,
    Closed,
    Stale,
    Missing,
    Inactive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SymbolProblemSeverity {
    Live,
    Closed,
    Warn,
    Bad,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SymbolSort {
    #[default]
    SymbolAsc,
    StatusAsc,
    ReceiveGapDesc,
    MarketTimeLagDesc,
    TicksIngestedDesc,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SymbolMetricsQuery {
    pub statuses: Vec<SymbolStatus>,
    pub subscribed_only: bool,
    pub q: Option<String>,
    pub sort: SymbolSort,
    pub limit: Option<usize>,
}

impl SymbolMetricsQuery {
    pub fn from_query_string(query: &str) -> Result<Self, &'static str> {
        let mut parsed = Self::default();
        if query.is_empty() {
            return Ok(parsed);
        }
        for pair in query.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let key = decode_query_component(key)?;
            let value = decode_query_component(value)?;
            match key.as_str() {
                "status" => parsed.statuses = parse_statuses(&value)?,
                "subscribed" => parsed.subscribed_only = parse_bool(&value)?,
                "q" => {
                    if !value.is_empty() {
                        parsed.q = Some(value);
                    }
                }
                "sort" => parsed.sort = parse_sort(&value)?,
                "limit" => parsed.limit = Some(parse_limit(&value)?),
                _ => return Err("unknown query parameter"),
            }
        }
        Ok(parsed)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SymbolSubscriptionCounts {
    pub quote_subscriber_count: usize,
    pub chart_subscriber_count: usize,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SymbolTelemetry {
    instrument_name: Option<String>,
    trading_segments: Option<Vec<TradingSessionSegment>>,
    ticks_ingested: u64,
    last_receive_unix_millis: Option<u64>,
    last_tick_datetime_ns: Option<i64>,
    last_price: Option<f64>,
    last_volume: Option<i64>,
    last_open_interest: Option<i64>,
    invalid_rows: u64,
    last_invalid_row_error: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct SymbolTelemetryStore {
    universe: BTreeSet<String>,
    telemetry: BTreeMap<String, SymbolTelemetry>,
    last_universe_refresh_unix_millis: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SymbolMetricsSnapshot {
    pub now_unix_millis: u64,
    pub data_stale_after_millis: u64,
    pub summary: SymbolMetricsSummary,
    pub symbols: Vec<SymbolTelemetrySnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SymbolMetricsSummary {
    pub total: usize,
    pub live: usize,
    pub closed: usize,
    pub stale: usize,
    pub missing: usize,
    pub inactive: usize,
    pub subscribed: usize,
    pub p95_receive_gap_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SymbolTelemetrySnapshot {
    pub symbol: String,
    pub instrument_name: Option<String>,
    pub status: SymbolStatus,
    pub problem: bool,
    pub problem_severity: SymbolProblemSeverity,
    pub in_universe: bool,
    pub subscribed: bool,
    pub quote_subscriber_count: usize,
    pub chart_subscriber_count: usize,
    pub ticks_ingested: u64,
    pub receive_gap_ms: Option<u64>,
    pub market_time_lag_ms: Option<u64>,
    pub last_receive_unix_millis: Option<u64>,
    pub last_tick_datetime_ns: Option<i64>,
    pub last_price: Option<f64>,
    pub last_volume: Option<i64>,
    pub last_open_interest: Option<i64>,
    pub invalid_rows: u64,
    pub last_invalid_row_error: Option<String>,
}

impl SymbolTelemetryStore {
    pub fn record_universe<I, S>(&mut self, symbols: I, unix_millis: u64)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.universe = symbols
            .into_iter()
            .map(|symbol| symbol.as_ref().trim().to_string())
            .filter(|symbol| !symbol.is_empty())
            .collect();
        self.last_universe_refresh_unix_millis = Some(unix_millis);
    }

    pub fn record_symbol_trading_time(&mut self, symbol: &str, trading_time: &TradingTime) {
        if let Some(trading_segments) = trading_segments_from_trading_time(trading_time) {
            self.telemetry
                .entry(symbol.to_string())
                .or_default()
                .trading_segments = Some(trading_segments);
        }
    }

    pub fn record_tick_at(&mut self, symbol: &str, row: &RelayTickRow, receive_unix_millis: u64) {
        let telemetry = self.telemetry.entry(symbol.to_string()).or_default();
        telemetry.ticks_ingested = telemetry.ticks_ingested.saturating_add(1);
        telemetry.last_receive_unix_millis = Some(receive_unix_millis);
        telemetry.last_tick_datetime_ns = Some(row.datetime);
        telemetry.last_price = Some(row.last_price);
        telemetry.last_volume = Some(row.volume);
        telemetry.last_open_interest = Some(row.open_interest);
    }

    pub fn record_quote_at(&mut self, symbol: &str, quote: &Quote, receive_unix_millis: u64) {
        let telemetry = self.telemetry.entry(symbol.to_string()).or_default();
        let instrument_name = quote.instrument_name.trim();
        if !instrument_name.is_empty() {
            telemetry.instrument_name = Some(instrument_name.to_string());
        }
        if let Some(trading_segments) = trading_segments_from_trading_time(&quote.trading_time) {
            telemetry.trading_segments = Some(trading_segments);
        }
        telemetry.last_receive_unix_millis = Some(receive_unix_millis);
        telemetry.last_tick_datetime_ns = quote.datetime.parse::<i64>().ok();
        telemetry.last_price = Some(quote.last_price);
        telemetry.last_volume = Some(quote.volume);
        telemetry.last_open_interest = Some(quote.open_interest);
    }

    pub fn record_invalid_row(&mut self, symbol: &str, message: impl Into<String>) {
        self.record_invalid_rows(symbol, 1, Some(message.into()));
    }

    pub fn record_invalid_rows(&mut self, symbol: &str, count: u64, message: Option<String>) {
        if count == 0 {
            return;
        }
        let telemetry = self.telemetry.entry(symbol.to_string()).or_default();
        telemetry.invalid_rows = telemetry.invalid_rows.saturating_add(count);
        if let Some(message) = message {
            telemetry.last_invalid_row_error = Some(message);
        }
    }

    pub fn snapshot_at(
        &self,
        now_unix_millis: u64,
        stale_after_millis: u64,
        subscriptions: &BTreeMap<String, SymbolSubscriptionCounts>,
        query: &SymbolMetricsQuery,
    ) -> SymbolMetricsSnapshot {
        let mut symbols = BTreeSet::new();
        symbols.extend(self.universe.iter().cloned());
        symbols.extend(self.telemetry.keys().cloned());
        symbols.extend(subscriptions.keys().cloned());

        let mut unfiltered = Vec::new();
        let local_day_offset = local_day_offset_from_unix_millis(now_unix_millis);
        for symbol in symbols {
            let in_universe = self.universe.contains(&symbol);
            let telemetry = self.telemetry.get(&symbol);
            let subscriptions = subscriptions.get(&symbol).copied().unwrap_or_default();
            let subscribed = subscriptions.quote_subscriber_count > 0
                || subscriptions.chart_subscriber_count > 0;
            let receive_gap_ms = telemetry
                .and_then(|telemetry| telemetry.last_receive_unix_millis)
                .map(|last_receive| now_unix_millis.saturating_sub(last_receive));
            let market_time_lag_ms = telemetry
                .and_then(|telemetry| telemetry.last_tick_datetime_ns)
                .and_then(tick_datetime_ns_to_unix_millis)
                .map(|tick_millis| now_unix_millis.saturating_sub(tick_millis));
            let trading_phase = trading_phase_for_symbol(&symbol, telemetry, local_day_offset);
            let status = classify_symbol(
                in_universe,
                receive_gap_ms,
                stale_after_millis,
                trading_phase,
            );
            let telemetry = telemetry.cloned().unwrap_or_default();
            let problem_severity = problem_severity_for(status, telemetry.invalid_rows);
            unfiltered.push(SymbolTelemetrySnapshot {
                symbol,
                instrument_name: telemetry.instrument_name,
                status,
                problem: is_problem_severity(problem_severity),
                problem_severity,
                in_universe,
                subscribed,
                quote_subscriber_count: subscriptions.quote_subscriber_count,
                chart_subscriber_count: subscriptions.chart_subscriber_count,
                ticks_ingested: telemetry.ticks_ingested,
                receive_gap_ms,
                market_time_lag_ms,
                last_receive_unix_millis: telemetry.last_receive_unix_millis,
                last_tick_datetime_ns: telemetry.last_tick_datetime_ns,
                last_price: telemetry.last_price,
                last_volume: telemetry.last_volume,
                last_open_interest: telemetry.last_open_interest,
                invalid_rows: telemetry.invalid_rows,
                last_invalid_row_error: telemetry.last_invalid_row_error,
            });
        }

        let summary = summarize(&unfiltered);
        let needle = query.q.as_ref().map(|needle| needle.to_lowercase());
        let mut symbols = unfiltered
            .into_iter()
            .filter(|symbol| query.statuses.is_empty() || query.statuses.contains(&symbol.status))
            .filter(|symbol| !query.subscribed_only || symbol.subscribed)
            .filter(|symbol| {
                needle.as_ref().is_none_or(|needle| {
                    symbol.symbol.to_lowercase().contains(needle)
                        || symbol
                            .instrument_name
                            .as_deref()
                            .is_some_and(|name| name.to_lowercase().contains(needle))
                })
            })
            .collect::<Vec<_>>();
        sort_symbols(&mut symbols, query.sort);
        if let Some(limit) = query.limit {
            symbols.truncate(limit);
        }

        SymbolMetricsSnapshot {
            now_unix_millis,
            data_stale_after_millis: stale_after_millis,
            summary,
            symbols,
        }
    }
}

fn problem_severity_for(status: SymbolStatus, invalid_rows: u64) -> SymbolProblemSeverity {
    match status {
        SymbolStatus::Closed => SymbolProblemSeverity::Closed,
        SymbolStatus::Missing | SymbolStatus::Inactive => SymbolProblemSeverity::Bad,
        SymbolStatus::Stale => SymbolProblemSeverity::Warn,
        SymbolStatus::Live if invalid_rows > 0 => SymbolProblemSeverity::Bad,
        SymbolStatus::Live => SymbolProblemSeverity::Live,
    }
}

fn is_problem_severity(severity: SymbolProblemSeverity) -> bool {
    matches!(
        severity,
        SymbolProblemSeverity::Bad | SymbolProblemSeverity::Warn
    )
}

fn classify_symbol(
    in_universe: bool,
    receive_gap_ms: Option<u64>,
    stale_after_millis: u64,
    trading_phase: Option<TradingSessionPhase>,
) -> SymbolStatus {
    if matches!(trading_phase, Some(TradingSessionPhase::Closed)) {
        return SymbolStatus::Closed;
    }
    match receive_gap_ms {
        Some(gap) if gap <= stale_after_millis => SymbolStatus::Live,
        Some(_) => SymbolStatus::Stale,
        None if in_universe => SymbolStatus::Missing,
        None => SymbolStatus::Inactive,
    }
}

fn summarize(symbols: &[SymbolTelemetrySnapshot]) -> SymbolMetricsSummary {
    let mut receive_gaps = Vec::new();
    let mut summary = SymbolMetricsSummary {
        total: symbols.len(),
        live: 0,
        closed: 0,
        stale: 0,
        missing: 0,
        inactive: 0,
        subscribed: 0,
        p95_receive_gap_ms: None,
    };
    for symbol in symbols {
        match symbol.status {
            SymbolStatus::Live => summary.live += 1,
            SymbolStatus::Closed => summary.closed += 1,
            SymbolStatus::Stale => summary.stale += 1,
            SymbolStatus::Missing => summary.missing += 1,
            SymbolStatus::Inactive => summary.inactive += 1,
        }
        if symbol.subscribed {
            summary.subscribed += 1;
        }
        if let Some(gap) = symbol.receive_gap_ms {
            receive_gaps.push(gap);
        }
    }
    summary.p95_receive_gap_ms = percentile_95(receive_gaps);
    summary
}

fn sort_symbols(symbols: &mut [SymbolTelemetrySnapshot], sort: SymbolSort) {
    match sort {
        SymbolSort::SymbolAsc => symbols.sort_by(|left, right| left.symbol.cmp(&right.symbol)),
        SymbolSort::StatusAsc => {
            symbols.sort_by(|left, right| {
                (left.status, &left.symbol).cmp(&(right.status, &right.symbol))
            });
        }
        SymbolSort::ReceiveGapDesc => symbols.sort_by(|left, right| {
            right
                .receive_gap_ms
                .cmp(&left.receive_gap_ms)
                .then_with(|| left.symbol.cmp(&right.symbol))
        }),
        SymbolSort::MarketTimeLagDesc => symbols.sort_by(|left, right| {
            right
                .market_time_lag_ms
                .cmp(&left.market_time_lag_ms)
                .then_with(|| left.symbol.cmp(&right.symbol))
        }),
        SymbolSort::TicksIngestedDesc => symbols.sort_by(|left, right| {
            right
                .ticks_ingested
                .cmp(&left.ticks_ingested)
                .then_with(|| left.symbol.cmp(&right.symbol))
        }),
    }
}

fn percentile_95(mut values: Vec<u64>) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let index = ((values.len() - 1) * 95).div_ceil(100);
    values.get(index).copied()
}

fn tick_datetime_ns_to_unix_millis(datetime_ns: i64) -> Option<u64> {
    u64::try_from(datetime_ns)
        .ok()
        .map(|value| value / 1_000_000)
}

fn trading_phase_for_symbol(
    symbol: &str,
    telemetry: Option<&SymbolTelemetry>,
    local_day_offset: Duration,
) -> Option<TradingSessionPhase> {
    telemetry
        .and_then(|telemetry| {
            trading_phase_at(telemetry.trading_segments.as_deref(), local_day_offset)
        })
        .or_else(|| fallback_trading_phase_for_symbol(symbol, local_day_offset))
}

fn fallback_trading_phase_for_symbol(
    symbol: &str,
    local_day_offset: Duration,
) -> Option<TradingSessionPhase> {
    let segments = fallback_trading_segments_for_symbol(symbol)?;
    trading_phase_at(Some(&segments), local_day_offset)
}

fn fallback_trading_segments_for_symbol(symbol: &str) -> Option<Vec<TradingSessionSegment>> {
    let (exchange, instrument_id) = symbol.split_once('.')?;
    let exchange = exchange.to_ascii_uppercase();
    let product_id = futures_product_id(instrument_id)?;
    let product_id = product_id.to_ascii_lowercase();
    match exchange.as_str() {
        "CFFEX" => Some(cffex_trading_segments(&product_id)),
        "SHFE" => Some(shfe_trading_segments(&product_id)),
        "INE" => Some(ine_trading_segments(&product_id)),
        "DCE" | "CZCE" | "GFEX" => Some(commodity_trading_segments(Some("23:00:00"))),
        _ => None,
    }
}

fn futures_product_id(instrument_id: &str) -> Option<&str> {
    let end = instrument_id
        .char_indices()
        .find_map(|(index, ch)| ch.is_ascii_digit().then_some(index))
        .unwrap_or(instrument_id.len());
    (end > 0).then_some(&instrument_id[..end])
}

fn cffex_trading_segments(product_id: &str) -> Vec<TradingSessionSegment> {
    let afternoon_end = if matches!(product_id, "t" | "tf" | "tl" | "ts") {
        "15:15:00"
    } else {
        "15:00:00"
    };
    segments_from_windows(&[
        ("09:30:00", "11:30:00", false),
        ("13:00:00", afternoon_end, false),
    ])
}

fn shfe_trading_segments(product_id: &str) -> Vec<TradingSessionSegment> {
    let night_end = match product_id {
        "au" | "ag" => Some("02:30:00"),
        "al" | "ao" | "cu" | "ni" | "pb" | "sn" | "zn" => Some("01:00:00"),
        "wr" => None,
        _ => Some("23:00:00"),
    };
    commodity_trading_segments(night_end)
}

fn ine_trading_segments(product_id: &str) -> Vec<TradingSessionSegment> {
    let night_end = match product_id {
        "sc" => Some("02:30:00"),
        "bc" => Some("01:00:00"),
        _ => Some("23:00:00"),
    };
    commodity_trading_segments(night_end)
}

fn commodity_trading_segments(night_end: Option<&str>) -> Vec<TradingSessionSegment> {
    let mut windows = vec![
        ("09:00:00", "10:15:00", false),
        ("10:30:00", "11:30:00", false),
        ("13:30:00", "15:00:00", false),
    ];
    if let Some(night_end) = night_end {
        windows.push(("21:00:00", night_end, true));
    }
    segments_from_windows(&windows)
}

fn segments_from_windows(windows: &[(&str, &str, bool)]) -> Vec<TradingSessionSegment> {
    windows
        .iter()
        .map(|(start, end, allow_cross_midnight)| {
            parse_trading_segment(start, end, *allow_cross_midnight)
                .expect("built-in futures trading session must be valid")
        })
        .collect()
}

fn trading_segments_from_trading_time(
    trading_time: &TradingTime,
) -> Option<Vec<TradingSessionSegment>> {
    let segments = trading_time
        .day
        .iter()
        .filter_map(|window| parse_trading_time_window(window, false))
        .chain(
            trading_time
                .night
                .iter()
                .filter_map(|window| parse_trading_time_window(window, true)),
        )
        .collect::<Vec<_>>();
    (!segments.is_empty()).then_some(segments)
}

fn parse_trading_time_window(
    window: &[String],
    allow_cross_midnight: bool,
) -> Option<TradingSessionSegment> {
    parse_trading_segment(window.first()?, window.get(1)?, allow_cross_midnight)
}

fn parse_trading_segment(
    start: &str,
    end: &str,
    allow_cross_midnight: bool,
) -> Option<TradingSessionSegment> {
    let start = parse_hms_seconds(start)?;
    let end = parse_hms_seconds(end)?;
    if start == end {
        return None;
    }
    if !allow_cross_midnight && end <= start {
        return None;
    }
    TradingSessionSegment::new(Duration::from_secs(start), Duration::from_secs(end))
}

fn parse_hms_seconds(value: &str) -> Option<u64> {
    let time = NaiveTime::parse_from_str(value, "%H:%M:%S").ok()?;
    Some(u64::from(time.num_seconds_from_midnight()))
}

fn local_day_offset_from_unix_millis(now_unix_millis: u64) -> Duration {
    let Ok(now_unix_millis) = i64::try_from(now_unix_millis) else {
        return Duration::ZERO;
    };
    chrono::Local
        .timestamp_millis_opt(now_unix_millis)
        .single()
        .map(|datetime| Duration::from_secs(u64::from(datetime.num_seconds_from_midnight())))
        .unwrap_or(Duration::ZERO)
}

fn trading_phase_at(
    segments: Option<&[TradingSessionSegment]>,
    local_day_offset: Duration,
) -> Option<TradingSessionPhase> {
    let segments = segments.filter(|segments| !segments.is_empty())?;
    Some(
        TradingSessionSchedule::from_segments(segments.iter().copied())
            .status_at(local_day_offset)
            .phase,
    )
}

fn decode_query_component(value: &str) -> Result<String, &'static str> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            b'%' => {
                let high = bytes
                    .get(index + 1)
                    .copied()
                    .ok_or("invalid query encoding")?;
                let low = bytes
                    .get(index + 2)
                    .copied()
                    .ok_or("invalid query encoding")?;
                decoded.push(
                    hex_value(high)
                        .and_then(|high| hex_value(low).map(|low| (high << 4) | low))
                        .ok_or("invalid query encoding")?,
                );
                index += 3;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(decoded).map_err(|_| "invalid query encoding")
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn parse_statuses(value: &str) -> Result<Vec<SymbolStatus>, &'static str> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value.split(',').map(parse_status).collect()
}

fn parse_status(value: &str) -> Result<SymbolStatus, &'static str> {
    match value {
        "live" => Ok(SymbolStatus::Live),
        "closed" => Ok(SymbolStatus::Closed),
        "stale" => Ok(SymbolStatus::Stale),
        "missing" => Ok(SymbolStatus::Missing),
        "inactive" => Ok(SymbolStatus::Inactive),
        _ => Err("invalid status"),
    }
}

fn parse_bool(value: &str) -> Result<bool, &'static str> {
    match value {
        "1" | "true" => Ok(true),
        "0" | "false" => Ok(false),
        _ => Err("invalid subscribed"),
    }
}

fn parse_sort(value: &str) -> Result<SymbolSort, &'static str> {
    match value {
        "" | "symbol_asc" => Ok(SymbolSort::SymbolAsc),
        "status_asc" => Ok(SymbolSort::StatusAsc),
        "receive_gap_ms_desc" => Ok(SymbolSort::ReceiveGapDesc),
        "market_time_lag_ms_desc" => Ok(SymbolSort::MarketTimeLagDesc),
        "ticks_ingested_desc" => Ok(SymbolSort::TicksIngestedDesc),
        _ => Err("invalid sort"),
    }
}

fn parse_limit(value: &str) -> Result<usize, &'static str> {
    value
        .parse::<usize>()
        .ok()
        .filter(|limit| *limit > 0)
        .ok_or("invalid limit")
}
