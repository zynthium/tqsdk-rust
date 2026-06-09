#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use tqsdk_core::Quote;

use crate::protocol::RelayTickRow;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SymbolStatus {
    Live,
    Stale,
    Missing,
    Inactive,
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
            match key {
                "status" => parsed.statuses = parse_statuses(value)?,
                "subscribed" => parsed.subscribed_only = parse_bool(value)?,
                "q" => {
                    if !value.is_empty() {
                        parsed.q = Some(value.to_string());
                    }
                }
                "sort" => parsed.sort = parse_sort(value)?,
                "limit" => parsed.limit = Some(parse_limit(value)?),
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
    pub stale: usize,
    pub missing: usize,
    pub inactive: usize,
    pub subscribed: usize,
    pub p95_receive_gap_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SymbolTelemetrySnapshot {
    pub symbol: String,
    pub status: SymbolStatus,
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
        telemetry.last_receive_unix_millis = Some(receive_unix_millis);
        telemetry.last_tick_datetime_ns = quote.datetime.parse::<i64>().ok();
        telemetry.last_price = Some(quote.last_price);
        telemetry.last_volume = Some(quote.volume);
        telemetry.last_open_interest = Some(quote.open_interest);
    }

    pub fn record_invalid_row(&mut self, symbol: &str, message: impl Into<String>) {
        let telemetry = self.telemetry.entry(symbol.to_string()).or_default();
        telemetry.invalid_rows = telemetry.invalid_rows.saturating_add(1);
        telemetry.last_invalid_row_error = Some(message.into());
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
            let status = classify_symbol(in_universe, receive_gap_ms, stale_after_millis);
            let telemetry = telemetry.cloned().unwrap_or_default();
            unfiltered.push(SymbolTelemetrySnapshot {
                symbol,
                status,
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
        let mut symbols = unfiltered
            .into_iter()
            .filter(|symbol| query.statuses.is_empty() || query.statuses.contains(&symbol.status))
            .filter(|symbol| !query.subscribed_only || symbol.subscribed)
            .filter(|symbol| {
                query
                    .q
                    .as_ref()
                    .is_none_or(|needle| symbol.symbol.contains(needle))
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

fn classify_symbol(
    in_universe: bool,
    receive_gap_ms: Option<u64>,
    stale_after_millis: u64,
) -> SymbolStatus {
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
        stale: 0,
        missing: 0,
        inactive: 0,
        subscribed: 0,
        p95_receive_gap_ms: None,
    };
    for symbol in symbols {
        match symbol.status {
            SymbolStatus::Live => summary.live += 1,
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

fn parse_statuses(value: &str) -> Result<Vec<SymbolStatus>, &'static str> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value.split(',').map(parse_status).collect()
}

fn parse_status(value: &str) -> Result<SymbolStatus, &'static str> {
    match value {
        "live" => Ok(SymbolStatus::Live),
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
