#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use chrono::{Datelike, NaiveDate, Weekday};
use serde::Serialize;
use tqsdk_core::{
    Quote, TradingSessionPhase, TradingSessionSchedule, TradingSessionSegment, TradingTime,
};

use crate::protocol::RelayTickRow;
use crate::symbol_identity::{continuous_contract_display_name, continuous_contract_parts};

const RECEIVE_GAP_SAMPLE_LIMIT: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SymbolStatus {
    Live,
    Closed,
    Initializing,
    Stale,
    Missing,
    Inactive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SymbolProblemSeverity {
    Live,
    Closed,
    Initializing,
    Warn,
    Bad,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SymbolCoverage {
    Covered,
    Uncovered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SymbolSession {
    Open,
    Closed,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SymbolTradingPhase {
    Continuous,
    AuctionOrdering,
    AuctionBalance,
    AuctionMatch,
    PreClose,
    Closed,
    Unknown,
}

impl SymbolTradingPhase {
    #[must_use]
    pub fn is_auction(self) -> bool {
        matches!(
            self,
            Self::AuctionOrdering | Self::AuctionBalance | Self::AuctionMatch
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SymbolTradingPhaseSource {
    Schedule,
    TradingStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SymbolFlow {
    Flowing,
    Silent,
    NoSample,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SymbolIntegrity {
    Intact,
    Suspected,
    ConfirmedGap,
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
    pub sessions: Vec<SymbolSession>,
    pub subscribed_only: bool,
    pub q: Option<String>,
    pub sort: SymbolSort,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SymbolMetricsContext {
    pub initializing_universe: bool,
    pub initializing_pending_samples: bool,
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
                "session" => parsed.sessions = parse_sessions(&value)?,
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
    trading_phase: Option<SymbolTradingPhase>,
    trading_phase_source: Option<SymbolTradingPhaseSource>,
    raw_trade_status: Option<String>,
    ticks_ingested: u64,
    source_epoch: u64,
    last_tick_id: Option<i64>,
    gap_event_count: u64,
    estimated_missing_rows: u64,
    duplicate_rows: u64,
    out_of_order_rows: u64,
    last_gap_unix_millis: Option<u64>,
    last_receive_unix_millis: Option<u64>,
    last_tick_receive_unix_millis: Option<u64>,
    last_tick_datetime_ns: Option<i64>,
    receive_gap_samples_ms: VecDeque<u64>,

    invalid_rows: u64,
    last_invalid_row_error: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct SymbolTelemetryStore {
    universe: BTreeSet<String>,
    pending_initial_samples: BTreeSet<String>,
    telemetry: BTreeMap<String, SymbolTelemetry>,
    source_epoch: u64,
    last_universe_refresh_unix_millis: Option<u64>,
    trading_calendar_days: BTreeSet<NaiveDate>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SymbolTelemetryReadModel {
    universe: BTreeSet<String>,
    pending_initial_samples: BTreeSet<String>,
    telemetry: BTreeMap<String, SymbolTelemetry>,
    trading_calendar_days: BTreeSet<NaiveDate>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SymbolMetricsSnapshot {
    pub now_unix_millis: u64,
    pub data_stale_after_millis: u64,
    pub summary: SymbolMetricsSummary,
    pub filtered_total: usize,
    pub symbols: Vec<SymbolTelemetrySnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SymbolMetricsSummary {
    pub total: usize,
    pub live: usize,
    pub closed: usize,
    pub initializing: usize,
    pub stale: usize,
    pub missing: usize,
    pub inactive: usize,
    pub subscribed: usize,
    pub problem: usize,
    pub subscribed_problem: usize,
    pub universe_total: usize,
    pub universe_observed: usize,
    pub active_invalid_rows: u64,
    /// Diff row-id diagnostics only. TQ diff may patch or refill sparse rows later,
    /// so these counters are not confirmed market-data integrity failures.
    pub gap_event_count: u64,
    pub estimated_missing_rows: u64,
    pub duplicate_rows: u64,
    pub out_of_order_rows: u64,
    pub p95_receive_gap_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SymbolTelemetrySnapshot {
    pub symbol: String,
    pub instrument_name: Option<String>,
    pub status: SymbolStatus,
    pub coverage: SymbolCoverage,
    pub session: SymbolSession,
    pub phase: SymbolTradingPhase,
    pub phase_source: Option<SymbolTradingPhaseSource>,
    pub raw_trade_status: Option<String>,
    pub flow: SymbolFlow,
    pub integrity: SymbolIntegrity,
    pub problem: bool,
    pub problem_severity: SymbolProblemSeverity,
    pub in_universe: bool,
    pub subscribed: bool,
    pub quote_subscriber_count: usize,
    pub chart_subscriber_count: usize,
    pub ticks_ingested: u64,
    pub source_epoch: u64,
    /// Raw TQ diff row-id diagnostics. Skips, repeats, or older row ids can appear
    /// during sparse diff patches and are not treated as confirmed tick loss.
    pub last_tick_id: Option<i64>,
    pub gap_event_count: u64,
    pub estimated_missing_rows: u64,
    pub duplicate_rows: u64,
    pub out_of_order_rows: u64,
    pub last_gap_unix_millis: Option<u64>,
    pub receive_gap_ms: Option<u64>,
    pub avg_receive_gap_ms: Option<u64>,
    pub market_time_lag_ms: Option<u64>,
    pub last_receive_unix_millis: Option<u64>,
    pub last_tick_datetime_ns: Option<i64>,

    pub invalid_rows: u64,
    pub last_invalid_row_error: Option<String>,
}

impl SymbolTelemetryStore {
    pub fn record_universe<I, S>(&mut self, symbols: I, unix_millis: u64)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let universe: BTreeSet<String> = symbols
            .into_iter()
            .map(|symbol| symbol.as_ref().trim().to_string())
            .filter(|symbol| !symbol.is_empty())
            .collect();
        self.pending_initial_samples
            .retain(|symbol| universe.contains(symbol));
        for symbol in &universe {
            if self
                .telemetry
                .get(symbol)
                .and_then(|telemetry| telemetry.last_receive_unix_millis)
                .is_some()
            {
                self.pending_initial_samples.remove(symbol);
            } else {
                self.pending_initial_samples.insert(symbol.clone());
            }
        }
        self.universe = universe;
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

    pub fn record_symbol_instrument_name(&mut self, symbol: &str, instrument_name: &str) {
        let instrument_name = instrument_name.trim();
        if instrument_name.is_empty() {
            return;
        }
        self.telemetry
            .entry(symbol.to_string())
            .or_default()
            .instrument_name = Some(instrument_name.to_string());
    }

    pub fn advance_source_epoch(&mut self) {
        self.source_epoch = self.source_epoch.saturating_add(1);
        for telemetry in self.telemetry.values_mut() {
            telemetry.source_epoch = self.source_epoch;
            telemetry.last_tick_id = None;
        }
    }

    pub fn record_tick_at(&mut self, symbol: &str, row: &RelayTickRow, receive_unix_millis: u64) {
        self.pending_initial_samples.remove(symbol);
        let telemetry = self.telemetry.entry(symbol.to_string()).or_default();
        if telemetry.source_epoch != self.source_epoch {
            telemetry.source_epoch = self.source_epoch;
            telemetry.last_tick_id = None;
        }
        if let Some(last_receive) = telemetry.last_tick_receive_unix_millis {
            record_receive_gap_sample(telemetry, receive_unix_millis.saturating_sub(last_receive));
        }
        record_tick_continuity(telemetry, row.id, receive_unix_millis);
        telemetry.ticks_ingested = telemetry.ticks_ingested.saturating_add(1);
        telemetry.last_receive_unix_millis = Some(receive_unix_millis);
        telemetry.last_tick_receive_unix_millis = Some(receive_unix_millis);
        telemetry.last_tick_datetime_ns = Some(row.datetime);
    }

    pub fn record_quote_at(&mut self, symbol: &str, quote: &Quote, receive_unix_millis: u64) {
        self.pending_initial_samples.remove(symbol);
        let telemetry = self.telemetry.entry(symbol.to_string()).or_default();
        if telemetry.source_epoch != self.source_epoch {
            telemetry.source_epoch = self.source_epoch;
            telemetry.last_tick_id = None;
        }
        let instrument_name = quote.instrument_name.trim();
        if !instrument_name.is_empty() {
            telemetry.instrument_name = Some(instrument_name.to_string());
        }
        if let Some(trading_segments) = trading_segments_from_trading_time(&quote.trading_time) {
            telemetry.trading_segments = Some(trading_segments);
        }
        telemetry.last_receive_unix_millis = Some(receive_unix_millis);
        telemetry.last_tick_datetime_ns = quote.datetime.parse::<i64>().ok();
    }

    pub fn record_trading_status_at(
        &mut self,
        symbol: &str,
        trade_status: &str,
        _receive_unix_millis: u64,
    ) {
        let trade_status = trade_status.trim();
        if trade_status.is_empty() {
            return;
        }
        self.pending_initial_samples.remove(symbol);
        let telemetry = self.telemetry.entry(symbol.to_string()).or_default();
        telemetry.trading_phase = Some(phase_from_trade_status(trade_status));
        telemetry.trading_phase_source = Some(SymbolTradingPhaseSource::TradingStatus);
        telemetry.raw_trade_status = Some(trade_status.to_string());
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

    #[must_use]
    pub fn read_model(&self) -> SymbolTelemetryReadModel {
        SymbolTelemetryReadModel {
            universe: self.universe.clone(),
            pending_initial_samples: self.pending_initial_samples.clone(),
            telemetry: self.telemetry.clone(),
            trading_calendar_days: self.trading_calendar_days.clone(),
        }
    }

    pub fn record_trading_calendar(&mut self, calendar: &[tqsdk_core::TradingCalendarDay]) {
        for day in calendar {
            if day.trading {
                self.trading_calendar_days.insert(day.date);
            } else {
                self.trading_calendar_days.remove(&day.date);
            }
        }
    }

    pub fn snapshot_at(
        &self,
        now_unix_millis: u64,
        stale_after_millis: u64,
        subscriptions: &BTreeMap<String, SymbolSubscriptionCounts>,
        query: &SymbolMetricsQuery,
    ) -> SymbolMetricsSnapshot {
        self.read_model()
            .snapshot_at(now_unix_millis, stale_after_millis, subscriptions, query)
    }

    pub fn snapshot_at_with_context(
        &self,
        now_unix_millis: u64,
        stale_after_millis: u64,
        subscriptions: &BTreeMap<String, SymbolSubscriptionCounts>,
        query: &SymbolMetricsQuery,
        context: SymbolMetricsContext,
    ) -> SymbolMetricsSnapshot {
        self.read_model().snapshot_at_with_context(
            now_unix_millis,
            stale_after_millis,
            subscriptions,
            query,
            context,
        )
    }
}

impl SymbolTelemetryReadModel {
    pub fn snapshot_at(
        &self,
        now_unix_millis: u64,
        stale_after_millis: u64,
        subscriptions: &BTreeMap<String, SymbolSubscriptionCounts>,
        query: &SymbolMetricsQuery,
    ) -> SymbolMetricsSnapshot {
        self.snapshot_at_with_context(
            now_unix_millis,
            stale_after_millis,
            subscriptions,
            query,
            SymbolMetricsContext::default(),
        )
    }

    pub fn snapshot_at_with_context(
        &self,
        now_unix_millis: u64,
        stale_after_millis: u64,
        subscriptions: &BTreeMap<String, SymbolSubscriptionCounts>,
        query: &SymbolMetricsQuery,
        context: SymbolMetricsContext,
    ) -> SymbolMetricsSnapshot {
        let mut symbols = BTreeSet::new();
        symbols.extend(self.universe.iter().cloned());
        symbols.extend(subscriptions.keys().cloned());

        let mut unfiltered = Vec::new();
        let local_day_offset = local_day_offset_from_unix_millis(now_unix_millis);
        for symbol in symbols {
            let in_universe = self.universe.contains(&symbol);
            let telemetry = self.telemetry.get(&symbol);
            let subscriptions = subscriptions.get(&symbol).copied().unwrap_or_default();
            let subscribed = subscriptions.quote_subscriber_count > 0
                || subscriptions.chart_subscriber_count > 0;
            let raw_receive_gap_ms = telemetry
                .and_then(|telemetry| telemetry.last_receive_unix_millis)
                .map(|last_receive| now_unix_millis.saturating_sub(last_receive));
            let pending_initial_sample = self.pending_initial_samples.contains(&symbol);
            let raw_market_time_lag_ms = telemetry
                .and_then(|telemetry| telemetry.last_tick_datetime_ns)
                .and_then(tick_datetime_ns_to_unix_millis)
                .and_then(|tick_millis| now_unix_millis.checked_sub(tick_millis));
            let schedule_phase = (in_universe || raw_receive_gap_ms.is_some())
                .then(|| {
                    trading_phase_for_symbol(
                        &symbol,
                        telemetry,
                        local_day_offset,
                        now_unix_millis,
                        &self.trading_calendar_days,
                    )
                })
                .flatten()
                .and_then(|phase| {
                    (raw_receive_gap_ms.is_some() || matches!(phase, TradingSessionPhase::Closed))
                        .then_some(phase)
                });
            let phase = symbol_trading_phase_for(telemetry, schedule_phase);
            let phase_source = symbol_trading_phase_source_for(telemetry, schedule_phase);
            let status = classify_symbol(
                in_universe,
                raw_receive_gap_ms,
                stale_after_millis,
                phase,
                pending_initial_sample,
                context,
            );
            let telemetry = telemetry.cloned().unwrap_or_default();
            let coverage = coverage_for(in_universe);
            let session = session_for(phase);
            let suppress_continuity = status == SymbolStatus::Closed || phase.is_auction();
            let receive_gap_ms = (!suppress_continuity)
                .then_some(raw_receive_gap_ms)
                .flatten();
            let avg_receive_gap_ms = (!suppress_continuity)
                .then(|| average_receive_gap_ms(&telemetry.receive_gap_samples_ms))
                .flatten();
            let market_time_lag_ms = (!suppress_continuity)
                .then_some(raw_market_time_lag_ms)
                .flatten();
            let flow = if suppress_continuity {
                SymbolFlow::NoSample
            } else {
                flow_for(raw_receive_gap_ms, stale_after_millis)
            };
            let integrity = integrity_for(status);
            let problem_severity = problem_severity_for(status, coverage, telemetry.invalid_rows);
            let instrument_name = telemetry
                .instrument_name
                .clone()
                .or_else(|| continuous_contract_display_name(&symbol));
            unfiltered.push(SymbolTelemetrySnapshot {
                symbol,
                instrument_name,
                status,
                coverage,
                session,
                phase,
                phase_source,
                raw_trade_status: telemetry.raw_trade_status,
                flow,
                integrity,
                problem: is_problem_severity(problem_severity),
                problem_severity,
                in_universe,
                subscribed,
                quote_subscriber_count: subscriptions.quote_subscriber_count,
                chart_subscriber_count: subscriptions.chart_subscriber_count,
                ticks_ingested: telemetry.ticks_ingested,
                source_epoch: telemetry.source_epoch,
                last_tick_id: telemetry.last_tick_id,
                gap_event_count: telemetry.gap_event_count,
                estimated_missing_rows: telemetry.estimated_missing_rows,
                duplicate_rows: telemetry.duplicate_rows,
                out_of_order_rows: telemetry.out_of_order_rows,
                last_gap_unix_millis: telemetry.last_gap_unix_millis,
                receive_gap_ms,
                avg_receive_gap_ms,
                market_time_lag_ms,
                last_receive_unix_millis: telemetry.last_receive_unix_millis,
                last_tick_datetime_ns: telemetry.last_tick_datetime_ns,

                invalid_rows: telemetry.invalid_rows,
                last_invalid_row_error: telemetry.last_invalid_row_error,
            });
        }

        let summary = summarize(&unfiltered);
        let needle = query.q.as_ref().map(|needle| needle.to_lowercase());
        let mut symbols = unfiltered
            .into_iter()
            .filter(|symbol| query.statuses.is_empty() || query.statuses.contains(&symbol.status))
            .filter(|symbol| query.sessions.is_empty() || query.sessions.contains(&symbol.session))
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
        let filtered_total = symbols.len();
        if let Some(limit) = query.limit {
            symbols.truncate(limit);
        }

        SymbolMetricsSnapshot {
            now_unix_millis,
            data_stale_after_millis: stale_after_millis,
            summary,
            filtered_total,
            symbols,
        }
    }
}

fn record_tick_continuity(telemetry: &mut SymbolTelemetry, tick_id: i64, receive_unix_millis: u64) {
    match telemetry.last_tick_id {
        None => {
            telemetry.last_tick_id = Some(tick_id);
        }
        Some(last_tick_id) if tick_id == last_tick_id => {
            telemetry.duplicate_rows = telemetry.duplicate_rows.saturating_add(1);
        }
        Some(last_tick_id) if tick_id < last_tick_id => {
            // 忽略历史行更新或 diff null 删除。
            // 协议层的 data dict 是稀疏的，旧 ID 的更新或删除是正常现象，
            // 并非 head 倒序或流重置。由于 last_id 永远单调递增，
            // 收到小于 last_tick_id 的 row id 不应触发乱序告警或重置游标。
        }
        Some(last_tick_id) => {
            if tick_id > last_tick_id.saturating_add(1) {
                let missing_rows =
                    u64::try_from(tick_id.saturating_sub(last_tick_id).saturating_sub(1))
                        .unwrap_or(u64::MAX);
                telemetry.gap_event_count = telemetry.gap_event_count.saturating_add(1);
                telemetry.estimated_missing_rows = telemetry
                    .estimated_missing_rows
                    .saturating_add(missing_rows);
                telemetry.last_gap_unix_millis = Some(receive_unix_millis);
            }
            telemetry.last_tick_id = Some(tick_id);
        }
    }
}

fn record_receive_gap_sample(telemetry: &mut SymbolTelemetry, gap_ms: u64) {
    if telemetry.receive_gap_samples_ms.len() >= RECEIVE_GAP_SAMPLE_LIMIT {
        telemetry.receive_gap_samples_ms.pop_front();
    }
    telemetry.receive_gap_samples_ms.push_back(gap_ms);
}

fn average_receive_gap_ms(samples: &VecDeque<u64>) -> Option<u64> {
    if samples.is_empty() {
        return None;
    }
    let total = samples.iter().fold(0_u128, |total, sample| {
        total.saturating_add(u128::from(*sample))
    });
    Some(
        (total / samples.len() as u128)
            .try_into()
            .unwrap_or(u64::MAX),
    )
}

fn problem_severity_for(
    status: SymbolStatus,
    coverage: SymbolCoverage,
    invalid_rows: u64,
) -> SymbolProblemSeverity {
    if coverage == SymbolCoverage::Uncovered {
        return SymbolProblemSeverity::Bad;
    }
    match status {
        SymbolStatus::Closed => SymbolProblemSeverity::Closed,
        SymbolStatus::Initializing => SymbolProblemSeverity::Initializing,
        SymbolStatus::Missing | SymbolStatus::Inactive => SymbolProblemSeverity::Bad,
        SymbolStatus::Stale => SymbolProblemSeverity::Warn,
        SymbolStatus::Live if invalid_rows > 0 => SymbolProblemSeverity::Bad,
        SymbolStatus::Live => SymbolProblemSeverity::Live,
    }
}

fn coverage_for(in_universe: bool) -> SymbolCoverage {
    if in_universe {
        SymbolCoverage::Covered
    } else {
        SymbolCoverage::Uncovered
    }
}

fn session_for(phase: SymbolTradingPhase) -> SymbolSession {
    match phase {
        SymbolTradingPhase::Continuous
        | SymbolTradingPhase::AuctionOrdering
        | SymbolTradingPhase::AuctionBalance
        | SymbolTradingPhase::AuctionMatch
        | SymbolTradingPhase::PreClose => SymbolSession::Open,
        SymbolTradingPhase::Closed => SymbolSession::Closed,
        SymbolTradingPhase::Unknown => SymbolSession::Unknown,
    }
}

fn flow_for(receive_gap_ms: Option<u64>, stale_after_millis: u64) -> SymbolFlow {
    match receive_gap_ms {
        Some(gap) if gap <= stale_after_millis => SymbolFlow::Flowing,
        Some(_) => SymbolFlow::Silent,
        None => SymbolFlow::NoSample,
    }
}

fn integrity_for(status: SymbolStatus) -> SymbolIntegrity {
    if matches!(status, SymbolStatus::Stale | SymbolStatus::Missing) {
        return SymbolIntegrity::Suspected;
    }
    SymbolIntegrity::Intact
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
    phase: SymbolTradingPhase,
    pending_initial_sample: bool,
    context: SymbolMetricsContext,
) -> SymbolStatus {
    if !in_universe && receive_gap_ms.is_none() {
        return SymbolStatus::Inactive;
    }
    if phase == SymbolTradingPhase::Closed {
        return SymbolStatus::Closed;
    }
    if phase.is_auction() {
        return SymbolStatus::Live;
    }
    if in_universe
        && receive_gap_ms.is_none()
        && (context.initializing_universe
            || (context.initializing_pending_samples && pending_initial_sample))
    {
        return SymbolStatus::Initializing;
    }
    if receive_gap_ms.is_none() {
        return SymbolStatus::Missing;
    }
    match receive_gap_ms {
        Some(gap) if gap <= stale_after_millis => SymbolStatus::Live,
        Some(_) => SymbolStatus::Stale,
        None if in_universe => SymbolStatus::Missing,
        None => SymbolStatus::Inactive,
    }
}

fn symbol_trading_phase_for(
    telemetry: Option<&SymbolTelemetry>,
    schedule_phase: Option<TradingSessionPhase>,
) -> SymbolTradingPhase {
    telemetry
        .and_then(|telemetry| telemetry.trading_phase)
        .unwrap_or_else(|| phase_from_schedule(schedule_phase))
}

fn symbol_trading_phase_source_for(
    telemetry: Option<&SymbolTelemetry>,
    schedule_phase: Option<TradingSessionPhase>,
) -> Option<SymbolTradingPhaseSource> {
    telemetry
        .and_then(|telemetry| telemetry.trading_phase_source)
        .or_else(|| schedule_phase.map(|_| SymbolTradingPhaseSource::Schedule))
}

fn phase_from_schedule(phase: Option<TradingSessionPhase>) -> SymbolTradingPhase {
    match phase {
        Some(TradingSessionPhase::Open) => SymbolTradingPhase::Continuous,
        Some(TradingSessionPhase::PreClose) => SymbolTradingPhase::PreClose,
        Some(TradingSessionPhase::Closed) => SymbolTradingPhase::Closed,
        None => SymbolTradingPhase::Unknown,
    }
}

fn phase_from_trade_status(trade_status: &str) -> SymbolTradingPhase {
    let normalized = trade_status
        .trim()
        .chars()
        .filter(|ch| !matches!(ch, '_' | '-' | ' '))
        .collect::<String>()
        .to_ascii_uppercase();
    match normalized.as_str() {
        "AUCTIONORDERING" => SymbolTradingPhase::AuctionOrdering,
        "AUCTIONBALANCE" => SymbolTradingPhase::AuctionBalance,
        "AUCTIONMATCH" => SymbolTradingPhase::AuctionMatch,
        "CONTINOUS" | "CONTINUOUS" => SymbolTradingPhase::Continuous,
        "PRECLOSE" => SymbolTradingPhase::PreClose,
        "CLOSED" | "NOTTRADE" | "NOTTRADING" | "BEFORETRADING" => SymbolTradingPhase::Closed,
        _ => SymbolTradingPhase::Unknown,
    }
}

fn summarize(symbols: &[SymbolTelemetrySnapshot]) -> SymbolMetricsSummary {
    let mut receive_gaps = Vec::new();
    let mut summary = SymbolMetricsSummary {
        total: symbols.len(),
        live: 0,
        closed: 0,
        stale: 0,
        initializing: 0,
        missing: 0,
        inactive: 0,
        subscribed: 0,
        problem: 0,
        subscribed_problem: 0,
        universe_total: 0,
        universe_observed: 0,
        active_invalid_rows: 0,
        gap_event_count: 0,
        estimated_missing_rows: 0,
        duplicate_rows: 0,
        out_of_order_rows: 0,
        p95_receive_gap_ms: None,
    };
    for symbol in symbols {
        match symbol.status {
            SymbolStatus::Live => summary.live += 1,
            SymbolStatus::Closed => summary.closed += 1,
            SymbolStatus::Initializing => summary.initializing += 1,
            SymbolStatus::Stale => summary.stale += 1,
            SymbolStatus::Missing => summary.missing += 1,
            SymbolStatus::Inactive => summary.inactive += 1,
        }
        if symbol.subscribed {
            summary.subscribed += 1;
        }
        if symbol.problem {
            summary.problem += 1;
            summary.active_invalid_rows = summary
                .active_invalid_rows
                .saturating_add(symbol.invalid_rows);
            if symbol.subscribed {
                summary.subscribed_problem += 1;
            }
        }
        if symbol.in_universe {
            summary.universe_total += 1;
            if symbol.last_receive_unix_millis.is_some() {
                summary.universe_observed += 1;
            }
        }
        summary.gap_event_count = summary
            .gap_event_count
            .saturating_add(symbol.gap_event_count);
        summary.estimated_missing_rows = summary
            .estimated_missing_rows
            .saturating_add(symbol.estimated_missing_rows);
        summary.duplicate_rows = summary.duplicate_rows.saturating_add(symbol.duplicate_rows);
        summary.out_of_order_rows = summary
            .out_of_order_rows
            .saturating_add(symbol.out_of_order_rows);
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
    now_unix_millis: u64,
    trading_calendar_days: &BTreeSet<NaiveDate>,
) -> Option<TradingSessionPhase> {
    let segments = telemetry
        .and_then(|telemetry| telemetry.trading_segments.clone())
        .or_else(|| fallback_trading_segments_for_symbol(symbol))?;
    let phase = trading_phase_at(Some(&segments), local_day_offset)?;
    if matches!(
        phase,
        TradingSessionPhase::Open | TradingSessionPhase::PreClose
    ) && !trading_calendar_days.is_empty()
        && !trading_calendar_allows_open(
            &segments,
            local_day_offset,
            now_unix_millis,
            trading_calendar_days,
        )
    {
        return Some(TradingSessionPhase::Closed);
    }
    Some(phase)
}

fn night_session_has_next_trading_day(
    local_date: chrono::NaiveDate,
    trading_calendar_days: &BTreeSet<NaiveDate>,
) -> bool {
    for day_offset in 1..=5 {
        let candidate = local_date + chrono::Duration::days(day_offset);
        if !trading_calendar_days.contains(&candidate) {
            continue;
        }
        return day_offset == 1
            || (day_offset == 3
                && local_date.weekday() == Weekday::Fri
                && candidate.weekday() == Weekday::Mon);
    }
    false
}

fn trading_calendar_allows_open(
    segments: &[TradingSessionSegment],
    local_day_offset: Duration,
    now_unix_millis: u64,
    trading_calendar_days: &BTreeSet<NaiveDate>,
) -> bool {
    let local_date = china_date_from_unix_millis(now_unix_millis);
    if !trading_calendar_contains(local_date, trading_calendar_days) {
        return false;
    }
    let Some(open_segment) = segments
        .iter()
        .find(|segment| segment_contains(**segment, local_day_offset))
    else {
        return true;
    };
    if !segment_wraps_midnight(*open_segment) {
        return true;
    }
    if local_day_offset < open_segment.end() {
        let previous_date = local_date - chrono::Duration::days(1);
        return trading_calendar_contains(previous_date, trading_calendar_days);
    }
    night_session_has_next_trading_day(local_date, trading_calendar_days)
}

fn trading_calendar_contains(
    local_date: chrono::NaiveDate,
    trading_calendar_days: &BTreeSet<NaiveDate>,
) -> bool {
    trading_calendar_days.contains(&local_date)
}

fn china_date_from_unix_millis(now_unix_millis: u64) -> chrono::NaiveDate {
    let china_millis = now_unix_millis.saturating_add(8 * 60 * 60 * 1_000);
    let days_since_epoch = (china_millis / (24 * 60 * 60 * 1_000)) as i64;
    chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + chrono::Duration::days(days_since_epoch)
}

fn segment_contains(segment: TradingSessionSegment, local_day_offset: Duration) -> bool {
    if segment_wraps_midnight(segment) {
        local_day_offset >= segment.start() || local_day_offset < segment.end()
    } else {
        segment.start() <= local_day_offset && local_day_offset < segment.end()
    }
}

fn segment_wraps_midnight(segment: TradingSessionSegment) -> bool {
    segment.end() <= segment.start()
}

fn fallback_trading_segments_for_symbol(symbol: &str) -> Option<Vec<TradingSessionSegment>> {
    let (exchange, product_id) = if let Some(parts) = continuous_contract_parts(symbol) {
        (parts.exchange_id, parts.product_id)
    } else {
        let (exchange, instrument_id) = symbol.split_once('.')?;
        (exchange, futures_product_id(instrument_id)?)
    };
    let exchange = exchange.to_ascii_uppercase();
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
    const TRADING_DAY_SECONDS: u64 = 24 * 60 * 60;

    let start = parse_hms_seconds(start)?;
    let mut end = parse_hms_seconds(end)?;
    if start >= TRADING_DAY_SECONDS {
        return None;
    }
    if end > TRADING_DAY_SECONDS {
        if !allow_cross_midnight || end >= TRADING_DAY_SECONDS.saturating_mul(2) {
            return None;
        }
        end -= TRADING_DAY_SECONDS;
    }
    if start == end {
        return None;
    }
    if !allow_cross_midnight && end <= start {
        return None;
    }
    TradingSessionSegment::new(Duration::from_secs(start), Duration::from_secs(end))
}

fn parse_hms_seconds(value: &str) -> Option<u64> {
    let mut parts = value.split(':');
    let hour = parts.next()?.parse::<u64>().ok()?;
    let minute = parts.next()?.parse::<u64>().ok()?;
    let second = parts.next()?.parse::<u64>().ok()?;
    if parts.next().is_some() || minute >= 60 || second >= 60 {
        return None;
    }
    Some(hour * 3600 + minute * 60 + second)
}

fn local_day_offset_from_unix_millis(now_unix_millis: u64) -> Duration {
    const CHINA_OFFSET_MILLIS: u64 = 8 * 60 * 60 * 1_000;
    const DAY_MILLIS: u64 = 24 * 60 * 60 * 1_000;
    Duration::from_millis((now_unix_millis.saturating_add(CHINA_OFFSET_MILLIS)) % DAY_MILLIS)
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

fn parse_sessions(value: &str) -> Result<Vec<SymbolSession>, &'static str> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value.split(',').map(parse_session).collect()
}

fn parse_session(value: &str) -> Result<SymbolSession, &'static str> {
    match value {
        "open" => Ok(SymbolSession::Open),
        "closed" => Ok(SymbolSession::Closed),
        "unknown" => Ok(SymbolSession::Unknown),
        _ => Err("invalid session"),
    }
}

fn parse_status(value: &str) -> Result<SymbolStatus, &'static str> {
    match value {
        "live" => Ok(SymbolStatus::Live),
        "closed" => Ok(SymbolStatus::Closed),
        "initializing" => Ok(SymbolStatus::Initializing),
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
