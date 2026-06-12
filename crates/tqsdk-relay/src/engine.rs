#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::{Value, json};
use tqsdk_core::Quote;

use crate::bootstrap::{BootstrapQueue, BootstrapRequest};
use crate::cache::MarketCache;
use crate::error::RelayResult;
use crate::interest::{ClientId, InterestRegistry, SourceKey};
use crate::kline::KlineSynthesis;
use crate::observability::{
    DECODE_HEALTH_WINDOW_SECS, DEFAULT_DATA_STALE_AFTER_SECS, DecodeHealth,
    EVENT_IDLE_CRITICAL_AFTER_MS, EVENT_IDLE_WARN_AFTER_MS, FRAME_IDLE_CRITICAL_AFTER_MS,
    FRAME_IDLE_WARN_AFTER_MS, FlowIdleHealth, HealthSnapshot, MetricsSnapshot, RelaySourceStage,
    RelaySourceStatus,
};
use crate::protocol::{DownstreamCommand, RelayMarketFrame, RelayTickRow};
use crate::symbol_metrics::{
    SymbolFlow, SymbolMetricsQuery, SymbolMetricsSnapshot, SymbolMetricsSummary,
    SymbolProblemSeverity, SymbolSession, SymbolSubscriptionCounts, SymbolTelemetryReadModel,
    SymbolTelemetrySnapshot, SymbolTelemetryStore,
};
use crate::universe::FuturesContract;

const DEFAULT_RELAY_EVENT_LEDGER_LIMIT: usize = 128;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DashboardSnapshot {
    pub received_at_unix_millis: u64,
    pub metrics: MetricsSnapshot,
    pub global: SymbolMetricsSummary,
    pub timeline: DashboardTimelineSample,
    pub page: SymbolMetricsSnapshot,
    pub events: Vec<RelayEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DashboardTimelineSeverity {
    Live,
    Closed,
    Warn,
    Bad,
    Unknown,
    NoSample,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DashboardTimelineScope {
    pub severity: DashboardTimelineSeverity,
    pub total: usize,
    pub problem: usize,
    pub receive_gap_ms: Option<u64>,
    pub avg_receive_gap_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DashboardTimelineSample {
    pub global: DashboardTimelineScope,
    pub subscribed: DashboardTimelineScope,
    pub exchanges: BTreeMap<String, DashboardTimelineScope>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DashboardSnapshotInputs {
    pub received_at_unix_millis: u64,
    pub metrics: MetricsSnapshot,
    pub symbols: SymbolTelemetryReadModel,
    pub subscriptions: BTreeMap<String, SymbolSubscriptionCounts>,
    pub events: Vec<RelayEvent>,
}

impl DashboardSnapshotInputs {
    #[must_use]
    pub fn symbol_metrics_snapshot(&self, query: &SymbolMetricsQuery) -> SymbolMetricsSnapshot {
        self.symbols.snapshot_at(
            self.received_at_unix_millis,
            DEFAULT_DATA_STALE_AFTER_SECS.saturating_mul(1_000),
            &self.subscriptions,
            query,
        )
    }

    #[must_use]
    pub fn into_dashboard_snapshot(self, query: &SymbolMetricsQuery) -> DashboardSnapshot {
        let global_page = self.symbol_metrics_snapshot(&SymbolMetricsQuery::default());
        let timeline = dashboard_timeline(&global_page.symbols);
        let page = self.symbol_metrics_snapshot(query);
        DashboardSnapshot {
            received_at_unix_millis: self.received_at_unix_millis,
            metrics: self.metrics,
            global: global_page.summary,
            timeline,
            page,
            events: self.events,
        }
    }
}

fn exchange_of(symbol: &str) -> String {
    symbol
        .split_once('.')
        .map(|(exchange, _)| exchange.to_ascii_uppercase())
        .unwrap_or_else(|| "UNKNOWN".to_string())
}

fn dashboard_scope_for<'a>(
    rows: impl IntoIterator<Item = &'a SymbolTelemetrySnapshot>,
) -> DashboardTimelineScope {
    let mut total = 0;
    let mut problem = 0;
    let mut bad = false;
    let mut warn = false;
    let mut all_closed = true;
    let mut all_no_sample = true;
    let mut max_receive_gap_ms = None::<u64>;
    let mut avg_gap_total = 0_u128;
    let mut avg_gap_count = 0_u128;

    for row in rows {
        total += 1;
        if row.problem {
            problem += 1;
        }
        bad |= row.problem_severity == SymbolProblemSeverity::Bad;
        warn |= row.problem_severity == SymbolProblemSeverity::Warn;
        all_closed &= row.session == SymbolSession::Closed;
        all_no_sample &= row.flow == SymbolFlow::NoSample;
        if let Some(gap) = row.receive_gap_ms {
            max_receive_gap_ms = Some(max_receive_gap_ms.map_or(gap, |current| current.max(gap)));
        }
        if let Some(gap) = row.avg_receive_gap_ms {
            avg_gap_total = avg_gap_total.saturating_add(u128::from(gap));
            avg_gap_count = avg_gap_count.saturating_add(1);
        }
    }

    let severity = if total == 0 {
        DashboardTimelineSeverity::Unknown
    } else if all_closed {
        DashboardTimelineSeverity::Closed
    } else if bad {
        DashboardTimelineSeverity::Bad
    } else if warn {
        DashboardTimelineSeverity::Warn
    } else if all_no_sample {
        DashboardTimelineSeverity::NoSample
    } else {
        DashboardTimelineSeverity::Live
    };

    DashboardTimelineScope {
        severity,
        total,
        problem,
        receive_gap_ms: max_receive_gap_ms,
        avg_receive_gap_ms: (avg_gap_count > 0).then(|| {
            (avg_gap_total / avg_gap_count)
                .try_into()
                .unwrap_or(u64::MAX)
        }),
    }
}

fn dashboard_timeline(rows: &[SymbolTelemetrySnapshot]) -> DashboardTimelineSample {
    let mut exchanges = BTreeMap::new();
    for row in rows {
        exchanges
            .entry(exchange_of(&row.symbol))
            .or_insert_with(Vec::new)
            .push(row);
    }

    DashboardTimelineSample {
        global: dashboard_scope_for(rows.iter()),
        subscribed: dashboard_scope_for(rows.iter().filter(|row| row.subscribed)),
        exchanges: exchanges
            .into_iter()
            .map(|(exchange, rows)| (exchange, dashboard_scope_for(rows.into_iter())))
            .collect(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RelayEventKind {
    UniverseRefreshed,
    UniverseRefreshFailed,
    FlowIncident,
    DecodeIncident,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RelayEvent {
    pub sequence: u64,
    pub at_unix_secs: u64,
    pub kind: RelayEventKind,
    pub detail: String,
}

#[derive(Debug, Clone)]
struct RelayEventLedger {
    limit: usize,
    next_sequence: u64,
    events: VecDeque<RelayEvent>,
}

impl Default for RelayEventLedger {
    fn default() -> Self {
        Self {
            limit: DEFAULT_RELAY_EVENT_LEDGER_LIMIT,
            next_sequence: 1,
            events: VecDeque::with_capacity(DEFAULT_RELAY_EVENT_LEDGER_LIMIT),
        }
    }
}

impl RelayEventLedger {
    fn push(&mut self, at_unix_secs: u64, kind: RelayEventKind, detail: impl Into<String>) {
        if self.limit == 0 {
            return;
        }
        while self.events.len() >= self.limit {
            self.events.pop_front();
        }
        self.events.push_back(RelayEvent {
            sequence: self.next_sequence,
            at_unix_secs,
            kind,
            detail: detail.into(),
        });
        self.next_sequence = self.next_sequence.saturating_add(1);
    }

    fn snapshot(&self) -> Vec<RelayEvent> {
        self.events.iter().cloned().collect()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DownstreamFrame {
    pub client_id: ClientId,
    pub payload: Value,
}

#[derive(Debug)]
pub struct RelayEngine {
    cache: MarketCache,
    interests: InterestRegistry,
    bootstrap: BootstrapQueue,
    klines: HashMap<SourceKey, KlineSynthesis>,
    symbol_metrics: SymbolTelemetryStore,
    upstream_status: RelaySourceStatus,
    upstream_stage: RelaySourceStage,
    upstream_stage_started_unix_secs: Option<u64>,
    upstream_transport_connected: bool,
    upstream_subscription_sent: bool,
    upstream_frames_received: u64,
    upstream_events_decoded: u64,
    last_upstream_frame_unix_secs: Option<u64>,
    last_decoded_event_unix_secs: Option<u64>,
    ticks_ingested: u64,
    upstream_symbols: usize,
    upstream_ins_list_chars: usize,
    upstream_ins_list_warn_chars: Option<usize>,
    upstream_ins_list_max_chars: Option<usize>,
    upstream_ins_list_over_warn: bool,
    last_universe_refresh_unix_secs: Option<u64>,
    last_universe_refresh_error: Option<String>,
    last_tick_unix_secs: Option<u64>,
    upstream_invalid_tick_rows: u64,
    invalid_tick_row_events: VecDeque<(u64, u64)>,
    last_invalid_row_unix_secs: Option<u64>,
    last_upstream_invalid_tick_row_error: Option<String>,
    event_ledger: RelayEventLedger,
}

impl RelayEngine {
    #[must_use]
    pub fn new_memory_only(tick_capacity: usize, kline_capacity: usize) -> Self {
        Self {
            cache: MarketCache::new(tick_capacity, kline_capacity),
            interests: InterestRegistry::default(),
            bootstrap: BootstrapQueue::new(4, Duration::from_millis(250)),
            klines: HashMap::new(),
            symbol_metrics: SymbolTelemetryStore::default(),
            upstream_status: RelaySourceStatus::Connecting,
            upstream_stage: RelaySourceStage::Connecting,
            upstream_stage_started_unix_secs: None,
            upstream_transport_connected: false,
            upstream_subscription_sent: false,
            upstream_frames_received: 0,
            upstream_events_decoded: 0,
            last_upstream_frame_unix_secs: None,
            last_decoded_event_unix_secs: None,
            ticks_ingested: 0,
            upstream_symbols: 0,
            upstream_ins_list_chars: 0,
            upstream_ins_list_warn_chars: None,
            upstream_ins_list_max_chars: None,
            upstream_ins_list_over_warn: false,
            last_universe_refresh_unix_secs: None,
            last_universe_refresh_error: None,
            last_tick_unix_secs: None,
            upstream_invalid_tick_rows: 0,
            invalid_tick_row_events: VecDeque::new(),
            last_invalid_row_unix_secs: None,
            last_upstream_invalid_tick_row_error: None,
            event_ledger: RelayEventLedger::default(),
        }
    }

    pub fn handle_command(
        &mut self,
        client_id: ClientId,
        command: DownstreamCommand,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        match command {
            DownstreamCommand::SubscribeQuote { symbols } => {
                let frames = self.cached_quote_frames_for_client(client_id, &symbols);
                self.interests.set_quotes(client_id, symbols);
                Ok(frames)
            }
            DownstreamCommand::SetChart(command) => {
                let source = self.interests.set_chart(client_id, command);
                self.bootstrap.enqueue(BootstrapRequest {
                    source: source.clone(),
                    start_id: i64::MIN,
                    end_id: i64::MAX,
                });
                self.replay_cached_kline_frames(client_id, &source)
            }
            DownstreamCommand::PeekMessage => Ok(Vec::new()),
        }
    }

    pub fn ingest_tick(
        &mut self,
        symbol: impl AsRef<str>,
        row: RelayTickRow,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        self.ingest_tick_at(symbol, row, current_unix_millis())
    }

    pub fn ingest_tick_at_for_test(
        &mut self,
        symbol: impl AsRef<str>,
        row: RelayTickRow,
        receive_unix_millis: u64,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        self.ingest_tick_at(symbol, row, receive_unix_millis)
    }

    fn ingest_tick_at(
        &mut self,
        symbol: impl AsRef<str>,
        row: RelayTickRow,
        receive_unix_millis: u64,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        let symbol = symbol.as_ref();
        self.ticks_ingested = self.ticks_ingested.saturating_add(1);
        self.mark_upstream_live();
        self.record_data_activity_at(receive_unix_millis / 1_000);
        self.symbol_metrics
            .record_tick_at(symbol, &row, receive_unix_millis);
        self.cache.push_tick(symbol, row.clone());
        let mut frames = self.quote_frames(symbol);
        frames.extend(self.kline_frames(symbol, row)?);
        Ok(frames)
    }

    pub fn ingest_quote(
        &mut self,
        symbol: impl AsRef<str>,
        quote: Quote,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        self.ingest_quote_at(symbol, quote, current_unix_millis())
    }

    pub fn ingest_quote_at(
        &mut self,
        symbol: impl AsRef<str>,
        quote: Quote,
        receive_unix_millis: u64,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        let symbol = symbol.as_ref();
        self.mark_upstream_live();
        self.record_data_activity_at(receive_unix_millis / 1_000);
        self.symbol_metrics
            .record_quote_at(symbol, &quote, receive_unix_millis);
        self.cache.push_quote(symbol, quote);
        Ok(self.quote_frames(symbol))
    }

    pub fn remove_client(&mut self, client_id: ClientId) {
        self.interests.remove_client(client_id);
        let interests = &self.interests;
        self.bootstrap
            .retain_pending(|request| interests.chart_interest_count(&request.source) > 0);
    }

    pub fn mark_upstream_degraded(&mut self) {
        self.upstream_status = RelaySourceStatus::Degraded;
        self.set_upstream_stage(RelaySourceStage::Degraded, None);
        self.event_ledger.push(
            current_unix_secs(),
            RelayEventKind::FlowIncident,
            "upstream source marked degraded",
        );
    }

    pub fn record_upstream_transport_connected_at(&mut self, unix_secs: u64) {
        self.upstream_transport_connected = true;
        if self.upstream_stage == RelaySourceStage::Connecting {
            self.set_upstream_stage(RelaySourceStage::Subscribing, Some(unix_secs));
        }
    }

    pub fn record_upstream_subscription_sent_at(&mut self, unix_secs: u64) {
        self.upstream_transport_connected = true;
        self.upstream_subscription_sent = true;
        self.symbol_metrics.advance_source_epoch();
        if matches!(
            self.upstream_stage,
            RelaySourceStage::Connecting | RelaySourceStage::Subscribing
        ) {
            self.set_upstream_stage(RelaySourceStage::Backfilling, Some(unix_secs));
        }
    }

    pub fn record_upstream_frame_received_at(&mut self, unix_secs: u64, decoded_events: usize) {
        self.upstream_transport_connected = true;
        self.upstream_frames_received = self.upstream_frames_received.saturating_add(1);
        self.upstream_events_decoded = self
            .upstream_events_decoded
            .saturating_add(u64::try_from(decoded_events).unwrap_or(u64::MAX));
        self.last_upstream_frame_unix_secs = Some(unix_secs);
        if decoded_events > 0 {
            self.last_decoded_event_unix_secs = Some(unix_secs);
        }
        if decoded_events == 0
            && matches!(
                self.upstream_stage,
                RelaySourceStage::Connecting | RelaySourceStage::Subscribing
            )
        {
            self.set_upstream_stage(RelaySourceStage::Backfilling, Some(unix_secs));
        }
    }

    pub fn record_upstream_progress(&mut self, progress: crate::upstream::UpstreamSourceProgress) {
        if progress.transport_connected {
            self.record_upstream_transport_connected_at(progress.unix_secs);
        }
        if progress.subscription_sent {
            self.record_upstream_subscription_sent_at(progress.unix_secs);
        }
        if progress.frames_received > 0 {
            self.upstream_transport_connected = true;
            self.upstream_frames_received = self
                .upstream_frames_received
                .saturating_add(progress.frames_received);
            self.upstream_events_decoded = self
                .upstream_events_decoded
                .saturating_add(progress.events_decoded);
            self.last_upstream_frame_unix_secs = Some(progress.unix_secs);
            if progress.events_decoded > 0 {
                self.last_decoded_event_unix_secs = Some(progress.unix_secs);
            }
            if progress.events_decoded == 0
                && matches!(
                    self.upstream_stage,
                    RelaySourceStage::Connecting | RelaySourceStage::Subscribing
                )
            {
                self.set_upstream_stage(RelaySourceStage::Backfilling, Some(progress.unix_secs));
            }
        }
    }

    fn mark_upstream_live(&mut self) {
        self.upstream_status = RelaySourceStatus::Up;
        self.set_upstream_stage(RelaySourceStage::Live, None);
        self.upstream_transport_connected = true;
        self.upstream_subscription_sent = true;
    }

    fn set_upstream_stage(&mut self, stage: RelaySourceStage, unix_secs: Option<u64>) {
        if self.upstream_stage != stage {
            self.upstream_stage = stage;
            self.upstream_stage_started_unix_secs = unix_secs;
        } else if self.upstream_stage_started_unix_secs.is_none() {
            self.upstream_stage_started_unix_secs = unix_secs;
        }
    }

    pub fn record_universe_refresh_success(
        &mut self,
        upstream_symbols: usize,
        upstream_ins_list_chars: usize,
        warn_chars: Option<usize>,
        max_chars: Option<usize>,
        unix_secs: u64,
    ) {
        self.upstream_symbols = upstream_symbols;
        self.upstream_ins_list_chars = upstream_ins_list_chars;
        self.upstream_ins_list_warn_chars = warn_chars;
        self.upstream_ins_list_max_chars = max_chars;
        self.upstream_ins_list_over_warn =
            warn_chars.is_some_and(|warn_chars| upstream_ins_list_chars > warn_chars);
        self.last_universe_refresh_unix_secs = Some(unix_secs);
        self.last_universe_refresh_error = None;
        self.event_ledger.push(
            unix_secs,
            RelayEventKind::UniverseRefreshed,
            format!(
                "universe refreshed: symbols={upstream_symbols}, ins_list_chars={upstream_ins_list_chars}"
            ),
        );
    }

    pub fn record_universe_refresh_success_for_symbols<I, S>(
        &mut self,
        symbols: I,
        upstream_ins_list_chars: usize,
        warn_chars: Option<usize>,
        max_chars: Option<usize>,
        unix_secs: u64,
    ) where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let symbols: Vec<String> = symbols
            .into_iter()
            .map(|symbol| symbol.as_ref().to_string())
            .collect();
        self.record_universe_refresh_success(
            symbols.len(),
            upstream_ins_list_chars,
            warn_chars,
            max_chars,
            unix_secs,
        );
        self.symbol_metrics
            .record_universe(symbols, unix_secs.saturating_mul(1_000));
    }

    pub fn record_universe_refresh_success_for_contracts(
        &mut self,
        contracts: &[FuturesContract],
        upstream_ins_list_chars: usize,
        warn_chars: Option<usize>,
        max_chars: Option<usize>,
        unix_secs: u64,
    ) {
        self.record_universe_refresh_success(
            contracts.len(),
            upstream_ins_list_chars,
            warn_chars,
            max_chars,
            unix_secs,
        );
        self.symbol_metrics.record_universe(
            contracts.iter().map(|contract| contract.symbol.as_str()),
            unix_secs.saturating_mul(1_000),
        );
        for contract in contracts {
            if let Some(instrument_name) = contract.instrument_name.as_deref() {
                self.symbol_metrics
                    .record_symbol_instrument_name(&contract.symbol, instrument_name);
            }
            self.symbol_metrics
                .record_symbol_trading_time(&contract.symbol, &contract.trading_time);
        }
    }

    pub fn record_universe_refresh_error(&mut self, message: impl Into<String>, unix_secs: u64) {
        let message = message.into();
        self.last_universe_refresh_unix_secs = Some(unix_secs);
        self.last_universe_refresh_error = Some(message.clone());
        self.event_ledger.push(
            unix_secs,
            RelayEventKind::UniverseRefreshFailed,
            format!("universe refresh failed: {message}"),
        );
    }

    pub fn record_data_activity_at(&mut self, unix_secs: u64) {
        self.last_tick_unix_secs = Some(unix_secs);
    }

    pub fn record_upstream_invalid_tick_rows(&mut self, count: u64, last_error: Option<String>) {
        self.record_upstream_invalid_tick_rows_at(count, last_error, current_unix_secs());
    }

    pub fn record_upstream_invalid_tick_rows_at(
        &mut self,
        count: u64,
        last_error: Option<String>,
        unix_secs: u64,
    ) {
        if count == 0 {
            return;
        }
        self.upstream_invalid_tick_rows = self.upstream_invalid_tick_rows.saturating_add(count);
        self.invalid_tick_row_events.push_back((unix_secs, count));
        self.last_invalid_row_unix_secs = Some(unix_secs);
        self.prune_invalid_tick_row_events(unix_secs);
        let detail = match &last_error {
            Some(error) => format!("invalid upstream tick rows: count={count}, error={error}"),
            None => format!("invalid upstream tick rows: count={count}"),
        };
        self.event_ledger
            .push(unix_secs, RelayEventKind::DecodeIncident, detail);
        if let Some(error) = last_error {
            self.last_upstream_invalid_tick_row_error = Some(error);
        }
    }

    pub fn record_upstream_invalid_tick_rows_by_symbol(
        &mut self,
        count: u64,
        invalid_rows_by_symbol: BTreeMap<String, u64>,
        last_error: Option<String>,
    ) {
        self.record_upstream_invalid_tick_rows_by_symbol_at(
            count,
            invalid_rows_by_symbol,
            last_error,
            current_unix_secs(),
        );
    }

    pub fn record_upstream_invalid_tick_rows_by_symbol_at(
        &mut self,
        count: u64,
        invalid_rows_by_symbol: BTreeMap<String, u64>,
        last_error: Option<String>,
        unix_secs: u64,
    ) {
        self.record_upstream_invalid_tick_rows_at(count, last_error.clone(), unix_secs);
        if invalid_rows_by_symbol.is_empty() {
            return;
        }
        let last_error_symbol = last_error.as_deref().and_then(invalid_row_error_symbol);
        for (symbol, count) in invalid_rows_by_symbol {
            let message = (last_error_symbol == Some(symbol.as_str()))
                .then(|| last_error.clone())
                .flatten();
            self.symbol_metrics
                .record_invalid_rows(&symbol, count, message);
        }
    }

    fn prune_invalid_tick_row_events(&mut self, now_unix_secs: u64) {
        let cutoff = now_unix_secs.saturating_sub(DECODE_HEALTH_WINDOW_SECS.saturating_mul(5));
        while self
            .invalid_tick_row_events
            .front()
            .is_some_and(|(unix_secs, _)| *unix_secs < cutoff)
        {
            self.invalid_tick_row_events.pop_front();
        }
    }

    fn recent_invalid_rows_at(&self, now_unix_secs: u64) -> u64 {
        let cutoff = now_unix_secs.saturating_sub(DECODE_HEALTH_WINDOW_SECS);
        self.invalid_tick_row_events
            .iter()
            .filter(|(unix_secs, _)| *unix_secs >= cutoff && *unix_secs <= now_unix_secs)
            .fold(0_u64, |sum, (_, count)| sum.saturating_add(*count))
    }

    #[must_use]
    pub fn interests(&self) -> &InterestRegistry {
        &self.interests
    }

    #[must_use]
    pub fn bootstrap_pending_len(&self) -> usize {
        self.bootstrap.len()
    }

    #[must_use]
    pub fn health_snapshot(&self) -> HealthSnapshot {
        self.health_snapshot_at(current_unix_secs())
    }

    #[must_use]
    pub fn health_snapshot_at(&self, now_unix_secs: u64) -> HealthSnapshot {
        let process_started = true;
        let downstream_listening = true;
        let upstream_connected = self.upstream_status == RelaySourceStatus::Up;
        let universe_ready = self.last_universe_refresh_unix_secs.is_some()
            && self.last_universe_refresh_error.is_none();
        let data_fresh = self.last_tick_unix_secs.is_some_and(|last_tick_unix_secs| {
            now_unix_secs.saturating_sub(last_tick_unix_secs) <= DEFAULT_DATA_STALE_AFTER_SECS
        });
        let recent_invalid_rows_1m = self.recent_invalid_rows_at(now_unix_secs);
        let upstream_frame_idle_ms =
            idle_millis_since(now_unix_secs, self.last_upstream_frame_unix_secs);
        let upstream_event_idle_ms =
            idle_millis_since(now_unix_secs, self.last_decoded_event_unix_secs);
        let market_data_ready = upstream_connected && universe_ready && data_fresh;
        HealthSnapshot {
            ready: process_started && downstream_listening,
            market_data_ready,
            process_started,
            downstream_listening,
            upstream_status: self.upstream_status,
            upstream_stage: self.upstream_stage,
            upstream_stage_started_unix_secs: self.upstream_stage_started_unix_secs,
            upstream_connected,
            upstream_transport_connected: self.upstream_transport_connected,
            upstream_subscription_sent: self.upstream_subscription_sent,
            universe_ready,
            data_fresh,
            downstream_clients: self.interests.client_count(),
            upstream_symbols: self.upstream_symbols,
            ticks_ingested: self.ticks_ingested,
            upstream_frames_received: self.upstream_frames_received,
            upstream_events_decoded: self.upstream_events_decoded,
            upstream_invalid_tick_rows: self.upstream_invalid_tick_rows,
            lifetime_invalid_rows: self.upstream_invalid_tick_rows,
            recent_invalid_rows_1m,
            current_decode_health: decode_health_for(recent_invalid_rows_1m),
            last_upstream_invalid_tick_row_error: self.last_upstream_invalid_tick_row_error.clone(),
            last_invalid_row_unix_secs: self.last_invalid_row_unix_secs,
            last_universe_refresh_unix_secs: self.last_universe_refresh_unix_secs,
            last_universe_refresh_error: self.last_universe_refresh_error.clone(),
            last_tick_unix_secs: self.last_tick_unix_secs,
            last_upstream_frame_unix_secs: self.last_upstream_frame_unix_secs,
            last_decoded_event_unix_secs: self.last_decoded_event_unix_secs,
            upstream_frame_idle_ms,
            upstream_frame_idle_health: flow_idle_health_for(
                upstream_frame_idle_ms,
                FRAME_IDLE_WARN_AFTER_MS,
                FRAME_IDLE_CRITICAL_AFTER_MS,
            ),
            upstream_event_idle_ms,
            upstream_event_idle_health: flow_idle_health_for(
                upstream_event_idle_ms,
                EVENT_IDLE_WARN_AFTER_MS,
                EVENT_IDLE_CRITICAL_AFTER_MS,
            ),
            data_stale_after_secs: DEFAULT_DATA_STALE_AFTER_SECS,
        }
    }

    #[must_use]
    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        self.metrics_snapshot_at(current_unix_secs())
    }

    #[must_use]
    pub fn metrics_snapshot_at(&self, now_unix_secs: u64) -> MetricsSnapshot {
        let recent_invalid_rows_1m = self.recent_invalid_rows_at(now_unix_secs);
        let upstream_frame_idle_ms =
            idle_millis_since(now_unix_secs, self.last_upstream_frame_unix_secs);
        let upstream_event_idle_ms =
            idle_millis_since(now_unix_secs, self.last_decoded_event_unix_secs);
        MetricsSnapshot {
            downstream_clients: self.interests.client_count(),
            quote_subscriptions: self.interests.total_quote_subscriptions(),
            chart_subscriptions: self.interests.total_chart_subscriptions(),
            ticks_ingested: self.ticks_ingested,
            bootstrap_pending: self.bootstrap.len(),
            bootstrap_inflight: self.bootstrap.inflight(),
            upstream_stage: self.upstream_stage,
            upstream_stage_started_unix_secs: self.upstream_stage_started_unix_secs,
            upstream_transport_connected: self.upstream_transport_connected,
            upstream_subscription_sent: self.upstream_subscription_sent,
            upstream_frames_received: self.upstream_frames_received,
            upstream_events_decoded: self.upstream_events_decoded,
            last_decoded_event_unix_secs: self.last_decoded_event_unix_secs,
            upstream_frame_idle_ms,
            upstream_frame_idle_health: flow_idle_health_for(
                upstream_frame_idle_ms,
                FRAME_IDLE_WARN_AFTER_MS,
                FRAME_IDLE_CRITICAL_AFTER_MS,
            ),
            upstream_frame_idle_warn_after_ms: FRAME_IDLE_WARN_AFTER_MS,
            upstream_frame_idle_critical_after_ms: FRAME_IDLE_CRITICAL_AFTER_MS,
            upstream_event_idle_ms,
            upstream_event_idle_health: flow_idle_health_for(
                upstream_event_idle_ms,
                EVENT_IDLE_WARN_AFTER_MS,
                EVENT_IDLE_CRITICAL_AFTER_MS,
            ),
            upstream_event_idle_warn_after_ms: EVENT_IDLE_WARN_AFTER_MS,
            upstream_event_idle_critical_after_ms: EVENT_IDLE_CRITICAL_AFTER_MS,
            upstream_symbols: self.upstream_symbols,
            upstream_ins_list_chars: self.upstream_ins_list_chars,
            upstream_ins_list_warn_chars: self.upstream_ins_list_warn_chars,
            upstream_ins_list_max_chars: self.upstream_ins_list_max_chars,
            upstream_ins_list_over_warn: self.upstream_ins_list_over_warn,
            upstream_invalid_tick_rows: self.upstream_invalid_tick_rows,
            lifetime_invalid_rows: self.upstream_invalid_tick_rows,
            recent_invalid_rows_1m,
            current_decode_health: decode_health_for(recent_invalid_rows_1m),
            last_upstream_invalid_tick_row_error: self.last_upstream_invalid_tick_row_error.clone(),
            last_invalid_row_unix_secs: self.last_invalid_row_unix_secs,
            last_universe_refresh_unix_secs: self.last_universe_refresh_unix_secs,
            last_universe_refresh_error: self.last_universe_refresh_error.clone(),
            last_tick_unix_secs: self.last_tick_unix_secs,
            last_upstream_frame_unix_secs: self.last_upstream_frame_unix_secs,
        }
    }

    #[must_use]
    pub fn dashboard_snapshot_inputs_at(&self, now_unix_millis: u64) -> DashboardSnapshotInputs {
        DashboardSnapshotInputs {
            received_at_unix_millis: now_unix_millis,
            metrics: self.metrics_snapshot_at(now_unix_millis / 1_000),
            symbols: self.symbol_metrics.read_model(),
            subscriptions: self.interests.symbol_subscription_counts(),
            events: self.event_ledger.snapshot(),
        }
    }

    #[must_use]
    pub fn event_ledger_snapshot(&self) -> Vec<RelayEvent> {
        self.event_ledger.snapshot()
    }

    #[must_use]
    pub fn symbol_metrics_snapshot_at(
        &self,
        now_unix_millis: u64,
        query: &SymbolMetricsQuery,
    ) -> SymbolMetricsSnapshot {
        self.symbol_metrics.snapshot_at(
            now_unix_millis,
            DEFAULT_DATA_STALE_AFTER_SECS.saturating_mul(1_000),
            &self.interests.symbol_subscription_counts(),
            query,
        )
    }

    #[must_use]
    pub fn symbol_metrics_snapshot(&self, query: &SymbolMetricsQuery) -> SymbolMetricsSnapshot {
        self.symbol_metrics_snapshot_at(current_unix_millis(), query)
    }

    #[must_use]
    pub fn dashboard_snapshot_at(
        &self,
        now_unix_millis: u64,
        query: &SymbolMetricsQuery,
    ) -> DashboardSnapshot {
        self.dashboard_snapshot_inputs_at(now_unix_millis)
            .into_dashboard_snapshot(query)
    }

    #[must_use]
    pub fn dashboard_snapshot(&self, query: &SymbolMetricsQuery) -> DashboardSnapshot {
        self.dashboard_snapshot_at(current_unix_millis(), query)
    }

    fn quote_frames(&self, symbol: &str) -> Vec<DownstreamFrame> {
        let Some(quote) = self.cache.quote(symbol) else {
            return Vec::new();
        };
        let payload = quote_payload(symbol, quote);

        self.interests
            .quote_clients(symbol)
            .into_iter()
            .map(|client_id| DownstreamFrame {
                client_id,
                payload: payload.clone(),
            })
            .collect()
    }

    fn cached_quote_frames_for_client(
        &self,
        client_id: ClientId,
        symbols: &[String],
    ) -> Vec<DownstreamFrame> {
        symbols
            .iter()
            .filter_map(|symbol| {
                self.cache.quote(symbol).map(|quote| DownstreamFrame {
                    client_id,
                    payload: quote_payload(symbol, quote),
                })
            })
            .collect()
    }

    fn kline_frames(
        &mut self,
        symbol: &str,
        row: RelayTickRow,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        let sources = self.interests.sources_for_symbol(symbol);
        let mut frames = Vec::new();
        for source in sources {
            if source.duration_ns <= 0 {
                continue;
            }
            let completed_rows = {
                let synthesizer = self
                    .klines
                    .entry(source.clone())
                    .or_insert_with(|| KlineSynthesis::new(symbol.to_string(), source.duration_ns));
                synthesizer.push_tick(row.clone())?
            };
            for completed in completed_rows {
                let kline_payload =
                    RelayMarketFrame::rtn_data(vec![RelayMarketFrame::kline_update(
                        symbol,
                        source.duration_ns,
                        completed.clone(),
                    )])
                    .into_value();
                for (client_id, chart_id) in self.interests.chart_clients_with_ids(&source) {
                    frames.push(DownstreamFrame {
                        client_id,
                        payload: kline_payload.clone(),
                    });
                    frames.push(DownstreamFrame {
                        client_id,
                        payload: chart_payload(&chart_id, completed.id),
                    });
                }
            }
        }
        Ok(frames)
    }

    fn replay_cached_kline_frames(
        &mut self,
        client_id: ClientId,
        source: &SourceKey,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        if source.duration_ns <= 0 || self.klines.contains_key(source) {
            return Ok(Vec::new());
        }
        let Some(symbol) = source.symbols.first() else {
            return Ok(Vec::new());
        };
        let ticks = self.cache.ticks(symbol);
        if ticks.is_empty() {
            return Ok(Vec::new());
        }

        let mut synthesis = KlineSynthesis::new(symbol.clone(), source.duration_ns);
        let mut completed_rows = Vec::new();
        for tick in ticks {
            completed_rows.extend(synthesis.push_tick(tick)?);
        }
        self.klines.insert(source.clone(), synthesis);

        let mut frames = Vec::new();
        for completed in completed_rows {
            frames.push(DownstreamFrame {
                client_id,
                payload: RelayMarketFrame::rtn_data(vec![RelayMarketFrame::kline_update(
                    symbol,
                    source.duration_ns,
                    completed.clone(),
                )])
                .into_value(),
            });
            if let Some(chart_id) = self.interests.downstream_chart_id(client_id, source) {
                frames.push(DownstreamFrame {
                    client_id,
                    payload: chart_payload(chart_id, completed.id),
                });
            }
        }
        Ok(frames)
    }
}

fn chart_payload(chart_id: &str, right_id: i64) -> Value {
    json!({
        "aid": "rtn_data",
        "data": [
            {
                "charts": {
                    chart_id: {
                        "left_id": right_id,
                        "right_id": right_id,
                        "more_data": false,
                        "ready": true
                    }
                }
            }
        ]
    })
}

fn quote_payload(symbol: &str, quote: Quote) -> Value {
    RelayMarketFrame::rtn_data(vec![RelayMarketFrame::RtnData(vec![json!({
        "quotes": {
            symbol: {
                "instrument_id": quote.instrument_id,
                "datetime": quote.datetime,
                "last_price": quote.last_price,
                "volume": quote.volume,
                "open_interest": quote.open_interest
            }
        }
    })])])
    .into_value()
}

fn invalid_row_error_symbol(message: &str) -> Option<&str> {
    message.split_once(" row ").map(|(symbol, _)| symbol)
}

fn idle_millis_since(now_unix_secs: u64, last_unix_secs: Option<u64>) -> Option<u64> {
    last_unix_secs.map(|last_unix_secs| now_unix_secs.saturating_sub(last_unix_secs) * 1_000)
}

fn flow_idle_health_for(
    idle_ms: Option<u64>,
    warn_after_ms: u64,
    critical_after_ms: u64,
) -> FlowIdleHealth {
    match idle_ms {
        None => FlowIdleHealth::NoSample,
        Some(idle_ms) if idle_ms > critical_after_ms => FlowIdleHealth::Critical,
        Some(idle_ms) if idle_ms > warn_after_ms => FlowIdleHealth::Warn,
        Some(_) => FlowIdleHealth::Live,
    }
}

fn decode_health_for(recent_invalid_rows: u64) -> DecodeHealth {
    if recent_invalid_rows > 0 {
        DecodeHealth::Degraded
    } else {
        DecodeHealth::Healthy
    }
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

fn current_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
