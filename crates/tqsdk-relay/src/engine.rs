#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::{Value, json};
use tqsdk_core::{Kline, Quote, Tick, TradingStatus};

use crate::bootstrap::{BootstrapQueue, BootstrapRequest};
use crate::cache::{MarketCache, MarketCacheLimits, MarketCacheWriteReport};
use crate::dashboard_read_model::{
    DashboardSnapshot, DashboardSnapshotInputs, symbol_metrics_context_for_stage,
};
use crate::error::RelayResult;
use crate::interest::{ChartSubscription, ClientId, InterestRegistry, SourceKey};
use crate::kline::KlineSynthesis;
use crate::observability::{
    DECODE_HEALTH_WINDOW_SECS, DEFAULT_DATA_STALE_AFTER_SECS, DecodeHealth,
    EVENT_IDLE_CRITICAL_AFTER_MS, EVENT_IDLE_WARN_AFTER_MS, FRAME_IDLE_CRITICAL_AFTER_MS,
    FRAME_IDLE_WARN_AFTER_MS, FlowIdleHealth, HealthSnapshot, MetricsSnapshot, RelaySourceStage,
    RelaySourceStatus,
};
use crate::protocol::{DownstreamCommand, RelayKlineRow, RelayMarketFrame, RelayTickRow};
use crate::rolling_writer::RollingWriterStatus;
use crate::symbol_metrics::{
    SymbolMetricsQuery, SymbolMetricsSnapshot, SymbolTelemetryStore, parse_quote_datetime_ns,
};
use crate::universe::FuturesContract;

const DEFAULT_RELAY_EVENT_LEDGER_LIMIT: usize = 128;

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

/// Immutable market payload routed to one downstream client.
///
/// A quote update may target many clients. The engine shares one immutable
/// JSON value here; the server serializes it once and shares the encoded
/// WebSocket frame across recipients.
#[derive(Debug, Clone, PartialEq)]
pub struct DownstreamFrame {
    pub client_id: ClientId,
    pub payload: Arc<Value>,
}

impl DownstreamFrame {
    #[must_use]
    pub fn new(client_id: ClientId, payload: Value) -> Self {
        Self::shared(client_id, Arc::new(payload))
    }

    #[must_use]
    pub fn shared(client_id: ClientId, payload: Arc<Value>) -> Self {
        Self { client_id, payload }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct KlineSourceKey {
    duration_ns: i64,
    symbol: String,
}

impl KlineSourceKey {
    fn new(source: &SourceKey, symbol: impl Into<String>) -> Self {
        Self {
            duration_ns: source.duration_ns,
            symbol: symbol.into(),
        }
    }
}

#[derive(Debug)]
pub struct RelayEngine {
    cache: MarketCache,
    interests: InterestRegistry,
    bootstrap: BootstrapQueue,
    klines: HashMap<KlineSourceKey, KlineSynthesis>,
    completed_klines: BTreeMap<KlineSourceKey, VecDeque<RelayKlineRow>>,
    persisted_official_klines: BTreeMap<KlineSourceKey, VecDeque<RelayKlineRow>>,
    official_kline_sources: BTreeSet<KlineSourceKey>,
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
    last_upstream_peek_delay_ms: Option<u64>,
    last_upstream_decode_ms: Option<u64>,
    ticks_ingested: u64,
    cache_evicted_symbols: u64,
    cache_admission_drops: u64,
    rolling_writer_status: RollingWriterStatus,
    upstream_symbols: usize,
    upstream_base_symbols: BTreeSet<String>,
    upstream_subscribed_symbols: BTreeSet<String>,
    upstream_tick_chart_symbols: BTreeSet<String>,
    pending_upstream_subscription_symbols: BTreeSet<String>,
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
            completed_klines: BTreeMap::new(),
            persisted_official_klines: BTreeMap::new(),
            official_kline_sources: BTreeSet::new(),
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
            last_upstream_peek_delay_ms: None,
            last_upstream_decode_ms: None,
            ticks_ingested: 0,
            cache_evicted_symbols: 0,
            cache_admission_drops: 0,
            rolling_writer_status: RollingWriterStatus::default(),
            upstream_symbols: 0,
            upstream_base_symbols: BTreeSet::new(),
            upstream_subscribed_symbols: BTreeSet::new(),
            upstream_tick_chart_symbols: BTreeSet::new(),
            pending_upstream_subscription_symbols: BTreeSet::new(),
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

    /// Builds the memory-only engine with explicit cache admission limits.
    #[must_use]
    pub fn new_memory_only_with_cache_limits(
        tick_capacity: usize,
        kline_capacity: usize,
        cache_limits: MarketCacheLimits,
    ) -> Self {
        let mut engine = Self::new_memory_only(tick_capacity, kline_capacity);
        engine.cache = MarketCache::with_limits(tick_capacity, kline_capacity, cache_limits);
        engine
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
                self.queue_missing_upstream_symbols_for_current_interests();
                Ok(frames)
            }
            DownstreamCommand::SetChart(command) => {
                if command.symbols.is_empty() {
                    self.interests.remove_chart(client_id, &command.chart_id);
                    self.retain_bootstrap_with_current_chart_interests();
                    self.prune_pending_upstream_subscription_symbols();
                    self.prune_inactive_kline_state();
                    return Ok(vec![DownstreamFrame::new(
                        client_id,
                        delete_chart_payload(&command.chart_id),
                    )]);
                }
                let replay_subscription = ChartSubscription::new(
                    client_id,
                    command.chart_id.clone(),
                    command.symbols.clone(),
                );
                let source = self.interests.set_chart(client_id, command);
                self.prune_inactive_kline_state();
                self.bootstrap.enqueue(BootstrapRequest {
                    source: source.clone(),
                    start_id: i64::MIN,
                    end_id: i64::MAX,
                });
                self.queue_missing_upstream_symbols_for_source(&source);
                self.replay_cached_kline_frames(&replay_subscription, &source)
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
        let cache_report = self.cache.push_tick(symbol, row.clone());
        self.record_cache_write(cache_report);
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
        let synthetic_tick = quote_to_synthetic_tick(&quote);
        if let Some(row) = synthetic_tick.clone() {
            let tick_cache_report = self.cache.push_tick(symbol, row.clone());
            self.record_cache_write(tick_cache_report);
        }
        let quote_cache_report = self.cache.push_quote(symbol, quote);
        self.record_cache_write(quote_cache_report);
        let mut frames = self.quote_frames(symbol);
        if let Some(row) = synthetic_tick {
            frames.extend(self.kline_frames(symbol, row)?);
        }
        Ok(frames)
    }

    /// Routes an official upstream Kline. Once one is observed for a source,
    /// local tick synthesis stops producing competing rows for that source.
    pub fn ingest_official_kline(
        &mut self,
        symbol: impl AsRef<str>,
        duration_ns: i64,
        row: RelayKlineRow,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        let symbol = symbol.as_ref();
        if duration_ns <= 0 {
            return Err(crate::error::RelayError::invalid_protocol(
                "official Kline duration must be positive",
            ));
        }
        let sources = self
            .interests
            .sources_for_symbol(symbol)
            .into_iter()
            .filter(|source| source.duration_ns == duration_ns)
            .collect::<Vec<_>>();
        let mut frames = Vec::new();
        self.record_persisted_official_kline(
            &KlineSourceKey {
                duration_ns,
                symbol: symbol.to_owned(),
            },
            &row,
        );
        for source in sources {
            let key = KlineSourceKey::new(&source, symbol);
            self.official_kline_sources.insert(key.clone());
            self.klines.remove(&key);
            let rows = self.completed_klines.entry(key).or_default();
            if let Some(existing) = rows.iter_mut().find(|existing| existing.id == row.id) {
                *existing = row.clone();
            } else {
                rows.push_back(row.clone());
                while rows.len() > self.cache.kline_capacity() {
                    let _ = rows.pop_front();
                }
            }
            let payload = Arc::new(
                RelayMarketFrame::rtn_data(vec![RelayMarketFrame::kline_update(
                    symbol,
                    duration_ns,
                    row.clone(),
                )])
                .into_value(),
            );
            for subscription in self.interests.chart_subscriptions(&source) {
                frames.push(DownstreamFrame::shared(
                    subscription.client_id,
                    Arc::clone(&payload),
                ));
            }
        }
        Ok(frames)
    }

    /// Restores durable lossless ticks before downstream clients connect.
    pub fn restore_rolling_ticks(&mut self, symbol: &str, rows: &[Tick]) {
        for row in rows {
            let report = self.cache.push_tick(symbol, relay_tick_from_core(row));
            self.record_cache_write(report);
        }
    }

    /// Restores durable official Klines before downstream clients connect.
    pub fn restore_official_rolling_klines(
        &mut self,
        symbol: &str,
        duration_ns: i64,
        rows: &[Kline],
    ) -> RelayResult<()> {
        if duration_ns <= 0 {
            return Err(crate::error::RelayError::invalid_protocol(
                "restored official Kline duration must be positive",
            ));
        }
        let key = KlineSourceKey {
            duration_ns,
            symbol: symbol.to_owned(),
        };
        for row in rows {
            self.record_persisted_official_kline(&key, &relay_kline_from_core(row));
        }
        Ok(())
    }

    pub fn ingest_trading_status(
        &mut self,
        symbol: impl AsRef<str>,
        trading_status: TradingStatus,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        self.ingest_trading_status_at(symbol, trading_status, current_unix_millis())
    }

    pub fn ingest_trading_status_at_for_test(
        &mut self,
        symbol: impl AsRef<str>,
        trade_status: impl AsRef<str>,
        receive_unix_millis: u64,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        let symbol = symbol.as_ref();
        self.ingest_trading_status_at(
            symbol,
            TradingStatus {
                symbol: symbol.to_string(),
                trade_status: trade_status.as_ref().to_string(),
                epoch: None,
            },
            receive_unix_millis,
        )
    }

    fn ingest_trading_status_at(
        &mut self,
        symbol: impl AsRef<str>,
        trading_status: TradingStatus,
        receive_unix_millis: u64,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        let symbol = symbol.as_ref();
        self.mark_upstream_live();
        self.record_data_activity_at(receive_unix_millis / 1_000);
        self.symbol_metrics.record_trading_status_at(
            symbol,
            &trading_status.trade_status,
            receive_unix_millis,
        );
        Ok(Vec::new())
    }

    fn record_cache_write(&mut self, report: MarketCacheWriteReport) {
        self.cache_evicted_symbols = self
            .cache_evicted_symbols
            .saturating_add(u64::try_from(report.evicted_symbols).unwrap_or(u64::MAX));
        if !report.stored {
            self.cache_admission_drops = self.cache_admission_drops.saturating_add(1);
        }
    }

    pub fn record_rolling_writer_status(&mut self, status: RollingWriterStatus) {
        self.rolling_writer_status = status;
    }

    pub fn remove_client(&mut self, client_id: ClientId) {
        self.interests.remove_client(client_id);
        self.retain_bootstrap_with_current_chart_interests();
        self.prune_pending_upstream_subscription_symbols();
        self.prune_inactive_kline_state();
    }

    fn retain_bootstrap_with_current_chart_interests(&mut self) {
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
        if progress.last_peek_delay_ms.is_some() {
            self.last_upstream_peek_delay_ms = progress.last_peek_delay_ms;
        }
        if progress.last_decode_ms.is_some() {
            self.last_upstream_decode_ms = progress.last_decode_ms;
        }
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
        self.upstream_base_symbols = symbols.iter().cloned().collect();
        self.upstream_subscribed_symbols = self.upstream_base_symbols.clone();
        self.upstream_tick_chart_symbols.clear();
        self.symbol_metrics
            .record_universe(symbols, unix_secs.saturating_mul(1_000));
        self.queue_missing_upstream_symbols_for_current_interests();
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
        self.upstream_base_symbols = contracts
            .iter()
            .map(|contract| contract.symbol.clone())
            .collect();
        self.upstream_subscribed_symbols = self.upstream_base_symbols.clone();
        self.upstream_tick_chart_symbols.clear();
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
        self.queue_missing_upstream_symbols_for_current_interests();
    }

    /// Records the exact dynamic tick-chart set confirmed after upstream writes.
    ///
    /// The historical method name is retained for callers; `symbols` replaces
    /// the prior dynamic set rather than accumulating forever.
    pub fn record_dynamic_upstream_subscription_sent<I, S>(
        &mut self,
        symbols: I,
        upstream_ins_list_chars: usize,
        unix_secs: u64,
    ) where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.upstream_tick_chart_symbols = symbols
            .into_iter()
            .map(|symbol| symbol.as_ref().trim().to_string())
            .filter(|symbol| !symbol.is_empty())
            .collect();
        self.upstream_subscribed_symbols = self.upstream_base_symbols.clone();
        self.upstream_subscribed_symbols
            .extend(self.upstream_tick_chart_symbols.iter().cloned());
        self.upstream_symbols = self.upstream_subscribed_symbols.len();
        self.pending_upstream_subscription_symbols.clear();
        self.upstream_ins_list_chars = self.upstream_ins_list_chars.max(upstream_ins_list_chars);
        self.upstream_ins_list_over_warn = self
            .upstream_ins_list_warn_chars
            .is_some_and(|warn_chars| self.upstream_ins_list_chars > warn_chars);
        self.symbol_metrics.record_universe(
            self.upstream_subscribed_symbols.iter().map(String::as_str),
            unix_secs.saturating_mul(1_000),
        );
    }

    pub fn retain_missing_upstream_subscription_symbols<I, S>(&self, symbols: I) -> Vec<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        symbols
            .into_iter()
            .map(|symbol| symbol.as_ref().trim().to_string())
            .filter(|symbol| !symbol.is_empty())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|symbol| !self.upstream_tick_chart_symbols.contains(symbol))
            .collect()
    }

    pub fn queue_missing_upstream_symbols_for_current_interests(&mut self) {
        let symbols = self.interests.subscribed_symbols();
        self.queue_missing_upstream_symbols(symbols);
        let chart_symbols = self.interests.chart_symbols();
        self.queue_missing_upstream_tick_chart_symbols(chart_symbols);
    }

    pub fn drain_pending_upstream_subscription_symbols(&mut self) -> Vec<String> {
        std::mem::take(&mut self.pending_upstream_subscription_symbols)
            .into_iter()
            .collect()
    }

    pub fn record_trading_calendar(&mut self, calendar: &[tqsdk_core::TradingCalendarDay]) {
        self.symbol_metrics.record_trading_calendar(calendar);
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

    fn queue_missing_upstream_symbols_for_source(&mut self, source: &SourceKey) {
        self.queue_missing_upstream_tick_chart_symbols(source.symbols.iter().map(String::as_str));
    }

    fn queue_missing_upstream_symbols<I, S>(&mut self, symbols: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for symbol in symbols {
            let symbol = symbol.as_ref().trim();
            if symbol.is_empty() || self.upstream_subscribed_symbols.contains(symbol) {
                continue;
            }
            self.pending_upstream_subscription_symbols
                .insert(symbol.to_string());
        }
    }

    fn queue_missing_upstream_tick_chart_symbols<I, S>(&mut self, symbols: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for symbol in symbols {
            let symbol = symbol.as_ref().trim();
            if symbol.is_empty() || self.upstream_tick_chart_symbols.contains(symbol) {
                continue;
            }
            self.pending_upstream_subscription_symbols
                .insert(symbol.to_string());
        }
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

    /// Returns the exact upstream tick-chart symbols required by current
    /// downstream interests. Symbols already covered by the configured
    /// quote-only universe do not need a tick chart unless a client explicitly
    /// owns a chart for them.
    #[must_use]
    pub fn desired_upstream_tick_chart_symbols(&self) -> BTreeSet<String> {
        let mut desired = self.interests.chart_symbols();
        desired.extend(
            self.interests
                .subscribed_symbols()
                .into_iter()
                .filter(|symbol| !self.upstream_base_symbols.contains(symbol)),
        );
        desired
    }

    /// Active downstream Kline sources eligible for official upstream charts.
    /// Tick charts remain managed separately for quote and true-tick delivery.
    #[must_use]
    pub fn desired_upstream_kline_sources(&self) -> Vec<SourceKey> {
        self.interests
            .sources()
            .into_iter()
            .filter(|source| source.duration_ns > 0 && source.symbols.len() == 1)
            .collect()
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
            last_upstream_peek_delay_ms: self.last_upstream_peek_delay_ms,
            last_upstream_decode_ms: self.last_upstream_decode_ms,
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
            market_cache_symbols: self.cache.cached_symbols(),
            market_cache_retained_bytes: self.cache.retained_bytes(),
            market_cache_max_symbols: self.cache.limits().max_symbols,
            market_cache_max_retained_bytes: self.cache.limits().max_retained_bytes,
            market_cache_evicted_symbols: self.cache_evicted_symbols,
            market_cache_admission_drops: self.cache_admission_drops,
            rolling_cache_source_epoch: self.rolling_writer_status.source_epoch,
            rolling_cache_enqueued_revision: self.rolling_writer_status.enqueued_revision,
            rolling_cache_durable_revision: self.rolling_writer_status.durable_revision,
            rolling_cache_discontinuities: self.rolling_writer_status.discontinuities,
            rolling_cache_degraded: self.rolling_writer_status.degraded,
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
            last_upstream_peek_delay_ms: self.last_upstream_peek_delay_ms,
            last_upstream_decode_ms: self.last_upstream_decode_ms,
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
        self.symbol_metrics.snapshot_at_with_context(
            now_unix_millis,
            DEFAULT_DATA_STALE_AFTER_SECS.saturating_mul(1_000),
            &self.interests.symbol_subscription_counts(),
            query,
            symbol_metrics_context_for_stage(self.upstream_stage),
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
        let Some(clients) = self
            .interests
            .quote_clients_ref(symbol)
            .filter(|clients| !clients.is_empty())
        else {
            return Vec::new();
        };
        let Some(quote) = self.cache.quote_ref(symbol) else {
            return Vec::new();
        };
        let payload = Arc::new(quote_payload(symbol, quote));
        clients
            .iter()
            .copied()
            .map(|client_id| DownstreamFrame::shared(client_id, Arc::clone(&payload)))
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
                self.cache
                    .quote_ref(symbol)
                    .map(|quote| DownstreamFrame::new(client_id, quote_payload(symbol, quote)))
            })
            .collect()
    }

    fn kline_frames(
        &mut self,
        symbol: &str,
        row: RelayTickRow,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        let mut sources_by_duration = BTreeMap::<i64, Vec<&SourceKey>>::new();
        for source in self
            .interests
            .sources_for_symbol_ref(symbol)
            .into_iter()
            .flatten()
        {
            if source.duration_ns > 0 {
                sources_by_duration
                    .entry(source.duration_ns)
                    .or_default()
                    .push(source);
            }
        }

        let mut frames = Vec::new();
        for sources in sources_by_duration.into_values() {
            let source = sources
                .first()
                .expect("non-empty duration group from source insertion");
            let key = KlineSourceKey::new(source, symbol);
            if self.official_kline_sources.contains(&key) {
                continue;
            }
            let completed_rows = {
                let synthesizer = self
                    .klines
                    .entry(key.clone())
                    .or_insert_with(|| KlineSynthesis::new(symbol.to_string(), source.duration_ns));
                synthesizer.push_tick_ref(&row)?
            };

            for completed in completed_rows {
                let kline_capacity = self.cache.kline_capacity();
                let rows = self.completed_klines.entry(key.clone()).or_default();
                if !rows.back().is_some_and(|last| last.id == completed.id) {
                    rows.push_back(completed.clone());
                    while rows.len() > kline_capacity {
                        let _ = rows.pop_front();
                    }
                }
                let kline_payload = Arc::new(
                    RelayMarketFrame::rtn_data(vec![RelayMarketFrame::kline_update(
                        symbol,
                        source.duration_ns,
                        completed.clone(),
                    )])
                    .into_value(),
                );

                for source in &sources {
                    for subscription in self.interests.chart_subscriptions(source) {
                        frames.push(DownstreamFrame::shared(
                            subscription.client_id,
                            Arc::clone(&kline_payload),
                        ));
                        frames.extend(self.binding_frames_for_completed(
                            source,
                            &subscription,
                            symbol,
                            &completed,
                        ));
                        if subscription
                            .symbols
                            .first()
                            .is_some_and(|primary| primary == symbol)
                        {
                            frames.push(DownstreamFrame::new(
                                subscription.client_id,
                                chart_payload(&subscription, source, completed.id),
                            ));
                        }
                    }
                }
            }
        }
        Ok(frames)
    }

    fn replay_cached_kline_frames(
        &mut self,
        subscription: &ChartSubscription,
        source: &SourceKey,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        if source.duration_ns <= 0 {
            return Ok(Vec::new());
        }

        let mut frames = Vec::new();
        for symbol in &source.symbols {
            let key = KlineSourceKey::new(source, symbol);
            if let Some(rows) = self.persisted_official_klines.get(&key).cloned() {
                self.official_kline_sources.insert(key.clone());
                self.klines.remove(&key);
                self.completed_klines.insert(key.clone(), rows);
            }
            if !self.klines.contains_key(&key) {
                let Some(ticks) = self.cache.tick_ring(symbol) else {
                    continue;
                };
                if ticks.is_empty() {
                    continue;
                }

                let mut synthesis = KlineSynthesis::new(symbol.clone(), source.duration_ns);
                let mut completed_rows = Vec::new();
                for tick in ticks {
                    completed_rows.extend(synthesis.push_tick_ref(tick)?);
                }
                for completed in &completed_rows {
                    self.record_completed_kline(&key, completed);
                }
                self.klines.insert(key.clone(), synthesis);
            }

            if let Some(completed_rows) = self.completed_klines.get(&key) {
                for completed in completed_rows {
                    frames.push(DownstreamFrame::new(
                        subscription.client_id,
                        RelayMarketFrame::rtn_data(vec![RelayMarketFrame::kline_update(
                            symbol,
                            source.duration_ns,
                            completed.clone(),
                        )])
                        .into_value(),
                    ));
                    frames.extend(self.binding_frames_for_completed(
                        source,
                        subscription,
                        symbol,
                        completed,
                    ));
                    if subscription
                        .symbols
                        .first()
                        .is_some_and(|primary| primary == symbol)
                    {
                        frames.push(DownstreamFrame::new(
                            subscription.client_id,
                            chart_payload(subscription, source, completed.id),
                        ));
                    }
                }
            }
        }
        Ok(frames)
    }

    fn record_completed_kline(&mut self, key: &KlineSourceKey, row: &RelayKlineRow) {
        let kline_capacity = self.cache.kline_capacity();
        let rows = self.completed_klines.entry(key.clone()).or_default();
        if rows.back().is_some_and(|last| last.id == row.id) {
            return;
        }
        rows.push_back(row.clone());
        while rows.len() > kline_capacity {
            let _ = rows.pop_front();
        }
    }

    fn record_persisted_official_kline(&mut self, key: &KlineSourceKey, row: &RelayKlineRow) {
        let kline_capacity = self.cache.kline_capacity();
        let rows = self
            .persisted_official_klines
            .entry(key.clone())
            .or_default();
        if let Some(existing) = rows.iter_mut().find(|existing| existing.id == row.id) {
            *existing = row.clone();
            return;
        }
        rows.push_back(row.clone());
        while rows.len() > kline_capacity {
            let _ = rows.pop_front();
        }
    }

    fn completed_kline_id(&self, source: &SourceKey, symbol: &str, datetime: i64) -> Option<i64> {
        self.completed_klines
            .get(&KlineSourceKey::new(source, symbol))
            .and_then(|rows| {
                rows.iter()
                    .find(|row| row.datetime == datetime)
                    .map(|row| row.id)
            })
    }

    fn prune_inactive_kline_state(&mut self) {
        let mut active_keys = BTreeSet::new();
        for source in self.interests.active_chart_sources() {
            for symbol in &source.symbols {
                active_keys.insert(KlineSourceKey::new(&source, symbol));
            }
        }
        self.klines.retain(|key, _| active_keys.contains(key));
        self.completed_klines
            .retain(|key, _| active_keys.contains(key));
    }

    fn binding_frames_for_completed(
        &self,
        source: &SourceKey,
        subscription: &ChartSubscription,
        completed_symbol: &str,
        completed: &RelayKlineRow,
    ) -> Vec<DownstreamFrame> {
        let Some(primary_symbol) = subscription.symbols.first() else {
            return Vec::new();
        };
        if subscription.symbols.len() < 2 {
            return Vec::new();
        }

        let mut frames = Vec::new();
        if completed_symbol == primary_symbol {
            for secondary_symbol in subscription.symbols.iter().skip(1) {
                if let Some(secondary_id) =
                    self.completed_kline_id(source, secondary_symbol, completed.datetime)
                {
                    frames.push(DownstreamFrame::new(
                        subscription.client_id,
                        binding_payload(
                            primary_symbol,
                            source.duration_ns,
                            secondary_symbol,
                            completed.id,
                            secondary_id,
                        ),
                    ));
                }
            }
            return frames;
        }

        if !subscription
            .symbols
            .iter()
            .skip(1)
            .any(|symbol| symbol == completed_symbol)
        {
            return Vec::new();
        }
        if let Some(primary_id) =
            self.completed_kline_id(source, primary_symbol, completed.datetime)
        {
            frames.push(DownstreamFrame::new(
                subscription.client_id,
                binding_payload(
                    primary_symbol,
                    source.duration_ns,
                    completed_symbol,
                    primary_id,
                    completed.id,
                ),
            ));
        }
        frames
    }

    fn prune_pending_upstream_subscription_symbols(&mut self) {
        let subscribed_symbols = self.interests.subscribed_symbols();
        let chart_symbols = self.interests.chart_symbols();
        self.pending_upstream_subscription_symbols.retain(|symbol| {
            (!self.upstream_subscribed_symbols.contains(symbol)
                && subscribed_symbols.contains(symbol))
                || (!self.upstream_tick_chart_symbols.contains(symbol)
                    && chart_symbols.contains(symbol))
        });
    }
}

fn chart_payload(subscription: &ChartSubscription, source: &SourceKey, right_id: i64) -> Value {
    let ins_list = subscription.symbols.join(",");
    json!({
        "aid": "rtn_data",
        "data": [
            {
                "charts": {
                    subscription.chart_id.as_str(): {
                        "state": {
                            "aid": "set_chart",
                            "chart_id": subscription.chart_id.as_str(),
                            "ins_list": ins_list,
                            "duration": source.duration_ns,
                            "view_width": source.view_width
                        },
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

fn delete_chart_payload(chart_id: &str) -> Value {
    json!({
        "aid": "rtn_data",
        "data": [
            {
                "charts": {
                    chart_id: Value::Null
                }
            }
        ]
    })
}

fn binding_payload(
    primary_symbol: &str,
    duration_ns: i64,
    secondary_symbol: &str,
    primary_id: i64,
    secondary_id: i64,
) -> Value {
    json!({
        "aid": "rtn_data",
        "data": [
            {
                "klines": {
                    primary_symbol: {
                        duration_ns.to_string(): {
                            "binding": {
                                secondary_symbol: {
                                    primary_id.to_string(): secondary_id
                                }
                            }
                        }
                    }
                }
            }
        ]
    })
}

fn quote_payload(symbol: &str, quote: &Quote) -> Value {
    RelayMarketFrame::rtn_data(vec![RelayMarketFrame::RtnData(vec![json!({
        "quotes": {
            symbol: {
                "instrument_id": quote.instrument_id.as_str(),
                "datetime": quote.datetime.as_str(),
                "last_price": quote.last_price,
                "volume": quote.volume,
                "open_interest": quote.open_interest
            }
        }
    })])])
    .into_value()
}

fn quote_to_synthetic_tick(quote: &Quote) -> Option<RelayTickRow> {
    let datetime = parse_quote_datetime_ns(&quote.datetime)?;
    quote.last_price.is_finite().then_some(RelayTickRow {
        id: datetime,
        datetime,
        last_price: quote.last_price,
        volume: quote.volume,
        open_interest: quote.open_interest,
    })
}

fn relay_tick_from_core(row: &Tick) -> RelayTickRow {
    RelayTickRow {
        id: row.id,
        datetime: row.datetime,
        last_price: row.last_price,
        volume: row.volume,
        open_interest: row.open_interest,
    }
}

fn relay_kline_from_core(row: &Kline) -> RelayKlineRow {
    RelayKlineRow {
        id: row.id,
        datetime: row.datetime,
        open: row.open,
        high: row.high,
        low: row.low,
        close: row.close,
        volume: row.volume,
        open_oi: row.open_oi,
        close_oi: row.close_oi,
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::SetChartCommand;

    fn chart(symbol: &str) -> DownstreamCommand {
        chart_with_view_width(symbol, 16)
    }

    fn chart_with_view_width(symbol: &str, view_width: usize) -> DownstreamCommand {
        DownstreamCommand::SetChart(SetChartCommand {
            chart_id: "chart".to_string(),
            symbols: vec![symbol.to_string()],
            duration_ns: 60,
            view_width,
            left_kline_id: None,
            focus_datetime_ns: None,
            focus_position: None,
        })
    }

    fn tick(id: i64, datetime: i64) -> RelayTickRow {
        RelayTickRow {
            id,
            datetime,
            last_price: 600.0 + id as f64,
            volume: id * 10,
            open_interest: id * 100,
        }
    }

    #[test]
    fn completed_klines_are_capacity_bounded() {
        let client = ClientId::new(1);
        let mut engine = RelayEngine::new_memory_only(16, 2);
        engine.handle_command(client, chart("SHFE.au2602")).unwrap();

        for id in 0..=3 {
            engine
                .ingest_tick_at_for_test("SHFE.au2602", tick(id, id * 60), 0)
                .unwrap();
        }

        let rows = engine.completed_klines.values().next().unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|row| row.datetime == 60));
        assert!(rows.iter().any(|row| row.datetime == 120));
    }

    #[test]
    fn rolling_writer_status_is_exposed_in_metrics_and_dashboard() {
        let mut engine = RelayEngine::new_memory_only(16, 4);
        engine.record_rolling_writer_status(RollingWriterStatus {
            source_epoch: 3,
            enqueued_revision: 9,
            durable_revision: 8,
            discontinuities: 1,
            degraded: true,
        });

        let metrics = engine.metrics_snapshot();
        assert_eq!(metrics.rolling_cache_source_epoch, 3);
        assert_eq!(metrics.rolling_cache_enqueued_revision, 9);
        assert_eq!(metrics.rolling_cache_durable_revision, 8);
        assert_eq!(metrics.rolling_cache_discontinuities, 1);
        assert!(metrics.rolling_cache_degraded);
        assert!(
            engine
                .dashboard_snapshot_at(1_000, &SymbolMetricsQuery::default())
                .metrics
                .rolling_cache_degraded
        );
    }

    #[test]
    fn restored_official_klines_are_preferred_over_tick_synthesis() {
        let mut engine = RelayEngine::new_memory_only(16, 4);
        engine
            .restore_official_rolling_klines(
                "SHFE.au2602",
                60,
                &[Kline {
                    id: 7,
                    datetime: 60,
                    open: 600.0,
                    high: 602.0,
                    low: 599.0,
                    close: 601.0,
                    volume: 10,
                    open_oi: 20,
                    close_oi: 21,
                    ..Kline::default()
                }],
            )
            .unwrap();

        let frames = engine
            .handle_command(ClientId::new(1), chart("SHFE.au2602"))
            .unwrap();

        assert!(frames.is_empty());
        assert!(engine.klines.is_empty());
        assert_eq!(engine.completed_klines.values().next().unwrap()[0].id, 7);
        assert_eq!(engine.official_kline_sources.len(), 1);
    }

    #[test]
    fn removing_last_chart_client_reclaims_kline_state() {
        let client = ClientId::new(1);
        let mut engine = RelayEngine::new_memory_only(16, 4);
        engine.handle_command(client, chart("SHFE.au2602")).unwrap();
        engine
            .ingest_tick_at_for_test("SHFE.au2602", tick(0, 0), 0)
            .unwrap();
        engine
            .ingest_tick_at_for_test("SHFE.au2602", tick(1, 60), 0)
            .unwrap();
        assert!(!engine.klines.is_empty());
        assert!(!engine.completed_klines.is_empty());

        engine.remove_client(client);

        assert!(engine.klines.is_empty());
        assert!(engine.completed_klines.is_empty());
    }

    #[test]
    fn matching_symbol_and_duration_share_synthesis_across_view_widths() {
        let mut engine = RelayEngine::new_memory_only(16, 4);
        engine
            .handle_command(ClientId::new(1), chart_with_view_width("SHFE.au2602", 16))
            .unwrap();
        engine
            .ingest_tick_at_for_test("SHFE.au2602", tick(0, 0), 0)
            .unwrap();
        engine
            .ingest_tick_at_for_test("SHFE.au2602", tick(1, 60), 0)
            .unwrap();

        let replay = engine
            .handle_command(ClientId::new(2), chart_with_view_width("SHFE.au2602", 256))
            .unwrap();

        assert_eq!(engine.klines.len(), 1);
        assert!(
            replay
                .iter()
                .any(|frame| { frame.payload["data"][0].get("klines").is_some() })
        );
    }
}
