use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::NaiveDate;
use tqsdk_core::Tick;
use tqsdk_data::{
    BacktestTickCache, BacktestTickFillReport, DataError,
    backtest_tick_trading_day_for_timestamp_ns,
};

use crate::{Result, data_validation};

const REMOTE_TICK_DATA_LENGTH: usize = 10_000;
const REMOTE_FILL_END_TOLERANCE_NS: i64 = 1_000_000_000;
const REMOTE_STEP_POLL_TIMEOUT: Duration = Duration::from_secs(5);
const REMOTE_FILL_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
// A productive historical fill may legitimately take longer than a fixed wall-clock limit.
// Stalled streams are guarded by REMOTE_FILL_IDLE_TIMEOUT instead.
const REMOTE_FILL_BATCH_TIMEOUT: Duration = Duration::ZERO;
const REMOTE_FILL_SYMBOL_BATCH_SIZE: usize = 1;
const REMOTE_FILL_SYMBOL_BATCH_SIZE_MAX: usize = 4;
const REMOTE_FILL_SYMBOL_CONCURRENCY: usize = 2;
const REMOTE_FILL_SYMBOL_CONCURRENCY_MAX: usize = 4;
const REMOTE_CONNECT_RETRY_ATTEMPTS: usize = 5;
const REMOTE_TICK_WRITE_BUFFER_ROWS: usize = 8_192;
const REMOTE_FILL_TELEMETRY_INTERVAL: Duration = Duration::from_millis(500);

/// Typed configuration for remote historical tick cache fills.
///
/// [`Self::from_environment`] preserves the SDK's existing environment-based
/// defaults. An explicit builder configuration takes precedence over those
/// values and is scoped to that one backtest operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BacktestRemoteFillConfig {
    pub symbol_batch_size: usize,
    pub symbol_concurrency: usize,
    pub idle_timeout: Duration,
    pub batch_timeout: Option<Duration>,
    pub slice: Option<Duration>,
    pub allow_empty_idle: bool,
}

impl Default for BacktestRemoteFillConfig {
    fn default() -> Self {
        Self {
            symbol_batch_size: REMOTE_FILL_SYMBOL_BATCH_SIZE,
            symbol_concurrency: REMOTE_FILL_SYMBOL_CONCURRENCY,
            idle_timeout: REMOTE_FILL_IDLE_TIMEOUT,
            batch_timeout: None,
            slice: None,
            allow_empty_idle: false,
        }
    }
}

impl BacktestRemoteFillConfig {
    /// Resolve the legacy `TQSDK_REMOTE_FILL_*` environment variables.
    #[must_use]
    pub fn from_environment() -> Self {
        let batch_timeout = parse_remote_fill_batch_timeout(
            std::env::var("TQSDK_REMOTE_FILL_BATCH_TIMEOUT_SECS")
                .ok()
                .as_deref(),
        );
        let slice_ns = parse_remote_fill_slice_ns(
            std::env::var("TQSDK_REMOTE_FILL_SLICE_SECS")
                .ok()
                .as_deref(),
        );
        Self {
            symbol_batch_size: parse_remote_fill_symbol_batch_size(
                std::env::var("TQSDK_REMOTE_FILL_SYMBOL_BATCH_SIZE")
                    .ok()
                    .as_deref(),
            ),
            symbol_concurrency: parse_remote_fill_symbol_concurrency(
                std::env::var("TQSDK_REMOTE_FILL_SYMBOL_CONCURRENCY")
                    .ok()
                    .as_deref(),
            ),
            idle_timeout: parse_remote_fill_idle_timeout(
                std::env::var("TQSDK_REMOTE_FILL_IDLE_TIMEOUT_SECS")
                    .ok()
                    .as_deref(),
            ),
            batch_timeout: (!batch_timeout.is_zero()).then_some(batch_timeout),
            slice: slice_ns
                .and_then(|value| u64::try_from(value).ok())
                .map(Duration::from_nanos),
            allow_empty_idle: parse_remote_fill_allow_empty_idle(
                std::env::var("TQSDK_REMOTE_FILL_ALLOW_EMPTY_IDLE")
                    .ok()
                    .as_deref(),
            ),
        }
        .normalized()
    }

    #[must_use]
    pub fn with_symbol_batch_size(mut self, value: usize) -> Self {
        self.symbol_batch_size = value;
        self.normalized()
    }

    #[must_use]
    pub fn with_symbol_concurrency(mut self, value: usize) -> Self {
        self.symbol_concurrency = value;
        self.normalized()
    }

    #[must_use]
    pub fn with_idle_timeout(mut self, value: Duration) -> Self {
        self.idle_timeout = value;
        self.normalized()
    }

    #[must_use]
    pub fn with_batch_timeout(mut self, value: Option<Duration>) -> Self {
        self.batch_timeout = value;
        self.normalized()
    }

    #[must_use]
    pub fn with_slice(mut self, value: Option<Duration>) -> Self {
        self.slice = value;
        self.normalized()
    }

    #[must_use]
    pub fn with_allow_empty_idle(mut self, value: bool) -> Self {
        self.allow_empty_idle = value;
        self
    }

    fn normalized(mut self) -> Self {
        self.symbol_batch_size = normalize_symbol_batch_size(self.symbol_batch_size);
        self.symbol_concurrency = normalize_symbol_concurrency(self.symbol_concurrency);
        if self.idle_timeout.is_zero() {
            self.idle_timeout = REMOTE_FILL_IDLE_TIMEOUT;
        }
        self.batch_timeout = self.batch_timeout.filter(|timeout| !timeout.is_zero());
        self.slice = self.slice.filter(|slice| !slice.is_zero());
        self
    }

    fn slice_ns(self) -> Option<i64> {
        self.slice
            .and_then(|slice| i64::try_from(slice.as_nanos()).ok())
            .filter(|slice| *slice > 0)
    }
}

/// Low-frequency lifecycle updates emitted by a configured remote cache fill.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BacktestRemoteFillProgress {
    FillStarted {
        requested_symbols: usize,
        total_batches: usize,
        symbol_batch_size: usize,
        symbol_concurrency: usize,
        batch_timeout: Option<Duration>,
    },
    BatchStarted {
        batch_number: usize,
        total_batches: usize,
        pending_batches: usize,
        active_batches: usize,
        requested_range: (i64, i64),
        symbols: Vec<String>,
    },
    TickObserved {
        symbol: String,
        trading_day: NaiveDate,
        accepted_rows: usize,
    },
    BatchFinished {
        batch_number: usize,
        total_batches: usize,
        completed_batches: usize,
        requested_range: (i64, i64),
        symbols: Vec<String>,
        elapsed: Duration,
        rows: usize,
    },
    BatchFailed {
        batch_number: usize,
        total_batches: usize,
        requested_range: (i64, i64),
        symbols: Vec<String>,
        error: String,
    },
}

/// Synchronous observer for [`BacktestRemoteFillProgress`] events.
///
/// Handlers run only when explicitly configured and should return quickly.
pub type BacktestRemoteFillProgressHandler =
    Arc<dyn Fn(&BacktestRemoteFillProgress) + Send + Sync + 'static>;

/// Immutable physical-cache planning details for one remote fill operation.
///
/// A logical symbol can resolve to multiple physical symbols, for example when
/// a main-contract request crosses an underlying roll. The ranges here retain
/// that physical ownership so observers do not need to repeat planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteFillPlanSymbol {
    physical_symbol: String,
    requested_ranges: Vec<(i64, i64)>,
    missing_ranges: Vec<(i64, i64)>,
}

impl RemoteFillPlanSymbol {
    pub(crate) fn new(
        physical_symbol: String,
        requested_ranges: Vec<(i64, i64)>,
        missing_ranges: Vec<(i64, i64)>,
    ) -> Self {
        Self {
            physical_symbol,
            requested_ranges,
            missing_ranges,
        }
    }

    #[must_use]
    pub fn physical_symbol(&self) -> &str {
        &self.physical_symbol
    }

    #[must_use]
    pub fn requested_ranges(&self) -> &[(i64, i64)] {
        &self.requested_ranges
    }

    #[must_use]
    pub fn missing_ranges(&self) -> &[(i64, i64)] {
        &self.missing_ranges
    }

    #[must_use]
    pub fn requires_remote_fill(&self) -> bool {
        !self.missing_ranges.is_empty()
    }
}

/// Fully resolved cache-fill plan emitted before a remote request is started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteFillPlan {
    requested_range: (i64, i64),
    logical_symbols: Vec<String>,
    physical_symbols: Vec<RemoteFillPlanSymbol>,
    logical_batches: usize,
}

impl RemoteFillPlan {
    pub(crate) fn new(
        requested_range: (i64, i64),
        logical_symbols: Vec<String>,
        physical_symbols: Vec<RemoteFillPlanSymbol>,
        logical_batches: usize,
    ) -> Self {
        Self {
            requested_range,
            logical_symbols,
            physical_symbols,
            logical_batches,
        }
    }

    #[must_use]
    pub fn requested_range(&self) -> (i64, i64) {
        self.requested_range
    }

    #[must_use]
    pub fn logical_symbols(&self) -> &[String] {
        &self.logical_symbols
    }

    #[must_use]
    pub fn physical_symbols(&self) -> &[RemoteFillPlanSymbol] {
        &self.physical_symbols
    }

    #[must_use]
    pub fn logical_batches(&self) -> usize {
        self.logical_batches
    }

    #[must_use]
    pub fn requires_remote_fill(&self) -> bool {
        self.physical_symbols
            .iter()
            .any(RemoteFillPlanSymbol::requires_remote_fill)
    }
}

/// Lifecycle phase for a remote cache-fill telemetry snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BacktestRemoteFillPhase {
    PlanReady,
    Started,
    Streaming,
    Retrying,
    SplitFallback,
    Finished,
    Failed,
    Cancelled,
}

/// Immutable, low-overhead remote cache-fill telemetry snapshot.
///
/// The callback configured through
/// [`crate::BacktestBuilder::on_remote_fill_telemetry`] runs on the remote
/// fill path. It must not perform terminal I/O or otherwise block that path.
/// Streaming updates are rate-limited per active physical symbol; lifecycle
/// transitions are emitted immediately.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BacktestRemoteFillTelemetry {
    phase: BacktestRemoteFillPhase,
    plan: Option<RemoteFillPlan>,
    logical_batch_id: Option<usize>,
    attempt: usize,
    physical_symbol: Option<String>,
    requested_range: Option<(i64, i64)>,
    accepted_rows: usize,
    latest_cursor_ns: Option<i64>,
    elapsed: Duration,
    error: Option<String>,
}

impl BacktestRemoteFillTelemetry {
    pub(crate) fn plan_ready(plan: RemoteFillPlan) -> Self {
        Self {
            phase: BacktestRemoteFillPhase::PlanReady,
            plan: Some(plan),
            logical_batch_id: None,
            attempt: 0,
            physical_symbol: None,
            requested_range: None,
            accepted_rows: 0,
            latest_cursor_ns: None,
            elapsed: Duration::ZERO,
            error: None,
        }
    }

    fn lifecycle(update: RemoteFillTelemetryUpdate) -> Self {
        Self {
            phase: update.phase,
            plan: None,
            logical_batch_id: Some(update.logical_batch_id),
            attempt: update.attempt,
            physical_symbol: Some(update.physical_symbol),
            requested_range: Some(update.requested_range),
            accepted_rows: update.accepted_rows,
            latest_cursor_ns: update.latest_cursor_ns,
            elapsed: update.elapsed,
            error: update.error,
        }
    }

    #[must_use]
    pub fn phase(&self) -> BacktestRemoteFillPhase {
        self.phase
    }

    #[must_use]
    pub fn plan(&self) -> Option<&RemoteFillPlan> {
        self.plan.as_ref()
    }

    #[must_use]
    pub fn logical_batch_id(&self) -> Option<usize> {
        self.logical_batch_id
    }

    #[must_use]
    pub fn attempt(&self) -> usize {
        self.attempt
    }

    #[must_use]
    pub fn physical_symbol(&self) -> Option<&str> {
        self.physical_symbol.as_deref()
    }

    #[must_use]
    pub fn requested_range(&self) -> Option<(i64, i64)> {
        self.requested_range
    }

    /// Rows accepted by the current physical stream attempt.
    #[must_use]
    pub fn accepted_rows(&self) -> usize {
        self.accepted_rows
    }

    #[must_use]
    pub fn latest_cursor_ns(&self) -> Option<i64> {
        self.latest_cursor_ns
    }

    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.elapsed
    }

    #[must_use]
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }
}

/// Synchronous observer for [`BacktestRemoteFillTelemetry`] snapshots.
pub type BacktestRemoteFillTelemetryHandler =
    Arc<dyn Fn(&BacktestRemoteFillTelemetry) + Send + Sync + 'static>;

struct RemoteFillTelemetryUpdate {
    phase: BacktestRemoteFillPhase,
    logical_batch_id: usize,
    attempt: usize,
    physical_symbol: String,
    requested_range: (i64, i64),
    accepted_rows: usize,
    latest_cursor_ns: Option<i64>,
    elapsed: Duration,
    error: Option<String>,
}

/// Cooperative cancellation handle for a remote cache fill.
///
/// Cancellation flushes accepted partial tick rows but intentionally does not
/// commit coverage for the interrupted range.
#[derive(Clone, Default)]
pub struct BacktestRemoteFillCancellation {
    cancelled: Arc<AtomicBool>,
}

impl BacktestRemoteFillCancellation {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

#[derive(Clone)]
pub(crate) struct RemoteBacktestFillRuntime {
    config: BacktestRemoteFillConfig,
    progress: Option<BacktestRemoteFillProgressHandler>,
    telemetry: Option<BacktestRemoteFillTelemetryHandler>,
    cancellation: Option<BacktestRemoteFillCancellation>,
}

impl RemoteBacktestFillRuntime {
    pub(crate) fn new(
        config: Option<BacktestRemoteFillConfig>,
        progress: Option<BacktestRemoteFillProgressHandler>,
        telemetry: Option<BacktestRemoteFillTelemetryHandler>,
        cancellation: Option<BacktestRemoteFillCancellation>,
    ) -> Self {
        Self {
            config: config
                .unwrap_or_else(BacktestRemoteFillConfig::from_environment)
                .normalized(),
            progress,
            telemetry,
            cancellation,
        }
    }

    fn emit(&self, event: BacktestRemoteFillProgress) {
        if let Some(progress) = &self.progress {
            progress(&event);
        }
    }

    pub(crate) fn emit_plan(&self, plan: RemoteFillPlan) {
        self.emit_telemetry(BacktestRemoteFillTelemetry::plan_ready(plan));
    }

    pub(crate) fn config(&self) -> BacktestRemoteFillConfig {
        self.config
    }

    fn emit_telemetry(&self, event: BacktestRemoteFillTelemetry) {
        if let Some(telemetry) = &self.telemetry {
            telemetry(&event);
        }
    }

    fn is_cancelled(&self) -> bool {
        self.cancellation
            .as_ref()
            .is_some_and(BacktestRemoteFillCancellation::is_cancelled)
    }

    fn has_progress_handler(&self) -> bool {
        self.progress.is_some()
    }

    fn has_telemetry_handler(&self) -> bool {
        self.telemetry.is_some()
    }
}

pub(crate) struct RemoteBacktestCachingStream {
    api: tqsdk_wait::TqApi,
    handles: BTreeMap<String, tqsdk_wait::TickHandle>,
    cache: BacktestTickCache,
    fills: BTreeMap<String, RemoteTickFillState>,
    write_buffer: RemoteTickWriteBuffer,
    range_start_ns: i64,
    range_end_ns: i64,
    accepted_rows_by_symbol: BTreeMap<String, usize>,
    reported_trading_days: BTreeMap<String, NaiveDate>,
    latest_cursor_by_symbol: BTreeMap<String, i64>,
    last_telemetry_by_symbol: BTreeMap<String, tokio::time::Instant>,
    last_progress: tokio::time::Instant,
    telemetry_context: RemoteFillTelemetryContext,
    runtime: RemoteBacktestFillRuntime,
    finalized: bool,
}

pub(crate) struct RemoteBacktestCacheFillReport {
    pub(crate) rows_by_symbol: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteBacktestCacheFillRequest {
    pub(crate) symbol: String,
    pub(crate) start_ns: i64,
    pub(crate) end_ns: i64,
}

impl RemoteBacktestCacheFillRequest {
    pub(crate) fn new(symbol: impl Into<String>, start_ns: i64, end_ns: i64) -> Self {
        Self {
            symbol: symbol.into(),
            start_ns,
            end_ns,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemoteFillBatch {
    start_ns: i64,
    end_ns: i64,
    symbols: Vec<String>,
}

struct RemoteFillBatchTaskReport {
    batch_index: usize,
    symbols: Vec<String>,
    start_ns: i64,
    end_ns: i64,
    elapsed: Duration,
    fill_report: RemoteBacktestCacheFillReport,
}

struct RemoteFillBatchTask {
    batch_index: usize,
    total_batches: usize,
    batch_timeout: Option<Duration>,
    user: String,
    pass: String,
    batch: RemoteFillBatch,
    cache: BacktestTickCache,
    runtime: RemoteBacktestFillRuntime,
}

#[derive(Clone)]
struct RemoteFillTelemetryContext {
    logical_batch_id: usize,
    attempt: usize,
    phase: BacktestRemoteFillPhase,
    started_at: tokio::time::Instant,
}

impl RemoteFillTelemetryContext {
    fn new(logical_batch_id: usize) -> Self {
        Self {
            logical_batch_id,
            attempt: 1,
            phase: BacktestRemoteFillPhase::Started,
            started_at: tokio::time::Instant::now(),
        }
    }

    fn with_attempt(&self, attempt: usize) -> Self {
        Self {
            attempt,
            ..self.clone()
        }
    }

    fn with_phase(&self, phase: BacktestRemoteFillPhase) -> Self {
        Self {
            phase,
            ..self.clone()
        }
    }
}

#[derive(Clone)]
struct RemoteFillRequest {
    user: String,
    pass: String,
    start_ns: i64,
    end_ns: i64,
    symbols: Vec<String>,
    cache: BacktestTickCache,
    runtime: RemoteBacktestFillRuntime,
    telemetry_context: RemoteFillTelemetryContext,
}

fn emit_telemetry_for_symbols(
    runtime: &RemoteBacktestFillRuntime,
    context: &RemoteFillTelemetryContext,
    symbols: &[String],
    requested_range: (i64, i64),
    accepted_rows: usize,
    latest_cursor_ns: Option<i64>,
    error: Option<String>,
) {
    if !runtime.has_telemetry_handler() {
        return;
    }
    for symbol in symbols {
        runtime.emit_telemetry(BacktestRemoteFillTelemetry::lifecycle(
            RemoteFillTelemetryUpdate {
                phase: context.phase,
                logical_batch_id: context.logical_batch_id,
                attempt: context.attempt,
                physical_symbol: symbol.clone(),
                requested_range,
                accepted_rows,
                latest_cursor_ns,
                elapsed: context.started_at.elapsed(),
                error: error.clone(),
            },
        ));
    }
}

#[derive(Debug, Clone, Copy)]
enum FinalizeMode {
    Strict,
    Idle,
}

/// Streaming validation state for one remote tick range.
///
/// The remote fill path only needs id continuity and range endpoints to commit
/// coverage. Retaining every `Tick` duplicates the on-disk write buffer and
/// makes a dense multi-day fill grow linearly in memory.
#[derive(Debug, Clone)]
struct RemoteTickFillState {
    symbol: String,
    range_start_ns: i64,
    range_end_ns: i64,
    // Inclusive id intervals. A normal ordered stream retains one entry.
    id_intervals: BTreeMap<i64, i64>,
    unique_rows: usize,
    first_id: Option<(i64, i64)>,
    last_id: Option<(i64, i64)>,
}

impl RemoteTickFillState {
    fn new(symbol: impl Into<String>, range_start_ns: i64, range_end_ns: i64) -> Self {
        Self {
            symbol: symbol.into(),
            range_start_ns,
            range_end_ns,
            id_intervals: BTreeMap::new(),
            unique_rows: 0,
            first_id: None,
            last_id: None,
        }
    }

    fn push(&mut self, row: &Tick) -> bool {
        if row.datetime < self.range_start_ns || row.datetime >= self.range_end_ns {
            return false;
        }
        if self.contains_id(row.id) {
            self.update_boundary_datetime(row);
            return false;
        }

        self.insert_id(row.id);
        self.unique_rows = self.unique_rows.saturating_add(1);
        self.update_boundary_datetime(row);
        true
    }

    fn finish(&self, end_tolerance_ns: i64) -> BacktestTickFillReport {
        self.finish_inner(end_tolerance_ns, false)
    }

    fn finish_after_idle(&self, end_tolerance_ns: i64) -> BacktestTickFillReport {
        self.finish_inner(end_tolerance_ns, true)
    }

    fn finish_inner(&self, end_tolerance_ns: i64, allow_idle_tail: bool) -> BacktestTickFillReport {
        let id_range = self
            .first_id
            .zip(self.last_id)
            .map(|(first, last)| (first.0, last.0));
        let first_datetime_ns = self.first_id.map(|(_, datetime)| datetime);
        let last_datetime_ns = self.last_id.map(|(_, datetime)| datetime);
        let mut complete = self.first_id.is_some() || allow_idle_tail;
        let mut gap_summary = None;
        if let Some((first_id, last_id)) = id_range {
            let expected = last_id.saturating_sub(first_id).saturating_add(1);
            if expected != self.unique_rows as i64 || self.id_intervals.len() != 1 {
                complete = false;
                gap_summary = Some(format!(
                    "tick id range {first_id}..={last_id} contains {} unique rows",
                    self.unique_rows
                ));
            }
        } else if !allow_idle_tail {
            complete = false;
        }
        if !allow_idle_tail
            && last_datetime_ns
                .is_none_or(|last_ns| last_ns < self.range_end_ns.saturating_sub(end_tolerance_ns))
        {
            complete = false;
        }
        BacktestTickFillReport {
            symbol: self.symbol.clone(),
            requested_range: (self.range_start_ns, self.range_end_ns),
            unique_rows: self.unique_rows,
            id_range,
            first_datetime_ns,
            last_datetime_ns,
            complete,
            gap_summary,
        }
    }

    fn contains_id(&self, id: i64) -> bool {
        self.id_intervals
            .range(..=id)
            .next_back()
            .is_some_and(|(_, end)| id <= *end)
    }

    fn insert_id(&mut self, id: i64) {
        let left = self
            .id_intervals
            .range(..=id)
            .next_back()
            .map(|(&start, &end)| (start, end));
        let right = self
            .id_intervals
            .range(id..)
            .next()
            .map(|(&start, &end)| (start, end));
        let joins_left = left.is_some_and(|(_, end)| end.checked_add(1) == Some(id));
        let joins_right = right.is_some_and(|(start, _)| id.checked_add(1) == Some(start));

        match (joins_left, joins_right) {
            (true, true) => {
                let (left_start, _) = left.expect("left interval must exist");
                let (right_start, right_end) = right.expect("right interval must exist");
                self.id_intervals.insert(left_start, right_end);
                self.id_intervals.remove(&right_start);
            }
            (true, false) => {
                let (left_start, _) = left.expect("left interval must exist");
                self.id_intervals.insert(left_start, id);
            }
            (false, true) => {
                let (right_start, right_end) = right.expect("right interval must exist");
                self.id_intervals.remove(&right_start);
                self.id_intervals.insert(id, right_end);
            }
            (false, false) => {
                self.id_intervals.insert(id, id);
            }
        }
    }

    fn update_boundary_datetime(&mut self, row: &Tick) {
        if self.first_id.is_none_or(|(id, _)| row.id <= id) {
            self.first_id = Some((row.id, row.datetime));
        }
        if self.last_id.is_none_or(|(id, _)| row.id >= id) {
            self.last_id = Some((row.id, row.datetime));
        }
    }
}

#[derive(Debug)]
struct RemoteTickWriteBuffer {
    threshold_rows: usize,
    rows_by_symbol: BTreeMap<String, Vec<Tick>>,
}

impl Default for RemoteTickWriteBuffer {
    fn default() -> Self {
        Self::new(REMOTE_TICK_WRITE_BUFFER_ROWS)
    }
}

impl RemoteTickWriteBuffer {
    fn new(threshold_rows: usize) -> Self {
        Self {
            threshold_rows: threshold_rows.max(1),
            rows_by_symbol: BTreeMap::new(),
        }
    }

    fn push_rows(
        &mut self,
        cache: &BacktestTickCache,
        symbol: &str,
        rows: Vec<Tick>,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let buffered_rows = {
            let buffer = self.rows_by_symbol.entry(symbol.to_string()).or_default();
            buffer.extend(rows);
            buffer.len()
        };
        if buffered_rows >= self.threshold_rows {
            self.flush_symbol(cache, symbol)?;
        }
        Ok(())
    }

    fn flush_symbol(&mut self, cache: &BacktestTickCache, symbol: &str) -> Result<()> {
        let Some(rows) = self.rows_by_symbol.remove(symbol) else {
            return Ok(());
        };
        if rows.is_empty() {
            return Ok(());
        }
        cache.append_partial_ticks(symbol, rows)?;
        Ok(())
    }

    fn flush_all(&mut self, cache: &BacktestTickCache) -> Result<()> {
        let symbols = self.rows_by_symbol.keys().cloned().collect::<Vec<_>>();
        for symbol in symbols {
            self.flush_symbol(cache, symbol.as_str())?;
        }
        Ok(())
    }
}

pub(crate) async fn fill_backtest_tick_cache(
    user: String,
    pass: String,
    requests: Vec<RemoteBacktestCacheFillRequest>,
    cache: BacktestTickCache,
    runtime: RemoteBacktestFillRuntime,
) -> Result<RemoteBacktestCacheFillReport> {
    let requested_symbol_count = requests
        .iter()
        .map(|request| request.symbol.as_str())
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let config = runtime.config;
    let symbol_batch_size = config.symbol_batch_size;
    let mut pending_batches = remote_fill_batches(requests, symbol_batch_size)?;
    let max_concurrency = config.symbol_concurrency;
    let batch_timeout = config.batch_timeout;
    let total_batches = pending_batches.len();
    let mut next_batch_index = 0usize;
    let mut completed_batches = 0usize;
    let mut tasks = tokio::task::JoinSet::new();
    let mut rows_by_symbol = BTreeMap::new();
    let mut batch_errors = Vec::new();
    remote_fill_progress(format_args!(
        "event=fill_start symbols={requested_symbol_count} batches={total_batches} \
         batch_size={symbol_batch_size} concurrency={max_concurrency} \
         batch_timeout_s={}",
        batch_timeout
            .map(|timeout| timeout.as_secs().to_string())
            .unwrap_or_else(|| "disabled".to_string())
    ));
    runtime.emit(BacktestRemoteFillProgress::FillStarted {
        requested_symbols: requested_symbol_count,
        total_batches,
        symbol_batch_size,
        symbol_concurrency: max_concurrency,
        batch_timeout,
    });

    while !pending_batches.is_empty() || !tasks.is_empty() {
        while !runtime.is_cancelled() && tasks.len() < max_concurrency {
            let Some(batch) = pending_batches.pop_front() else {
                break;
            };
            let batch_index = next_batch_index;
            next_batch_index = next_batch_index.saturating_add(1);
            remote_fill_progress(format_args!(
                "event=batch_start batch={} total_batches={total_batches} pending={} active={} \
                 range=[{}, {}) symbols={}",
                batch_index + 1,
                pending_batches.len(),
                tasks.len() + 1,
                batch.start_ns,
                batch.end_ns,
                batch.symbols.join(",")
            ));
            runtime.emit(BacktestRemoteFillProgress::BatchStarted {
                batch_number: batch_index + 1,
                total_batches,
                pending_batches: pending_batches.len(),
                active_batches: tasks.len() + 1,
                requested_range: (batch.start_ns, batch.end_ns),
                symbols: batch.symbols.clone(),
            });
            tasks.spawn(fill_backtest_tick_cache_symbol_batch_timed(
                RemoteFillBatchTask {
                    batch_index,
                    total_batches,
                    batch_timeout,
                    user: user.clone(),
                    pass: pass.clone(),
                    batch,
                    cache: cache.clone(),
                    runtime: runtime.clone(),
                },
            ));
        }

        let Some(result) = tasks.join_next().await else {
            if runtime.is_cancelled() {
                return Err(remote_fill_cancelled_error());
            }
            continue;
        };
        let task_report = match result {
            Ok(Ok(task_report)) => task_report,
            Ok(Err(error)) => {
                remote_fill_progress(format_args!("event=batch_error error={error}"));
                batch_errors.push(error.to_string());
                continue;
            }
            Err(error) => {
                let error = format!("remote backtest cache fill task failed: {error}");
                remote_fill_progress(format_args!("event=batch_error error={error}"));
                batch_errors.push(error);
                continue;
            }
        };
        completed_batches = completed_batches.saturating_add(1);
        let batch_rows = task_report
            .fill_report
            .rows_by_symbol
            .values()
            .copied()
            .sum::<usize>();
        remote_fill_progress(format_args!(
            "event=batch_done batch={} total_batches={total_batches} completed={completed_batches} \
             elapsed_ms={} range=[{}, {}) symbols={} rows={batch_rows}",
            task_report.batch_index + 1,
            task_report.elapsed.as_millis(),
            task_report.start_ns,
            task_report.end_ns,
            task_report.symbols.join(",")
        ));
        runtime.emit(BacktestRemoteFillProgress::BatchFinished {
            batch_number: task_report.batch_index + 1,
            total_batches,
            completed_batches,
            requested_range: (task_report.start_ns, task_report.end_ns),
            symbols: task_report.symbols.clone(),
            elapsed: task_report.elapsed,
            rows: batch_rows,
        });
        for (symbol, rows) in task_report.fill_report.rows_by_symbol {
            *rows_by_symbol.entry(symbol).or_insert(0) += rows;
        }
    }
    if runtime.is_cancelled() {
        return Err(remote_fill_cancelled_error());
    }
    if !batch_errors.is_empty() {
        return Err(data_validation(format!(
            "remote backtest cache fill completed {completed_batches}/{total_batches} batches; \
             {} batch(es) failed: {}",
            batch_errors.len(),
            batch_errors.join(" | ")
        )));
    }
    let accepted_rows_total = rows_by_symbol.values().copied().sum();
    if should_reject_empty_remote_fill(
        requested_symbol_count,
        accepted_rows_total,
        config.allow_empty_idle,
    ) {
        return Err(data_validation(format!(
            "remote backtest cache fill completed without accepted ticks for {requested_symbol_count} symbols; refusing to mark complete empty coverage"
        )));
    }
    Ok(RemoteBacktestCacheFillReport { rows_by_symbol })
}

fn remote_fill_batches(
    requests: Vec<RemoteBacktestCacheFillRequest>,
    symbol_batch_size: usize,
) -> Result<VecDeque<RemoteFillBatch>> {
    let symbol_batch_size = symbol_batch_size.max(1);
    let mut by_range: BTreeMap<(i64, i64), Vec<String>> = BTreeMap::new();
    for request in requests {
        if request.symbol.is_empty() {
            return Err(data_validation(
                "remote backtest cache fill symbol is empty",
            ));
        }
        if request.start_ns >= request.end_ns {
            return Err(data_validation(format!(
                "remote backtest cache fill range is invalid for {}: [{}, {})",
                request.symbol, request.start_ns, request.end_ns
            )));
        }
        by_range
            .entry((request.start_ns, request.end_ns))
            .or_default()
            .push(request.symbol);
    }

    let mut batches = VecDeque::new();
    for ((start_ns, end_ns), mut symbols) in by_range {
        symbols.sort();
        symbols.dedup();
        for chunk in symbols.chunks(symbol_batch_size) {
            batches.push_back(RemoteFillBatch {
                start_ns,
                end_ns,
                symbols: chunk.to_vec(),
            });
        }
    }
    Ok(batches)
}

pub(crate) fn remote_fill_logical_batch_count(
    requests: Vec<RemoteBacktestCacheFillRequest>,
    symbol_batch_size: usize,
) -> Result<usize> {
    Ok(remote_fill_batches(requests, symbol_batch_size)?.len())
}

async fn fill_backtest_tick_cache_symbol_batch_timed(
    task: RemoteFillBatchTask,
) -> Result<RemoteFillBatchTaskReport> {
    let RemoteFillBatchTask {
        batch_index,
        total_batches,
        batch_timeout,
        user,
        pass,
        batch,
        cache,
        runtime,
    } = task;
    let started = tokio::time::Instant::now();
    let timeout_symbols = batch.symbols.clone();
    let start_ns = batch.start_ns;
    let end_ns = batch.end_ns;
    let symbols = batch.symbols;
    let telemetry_context = RemoteFillTelemetryContext::new(batch_index + 1);
    emit_telemetry_for_symbols(
        &runtime,
        &telemetry_context,
        &symbols,
        (start_ns, end_ns),
        0,
        None,
        None,
    );
    let fill = fill_backtest_tick_cache_symbol_batch(RemoteFillRequest {
        user,
        pass,
        start_ns,
        end_ns,
        symbols: symbols.clone(),
        cache,
        runtime: runtime.clone(),
        telemetry_context: telemetry_context.clone(),
    });
    let result = match batch_timeout {
        Some(batch_timeout) => match tokio::time::timeout(batch_timeout, fill).await {
            Ok(result) => result,
            Err(_) => Err(data_validation(format!(
                "remote backtest cache fill batch timed out after {}s for {} symbols ({}) \
                     in range [{start_ns}, {end_ns})",
                batch_timeout.as_secs(),
                timeout_symbols.len(),
                timeout_symbols.join(",")
            ))),
        },
        None => fill.await,
    };
    let fill_report = match result {
        Ok(report) => report,
        Err(error) => {
            runtime.emit(BacktestRemoteFillProgress::BatchFailed {
                batch_number: batch_index + 1,
                total_batches,
                requested_range: (start_ns, end_ns),
                symbols: timeout_symbols.clone(),
                error: error.to_string(),
            });
            emit_telemetry_for_symbols(
                &runtime,
                &telemetry_context.with_phase(if runtime.is_cancelled() {
                    BacktestRemoteFillPhase::Cancelled
                } else {
                    BacktestRemoteFillPhase::Failed
                }),
                &timeout_symbols,
                (start_ns, end_ns),
                0,
                None,
                Some(error.to_string()),
            );
            return Err(error);
        }
    };
    let elapsed = started.elapsed();
    Ok(RemoteFillBatchTaskReport {
        batch_index,
        symbols: timeout_symbols,
        start_ns,
        end_ns,
        elapsed,
        fill_report,
    })
}

async fn fill_backtest_tick_cache_symbol_batch(
    request: RemoteFillRequest,
) -> Result<RemoteBacktestCacheFillReport> {
    let result = fill_backtest_tick_cache_symbol_batch_once(request.clone()).await;
    if !matches!(
        result.as_ref(),
        Err(error) if should_split_empty_idle_batch(error, request.symbols.len())
    ) {
        return result;
    }
    if let Err(error) = &result {
        remote_fill_progress(format_args!(
            "event=batch_split symbols={} error={error}",
            request.symbols.join(",")
        ));
        emit_telemetry_for_symbols(
            &request.runtime,
            &request
                .telemetry_context
                .with_phase(BacktestRemoteFillPhase::SplitFallback),
            &request.symbols,
            (request.start_ns, request.end_ns),
            0,
            None,
            Some(error.to_string()),
        );
    }

    let mut rows_by_symbol = BTreeMap::new();
    for symbol in request.symbols.clone() {
        let mut split_request = request.clone();
        split_request.symbols = vec![symbol];
        split_request.telemetry_context = split_request
            .telemetry_context
            .with_phase(BacktestRemoteFillPhase::SplitFallback);
        let fill_report = fill_backtest_tick_cache_symbol_batch_once(split_request).await?;
        for (symbol, rows) in fill_report.rows_by_symbol {
            *rows_by_symbol.entry(symbol).or_insert(0) += rows;
        }
    }

    Ok(RemoteBacktestCacheFillReport { rows_by_symbol })
}

async fn fill_backtest_tick_cache_symbol_batch_once(
    request: RemoteFillRequest,
) -> Result<RemoteBacktestCacheFillReport> {
    let mut attempt = 1usize;
    loop {
        let mut attempt_request = request.clone();
        attempt_request.telemetry_context = attempt_request.telemetry_context.with_attempt(attempt);
        let result = fill_backtest_tick_cache_symbol_batch_attempt(attempt_request).await;
        match result {
            Ok(report) => return Ok(report),
            Err(error)
                if attempt < REMOTE_CONNECT_RETRY_ATTEMPTS
                    && should_retry_remote_fill_attempt_error(&error, request.symbols.len()) =>
            {
                remote_fill_progress(format_args!(
                    "event=batch_attempt_retry attempt={} next_attempt={} symbols={} error={error}",
                    attempt,
                    attempt + 1,
                    request.symbols.join(",")
                ));
                emit_telemetry_for_symbols(
                    &request.runtime,
                    &request
                        .telemetry_context
                        .with_attempt(attempt.saturating_add(1))
                        .with_phase(BacktestRemoteFillPhase::Retrying),
                    &request.symbols,
                    (request.start_ns, request.end_ns),
                    0,
                    None,
                    Some(error.to_string()),
                );
                tokio::time::sleep(remote_connect_retry_delay(attempt)).await;
                attempt = attempt.saturating_add(1);
            }
            Err(error) => return Err(error),
        }
    }
}

async fn fill_backtest_tick_cache_symbol_batch_attempt(
    request: RemoteFillRequest,
) -> Result<RemoteBacktestCacheFillReport> {
    let mut rows_by_symbol = BTreeMap::new();
    for (slice_start_ns, slice_end_ns) in
        remote_fill_ranges(request.start_ns, request.end_ns, request.runtime.config)
    {
        let mut slice_request = request.clone();
        slice_request.start_ns = slice_start_ns;
        slice_request.end_ns = slice_end_ns;
        slice_request.telemetry_context = slice_request
            .telemetry_context
            .with_phase(BacktestRemoteFillPhase::Streaming);
        let mut stream = connect_remote_backtest_caching_stream(slice_request)
            .await
            .map_err(|error| remote_slice_error(slice_start_ns, slice_end_ns, error))?;
        let slice_report = stream
            .fill_cache()
            .await
            .map_err(|error| remote_slice_error(slice_start_ns, slice_end_ns, error))?;
        for (symbol, rows) in slice_report.rows_by_symbol {
            *rows_by_symbol.entry(symbol).or_insert(0) += rows;
        }
    }
    for symbol in &request.symbols {
        request.cache.compact_symbol_ticks(symbol)?;
    }
    Ok(RemoteBacktestCacheFillReport { rows_by_symbol })
}

async fn connect_remote_backtest_caching_stream(
    request: RemoteFillRequest,
) -> Result<RemoteBacktestCachingStream> {
    let mut attempt = 1usize;
    loop {
        let mut connect_request = request.clone();
        connect_request.telemetry_context = connect_request.telemetry_context.with_attempt(
            request
                .telemetry_context
                .attempt
                .saturating_add(attempt.saturating_sub(1)),
        );
        let result = RemoteBacktestCachingStream::connect(connect_request).await;
        match result {
            Ok(stream) => return Ok(stream),
            Err(error)
                if attempt < REMOTE_CONNECT_RETRY_ATTEMPTS
                    && should_retry_remote_connect_error(&error) =>
            {
                emit_telemetry_for_symbols(
                    &request.runtime,
                    &request
                        .telemetry_context
                        .with_attempt(request.telemetry_context.attempt.saturating_add(attempt))
                        .with_phase(BacktestRemoteFillPhase::Retrying),
                    &request.symbols,
                    (request.start_ns, request.end_ns),
                    0,
                    None,
                    Some(error.to_string()),
                );
                tokio::time::sleep(remote_connect_retry_delay(attempt)).await;
                attempt = attempt.saturating_add(1);
            }
            Err(error) => return Err(error),
        }
    }
}

fn remote_connect_retry_delay(attempt: usize) -> Duration {
    Duration::from_secs((attempt as u64).saturating_mul(2))
}

fn remote_fill_ranges(
    start_ns: i64,
    end_ns: i64,
    config: BacktestRemoteFillConfig,
) -> Vec<(i64, i64)> {
    remote_fill_ranges_for_slice_ns(start_ns, end_ns, config.slice_ns())
}

fn remote_fill_ranges_for_slice_ns(
    start_ns: i64,
    end_ns: i64,
    slice_ns: Option<i64>,
) -> Vec<(i64, i64)> {
    match slice_ns {
        Some(slice_ns) => remote_fill_ranges_with_slice_ns(start_ns, end_ns, slice_ns),
        None => vec![(start_ns, end_ns)],
    }
}

fn remote_fill_ranges_with_slice_ns(start_ns: i64, end_ns: i64, slice_ns: i64) -> Vec<(i64, i64)> {
    let mut ranges = Vec::new();
    let mut cursor = start_ns;
    while cursor < end_ns {
        let next = cursor.saturating_add(slice_ns).min(end_ns);
        ranges.push((cursor, next));
        cursor = next;
    }
    ranges
}

fn remote_slice_error(slice_start_ns: i64, slice_end_ns: i64, error: crate::Error) -> crate::Error {
    data_validation(format!(
        "remote backtest cache fill failed for slice [{slice_start_ns}, {slice_end_ns}): {error}"
    ))
}

fn remote_fill_cancelled_error() -> crate::Error {
    data_validation("remote backtest cache fill cancelled")
}

impl RemoteBacktestCachingStream {
    async fn connect(request: RemoteFillRequest) -> Result<Self> {
        let RemoteFillRequest {
            user,
            pass,
            start_ns,
            end_ns,
            symbols,
            cache,
            runtime,
            telemetry_context,
        } = request;
        let mut api = tqsdk_wait::TqApiBuilder::new(user, pass)
            .futures_backtest(start_ns, end_ns)?
            .backtest_cache_fill_mode()
            .build()
            .await?;
        let mut handles = BTreeMap::new();
        let mut fills = BTreeMap::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        for symbol in symbols {
            let handle = api
                .tick_ready(&symbol, REMOTE_TICK_DATA_LENGTH, Some(deadline))
                .await?;
            fills.insert(
                symbol.clone(),
                RemoteTickFillState::new(symbol.clone(), start_ns, end_ns),
            );
            handles.insert(symbol, handle);
        }
        let active_symbols = handles.keys().cloned().collect::<Vec<_>>();
        emit_telemetry_for_symbols(
            &runtime,
            &telemetry_context,
            &active_symbols,
            (start_ns, end_ns),
            0,
            None,
            None,
        );
        Ok(Self {
            api,
            handles,
            cache,
            fills,
            write_buffer: RemoteTickWriteBuffer::default(),
            range_start_ns: start_ns,
            range_end_ns: end_ns,
            accepted_rows_by_symbol: BTreeMap::new(),
            reported_trading_days: BTreeMap::new(),
            latest_cursor_by_symbol: BTreeMap::new(),
            last_telemetry_by_symbol: BTreeMap::new(),
            last_progress: tokio::time::Instant::now(),
            telemetry_context,
            runtime,
            finalized: false,
        })
    }

    async fn fill_cache(&mut self) -> Result<RemoteBacktestCacheFillReport> {
        loop {
            if self.runtime.is_cancelled() {
                self.write_buffer.flush_all(&self.cache)?;
                self.emit_terminal_telemetry(BacktestRemoteFillPhase::Cancelled, None);
                return Err(remote_fill_cancelled_error());
            }
            if self.fills_complete() {
                self.finalize_cache(FinalizeMode::Strict)?;
                self.emit_terminal_telemetry(BacktestRemoteFillPhase::Finished, None);
                return Ok(self.fill_report());
            }
            if self.all_tick_serials_exhausted() {
                let now_ns = current_unix_time_ns();
                if !should_finalize_idle_after_serial_exhaustion(true, self.range_end_ns, now_ns) {
                    return Err(future_idle_finalize_error(self.range_end_ns, now_ns));
                }
                self.finalize_idle_after_terminal(now_ns)?;
                self.emit_terminal_telemetry(BacktestRemoteFillPhase::Finished, None);
                return Ok(self.fill_report());
            }
            if self.last_progress.elapsed() >= self.runtime.config.idle_timeout {
                if self
                    .poll_remote_step_until(tokio::time::Instant::now())
                    .await?
                {
                    continue;
                }
                let now_ns = current_unix_time_ns();
                self.finalize_idle_after_terminal(now_ns)?;
                self.emit_terminal_telemetry(BacktestRemoteFillPhase::Finished, None);
                return Ok(self.fill_report());
            }

            let deadline = tokio::time::Instant::now() + REMOTE_STEP_POLL_TIMEOUT;
            if !self.poll_remote_step_until(deadline).await? {
                continue;
            }
        }
    }

    async fn poll_remote_step_until(&mut self, deadline: tokio::time::Instant) -> Result<bool> {
        let Some(step) = self.api.step_until(Some(deadline)).await? else {
            return Ok(false);
        };
        self.process_remote_step(&step)?;
        Ok(true)
    }

    fn process_remote_step(&mut self, step: &tqsdk_wait::WaitStep) -> Result<()> {
        let mut made_progress = false;
        let observe_progress = self.runtime.has_progress_handler();
        let observe_telemetry = self.runtime.has_telemetry_handler();
        for (symbol, handle) in &self.handles {
            if !step.is_changing(handle) {
                continue;
            }

            let mut accepted_rows = Vec::new();
            for row in handle.changed_rows(step)? {
                let Some(fill) = self.fills.get_mut(symbol) else {
                    continue;
                };
                if !fill.push(&row) {
                    continue;
                }

                accepted_rows.push(row);
            }

            if !accepted_rows.is_empty() {
                let latest_datetime_ns = (observe_progress || observe_telemetry)
                    .then(|| accepted_rows.iter().map(|row| row.datetime).max())
                    .flatten();
                let accepted_rows_total = {
                    let total = self
                        .accepted_rows_by_symbol
                        .entry(symbol.clone())
                        .or_insert(0);
                    *total = total.saturating_add(accepted_rows.len());
                    *total
                };
                self.write_buffer
                    .push_rows(&self.cache, symbol, accepted_rows)?;
                if let Some(datetime_ns) = latest_datetime_ns {
                    self.latest_cursor_by_symbol
                        .entry(symbol.clone())
                        .and_modify(|cursor| *cursor = (*cursor).max(datetime_ns))
                        .or_insert(datetime_ns);
                    let trading_day = backtest_tick_trading_day_for_timestamp_ns(datetime_ns)?;
                    let day_changed = self.reported_trading_days.get(symbol) != Some(&trading_day);
                    if day_changed {
                        self.reported_trading_days
                            .insert(symbol.clone(), trading_day);
                    }
                    if observe_progress && day_changed {
                        self.runtime.emit(BacktestRemoteFillProgress::TickObserved {
                            symbol: symbol.clone(),
                            trading_day,
                            accepted_rows: accepted_rows_total,
                        });
                    }
                    let now = tokio::time::Instant::now();
                    let should_emit_telemetry = observe_telemetry
                        && (day_changed
                            || self
                                .last_telemetry_by_symbol
                                .get(symbol)
                                .is_none_or(|last| {
                                    now.duration_since(*last) >= REMOTE_FILL_TELEMETRY_INTERVAL
                                }));
                    if should_emit_telemetry {
                        self.last_telemetry_by_symbol.insert(symbol.clone(), now);
                        self.emit_symbol_telemetry(
                            symbol,
                            BacktestRemoteFillPhase::Streaming,
                            accepted_rows_total,
                            Some(datetime_ns),
                            None,
                        );
                    }
                }
                made_progress = true;
            }
        }

        if made_progress {
            self.last_progress = tokio::time::Instant::now();
        }
        Ok(())
    }

    fn emit_symbol_telemetry(
        &self,
        symbol: &str,
        phase: BacktestRemoteFillPhase,
        accepted_rows: usize,
        latest_cursor_ns: Option<i64>,
        error: Option<String>,
    ) {
        if !self.runtime.has_telemetry_handler() {
            return;
        }
        let context = self.telemetry_context.with_phase(phase);
        self.runtime
            .emit_telemetry(BacktestRemoteFillTelemetry::lifecycle(
                RemoteFillTelemetryUpdate {
                    phase: context.phase,
                    logical_batch_id: context.logical_batch_id,
                    attempt: context.attempt,
                    physical_symbol: symbol.to_string(),
                    requested_range: (self.range_start_ns, self.range_end_ns),
                    accepted_rows,
                    latest_cursor_ns,
                    elapsed: context.started_at.elapsed(),
                    error,
                },
            ));
    }

    fn emit_terminal_telemetry(&self, phase: BacktestRemoteFillPhase, error: Option<String>) {
        if !self.runtime.has_telemetry_handler() {
            return;
        }
        for symbol in self.fills.keys() {
            self.emit_symbol_telemetry(
                symbol,
                phase,
                self.accepted_rows_by_symbol
                    .get(symbol)
                    .copied()
                    .unwrap_or_default(),
                self.latest_cursor_by_symbol.get(symbol).copied(),
                error.clone(),
            );
        }
    }

    fn unconfirmed_incomplete_idle_symbols(&self) -> Result<Vec<String>> {
        let mut symbols = Vec::new();
        for (symbol, fill) in &self.fills {
            let report = fill.finish(REMOTE_FILL_END_TOLERANCE_NS);
            if report.complete || report.unique_rows == 0 {
                continue;
            }
            let Some(handle) = self.handles.get(symbol) else {
                symbols.push(symbol.clone());
                continue;
            };
            if self.api.backtest_tick_serial_exhausted(handle) != Some(true) {
                symbols.push(symbol.clone());
            }
        }
        symbols.sort();
        Ok(symbols)
    }

    fn unconfirmed_empty_idle_symbols(&self) -> Result<Vec<String>> {
        let mut symbols = Vec::new();
        for (symbol, fill) in &self.fills {
            if fill
                .finish_after_idle(REMOTE_FILL_END_TOLERANCE_NS)
                .unique_rows
                != 0
            {
                continue;
            }
            let Some(handle) = self.handles.get(symbol) else {
                symbols.push(symbol.clone());
                continue;
            };
            if self.api.backtest_tick_serial_exhausted(handle) != Some(true) {
                symbols.push(symbol.clone());
            }
        }
        symbols.sort();
        Ok(symbols)
    }

    fn fills_complete(&self) -> bool {
        for fill in self.fills.values() {
            if !fill.finish(REMOTE_FILL_END_TOLERANCE_NS).complete {
                return false;
            }
        }
        true
    }

    fn all_tick_serials_exhausted(&self) -> bool {
        !self.handles.is_empty()
            && self
                .handles
                .values()
                .all(|handle| self.api.backtest_tick_serial_exhausted(handle) == Some(true))
    }

    fn finalize_idle_after_terminal(&mut self, now_ns: Option<i64>) -> Result<()> {
        if should_reject_future_idle_finalize(self.range_end_ns, now_ns) {
            return Err(future_idle_finalize_error(self.range_end_ns, now_ns));
        }
        // Closed-session tails can legitimately end before the requested slice end.
        let unconfirmed_incomplete_idle_symbols = self.unconfirmed_incomplete_idle_symbols()?;
        if should_reject_incomplete_idle_finalize(!unconfirmed_incomplete_idle_symbols.is_empty()) {
            return Err(data_validation(format!(
                "remote backtest cache fill idled before tick ranges were confirmed \
                 for {} symbols ({}) in range [{}, {}); refusing to mark complete partial coverage",
                unconfirmed_incomplete_idle_symbols.len(),
                unconfirmed_incomplete_idle_symbols.join(","),
                self.range_start_ns,
                self.range_end_ns
            )));
        }

        let unconfirmed_empty_idle_symbols = self.unconfirmed_empty_idle_symbols()?;
        if should_reject_empty_idle_finalize(
            self.runtime.config.allow_empty_idle,
            !unconfirmed_empty_idle_symbols.is_empty(),
        ) {
            return Err(data_validation(format!(
                "remote backtest cache fill idled before empty tick ranges were confirmed \
                 for {} symbols ({}) in range [{}, {}); refusing to mark complete empty coverage",
                unconfirmed_empty_idle_symbols.len(),
                unconfirmed_empty_idle_symbols.join(","),
                self.range_start_ns,
                self.range_end_ns
            )));
        }
        self.finalize_cache(FinalizeMode::Idle)
    }

    fn fill_report(&self) -> RemoteBacktestCacheFillReport {
        RemoteBacktestCacheFillReport {
            rows_by_symbol: self.accepted_rows_by_symbol.clone(),
        }
    }

    fn finalize_cache(&mut self, mode: FinalizeMode) -> Result<()> {
        if self.finalized {
            return Ok(());
        }
        self.write_buffer.flush_all(&self.cache)?;
        for (symbol, fill) in &self.fills {
            let report = match mode {
                FinalizeMode::Strict => fill.finish(REMOTE_FILL_END_TOLERANCE_NS),
                FinalizeMode::Idle => fill.finish_after_idle(REMOTE_FILL_END_TOLERANCE_NS),
            };
            if !report.complete {
                return Err(data_validation(format!(
                    "incomplete remote backtest cache fill for {symbol}: {:?}",
                    report.gap_summary
                )));
            }
            self.cache.mark_complete(
                symbol,
                report.requested_range.0,
                report.requested_range.1,
                report.unique_rows,
                report.id_range,
            )?;
        }
        self.finalized = true;
        Ok(())
    }
}

fn remote_fill_progress_enabled() -> bool {
    let value = std::env::var("TQSDK_REMOTE_FILL_PROGRESS").ok();
    parse_remote_fill_progress_enabled(value.as_deref())
}

fn remote_fill_progress(args: fmt::Arguments<'_>) {
    if remote_fill_progress_enabled() {
        eprintln!("TQSDK_REMOTE_FILL_PROGRESS {args}");
    }
}

fn parse_remote_fill_idle_timeout(value: Option<&str>) -> Duration {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(REMOTE_FILL_IDLE_TIMEOUT)
}

fn parse_remote_fill_batch_timeout(value: Option<&str>) -> Duration {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(REMOTE_FILL_BATCH_TIMEOUT)
}

fn parse_remote_fill_allow_empty_idle(value: Option<&str>) -> bool {
    matches!(
        value.map(str::trim).map(str::to_ascii_lowercase).as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

fn parse_remote_fill_progress_enabled(value: Option<&str>) -> bool {
    matches!(
        value.map(str::trim).map(str::to_ascii_lowercase).as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

fn parse_remote_fill_symbol_batch_size(value: Option<&str>) -> usize {
    normalize_symbol_batch_size(
        value
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(REMOTE_FILL_SYMBOL_BATCH_SIZE),
    )
}

fn parse_remote_fill_symbol_concurrency(value: Option<&str>) -> usize {
    normalize_symbol_concurrency(
        value
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(REMOTE_FILL_SYMBOL_CONCURRENCY),
    )
}

fn normalize_symbol_batch_size(value: usize) -> usize {
    value.clamp(1, REMOTE_FILL_SYMBOL_BATCH_SIZE_MAX)
}

fn normalize_symbol_concurrency(value: usize) -> usize {
    if value == 0 {
        REMOTE_FILL_SYMBOL_CONCURRENCY
    } else {
        value.min(REMOTE_FILL_SYMBOL_CONCURRENCY_MAX)
    }
}

fn parse_remote_fill_slice_ns(value: Option<&str>) -> Option<i64> {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .and_then(|secs| secs.checked_mul(1_000_000_000))
        .and_then(|ns| i64::try_from(ns).ok())
        .filter(|ns| *ns > 0)
}

fn should_reject_empty_idle_finalize(
    allow_empty_idle: bool,
    has_unconfirmed_empty_idle_symbols: bool,
) -> bool {
    has_unconfirmed_empty_idle_symbols && !allow_empty_idle
}

fn should_reject_incomplete_idle_finalize(has_unconfirmed_incomplete_idle_symbols: bool) -> bool {
    has_unconfirmed_incomplete_idle_symbols
}

fn should_reject_future_idle_finalize(range_end_ns: i64, now_ns: Option<i64>) -> bool {
    now_ns.is_none_or(|now_ns| range_end_ns > now_ns)
}

fn should_finalize_idle_after_serial_exhaustion(
    all_serials_exhausted: bool,
    range_end_ns: i64,
    now_ns: Option<i64>,
) -> bool {
    all_serials_exhausted && !should_reject_future_idle_finalize(range_end_ns, now_ns)
}

fn future_idle_finalize_error(range_end_ns: i64, now_ns: Option<i64>) -> crate::Error {
    data_validation(format!(
        "remote backtest cache fill idled before requested range end {range_end_ns} was \
         reachable by local time {}; refusing to mark complete future coverage",
        now_ns
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    ))
}

fn should_reject_empty_remote_fill(
    symbol_count: usize,
    accepted_rows_total: usize,
    allow_empty_idle: bool,
) -> bool {
    symbol_count > 1 && accepted_rows_total == 0 && !allow_empty_idle
}

fn should_split_empty_idle_batch(error: &crate::Error, symbol_count: usize) -> bool {
    symbol_count > 1
        && matches!(
            error,
            crate::Error::Data(data)
                if matches!(
                    &**data,
                    DataError::Validation(message)
                        if is_remote_fill_idle_error_message(message)
                )
        )
}

fn current_unix_time_ns() -> Option<i64> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_nanos();
    i64::try_from(nanos).ok()
}

fn should_retry_remote_fill_attempt_error(error: &crate::Error, symbol_count: usize) -> bool {
    should_retry_remote_connect_error(error)
        || (symbol_count == 1
            && matches!(
            error,
            crate::Error::Data(data)
                if matches!(
                    &**data,
                    DataError::Validation(message) if is_remote_fill_idle_error_message(message)
                )
            ))
}

fn is_remote_fill_idle_error_message(message: &str) -> bool {
    message.contains("remote backtest cache fill idled without accepted ticks")
        || message
            .contains("remote backtest cache fill idled before empty tick ranges were confirmed")
        || message.contains("remote backtest cache fill idled before tick ranges were confirmed")
}

fn should_retry_remote_connect_error(error: &crate::Error) -> bool {
    matches!(
        error,
        crate::Error::Data(data)
            if matches!(
                &**data,
                DataError::Validation(message)
                    if (message.contains("token request failed")
                        || message.contains("market endpoint request failed"))
                        && message.contains("error sending request")
            )
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        BacktestRemoteFillCancellation, BacktestRemoteFillConfig, BacktestRemoteFillPhase,
        BacktestRemoteFillTelemetry, REMOTE_FILL_BATCH_TIMEOUT, REMOTE_FILL_IDLE_TIMEOUT,
        RemoteBacktestCacheFillRequest, RemoteBacktestFillRuntime, RemoteFillPlan,
        RemoteFillPlanSymbol, RemoteFillTelemetryUpdate, RemoteTickFillState,
        RemoteTickWriteBuffer, parse_remote_fill_allow_empty_idle, parse_remote_fill_batch_timeout,
        parse_remote_fill_idle_timeout, parse_remote_fill_progress_enabled,
        parse_remote_fill_slice_ns, parse_remote_fill_symbol_batch_size,
        parse_remote_fill_symbol_concurrency, remote_fill_batches, remote_fill_ranges_for_slice_ns,
        remote_fill_ranges_with_slice_ns, should_finalize_idle_after_serial_exhaustion,
        should_reject_empty_idle_finalize, should_reject_empty_remote_fill,
        should_reject_future_idle_finalize, should_reject_incomplete_idle_finalize,
        should_retry_remote_connect_error, should_retry_remote_fill_attempt_error,
        should_split_empty_idle_batch,
    };
    use tqsdk_core::Tick;
    use tqsdk_data::{BacktestTickCache, BacktestTickFill, TickDataSeriesRequest};

    #[test]
    fn typed_remote_fill_config_normalizes_direct_cli_overrides() {
        let config = BacktestRemoteFillConfig::default()
            .with_symbol_batch_size(128)
            .with_symbol_concurrency(0)
            .with_idle_timeout(Duration::ZERO)
            .with_batch_timeout(Some(Duration::ZERO))
            .with_slice(Some(Duration::from_secs(2)));

        assert_eq!(config.symbol_batch_size, 4);
        assert_eq!(config.symbol_concurrency, 2);
        assert_eq!(config.idle_timeout, REMOTE_FILL_IDLE_TIMEOUT);
        assert_eq!(config.batch_timeout, None);
        assert_eq!(config.slice_ns(), Some(2_000_000_000));
    }

    #[test]
    fn remote_fill_cancellation_is_shared_with_the_runtime() {
        let cancellation = BacktestRemoteFillCancellation::new();
        let runtime = RemoteBacktestFillRuntime::new(
            Some(BacktestRemoteFillConfig::default()),
            None,
            None,
            Some(cancellation.clone()),
        );

        assert!(!runtime.is_cancelled());
        cancellation.cancel();
        assert!(runtime.is_cancelled());
    }

    #[test]
    fn telemetry_snapshots_expose_plan_and_retry_identity() {
        let plan = RemoteFillPlan::new(
            (1_000, 4_000),
            vec!["KQ.m@SHFE.au".to_string()],
            vec![RemoteFillPlanSymbol::new(
                "SHFE.au2608".to_string(),
                vec![(1_000, 4_000)],
                vec![(2_000, 4_000)],
            )],
            2,
        );
        let ready = BacktestRemoteFillTelemetry::plan_ready(plan.clone());

        assert_eq!(ready.phase(), BacktestRemoteFillPhase::PlanReady);
        assert_eq!(ready.plan(), Some(&plan));
        assert!(ready.logical_batch_id().is_none());
        assert!(ready.plan().unwrap().requires_remote_fill());
        assert_eq!(ready.plan().unwrap().logical_batches(), 2);

        let retry = BacktestRemoteFillTelemetry::lifecycle(RemoteFillTelemetryUpdate {
            phase: BacktestRemoteFillPhase::Retrying,
            logical_batch_id: 2,
            attempt: 3,
            physical_symbol: "SHFE.au2608".to_string(),
            requested_range: (2_000, 4_000),
            accepted_rows: 128,
            latest_cursor_ns: Some(3_000),
            elapsed: Duration::from_secs(5),
            error: Some("temporary failure".to_string()),
        });
        assert_eq!(retry.phase(), BacktestRemoteFillPhase::Retrying);
        assert_eq!(retry.logical_batch_id(), Some(2));
        assert_eq!(retry.attempt(), 3);
        assert_eq!(retry.physical_symbol(), Some("SHFE.au2608"));
        assert_eq!(retry.requested_range(), Some((2_000, 4_000)));
        assert_eq!(retry.accepted_rows(), 128);
        assert_eq!(retry.latest_cursor_ns(), Some(3_000));
        assert_eq!(retry.error(), Some("temporary failure"));
    }

    #[test]
    fn remote_fill_ranges_default_to_single_python_style_backtest_session() {
        let start_ns = 1_781_182_800_000_000_000;
        let end_ns = start_ns + 48 * 60 * 60 * 1_000_000_000;

        let ranges = remote_fill_ranges_for_slice_ns(start_ns, end_ns, None);

        assert_eq!(ranges, vec![(start_ns, end_ns)]);
    }

    #[test]
    fn remote_fill_ranges_can_split_long_requests_for_fallback() {
        let start_ns = 1_781_182_800_000_000_000;
        let two_hours_ns = 2 * 60 * 60 * 1_000_000_000;
        let end_ns = start_ns + 3 * two_hours_ns;

        let ranges = remote_fill_ranges_with_slice_ns(start_ns, end_ns, two_hours_ns);

        assert_eq!(
            ranges,
            vec![
                (start_ns, start_ns + two_hours_ns),
                (start_ns + two_hours_ns, start_ns + 2 * two_hours_ns),
                (start_ns + 2 * two_hours_ns, end_ns),
            ]
        );
    }

    #[test]
    fn remote_fill_batches_group_only_equal_missing_ranges() {
        let requests = vec![
            RemoteBacktestCacheFillRequest::new("SHFE.rb2601", 1_000, 2_000),
            RemoteBacktestCacheFillRequest::new("DCE.m2601", 1_000, 2_000),
            RemoteBacktestCacheFillRequest::new("SHFE.rb2601", 3_000, 4_000),
            RemoteBacktestCacheFillRequest::new("SHFE.rb2601", 1_000, 2_000),
        ];

        let batches = remote_fill_batches(requests, 2)
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].start_ns, 1_000);
        assert_eq!(batches[0].end_ns, 2_000);
        assert_eq!(
            batches[0].symbols,
            vec!["DCE.m2601".to_string(), "SHFE.rb2601".to_string()]
        );
        assert_eq!(batches[1].start_ns, 3_000);
        assert_eq!(batches[1].end_ns, 4_000);
        assert_eq!(batches[1].symbols, vec!["SHFE.rb2601".to_string()]);
    }

    #[test]
    fn remote_tick_write_buffer_batches_cache_appends() {
        let symbol = "SHFE.rb2601";
        let cache_dir = temp_cache_dir("remote-tick-write-buffer");
        let _ = std::fs::remove_dir_all(&cache_dir);
        let cache = BacktestTickCache::open(&cache_dir).unwrap();
        let mut buffer = RemoteTickWriteBuffer::new(2);

        buffer
            .push_rows(&cache, symbol, vec![tick(1, 1_000, 100.0)])
            .unwrap();
        assert!(!cache.tick_series_path(symbol).exists());

        buffer
            .push_rows(&cache, symbol, vec![tick(2, 2_000, 101.0)])
            .unwrap();
        assert!(
            cache
                .inspect(symbol, 1_000, 3_000)
                .unwrap()
                .series_path_exists
        );
        cache
            .mark_complete(symbol, 1_000, 3_000, 2, Some((1, 3)))
            .unwrap();
        let first_batch = cache
            .load_series(TickDataSeriesRequest::new(symbol, 1_000, 3_000))
            .unwrap();
        assert_eq!(
            first_batch.iter().map(|row| row.id).collect::<Vec<_>>(),
            vec![1, 2]
        );

        buffer
            .push_rows(&cache, symbol, vec![tick(3, 3_000, 102.0)])
            .unwrap();
        buffer.flush_all(&cache).unwrap();
        cache
            .mark_complete(symbol, 3_000, 4_000, 1, Some((3, 4)))
            .unwrap();
        let all_rows = cache
            .load_series(TickDataSeriesRequest::new(symbol, 1_000, 4_000))
            .unwrap();
        assert_eq!(
            all_rows.iter().map(|row| row.id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );

        let _ = std::fs::remove_dir_all(&cache_dir);
    }

    #[test]
    fn remote_fill_idle_timeout_can_be_overridden_for_validation() {
        assert_eq!(
            parse_remote_fill_idle_timeout(Some("5")),
            Duration::from_secs(5)
        );
        assert_eq!(
            parse_remote_fill_idle_timeout(Some("0")),
            REMOTE_FILL_IDLE_TIMEOUT
        );
        assert_eq!(
            parse_remote_fill_idle_timeout(Some("invalid")),
            REMOTE_FILL_IDLE_TIMEOUT
        );
        assert_eq!(
            parse_remote_fill_idle_timeout(None),
            REMOTE_FILL_IDLE_TIMEOUT
        );
    }

    #[test]
    fn remote_fill_batch_timeout_is_disabled_unless_explicitly_configured() {
        assert_eq!(
            parse_remote_fill_batch_timeout(Some("30")),
            Duration::from_secs(30)
        );
        assert_eq!(parse_remote_fill_batch_timeout(Some("0")), Duration::ZERO);
        assert_eq!(
            parse_remote_fill_batch_timeout(Some("invalid")),
            Duration::ZERO
        );
        assert_eq!(parse_remote_fill_batch_timeout(None), Duration::ZERO);
        assert_eq!(REMOTE_FILL_BATCH_TIMEOUT, Duration::ZERO);
    }

    #[test]
    fn remote_tick_fill_state_coalesces_out_of_order_contiguous_ids() {
        let mut fill = RemoteTickFillState::new("SHFE.rb2601", 1_000, 4_000);

        assert!(fill.push(&tick(1, 1_000, 100.0)));
        assert!(fill.push(&tick(3, 3_500, 102.0)));
        assert!(fill.push(&tick(2, 2_000, 101.0)));
        assert!(!fill.push(&tick(2, 2_000, 101.0)));

        let report = fill.finish(1_000_000_000);
        assert!(report.complete);
        assert_eq!(report.unique_rows, 3);
        assert_eq!(report.id_range, Some((1, 3)));
        assert_eq!(fill.id_intervals.len(), 1);
    }

    #[test]
    fn remote_tick_fill_state_rejects_discontinuous_id_ranges() {
        let mut fill = RemoteTickFillState::new("SHFE.rb2601", 1_000, 4_000);

        assert!(fill.push(&tick(1, 1_000, 100.0)));
        assert!(fill.push(&tick(3, 3_500, 102.0)));

        let report = fill.finish_after_idle(1_000_000_000);
        assert!(!report.complete);
        assert_eq!(
            report.gap_summary.as_deref(),
            Some("tick id range 1..=3 contains 2 unique rows")
        );
    }

    #[test]
    fn remote_tick_fill_state_matches_public_accumulator_semantics() {
        let mut state = RemoteTickFillState::new("SHFE.rb2601", 1_000, 4_000);
        let mut baseline = BacktestTickFill::new("SHFE.rb2601", 1_000, 4_000);
        let rows = [
            tick(2, 2_000, 101.0),
            tick(1, 1_000, 100.0),
            tick(3, 3_500, 102.0),
            tick(1, 1_100, 100.5),
            tick(3, 3_600, 102.5),
            tick(0, 999, 99.0),
            tick(4, 4_000, 103.0),
        ];

        for row in rows {
            assert_eq!(state.push(&row), baseline.push(row).unwrap());
        }

        assert_eq!(state.finish(1_000_000_000).first_datetime_ns, Some(1_100));
        assert_eq!(state.finish(1_000_000_000).last_datetime_ns, Some(3_600));
        assert_eq!(
            state.finish(1_000_000_000),
            baseline.finish(1_000_000_000).unwrap()
        );
        assert_eq!(
            state.finish_after_idle(1_000_000_000),
            baseline.finish_after_idle(1_000_000_000).unwrap()
        );
    }

    #[test]
    fn serial_exhaustion_can_finalize_only_non_future_ranges() {
        assert!(should_finalize_idle_after_serial_exhaustion(
            true,
            2_000,
            Some(2_000)
        ));
        assert!(should_finalize_idle_after_serial_exhaustion(
            true,
            1_999,
            Some(2_000)
        ));
        assert!(!should_finalize_idle_after_serial_exhaustion(
            false,
            1_999,
            Some(2_000)
        ));
        assert!(!should_finalize_idle_after_serial_exhaustion(
            true,
            2_001,
            Some(2_000)
        ));
        assert!(!should_finalize_idle_after_serial_exhaustion(
            true, 2_000, None
        ));
    }

    #[test]
    fn remote_fill_empty_idle_flag_accepts_common_truthy_values() {
        for value in ["1", "true", "TRUE", "yes", "on"] {
            assert!(parse_remote_fill_allow_empty_idle(Some(value)));
        }
        for value in [None, Some("0"), Some("false"), Some("off"), Some("invalid")] {
            assert!(!parse_remote_fill_allow_empty_idle(value));
        }
    }

    #[test]
    fn remote_fill_progress_flag_accepts_common_truthy_values() {
        for value in ["1", "true", "TRUE", "yes", "on"] {
            assert!(parse_remote_fill_progress_enabled(Some(value)));
        }
        for value in [None, Some("0"), Some("false"), Some("off"), Some("invalid")] {
            assert!(!parse_remote_fill_progress_enabled(value));
        }
    }

    #[test]
    fn remote_fill_rejects_unconfirmed_empty_idle_finalize() {
        assert!(should_reject_empty_idle_finalize(false, true));
        assert!(!should_reject_empty_idle_finalize(false, false));
        assert!(!should_reject_empty_idle_finalize(true, true));
    }

    #[test]
    fn remote_fill_rejects_unconfirmed_incomplete_idle_finalize() {
        assert!(should_reject_incomplete_idle_finalize(true));
        assert!(!should_reject_incomplete_idle_finalize(false));
    }

    #[test]
    fn remote_fill_rejects_future_idle_finalize() {
        assert!(should_reject_future_idle_finalize(2_001, Some(2_000)));
        assert!(!should_reject_future_idle_finalize(2_000, Some(2_000)));
        assert!(!should_reject_future_idle_finalize(1_999, Some(2_000)));
        assert!(should_reject_future_idle_finalize(2_000, None));
    }

    #[test]
    fn remote_fill_splits_only_multi_symbol_empty_idle_errors() {
        let empty_idle = crate::data_validation(
            "remote backtest cache fill idled without accepted ticks for 4 symbols in range [1, 2)",
        );
        let unconfirmed_empty_idle = crate::data_validation(
            "remote backtest cache fill idled before empty tick ranges were confirmed for 4 symbols (A,B,C,D) in range [1, 2)",
        );
        let unconfirmed_incomplete_idle = crate::data_validation(
            "remote backtest cache fill idled before tick ranges were confirmed for 4 symbols (A,B,C,D) in range [1, 2)",
        );
        let other = crate::data_validation("remote backtest cache fill failed for another reason");

        assert!(should_split_empty_idle_batch(&empty_idle, 4));
        assert!(should_split_empty_idle_batch(&unconfirmed_empty_idle, 4));
        assert!(should_split_empty_idle_batch(
            &unconfirmed_incomplete_idle,
            4
        ));
        assert!(!should_split_empty_idle_batch(&empty_idle, 1));
        assert!(!should_split_empty_idle_batch(&unconfirmed_empty_idle, 1));
        assert!(!should_split_empty_idle_batch(
            &unconfirmed_incomplete_idle,
            1
        ));
        assert!(!should_split_empty_idle_batch(&other, 4));
    }

    #[test]
    fn remote_fill_retries_transient_remote_connect_errors() {
        let token_error = crate::data_validation(
            "auth error: token request failed: error sending request for url (https://auth.example/token)",
        );
        let endpoint_error = crate::data_validation(
            "market endpoint request failed: error sending request for url (https://api.example/ns)",
        );
        let validation_error = crate::data_validation("remote fill rejected empty coverage");

        assert!(should_retry_remote_connect_error(&token_error));
        assert!(should_retry_remote_connect_error(&endpoint_error));
        assert!(!should_retry_remote_connect_error(&validation_error));
    }

    #[test]
    fn remote_fill_retries_single_symbol_transient_attempt_idle_errors() {
        let wrapped_unconfirmed_empty_idle = crate::data_validation(
            "remote backtest cache fill failed for slice [1, 2): invalid data query input: \
             remote backtest cache fill idled before empty tick ranges were confirmed \
             for 1 symbols (KQ.i@SHFE.ao) in range [1, 2); refusing to mark complete empty coverage",
        );
        let wrapped_unconfirmed_incomplete_idle = crate::data_validation(
            "remote backtest cache fill failed for slice [1, 2): invalid data query input: \
             remote backtest cache fill idled before tick ranges were confirmed \
             for 1 symbols (SHFE.ao2609) in range [1, 2); refusing to mark complete partial coverage",
        );
        let other = crate::data_validation("remote fill rejected empty coverage");

        assert!(should_retry_remote_fill_attempt_error(
            &wrapped_unconfirmed_empty_idle,
            1
        ));
        assert!(should_retry_remote_fill_attempt_error(
            &wrapped_unconfirmed_incomplete_idle,
            1
        ));
        assert!(!should_retry_remote_fill_attempt_error(&other, 1));
        assert!(!should_retry_remote_fill_attempt_error(
            &wrapped_unconfirmed_empty_idle,
            4
        ));
        assert!(!should_retry_remote_fill_attempt_error(
            &wrapped_unconfirmed_incomplete_idle,
            4
        ));
    }

    #[test]
    fn remote_fill_symbol_batch_defaults_to_single_symbol_safe_mode() {
        assert_eq!(parse_remote_fill_symbol_batch_size(None), 1);
        assert_eq!(parse_remote_fill_symbol_batch_size(Some("0")), 1);
        assert_eq!(parse_remote_fill_symbol_batch_size(Some("invalid")), 1);
        assert_eq!(parse_remote_fill_symbol_batch_size(Some("2")), 2);
        assert_eq!(parse_remote_fill_symbol_batch_size(Some("128")), 4);
    }

    #[test]
    fn remote_fill_symbol_concurrency_defaults_to_bounded_parallelism() {
        assert_eq!(parse_remote_fill_symbol_concurrency(None), 2);
        assert_eq!(parse_remote_fill_symbol_concurrency(Some("0")), 2);
        assert_eq!(parse_remote_fill_symbol_concurrency(Some("invalid")), 2);
        assert_eq!(parse_remote_fill_symbol_concurrency(Some("1")), 1);
        assert_eq!(parse_remote_fill_symbol_concurrency(Some("2")), 2);
        assert_eq!(parse_remote_fill_symbol_concurrency(Some("8")), 4);
    }

    #[test]
    fn remote_fill_rejects_multi_symbol_empty_overall_fill_by_default() {
        assert!(should_reject_empty_remote_fill(2, 0, false));
        assert!(should_reject_empty_remote_fill(128, 0, false));
        assert!(!should_reject_empty_remote_fill(1, 0, false));
        assert!(!should_reject_empty_remote_fill(2, 1, false));
        assert!(!should_reject_empty_remote_fill(2, 0, true));
    }

    #[test]
    fn remote_fill_slice_can_be_overridden_for_fallback() {
        assert_eq!(
            parse_remote_fill_slice_ns(Some("172800")),
            Some(172_800_000_000_000)
        );
        assert_eq!(parse_remote_fill_slice_ns(Some("0")), None);
        assert_eq!(parse_remote_fill_slice_ns(Some("invalid")), None);
        assert_eq!(parse_remote_fill_slice_ns(None), None);
    }

    fn temp_cache_dir(name: &str) -> std::path::PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "tqsdk-backtest-remote-{name}-{}-{unique}",
            std::process::id()
        ))
    }

    fn tick(id: i64, datetime: i64, last_price: f64) -> Tick {
        Tick {
            id,
            datetime,
            last_price,
            ask_price1: last_price + 0.5,
            ask_volume1: 1,
            bid_price1: last_price - 0.5,
            bid_volume1: 1,
            ..Tick::default()
        }
    }
}
