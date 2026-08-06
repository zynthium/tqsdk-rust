//! Facade compatibility types and adapters for remote backtest cache fills.
//!
//! The durable cache protocol, official server-backtest transport, retry
//! policy, and single-flight coordination are owned by `tqsdk-data`.  This
//! module deliberately retains the established facade configuration and
//! callback types, then translates them into bounded data-layer requests.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::NaiveDate;
use tokio::task::JoinSet;
use tqsdk_data::{
    BacktestHistoryAuthProvider, BacktestHistoryBatchReport, BacktestHistoryClient,
    BacktestHistoryCredentials, BacktestHistoryEvent, BacktestHistoryPhase, BacktestHistoryPolicy,
    BacktestHistoryRequest, BacktestHistoryRows, BacktestHistoryTelemetryEvent, BacktestTickCache,
    BacktestTickCacheOperationLock, DataError, MinuteKlineCache,
};

use crate::{Auth, Result, data_validation};

const REMOTE_FILL_BATCH_TIMEOUT: Duration = Duration::ZERO;
const REMOTE_FILL_SYMBOL_BATCH_SIZE: usize = 1;
const REMOTE_FILL_SYMBOL_BATCH_SIZE_MAX: usize = 4;
const REMOTE_FILL_SYMBOL_CONCURRENCY: usize = 2;
const REMOTE_FILL_SYMBOL_CONCURRENCY_MAX: usize = 4;

/// Typed configuration for remote historical cache fills.
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
            idle_timeout: Duration::from_secs(60),
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
            self.idle_timeout = Duration::from_secs(60);
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
pub type BacktestRemoteFillProgressHandler =
    Arc<dyn Fn(&BacktestRemoteFillProgress) + Send + Sync + 'static>;

/// Immutable physical-cache planning details for one remote fill operation.
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
    Inspecting,
    PlanReady,
    Started,
    Streaming,
    Retrying,
    SplitFallback,
    Finished,
    Failed,
    Cancelled,
}

/// Cumulative cache-coverage inspection progress before remote fill planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BacktestRemoteFillInspectionProgress {
    total_ranges: usize,
    checked_ranges: usize,
    complete_ranges: usize,
    incomplete_ranges: usize,
}

impl BacktestRemoteFillInspectionProgress {
    pub(crate) fn new(
        total_ranges: usize,
        checked_ranges: usize,
        complete_ranges: usize,
        incomplete_ranges: usize,
    ) -> Self {
        Self {
            total_ranges,
            checked_ranges,
            complete_ranges,
            incomplete_ranges,
        }
    }

    #[must_use]
    pub fn total_ranges(&self) -> usize {
        self.total_ranges
    }

    #[must_use]
    pub fn checked_ranges(&self) -> usize {
        self.checked_ranges
    }

    #[must_use]
    pub fn complete_ranges(&self) -> usize {
        self.complete_ranges
    }

    #[must_use]
    pub fn incomplete_ranges(&self) -> usize {
        self.incomplete_ranges
    }
}

/// Immutable, low-overhead remote cache-fill telemetry snapshot.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BacktestRemoteFillTelemetry {
    phase: BacktestRemoteFillPhase,
    plan: Option<RemoteFillPlan>,
    inspection: Option<BacktestRemoteFillInspectionProgress>,
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
            inspection: None,
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

    fn inspection(
        physical_symbol: &str,
        requested_range: (i64, i64),
        inspection: BacktestRemoteFillInspectionProgress,
    ) -> Self {
        Self {
            phase: BacktestRemoteFillPhase::Inspecting,
            plan: None,
            inspection: Some(inspection),
            logical_batch_id: None,
            attempt: 0,
            physical_symbol: Some(physical_symbol.to_string()),
            requested_range: Some(requested_range),
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
            inspection: None,
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
    pub fn inspection_progress(&self) -> Option<&BacktestRemoteFillInspectionProgress> {
        self.inspection.as_ref()
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

    pub(crate) fn emit(&self, event: BacktestRemoteFillProgress) {
        if let Some(progress) = &self.progress {
            progress(&event);
        }
    }

    pub(crate) fn emit_plan(&self, plan: RemoteFillPlan) {
        self.emit_telemetry(BacktestRemoteFillTelemetry::plan_ready(plan));
    }

    pub(crate) fn emit_inspection(
        &self,
        physical_symbol: &str,
        requested_range: (i64, i64),
        inspection: BacktestRemoteFillInspectionProgress,
    ) {
        if self.telemetry.is_none() {
            return;
        }
        self.emit_telemetry(BacktestRemoteFillTelemetry::inspection(
            physical_symbol,
            requested_range,
            inspection,
        ));
    }

    pub(crate) fn config(&self) -> BacktestRemoteFillConfig {
        self.config
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancellation
            .as_ref()
            .is_some_and(BacktestRemoteFillCancellation::is_cancelled)
    }

    fn emit_telemetry(&self, event: BacktestRemoteFillTelemetry) {
        if let Some(telemetry) = &self.telemetry {
            telemetry(&event);
        }
    }
}

pub(crate) struct RemoteBacktestCacheFillReport {
    pub(crate) rows_by_symbol: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteBacktestCacheFillRequest {
    pub(crate) symbol: String,
    pub(crate) start_ns: i64,
    pub(crate) end_ns: i64,
    pub(crate) commit_mode: RemoteCacheCommitMode,
}

impl RemoteBacktestCacheFillRequest {
    pub(crate) fn new(symbol: impl Into<String>, start_ns: i64, end_ns: i64) -> Self {
        Self {
            symbol: symbol.into(),
            start_ns,
            end_ns,
            commit_mode: RemoteCacheCommitMode::Final,
        }
    }

    pub(crate) fn provisional(
        symbol: impl Into<String>,
        start_ns: i64,
        end_ns: i64,
        provisional_start_ns: i64,
        as_of_ns: i64,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            start_ns,
            end_ns,
            commit_mode: RemoteCacheCommitMode::Provisional {
                provisional_start_ns,
                as_of_ns,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RemoteCacheCommitMode {
    Final,
    Provisional {
        provisional_start_ns: i64,
        as_of_ns: i64,
    },
}

#[cfg(test)]
impl RemoteCacheCommitMode {
    pub(crate) fn is_provisional(self) -> bool {
        matches!(self, Self::Provisional { .. })
    }

    pub(crate) fn provisional_start_ns(self) -> Option<i64> {
        match self {
            Self::Final => None,
            Self::Provisional {
                provisional_start_ns,
                ..
            } => Some(provisional_start_ns),
        }
    }
}

/// One canonical-minute range that must be materialized from the official
/// server-backtest source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BacktestMinuteKlineFillRequest {
    pub(crate) symbol: String,
    pub(crate) start_ns: i64,
    pub(crate) end_ns: i64,
}

impl BacktestMinuteKlineFillRequest {
    pub(crate) fn new(symbol: impl Into<String>, start_ns: i64, end_ns: i64) -> Self {
        Self {
            symbol: symbol.into(),
            start_ns,
            end_ns,
        }
    }
}

pub(crate) struct BacktestMinuteKlineFillReport {
    pub(crate) rows_by_symbol: BTreeMap<String, usize>,
}

#[derive(Debug, Clone)]
struct RemoteFillBatch {
    start_ns: i64,
    end_ns: i64,
    symbols: Vec<String>,
    commit_mode: RemoteCacheCommitMode,
}

struct RemoteFillBatchTaskReport {
    batch_index: usize,
    start_ns: i64,
    end_ns: i64,
    symbols: Vec<String>,
    elapsed: Duration,
    rows_by_symbol: BTreeMap<String, usize>,
    filled_ranges_by_symbol: BTreeMap<String, Vec<(i64, i64)>>,
}

struct RemoteFillBatchTask {
    batch_index: usize,
    total_batches: usize,
    batch: RemoteFillBatch,
    kind: FacadeHistoryFillKind,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FacadeHistoryFillKind {
    Tick,
    CanonicalMinute,
}

impl FacadeHistoryFillKind {
    fn request(
        self,
        request_id: u64,
        symbol: String,
        start_ns: i64,
        end_ns: i64,
        commit_mode: RemoteCacheCommitMode,
    ) -> BacktestHistoryRequest {
        match self {
            Self::Tick => {
                let request = BacktestHistoryRequest::tick(request_id, symbol, start_ns, end_ns);
                match commit_mode {
                    RemoteCacheCommitMode::Final => request,
                    RemoteCacheCommitMode::Provisional { as_of_ns, .. } => {
                        request.with_provisional_as_of_ns(as_of_ns)
                    }
                }
            }
            Self::CanonicalMinute => BacktestHistoryRequest::kline(
                request_id,
                symbol,
                Duration::from_secs(60),
                start_ns,
                end_ns,
            ),
        }
    }
}

/// Fill the durable Tick cache through `tqsdk-data`'s shared historical query
/// path. The facade retains this private signature for its stable warmup
/// report, while all server IO and cache commits live below the facade.
pub(crate) async fn fill_backtest_tick_cache(
    user: String,
    pass: String,
    requests: Vec<RemoteBacktestCacheFillRequest>,
    cache: BacktestTickCache,
    root_gate: Arc<BacktestTickCacheOperationLock>,
    runtime: RemoteBacktestFillRuntime,
) -> Result<RemoteBacktestCacheFillReport> {
    let report = fill_backtest_history_cache(
        FacadeBacktestHistoryAuthProvider::new(user, pass),
        cache.cache_dir(),
        split_remote_fill_requests(requests, runtime.config())?,
        FacadeHistoryFillKind::Tick,
        root_gate,
        runtime,
    )
    .await?;
    Ok(RemoteBacktestCacheFillReport {
        rows_by_symbol: report,
    })
}

/// Fill the canonical-minute cache through the same data-layer pipeline used
/// for Tick fills. `MinuteKlineCache` is retained only to obtain the shared
/// cache root; snapshot resolution is owned by the data planner.
pub(crate) async fn fill_backtest_minute_kline_cache(
    auth: &Auth,
    cache: &MinuteKlineCache,
    requests: Vec<BacktestMinuteKlineFillRequest>,
    root_gate: Arc<BacktestTickCacheOperationLock>,
    runtime: RemoteBacktestFillRuntime,
) -> Result<BacktestMinuteKlineFillReport> {
    let requests = requests
        .into_iter()
        .map(|request| {
            RemoteBacktestCacheFillRequest::new(request.symbol, request.start_ns, request.end_ns)
        })
        .collect();
    let report = fill_backtest_history_cache(
        FacadeBacktestHistoryAuthProvider::new(auth.user.clone(), auth.pass.clone()),
        cache.root_dir(),
        split_remote_fill_requests(requests, runtime.config())?,
        FacadeHistoryFillKind::CanonicalMinute,
        root_gate,
        runtime,
    )
    .await?;
    Ok(BacktestMinuteKlineFillReport {
        rows_by_symbol: report,
    })
}

/// Lazily resolves missing `KQ.m@...` metadata before the facade needs its
/// physical tick-cache ranges. Ordinary concrete/index symbols are left to the
/// data query planner, and an already-covering sidecar never loads auth.
pub(crate) async fn ensure_remote_main_contract_metadata(
    auth: Option<&Auth>,
    cache_dir: &std::path::Path,
    symbols: impl IntoIterator<Item = String>,
    start_ns: i64,
    end_ns: i64,
) -> Result<()> {
    let symbols = symbols
        .into_iter()
        .filter(|symbol| symbol.starts_with("KQ.m@"))
        .collect::<BTreeSet<_>>();
    if symbols.is_empty() {
        return Ok(());
    }

    let mut missing = Vec::new();
    for symbol in symbols {
        let is_covered =
            tqsdk_data::resolve_backtest_metadata_snapshot(cache_dir, &symbol, start_ns, end_ns)?
                .is_some_and(|snapshot| {
                    metadata_covers_range(&snapshot.physical_segments, start_ns, end_ns)
                });
        if !is_covered {
            missing.push(symbol);
        }
    }
    if missing.is_empty() {
        return Ok(());
    }

    let auth = auth.ok_or_else(|| data_validation("remote backtest cache fill requires auth"))?;
    let client = tqsdk_data::BacktestHistoryMaintenanceClient::builder(cache_dir.to_path_buf())
        .auth_provider(FacadeBacktestHistoryAuthProvider::new(
            auth.user.clone(),
            auth.pass.clone(),
        ))
        .build()?;

    let mut pending = missing.into_iter();
    let mut tasks = JoinSet::new();
    loop {
        while tasks.len() < REMOTE_FILL_SYMBOL_CONCURRENCY_MAX {
            let Some(symbol) = pending.next() else {
                break;
            };
            let client = client.clone();
            tasks.spawn(async move {
                client
                    .refresh_metadata(&symbol, start_ns, end_ns)
                    .await
                    .map(|_| ())
            });
        }
        let Some(result) = tasks.join_next().await else {
            break;
        };
        result
            .map_err(|error| data_validation(format!("metadata refresh task failed: {error}")))??;
    }
    Ok(())
}

fn metadata_covers_range(
    segments: &[tqsdk_data::BacktestHistoryPhysicalSegment],
    start_ns: i64,
    end_ns: i64,
) -> bool {
    let mut cursor = start_ns;
    for segment in segments {
        if segment.end_ns <= cursor || segment.start_ns >= end_ns {
            continue;
        }
        if segment.start_ns > cursor {
            return false;
        }
        cursor = cursor.max(segment.end_ns);
        if cursor >= end_ns {
            return true;
        }
    }
    false
}

async fn fill_backtest_history_cache(
    auth: FacadeBacktestHistoryAuthProvider,
    cache_dir: &std::path::Path,
    requests: Vec<RemoteBacktestCacheFillRequest>,
    kind: FacadeHistoryFillKind,
    root_gate: Arc<BacktestTickCacheOperationLock>,
    runtime: RemoteBacktestFillRuntime,
) -> Result<BTreeMap<String, usize>> {
    if requests.is_empty() {
        return Ok(BTreeMap::new());
    }
    if runtime.is_cancelled() {
        return Err(remote_fill_cancelled_error());
    }

    let config = runtime.config();
    let mut pending_batches = remote_fill_batches(requests, config.symbol_batch_size)?;
    let all_requests_provisional = pending_batches
        .iter()
        .all(|batch| matches!(batch.commit_mode, RemoteCacheCommitMode::Provisional { .. }));
    let requested_symbols = pending_batches
        .iter()
        .flat_map(|batch| batch.symbols.iter().map(String::as_str))
        .collect::<BTreeSet<_>>()
        .len();
    let total_batches = pending_batches.len();
    runtime.emit(BacktestRemoteFillProgress::FillStarted {
        requested_symbols,
        total_batches,
        symbol_batch_size: config.symbol_batch_size,
        symbol_concurrency: config.symbol_concurrency,
        batch_timeout: config.batch_timeout,
    });

    let mut tasks = JoinSet::new();
    let mut next_batch_index = 0usize;
    let mut completed_batches = 0usize;
    let mut rows_by_symbol = BTreeMap::new();
    let mut filled_ranges_by_symbol = BTreeMap::<String, Vec<(i64, i64)>>::new();
    let mut errors = Vec::new();
    while !pending_batches.is_empty() || !tasks.is_empty() {
        while !runtime.is_cancelled() && tasks.len() < config.symbol_concurrency {
            let Some(batch) = pending_batches.pop_front() else {
                break;
            };
            let batch_index = next_batch_index;
            next_batch_index = next_batch_index.saturating_add(1);
            runtime.emit(BacktestRemoteFillProgress::BatchStarted {
                batch_number: batch_index.saturating_add(1),
                total_batches,
                pending_batches: pending_batches.len(),
                active_batches: tasks.len().saturating_add(1),
                requested_range: (batch.start_ns, batch.end_ns),
                symbols: batch.symbols.clone(),
            });
            emit_batch_telemetry(
                &runtime,
                batch_index.saturating_add(1),
                BacktestRemoteFillPhase::Started,
                &batch,
                0,
                None,
            );
            let auth = auth.clone();
            let cache_dir = cache_dir.to_path_buf();
            let root_gate = Arc::clone(&root_gate);
            let runtime = runtime.clone();
            tasks.spawn(async move {
                fill_backtest_history_batch(
                    RemoteFillBatchTask {
                        batch_index,
                        total_batches,
                        batch,
                        kind,
                    },
                    auth,
                    cache_dir,
                    root_gate,
                    runtime,
                )
                .await
            });
        }

        let Some(result) = tasks.join_next().await else {
            break;
        };
        match result {
            Ok(Ok(report)) => {
                completed_batches = completed_batches.saturating_add(1);
                let rows = report.rows_by_symbol.values().copied().sum();
                runtime.emit(BacktestRemoteFillProgress::BatchFinished {
                    batch_number: report.batch_index.saturating_add(1),
                    total_batches,
                    completed_batches,
                    requested_range: (report.start_ns, report.end_ns),
                    symbols: report.symbols.clone(),
                    elapsed: report.elapsed,
                    rows,
                });
                for (symbol, count) in report.rows_by_symbol {
                    *rows_by_symbol.entry(symbol).or_insert(0) += count;
                }
                for (symbol, ranges) in report.filled_ranges_by_symbol {
                    filled_ranges_by_symbol
                        .entry(symbol)
                        .or_default()
                        .extend(ranges);
                }
            }
            Ok(Err(error)) => errors.push(error.to_string()),
            Err(error) => errors.push(format!("data-layer remote fill task failed: {error}")),
        }
    }
    if runtime.is_cancelled() {
        return Err(remote_fill_cancelled_error());
    }
    if !errors.is_empty() {
        return Err(data_validation(format!(
            "remote backtest cache fill completed {completed_batches}/{total_batches} batches; {} batch(es) failed: {}",
            errors.len(),
            errors.join(" | ")
        )));
    }
    // Successful data-layer batches are reported only after an explicit
    // server terminal has durably committed coverage. Such a terminal
    // zero-row window is valid (for example, a holiday or pre-listing range).
    if should_reject_empty_remote_tick_fill(
        kind,
        rows_by_symbol.values().copied().sum(),
        all_requests_provisional,
        config.allow_empty_idle,
        completed_batches == total_batches,
    ) {
        return Err(data_validation(format!(
            "remote backtest cache fill completed without accepted ticks for {requested_symbols} symbols; refusing to mark complete empty coverage"
        )));
    }
    let compaction_ranges = final_tick_compaction_ranges(kind, &filled_ranges_by_symbol)?;
    if !compaction_ranges.is_empty() {
        let compaction_cache_dir = cache_dir.to_path_buf();
        let compaction_runtime = runtime.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let cache = BacktestTickCache::open(compaction_cache_dir)?;
            for (symbol, ranges) in compaction_ranges {
                for (start_ns, end_ns) in ranges {
                    if compaction_runtime.is_cancelled() {
                        return Err(remote_fill_cancelled_error());
                    }
                    cache.compact_symbol_ticks_in_range(&symbol, start_ns, end_ns)?;
                }
            }
            Ok(())
        })
        .await
        .map_err(|error| {
            data_validation(format!(
                "remote backtest cache compaction task failed: {error}"
            ))
        })??;
        if runtime.is_cancelled() {
            return Err(remote_fill_cancelled_error());
        }
    }
    Ok(rows_by_symbol)
}

fn final_tick_compaction_ranges(
    kind: FacadeHistoryFillKind,
    filled_ranges_by_symbol: &BTreeMap<String, Vec<(i64, i64)>>,
) -> Result<BTreeMap<String, Vec<(i64, i64)>>> {
    if kind != FacadeHistoryFillKind::Tick {
        return Ok(BTreeMap::new());
    }
    let mut by_symbol = BTreeMap::<String, BTreeSet<(i64, i64)>>::new();
    for (symbol, ranges) in filled_ranges_by_symbol {
        for &(start_ns, end_ns) in ranges {
            if start_ns >= end_ns {
                return Err(data_validation(
                    "TQBN filled range for compaction must be non-empty",
                ));
            }
            let mut cursor = start_ns;
            while cursor < end_ns {
                let day = tqsdk_data::backtest_tick_trading_day_for_timestamp_ns(cursor)?;
                let partition = tqsdk_data::backtest_tick_trading_day_range(day)?;
                if partition.end_ns <= cursor {
                    return Err(data_validation(
                        "TQBN trading-day compaction range did not advance",
                    ));
                }
                by_symbol
                    .entry(symbol.clone())
                    .or_default()
                    .insert((partition.start_ns, partition.end_ns));
                cursor = partition.end_ns.min(end_ns);
            }
        }
    }
    Ok(by_symbol
        .into_iter()
        .map(|(symbol, ranges)| (symbol, ranges.into_iter().collect()))
        .collect())
}

fn should_reject_empty_remote_tick_fill(
    kind: FacadeHistoryFillKind,
    accepted_rows: usize,
    all_requests_provisional: bool,
    allow_empty_idle: bool,
    terminal_confirmed: bool,
) -> bool {
    kind == FacadeHistoryFillKind::Tick
        && accepted_rows == 0
        && !all_requests_provisional
        && !allow_empty_idle
        && !terminal_confirmed
}

async fn fill_backtest_history_batch(
    task: RemoteFillBatchTask,
    auth: FacadeBacktestHistoryAuthProvider,
    cache_dir: std::path::PathBuf,
    root_gate: Arc<BacktestTickCacheOperationLock>,
    runtime: RemoteBacktestFillRuntime,
) -> Result<RemoteFillBatchTaskReport> {
    let RemoteFillBatchTask {
        batch_index,
        total_batches,
        batch,
        kind,
    } = task;
    let started = tokio::time::Instant::now();
    let requests = batch
        .symbols
        .iter()
        .enumerate()
        .map(|(index, symbol)| {
            kind.request(
                u64::try_from(index).unwrap_or(u64::MAX),
                symbol.clone(),
                batch.start_ns,
                batch.end_ns,
                batch.commit_mode,
            )
        })
        .collect::<Vec<_>>();
    let client = BacktestHistoryClient::builder(cache_dir)
        .policy(BacktestHistoryPolicy::RemoteOnMiss)
        .logical_concurrency(batch.symbols.len().max(1))
        .auth_provider(auth)
        .build()?;
    let report = materialize_cache_with_runtime(
        client,
        requests,
        batch_index,
        &batch,
        kind,
        root_gate,
        &runtime,
    )
    .await;
    let report = match report {
        Ok(report) => report,
        Err(error) => {
            runtime.emit(BacktestRemoteFillProgress::BatchFailed {
                batch_number: batch_index.saturating_add(1),
                total_batches,
                requested_range: (batch.start_ns, batch.end_ns),
                symbols: batch.symbols.clone(),
                error: error.to_string(),
            });
            emit_batch_telemetry(
                &runtime,
                batch_index.saturating_add(1),
                if runtime.is_cancelled() {
                    BacktestRemoteFillPhase::Cancelled
                } else {
                    BacktestRemoteFillPhase::Failed
                },
                &batch,
                0,
                Some(error.to_string()),
            );
            return Err(error.into());
        }
    };
    let mut rows_by_symbol = BTreeMap::new();
    let mut filled_ranges_by_symbol = BTreeMap::<String, Vec<(i64, i64)>>::new();
    let compact_final_ticks = kind == FacadeHistoryFillKind::Tick
        && !matches!(batch.commit_mode, RemoteCacheCommitMode::Provisional { .. });
    for completed in report.completed {
        let symbol = completed.symbol;
        if compact_final_ticks
            && completed.remote_used
            && !completed.coverage.remote_filled_ranges.is_empty()
        {
            filled_ranges_by_symbol
                .entry(symbol.clone())
                .or_default()
                .extend(completed.coverage.remote_filled_ranges);
        }
        *rows_by_symbol.entry(symbol).or_insert(0) += completed.rows;
    }
    let rows = rows_by_symbol.values().copied().sum();
    emit_batch_telemetry(
        &runtime,
        batch_index.saturating_add(1),
        BacktestRemoteFillPhase::Finished,
        &batch,
        rows,
        None,
    );
    Ok(RemoteFillBatchTaskReport {
        batch_index,
        start_ns: batch.start_ns,
        end_ns: batch.end_ns,
        symbols: batch.symbols,
        elapsed: started.elapsed(),
        rows_by_symbol,
        filled_ranges_by_symbol,
    })
}

async fn materialize_cache_with_runtime(
    client: BacktestHistoryClient,
    requests: Vec<BacktestHistoryRequest>,
    logical_batch_id: usize,
    batch: &RemoteFillBatch,
    kind: FacadeHistoryFillKind,
    root_gate: Arc<BacktestTickCacheOperationLock>,
    runtime: &RemoteBacktestFillRuntime,
) -> tqsdk_data::Result<BacktestHistoryBatchReport> {
    let started = tokio::time::Instant::now();
    let mut run = client
        .materialize_cache_run_with_root_gate(requests, root_gate)
        .await?;
    let mut telemetry = run.take_telemetry();
    let mut last_activity = tokio::time::Instant::now();
    let batch_deadline = runtime
        .config()
        .batch_timeout
        .map(|timeout| started + timeout);
    let mut accepted_rows_by_symbol = BTreeMap::<String, usize>::new();
    let mut history_progress = MaterializedHistoryProgress::default();
    loop {
        tokio::select! {
            event = run.next() => match event {
                Some(BacktestHistoryEvent::Chunk(chunk)) => {
                    last_activity = tokio::time::Instant::now();
                    observe_materialized_chunk(
                        runtime,
                        logical_batch_id,
                        batch,
                        kind,
                        &mut accepted_rows_by_symbol,
                        chunk,
                        started.elapsed(),
                    );
                }
                Some(BacktestHistoryEvent::RequestCompleted(_))
                | Some(BacktestHistoryEvent::RequestFailed(_)) => {
                    last_activity = tokio::time::Instant::now();
                }
                None => break,
            },
            telemetry_event = async {
                match telemetry.as_mut() {
                    Some(stream) => stream.next().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some(event) = telemetry_event {
                    if observe_materialized_telemetry(
                        runtime,
                        logical_batch_id,
                        batch,
                        &mut history_progress,
                        event,
                        started.elapsed(),
                    ) {
                        last_activity = tokio::time::Instant::now();
                    }
                } else {
                    telemetry = None;
                }
            },
            _ = tokio::time::sleep_until(last_activity + runtime.config().idle_timeout) => {
                let _ = run.cancel_and_finish().await;
                return Err(DataError::InvalidState(
                    "remote backtest cache fill became idle before cache coverage was finalized",
                ));
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {
                if runtime.is_cancelled() {
                    let _ = run.cancel_and_finish().await;
                    return Err(DataError::InvalidState("remote backtest cache fill cancelled"));
                }
            }
            _ = async {
                match batch_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => {
                let _ = run.cancel_and_finish().await;
                let timeout = runtime
                    .config()
                    .batch_timeout
                    .expect("batch deadline requires a configured timeout");
                return Err(DataError::Validation(format!(
                    "remote backtest cache fill batch timed out after {}s for {} symbols ({}) in range [{}, {})",
                    timeout.as_secs(),
                    batch.symbols.len(),
                    batch.symbols.join(","),
                    batch.start_ns,
                    batch.end_ns,
                )));
            }
        }
    }
    let report = run.finish().await;
    if let Some(failure) = report.failed.first() {
        return Err(DataError::RequestFailed {
            request_id: failure.request_id,
            message: failure.error.clone(),
            emitted_rows: failure.emitted_rows,
        });
    }
    Ok(report)
}

/// Translates the data layer's cache-fill telemetry before its rows are read
/// back from disk.  Canonical-minute fills are split into bounded remote
/// windows, so waiting for [`BacktestHistoryEvent::Chunk`] would otherwise
/// leave the CLI at zero until every window for the whole request has reached
/// a terminal and the cache reader starts.
fn observe_materialized_telemetry(
    runtime: &RemoteBacktestFillRuntime,
    logical_batch_id: usize,
    batch: &RemoteFillBatch,
    history_progress: &mut MaterializedHistoryProgress,
    event: BacktestHistoryTelemetryEvent,
    elapsed: Duration,
) -> bool {
    let phase = match event.phase {
        BacktestHistoryPhase::Retry => BacktestRemoteFillPhase::Retrying,
        BacktestHistoryPhase::Fill
        | BacktestHistoryPhase::Read
        | BacktestHistoryPhase::Aggregate => BacktestRemoteFillPhase::Streaming,
        BacktestHistoryPhase::Inspect | BacktestHistoryPhase::WaitForFill => {
            BacktestRemoteFillPhase::Started
        }
    };
    let (accepted_rows, made_progress) = history_progress.observe(&event);
    runtime.emit_telemetry(BacktestRemoteFillTelemetry::lifecycle(
        RemoteFillTelemetryUpdate {
            phase,
            logical_batch_id: logical_batch_id.saturating_add(1),
            attempt: 1,
            physical_symbol: event.symbol,
            requested_range: (batch.start_ns, batch.end_ns),
            accepted_rows,
            latest_cursor_ns: None,
            elapsed,
            error: None,
        },
    ));
    made_progress
}

/// The data layer reports rows cumulatively within one official-server window.
/// A large minute request may begin a following window at zero, so retain a
/// monotonic total for the facade/UI while accepting that reset.
#[derive(Debug, Default)]
struct MaterializedHistoryProgress {
    previous_fill_rows: BTreeMap<String, usize>,
    accepted_rows: BTreeMap<String, usize>,
}

impl MaterializedHistoryProgress {
    fn observe(&mut self, event: &BacktestHistoryTelemetryEvent) -> (usize, bool) {
        let previous_accepted = self
            .accepted_rows
            .get(event.symbol.as_str())
            .copied()
            .unwrap_or_default();
        if event.phase == BacktestHistoryPhase::Fill {
            let previous = self
                .previous_fill_rows
                .entry(event.symbol.clone())
                .or_default();
            let added_rows = if event.completed_rows >= *previous {
                event.completed_rows.saturating_sub(*previous)
            } else {
                // A new bounded source window starts its counter from zero.
                event.completed_rows
            };
            *previous = event.completed_rows;
            let accepted = self.accepted_rows.entry(event.symbol.clone()).or_default();
            *accepted = accepted.saturating_add(added_rows);
        } else if event.phase == BacktestHistoryPhase::Aggregate {
            let accepted = self.accepted_rows.entry(event.symbol.clone()).or_default();
            *accepted = (*accepted).max(event.completed_rows);
        }
        let accepted_rows = self
            .accepted_rows
            .get(event.symbol.as_str())
            .copied()
            .unwrap_or_default();
        (accepted_rows, accepted_rows > previous_accepted)
    }
}

fn observe_materialized_chunk(
    runtime: &RemoteBacktestFillRuntime,
    logical_batch_id: usize,
    batch: &RemoteFillBatch,
    kind: FacadeHistoryFillKind,
    accepted_rows_by_symbol: &mut BTreeMap<String, usize>,
    chunk: tqsdk_data::BacktestHistoryChunk,
    elapsed: Duration,
) {
    let (rows, latest_cursor_ns) = match &chunk.rows {
        BacktestHistoryRows::Ticks(rows) => (rows.len(), rows.last().map(|row| row.datetime)),
        BacktestHistoryRows::Klines { rows, .. } => {
            (rows.len(), rows.last().map(|row| row.datetime))
        }
    };
    let accepted_rows = {
        let accepted = accepted_rows_by_symbol
            .entry(chunk.symbol.clone())
            .or_default();
        *accepted = accepted.saturating_add(rows);
        *accepted
    };
    if kind == FacadeHistoryFillKind::Tick
        && let (Some(latest_cursor_ns), BacktestHistoryRows::Ticks(_)) =
            (latest_cursor_ns, &chunk.rows)
        && let Ok(trading_day) =
            tqsdk_data::backtest_tick_trading_day_for_timestamp_ns(latest_cursor_ns)
    {
        runtime.emit(BacktestRemoteFillProgress::TickObserved {
            symbol: chunk.symbol.clone(),
            trading_day,
            accepted_rows,
        });
    }
    runtime.emit_telemetry(BacktestRemoteFillTelemetry::lifecycle(
        RemoteFillTelemetryUpdate {
            phase: BacktestRemoteFillPhase::Streaming,
            logical_batch_id: logical_batch_id.saturating_add(1),
            attempt: 1,
            physical_symbol: chunk.symbol,
            requested_range: (batch.start_ns, batch.end_ns),
            accepted_rows,
            latest_cursor_ns,
            elapsed,
            error: None,
        },
    ));
}

fn emit_batch_telemetry(
    runtime: &RemoteBacktestFillRuntime,
    logical_batch_id: usize,
    phase: BacktestRemoteFillPhase,
    batch: &RemoteFillBatch,
    accepted_rows: usize,
    error: Option<String>,
) {
    for symbol in &batch.symbols {
        runtime.emit_telemetry(BacktestRemoteFillTelemetry::lifecycle(
            RemoteFillTelemetryUpdate {
                phase,
                logical_batch_id,
                attempt: 1,
                physical_symbol: symbol.clone(),
                requested_range: (batch.start_ns, batch.end_ns),
                accepted_rows,
                latest_cursor_ns: None,
                elapsed: Duration::ZERO,
                error: error.clone(),
            },
        ));
    }
}

fn split_remote_fill_requests(
    requests: Vec<RemoteBacktestCacheFillRequest>,
    config: BacktestRemoteFillConfig,
) -> Result<Vec<RemoteBacktestCacheFillRequest>> {
    let mut split = Vec::new();
    for request in requests {
        validate_remote_fill_request(&request)?;
        for (start_ns, end_ns) in remote_fill_ranges(request.start_ns, request.end_ns, config) {
            split.push(RemoteBacktestCacheFillRequest {
                start_ns,
                end_ns,
                ..request.clone()
            });
        }
    }
    Ok(split)
}

fn remote_fill_ranges(
    start_ns: i64,
    end_ns: i64,
    config: BacktestRemoteFillConfig,
) -> Vec<(i64, i64)> {
    let Some(slice_ns) = config.slice_ns() else {
        return vec![(start_ns, end_ns)];
    };
    let mut ranges = Vec::new();
    let mut cursor = start_ns;
    while cursor < end_ns {
        let next = cursor.saturating_add(slice_ns).min(end_ns);
        ranges.push((cursor, next));
        cursor = next;
    }
    ranges
}

fn remote_fill_batches(
    requests: Vec<RemoteBacktestCacheFillRequest>,
    symbol_batch_size: usize,
) -> Result<VecDeque<RemoteFillBatch>> {
    let mut by_range: BTreeMap<(i64, i64, RemoteCacheCommitMode), Vec<String>> = BTreeMap::new();
    for request in requests {
        validate_remote_fill_request(&request)?;
        by_range
            .entry((request.start_ns, request.end_ns, request.commit_mode))
            .or_default()
            .push(request.symbol);
    }
    let mut batches = VecDeque::new();
    for ((start_ns, end_ns, commit_mode), mut symbols) in by_range {
        symbols.sort();
        symbols.dedup();
        for chunk in symbols.chunks(symbol_batch_size.max(1)) {
            batches.push_back(RemoteFillBatch {
                start_ns,
                end_ns,
                symbols: chunk.to_vec(),
                commit_mode,
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

fn validate_remote_fill_request(request: &RemoteBacktestCacheFillRequest) -> Result<()> {
    if request.symbol.trim().is_empty() {
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
    if let RemoteCacheCommitMode::Provisional { as_of_ns, .. } = request.commit_mode
        && (as_of_ns < request.start_ns || as_of_ns > request.end_ns)
    {
        return Err(data_validation(
            "remote provisional cache fill as-of timestamp must be inside its requested range",
        ));
    }
    Ok(())
}

fn remote_fill_cancelled_error() -> crate::Error {
    data_validation("remote backtest cache fill cancelled")
}

#[derive(Clone)]
struct FacadeBacktestHistoryAuthProvider {
    user: String,
    pass: String,
}

impl FacadeBacktestHistoryAuthProvider {
    fn new(user: String, pass: String) -> Self {
        Self { user, pass }
    }
}

impl BacktestHistoryAuthProvider for FacadeBacktestHistoryAuthProvider {
    fn load<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = tqsdk_data::Result<BacktestHistoryCredentials>> + Send + 'a>>
    {
        Box::pin(async move {
            if self.user.trim().is_empty() || self.pass.is_empty() {
                return Err(DataError::Validation(
                    "remote backtest cache fill requires auth".to_string(),
                ));
            }
            Ok(BacktestHistoryCredentials::new(
                self.user.clone(),
                self.pass.clone(),
            ))
        })
    }
}

fn parse_remote_fill_idle_timeout(value: Option<&str>) -> Duration {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(60))
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        BacktestRemoteFillConfig, FacadeHistoryFillKind, MaterializedHistoryProgress,
        RemoteBacktestCacheFillRequest, RemoteCacheCommitMode,
        ensure_remote_main_contract_metadata, final_tick_compaction_ranges,
        parse_remote_fill_allow_empty_idle, parse_remote_fill_batch_timeout,
        parse_remote_fill_idle_timeout, parse_remote_fill_slice_ns,
        parse_remote_fill_symbol_batch_size, parse_remote_fill_symbol_concurrency,
        remote_fill_batches, remote_fill_logical_batch_count, remote_fill_ranges,
        should_reject_empty_remote_tick_fill, split_remote_fill_requests,
    };
    use tqsdk_data::{BacktestHistoryPhase, BacktestHistoryTelemetryEvent};

    #[tokio::test]
    async fn retained_metadata_coverage_does_not_require_remote_refresh() {
        let root = test_root("retained-metadata-coverage");
        let cache = tqsdk_data::BacktestHistoryMetadataCache::open(&root).unwrap();
        cache.store_snapshot(snapshot(1_000, 2_000, 1)).unwrap();
        cache.store_snapshot(snapshot(3_000, 4_000, 2)).unwrap();

        ensure_remote_main_contract_metadata(
            None,
            &root,
            ["KQ.m@SHFE.au".to_string()],
            1_000,
            2_000,
        )
        .await
        .expect("a retained covering snapshot must remain an offline cache hit");

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn remote_fill_config_normalizes_legacy_values() {
        assert_eq!(parse_remote_fill_symbol_batch_size(None), 1);
        assert_eq!(parse_remote_fill_symbol_batch_size(Some("0")), 1);
        assert_eq!(parse_remote_fill_symbol_batch_size(Some("8")), 4);
        assert_eq!(parse_remote_fill_symbol_concurrency(None), 2);
        assert_eq!(parse_remote_fill_symbol_concurrency(Some("0")), 2);
        assert_eq!(parse_remote_fill_symbol_concurrency(Some("8")), 4);
        assert_eq!(
            parse_remote_fill_idle_timeout(Some("0")),
            Duration::from_secs(60)
        );
        assert_eq!(parse_remote_fill_batch_timeout(Some("0")), Duration::ZERO);
        assert_eq!(parse_remote_fill_slice_ns(Some("60")), Some(60_000_000_000));
        assert!(parse_remote_fill_allow_empty_idle(Some("yes")));
    }

    #[test]
    fn batches_only_group_equal_range_and_commit_mode() {
        let requests = vec![
            RemoteBacktestCacheFillRequest::new("SHFE.au2608", 10, 20),
            RemoteBacktestCacheFillRequest::new("SHFE.ag2608", 10, 20),
            RemoteBacktestCacheFillRequest::provisional("SHFE.rb2608", 10, 20, 10, 20),
        ];
        let batches = remote_fill_batches(requests.clone(), 2).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(remote_fill_logical_batch_count(requests, 2).unwrap(), 2);
        assert!(matches!(
            batches[1].commit_mode,
            RemoteCacheCommitMode::Provisional { .. }
        ));
    }

    #[test]
    fn explicit_slice_keeps_coverage_requests_disjoint() {
        let config = BacktestRemoteFillConfig::default().with_slice(Some(Duration::from_nanos(3)));
        assert_eq!(
            remote_fill_ranges(0, 8, config),
            vec![(0, 3), (3, 6), (6, 8)]
        );
        let split = split_remote_fill_requests(
            vec![RemoteBacktestCacheFillRequest::new("SHFE.au2608", 0, 8)],
            config,
        )
        .unwrap();
        assert_eq!(split.len(), 3);
    }

    #[test]
    fn explicitly_terminal_empty_tick_fill_does_not_require_legacy_opt_in() {
        assert!(!should_reject_empty_remote_tick_fill(
            FacadeHistoryFillKind::Tick,
            0,
            false,
            false,
            true,
        ));
        assert!(should_reject_empty_remote_tick_fill(
            FacadeHistoryFillKind::Tick,
            0,
            false,
            false,
            false,
        ));
        assert!(!should_reject_empty_remote_tick_fill(
            FacadeHistoryFillKind::Tick,
            0,
            false,
            true,
            false,
        ));
        assert!(!should_reject_empty_remote_tick_fill(
            FacadeHistoryFillKind::Tick,
            0,
            true,
            false,
            false,
        ));
        assert!(!should_reject_empty_remote_tick_fill(
            FacadeHistoryFillKind::CanonicalMinute,
            0,
            false,
            false,
            false,
        ));
    }

    #[test]
    fn final_tick_compaction_uses_only_actual_filled_ranges_and_deduplicates_partitions() {
        let filled_ranges = std::collections::BTreeMap::from([
            ("SHFE.au2608".to_string(), vec![(10, 20), (25, 30)]),
            ("DCE.i2609".to_string(), Vec::new()),
        ]);

        let ranges =
            final_tick_compaction_ranges(FacadeHistoryFillKind::Tick, &filled_ranges).unwrap();
        let day = tqsdk_data::backtest_tick_trading_day_for_timestamp_ns(10).unwrap();
        let partition = tqsdk_data::backtest_tick_trading_day_range(day).unwrap();

        assert_eq!(ranges.len(), 1);
        assert_eq!(
            ranges["SHFE.au2608"],
            vec![(partition.start_ns, partition.end_ns)]
        );
        assert!(
            final_tick_compaction_ranges(FacadeHistoryFillKind::CanonicalMinute, &filled_ranges,)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn materialized_history_progress_keeps_minute_windows_monotonic() {
        let mut progress = MaterializedHistoryProgress::default();
        let event = |phase, completed_rows| BacktestHistoryTelemetryEvent {
            request_id: Some(1),
            symbol: "KQ.m@SHFE.au".to_string(),
            phase,
            completed_rows,
            message: "buffering canonical-minute rows".to_string(),
        };

        assert_eq!(
            progress.observe(&event(BacktestHistoryPhase::Fill, 80)),
            (80, true)
        );
        assert_eq!(
            progress.observe(&event(BacktestHistoryPhase::Fill, 120)),
            (120, true)
        );
        assert_eq!(
            progress.observe(&event(BacktestHistoryPhase::WaitForFill, 0)),
            (120, false)
        );
        // A retry keeps the data layer's cumulative unique-row counter.
        assert_eq!(
            progress.observe(&event(BacktestHistoryPhase::Retry, 0)),
            (120, false)
        );
        assert_eq!(
            progress.observe(&event(BacktestHistoryPhase::Fill, 150)),
            (150, true)
        );
        // The next bounded source window emits an explicit zero before rows.
        assert_eq!(
            progress.observe(&event(BacktestHistoryPhase::Fill, 0)),
            (150, false)
        );
        assert_eq!(
            progress.observe(&event(BacktestHistoryPhase::Fill, 200)),
            (350, true)
        );
        assert_eq!(
            progress.observe(&event(BacktestHistoryPhase::Aggregate, 350)),
            (350, false)
        );

        let mut coalesced = MaterializedHistoryProgress::default();
        assert_eq!(
            coalesced.observe(&event(BacktestHistoryPhase::Fill, 120)),
            (120, true)
        );
        // The zero reset can be coalesced away; aggregate terminal restores
        // the authoritative physical-write total.
        assert_eq!(
            coalesced.observe(&event(BacktestHistoryPhase::Fill, 200)),
            (200, true)
        );
        assert_eq!(
            coalesced.observe(&event(BacktestHistoryPhase::Aggregate, 320)),
            (320, true)
        );
    }

    fn snapshot(
        start_ns: i64,
        end_ns: i64,
        captured_at_ns: i64,
    ) -> tqsdk_data::BacktestHistoryMetadataSnapshot {
        tqsdk_data::BacktestHistoryMetadataSnapshot {
            schema_version: tqsdk_data::BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
            market_kind: tqsdk_data::BacktestHistoryMarketKind::Futures,
            logical_symbol: "KQ.m@SHFE.au".to_string(),
            captured_at_ns,
            trading_days: vec![tqsdk_data::BacktestHistoryTradingDay {
                date: "2026-01-05".to_string(),
                is_trading_day: true,
                start_ns,
                end_ns,
            }],
            session: tqsdk_data::KlineSessionTemplate::cst_trading_day(),
            physical_segments: vec![tqsdk_data::BacktestHistoryPhysicalSegment {
                physical_symbol: "SHFE.au2606".to_string(),
                start_ns,
                end_ns,
            }],
            snapshot_hash: String::new(),
        }
    }

    fn test_root(label: &str) -> std::path::PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "tqsdk-backtest-remote-{label}-{}-{unique}",
            std::process::id()
        ))
    }
}
