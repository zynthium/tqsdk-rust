use std::collections::{BTreeMap, VecDeque};
use std::future::pending;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::aggregation::CANONICAL_MINUTE_KLINE_NS;
use crate::backtest_tick_cache::{BacktestTickCache, BacktestTickCacheOperationLock};
use crate::daily_kline_cache::DAILY_KLINE_DURATION_NS;
use crate::error::{DataError, Result};

use super::executor::BacktestHistoryExecutionMode;
use super::request::ValidatedBacktestHistoryRequest;
use super::{
    BacktestHistoryClient, BacktestHistoryKind, BacktestHistoryPolicy, BacktestHistoryRequest,
    BacktestHistoryRequestId, BacktestHistoryTelemetryEvent,
};

const DEFAULT_SYMBOL_BATCH_SIZE: usize = 1;
const DEFAULT_SYMBOL_CONCURRENCY: usize = 2;
const MAX_SYMBOL_BATCH_SIZE: usize = 4;
const MAX_SYMBOL_CONCURRENCY: usize = 4;
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Cache family materialized by one history-fill request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BacktestHistoryFillFamily {
    /// Durable Tick history partitions.
    Tick,
    /// Canonical server-side one-minute Kline history.
    Minute,
    /// Native server-side one-day Kline history.
    Daily,
}

/// Validated scheduling and timeout settings shared by all history cache fills.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BacktestHistoryFillConfig {
    symbol_batch_size: usize,
    symbol_concurrency: usize,
    idle_timeout: Duration,
    batch_timeout: Option<Duration>,
    lock_wait: Option<Duration>,
}

impl Default for BacktestHistoryFillConfig {
    fn default() -> Self {
        Self {
            symbol_batch_size: DEFAULT_SYMBOL_BATCH_SIZE,
            symbol_concurrency: DEFAULT_SYMBOL_CONCURRENCY,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            batch_timeout: None,
            lock_wait: None,
        }
    }
}

impl BacktestHistoryFillConfig {
    /// Sets symbols per batch. Values outside `1..=4` are rejected.
    pub fn with_symbol_batch_size(mut self, value: usize) -> Result<Self> {
        validate_bounded_count("symbol_batch_size", value, MAX_SYMBOL_BATCH_SIZE)?;
        self.symbol_batch_size = value;
        Ok(self)
    }

    /// Sets concurrently active symbol batches. Values outside `1..=4` are rejected.
    pub fn with_symbol_concurrency(mut self, value: usize) -> Result<Self> {
        validate_bounded_count("symbol_concurrency", value, MAX_SYMBOL_CONCURRENCY)?;
        self.symbol_concurrency = value;
        Ok(self)
    }

    /// Sets the maximum interval without observable fill progress.
    pub fn with_idle_timeout(mut self, value: Duration) -> Result<Self> {
        validate_nonzero_duration("idle_timeout", value)?;
        self.idle_timeout = value;
        Ok(self)
    }

    /// Sets or disables the maximum wall time for one symbol batch.
    pub fn with_batch_timeout(mut self, value: Option<Duration>) -> Result<Self> {
        if let Some(value) = value {
            validate_nonzero_duration("batch_timeout", value)?;
        }
        self.batch_timeout = value;
        Ok(self)
    }

    /// Disables the per-batch wall-clock timeout.
    #[must_use]
    pub fn without_batch_timeout(mut self) -> Self {
        self.batch_timeout = None;
        self
    }

    /// Sets or disables waiting for the shared cache-root fill lock.
    pub fn with_lock_wait(mut self, value: Option<Duration>) -> Result<Self> {
        if let Some(value) = value {
            validate_nonzero_duration("lock_wait", value)?;
        }
        self.lock_wait = value;
        Ok(self)
    }

    #[must_use]
    pub const fn symbol_batch_size(self) -> usize {
        self.symbol_batch_size
    }

    #[must_use]
    pub const fn symbol_concurrency(self) -> usize {
        self.symbol_concurrency
    }

    #[must_use]
    pub const fn idle_timeout(self) -> Duration {
        self.idle_timeout
    }

    #[must_use]
    pub const fn batch_timeout(self) -> Option<Duration> {
        self.batch_timeout
    }

    #[must_use]
    pub const fn lock_wait(self) -> Option<Duration> {
        self.lock_wait
    }
}

fn validate_bounded_count(name: &str, value: usize, maximum: usize) -> Result<()> {
    if !(1..=maximum).contains(&value) {
        return Err(DataError::Validation(format!(
            "backtest history fill {name} must be between 1 and {maximum}, got {value}"
        )));
    }
    Ok(())
}

fn validate_nonzero_duration(name: &str, value: Duration) -> Result<()> {
    if value.is_zero() {
        return Err(DataError::Validation(format!(
            "backtest history fill {name} must be greater than zero"
        )));
    }
    Ok(())
}

/// Cloneable, monotonic cancellation signal for one fill orchestration run.
#[derive(Clone, Default)]
pub struct BacktestHistoryFillCancellation {
    state: Arc<BacktestHistoryFillCancellationState>,
}

#[derive(Default)]
struct BacktestHistoryFillCancellationState {
    cancelled: AtomicBool,
    notify: Notify,
}

impl std::fmt::Debug for BacktestHistoryFillCancellation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BacktestHistoryFillCancellation")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

impl BacktestHistoryFillCancellation {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Requests cancellation. Accepted cache rows are flushed before the run ends.
    pub fn cancel(&self) {
        self.state.cancelled.store(true, Ordering::Release);
        self.state.notify.notify_waiters();
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    /// Waits until cancellation is requested.
    pub async fn cancelled(&self) {
        loop {
            let notified = self.state.notify.notified();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

/// Normalized lifecycle and telemetry updates for Tick, minute, and daily fills.
#[derive(Debug, Clone)]
pub enum BacktestHistoryFillProgress {
    Planning {
        family: BacktestHistoryFillFamily,
        requested_symbols: usize,
        total_batches: usize,
        symbol_batch_size: usize,
        symbol_concurrency: usize,
    },
    BatchStarted {
        family: BacktestHistoryFillFamily,
        batch_number: usize,
        total_batches: usize,
        requested_range: (i64, i64),
        pending_batches: usize,
        active_batches: usize,
        symbols: Vec<String>,
    },
    Telemetry {
        family: BacktestHistoryFillFamily,
        batch_number: usize,
        total_batches: usize,
        requested_range: (i64, i64),
        event: BacktestHistoryTelemetryEvent,
    },
    BatchFinished {
        family: BacktestHistoryFillFamily,
        batch_number: usize,
        total_batches: usize,
        requested_range: (i64, i64),
        symbols: Vec<String>,
        rows_written: usize,
        elapsed: Duration,
    },
    BatchFailed {
        family: BacktestHistoryFillFamily,
        batch_number: usize,
        total_batches: usize,
        requested_range: (i64, i64),
        symbols: Vec<String>,
        error: String,
    },
    Finished {
        status: BacktestHistoryFillTerminalStatus,
        completed_symbols: usize,
        failed_symbols: usize,
        interrupted_symbols: usize,
        rows_written: usize,
    },
}

/// Terminal outcome for one symbol request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktestHistoryFillSymbolStatus {
    Complete,
    Failed,
    Interrupted,
}

/// Terminal outcome for a complete orchestration run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktestHistoryFillTerminalStatus {
    Complete,
    Failed,
    Interrupted,
}

/// Durable fill result for one logical symbol and requested range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestHistoryFillSymbolResult {
    pub request_id: BacktestHistoryRequestId,
    pub symbol: String,
    pub family: BacktestHistoryFillFamily,
    pub requested_range: (i64, i64),
    pub status: BacktestHistoryFillSymbolStatus,
    pub rows_written: usize,
    pub remote_used: bool,
    pub remote_filled_ranges: Vec<(i64, i64)>,
    pub error: Option<String>,
}

/// All symbol outcomes returned after a fill run reaches a terminal state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestHistoryFillTerminalReport {
    status: BacktestHistoryFillTerminalStatus,
    symbols: Vec<BacktestHistoryFillSymbolResult>,
    completed_symbols: usize,
    failed_symbols: usize,
    interrupted_symbols: usize,
    rows_written: usize,
}

impl BacktestHistoryFillTerminalReport {
    #[must_use]
    pub fn from_symbols(symbols: Vec<BacktestHistoryFillSymbolResult>) -> Self {
        let completed_symbols = count_status(&symbols, BacktestHistoryFillSymbolStatus::Complete);
        let failed_symbols = count_status(&symbols, BacktestHistoryFillSymbolStatus::Failed);
        let interrupted_symbols =
            count_status(&symbols, BacktestHistoryFillSymbolStatus::Interrupted);
        let rows_written = symbols.iter().map(|symbol| symbol.rows_written).sum();
        let status = if failed_symbols > 0 {
            BacktestHistoryFillTerminalStatus::Failed
        } else if interrupted_symbols > 0 {
            BacktestHistoryFillTerminalStatus::Interrupted
        } else {
            BacktestHistoryFillTerminalStatus::Complete
        };
        Self {
            status,
            symbols,
            completed_symbols,
            failed_symbols,
            interrupted_symbols,
            rows_written,
        }
    }

    #[must_use]
    pub const fn status(&self) -> BacktestHistoryFillTerminalStatus {
        self.status
    }

    #[must_use]
    pub fn symbols(&self) -> &[BacktestHistoryFillSymbolResult] {
        &self.symbols
    }

    #[must_use]
    pub const fn completed_symbols(&self) -> usize {
        self.completed_symbols
    }

    #[must_use]
    pub const fn failed_symbols(&self) -> usize {
        self.failed_symbols
    }

    #[must_use]
    pub const fn interrupted_symbols(&self) -> usize {
        self.interrupted_symbols
    }

    #[must_use]
    pub const fn rows_written(&self) -> usize {
        self.rows_written
    }
}

fn count_status(
    symbols: &[BacktestHistoryFillSymbolResult],
    status: BacktestHistoryFillSymbolStatus,
) -> usize {
    symbols
        .iter()
        .filter(|symbol| symbol.status == status)
        .count()
}

#[derive(Debug, Clone)]
struct FillRequestMeta {
    request_id: BacktestHistoryRequestId,
    symbol: String,
    family: BacktestHistoryFillFamily,
    requested_range: (i64, i64),
}

#[derive(Debug, Clone)]
struct FillBatch {
    number: usize,
    total: usize,
    family: BacktestHistoryFillFamily,
    requested_range: (i64, i64),
    pending_batches: usize,
    active_batches: usize,
    requests: Vec<ValidatedBacktestHistoryRequest>,
    meta: Vec<FillRequestMeta>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct FillBatchKey {
    family: BacktestHistoryFillFamily,
    requested_range: (i64, i64),
    provisional_as_of_ns: Option<i64>,
}

struct FillBatchOutcome {
    results: Vec<BacktestHistoryFillSymbolResult>,
}

impl BacktestHistoryClient {
    /// Materializes cache coverage using one bounded scheduler for Tick,
    /// canonical-minute, and native-daily requests.
    pub async fn orchestrate_fill<F>(
        &self,
        requests: impl IntoIterator<Item = BacktestHistoryRequest>,
        config: BacktestHistoryFillConfig,
        cancellation: BacktestHistoryFillCancellation,
        observer: F,
    ) -> Result<BacktestHistoryFillTerminalReport>
    where
        F: Fn(BacktestHistoryFillProgress) + Send + Sync + 'static,
    {
        self.orchestrate_fill_inner(requests, config, cancellation, None, Arc::new(observer))
            .await
    }

    /// Uses a caller-owned cache-root gate while retaining data-layer fill scheduling.
    #[doc(hidden)]
    pub async fn orchestrate_fill_with_root_gate<F>(
        &self,
        requests: impl IntoIterator<Item = BacktestHistoryRequest>,
        config: BacktestHistoryFillConfig,
        cancellation: BacktestHistoryFillCancellation,
        root_gate: Arc<BacktestTickCacheOperationLock>,
        observer: F,
    ) -> Result<BacktestHistoryFillTerminalReport>
    where
        F: Fn(BacktestHistoryFillProgress) + Send + Sync + 'static,
    {
        self.orchestrate_fill_inner(
            requests,
            config,
            cancellation,
            Some(root_gate),
            Arc::new(observer),
        )
        .await
    }

    async fn orchestrate_fill_inner(
        &self,
        requests: impl IntoIterator<Item = BacktestHistoryRequest>,
        config: BacktestHistoryFillConfig,
        cancellation: BacktestHistoryFillCancellation,
        supplied_root_gate: Option<Arc<BacktestTickCacheOperationLock>>,
        observer: Arc<dyn Fn(BacktestHistoryFillProgress) + Send + Sync>,
    ) -> Result<BacktestHistoryFillTerminalReport> {
        let validated = super::validate_requests(requests)?;
        let mut grouped = BTreeMap::<FillBatchKey, Vec<ValidatedBacktestHistoryRequest>>::new();
        for request in validated {
            let key = FillBatchKey {
                family: fill_family(&request),
                requested_range: (request.start_ns, request.end_ns),
                provisional_as_of_ns: request.provisional_as_of_ns,
            };
            grouped.entry(key).or_default().push(request);
        }

        let total_batches = grouped
            .values()
            .map(|requests| requests.len().div_ceil(config.symbol_batch_size))
            .sum::<usize>();
        let mut requested_by_family = BTreeMap::<BacktestHistoryFillFamily, usize>::new();
        for (key, requests) in &grouped {
            *requested_by_family.entry(key.family).or_default() += requests.len();
        }
        for (family, requested_symbols) in requested_by_family {
            observer(BacktestHistoryFillProgress::Planning {
                family,
                requested_symbols,
                total_batches,
                symbol_batch_size: config.symbol_batch_size,
                symbol_concurrency: config.symbol_concurrency,
            });
        }

        let all_meta = grouped
            .iter()
            .flat_map(|(key, requests)| {
                requests
                    .iter()
                    .map(|request| request_meta(request, key.family))
            })
            .collect::<Vec<_>>();
        if cancellation.is_cancelled() {
            return Ok(finish_interrupted(all_meta, observer.as_ref()));
        }

        let root_gate = if let Some(root_gate) = supplied_root_gate {
            validate_root_gate(self, &root_gate)?;
            Some(root_gate)
        } else {
            self.acquire_orchestration_root_gate(config.lock_wait, &cancellation)
                .await?
        };
        if cancellation.is_cancelled() {
            return Ok(finish_interrupted(all_meta, observer.as_ref()));
        }

        let mut batches = VecDeque::new();
        for (key, requests) in grouped {
            for chunk in requests.chunks(config.symbol_batch_size) {
                let requests = chunk.to_vec();
                let meta = requests
                    .iter()
                    .map(|request| request_meta(request, key.family))
                    .collect();
                batches.push_back(FillBatch {
                    number: 0,
                    total: total_batches,
                    family: key.family,
                    requested_range: key.requested_range,
                    pending_batches: 0,
                    active_batches: 0,
                    requests,
                    meta,
                });
            }
        }
        for (index, batch) in batches.iter_mut().enumerate() {
            batch.number = index + 1;
        }

        let mut tasks = JoinSet::new();
        let mut results = Vec::with_capacity(all_meta.len());
        while !batches.is_empty() || !tasks.is_empty() {
            while tasks.len() < config.symbol_concurrency
                && !batches.is_empty()
                && !cancellation.is_cancelled()
            {
                let mut batch = batches.pop_front().expect("checked non-empty fill queue");
                batch.pending_batches = batches.len();
                batch.active_batches = tasks.len().saturating_add(1);
                let client = self.clone();
                let cancellation = cancellation.clone();
                let observer = Arc::clone(&observer);
                let root_gate = root_gate.clone();
                tasks.spawn(async move {
                    execute_fill_batch(client, batch, config, cancellation, root_gate, observer)
                        .await
                });
            }

            if tasks.is_empty() {
                break;
            }
            let joined = tasks
                .join_next()
                .await
                .ok_or(DataError::InvalidState("fill task set ended unexpectedly"))?;
            let outcome = joined
                .map_err(|_| DataError::InvalidState("history fill batch task failed to join"))??;
            results.extend(outcome.results);
        }

        for batch in batches {
            results.extend(symbol_results(
                batch.meta,
                None,
                FillStop::Interrupted("cancelled".to_string()),
            ));
        }
        results.sort_by_key(|symbol| symbol.request_id);
        let report = BacktestHistoryFillTerminalReport::from_symbols(results);
        emit_finished(observer.as_ref(), &report);
        Ok(report)
    }

    async fn acquire_orchestration_root_gate(
        &self,
        lock_wait: Option<Duration>,
        cancellation: &BacktestHistoryFillCancellation,
    ) -> Result<Option<Arc<BacktestTickCacheOperationLock>>> {
        if self.config.policy == BacktestHistoryPolicy::CacheOnly {
            return Ok(None);
        }
        let cache = BacktestTickCache::open(self.config.cache_dir.as_path())?;
        let try_acquire = || cache.try_acquire_remote_fill_shared_lock().map(Arc::new);
        let Some(lock_wait) = lock_wait else {
            return try_acquire().map(Some);
        };
        let deadline = Instant::now() + lock_wait;
        loop {
            if cancellation.is_cancelled() {
                return Ok(None);
            }
            match try_acquire() {
                Ok(gate) => return Ok(Some(gate)),
                Err(DataError::CacheBusy { .. }) if Instant::now() < deadline => {
                    let delay = deadline
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_millis(200));
                    tokio::select! {
                        () = cancellation.cancelled() => return Ok(None),
                        () = tokio::time::sleep(delay) => {}
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }
}

fn validate_root_gate(
    client: &BacktestHistoryClient,
    root_gate: &BacktestTickCacheOperationLock,
) -> Result<()> {
    if root_gate.cache_dir() != client.config.cache_dir.as_path() {
        return Err(DataError::Validation(format!(
            "backtest history root gate {} does not match cache root {}",
            root_gate.cache_dir().display(),
            client.config.cache_dir.display(),
        )));
    }
    Ok(())
}

async fn execute_fill_batch(
    client: BacktestHistoryClient,
    batch: FillBatch,
    config: BacktestHistoryFillConfig,
    cancellation: BacktestHistoryFillCancellation,
    root_gate: Option<Arc<BacktestTickCacheOperationLock>>,
    observer: Arc<dyn Fn(BacktestHistoryFillProgress) + Send + Sync>,
) -> Result<FillBatchOutcome> {
    let started = Instant::now();
    let symbols = batch
        .meta
        .iter()
        .map(|request| request.symbol.clone())
        .collect::<Vec<_>>();
    observer(BacktestHistoryFillProgress::BatchStarted {
        family: batch.family,
        batch_number: batch.number,
        total_batches: batch.total,
        requested_range: batch.requested_range,
        pending_batches: batch.pending_batches,
        active_batches: batch.active_batches,
        symbols: symbols.clone(),
    });

    let mut run = client.start_run(
        batch.requests,
        BacktestHistoryExecutionMode::MaterializeCache,
        root_gate,
    )?;
    let mut telemetry = run.take_telemetry();
    let mut events_open = true;
    let mut telemetry_open = telemetry.is_some();
    let mut idle_deadline = Instant::now() + config.idle_timeout;
    let batch_deadline = config.batch_timeout.map(|timeout| Instant::now() + timeout);
    let stop = loop {
        if !events_open && !telemetry_open {
            break FillStop::Completed;
        }
        tokio::select! {
            () = cancellation.cancelled() => {
                break FillStop::Interrupted("cancelled".to_string());
            }
            () = tokio::time::sleep_until(idle_deadline) => {
                break FillStop::Failed(format!(
                    "history fill batch made no progress for {:?}",
                    config.idle_timeout,
                ));
            }
            () = wait_for_deadline(batch_deadline) => {
                break FillStop::Failed(format!(
                    "history fill batch exceeded {:?}",
                    config.batch_timeout.expect("deadline requires timeout"),
                ));
            }
            event = run.next(), if events_open => {
                match event {
                    Some(_) => idle_deadline = Instant::now() + config.idle_timeout,
                    None => events_open = false,
                }
            }
            event = next_telemetry(&mut telemetry), if telemetry_open => {
                match event {
                    Some(event) => {
                        idle_deadline = Instant::now() + config.idle_timeout;
                        observer(BacktestHistoryFillProgress::Telemetry {
                            family: batch.family,
                            batch_number: batch.number,
                            total_batches: batch.total,
                            requested_range: batch.requested_range,
                            event,
                        });
                    }
                    None => telemetry_open = false,
                }
            }
        }
    };

    let run_report = match stop {
        FillStop::Completed => run.finish().await,
        FillStop::Failed(_) | FillStop::Interrupted(_) => run.cancel_and_finish().await,
    };
    let results = symbol_results(batch.meta, Some(run_report), stop.clone());
    let rows_written = results.iter().map(|result| result.rows_written).sum();
    match stop {
        FillStop::Completed
            if results
                .iter()
                .all(|result| result.status == BacktestHistoryFillSymbolStatus::Complete) =>
        {
            observer(BacktestHistoryFillProgress::BatchFinished {
                family: batch.family,
                batch_number: batch.number,
                total_batches: batch.total,
                requested_range: batch.requested_range,
                symbols,
                rows_written,
                elapsed: started.elapsed(),
            });
        }
        stop => {
            let error = stop.message().unwrap_or_else(|| {
                results
                    .iter()
                    .find_map(|result| result.error.clone())
                    .unwrap_or_else(|| "history fill batch failed".to_string())
            });
            observer(BacktestHistoryFillProgress::BatchFailed {
                family: batch.family,
                batch_number: batch.number,
                total_batches: batch.total,
                requested_range: batch.requested_range,
                symbols,
                error: error.clone(),
            });
        }
    }
    Ok(FillBatchOutcome { results })
}

#[derive(Debug, Clone)]
enum FillStop {
    Completed,
    Failed(String),
    Interrupted(String),
}

impl FillStop {
    fn message(&self) -> Option<String> {
        match self {
            Self::Completed => None,
            Self::Failed(message) | Self::Interrupted(message) => Some(message.clone()),
        }
    }
}

async fn wait_for_deadline(deadline: Option<Instant>) {
    if let Some(deadline) = deadline {
        tokio::time::sleep_until(deadline).await;
    } else {
        pending::<()>().await;
    }
}

async fn next_telemetry(
    telemetry: &mut Option<super::BacktestHistoryTelemetryStream>,
) -> Option<BacktestHistoryTelemetryEvent> {
    match telemetry {
        Some(telemetry) => telemetry.next().await,
        None => pending().await,
    }
}

fn fill_family(request: &ValidatedBacktestHistoryRequest) -> BacktestHistoryFillFamily {
    match request.kind {
        BacktestHistoryKind::Tick => BacktestHistoryFillFamily::Tick,
        BacktestHistoryKind::Kline { .. }
            if request
                .duration_ns
                .is_some_and(|duration| duration < CANONICAL_MINUTE_KLINE_NS) =>
        {
            BacktestHistoryFillFamily::Tick
        }
        BacktestHistoryKind::Kline { .. }
            if request
                .duration_ns
                .is_some_and(|duration| duration < DAILY_KLINE_DURATION_NS) =>
        {
            BacktestHistoryFillFamily::Minute
        }
        BacktestHistoryKind::Kline { .. } => BacktestHistoryFillFamily::Daily,
    }
}

fn request_meta(
    request: &ValidatedBacktestHistoryRequest,
    family: BacktestHistoryFillFamily,
) -> FillRequestMeta {
    FillRequestMeta {
        request_id: request.request_id,
        symbol: request.symbol.clone(),
        family,
        requested_range: (request.start_ns, request.end_ns),
    }
}

fn symbol_results(
    meta: Vec<FillRequestMeta>,
    report: Option<super::BacktestHistoryBatchReport>,
    stop: FillStop,
) -> Vec<BacktestHistoryFillSymbolResult> {
    let mut completed = BTreeMap::new();
    let mut failed = BTreeMap::new();
    if let Some(report) = report {
        completed.extend(
            report
                .completed
                .into_iter()
                .map(|report| (report.request_id, report)),
        );
        failed.extend(
            report
                .failed
                .into_iter()
                .map(|report| (report.request_id, report)),
        );
    }
    meta.into_iter()
        .map(|meta| {
            if let Some(report) = completed.remove(&meta.request_id) {
                return BacktestHistoryFillSymbolResult {
                    request_id: meta.request_id,
                    symbol: meta.symbol,
                    family: meta.family,
                    requested_range: meta.requested_range,
                    status: BacktestHistoryFillSymbolStatus::Complete,
                    rows_written: report.rows,
                    remote_used: report.remote_used,
                    remote_filled_ranges: report.coverage.remote_filled_ranges,
                    error: None,
                };
            }
            if let Some(report) = failed.remove(&meta.request_id) {
                let (status, error) = match &stop {
                    FillStop::Completed => (BacktestHistoryFillSymbolStatus::Failed, report.error),
                    FillStop::Failed(message) => {
                        (BacktestHistoryFillSymbolStatus::Failed, message.clone())
                    }
                    FillStop::Interrupted(message) => (
                        BacktestHistoryFillSymbolStatus::Interrupted,
                        message.clone(),
                    ),
                };
                return BacktestHistoryFillSymbolResult {
                    request_id: meta.request_id,
                    symbol: meta.symbol,
                    family: meta.family,
                    requested_range: meta.requested_range,
                    status,
                    rows_written: report.emitted_rows,
                    remote_used: false,
                    remote_filled_ranges: Vec::new(),
                    error: Some(error),
                };
            }
            let (status, error) = match &stop {
                FillStop::Completed => (
                    BacktestHistoryFillSymbolStatus::Failed,
                    "history fill ended without a terminal request outcome".to_string(),
                ),
                FillStop::Failed(message) => {
                    (BacktestHistoryFillSymbolStatus::Failed, message.clone())
                }
                FillStop::Interrupted(message) => (
                    BacktestHistoryFillSymbolStatus::Interrupted,
                    message.clone(),
                ),
            };
            BacktestHistoryFillSymbolResult {
                request_id: meta.request_id,
                symbol: meta.symbol,
                family: meta.family,
                requested_range: meta.requested_range,
                status,
                rows_written: 0,
                remote_used: false,
                remote_filled_ranges: Vec::new(),
                error: Some(error),
            }
        })
        .collect()
}

fn finish_interrupted(
    meta: Vec<FillRequestMeta>,
    observer: &(dyn Fn(BacktestHistoryFillProgress) + Send + Sync),
) -> BacktestHistoryFillTerminalReport {
    let report = BacktestHistoryFillTerminalReport::from_symbols(symbol_results(
        meta,
        None,
        FillStop::Interrupted("cancelled".to_string()),
    ));
    emit_finished(observer, &report);
    report
}

fn emit_finished(
    observer: &(dyn Fn(BacktestHistoryFillProgress) + Send + Sync),
    report: &BacktestHistoryFillTerminalReport,
) {
    observer(BacktestHistoryFillProgress::Finished {
        status: report.status,
        completed_symbols: report.completed_symbols,
        failed_symbols: report.failed_symbols,
        interrupted_symbols: report.interrupted_symbols,
        rows_written: report.rows_written,
    });
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tqsdk_session::{ServerBacktestHistoryEvent, ServerBacktestHistoryRequest};

    use super::*;
    use crate::backtest_history::fill::{ServerHistorySource, ServerHistorySourceFactory};
    use crate::backtest_history::request::{
        BacktestHistoryAuthProvider, BacktestHistoryClientConfig, BacktestHistoryCredentials,
        DEFAULT_COLLECT_LIMIT_BYTES, DEFAULT_PER_SYMBOL_BUFFER_BYTES,
    };
    use crate::backtest_tick_cache::backtest_tick_trading_day_range;

    #[tokio::test]
    async fn idle_timeout_cancels_the_batch_and_preserves_its_cause() {
        let opens = Arc::new(AtomicUsize::new(0));
        let root = temporary_root("idle-timeout");
        let client = client_with_never_source(root.clone(), Arc::clone(&opens));
        let range = closed_range();
        let progress = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = Arc::clone(&progress);
        let config = BacktestHistoryFillConfig::default()
            .with_idle_timeout(Duration::from_millis(200))
            .unwrap();

        let report = client
            .orchestrate_fill(
                [BacktestHistoryRequest::tick(
                    1,
                    "SHFE.au2608",
                    range.start_ns,
                    range.end_ns,
                )],
                config,
                BacktestHistoryFillCancellation::new(),
                move |event| observed.lock().unwrap().push(event),
            )
            .await
            .unwrap();

        assert_eq!(report.status(), BacktestHistoryFillTerminalStatus::Failed);
        assert!(
            report.symbols()[0]
                .error
                .as_deref()
                .is_some_and(|error| error.contains("no progress"))
        );
        assert!(progress.lock().unwrap().iter().any(|event| matches!(
            event,
            BacktestHistoryFillProgress::BatchFailed { error, .. }
                if error.contains("no progress")
        )));
        assert!(opens.load(Ordering::SeqCst) <= 1);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn batch_timeout_is_distinct_from_idle_timeout() {
        let root = temporary_root("batch-timeout");
        let client = client_with_never_source(root.clone(), Arc::new(AtomicUsize::new(0)));
        let range = closed_range();
        let config = BacktestHistoryFillConfig::default()
            .with_idle_timeout(Duration::from_secs(1))
            .unwrap()
            .with_batch_timeout(Some(Duration::from_millis(200)))
            .unwrap();

        let report = client
            .orchestrate_fill(
                [BacktestHistoryRequest::tick(
                    2,
                    "SHFE.au2608",
                    range.start_ns,
                    range.end_ns,
                )],
                config,
                BacktestHistoryFillCancellation::new(),
                |_| {},
            )
            .await
            .unwrap();

        assert!(
            report.symbols()[0]
                .error
                .as_deref()
                .is_some_and(|error| error.contains("exceeded"))
        );
        let _ = std::fs::remove_dir_all(root);
    }

    fn client_with_never_source(
        cache_dir: std::path::PathBuf,
        opens: Arc<AtomicUsize>,
    ) -> BacktestHistoryClient {
        BacktestHistoryClient::from_config(BacktestHistoryClientConfig {
            cache_dir,
            policy: BacktestHistoryPolicy::RemoteOnMiss,
            logical_concurrency: 4,
            blocking_workers: 1,
            per_symbol_buffer_bytes: DEFAULT_PER_SYMBOL_BUFFER_BYTES,
            collect_limit_bytes: DEFAULT_COLLECT_LIMIT_BYTES,
            auth_provider: Some(Arc::new(StaticAuth)),
            source_factory: Arc::new(NeverSourceFactory { opens }),
        })
    }

    fn closed_range() -> crate::BacktestTickTradingDayRange {
        backtest_tick_trading_day_range(
            chrono::NaiveDate::from_ymd_opt(2026, 8, 10).expect("valid fixture date"),
        )
        .unwrap()
    }

    fn temporary_root(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "tqsdk-backtest-history-orchestration-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ))
    }

    struct StaticAuth;

    impl BacktestHistoryAuthProvider for StaticAuth {
        fn load<'a>(
            &'a self,
        ) -> Pin<Box<dyn Future<Output = Result<BacktestHistoryCredentials>> + Send + 'a>> {
            Box::pin(async { Ok(BacktestHistoryCredentials::new("test-user", "test-pass")) })
        }
    }

    struct NeverSourceFactory {
        opens: Arc<AtomicUsize>,
    }

    impl ServerHistorySourceFactory for NeverSourceFactory {
        fn open<'a>(
            &'a self,
            _credentials: BacktestHistoryCredentials,
            _request: ServerBacktestHistoryRequest,
        ) -> Pin<Box<dyn Future<Output = Result<Box<dyn ServerHistorySource>>> + Send + 'a>>
        {
            Box::pin(async move {
                self.opens.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(NeverSource) as Box<dyn ServerHistorySource>)
            })
        }
    }

    struct NeverSource;

    impl ServerHistorySource for NeverSource {
        fn next_event<'a>(
            &'a mut self,
        ) -> Pin<Box<dyn Future<Output = Result<Option<ServerBacktestHistoryEvent>>> + Send + 'a>>
        {
            Box::pin(pending())
        }
    }
}
