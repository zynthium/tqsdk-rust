//! Async request scheduler and cache-reader execution for backtest history.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinSet;
use tqsdk_core::{Kline, Tick};

use crate::aggregation::{DailyKlineAggregator, MinuteKlineAggregator, TickKlineAggregator};
use crate::backtest_tick_cache::BacktestTickCache;
use crate::daily_kline_cache::DailyKlineCache;
use crate::minute_kline_cache::MinuteKlineCache;
use crate::{
    BacktestHistoryMetadataCache, DataError, Result, resolve_minute_cache_metadata_snapshot,
};

use super::fill::{BacktestHistoryFillRequest, RemoteFillCoordinator};
use super::metadata::{ensure_metadata_for_remote_miss, metadata_snapshot_covers_range};
use super::planner::{
    PlannedBacktestHistoryRequest, PlannedBaseSource, bar_end_ns, classify_request,
    is_direct_native_daily_cache_request, plan_request,
};
use super::report::{
    BacktestHistoryBatchReport, BacktestHistoryChunk, BacktestHistoryEvent,
    BacktestHistoryFailureReason, BacktestHistoryFinality, BacktestHistoryRows,
    BacktestHistoryTelemetryEvent,
};
use super::request::{
    BacktestHistoryClientConfig, BacktestHistoryPolicy, ValidatedBacktestHistoryRequest,
};
use super::store_worker::{
    DailyScanSpec, MinuteScanSpec, StoreRows, StoreScanFailure, StoreScanMessage, StoreScanSpec,
    SymbolBufferBudget, TickScanSpec, spawn_scan,
};
use super::telemetry::TelemetryHub;
use super::{
    BacktestHistoryEventEnvelope, BacktestHistoryRunReservations,
    BacktestHistorySnapshotResourceReservation,
};

const MAX_SOURCE_CHUNK_BYTES: usize = 1024 * 1024;
const REQUEST_CANCELLATION_POLL_INTERVAL: Duration = Duration::from_millis(25);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BacktestHistoryExecutionMode {
    Query,
    MaterializeCache,
}

pub(crate) struct BacktestHistoryExecutionState {
    lifecycle_pin: Option<super::BacktestHistoryLifecyclePin>,
    failure_reasons: super::BacktestHistoryFailureReasons,
    resources: Option<super::BacktestHistorySnapshotQueryResources>,
    event_reservations: BacktestHistoryRunReservations,
}

impl BacktestHistoryExecutionState {
    pub(crate) fn new(
        lifecycle_pin: Option<super::BacktestHistoryLifecyclePin>,
        failure_reasons: super::BacktestHistoryFailureReasons,
        resources: Option<super::BacktestHistorySnapshotQueryResources>,
        event_reservations: BacktestHistoryRunReservations,
    ) -> Self {
        Self {
            lifecycle_pin,
            failure_reasons,
            resources,
            event_reservations,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BaseScanKey {
    family: PlannedBaseSource,
    cache_symbol: String,
    snapshot_hash: String,
    finality: BacktestHistoryFinality,
}

struct BaseScanSpec {
    family: PlannedBaseSource,
    cache_dir: std::path::PathBuf,
    cache_symbol: String,
    range: (i64, i64),
    minute_snapshot: crate::MinuteKlineCacheSnapshot,
    provisional_as_of_ns: Option<i64>,
    chunk_bytes: usize,
    cancellation: Arc<AtomicBool>,
    blocking_permits: Arc<Semaphore>,
    buffer_budget: SymbolBufferBudget,
    lifecycle_pin: Option<super::BacktestHistoryLifecyclePin>,
    resources: Option<super::BacktestHistorySnapshotQueryResources>,
}

#[derive(Clone)]
struct SharedScanRegistry {
    entries: Arc<Mutex<Vec<SharedScanEntry>>>,
    budgets: Arc<Mutex<Vec<(String, SymbolBufferBudget)>>>,
    lifecycle_pin: Option<super::BacktestHistoryLifecyclePin>,
    resources: Option<super::BacktestHistorySnapshotQueryResources>,
}

#[derive(Clone)]
struct SharedScanEntry {
    key: BaseScanKey,
    state: Arc<Mutex<SharedScanState>>,
    resources: Option<super::BacktestHistorySnapshotQueryResources>,
}

enum SharedScanState {
    Collecting(Vec<SharedScanSubscription>),
    Started,
    Finished,
}

struct SharedScanSubscription {
    range: (i64, i64),
    sender: mpsc::Sender<StoreScanMessage>,
}

impl SharedScanRegistry {
    fn new(
        lifecycle_pin: Option<super::BacktestHistoryLifecyclePin>,
        resources: Option<super::BacktestHistorySnapshotQueryResources>,
    ) -> Self {
        Self {
            entries: Arc::new(Mutex::new(Vec::new())),
            budgets: Arc::new(Mutex::new(Vec::new())),
            lifecycle_pin,
            resources,
        }
    }

    fn source_stream(
        &self,
        config: &BacktestHistoryClientConfig,
        plan: &PlannedBacktestHistoryRequest,
        slice: &super::planner::PlannedSourceSlice,
        cancellation: Arc<AtomicBool>,
        blocking_permits: Arc<Semaphore>,
        chunk_bytes: usize,
    ) -> mpsc::Receiver<StoreScanMessage> {
        let budget = self.budget_for(plan.symbol.as_str(), config.per_symbol_buffer_bytes);
        if plan.source_slices.len() != 1 {
            return spawn_base_scan(BaseScanSpec {
                family: plan.base_source,
                cache_dir: config.cache_dir.clone(),
                cache_symbol: slice.cache_symbol.clone(),
                range: slice.range,
                minute_snapshot: plan.minute_snapshot.clone(),
                provisional_as_of_ns: provisional_as_of(plan),
                chunk_bytes,
                cancellation,
                blocking_permits,
                buffer_budget: budget,
                lifecycle_pin: self.lifecycle_pin.clone(),
                resources: self.resources.clone(),
            });
        }

        let key = BaseScanKey {
            family: plan.base_source,
            cache_symbol: slice.cache_symbol.clone(),
            snapshot_hash: plan.snapshot_hash.clone(),
            finality: plan.finality,
        };
        let (sender, receiver) = mpsc::channel(2);
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        entries.retain(|entry| {
            !matches!(
                *entry
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
                SharedScanState::Finished
            )
        });
        if let Some(entry) = entries.iter().find(|entry| entry.key == key).cloned() {
            let mut state = entry
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let SharedScanState::Collecting(subscribers) = &mut *state {
                subscribers.push(SharedScanSubscription {
                    range: slice.range,
                    sender,
                });
                return receiver;
            }
            drop(state);
            drop(entries);
            return spawn_base_scan(BaseScanSpec {
                family: plan.base_source,
                cache_dir: config.cache_dir.clone(),
                cache_symbol: slice.cache_symbol.clone(),
                range: slice.range,
                minute_snapshot: plan.minute_snapshot.clone(),
                provisional_as_of_ns: provisional_as_of(plan),
                chunk_bytes,
                cancellation,
                blocking_permits,
                buffer_budget: budget,
                lifecycle_pin: self.lifecycle_pin.clone(),
                resources: self.resources.clone(),
            });
        }

        let entry = SharedScanEntry {
            key,
            state: Arc::new(Mutex::new(SharedScanState::Collecting(vec![
                SharedScanSubscription {
                    range: slice.range,
                    sender,
                },
            ]))),
            resources: self.resources.clone(),
        };
        entries.push(entry.clone());
        drop(entries);
        tokio::spawn(run_shared_scan(
            entry,
            config.cache_dir.clone(),
            plan.minute_snapshot.clone(),
            chunk_bytes,
            blocking_permits,
            budget,
            self.lifecycle_pin.clone(),
        ));
        receiver
    }

    fn budget_for(&self, symbol: &str, capacity_bytes: usize) -> SymbolBufferBudget {
        let mut budgets = self
            .budgets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some((_, budget)) = budgets
            .iter()
            .find(|(registered_symbol, _)| registered_symbol == symbol)
        {
            return budget.clone();
        }
        let budget = SymbolBufferBudget::new(capacity_bytes);
        budgets.push((symbol.to_string(), budget.clone()));
        budget
    }
}

async fn run_shared_scan(
    entry: SharedScanEntry,
    cache_dir: std::path::PathBuf,
    minute_snapshot: crate::MinuteKlineCacheSnapshot,
    chunk_bytes: usize,
    blocking_permits: Arc<Semaphore>,
    buffer_budget: SymbolBufferBudget,
    lifecycle_pin: Option<super::BacktestHistoryLifecyclePin>,
) {
    // Give concurrently scheduled cache-hit requests one scheduler turn to
    // subscribe before the first source range is fixed. Later consumers fall
    // back to an independent bounded scan instead of delaying a ready run.
    tokio::task::yield_now().await;
    let mut subscribers = {
        let mut state = entry
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let SharedScanState::Collecting(subscribers) =
            std::mem::replace(&mut *state, SharedScanState::Started)
        else {
            return;
        };
        subscribers
    };
    let scan_cancellation = Arc::new(AtomicBool::new(false));
    let ranges = merge_ranges(
        subscribers
            .iter()
            .map(|subscriber| subscriber.range)
            .collect(),
    );
    for range in ranges {
        let mut source = spawn_base_scan(BaseScanSpec {
            family: entry.key.family,
            cache_dir: cache_dir.clone(),
            cache_symbol: entry.key.cache_symbol.clone(),
            range,
            minute_snapshot: minute_snapshot.clone(),
            provisional_as_of_ns: provisional_as_of_from_finality(entry.key.finality),
            chunk_bytes,
            cancellation: Arc::clone(&scan_cancellation),
            blocking_permits: Arc::clone(&blocking_permits),
            buffer_budget: buffer_budget.clone(),
            lifecycle_pin: lifecycle_pin.clone(),
            resources: entry.resources.clone(),
        });
        while let Some(message) = source.recv().await {
            match message {
                StoreScanMessage::Chunk(chunk) => {
                    let mut active = Vec::with_capacity(subscribers.len());
                    for subscriber in subscribers {
                        if !chunk_intersects_range(chunk.as_ref(), subscriber.range)
                            || subscriber
                                .sender
                                .send(StoreScanMessage::Chunk(Arc::clone(&chunk)))
                                .await
                                .is_ok()
                        {
                            active.push(subscriber);
                        }
                    }
                    subscribers = active;
                    if subscribers.is_empty() {
                        scan_cancellation.store(true, Ordering::Release);
                        finish_shared_scan(&entry);
                        return;
                    }
                }
                StoreScanMessage::Failed(error) => {
                    for subscriber in subscribers {
                        let _ = subscriber
                            .sender
                            .send(StoreScanMessage::Failed(error.clone()))
                            .await;
                    }
                    finish_shared_scan(&entry);
                    return;
                }
            }
        }
    }
    finish_shared_scan(&entry);
}

fn finish_shared_scan(entry: &SharedScanEntry) {
    let mut state = entry
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *state = SharedScanState::Finished;
}

fn spawn_base_scan(spec: BaseScanSpec) -> mpsc::Receiver<StoreScanMessage> {
    match spec.family {
        PlannedBaseSource::Tick => spawn_scan(StoreScanSpec::Tick(TickScanSpec {
            cache_dir: spec.cache_dir,
            symbol: spec.cache_symbol,
            range: spec.range,
            provisional_as_of_ns: spec.provisional_as_of_ns,
            target_bytes: spec.chunk_bytes,
            cancellation: spec.cancellation,
            permits: spec.blocking_permits,
            buffer_budget: spec.buffer_budget,
            lifecycle_pin: spec.lifecycle_pin,
            resources: spec.resources,
        })),
        PlannedBaseSource::CanonicalMinute => {
            spawn_scan(StoreScanSpec::CanonicalMinute(MinuteScanSpec {
                cache_dir: spec.cache_dir,
                symbol: spec.cache_symbol,
                range: spec.range,
                snapshot: spec.minute_snapshot,
                target_bytes: spec.chunk_bytes,
                cancellation: spec.cancellation,
                permits: spec.blocking_permits,
                buffer_budget: spec.buffer_budget,
                lifecycle_pin: spec.lifecycle_pin,
                resources: spec.resources,
            }))
        }
        PlannedBaseSource::CanonicalDaily => {
            spawn_scan(StoreScanSpec::CanonicalDaily(DailyScanSpec {
                cache_dir: spec.cache_dir,
                symbol: spec.cache_symbol,
                range: spec.range,
                snapshot: spec.minute_snapshot,
                cancellation: spec.cancellation,
                permits: spec.blocking_permits,
                buffer_budget: spec.buffer_budget,
                lifecycle_pin: spec.lifecycle_pin,
                resources: spec.resources,
            }))
        }
    }
}

fn chunk_intersects_range(chunk: &super::store_worker::StoreChunk, range: (i64, i64)) -> bool {
    let (first, last) = match &chunk.rows {
        StoreRows::Ticks(rows) => (
            rows.first().map(|row| row.datetime),
            rows.last().map(|row| row.datetime),
        ),
        StoreRows::CanonicalMinutes(rows) | StoreRows::CanonicalDaily(rows) => (
            rows.first().map(|row| row.datetime),
            rows.last().map(|row| row.datetime),
        ),
    };
    first.is_some_and(|first| first < range.1) && last.is_some_and(|last| last >= range.0)
}

pub(crate) async fn execute_batch(
    config: Arc<BacktestHistoryClientConfig>,
    requests: Vec<ValidatedBacktestHistoryRequest>,
    event_sender: mpsc::Sender<BacktestHistoryEventEnvelope>,
    telemetry: TelemetryHub,
    cancellation: Arc<AtomicBool>,
    mode: BacktestHistoryExecutionMode,
    execution_state: BacktestHistoryExecutionState,
) -> BacktestHistoryBatchReport {
    let BacktestHistoryExecutionState {
        lifecycle_pin,
        failure_reasons,
        resources,
        event_reservations,
    } = execution_state;
    let logical_permits = Arc::new(Semaphore::new(config.logical_concurrency));
    let blocking_permits = Arc::new(Semaphore::new(config.blocking_workers));
    let scan_registry = SharedScanRegistry::new(lifecycle_pin, resources.clone());
    let mut tasks = JoinSet::new();
    for request in requests {
        let config = Arc::clone(&config);
        let event_sender = event_sender.clone();
        let telemetry = telemetry.clone();
        let cancellation = Arc::clone(&cancellation);
        let logical_permits = Arc::clone(&logical_permits);
        let blocking_permits = Arc::clone(&blocking_permits);
        let scan_registry = scan_registry.clone();
        let failure_reasons = Arc::clone(&failure_reasons);
        let resources = resources.clone();
        let event_reservations = event_reservations.clone();
        tasks.spawn(async move {
            let request_id = request.request_id;
            let symbol = request.symbol.clone();
            let permit =
                acquire_logical_permit_until_cancelled(logical_permits, cancellation.as_ref())
                    .await;
            match permit {
                Ok(_permit) => {
                    let chunk_bytes = config
                        .per_symbol_buffer_bytes
                        .min(MAX_SOURCE_CHUNK_BYTES)
                        .max(std::mem::size_of::<Kline>());
                    let context = RequestExecutionContext {
                        config,
                        event_sender,
                        telemetry,
                        cancellation,
                        blocking_permits,
                        scan_registry,
                        chunk_bytes,
                        mode,
                        failure_reasons,
                        resources,
                        event_reservations,
                    };
                    run_request(context, request).await
                }
                Err(error) => {
                    failure_reasons
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .insert(request_id, BacktestHistoryFailureReason::Cancelled);
                    RequestTerminal::failed(request_id, symbol, error.to_string(), 0)
                }
            }
        });
    }
    drop(event_sender);

    let mut report = BacktestHistoryBatchReport::default();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(RequestTerminal::Completed(completed)) => report.completed.push(completed),
            Ok(RequestTerminal::Failed(failure)) => report.failed.push(failure),
            Err(error) => {
                failure_reasons
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(0, BacktestHistoryFailureReason::Internal);
                report
                    .failed
                    .push(super::report::BacktestHistoryRequestFailure {
                        request_id: 0,
                        symbol: "<scheduler>".to_string(),
                        error: format!("backtest history request task failed: {error}"),
                        emitted_rows: 0,
                    });
            }
        }
    }
    report
        .completed
        .sort_by_key(|completed| completed.request_id);
    report.failed.sort_by_key(|failed| failed.request_id);
    report
}

enum RequestTerminal {
    Completed(super::report::BacktestHistoryRequestReport),
    Failed(super::report::BacktestHistoryRequestFailure),
}

impl RequestTerminal {
    fn failed(request_id: u64, symbol: String, error: String, emitted_rows: usize) -> Self {
        Self::Failed(super::report::BacktestHistoryRequestFailure {
            request_id,
            symbol,
            error,
            emitted_rows,
        })
    }
}

async fn run_request(
    context: RequestExecutionContext,
    request: ValidatedBacktestHistoryRequest,
) -> RequestTerminal {
    let request_id = request.request_id;
    let symbol = request.symbol.clone();
    let event_sender = context.event_sender.clone();
    let telemetry = context.telemetry.clone();
    let mode = context.mode;
    let result = execute_request(&context, request).await;
    match result {
        Ok((report, emitted_rows)) => {
            let _ = event_sender
                .send(BacktestHistoryEventEnvelope::new(
                    BacktestHistoryEvent::RequestCompleted(report.clone()),
                    None,
                ))
                .await;
            let completed_rows = if mode == BacktestHistoryExecutionMode::MaterializeCache {
                report.rows
            } else {
                emitted_rows
            };
            telemetry.emit_terminal(BacktestHistoryTelemetryEvent {
                request_id: Some(report.request_id),
                symbol: report.symbol.clone(),
                phase: if mode == BacktestHistoryExecutionMode::MaterializeCache {
                    super::report::BacktestHistoryPhase::Aggregate
                } else {
                    super::report::BacktestHistoryPhase::Read
                },
                completed_rows,
                message: if mode == BacktestHistoryExecutionMode::MaterializeCache {
                    "backtest history cache materialization completed".to_string()
                } else {
                    "backtest history request completed".to_string()
                },
            });
            RequestTerminal::Completed(report)
        }
        Err(execution) => {
            let reason = super::classify_snapshot_failure(
                &execution.error,
                context.cancellation.load(Ordering::Acquire),
            );
            context
                .failure_reasons
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .entry(request_id)
                .or_insert(reason);
            let failure = super::report::BacktestHistoryRequestFailure {
                request_id,
                symbol,
                error: execution.error.to_string(),
                emitted_rows: execution.emitted_rows,
            };
            let _ = event_sender
                .send(BacktestHistoryEventEnvelope::new(
                    BacktestHistoryEvent::RequestFailed(failure.clone()),
                    None,
                ))
                .await;
            telemetry.emit_terminal(BacktestHistoryTelemetryEvent {
                request_id: Some(failure.request_id),
                symbol: failure.symbol.clone(),
                phase: if mode == BacktestHistoryExecutionMode::MaterializeCache {
                    super::report::BacktestHistoryPhase::Fill
                } else {
                    super::report::BacktestHistoryPhase::Read
                },
                completed_rows: failure.emitted_rows,
                message: format!("backtest history request failed: {}", failure.error),
            });
            RequestTerminal::Failed(failure)
        }
    }
}

struct ExecutionFailure {
    error: DataError,
    emitted_rows: usize,
}

struct ReservedProjectedRows<T> {
    rows: Vec<T>,
    reservation: Option<BacktestHistorySnapshotResourceReservation>,
}

impl<T> ReservedProjectedRows<T> {
    fn with_capacity(
        capacity: usize,
        resources: Option<&super::BacktestHistorySnapshotQueryResources>,
    ) -> Result<Self> {
        let reservation = resources
            .map(|resources| resources.try_reserve_for_projected_rows::<T>(capacity))
            .transpose()?;
        Ok(Self {
            rows: Vec::with_capacity(capacity),
            reservation,
        })
    }

    fn into_parts(self) -> (Vec<T>, Option<BacktestHistorySnapshotResourceReservation>) {
        (self.rows, self.reservation)
    }
}

fn projected_kline_capacity(chunk_bytes: usize) -> usize {
    let row_bytes = std::mem::size_of::<Kline>();
    let requested_rows = chunk_bytes
        .saturating_add(row_bytes.saturating_sub(1))
        .checked_div(row_bytes)
        .unwrap_or(usize::MAX)
        .saturating_add(1)
        .max(1);
    requested_rows
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX)
}

#[derive(Clone)]
struct RequestExecutionContext {
    config: Arc<BacktestHistoryClientConfig>,
    event_sender: mpsc::Sender<BacktestHistoryEventEnvelope>,
    telemetry: TelemetryHub,
    cancellation: Arc<AtomicBool>,
    blocking_permits: Arc<Semaphore>,
    scan_registry: SharedScanRegistry,
    chunk_bytes: usize,
    mode: BacktestHistoryExecutionMode,
    failure_reasons: super::BacktestHistoryFailureReasons,
    resources: Option<super::BacktestHistorySnapshotQueryResources>,
    event_reservations: BacktestHistoryRunReservations,
}

async fn execute_request(
    context: &RequestExecutionContext,
    request: ValidatedBacktestHistoryRequest,
) -> std::result::Result<(super::report::BacktestHistoryRequestReport, usize), ExecutionFailure> {
    let config = &context.config;
    let telemetry = &context.telemetry;
    let cancellation = &context.cancellation;
    let mode = context.mode;
    let plan = await_or_request_cancelled(
        cancellation.as_ref(),
        plan_request_for_execution(config, request),
    )
    .await
    .map_err(|error| ExecutionFailure {
        error,
        emitted_rows: 0,
    })?;
    telemetry.emit(BacktestHistoryTelemetryEvent {
        request_id: Some(plan.request_id),
        symbol: plan.symbol.clone(),
        phase: super::report::BacktestHistoryPhase::Inspect,
        completed_rows: 0,
        message: "planned durable cache sources".to_string(),
    });

    let mut cached_ranges = Vec::new();
    let mut remote_filled_ranges = Vec::new();
    let mut remote_used = false;
    let mut rows_written = 0usize;
    let fill_coordinator = RemoteFillCoordinator::new(Arc::clone(config), telemetry.clone());
    for slice in &plan.source_slices {
        if cancellation.load(Ordering::Acquire) {
            return Err(ExecutionFailure {
                error: DataError::InvalidState("backtest history request was cancelled"),
                emitted_rows: 0,
            });
        }
        let inspection =
            inspect_source(config, &plan, slice).map_err(|error| ExecutionFailure {
                error,
                emitted_rows: 0,
            })?;
        cached_ranges.extend(inspection.cached_ranges);
        if inspection.missing_ranges.is_empty() {
            continue;
        }
        if config.policy == BacktestHistoryPolicy::CacheOnly {
            return Err(ExecutionFailure {
                error: DataError::InvalidState("backtest history cache coverage is incomplete"),
                emitted_rows: 0,
            });
        }
        let fill_request = match plan.base_source {
            PlannedBaseSource::Tick => BacktestHistoryFillRequest::tick(
                slice.cache_symbol.clone(),
                slice.range,
                provisional_as_of(&plan),
                Some(plan.request_id),
                plan.symbol.clone(),
            ),
            PlannedBaseSource::CanonicalMinute => BacktestHistoryFillRequest::canonical_minute(
                slice.cache_symbol.clone(),
                slice.range,
                plan.minute_snapshot.clone(),
                Some(plan.request_id),
                plan.symbol.clone(),
            ),
            PlannedBaseSource::CanonicalDaily => BacktestHistoryFillRequest::canonical_daily(
                slice.cache_symbol.clone(),
                slice.range,
                plan.minute_snapshot.clone(),
                Some(plan.request_id),
                plan.symbol.clone(),
            ),
        };
        let outcome = fill_coordinator
            .ensure_coverage_until_cancelled(
                fill_request,
                cancellation.as_ref(),
                mode == BacktestHistoryExecutionMode::MaterializeCache,
            )
            .await
            .map_err(|error| ExecutionFailure {
                error,
                emitted_rows: 0,
            })?;
        remote_filled_ranges.extend(outcome.remote_filled_ranges);
        remote_used |= outcome.remote_used;
        rows_written = rows_written.saturating_add(outcome.rows_written);
    }

    if mode == BacktestHistoryExecutionMode::MaterializeCache {
        return Ok((
            plan.report_template(
                rows_written,
                merge_ranges(cached_ranges),
                merge_ranges(remote_filled_ranges),
                remote_used,
            ),
            0,
        ));
    }

    // A derived Kline request can own one producer buffer while the bounded
    // event queue holds two completed buffers and the consumer encodes one
    // delivered buffer. Retain that conservative four-buffer reservation
    // before any `Vec<Kline>` allocation, then keep it for the whole public
    // run so a slow encoder cannot create an unaccounted allocation window.
    let _derived_output_reservation = if plan.duration_ns.is_some() {
        if let Some(resources) = context.resources.as_ref() {
            let rows = projected_kline_capacity(context.chunk_bytes).saturating_mul(4);
            let reservation = resources
                .try_reserve_for_projected_rows::<Kline>(rows)
                .map_err(|error| ExecutionFailure {
                    error,
                    emitted_rows: 0,
                })?;
            let reservation = Arc::new(reservation);
            context
                .event_reservations
                .retain(BacktestHistorySnapshotResourceReservation::new(Arc::clone(
                    &reservation,
                )));
            Some(reservation)
        } else {
            None
        }
    } else {
        None
    };

    let emitted_rows = match plan.base_source {
        PlannedBaseSource::Tick => execute_tick_plan(context, &plan).await,
        PlannedBaseSource::CanonicalMinute => execute_minute_plan(context, &plan).await,
        PlannedBaseSource::CanonicalDaily => execute_daily_plan(context, &plan).await,
    }?;
    Ok((
        plan.report_template(
            emitted_rows,
            merge_ranges(cached_ranges),
            merge_ranges(remote_filled_ranges),
            remote_used,
        ),
        emitted_rows,
    ))
}

async fn await_or_request_cancelled<T>(
    cancellation: &AtomicBool,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    let cancellation_wait = async {
        while !cancellation.load(Ordering::Acquire) {
            tokio::time::sleep(REQUEST_CANCELLATION_POLL_INTERVAL).await;
        }
    };
    tokio::pin!(cancellation_wait);
    tokio::pin!(future);
    tokio::select! {
        biased;
        _ = &mut cancellation_wait => Err(DataError::InvalidState(
            "backtest history request was cancelled while planning cache sources",
        )),
        result = &mut future => result,
    }
}

async fn acquire_logical_permit_until_cancelled(
    permits: Arc<Semaphore>,
    cancellation: &AtomicBool,
) -> Result<OwnedSemaphorePermit> {
    let cancellation_wait = async {
        while !cancellation.load(Ordering::Acquire) {
            tokio::time::sleep(REQUEST_CANCELLATION_POLL_INTERVAL).await;
        }
    };
    tokio::pin!(cancellation_wait);
    tokio::select! {
        biased;
        _ = &mut cancellation_wait => Err(DataError::InvalidState(
            "backtest history request was cancelled while waiting for scheduler capacity",
        )),
        permit = permits.acquire_owned() => permit.map_err(|_| DataError::InvalidState(
            "backtest history logical request scheduler is unavailable",
        )),
    }
}

async fn plan_request_for_execution(
    config: &Arc<BacktestHistoryClientConfig>,
    request: ValidatedBacktestHistoryRequest,
) -> Result<PlannedBacktestHistoryRequest> {
    let requested_range = (request.start_ns, request.end_ns);
    let base_source = classify_request(&request)?;
    let is_main_continuous = request.symbol.starts_with("KQ.m@");
    let requires_metadata = !is_direct_native_daily_cache_request(&request, base_source);
    let active_metadata = if requires_metadata {
        BacktestHistoryMetadataCache::open_read_only(config.cache_dir.as_path())
            .load_active(request.symbol.as_str())?
    } else {
        None
    };
    let selected_metadata = if !requires_metadata {
        None
    } else if matches!(base_source, PlannedBaseSource::CanonicalMinute) {
        resolve_minute_cache_metadata_snapshot(
            config.cache_dir.as_path(),
            request.symbol.as_str(),
            request.start_ns,
            request.end_ns,
        )?
    } else {
        active_metadata.clone()
    };
    let metadata_needs_refresh = selected_metadata
        .as_ref()
        .is_some_and(|snapshot| !metadata_snapshot_covers_range(snapshot, requested_range));
    if requires_metadata
        && config.policy == BacktestHistoryPolicy::RemoteOnMiss
        && (metadata_needs_refresh || (is_main_continuous && active_metadata.is_none()))
    {
        ensure_metadata_for_remote_miss(
            config.cache_dir.as_path(),
            config.auth_provider.as_ref(),
            request.symbol.as_str(),
            request.start_ns,
            request.end_ns,
        )
        .await?;
        return plan_request(config.cache_dir.as_path(), request);
    }

    let fallback_plan = plan_request(config.cache_dir.as_path(), request.clone())?;
    if !requires_metadata
        || config.policy != BacktestHistoryPolicy::RemoteOnMiss
        || active_metadata.is_some()
    {
        return Ok(fallback_plan);
    }

    let has_cache_miss = fallback_plan
        .source_slices
        .iter()
        .map(|slice| inspect_source(config, &fallback_plan, slice))
        .collect::<Result<Vec<_>>>()?
        .iter()
        .any(|inspection| !inspection.missing_ranges.is_empty());
    if !has_cache_miss {
        return Ok(fallback_plan);
    }

    ensure_metadata_for_remote_miss(
        config.cache_dir.as_path(),
        config.auth_provider.as_ref(),
        request.symbol.as_str(),
        request.start_ns,
        request.end_ns,
    )
    .await?;
    plan_request(config.cache_dir.as_path(), request)
}

struct SourceInspection {
    cached_ranges: Vec<(i64, i64)>,
    missing_ranges: Vec<(i64, i64)>,
}

pub(crate) enum StrictInspectionFailure {
    Planning(DataError),
    Source(DataError),
    CoverageIncomplete(Vec<(i64, i64)>),
    Provisional { as_of_ns: i64 },
}

pub(crate) async fn strict_inspect_request(
    config: Arc<BacktestHistoryClientConfig>,
    request: ValidatedBacktestHistoryRequest,
) -> std::result::Result<super::report::BacktestHistoryRequestReport, StrictInspectionFailure> {
    let plan = plan_request_for_execution(&config, request)
        .await
        .map_err(StrictInspectionFailure::Planning)?;
    if let BacktestHistoryFinality::Provisional { as_of_ns } = plan.finality {
        return Err(StrictInspectionFailure::Provisional { as_of_ns });
    }

    let mut cached_ranges = Vec::new();
    let mut missing_ranges = Vec::new();
    for slice in &plan.source_slices {
        let inspection =
            inspect_source(&config, &plan, slice).map_err(StrictInspectionFailure::Source)?;
        cached_ranges.extend(inspection.cached_ranges);
        missing_ranges.extend(inspection.missing_ranges);
    }
    let missing_ranges = merge_ranges(missing_ranges);
    if !missing_ranges.is_empty() {
        return Err(StrictInspectionFailure::CoverageIncomplete(missing_ranges));
    }

    Ok(plan.report_template(0, merge_ranges(cached_ranges), Vec::new(), false))
}

fn inspect_source(
    config: &BacktestHistoryClientConfig,
    plan: &PlannedBacktestHistoryRequest,
    slice: &super::planner::PlannedSourceSlice,
) -> Result<SourceInspection> {
    match plan.base_source {
        PlannedBaseSource::Tick => {
            let coverage = BacktestTickCache::open_read_only(config.cache_dir.as_path()).coverage(
                slice.cache_symbol.as_str(),
                slice.range.0,
                slice.range.1,
            )?;
            Ok(SourceInspection {
                cached_ranges: coverage.cached_ranges,
                missing_ranges: coverage.missing_ranges,
            })
        }
        PlannedBaseSource::CanonicalMinute => {
            let coverage = MinuteKlineCache::open_read_only(config.cache_dir.as_path()).coverage(
                slice.cache_symbol.as_str(),
                slice.range.0,
                slice.range.1,
                &plan.minute_snapshot,
            )?;
            Ok(SourceInspection {
                cached_ranges: coverage.cached_ranges,
                missing_ranges: coverage.missing_ranges,
            })
        }
        PlannedBaseSource::CanonicalDaily => {
            let coverage = DailyKlineCache::open_read_only(config.cache_dir.as_path()).coverage(
                slice.cache_symbol.as_str(),
                slice.range.0,
                slice.range.1,
                &plan.minute_snapshot,
            )?;
            Ok(SourceInspection {
                cached_ranges: coverage.cached_ranges,
                missing_ranges: coverage.missing_ranges,
            })
        }
    }
}

async fn execute_tick_plan(
    context: &RequestExecutionContext,
    plan: &PlannedBacktestHistoryRequest,
) -> std::result::Result<usize, ExecutionFailure> {
    let config = context.config.as_ref();
    let event_sender = &context.event_sender;
    let telemetry = &context.telemetry;
    let cancellation = &context.cancellation;
    let blocking_permits = &context.blocking_permits;
    let scan_registry = &context.scan_registry;
    let chunk_bytes = context.chunk_bytes;
    let mut emitted_rows = 0usize;
    let result: Result<()> = async {
        match plan.duration_ns {
            None => {
                for slice in &plan.source_slices {
                    let mut source = scan_registry.source_stream(
                        config,
                        plan,
                        slice,
                        Arc::clone(cancellation),
                        Arc::clone(blocking_permits),
                        chunk_bytes,
                    );
                    while let Some(message) = source.recv().await {
                        if cancellation.load(Ordering::Acquire) {
                            return Err(DataError::InvalidState(
                                "backtest history request was cancelled",
                            ));
                        }
                        let mut projected = tick_rows_for_slice(
                            message,
                            slice,
                            &context.failure_reasons,
                            plan.request_id,
                            context.resources.as_ref(),
                        )?;
                        projected.rows.retain(|row| {
                            row.datetime >= plan.requested_range.0
                                && row.datetime < plan.effective_end_ns
                        });
                        let (rows, reservation) = projected.into_parts();
                        emitted_rows = emitted_rows.saturating_add(
                            send_tick_chunk_with_reservation(
                                event_sender,
                                plan,
                                rows,
                                reservation,
                                telemetry,
                                emitted_rows,
                            )
                            .await?,
                        );
                    }
                }
            }
            Some(duration_ns) => {
                let mut aggregator = TickKlineAggregator::new(
                    plan.symbol.clone(),
                    duration_ns,
                    plan.session.clone(),
                )?;
                let mut output = Vec::new();
                for slice in &plan.source_slices {
                    let mut source = scan_registry.source_stream(
                        config,
                        plan,
                        slice,
                        Arc::clone(cancellation),
                        Arc::clone(blocking_permits),
                        chunk_bytes,
                    );
                    while let Some(message) = source.recv().await {
                        if cancellation.load(Ordering::Acquire) {
                            return Err(DataError::InvalidState(
                                "backtest history request was cancelled",
                            ));
                        }
                        let rows = tick_rows_for_slice(
                            message,
                            slice,
                            &context.failure_reasons,
                            plan.request_id,
                            context.resources.as_ref(),
                        )?;
                        for row in &rows.rows {
                            if let Some(update) = aggregator.update(row)?
                                && let Some(closed) = update.closed
                                && should_emit_kline(&closed, plan, duration_ns)?
                            {
                                output.push(closed);
                            }
                        }
                        if estimated_kline_bytes(output.len()) >= chunk_bytes {
                            emitted_rows = emitted_rows.saturating_add(
                                send_kline_chunk(
                                    event_sender,
                                    plan,
                                    duration_ns,
                                    std::mem::take(&mut output),
                                    telemetry,
                                    emitted_rows,
                                )
                                .await?,
                            );
                        }
                    }
                }
                if let Some(closed) = aggregator.finish_closed_through(plan.expanded_source_range.1)
                    && should_emit_kline(&closed, plan, duration_ns)?
                {
                    output.push(closed);
                }
                emitted_rows = emitted_rows.saturating_add(
                    send_kline_chunk(
                        event_sender,
                        plan,
                        duration_ns,
                        output,
                        telemetry,
                        emitted_rows,
                    )
                    .await?,
                );
            }
        }
        Ok(())
    }
    .await;
    result
        .map(|()| emitted_rows)
        .map_err(|error| ExecutionFailure {
            error,
            emitted_rows,
        })
}

async fn execute_minute_plan(
    context: &RequestExecutionContext,
    plan: &PlannedBacktestHistoryRequest,
) -> std::result::Result<usize, ExecutionFailure> {
    let config = context.config.as_ref();
    let event_sender = &context.event_sender;
    let telemetry = &context.telemetry;
    let cancellation = &context.cancellation;
    let blocking_permits = &context.blocking_permits;
    let scan_registry = &context.scan_registry;
    let chunk_bytes = context.chunk_bytes;
    let duration_ns = plan.duration_ns.unwrap_or(crate::MINUTE_KLINE_DURATION_NS);
    let mut emitted_rows = 0usize;
    let result: Result<()> = async {
        if duration_ns == crate::MINUTE_KLINE_DURATION_NS {
            for slice in &plan.source_slices {
                let mut source = scan_registry.source_stream(
                    config,
                    plan,
                    slice,
                    Arc::clone(cancellation),
                    Arc::clone(blocking_permits),
                    chunk_bytes,
                );
                while let Some(message) = source.recv().await {
                    if cancellation.load(Ordering::Acquire) {
                        return Err(DataError::InvalidState(
                            "backtest history request was cancelled",
                        ));
                    }
                    let mut projected = minute_rows_for_slice(
                        message,
                        slice,
                        &context.failure_reasons,
                        plan.request_id,
                        context.resources.as_ref(),
                    )?;
                    projected.rows.retain(|row| {
                        row.datetime >= plan.requested_range.0
                            && row.datetime < plan.effective_end_ns
                    });
                    let (rows, reservation) = projected.into_parts();
                    emitted_rows = emitted_rows.saturating_add(
                        send_kline_chunk_with_reservation(
                            event_sender,
                            plan,
                            duration_ns,
                            rows,
                            reservation,
                            telemetry,
                            emitted_rows,
                        )
                        .await?,
                    );
                }
            }
            return Ok(());
        }

        let mut aggregator = MinuteKlineAggregator::new(duration_ns, plan.session.clone())?;
        let mut output = Vec::new();
        for slice in &plan.source_slices {
            let mut source = scan_registry.source_stream(
                config,
                plan,
                slice,
                Arc::clone(cancellation),
                Arc::clone(blocking_permits),
                chunk_bytes,
            );
            while let Some(message) = source.recv().await {
                if cancellation.load(Ordering::Acquire) {
                    return Err(DataError::InvalidState(
                        "backtest history request was cancelled",
                    ));
                }
                let rows = minute_rows_for_slice(
                    message,
                    slice,
                    &context.failure_reasons,
                    plan.request_id,
                    context.resources.as_ref(),
                )?;
                for row in &rows.rows {
                    if let Some(update) = aggregator.update(row)?
                        && let Some(closed) = update.closed
                        && should_emit_kline(&closed, plan, duration_ns)?
                    {
                        output.push(closed);
                    }
                }
                if estimated_kline_bytes(output.len()) >= chunk_bytes {
                    emitted_rows = emitted_rows.saturating_add(
                        send_kline_chunk(
                            event_sender,
                            plan,
                            duration_ns,
                            std::mem::take(&mut output),
                            telemetry,
                            emitted_rows,
                        )
                        .await?,
                    );
                }
            }
        }
        if let Some(closed) = aggregator.finish_closed_through(plan.expanded_source_range.1)
            && should_emit_kline(&closed, plan, duration_ns)?
        {
            output.push(closed);
        }
        emitted_rows = emitted_rows.saturating_add(
            send_kline_chunk(
                event_sender,
                plan,
                duration_ns,
                output,
                telemetry,
                emitted_rows,
            )
            .await?,
        );
        Ok(())
    }
    .await;
    result
        .map(|()| emitted_rows)
        .map_err(|error| ExecutionFailure {
            error,
            emitted_rows,
        })
}

async fn execute_daily_plan(
    context: &RequestExecutionContext,
    plan: &PlannedBacktestHistoryRequest,
) -> std::result::Result<usize, ExecutionFailure> {
    let duration_ns = plan.duration_ns.ok_or(ExecutionFailure {
        error: DataError::InvalidState("daily backtest plan is missing Kline duration"),
        emitted_rows: 0,
    })?;
    let mut emitted_rows = 0usize;
    let result = async {
        let mut output = Vec::new();
        if duration_ns == crate::DAILY_KLINE_DURATION_NS {
            for slice in &plan.source_slices {
                let mut source = context.scan_registry.source_stream(
                    context.config.as_ref(),
                    plan,
                    slice,
                    Arc::clone(&context.cancellation),
                    Arc::clone(&context.blocking_permits),
                    context.chunk_bytes,
                );
                while let Some(message) = source.recv().await {
                    if context.cancellation.load(Ordering::Acquire) {
                        return Err(DataError::InvalidState(
                            "backtest history request was cancelled",
                        ));
                    }
                    let projected = daily_rows_for_slice(
                        message,
                        slice,
                        &context.failure_reasons,
                        plan.request_id,
                        context.resources.as_ref(),
                    )?;
                    for row in &projected.rows {
                        if row.datetime >= plan.requested_range.0
                            && row.datetime < plan.effective_end_ns
                        {
                            output.push(row.clone());
                        }
                    }
                    if estimated_kline_bytes(output.len()) >= context.chunk_bytes {
                        emitted_rows = emitted_rows.saturating_add(
                            send_kline_chunk(
                                &context.event_sender,
                                plan,
                                duration_ns,
                                std::mem::take(&mut output),
                                &context.telemetry,
                                emitted_rows,
                            )
                            .await?,
                        );
                    }
                }
            }
        } else {
            let mut aggregator = DailyKlineAggregator::new(duration_ns)?;
            for slice in &plan.source_slices {
                let mut source = context.scan_registry.source_stream(
                    context.config.as_ref(),
                    plan,
                    slice,
                    Arc::clone(&context.cancellation),
                    Arc::clone(&context.blocking_permits),
                    context.chunk_bytes,
                );
                while let Some(message) = source.recv().await {
                    if context.cancellation.load(Ordering::Acquire) {
                        return Err(DataError::InvalidState(
                            "backtest history request was cancelled",
                        ));
                    }
                    let projected = daily_rows_for_slice(
                        message,
                        slice,
                        &context.failure_reasons,
                        plan.request_id,
                        context.resources.as_ref(),
                    )?;
                    for row in &projected.rows {
                        if let Some(closed) = aggregator.update(row)?
                            && should_emit_daily_kline(&closed, plan, duration_ns)?
                        {
                            output.push(closed);
                        }
                    }
                    if estimated_kline_bytes(output.len()) >= context.chunk_bytes {
                        emitted_rows = emitted_rows.saturating_add(
                            send_kline_chunk(
                                &context.event_sender,
                                plan,
                                duration_ns,
                                std::mem::take(&mut output),
                                &context.telemetry,
                                emitted_rows,
                            )
                            .await?,
                        );
                    }
                }
            }
            if let Some(closed) = aggregator.finish_closed_through(plan.effective_end_ns)?
                && should_emit_daily_kline(&closed, plan, duration_ns)?
            {
                output.push(closed);
            }
        }
        emitted_rows = emitted_rows.saturating_add(
            send_kline_chunk(
                &context.event_sender,
                plan,
                duration_ns,
                output,
                &context.telemetry,
                emitted_rows,
            )
            .await?,
        );
        Ok(())
    }
    .await;
    result
        .map(|()| emitted_rows)
        .map_err(|error| ExecutionFailure {
            error,
            emitted_rows,
        })
}

fn tick_rows_for_slice(
    message: StoreScanMessage,
    slice: &super::planner::PlannedSourceSlice,
    failure_reasons: &super::BacktestHistoryFailureReasons,
    request_id: u64,
    resources: Option<&super::BacktestHistorySnapshotQueryResources>,
) -> Result<ReservedProjectedRows<Tick>> {
    match message {
        StoreScanMessage::Failed(failure) => Err(record_store_scan_failure(
            failure_reasons,
            request_id,
            failure,
        )),
        StoreScanMessage::Chunk(chunk) => match &chunk.rows {
            StoreRows::Ticks(rows) => {
                let mut projected = ReservedProjectedRows::with_capacity(rows.len(), resources)?;
                projected.rows.extend(
                    rows.iter()
                        .filter(|row| row.datetime >= slice.range.0 && row.datetime < slice.range.1)
                        .cloned(),
                );
                projected
                    .rows
                    .sort_by_key(|row| (row.datetime, slice.physical_rank, row.id));
                Ok(projected)
            }
            StoreRows::CanonicalMinutes(_) | StoreRows::CanonicalDaily(_) => Err(
                DataError::InvalidState("Tick cache reader returned a canonical-minute chunk"),
            ),
        },
    }
}

fn minute_rows_for_slice(
    message: StoreScanMessage,
    slice: &super::planner::PlannedSourceSlice,
    failure_reasons: &super::BacktestHistoryFailureReasons,
    request_id: u64,
    resources: Option<&super::BacktestHistorySnapshotQueryResources>,
) -> Result<ReservedProjectedRows<Kline>> {
    match message {
        StoreScanMessage::Failed(failure) => Err(record_store_scan_failure(
            failure_reasons,
            request_id,
            failure,
        )),
        StoreScanMessage::Chunk(chunk) => match &chunk.rows {
            StoreRows::CanonicalMinutes(rows) => {
                let mut projected = ReservedProjectedRows::with_capacity(rows.len(), resources)?;
                projected.rows.extend(
                    rows.iter()
                        .filter(|row| row.datetime >= slice.range.0 && row.datetime < slice.range.1)
                        .cloned(),
                );
                projected.rows.sort_by_key(|row| (row.datetime, row.id));
                Ok(projected)
            }
            StoreRows::Ticks(_) | StoreRows::CanonicalDaily(_) => Err(DataError::InvalidState(
                "canonical-minute cache reader returned a Tick chunk",
            )),
        },
    }
}

fn daily_rows_for_slice(
    message: StoreScanMessage,
    slice: &super::planner::PlannedSourceSlice,
    failure_reasons: &super::BacktestHistoryFailureReasons,
    request_id: u64,
    resources: Option<&super::BacktestHistorySnapshotQueryResources>,
) -> Result<ReservedProjectedRows<Kline>> {
    match message {
        StoreScanMessage::Failed(failure) => Err(record_store_scan_failure(
            failure_reasons,
            request_id,
            failure,
        )),
        StoreScanMessage::Chunk(chunk) => match &chunk.rows {
            StoreRows::CanonicalDaily(rows) => {
                let mut projected = ReservedProjectedRows::with_capacity(rows.len(), resources)?;
                projected.rows.extend(
                    rows.iter()
                        .filter(|row| row.datetime >= slice.range.0 && row.datetime < slice.range.1)
                        .cloned(),
                );
                projected.rows.sort_by_key(|row| (row.datetime, row.id));
                Ok(projected)
            }
            StoreRows::Ticks(_) | StoreRows::CanonicalMinutes(_) => Err(DataError::InvalidState(
                "native daily cache reader returned wrong source rows",
            )),
        },
    }
}

fn record_store_scan_failure(
    failure_reasons: &super::BacktestHistoryFailureReasons,
    request_id: u64,
    failure: StoreScanFailure,
) -> DataError {
    failure_reasons
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(request_id, failure.reason);
    DataError::InvalidResponse(failure.message)
}

async fn send_tick_chunk_with_reservation(
    event_sender: &mpsc::Sender<BacktestHistoryEventEnvelope>,
    plan: &PlannedBacktestHistoryRequest,
    rows: Vec<Tick>,
    reservation: Option<BacktestHistorySnapshotResourceReservation>,
    telemetry: &TelemetryHub,
    emitted_rows: usize,
) -> Result<usize> {
    if rows.is_empty() {
        return Ok(0);
    }
    let count = rows.len();
    event_sender
        .send(BacktestHistoryEventEnvelope::new(
            BacktestHistoryEvent::Chunk(BacktestHistoryChunk {
                request_id: plan.request_id,
                symbol: plan.symbol.clone(),
                rows: BacktestHistoryRows::Ticks(rows),
            }),
            reservation,
        ))
        .await
        .map_err(|_| DataError::InvalidState("backtest history event consumer was dropped"))?;
    telemetry.emit(BacktestHistoryTelemetryEvent {
        request_id: Some(plan.request_id),
        symbol: plan.symbol.clone(),
        phase: super::report::BacktestHistoryPhase::Read,
        completed_rows: emitted_rows.saturating_add(count),
        message: "streamed Tick cache rows".to_string(),
    });
    Ok(count)
}

async fn send_kline_chunk(
    event_sender: &mpsc::Sender<BacktestHistoryEventEnvelope>,
    plan: &PlannedBacktestHistoryRequest,
    duration_ns: i64,
    rows: Vec<Kline>,
    telemetry: &TelemetryHub,
    emitted_rows: usize,
) -> Result<usize> {
    send_kline_chunk_with_reservation(
        event_sender,
        plan,
        duration_ns,
        rows,
        None,
        telemetry,
        emitted_rows,
    )
    .await
}

async fn send_kline_chunk_with_reservation(
    event_sender: &mpsc::Sender<BacktestHistoryEventEnvelope>,
    plan: &PlannedBacktestHistoryRequest,
    duration_ns: i64,
    rows: Vec<Kline>,
    reservation: Option<BacktestHistorySnapshotResourceReservation>,
    telemetry: &TelemetryHub,
    emitted_rows: usize,
) -> Result<usize> {
    if rows.is_empty() {
        return Ok(0);
    }
    let count = rows.len();
    event_sender
        .send(BacktestHistoryEventEnvelope::new(
            BacktestHistoryEvent::Chunk(BacktestHistoryChunk {
                request_id: plan.request_id,
                symbol: plan.symbol.clone(),
                rows: BacktestHistoryRows::Klines { duration_ns, rows },
            }),
            reservation,
        ))
        .await
        .map_err(|_| DataError::InvalidState("backtest history event consumer was dropped"))?;
    telemetry.emit(BacktestHistoryTelemetryEvent {
        request_id: Some(plan.request_id),
        symbol: plan.symbol.clone(),
        phase: super::report::BacktestHistoryPhase::Aggregate,
        completed_rows: emitted_rows.saturating_add(count),
        message: "streamed locally aggregated Kline rows".to_string(),
    });
    Ok(count)
}

fn should_emit_daily_kline(
    row: &Kline,
    plan: &PlannedBacktestHistoryRequest,
    duration_ns: i64,
) -> Result<bool> {
    let bar_end_ns = row
        .datetime
        .checked_add(duration_ns)
        .ok_or_else(|| DataError::Validation("daily kline bar end overflow".to_string()))?;
    Ok(row.datetime >= plan.requested_range.0
        && row.datetime < plan.requested_range.1
        && bar_end_ns <= plan.effective_end_ns)
}

fn should_emit_kline(
    row: &Kline,
    plan: &PlannedBacktestHistoryRequest,
    duration_ns: i64,
) -> Result<bool> {
    Ok(row.datetime >= plan.requested_range.0
        && row.datetime < plan.requested_range.1
        && bar_end_ns(row.datetime, duration_ns, &plan.session)? <= plan.effective_end_ns)
}

fn provisional_as_of(plan: &PlannedBacktestHistoryRequest) -> Option<i64> {
    provisional_as_of_from_finality(plan.finality)
}

fn provisional_as_of_from_finality(finality: BacktestHistoryFinality) -> Option<i64> {
    match finality {
        BacktestHistoryFinality::Final => None,
        BacktestHistoryFinality::Provisional { as_of_ns } => Some(as_of_ns),
    }
}

fn estimated_kline_bytes(rows: usize) -> usize {
    rows.saturating_mul(std::mem::size_of::<Kline>())
}

fn merge_ranges(mut ranges: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
    ranges.sort_unstable();
    let mut merged: Vec<(i64, i64)> = Vec::new();
    for range in ranges {
        match merged.last_mut() {
            Some(previous) if range.0 <= previous.1 => previous.1 = previous.1.max(range.1),
            _ => merged.push(range),
        }
    }
    merged
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use chrono::{TimeZone, Utc};
    use tqsdk_core::{Kline, Tick};

    use super::super::{BacktestHistoryClient, BacktestHistoryPolicy, BacktestHistoryRequest};
    use crate::{
        BacktestTickCache, MinuteKlineCache, MinuteKlineCacheSnapshot,
        backtest_tick_trading_day_for_timestamp_ns, backtest_tick_trading_day_range,
    };

    const SECOND_NS: i64 = 1_000_000_000;
    const MINUTE_NS: i64 = 60 * SECOND_NS;

    #[tokio::test]
    async fn cancellation_interrupts_a_pending_cache_source_plan() {
        let cancellation = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn({
            let cancellation = Arc::clone(&cancellation);
            async move {
                super::await_or_request_cancelled(
                    cancellation.as_ref(),
                    std::future::pending::<crate::Result<()>>(),
                )
                .await
            }
        });
        tokio::task::yield_now().await;
        cancellation.store(true, Ordering::Release);

        let error = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("cache source planning must observe cancellation")
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("cancelled while planning"));
    }

    #[tokio::test]
    async fn cancellation_interrupts_a_pending_logical_permit() {
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let held = Arc::clone(&permits).acquire_owned().await.unwrap();
        let cancellation = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn({
            let permits = Arc::clone(&permits);
            let cancellation = Arc::clone(&cancellation);
            async move {
                super::acquire_logical_permit_until_cancelled(permits, cancellation.as_ref()).await
            }
        });
        tokio::task::yield_now().await;
        cancellation.store(true, Ordering::Release);

        let error = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("scheduler wait must observe cancellation")
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("scheduler capacity"));
        drop(held);
    }

    #[tokio::test]
    async fn a_cache_hit_batch_fans_out_one_tick_and_one_minute_base_scan() {
        let root = temp_dir("shared-scan");
        let symbol = "SHFE.au2608";
        let start_ns = utc_ns(2026, 1, 5, 1, 0, 0);
        let tick_day = backtest_tick_trading_day_for_timestamp_ns(start_ns).unwrap();
        let tick_day_range = backtest_tick_trading_day_range(tick_day).unwrap();
        BacktestTickCache::open(&root)
            .unwrap()
            .store_ticks(
                symbol,
                tick_day_range.start_ns,
                tick_day_range.end_ns,
                vec![
                    tick(1, start_ns, 10.0, 10),
                    tick(2, start_ns + 10 * SECOND_NS, 11.0, 11),
                    tick(3, start_ns + 16 * SECOND_NS, 12.0, 12),
                    tick(4, start_ns + 26 * SECOND_NS, 13.0, 13),
                    tick(5, start_ns + 41 * SECOND_NS, 14.0, 14),
                ],
            )
            .unwrap();
        let minute_end_ns = start_ns + 10 * MINUTE_NS;
        let minutes = (0_i64..10)
            .map(|index| kline(100 + index, start_ns + index * MINUTE_NS, index as f64))
            .collect::<Vec<_>>();
        MinuteKlineCache::open(&root)
            .unwrap()
            .store_final_range(
                symbol,
                start_ns,
                minute_end_ns,
                &MinuteKlineCacheSnapshot::cst_v1(),
                &minutes,
            )
            .unwrap();

        crate::backtest_history::store_worker::reset_scan_open_counts();
        let client = BacktestHistoryClient::builder(root)
            .policy(BacktestHistoryPolicy::CacheOnly)
            .blocking_workers(2)
            .build()
            .unwrap();
        let collected = client
            .query_batch([
                BacktestHistoryRequest::tick(1, symbol, start_ns, start_ns + 30 * SECOND_NS),
                BacktestHistoryRequest::kline(
                    2,
                    symbol,
                    Duration::from_secs(15),
                    start_ns,
                    start_ns + 30 * SECOND_NS,
                ),
                BacktestHistoryRequest::kline(
                    5,
                    symbol,
                    Duration::from_secs(15),
                    start_ns + 15 * SECOND_NS,
                    start_ns + 45 * SECOND_NS,
                ),
                BacktestHistoryRequest::kline(
                    3,
                    symbol,
                    Duration::from_secs(60),
                    start_ns,
                    minute_end_ns,
                ),
                BacktestHistoryRequest::kline(
                    4,
                    symbol,
                    Duration::from_secs(5 * 60),
                    start_ns,
                    minute_end_ns,
                ),
            ])
            .await
            .unwrap()
            .collect_all(64 * 1024 * 1024)
            .await
            .unwrap();

        assert_eq!(collected.completed.len(), 5);
        assert!(collected.failed.is_empty());
        assert_eq!(
            crate::backtest_history::store_worker::scan_open_counts(),
            (1, 1)
        );
    }

    fn tick(id: i64, datetime: i64, last_price: f64, volume: i64) -> Tick {
        Tick {
            id,
            datetime,
            last_price,
            volume,
            ..Tick::default()
        }
    }

    fn kline(id: i64, datetime: i64, price: f64) -> Kline {
        Kline {
            id,
            datetime,
            open: price,
            high: price + 1.0,
            low: price - 1.0,
            close: price + 0.5,
            volume: 1,
            open_oi: id,
            close_oi: id + 1,
            ..Kline::default()
        }
    }

    fn utc_ns(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> i64 {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
            .single()
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap()
    }

    fn temp_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "tqsdk-backtest-history-executor-{name}-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }
}
