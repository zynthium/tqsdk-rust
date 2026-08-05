//! Async, cache-backed historical market-data queries for local backtests.

mod executor;
mod fill;
mod metadata;
mod planner;
mod report;
mod request;
mod store_worker;
mod telemetry;

use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::Stream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::backtest_tick_cache::{BacktestTickCache, BacktestTickCacheOperationLock};
use crate::error::{DataError, Result};

pub use metadata::{
    BACKTEST_HISTORY_METADATA_FORMAT_ID, BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
    BacktestHistoryMaintenanceClient, BacktestHistoryMaintenanceClientBuilder,
    BacktestHistoryMarketKind, BacktestHistoryMetadataCache, BacktestHistoryMetadataSnapshot,
    BacktestHistoryTradingDay,
};
#[doc(hidden)]
pub use metadata::{
    plan_minute_cache_stale_partition_repair, resolve_backtest_metadata_snapshot,
    resolve_minute_cache_metadata_snapshot,
};
pub use report::{
    BacktestHistoryBatchReport, BacktestHistoryChunk, BacktestHistoryCollected,
    BacktestHistoryCollectedBatch, BacktestHistoryCoverageReport, BacktestHistoryEvent,
    BacktestHistoryFinality, BacktestHistoryPhase, BacktestHistoryPhysicalSegment,
    BacktestHistoryRequestFailure, BacktestHistoryRequestReport, BacktestHistoryRows,
    BacktestHistoryTelemetryEvent,
};
pub use request::{
    BacktestHistoryAuthProvider, BacktestHistoryClientBuilder, BacktestHistoryCredentials,
    BacktestHistoryKind, BacktestHistoryPolicy, BacktestHistoryRequest, BacktestHistoryRequestId,
};
pub use telemetry::BacktestHistoryTelemetryStream;

use executor::BacktestHistoryExecutionMode;
use request::{BacktestHistoryClientConfig, ValidatedBacktestHistoryRequest};

/// Cache-backed client for Tick and locally derived Kline history used by a
/// backtest.
#[derive(Clone)]
pub struct BacktestHistoryClient {
    config: Arc<BacktestHistoryClientConfig>,
}

impl BacktestHistoryClient {
    /// Starts configuring a client rooted at the shared backtest cache path.
    #[must_use]
    pub fn builder(cache_dir: impl Into<std::path::PathBuf>) -> BacktestHistoryClientBuilder {
        BacktestHistoryClientBuilder::new(cache_dir)
    }

    pub(crate) fn from_config(config: BacktestHistoryClientConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }

    /// Opens an asynchronous stream for one request.
    pub async fn query(&self, request: BacktestHistoryRequest) -> Result<BacktestHistoryRun> {
        self.query_batch([request]).await
    }

    /// Opens an asynchronous stream for a batch of independently terminal
    /// requests.
    pub async fn query_batch(
        &self,
        requests: impl IntoIterator<Item = BacktestHistoryRequest>,
    ) -> Result<BacktestHistoryRun> {
        let validated = validate_requests(requests)?;
        self.start_run(validated, BacktestHistoryExecutionMode::Query, None)
    }

    /// Opens a terminal-event/telemetry run that only establishes durable
    /// cache coverage and never scans rows back from disk.
    #[doc(hidden)]
    pub async fn materialize_cache_run(
        &self,
        requests: impl IntoIterator<Item = BacktestHistoryRequest>,
    ) -> Result<BacktestHistoryRun> {
        let validated = validate_requests(requests)?;
        self.start_run(
            validated,
            BacktestHistoryExecutionMode::MaterializeCache,
            None,
        )
    }

    /// Starts cache materialization under a caller-owned cache-root gate.
    #[doc(hidden)]
    pub async fn materialize_cache_run_with_root_gate(
        &self,
        requests: impl IntoIterator<Item = BacktestHistoryRequest>,
        root_gate: Arc<BacktestTickCacheOperationLock>,
    ) -> Result<BacktestHistoryRun> {
        let validated = validate_requests(requests)?;
        self.start_run(
            validated,
            BacktestHistoryExecutionMode::MaterializeCache,
            Some(root_gate),
        )
    }

    /// Materializes durable cache coverage for facade-owned replay inputs.
    ///
    /// Completed report `rows` count only rows newly persisted by this run;
    /// cache hits report zero. No cached row chunks are read or emitted.
    #[doc(hidden)]
    pub async fn materialize_cache(
        &self,
        requests: impl IntoIterator<Item = BacktestHistoryRequest>,
    ) -> Result<BacktestHistoryBatchReport> {
        let mut run = self.materialize_cache_run(requests).await?;
        while run.next().await.is_some() {}
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

    fn start_run(
        &self,
        requests: Vec<ValidatedBacktestHistoryRequest>,
        mode: BacktestHistoryExecutionMode,
        root_gate: Option<Arc<BacktestTickCacheOperationLock>>,
    ) -> Result<BacktestHistoryRun> {
        let root_gate = match self.config.policy {
            BacktestHistoryPolicy::CacheOnly => None,
            BacktestHistoryPolicy::RemoteOnMiss => {
                let gate = match root_gate {
                    Some(gate) => gate,
                    None => Arc::new(
                        BacktestTickCache::open(self.config.cache_dir.as_path())?
                            .try_acquire_remote_fill_shared_lock()?,
                    ),
                };
                if gate.cache_dir() != self.config.cache_dir.as_path() {
                    return Err(DataError::Validation(format!(
                        "backtest history root gate {} does not match cache root {}",
                        gate.cache_dir().display(),
                        self.config.cache_dir.display(),
                    )));
                }
                Some(gate)
            }
        };
        let request_kinds = requests
            .iter()
            .map(|request| (request.request_id, (request.kind, request.duration_ns)))
            .collect();
        let event_capacity = self.config.logical_concurrency.saturating_mul(2).max(1);
        let (event_sender, event_receiver) = mpsc::channel(event_capacity);
        let telemetry = telemetry::TelemetryHub::new();
        let report = Arc::new(Mutex::new(BacktestHistoryBatchReport::default()));
        let report_for_task = Arc::clone(&report);
        let config = Arc::clone(&self.config);
        let cancellation = Arc::new(AtomicBool::new(false));
        let cancellation_for_task = Arc::clone(&cancellation);
        let telemetry_for_task = telemetry.clone();
        let coordinator = tokio::spawn(async move {
            let _root_gate = root_gate;
            let report = executor::execute_batch(
                config,
                requests,
                event_sender,
                telemetry_for_task.clone(),
                cancellation_for_task,
                mode,
            )
            .await;
            telemetry_for_task.close();
            let mut stored = report_for_task
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *stored = report.clone();
            report
        });

        Ok(BacktestHistoryRun {
            events: event_receiver,
            coordinator: Some(coordinator),
            report,
            request_kinds,
            collect_limit_bytes: self.config.collect_limit_bytes,
            telemetry: Some(telemetry.stream()),
            cancellation,
        })
    }
}

fn validate_requests(
    requests: impl IntoIterator<Item = BacktestHistoryRequest>,
) -> Result<Vec<ValidatedBacktestHistoryRequest>> {
    let mut seen_request_ids = BTreeSet::new();
    let mut validated = Vec::new();
    for request in requests {
        let request = request.validate()?;
        planner::validate_source_policy(&request)?;
        if !seen_request_ids.insert(request.request_id) {
            return Err(DataError::Validation(format!(
                "backtest history batch contains duplicate request_id {}",
                request.request_id
            )));
        }
        validated.push(request);
    }
    Ok(validated)
}

/// Stream of rows and terminal outcomes for one query or batch.
pub struct BacktestHistoryRun {
    events: mpsc::Receiver<BacktestHistoryEvent>,
    coordinator: Option<JoinHandle<BacktestHistoryBatchReport>>,
    report: Arc<Mutex<BacktestHistoryBatchReport>>,
    request_kinds: BTreeMap<BacktestHistoryRequestId, (BacktestHistoryKind, Option<i64>)>,
    collect_limit_bytes: usize,
    telemetry: Option<BacktestHistoryTelemetryStream>,
    cancellation: Arc<AtomicBool>,
}

impl Drop for BacktestHistoryRun {
    fn drop(&mut self) {
        self.cancellation.store(true, Ordering::Release);
    }
}

impl BacktestHistoryRun {
    /// Receives the next chunk or terminal event without requiring a stream
    /// extension trait import.
    pub async fn next(&mut self) -> Option<BacktestHistoryEvent> {
        self.events.recv().await
    }

    /// Takes the independent best-effort telemetry stream, if it has not
    /// already been taken.
    pub fn take_telemetry(&mut self) -> Option<BacktestHistoryTelemetryStream> {
        self.telemetry.take()
    }

    /// Drains unconsumed events and returns all terminal outcomes.
    pub async fn finish(mut self) -> BacktestHistoryBatchReport {
        while self.events.recv().await.is_some() {}
        self.await_coordinator().await
    }

    /// Requests cancellation, drains terminal events, and waits for cache-fill
    /// tasks to flush accepted partial rows before returning.
    #[doc(hidden)]
    pub async fn cancel_and_finish(mut self) -> BacktestHistoryBatchReport {
        self.cancellation.store(true, Ordering::Release);
        while self.events.recv().await.is_some() {}
        self.await_coordinator().await
    }

    /// Materializes the sole request using the configured collection limit.
    pub async fn collect(self) -> Result<BacktestHistoryCollected> {
        if self.request_kinds.len() != 1 {
            return Err(DataError::Validation(
                "BacktestHistoryRun::collect requires exactly one request; use collect_all(max_total_bytes) for batches"
                    .to_string(),
            ));
        }
        let collect_limit_bytes = self.collect_limit_bytes;
        let mut collected = self.collect_all(collect_limit_bytes).await?;
        if let Some(failure) = collected.failed.pop() {
            return Err(DataError::RequestFailed {
                request_id: failure.request_id,
                message: failure.error,
                emitted_rows: failure.emitted_rows,
            });
        }
        if collected.completed.len() != 1 {
            return Err(DataError::Validation(
                "backtest history query ended without exactly one terminal success".to_string(),
            ));
        }
        Ok(collected.completed.remove(0))
    }

    /// Materializes all successful requests while enforcing a caller-supplied
    /// total memory limit.
    pub async fn collect_all(
        mut self,
        max_total_bytes: usize,
    ) -> Result<BacktestHistoryCollectedBatch> {
        if max_total_bytes == 0 {
            return Err(DataError::Validation(
                "backtest history collect_all max_total_bytes must be greater than zero"
                    .to_string(),
            ));
        }

        let mut rows_by_request = self
            .request_kinds
            .iter()
            .map(|(request_id, (kind, duration_ns))| {
                BacktestHistoryRows::empty_for_kind(*kind, *duration_ns)
                    .map(|rows| (*request_id, rows))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let mut retained_bytes = rows_by_request.values().try_fold(0_usize, |total, rows| {
            total
                .checked_add(rows.estimated_heap_bytes()?)
                .ok_or(DataError::CollectLimitExceeded {
                    limit_bytes: max_total_bytes,
                    attempted_bytes: usize::MAX,
                })
        })?;
        if retained_bytes > max_total_bytes {
            return Err(DataError::CollectLimitExceeded {
                limit_bytes: max_total_bytes,
                attempted_bytes: retained_bytes,
            });
        }

        while let Some(event) = self.next().await {
            match event {
                BacktestHistoryEvent::Chunk(chunk) => {
                    let rows = rows_by_request.get_mut(&chunk.request_id).ok_or_else(|| {
                        DataError::Validation(format!(
                            "backtest history received chunk for unknown request_id {}",
                            chunk.request_id
                        ))
                    })?;
                    let previous_bytes = rows.estimated_heap_bytes()?;
                    let projected_bytes = rows.projected_heap_bytes_after_append(&chunk.rows)?;
                    let attempted_bytes = retained_bytes
                        .checked_sub(previous_bytes)
                        .and_then(|total| total.checked_add(projected_bytes))
                        .ok_or(DataError::CollectLimitExceeded {
                            limit_bytes: max_total_bytes,
                            attempted_bytes: usize::MAX,
                        })?;
                    if attempted_bytes > max_total_bytes {
                        return Err(DataError::CollectLimitExceeded {
                            limit_bytes: max_total_bytes,
                            attempted_bytes,
                        });
                    }
                    rows.append(chunk.rows)?;
                    let actual_bytes = rows.estimated_heap_bytes()?;
                    retained_bytes = retained_bytes
                        .checked_sub(previous_bytes)
                        .and_then(|total| total.checked_add(actual_bytes))
                        .ok_or(DataError::CollectLimitExceeded {
                            limit_bytes: max_total_bytes,
                            attempted_bytes: usize::MAX,
                        })?;
                    if retained_bytes > max_total_bytes {
                        return Err(DataError::CollectLimitExceeded {
                            limit_bytes: max_total_bytes,
                            attempted_bytes: retained_bytes,
                        });
                    }
                }
                BacktestHistoryEvent::RequestCompleted(_) => {}
                BacktestHistoryEvent::RequestFailed(failure) => {
                    if let Some(rows) = rows_by_request.remove(&failure.request_id) {
                        retained_bytes =
                            retained_bytes.saturating_sub(rows.estimated_heap_bytes()?);
                    }
                }
            }
        }

        let terminal = self.await_coordinator().await;
        let completed = terminal
            .completed
            .into_iter()
            .map(|request| {
                let rows = rows_by_request.remove(&request.request_id).ok_or_else(|| {
                    DataError::Validation(format!(
                        "backtest history completed unknown request_id {}",
                        request.request_id
                    ))
                })?;
                Ok(BacktestHistoryCollected { request, rows })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(BacktestHistoryCollectedBatch {
            completed,
            failed: terminal.failed,
        })
    }

    async fn await_coordinator(&mut self) -> BacktestHistoryBatchReport {
        let fallback = self.stored_report();
        let Some(coordinator) = self.coordinator.take() else {
            return fallback;
        };
        coordinator.await.unwrap_or(fallback)
    }

    fn stored_report(&self) -> BacktestHistoryBatchReport {
        self.report
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl Stream for BacktestHistoryRun {
    type Item = BacktestHistoryEvent;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.events).poll_recv(context)
    }
}
