//! Async, cache-backed historical market-data queries for local backtests.

mod report;
mod request;

use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::Stream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::error::{DataError, Result};

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
        let mut seen_request_ids = BTreeSet::new();
        let mut validated = Vec::new();
        for request in requests {
            let request = request.validate()?;
            if !seen_request_ids.insert(request.request_id) {
                return Err(DataError::Validation(format!(
                    "backtest history batch contains duplicate request_id {}",
                    request.request_id
                )));
            }
            validated.push(request);
        }

        Ok(self.closed_run(validated))
    }

    fn closed_run(&self, requests: Vec<ValidatedBacktestHistoryRequest>) -> BacktestHistoryRun {
        let request_kinds = requests
            .iter()
            .map(|request| (request.request_id, (request.kind, request.duration_ns)))
            .collect();
        let failures = requests
            .iter()
            .map(|request| BacktestHistoryRequestFailure {
                request_id: request.request_id,
                symbol: request.symbol.clone(),
                error: self.initial_unavailable_message(request),
                emitted_rows: 0,
            })
            .collect::<Vec<_>>();
        let (event_sender, event_receiver) = mpsc::channel(failures.len().max(1));
        let (telemetry_sender, telemetry_receiver) = mpsc::unbounded_channel();
        drop(telemetry_sender);

        let report = Arc::new(Mutex::new(BacktestHistoryBatchReport::default()));
        let report_for_task = Arc::clone(&report);
        let coordinator = tokio::spawn(async move {
            let report = BacktestHistoryBatchReport {
                completed: Vec::new(),
                failed: failures,
            };
            for failure in &report.failed {
                let _ = event_sender
                    .send(BacktestHistoryEvent::RequestFailed(failure.clone()))
                    .await;
            }
            let mut stored = report_for_task
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *stored = report.clone();
            report
        });

        BacktestHistoryRun {
            events: event_receiver,
            coordinator: Some(coordinator),
            report,
            request_kinds,
            collect_limit_bytes: self.config.collect_limit_bytes,
            telemetry: Some(BacktestHistoryTelemetryStream {
                events: telemetry_receiver,
            }),
        }
    }

    fn initial_unavailable_message(&self, request: &ValidatedBacktestHistoryRequest) -> String {
        let source = match self.config.policy {
            BacktestHistoryPolicy::CacheOnly => "CacheOnly",
            BacktestHistoryPolicy::RemoteOnMiss => "RemoteOnMiss",
        };
        format!(
            "{source} backtest history query for {} in [{}, {}) is not attached to the cache planner yet",
            request.symbol, request.start_ns, request.end_ns
        )
    }
}

/// Stream of rows and terminal outcomes for one query or batch.
pub struct BacktestHistoryRun {
    events: mpsc::Receiver<BacktestHistoryEvent>,
    coordinator: Option<JoinHandle<BacktestHistoryBatchReport>>,
    report: Arc<Mutex<BacktestHistoryBatchReport>>,
    request_kinds: BTreeMap<BacktestHistoryRequestId, (BacktestHistoryKind, Option<i64>)>,
    collect_limit_bytes: usize,
    telemetry: Option<BacktestHistoryTelemetryStream>,
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
        let mut retained_bytes = 0_usize;

        while let Some(event) = self.next().await {
            match event {
                BacktestHistoryEvent::Chunk(chunk) => {
                    let chunk_bytes = chunk.rows.estimated_heap_bytes()?;
                    let attempted_bytes = retained_bytes.checked_add(chunk_bytes).ok_or(
                        DataError::CollectLimitExceeded {
                            limit_bytes: max_total_bytes,
                            attempted_bytes: usize::MAX,
                        },
                    )?;
                    if attempted_bytes > max_total_bytes {
                        return Err(DataError::CollectLimitExceeded {
                            limit_bytes: max_total_bytes,
                            attempted_bytes,
                        });
                    }
                    let rows = rows_by_request.get_mut(&chunk.request_id).ok_or_else(|| {
                        DataError::Validation(format!(
                            "backtest history received chunk for unknown request_id {}",
                            chunk.request_id
                        ))
                    })?;
                    rows.append(chunk.rows)?;
                    retained_bytes = attempted_bytes;
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

/// Independent, best-effort progress stream for a backtest history run.
pub struct BacktestHistoryTelemetryStream {
    events: mpsc::UnboundedReceiver<BacktestHistoryTelemetryEvent>,
}

impl BacktestHistoryTelemetryStream {
    /// Receives the next telemetry event, if any.
    pub async fn next(&mut self) -> Option<BacktestHistoryTelemetryEvent> {
        self.events.recv().await
    }
}
