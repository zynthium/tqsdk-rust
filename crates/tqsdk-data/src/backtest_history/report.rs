use std::mem::size_of;

use tqsdk_core::{Kline, Tick};

use crate::error::{DataError, Result};

use super::{BacktestHistoryKind, BacktestHistoryRequestId};

/// Rows delivered by one backtest history chunk.
#[derive(Debug, Clone)]
pub enum BacktestHistoryRows {
    /// Tick rows from the durable Tick cache.
    Ticks(Vec<Tick>),
    /// Kline rows and their requested duration in nanoseconds.
    Klines { duration_ns: i64, rows: Vec<Kline> },
}

impl BacktestHistoryRows {
    /// Returns the number of rows in this chunk.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Ticks(rows) => rows.len(),
            Self::Klines { rows, .. } => rows.len(),
        }
    }

    /// Returns whether this chunk has no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Estimates owned heap use from vector capacity and row type size.
    ///
    /// The estimate intentionally uses capacity instead of length so collection
    /// limits reserve enough space for the vectors retained by callers.
    pub fn estimated_heap_bytes(&self) -> Result<usize> {
        match self {
            Self::Ticks(rows) => estimated_rows_heap_bytes(rows.capacity(), size_of::<Tick>()),
            Self::Klines { rows, .. } => {
                estimated_rows_heap_bytes(rows.capacity(), size_of::<Kline>())
            }
        }
    }

    pub(crate) fn empty_for_kind(
        kind: BacktestHistoryKind,
        duration_ns: Option<i64>,
    ) -> Result<Self> {
        match kind {
            BacktestHistoryKind::Tick => Ok(Self::Ticks(Vec::new())),
            BacktestHistoryKind::Kline { .. } => Ok(Self::Klines {
                duration_ns: duration_ns.ok_or_else(|| {
                    DataError::Validation(
                        "validated backtest history Kline request is missing its duration"
                            .to_string(),
                    )
                })?,
                rows: Vec::new(),
            }),
        }
    }

    pub(crate) fn append(&mut self, other: Self) -> Result<()> {
        match (self, other) {
            (Self::Ticks(current), Self::Ticks(mut incoming)) => {
                reserve_for_append(current, incoming.len())?;
                current.append(&mut incoming);
                Ok(())
            }
            (
                Self::Klines {
                    duration_ns: current_duration,
                    rows: current,
                },
                Self::Klines {
                    duration_ns: incoming_duration,
                    mut rows,
                },
            ) if *current_duration == incoming_duration => {
                reserve_for_append(current, rows.len())?;
                current.append(&mut rows);
                Ok(())
            }
            _ => Err(DataError::Validation(
                "backtest history chunks for one request must have the same row kind and duration"
                    .to_string(),
            )),
        }
    }

    /// Estimates the retained allocation after appending compatible rows.
    ///
    /// The result deliberately excludes the incoming vector's allocation: its
    /// elements are moved into `self` and the incoming buffer is dropped.
    pub(crate) fn projected_heap_bytes_after_append(&self, incoming: &Self) -> Result<usize> {
        match (self, incoming) {
            (Self::Ticks(current), Self::Ticks(incoming)) => projected_rows_heap_bytes(
                current.len(),
                current.capacity(),
                incoming.len(),
                size_of::<Tick>(),
            ),
            (
                Self::Klines {
                    duration_ns: current_duration,
                    rows: current,
                },
                Self::Klines {
                    duration_ns: incoming_duration,
                    rows: incoming,
                },
            ) if current_duration == incoming_duration => projected_rows_heap_bytes(
                current.len(),
                current.capacity(),
                incoming.len(),
                size_of::<Kline>(),
            ),
            _ => Err(DataError::Validation(
                "backtest history chunks for one request must have the same row kind and duration"
                    .to_string(),
            )),
        }
    }
}

fn reserve_for_append<T>(rows: &mut Vec<T>, incoming_len: usize) -> Result<()> {
    let required_len = rows
        .len()
        .checked_add(incoming_len)
        .ok_or_else(collect_size_overflow)?;
    if required_len > rows.capacity() {
        rows.try_reserve_exact(required_len - rows.len())
            .map_err(|_| collect_size_overflow())?;
    }
    Ok(())
}

fn projected_rows_heap_bytes(
    current_len: usize,
    current_capacity: usize,
    incoming_len: usize,
    row_size: usize,
) -> Result<usize> {
    let required_len = current_len
        .checked_add(incoming_len)
        .ok_or_else(collect_size_overflow)?;
    estimated_rows_heap_bytes(current_capacity.max(required_len), row_size)
}

fn estimated_rows_heap_bytes(capacity: usize, row_size: usize) -> Result<usize> {
    let rows_bytes = capacity
        .checked_mul(row_size)
        .ok_or_else(collect_size_overflow)?;
    size_of::<BacktestHistoryRows>()
        .checked_add(rows_bytes)
        .ok_or_else(collect_size_overflow)
}

fn collect_size_overflow() -> DataError {
    DataError::CollectLimitExceeded {
        limit_bytes: usize::MAX,
        attempted_bytes: usize::MAX,
    }
}

/// Whether a completed request is final or intentionally provisional.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktestHistoryFinality {
    /// All underlying cache coverage is terminal and closed.
    Final,
    /// Tick-derived rows observed at the caller-provided timestamp.
    Provisional { as_of_ns: i64 },
}

/// One physical-symbol interval serving a logical request.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BacktestHistoryPhysicalSegment {
    pub physical_symbol: String,
    pub start_ns: i64,
    pub end_ns: i64,
}

/// Cache and remote-fill coverage used to answer a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestHistoryCoverageReport {
    pub requested_range: (i64, i64),
    pub expanded_source_range: (i64, i64),
    pub cached_ranges: Vec<(i64, i64)>,
    pub remote_filled_ranges: Vec<(i64, i64)>,
    pub finality: BacktestHistoryFinality,
}

/// Terminal success report for one request.
#[derive(Debug, Clone)]
pub struct BacktestHistoryRequestReport {
    pub request_id: BacktestHistoryRequestId,
    pub symbol: String,
    pub kind: BacktestHistoryKind,
    pub rows: usize,
    pub physical_segments: Vec<BacktestHistoryPhysicalSegment>,
    pub snapshot_hash: String,
    pub coverage: BacktestHistoryCoverageReport,
    pub remote_used: bool,
}

/// Terminal failure report for one request.
#[derive(Debug, Clone)]
pub struct BacktestHistoryRequestFailure {
    pub request_id: BacktestHistoryRequestId,
    pub symbol: String,
    pub error: String,
    pub emitted_rows: usize,
}

/// One delivered data chunk. Chunks remain provisional until the matching
/// [`BacktestHistoryEvent::RequestCompleted`] event arrives.
#[derive(Debug, Clone)]
pub struct BacktestHistoryChunk {
    pub request_id: BacktestHistoryRequestId,
    pub symbol: String,
    pub rows: BacktestHistoryRows,
}

/// A data chunk or terminal event for a requested history range.
#[derive(Debug, Clone)]
pub enum BacktestHistoryEvent {
    /// A provisional chunk of ordered rows.
    Chunk(BacktestHistoryChunk),
    /// The corresponding request completed successfully.
    RequestCompleted(BacktestHistoryRequestReport),
    /// The corresponding request failed without cancelling other requests.
    RequestFailed(BacktestHistoryRequestFailure),
}

/// Success data materialized by [`super::BacktestHistoryRun::collect`].
#[derive(Debug, Clone)]
pub struct BacktestHistoryCollected {
    pub request: BacktestHistoryRequestReport,
    pub rows: BacktestHistoryRows,
}

/// Materialized results for a batch request.
#[derive(Debug, Clone, Default)]
pub struct BacktestHistoryCollectedBatch {
    pub completed: Vec<BacktestHistoryCollected>,
    pub failed: Vec<BacktestHistoryRequestFailure>,
}

/// All terminal outcomes observed for a batch.
#[derive(Debug, Clone, Default)]
pub struct BacktestHistoryBatchReport {
    pub completed: Vec<BacktestHistoryRequestReport>,
    pub failed: Vec<BacktestHistoryRequestFailure>,
}

/// Progress phase emitted on the independent telemetry stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktestHistoryPhase {
    Inspect,
    WaitForFill,
    Fill,
    Retry,
    Read,
    Aggregate,
}

/// A best-effort progress snapshot that never delays data chunks.
#[derive(Debug, Clone)]
pub struct BacktestHistoryTelemetryEvent {
    pub request_id: Option<BacktestHistoryRequestId>,
    pub symbol: String,
    pub phase: BacktestHistoryPhase,
    pub completed_rows: usize,
    pub message: String,
}
