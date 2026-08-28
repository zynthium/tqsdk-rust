use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::error::{DataError, Result};

use super::{BacktestHistoryRequestId, BacktestHistoryTelemetryEvent};

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
#[derive(Debug, Clone, Default)]
pub struct BacktestHistoryFillCancellation {
    cancelled: Arc<AtomicBool>,
}

impl BacktestHistoryFillCancellation {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Requests cancellation. Accepted cache rows are flushed before the run ends.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
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
        symbols: Vec<String>,
    },
    Telemetry {
        family: BacktestHistoryFillFamily,
        event: BacktestHistoryTelemetryEvent,
    },
    BatchFinished {
        family: BacktestHistoryFillFamily,
        batch_number: usize,
        total_batches: usize,
        symbols: Vec<String>,
        rows_written: usize,
    },
    BatchFailed {
        family: BacktestHistoryFillFamily,
        batch_number: usize,
        total_batches: usize,
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
