//! Bounded blocking cache readers used by the asynchronous query executor.

use std::mem::size_of;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

#[cfg(test)]
use std::sync::atomic::AtomicUsize;

use tokio::sync::{Semaphore, mpsc};
use tqsdk_core::{Kline, Tick};

use crate::backtest_tick_cache::BacktestTickCache;
use crate::client::TickDataSeriesRequest;
use crate::daily_kline_cache::{DailyKlineCache, DailyKlineCacheSnapshot};
use crate::minute_kline_cache::{MinuteKlineCache, MinuteKlineCacheSnapshot};

#[cfg(test)]
static TICK_SCAN_OPENS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static MINUTE_SCAN_OPENS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
pub(crate) fn reset_scan_open_counts() {
    TICK_SCAN_OPENS.store(0, Ordering::Release);
    MINUTE_SCAN_OPENS.store(0, Ordering::Release);
}

#[cfg(test)]
pub(crate) fn scan_open_counts() -> (usize, usize) {
    (
        TICK_SCAN_OPENS.load(Ordering::Acquire),
        MINUTE_SCAN_OPENS.load(Ordering::Acquire),
    )
}

/// Shared byte budget for every Tick and canonical-minute base scan belonging
/// to one logical symbol. The producer waits off the Tokio runtime when a
/// downstream consumer is holding all available source chunks.
#[derive(Clone)]
pub(crate) struct SymbolBufferBudget {
    capacity_bytes: usize,
    shared: Arc<(Mutex<usize>, Condvar)>,
}

impl SymbolBufferBudget {
    pub(crate) fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes: capacity_bytes.max(size_of::<Tick>().max(size_of::<Kline>())),
            shared: Arc::new((Mutex::new(0), Condvar::new())),
        }
    }

    fn acquire_blocking(&self, bytes: usize, cancellation: &AtomicBool) -> Option<BytePermit> {
        let bytes = bytes.min(self.capacity_bytes).max(1);
        let (lock, wake) = &*self.shared;
        let mut used = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        while used.saturating_add(bytes) > self.capacity_bytes {
            if cancellation.load(Ordering::Acquire) {
                return None;
            }
            let (next, _) = wake
                .wait_timeout(used, Duration::from_millis(10))
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            used = next;
        }
        *used = used.saturating_add(bytes);
        Some(BytePermit {
            bytes,
            shared: Arc::clone(&self.shared),
        })
    }
}

#[derive(Debug)]
struct BytePermit {
    bytes: usize,
    shared: Arc<(Mutex<usize>, Condvar)>,
}

impl Drop for BytePermit {
    fn drop(&mut self) {
        let (lock, wake) = &*self.shared;
        let mut used = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        *used = used.saturating_sub(self.bytes);
        wake.notify_all();
    }
}

/// Immutable source rows retained behind a shared byte permit.
#[derive(Debug)]
pub(crate) struct StoreChunk {
    pub(crate) rows: StoreRows,
    _buffer_permit: BytePermit,
}

impl StoreChunk {
    fn ticks(
        rows: Vec<Tick>,
        budget: &SymbolBufferBudget,
        cancellation: &AtomicBool,
    ) -> Option<Self> {
        let bytes = rows.capacity().saturating_mul(size_of::<Tick>());
        let _buffer_permit = budget.acquire_blocking(bytes, cancellation)?;
        Some(Self {
            rows: StoreRows::Ticks(Arc::from(rows)),
            _buffer_permit,
        })
    }

    fn canonical_minutes(
        rows: Vec<Kline>,
        budget: &SymbolBufferBudget,
        cancellation: &AtomicBool,
    ) -> Option<Self> {
        let bytes = rows.capacity().saturating_mul(size_of::<Kline>());
        let _buffer_permit = budget.acquire_blocking(bytes, cancellation)?;
        Some(Self {
            rows: StoreRows::CanonicalMinutes(Arc::from(rows)),
            _buffer_permit,
        })
    }

    fn canonical_daily(
        rows: Vec<Kline>,
        budget: &SymbolBufferBudget,
        cancellation: &AtomicBool,
    ) -> Option<Self> {
        let bytes = rows.capacity().saturating_mul(size_of::<Kline>());
        let _buffer_permit = budget.acquire_blocking(bytes, cancellation)?;
        Some(Self {
            rows: StoreRows::CanonicalDaily(Arc::from(rows)),
            _buffer_permit,
        })
    }
}

/// The rows inside a [`StoreChunk`]. Cloning its enclosing `Arc` shares both
/// the decoded rows and their buffer permit across every fan-out consumer.
#[derive(Debug)]
pub(crate) enum StoreRows {
    Ticks(Arc<[Tick]>),
    CanonicalMinutes(Arc<[Kline]>),
    CanonicalDaily(Arc<[Kline]>),
}

/// One source-reader message. Failures use a string because one failure is
/// fanned out to many independent requests, while [`DataError`] itself is not
/// cloneable.
#[derive(Debug)]
pub(crate) enum StoreScanMessage {
    Chunk(Arc<StoreChunk>),
    Failed(String),
}

/// Complete specification for one bounded blocking cache scan.
pub(crate) enum StoreScanSpec {
    Tick(TickScanSpec),
    CanonicalMinute(MinuteScanSpec),
    CanonicalDaily(DailyScanSpec),
}

pub(crate) struct TickScanSpec {
    pub(crate) cache_dir: PathBuf,
    pub(crate) symbol: String,
    pub(crate) range: (i64, i64),
    pub(crate) provisional_as_of_ns: Option<i64>,
    pub(crate) target_bytes: usize,
    pub(crate) cancellation: Arc<AtomicBool>,
    pub(crate) permits: Arc<Semaphore>,
    pub(crate) buffer_budget: SymbolBufferBudget,
}

pub(crate) struct MinuteScanSpec {
    pub(crate) cache_dir: PathBuf,
    pub(crate) symbol: String,
    pub(crate) range: (i64, i64),
    pub(crate) snapshot: MinuteKlineCacheSnapshot,
    pub(crate) target_bytes: usize,
    pub(crate) cancellation: Arc<AtomicBool>,
    pub(crate) permits: Arc<Semaphore>,
    pub(crate) buffer_budget: SymbolBufferBudget,
}

pub(crate) struct DailyScanSpec {
    pub(crate) cache_dir: PathBuf,
    pub(crate) symbol: String,
    pub(crate) range: (i64, i64),
    pub(crate) snapshot: DailyKlineCacheSnapshot,
    pub(crate) cancellation: Arc<AtomicBool>,
    pub(crate) permits: Arc<Semaphore>,
    pub(crate) buffer_budget: SymbolBufferBudget,
}

/// Starts the selected source reader without occupying a Tokio worker while
/// file decoding or source-buffer backpressure is active.
pub(crate) fn spawn_scan(spec: StoreScanSpec) -> mpsc::Receiver<StoreScanMessage> {
    match spec {
        StoreScanSpec::Tick(spec) => spawn_tick_scan(spec),
        StoreScanSpec::CanonicalMinute(spec) => spawn_minute_scan(spec),
        StoreScanSpec::CanonicalDaily(spec) => spawn_daily_scan(spec),
    }
}

/// Spawns one Tick reader after acquiring a bounded blocking-worker permit.
fn spawn_tick_scan(spec: TickScanSpec) -> mpsc::Receiver<StoreScanMessage> {
    let TickScanSpec {
        cache_dir,
        symbol,
        range,
        provisional_as_of_ns,
        target_bytes,
        cancellation,
        permits,
        buffer_budget,
    } = spec;
    #[cfg(test)]
    TICK_SCAN_OPENS.fetch_add(1, Ordering::AcqRel);
    let (sender, receiver) = mpsc::channel(2);
    tokio::spawn(async move {
        let permit = match permits.acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                let _ = sender
                    .send(StoreScanMessage::Failed(
                        "backtest history blocking scan workers are unavailable".to_string(),
                    ))
                    .await;
                return;
            }
        };
        let blocking_sender = sender.clone();
        let blocking_cancellation = Arc::clone(&cancellation);
        let join = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let cache = BacktestTickCache::open_read_only(&cache_dir);
            let request = TickDataSeriesRequest::new(symbol, range.0, range.1);
            let mut reader = match cache.open_history_query_reader(request, provisional_as_of_ns) {
                Ok(reader) => reader,
                Err(error) => {
                    let _ =
                        blocking_sender.blocking_send(StoreScanMessage::Failed(error.to_string()));
                    return;
                }
            };
            loop {
                if blocking_cancellation.load(Ordering::Acquire) {
                    return;
                }
                let rows = match reader.next_tick_chunk(target_bytes) {
                    Ok(rows) => rows,
                    Err(error) => {
                        let _ = blocking_sender
                            .blocking_send(StoreScanMessage::Failed(error.to_string()));
                        return;
                    }
                };
                if rows.is_empty() {
                    return;
                }
                let Some(chunk) = StoreChunk::ticks(rows, &buffer_budget, &blocking_cancellation)
                else {
                    return;
                };
                if blocking_sender
                    .blocking_send(StoreScanMessage::Chunk(Arc::new(chunk)))
                    .is_err()
                {
                    return;
                }
            }
        });
        if let Err(error) = join.await
            && !cancellation.load(Ordering::Acquire)
        {
            let _ = sender
                .send(StoreScanMessage::Failed(format!(
                    "backtest history Tick blocking reader failed: {error}"
                )))
                .await;
        }
    });
    receiver
}

/// Spawns one canonical-minute reader after acquiring a bounded blocking-worker
/// permit. It never opens a mutable cache handle.
fn spawn_minute_scan(spec: MinuteScanSpec) -> mpsc::Receiver<StoreScanMessage> {
    let MinuteScanSpec {
        cache_dir,
        symbol,
        range,
        snapshot,
        target_bytes,
        cancellation,
        permits,
        buffer_budget,
    } = spec;
    #[cfg(test)]
    MINUTE_SCAN_OPENS.fetch_add(1, Ordering::AcqRel);
    let (sender, receiver) = mpsc::channel(2);
    tokio::spawn(async move {
        let permit = match permits.acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                let _ = sender
                    .send(StoreScanMessage::Failed(
                        "backtest history blocking scan workers are unavailable".to_string(),
                    ))
                    .await;
                return;
            }
        };
        let blocking_sender = sender.clone();
        let blocking_cancellation = Arc::clone(&cancellation);
        let join = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let cache = MinuteKlineCache::open_read_only(&cache_dir);
            let mut reader = match cache.open_reader(symbol, range.0, range.1, &snapshot) {
                Ok(reader) => reader,
                Err(error) => {
                    let _ =
                        blocking_sender.blocking_send(StoreScanMessage::Failed(error.to_string()));
                    return;
                }
            };
            loop {
                if blocking_cancellation.load(Ordering::Acquire) {
                    return;
                }
                let rows = match reader.next_kline_chunk(target_bytes) {
                    Ok(rows) => rows,
                    Err(error) => {
                        let _ = blocking_sender
                            .blocking_send(StoreScanMessage::Failed(error.to_string()));
                        return;
                    }
                };
                if rows.is_empty() {
                    return;
                }
                let Some(chunk) =
                    StoreChunk::canonical_minutes(rows, &buffer_budget, &blocking_cancellation)
                else {
                    return;
                };
                if blocking_sender
                    .blocking_send(StoreScanMessage::Chunk(Arc::new(chunk)))
                    .is_err()
                {
                    return;
                }
            }
        });
        if let Err(error) = join.await
            && !cancellation.load(Ordering::Acquire)
        {
            let _ = sender
                .send(StoreScanMessage::Failed(format!(
                    "backtest history canonical-minute blocking reader failed: {error}"
                )))
                .await;
        }
    });
    receiver
}

/// Spawns one native-daily reader. A query can request at most 28 daily rows,
/// so decoding one final-covered symbol file range remains bounded.
fn spawn_daily_scan(spec: DailyScanSpec) -> mpsc::Receiver<StoreScanMessage> {
    let DailyScanSpec {
        cache_dir,
        symbol,
        range,
        snapshot,
        cancellation,
        permits,
        buffer_budget,
    } = spec;
    let (sender, receiver) = mpsc::channel(2);
    tokio::spawn(async move {
        let permit = match permits.acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                let _ = sender
                    .send(StoreScanMessage::Failed(
                        "backtest history blocking scan workers unavailable".to_string(),
                    ))
                    .await;
                return;
            }
        };
        let blocking_sender = sender.clone();
        let blocking_cancellation = Arc::clone(&cancellation);
        let join = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            if blocking_cancellation.load(Ordering::Acquire) {
                return;
            }
            let rows = match DailyKlineCache::open_read_only(&cache_dir)
                .read_range(symbol, range.0, range.1, &snapshot)
            {
                Ok(rows) => rows,
                Err(error) => {
                    let _ =
                        blocking_sender.blocking_send(StoreScanMessage::Failed(error.to_string()));
                    return;
                }
            };
            if rows.is_empty() {
                return;
            }
            let Some(chunk) =
                StoreChunk::canonical_daily(rows, &buffer_budget, &blocking_cancellation)
            else {
                return;
            };
            let _ = blocking_sender.blocking_send(StoreScanMessage::Chunk(Arc::new(chunk)));
        });
        if let Err(error) = join.await
            && !cancellation.load(Ordering::Acquire)
        {
            let _ = sender
                .send(StoreScanMessage::Failed(format!(
                    "backtest history daily blocking reader failed: {error}"
                )))
                .await;
        }
    });
    receiver
}
