//! Bounded blocking cache readers used by the asynchronous query executor.

use std::mem::size_of;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

#[cfg(test)]
use std::sync::atomic::AtomicUsize;
#[cfg(test)]
use std::sync::{MutexGuard, OnceLock, mpsc as std_mpsc};

use tokio::sync::{Semaphore, mpsc};
use tqsdk_core::{Kline, Tick};

use super::BacktestHistorySnapshotQueryResources;
use super::snapshot_resources::BacktestHistorySnapshotResourceReservation;
use crate::backtest_tick_cache::BacktestTickCache;
use crate::client::TickDataSeriesRequest;
use crate::daily_kline_cache::{DailyKlineCache, DailyKlineCacheSnapshot};
use crate::error::DataError;
use crate::minute_kline_cache::{MinuteKlineCache, MinuteKlineCacheSnapshot};

#[cfg(test)]
static TICK_SCAN_OPENS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static MINUTE_SCAN_OPENS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
struct BlockingScanTestGateState {
    entered: Mutex<Option<std_mpsc::SyncSender<()>>>,
    released: Mutex<bool>,
    wake: Condvar,
    panic_after_release: bool,
}

#[cfg(test)]
static BLOCKING_SCAN_TEST_GATE: OnceLock<Mutex<Option<Arc<BlockingScanTestGateState>>>> =
    OnceLock::new();
#[cfg(test)]
static BLOCKING_SCAN_TEST_SERIAL: Mutex<()> = Mutex::new(());

#[cfg(test)]
pub(crate) struct BlockingScanTestGate {
    state: Arc<BlockingScanTestGateState>,
    _serial: MutexGuard<'static, ()>,
}

#[cfg(test)]
impl BlockingScanTestGate {
    pub(crate) fn install() -> (Self, std_mpsc::Receiver<()>) {
        Self::install_with_panic(false)
    }

    pub(crate) fn install_panicking() -> (Self, std_mpsc::Receiver<()>) {
        Self::install_with_panic(true)
    }

    fn install_with_panic(panic_after_release: bool) -> (Self, std_mpsc::Receiver<()>) {
        let serial = BLOCKING_SCAN_TEST_SERIAL
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (entered_sender, entered_receiver) = std_mpsc::sync_channel(1);
        let state = Arc::new(BlockingScanTestGateState {
            entered: Mutex::new(Some(entered_sender)),
            released: Mutex::new(false),
            wake: Condvar::new(),
            panic_after_release,
        });
        let mut installed = BLOCKING_SCAN_TEST_GATE
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            installed.is_none(),
            "blocking scan test gate already installed"
        );
        *installed = Some(Arc::clone(&state));
        (
            Self {
                state,
                _serial: serial,
            },
            entered_receiver,
        )
    }

    pub(crate) fn release(&self) {
        {
            let mut released = self
                .state
                .released
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *released = true;
            self.state.wake.notify_all();
        }
        let mut installed = BLOCKING_SCAN_TEST_GATE
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if installed
            .as_ref()
            .is_some_and(|state| Arc::ptr_eq(state, &self.state))
        {
            *installed = None;
        }
    }
}

#[cfg(test)]
impl Drop for BlockingScanTestGate {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
fn wait_on_blocking_scan_test_gate() {
    let state = BLOCKING_SCAN_TEST_GATE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    let Some(state) = state else {
        return;
    };
    if let Some(entered) = state
        .entered
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take()
    {
        let _ = entered.send(());
    }
    let mut released = state
        .released
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    while !*released {
        released = state
            .wake
            .wait(released)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
    }
    if state.panic_after_release {
        panic!("blocking scan test failure");
    }
}

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

#[cfg(test)]
#[test]
fn store_scan_failure_preserves_error_category_before_stringification() {
    let failure = StoreScanFailure::from_error(DataError::CacheBusy {
        cache_dir: PathBuf::from("test-cache"),
        operation: "test scan",
    });
    assert_eq!(
        failure.reason,
        super::BacktestHistoryFailureReason::SnapshotUnavailable
    );
    assert!(failure.message.contains("test scan"));
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

fn chunk_allocation_upper_bound(target_bytes: usize, row_bytes: usize) -> Result<usize, DataError> {
    let rows = target_bytes
        .checked_add(row_bytes.saturating_sub(1))
        .and_then(|bytes| bytes.checked_div(row_bytes))
        .unwrap_or(usize::MAX)
        .max(1);
    let vector_capacity =
        rows.checked_next_power_of_two()
            .ok_or_else(|| DataError::CollectLimitExceeded {
                limit_bytes: target_bytes,
                attempted_bytes: usize::MAX,
            })?;
    vector_capacity
        .checked_add(rows)
        .and_then(|rows| rows.checked_mul(row_bytes))
        .ok_or(DataError::CollectLimitExceeded {
            limit_bytes: target_bytes,
            attempted_bytes: usize::MAX,
        })
}

fn reserve_scan_chunk(
    resources: Option<&BacktestHistorySnapshotQueryResources>,
    allocation_upper_bound: usize,
) -> std::result::Result<Option<BacktestHistorySnapshotResourceReservation>, StoreScanFailure> {
    resources
        .map(|resources| {
            resources
                .try_reserve_for_scan(allocation_upper_bound)
                .map_err(StoreScanFailure::from_error)
        })
        .transpose()
}

/// Immutable source rows retained behind a shared byte permit.
#[derive(Debug)]
pub(crate) struct StoreChunk {
    pub(crate) rows: StoreRows,
    _buffer_permit: BytePermit,
    _scan_reservation: Option<BacktestHistorySnapshotResourceReservation>,
}

impl StoreChunk {
    fn ticks(
        rows: Vec<Tick>,
        budget: &SymbolBufferBudget,
        cancellation: &AtomicBool,
        scan_reservation: Option<BacktestHistorySnapshotResourceReservation>,
    ) -> Option<Self> {
        let bytes = rows.capacity().saturating_mul(size_of::<Tick>());
        let _buffer_permit = budget.acquire_blocking(bytes, cancellation)?;
        Some(Self {
            rows: StoreRows::Ticks(Arc::from(rows)),
            _buffer_permit,
            _scan_reservation: scan_reservation,
        })
    }

    fn canonical_minutes(
        rows: Vec<Kline>,
        budget: &SymbolBufferBudget,
        cancellation: &AtomicBool,
        scan_reservation: Option<BacktestHistorySnapshotResourceReservation>,
    ) -> Option<Self> {
        let bytes = rows.capacity().saturating_mul(size_of::<Kline>());
        let _buffer_permit = budget.acquire_blocking(bytes, cancellation)?;
        Some(Self {
            rows: StoreRows::CanonicalMinutes(Arc::from(rows)),
            _buffer_permit,
            _scan_reservation: scan_reservation,
        })
    }

    fn canonical_daily(
        rows: Vec<Kline>,
        budget: &SymbolBufferBudget,
        cancellation: &AtomicBool,
        scan_reservation: Option<BacktestHistorySnapshotResourceReservation>,
    ) -> Option<Self> {
        let bytes = rows.capacity().saturating_mul(size_of::<Kline>());
        let _buffer_permit = budget.acquire_blocking(bytes, cancellation)?;
        Some(Self {
            rows: StoreRows::CanonicalDaily(Arc::from(rows)),
            _buffer_permit,
            _scan_reservation: scan_reservation,
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

/// One source-reader message. Failures retain a cloneable typed reason plus the
/// legacy display message so one failure can fan out to many requests.
#[derive(Debug)]
pub(crate) enum StoreScanMessage {
    Chunk(Arc<StoreChunk>),
    Failed(StoreScanFailure),
}

#[derive(Debug, Clone)]
pub(crate) struct StoreScanFailure {
    pub(crate) reason: super::BacktestHistoryFailureReason,
    pub(crate) message: String,
}

impl StoreScanFailure {
    fn from_error(error: DataError) -> Self {
        Self {
            reason: super::classify_snapshot_failure(&error, false),
            message: error.to_string(),
        }
    }

    fn unavailable(message: impl Into<String>) -> Self {
        Self {
            reason: super::BacktestHistoryFailureReason::SnapshotUnavailable,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            reason: super::BacktestHistoryFailureReason::Internal,
            message: message.into(),
        }
    }
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
    pub(crate) lifecycle_pin: Option<super::BacktestHistoryLifecyclePin>,
    pub(crate) resources: Option<BacktestHistorySnapshotQueryResources>,
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
    pub(crate) lifecycle_pin: Option<super::BacktestHistoryLifecyclePin>,
    pub(crate) resources: Option<BacktestHistorySnapshotQueryResources>,
}

pub(crate) struct DailyScanSpec {
    pub(crate) cache_dir: PathBuf,
    pub(crate) symbol: String,
    pub(crate) range: (i64, i64),
    pub(crate) snapshot: DailyKlineCacheSnapshot,
    pub(crate) cancellation: Arc<AtomicBool>,
    pub(crate) permits: Arc<Semaphore>,
    pub(crate) buffer_budget: SymbolBufferBudget,
    pub(crate) lifecycle_pin: Option<super::BacktestHistoryLifecyclePin>,
    pub(crate) resources: Option<BacktestHistorySnapshotQueryResources>,
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
        lifecycle_pin,
        resources,
    } = spec;
    #[cfg(test)]
    TICK_SCAN_OPENS.fetch_add(1, Ordering::AcqRel);
    let (sender, receiver) = mpsc::channel(2);
    tokio::spawn(async move {
        let scan_lifecycle_pin = lifecycle_pin;
        let permit = match permits.acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                let _ = sender
                    .send(StoreScanMessage::Failed(StoreScanFailure::unavailable(
                        "backtest history blocking scan workers are unavailable",
                    )))
                    .await;
                return;
            }
        };
        let blocking_sender = sender.clone();
        let blocking_cancellation = Arc::clone(&cancellation);
        let blocking_lifecycle_pin = scan_lifecycle_pin.clone();
        let blocking_resources = resources;
        let join = tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            if blocking_lifecycle_pin.is_some() {
                wait_on_blocking_scan_test_gate();
            }
            let _lifecycle_pin = blocking_lifecycle_pin;
            let _permit = permit;
            let scan_allocation_upper_bound =
                match chunk_allocation_upper_bound(target_bytes, size_of::<Tick>()) {
                    Ok(bound) => bound,
                    Err(error) => {
                        let _ = blocking_sender.blocking_send(StoreScanMessage::Failed(
                            StoreScanFailure::from_error(error),
                        ));
                        return;
                    }
                };
            let mut next_scan_reservation = Some(
                match reserve_scan_chunk(blocking_resources.as_ref(), scan_allocation_upper_bound) {
                    Ok(reservation) => reservation,
                    Err(error) => {
                        let _ = blocking_sender.blocking_send(StoreScanMessage::Failed(error));
                        return;
                    }
                },
            );
            let cache = BacktestTickCache::open_read_only(&cache_dir);
            let request = TickDataSeriesRequest::new(symbol, range.0, range.1);
            let mut reader = match cache.open_history_query_reader(request, provisional_as_of_ns) {
                Ok(reader) => reader,
                Err(error) => {
                    let _ = blocking_sender.blocking_send(StoreScanMessage::Failed(
                        StoreScanFailure::from_error(error),
                    ));
                    return;
                }
            };
            loop {
                if blocking_cancellation.load(Ordering::Acquire) {
                    return;
                }
                let scan_reservation = match next_scan_reservation.take() {
                    Some(reservation) => reservation,
                    None => match reserve_scan_chunk(
                        blocking_resources.as_ref(),
                        scan_allocation_upper_bound,
                    ) {
                        Ok(reservation) => reservation,
                        Err(error) => {
                            let _ = blocking_sender.blocking_send(StoreScanMessage::Failed(error));
                            return;
                        }
                    },
                };
                let rows = match reader.next_tick_chunk(target_bytes) {
                    Ok(rows) => rows,
                    Err(error) => {
                        let _ = blocking_sender.blocking_send(StoreScanMessage::Failed(
                            StoreScanFailure::from_error(error),
                        ));
                        return;
                    }
                };
                if rows.is_empty() {
                    return;
                }
                let Some(chunk) = StoreChunk::ticks(
                    rows,
                    &buffer_budget,
                    &blocking_cancellation,
                    scan_reservation,
                ) else {
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
                .send(StoreScanMessage::Failed(StoreScanFailure::internal(
                    format!("backtest history Tick blocking reader failed: {error}"),
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
        lifecycle_pin,
        resources,
    } = spec;
    #[cfg(test)]
    MINUTE_SCAN_OPENS.fetch_add(1, Ordering::AcqRel);
    let (sender, receiver) = mpsc::channel(2);
    tokio::spawn(async move {
        let scan_lifecycle_pin = lifecycle_pin;
        let permit = match permits.acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                let _ = sender
                    .send(StoreScanMessage::Failed(StoreScanFailure::unavailable(
                        "backtest history blocking scan workers are unavailable",
                    )))
                    .await;
                return;
            }
        };
        let blocking_sender = sender.clone();
        let blocking_cancellation = Arc::clone(&cancellation);
        let blocking_lifecycle_pin = scan_lifecycle_pin.clone();
        let blocking_resources = resources;
        let join = tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            if blocking_lifecycle_pin.is_some() {
                wait_on_blocking_scan_test_gate();
            }
            let _lifecycle_pin = blocking_lifecycle_pin;
            let _permit = permit;
            let scan_allocation_upper_bound =
                match chunk_allocation_upper_bound(target_bytes, size_of::<Kline>()) {
                    Ok(bound) => bound,
                    Err(error) => {
                        let _ = blocking_sender.blocking_send(StoreScanMessage::Failed(
                            StoreScanFailure::from_error(error),
                        ));
                        return;
                    }
                };
            let mut next_scan_reservation = Some(
                match reserve_scan_chunk(blocking_resources.as_ref(), scan_allocation_upper_bound) {
                    Ok(reservation) => reservation,
                    Err(error) => {
                        let _ = blocking_sender.blocking_send(StoreScanMessage::Failed(error));
                        return;
                    }
                },
            );
            let cache = MinuteKlineCache::open_read_only(&cache_dir);
            let mut reader = match cache.open_reader(symbol, range.0, range.1, &snapshot) {
                Ok(reader) => reader,
                Err(error) => {
                    let _ = blocking_sender.blocking_send(StoreScanMessage::Failed(
                        StoreScanFailure::from_error(error),
                    ));
                    return;
                }
            };
            loop {
                if blocking_cancellation.load(Ordering::Acquire) {
                    return;
                }
                let scan_reservation = match next_scan_reservation.take() {
                    Some(reservation) => reservation,
                    None => match reserve_scan_chunk(
                        blocking_resources.as_ref(),
                        scan_allocation_upper_bound,
                    ) {
                        Ok(reservation) => reservation,
                        Err(error) => {
                            let _ = blocking_sender.blocking_send(StoreScanMessage::Failed(error));
                            return;
                        }
                    },
                };
                let rows = match reader.next_kline_chunk(target_bytes) {
                    Ok(rows) => rows,
                    Err(error) => {
                        let _ = blocking_sender.blocking_send(StoreScanMessage::Failed(
                            StoreScanFailure::from_error(error),
                        ));
                        return;
                    }
                };
                if rows.is_empty() {
                    return;
                }
                let Some(chunk) = StoreChunk::canonical_minutes(
                    rows,
                    &buffer_budget,
                    &blocking_cancellation,
                    scan_reservation,
                ) else {
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
                .send(StoreScanMessage::Failed(StoreScanFailure::internal(
                    format!("backtest history canonical-minute blocking reader failed: {error}"),
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
        lifecycle_pin,
        resources,
    } = spec;
    let (sender, receiver) = mpsc::channel(2);
    tokio::spawn(async move {
        let scan_lifecycle_pin = lifecycle_pin;
        let permit = match permits.acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                let _ = sender
                    .send(StoreScanMessage::Failed(StoreScanFailure::unavailable(
                        "backtest history blocking scan workers unavailable",
                    )))
                    .await;
                return;
            }
        };
        let blocking_sender = sender.clone();
        let blocking_cancellation = Arc::clone(&cancellation);
        let blocking_lifecycle_pin = scan_lifecycle_pin.clone();
        let blocking_resources = resources;
        let join = tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            if blocking_lifecycle_pin.is_some() {
                wait_on_blocking_scan_test_gate();
            }
            let _lifecycle_pin = blocking_lifecycle_pin;
            let _permit = permit;
            if blocking_cancellation.load(Ordering::Acquire) {
                return;
            }
            let cache = DailyKlineCache::open_read_only(&cache_dir);
            let allocation_upper_bound = match cache.read_range_allocation_upper_bound(&symbol) {
                Ok(bound) => bound,
                Err(error) => {
                    let _ = blocking_sender.blocking_send(StoreScanMessage::Failed(
                        StoreScanFailure::from_error(error),
                    ));
                    return;
                }
            };
            let scan_reservation =
                match reserve_scan_chunk(blocking_resources.as_ref(), allocation_upper_bound) {
                    Ok(reservation) => reservation,
                    Err(error) => {
                        let _ = blocking_sender.blocking_send(StoreScanMessage::Failed(error));
                        return;
                    }
                };
            #[cfg(test)]
            if let Some(resources) = blocking_resources.as_ref() {
                resources.record_daily_reader_open();
            }
            let rows = match cache.read_range_bounded(
                symbol,
                range.0,
                range.1,
                &snapshot,
                allocation_upper_bound,
            ) {
                Ok(rows) => rows,
                Err(error) => {
                    let _ = blocking_sender.blocking_send(StoreScanMessage::Failed(
                        StoreScanFailure::from_error(error),
                    ));
                    return;
                }
            };
            if rows.is_empty() {
                return;
            }
            let Some(chunk) = StoreChunk::canonical_daily(
                rows,
                &buffer_budget,
                &blocking_cancellation,
                scan_reservation,
            ) else {
                return;
            };
            let _ = blocking_sender.blocking_send(StoreScanMessage::Chunk(Arc::new(chunk)));
        });
        if let Err(error) = join.await
            && !cancellation.load(Ordering::Acquire)
        {
            let _ = sender
                .send(StoreScanMessage::Failed(StoreScanFailure::internal(
                    format!("backtest history daily blocking reader failed: {error}"),
                )))
                .await;
        }
    });
    receiver
}
