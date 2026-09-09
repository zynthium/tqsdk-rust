//! Bounded blocking cache readers used by the asynchronous query executor.

use std::collections::BTreeSet;
use std::mem::size_of;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};

#[cfg(test)]
use std::sync::atomic::AtomicUsize;
#[cfg(test)]
use std::sync::{MutexGuard, OnceLock, mpsc as std_mpsc};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
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
static NEXT_SCAN_CANCELLATION_ID: AtomicU64 = AtomicU64::new(1);

/// Cancellation signal shared by async planning and blocking cache readers.
///
/// Blocking readers register their budget state so cancellation can wake a
/// full byte budget immediately instead of polling a timed condition wait.
pub(crate) struct ScanCancellation {
    id: u64,
    cancelled: AtomicBool,
    pub(super) stop_starting_fills: Arc<AtomicBool>,
    signal: watch::Sender<bool>,
    budgets: Mutex<Vec<Weak<BufferBudgetState>>>,
}

impl ScanCancellation {
    pub(crate) fn new() -> Self {
        Self::with_stop_signal(Arc::new(AtomicBool::new(false)))
    }

    pub(super) fn with_stop_signal(stop_starting_fills: Arc<AtomicBool>) -> Self {
        let (signal, _) = watch::channel(false);
        Self {
            id: NEXT_SCAN_CANCELLATION_ID.fetch_add(1, Ordering::Relaxed),
            cancelled: AtomicBool::new(false),
            stop_starting_fills,
            signal,
            budgets: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn as_atomic(&self) -> &AtomicBool {
        &self.cancelled
    }

    pub(crate) fn cancel(&self) {
        if self.cancelled.swap(true, Ordering::AcqRel) {
            return;
        }
        self.signal.send_replace(true);

        let mut budgets = self
            .budgets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        budgets.retain(|budget| {
            let Some(state) = budget.upgrade() else {
                return false;
            };
            let mut usage = state
                .usage
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            usage.cancelled.insert(self.id);
            state.wake.notify_all();
            true
        });
    }

    pub(crate) async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        let mut signal = self.signal.subscribe();
        if *signal.borrow() {
            return;
        }
        let _ = signal.changed().await;
    }

    fn register_budget(&self, budget: &Arc<BufferBudgetState>) {
        let mut budgets = self
            .budgets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        budgets.retain(|registered| registered.strong_count() > 0);
        let weak = Arc::downgrade(budget);
        if !budgets
            .iter()
            .any(|registered| Weak::ptr_eq(registered, &weak))
        {
            budgets.push(weak);
        }
        if self.is_cancelled() {
            let mut usage = budget
                .usage
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            usage.cancelled.insert(self.id);
            budget.wake.notify_all();
        }
    }
}

#[derive(Debug)]
struct BufferBudgetState {
    usage: Mutex<BufferBudgetUsage>,
    wake: Condvar,
}

#[derive(Debug, Default)]
struct BufferBudgetUsage {
    bytes: usize,
    cancelled: BTreeSet<u64>,
}

#[derive(Clone)]
pub(crate) struct SymbolBufferBudget {
    capacity_bytes: usize,
    shared: Arc<BufferBudgetState>,
}

impl SymbolBufferBudget {
    pub(crate) fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes: capacity_bytes.max(size_of::<Tick>().max(size_of::<Kline>())),
            shared: Arc::new(BufferBudgetState {
                usage: Mutex::new(BufferBudgetUsage::default()),
                wake: Condvar::new(),
            }),
        }
    }

    fn acquire_blocking(
        &self,
        bytes: usize,
        cancellation: &ScanCancellation,
    ) -> Option<BytePermit> {
        if cancellation.is_cancelled() {
            return None;
        }
        cancellation.register_budget(&self.shared);
        let bytes = bytes.min(self.capacity_bytes).max(1);
        let mut usage = self
            .shared
            .usage
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while usage.bytes.saturating_add(bytes) > self.capacity_bytes {
            if usage.cancelled.contains(&cancellation.id) {
                return None;
            }
            usage = self
                .shared
                .wake
                .wait(usage)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        if usage.cancelled.contains(&cancellation.id) {
            return None;
        }
        usage.bytes = usage.bytes.saturating_add(bytes);
        Some(BytePermit {
            bytes,
            shared: Arc::clone(&self.shared),
        })
    }
}

#[derive(Debug)]
struct BytePermit {
    bytes: usize,
    shared: Arc<BufferBudgetState>,
}

impl Drop for BytePermit {
    fn drop(&mut self) {
        let mut usage = self
            .shared
            .usage
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        usage.bytes = usage.bytes.saturating_sub(self.bytes);
        self.shared.wake.notify_all();
    }
}

#[cfg(test)]
#[test]
fn cancellation_wakes_a_full_byte_budget() {
    let budget = SymbolBufferBudget::new(size_of::<Tick>());
    let cancellation = Arc::new(ScanCancellation::new());
    let held = budget
        .acquire_blocking(size_of::<Tick>(), cancellation.as_ref())
        .expect("first permit fits budget");
    let (entered, entered_receiver) = std_mpsc::sync_channel(1);
    let (result, result_receiver) = std_mpsc::sync_channel(1);
    let waiting_budget = budget.clone();
    let waiting_cancellation = Arc::clone(&cancellation);
    let worker = std::thread::spawn(move || {
        let _ = entered.send(());
        let cancelled = waiting_budget
            .acquire_blocking(size_of::<Tick>(), waiting_cancellation.as_ref())
            .is_none();
        let _ = result.send(cancelled);
    });

    entered_receiver
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("second acquisition starts");
    cancellation.cancel();
    assert!(
        result_receiver
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("cancellation wakes blocked acquisition")
    );
    drop(held);
    worker.join().expect("blocked acquisition joins");
}

#[tokio::test(flavor = "current_thread")]
async fn shared_worker_budget_bounds_independent_local_worker_pools() {
    let first_local = Arc::new(Semaphore::new(1));
    let second_local = Arc::new(Semaphore::new(1));
    let shared = Arc::new(Semaphore::new(1));
    let first_cancellation = ScanCancellation::new();
    let first = acquire_blocking_worker_permits(
        first_local,
        Some(Arc::clone(&shared)),
        &first_cancellation,
    )
    .await
    .expect("first worker permit acquisition succeeds")
    .expect("first worker permit is not cancelled");
    assert_eq!(shared.available_permits(), 0);

    let second_cancellation = Arc::new(ScanCancellation::new());
    let waiting_cancellation = Arc::clone(&second_cancellation);
    let waiting_shared = Arc::clone(&shared);
    let waiting = tokio::spawn(async move {
        acquire_blocking_worker_permits(
            second_local,
            Some(waiting_shared),
            waiting_cancellation.as_ref(),
        )
        .await
    });
    tokio::task::yield_now().await;
    assert!(
        !waiting.is_finished(),
        "an independent local pool must wait for the shared daemon budget"
    );

    drop(first);
    let second = tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
        .await
        .expect("shared permit release wakes waiting scan")
        .expect("waiting task joins")
        .expect("second worker permit acquisition succeeds")
        .expect("second worker permit is not cancelled");
    assert_eq!(shared.available_permits(), 0);
    drop(second);
    assert_eq!(shared.available_permits(), 1);
}

fn chunk_allocation_upper_bound(target_bytes: usize, row_bytes: usize) -> Result<usize, DataError> {
    let rows = target_bytes
        .checked_add(row_bytes.saturating_sub(1))
        .and_then(|bytes| bytes.checked_div(row_bytes))
        .unwrap_or(usize::MAX)
        .max(1);
    let vector_capacity =
        rows.checked_next_power_of_two()
            .ok_or(DataError::CollectLimitExceeded {
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
        cancellation: &ScanCancellation,
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
        cancellation: &ScanCancellation,
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
        cancellation: &ScanCancellation,
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

#[cfg(test)]
pub(crate) fn test_tick_chunk(rows: Vec<Tick>) -> Arc<StoreChunk> {
    let budget = SymbolBufferBudget::new(usize::MAX);
    let cancellation = ScanCancellation::new();
    Arc::new(
        StoreChunk::ticks(rows, &budget, &cancellation, None)
            .expect("fresh test cancellation must acquire a byte permit"),
    )
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
    pub(crate) cancellation: Arc<ScanCancellation>,
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
    pub(crate) provisional_as_of_ns: Option<i64>,
    pub(crate) target_bytes: usize,
    pub(crate) cancellation: Arc<ScanCancellation>,
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
    pub(crate) cancellation: Arc<ScanCancellation>,
    pub(crate) permits: Arc<Semaphore>,
    pub(crate) buffer_budget: SymbolBufferBudget,
    pub(crate) lifecycle_pin: Option<super::BacktestHistoryLifecyclePin>,
    pub(crate) resources: Option<BacktestHistorySnapshotQueryResources>,
}

/// Starts the selected source reader without occupying a Tokio worker while
/// file decoding or source-buffer backpressure is active.
async fn acquire_blocking_worker_permits(
    local_permits: Arc<Semaphore>,
    shared_permits: Option<Arc<Semaphore>>,
    cancellation: &ScanCancellation,
) -> std::result::Result<
    Option<(OwnedSemaphorePermit, Option<OwnedSemaphorePermit>)>,
    StoreScanFailure,
> {
    let Some(local_permit) = acquire_worker_permit_until_cancelled(
        local_permits,
        cancellation,
        "backtest history blocking scan workers unavailable",
    )
    .await?
    else {
        return Ok(None);
    };

    let Some(shared_permits) = shared_permits else {
        return Ok(Some((local_permit, None)));
    };
    let Some(shared_permit) = acquire_worker_permit_until_cancelled(
        shared_permits,
        cancellation,
        "backtest history daemon blocking scan workers unavailable",
    )
    .await?
    else {
        return Ok(None);
    };
    Ok(Some((local_permit, Some(shared_permit))))
}

async fn acquire_worker_permit_until_cancelled(
    permits: Arc<Semaphore>,
    cancellation: &ScanCancellation,
    unavailable: &'static str,
) -> std::result::Result<Option<OwnedSemaphorePermit>, StoreScanFailure> {
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Ok(None),
        permit = permits.acquire_owned() => permit
            .map(Some)
            .map_err(|_| StoreScanFailure::unavailable(unavailable)),
    }
}

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
        let shared_worker_permits = resources
            .as_ref()
            .and_then(BacktestHistorySnapshotQueryResources::blocking_worker_permits);
        let (permit, shared_permit) = match acquire_blocking_worker_permits(
            permits,
            shared_worker_permits,
            cancellation.as_ref(),
        )
        .await
        {
            Ok(Some(permits)) => permits,
            Ok(None) => return,
            Err(error) => {
                let _ = sender.send(StoreScanMessage::Failed(error)).await;
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
            let _worker_permits = (permit, shared_permit);
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
                if blocking_cancellation.is_cancelled() {
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
            && !cancellation.is_cancelled()
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
        provisional_as_of_ns,
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
        let shared_worker_permits = resources
            .as_ref()
            .and_then(BacktestHistorySnapshotQueryResources::blocking_worker_permits);
        let (permit, shared_permit) = match acquire_blocking_worker_permits(
            permits,
            shared_worker_permits,
            cancellation.as_ref(),
        )
        .await
        {
            Ok(Some(permits)) => permits,
            Ok(None) => return,
            Err(error) => {
                let _ = sender.send(StoreScanMessage::Failed(error)).await;
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
            let _worker_permits = (permit, shared_permit);
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
            let mut reader = match cache.open_history_query_reader(
                symbol,
                range.0,
                range.1,
                &snapshot,
                provisional_as_of_ns,
            ) {
                Ok(reader) => reader,
                Err(error) => {
                    let _ = blocking_sender.blocking_send(StoreScanMessage::Failed(
                        StoreScanFailure::from_error(error),
                    ));
                    return;
                }
            };
            loop {
                if blocking_cancellation.is_cancelled() {
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
            && !cancellation.is_cancelled()
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
        let shared_worker_permits = resources
            .as_ref()
            .and_then(BacktestHistorySnapshotQueryResources::blocking_worker_permits);
        let (permit, shared_permit) = match acquire_blocking_worker_permits(
            permits,
            shared_worker_permits,
            cancellation.as_ref(),
        )
        .await
        {
            Ok(Some(permits)) => permits,
            Ok(None) => return,
            Err(error) => {
                let _ = sender.send(StoreScanMessage::Failed(error)).await;
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
            let _worker_permits = (permit, shared_permit);
            if blocking_cancellation.is_cancelled() {
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
            && !cancellation.is_cancelled()
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
