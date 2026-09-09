use std::{
    mem::size_of,
    ops::Deref,
    sync::{
        Arc, LockResult, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use serde_json::{Map, Value};

use crate::{Result, events::NormalizedMutation, ids::Revision};

use super::{
    AppliedChange, MarketStateReadGuard, MarketStateView, MarketTradeStateReadGuard, ObjectKey,
    PathSegment, StateReadView, TradeStateReadGuard, TradeStateView, read::get_at_path,
};

/// Owned snapshot clone of the runtime state tree.
///
/// Prefer `StateReadView` and `SnapshotReadGuard` on hot paths. Keep
/// `StateSnapshot` when detached ownership is required.
#[derive(Debug, Clone)]
pub struct StateSnapshot {
    revision: Revision,
    data: Arc<SnapshotData>,
}

#[derive(Debug)]
enum SnapshotData {
    Materialized(Value),
    Roots(SnapshotRoots),
}

/// Immutable partition root set used by revision snapshots.
///
/// The materialized object is deliberately lazy: normal typed/path reads
/// resolve their first root directly and do not rebuild a whole JSON tree.
#[derive(Debug)]
pub(crate) struct SnapshotRoots {
    quotes: Arc<Value>,
    trading_status: Arc<Value>,
    charts: Arc<Value>,
    klines: Arc<Value>,
    ticks: Arc<Value>,
    trade: Arc<Value>,
    query: Arc<Value>,
    schema: Arc<Value>,
    replay: Arc<Value>,
    system: Arc<Value>,
    runtime: Arc<Value>,
    other: Arc<Value>,
    materialized: OnceLock<Value>,
}

/// Cumulative read-side runtime-state telemetry.
///
/// Snapshot values count shared root references. COW snapshots intentionally
/// clone no JSON nodes or bytes on their read path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateReadTelemetry {
    pub snapshot_reads: u64,
    pub snapshot_shared_roots: u64,
    pub snapshot_bytes_cloned: u64,
    pub snapshot_nodes_cloned: u64,
    pub snapshot_lock_wait_ns: u64,
    pub snapshot_lock_hold_ns: u64,
    pub live_guard_acquisitions: u64,
    pub live_guard_lock_wait_ns: u64,
    pub live_guard_lock_hold_ns: u64,
}

#[derive(Debug, Default)]
pub(crate) struct StateReadTelemetryState {
    snapshot_reads: AtomicU64,
    snapshot_shared_roots: AtomicU64,
    snapshot_bytes_cloned: AtomicU64,
    snapshot_nodes_cloned: AtomicU64,
    snapshot_lock_wait_ns: AtomicU64,
    snapshot_lock_hold_ns: AtomicU64,
    live_guard_acquisitions: AtomicU64,
    live_guard_lock_wait_ns: AtomicU64,
    live_guard_lock_hold_ns: AtomicU64,
}

impl StateReadTelemetryState {
    fn snapshot(&self) -> StateReadTelemetry {
        StateReadTelemetry {
            snapshot_reads: self.snapshot_reads.load(Ordering::Relaxed),
            snapshot_shared_roots: self.snapshot_shared_roots.load(Ordering::Relaxed),
            snapshot_bytes_cloned: self.snapshot_bytes_cloned.load(Ordering::Relaxed),
            snapshot_nodes_cloned: self.snapshot_nodes_cloned.load(Ordering::Relaxed),
            snapshot_lock_wait_ns: self.snapshot_lock_wait_ns.load(Ordering::Relaxed),
            snapshot_lock_hold_ns: self.snapshot_lock_hold_ns.load(Ordering::Relaxed),
            live_guard_acquisitions: self.live_guard_acquisitions.load(Ordering::Relaxed),
            live_guard_lock_wait_ns: self.live_guard_lock_wait_ns.load(Ordering::Relaxed),
            live_guard_lock_hold_ns: self.live_guard_lock_hold_ns.load(Ordering::Relaxed),
        }
    }

    fn record_snapshot(&self, shared_roots: usize, lock_wait: Duration, lock_hold: Duration) {
        saturating_add(&self.snapshot_reads, 1);
        saturating_add(
            &self.snapshot_shared_roots,
            u64::try_from(shared_roots).unwrap_or(u64::MAX),
        );
        saturating_add(&self.snapshot_bytes_cloned, 0);
        saturating_add(&self.snapshot_nodes_cloned, 0);
        saturating_add(&self.snapshot_lock_wait_ns, duration_nanos(lock_wait));
        saturating_add(&self.snapshot_lock_hold_ns, duration_nanos(lock_hold));
    }

    fn record_live_guard(&self, lock_wait: Duration, lock_hold: Duration, count_wait: bool) {
        if count_wait {
            saturating_add(&self.live_guard_acquisitions, 1);
            saturating_add(&self.live_guard_lock_wait_ns, duration_nanos(lock_wait));
        }
        saturating_add(&self.live_guard_lock_hold_ns, duration_nanos(lock_hold));
    }
}

/// Deferred live-guard metric sink.
///
/// Guard structs declare this after their lock fields. Its `Drop` therefore
/// records after those locks have been released, outside the critical section.
pub(crate) struct LiveGuardTelemetry<'a> {
    telemetry: &'a StateReadTelemetryState,
    lock_wait: Duration,
    acquired_at: Instant,
    count_wait: bool,
}

impl<'a> LiveGuardTelemetry<'a> {
    pub(crate) fn new(
        telemetry: &'a StateReadTelemetryState,
        lock_wait: Duration,
        count_wait: bool,
    ) -> Self {
        Self {
            telemetry,
            lock_wait,
            acquired_at: Instant::now(),
            count_wait,
        }
    }
}

impl Drop for LiveGuardTelemetry<'_> {
    fn drop(&mut self) {
        self.telemetry.record_live_guard(
            self.lock_wait,
            self.acquired_at.elapsed(),
            self.count_wait,
        );
    }
}

fn saturating_add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

fn duration_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[derive(Debug)]
pub(crate) struct StateStore {
    revision: AtomicU64,
    quotes: RwLock<Arc<Value>>,
    trading_status: RwLock<Arc<Value>>,
    charts: RwLock<Arc<Value>>,
    klines: RwLock<Arc<Value>>,
    ticks: RwLock<Arc<Value>>,
    trade: RwLock<Arc<Value>>,
    query: RwLock<Arc<Value>>,
    schema: RwLock<Arc<Value>>,
    replay: RwLock<Arc<Value>>,
    system: RwLock<Arc<Value>>,
    runtime: RwLock<Arc<Value>>,
    other: RwLock<Arc<Value>>,
    read_telemetry: StateReadTelemetryState,
}

impl StateSnapshot {
    /// Creates an owned empty snapshot at the provided revision.
    pub fn new(revision: Revision) -> Self {
        Self {
            revision,
            data: Arc::new(SnapshotData::Materialized(Value::Object(Map::new()))),
        }
    }

    /// Returns the revision carried by this owned snapshot.
    pub fn revision(&self) -> Revision {
        self.revision
    }

    /// Looks up a value at the provided path.
    pub fn get<I, S>(&self, path: I) -> Option<&Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.read().get(path)
    }

    /// Looks up a value using a borrowed path slice.
    pub fn get_path(&self, path: &[&str]) -> Option<&Value> {
        self.read().get_path(path)
    }

    /// Decodes a value at the provided path.
    pub fn decode<T, I, S>(&self, path: I) -> Result<Option<T>>
    where
        T: serde::de::DeserializeOwned,
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.read().decode(path)
    }

    /// Decodes a value using a borrowed path slice.
    pub fn decode_path<T>(&self, path: &[&str]) -> Result<Option<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        self.read().decode_path(path)
    }

    /// Returns a borrowed read view over this owned snapshot.
    pub fn read(&self) -> StateReadView<'_> {
        match self.data.as_ref() {
            SnapshotData::Materialized(data) => StateReadView::new(self.revision, data),
            SnapshotData::Roots(roots) => StateReadView::from_roots(self.revision, roots),
        }
    }

    /// Returns a typed market-domain view over this owned snapshot.
    pub fn market_state(&self) -> MarketStateView<'_> {
        self.read().market_state()
    }

    /// Returns a typed trade-domain view over this owned snapshot.
    pub fn trade_state(&self) -> TradeStateView<'_> {
        self.read().trade_state()
    }

    fn from_roots(revision: Revision, roots: SnapshotRoots) -> Self {
        Self {
            revision,
            data: Arc::new(SnapshotData::Roots(roots)),
        }
    }

    pub(crate) fn retained_root_keys(&self) -> Vec<usize> {
        match self.data.as_ref() {
            SnapshotData::Materialized(_) => vec![Arc::as_ptr(&self.data) as usize],
            SnapshotData::Roots(roots) => roots.root_keys(),
        }
    }

    pub(crate) fn retained_root_bytes_for(&self, key: usize) -> Option<usize> {
        match self.data.as_ref() {
            SnapshotData::Materialized(data) if Arc::as_ptr(&self.data) as usize == key => {
                Some(estimated_value_bytes(data).saturating_add(size_of::<SnapshotData>()))
            }
            SnapshotData::Materialized(_) => None,
            SnapshotData::Roots(roots) => roots.root_bytes_for(key),
        }
    }
}

impl PartialEq for StateSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.revision == other.revision && self.read().get_path(&[]) == other.read().get_path(&[])
    }
}

impl SnapshotRoots {
    fn from_guards(guards: &RootReadLocks<'_>) -> Self {
        Self {
            quotes: Arc::clone(guards.get(StateRoot::Quotes)),
            trading_status: Arc::clone(guards.get(StateRoot::TradingStatus)),
            charts: Arc::clone(guards.get(StateRoot::Charts)),
            klines: Arc::clone(guards.get(StateRoot::Klines)),
            ticks: Arc::clone(guards.get(StateRoot::Ticks)),
            trade: Arc::clone(guards.get(StateRoot::Trade)),
            query: Arc::clone(guards.get(StateRoot::Query)),
            schema: Arc::clone(guards.get(StateRoot::Schema)),
            replay: Arc::clone(guards.get(StateRoot::Replay)),
            system: Arc::clone(guards.get(StateRoot::System)),
            runtime: Arc::clone(guards.get(StateRoot::Runtime)),
            other: Arc::clone(guards.get(StateRoot::Other)),
            materialized: OnceLock::new(),
        }
    }

    pub(crate) fn get<I, S>(&self, path: I) -> Option<&Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut segments = path.into_iter();
        let Some(first) = segments.next() else {
            return Some(self.materialized());
        };

        match first.as_ref() {
            "quotes" => snapshot_partition_get(self.quotes.as_ref(), segments),
            "trading_status" => snapshot_partition_get(self.trading_status.as_ref(), segments),
            "charts" => snapshot_partition_get(self.charts.as_ref(), segments),
            "klines" => snapshot_partition_get(self.klines.as_ref(), segments),
            "ticks" => snapshot_partition_get(self.ticks.as_ref(), segments),
            "trade" => snapshot_partition_get(self.trade.as_ref(), segments),
            "query" => snapshot_partition_get(self.query.as_ref(), segments),
            "schema" => snapshot_partition_get(self.schema.as_ref(), segments),
            "replay" => snapshot_partition_get(self.replay.as_ref(), segments),
            "system" => snapshot_partition_get(self.system.as_ref(), segments),
            "runtime" => snapshot_partition_get(self.runtime.as_ref(), segments),
            key => {
                let fallback = self.other.as_object()?.get(key)?;
                snapshot_partition_get(fallback, segments)
            }
        }
    }

    fn root_keys(&self) -> Vec<usize> {
        self.roots()
            .into_iter()
            .map(|root| Arc::as_ptr(root) as usize)
            .collect()
    }

    fn root_bytes_for(&self, key: usize) -> Option<usize> {
        self.roots()
            .into_iter()
            .find(|root| Arc::as_ptr(*root) as usize == key)
            .map(|root| {
                estimated_value_bytes(root.as_ref()).saturating_add(size_of::<Arc<Value>>())
            })
    }

    fn roots(&self) -> [&Arc<Value>; 12] {
        [
            &self.quotes,
            &self.trading_status,
            &self.charts,
            &self.klines,
            &self.ticks,
            &self.trade,
            &self.query,
            &self.schema,
            &self.replay,
            &self.system,
            &self.runtime,
            &self.other,
        ]
    }

    fn materialized(&self) -> &Value {
        self.materialized.get_or_init(|| {
            let mut data = Map::new();
            insert_snapshot_root(&mut data, "quotes", self.quotes.as_ref());
            insert_snapshot_root(&mut data, "trading_status", self.trading_status.as_ref());
            insert_snapshot_root(&mut data, "charts", self.charts.as_ref());
            insert_snapshot_root(&mut data, "klines", self.klines.as_ref());
            insert_snapshot_root(&mut data, "ticks", self.ticks.as_ref());
            insert_snapshot_root(&mut data, "trade", self.trade.as_ref());
            insert_snapshot_root(&mut data, "query", self.query.as_ref());
            insert_snapshot_root(&mut data, "schema", self.schema.as_ref());
            insert_snapshot_root(&mut data, "replay", self.replay.as_ref());
            insert_snapshot_root(&mut data, "system", self.system.as_ref());
            insert_snapshot_root(&mut data, "runtime", self.runtime.as_ref());
            merge_fallback_roots(&mut data, self.other.as_ref());
            Value::Object(data)
        })
    }
}

fn insert_snapshot_root(root: &mut Map<String, Value>, key: &str, value: &Value) {
    if !is_empty_partition(value) {
        root.insert(key.to_string(), value.clone());
    }
}

fn snapshot_partition_get<I, S>(partition: &Value, path: I) -> Option<&Value>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut path = path.into_iter();
    let Some(first) = path.next() else {
        return (!is_empty_partition(partition)).then_some(partition);
    };
    get_at_path(partition, std::iter::once(first).chain(path))
}

fn estimated_value_bytes(value: &Value) -> usize {
    const MAP_ENTRY_OVERHEAD: usize = 128;
    const ARC_ALLOCATION_OVERHEAD: usize = 64;

    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => size_of::<serde_json::Number>(),
        Value::String(value) => size_of::<String>().saturating_add(value.capacity()),
        Value::Array(values) => values
            .capacity()
            .saturating_mul(size_of::<Value>())
            .saturating_add(values.iter().fold(0_usize, |total, value| {
                total.saturating_add(estimated_value_bytes(value))
            }))
            .saturating_add(ARC_ALLOCATION_OVERHEAD),
        Value::Object(values) => {
            values
                .iter()
                .fold(ARC_ALLOCATION_OVERHEAD, |total, (key, value)| {
                    total
                        .saturating_add(MAP_ENTRY_OVERHEAD)
                        .saturating_add(key.capacity())
                        .saturating_add(estimated_value_bytes(value))
                })
        }
    }
}

pub(crate) struct StatePartitionReadGuard<'a> {
    partition: RwLockReadGuard<'a, Arc<Value>>,
    // Must remain after `partition`: its Drop records only after the lock is released.
    _hold_telemetry: LiveGuardTelemetry<'a>,
}

impl<'a> StatePartitionReadGuard<'a> {
    fn new(
        partition: RwLockReadGuard<'a, Arc<Value>>,
        hold_telemetry: LiveGuardTelemetry<'a>,
    ) -> Self {
        Self {
            partition,
            _hold_telemetry: hold_telemetry,
        }
    }

    pub(crate) fn get_path(&self, path: &[&str]) -> Option<&Value> {
        get_at_path(&self.partition, path.iter().copied())
    }
}

/// Read guards acquired through the one canonical [`StateRoot`] lock order.
///
/// Cross-partition paths must use this holder rather than acquiring named
/// partitions directly. Taking a guard from the holder does not change the
/// order in which the underlying locks were acquired.
struct RootReadLocks<'a> {
    guards: [Option<RwLockReadGuard<'a, Arc<Value>>>; StateRoot::COUNT],
}

impl<'a> RootReadLocks<'a> {
    fn get(&self, root: StateRoot) -> &Arc<Value> {
        self.guards[root.index()]
            .as_ref()
            .expect("requested root must have a held read guard")
            .deref()
    }

    fn take(&mut self, root: StateRoot) -> RwLockReadGuard<'a, Arc<Value>> {
        self.guards[root.index()]
            .take()
            .expect("requested root must have a held read guard")
    }
}

/// Write guards acquired through the one canonical [`StateRoot`] lock order.
struct RootWriteLocks<'a> {
    guards: [Option<RwLockWriteGuard<'a, Arc<Value>>>; StateRoot::COUNT],
}

impl<'a> RootWriteLocks<'a> {
    fn take(&mut self, root: StateRoot) -> RwLockWriteGuard<'a, Arc<Value>> {
        self.guards[root.index()]
            .take()
            .expect("requested root must have a held write guard")
    }

    fn get_mut(&mut self, root: StateRoot) -> &mut Value {
        Arc::make_mut(
            &mut *self.guards[root.index()]
                .as_mut()
                .expect("requested root must have a held write guard"),
        )
    }
}

impl StateStore {
    pub(crate) fn new(revision: Revision) -> Self {
        Self {
            revision: AtomicU64::new(revision.get()),
            quotes: empty_partition(),
            trading_status: empty_partition(),
            charts: empty_partition(),
            klines: empty_partition(),
            ticks: empty_partition(),
            trade: empty_partition(),
            query: empty_partition(),
            schema: empty_partition(),
            replay: empty_partition(),
            system: empty_partition(),
            runtime: empty_partition(),
            other: empty_partition(),
            read_telemetry: StateReadTelemetryState::default(),
        }
    }

    pub(crate) fn revision(&self) -> Revision {
        Revision::new(self.revision.load(Ordering::SeqCst))
    }

    pub(crate) fn read_telemetry(&self) -> StateReadTelemetry {
        self.read_telemetry.snapshot()
    }

    /// Acquires every selected partition in the single process-wide root
    /// order. This is the only multi-partition read-lock acquisition point.
    fn lock_read_roots(&self, roots: StateRootSet) -> RootReadLocks<'_> {
        let mut guards: [Option<RwLockReadGuard<'_, Arc<Value>>>; StateRoot::COUNT] =
            std::array::from_fn(|_| None);
        for root in roots.iter() {
            guards[root.index()] = Some(rwlock_read(root.partition(self)));
        }
        RootReadLocks { guards }
    }

    /// Acquires every selected partition in the single process-wide root
    /// order. This is the only multi-partition write-lock acquisition point.
    fn lock_write_roots(&self, roots: StateRootSet) -> RootWriteLocks<'_> {
        let mut guards: [Option<RwLockWriteGuard<'_, Arc<Value>>>; StateRoot::COUNT] =
            std::array::from_fn(|_| None);
        for root in roots.iter() {
            guards[root.index()] = Some(rwlock_write(root.partition(self)));
        }
        RootWriteLocks { guards }
    }

    pub(crate) fn snapshot(&self) -> StateSnapshot {
        let lock_started_at = Instant::now();
        let guards = self.lock_read_roots(StateRootSet::all());
        let lock_acquired_at = Instant::now();
        let revision = self.revision();
        let snapshot = StateSnapshot::from_roots(revision, SnapshotRoots::from_guards(&guards));
        drop(guards);
        self.read_telemetry.record_snapshot(
            StateRoot::COUNT,
            lock_acquired_at.duration_since(lock_started_at),
            lock_acquired_at.elapsed(),
        );
        snapshot
    }

    pub(crate) fn read_market_state(&self) -> MarketStateReadGuard<'_> {
        let lock_started_at = Instant::now();
        let mut guards =
            self.lock_read_roots(StateRootSet::from_roots(StateRoot::MARKET.iter().copied()));
        let telemetry =
            LiveGuardTelemetry::new(&self.read_telemetry, lock_started_at.elapsed(), true);
        MarketStateReadGuard::new(
            self.revision(),
            guards.take(StateRoot::Quotes),
            guards.take(StateRoot::TradingStatus),
            guards.take(StateRoot::Charts),
            guards.take(StateRoot::Klines),
            guards.take(StateRoot::Ticks),
            (guards.take(StateRoot::Other), telemetry),
        )
    }

    pub(crate) fn read_trade_state(&self) -> TradeStateReadGuard<'_> {
        let lock_started_at = Instant::now();
        let trade = rwlock_read(&self.trade);
        let telemetry =
            LiveGuardTelemetry::new(&self.read_telemetry, lock_started_at.elapsed(), true);
        TradeStateReadGuard::new(self.revision(), trade, telemetry)
    }

    pub(crate) fn read_market_trade_state(&self) -> MarketTradeStateReadGuard<'_> {
        let lock_started_at = Instant::now();
        let mut guards = self.lock_read_roots(StateRootSet::from_roots(
            StateRoot::MARKET_TRADE.iter().copied(),
        ));
        let lock_wait = lock_started_at.elapsed();
        let revision = self.revision();
        let market_telemetry = LiveGuardTelemetry::new(&self.read_telemetry, lock_wait, true);
        let trade_telemetry = LiveGuardTelemetry::new(&self.read_telemetry, Duration::ZERO, false);
        let market = MarketStateReadGuard::new(
            revision,
            guards.take(StateRoot::Quotes),
            guards.take(StateRoot::TradingStatus),
            guards.take(StateRoot::Charts),
            guards.take(StateRoot::Klines),
            guards.take(StateRoot::Ticks),
            (guards.take(StateRoot::Other), market_telemetry),
        );
        let trade =
            TradeStateReadGuard::new(revision, guards.take(StateRoot::Trade), trade_telemetry);
        MarketTradeStateReadGuard::new(revision, market, trade)
    }

    pub(crate) fn read_partition(&self, root: &str) -> Option<StatePartitionReadGuard<'_>> {
        let root = StateRoot::from_segment(root)?;
        let lock_started_at = Instant::now();
        let partition = rwlock_read(root.partition(self));
        let telemetry =
            LiveGuardTelemetry::new(&self.read_telemetry, lock_started_at.elapsed(), true);
        Some(StatePartitionReadGuard::new(partition, telemetry))
    }

    pub(crate) fn read_runtime_state(&self) -> StatePartitionReadGuard<'_> {
        self.read_partition("runtime")
            .expect("runtime partition root should be known")
    }

    #[cfg(test)]
    pub(crate) fn apply(
        &self,
        revision: Revision,
        mutations: &[NormalizedMutation],
    ) -> Vec<AppliedChange> {
        let mut mutations = mutations.to_vec();
        self.apply_with(revision, &mut mutations, |applied, _| applied)
            .unwrap_or_default()
    }

    pub(crate) fn apply_with<T, F>(
        &self,
        revision: Revision,
        mutations: &mut [NormalizedMutation],
        on_applied: F,
    ) -> Option<T>
    where
        F: FnOnce(Vec<AppliedChange>, &[NormalizedMutation]) -> T,
    {
        let first = mutations.first()?;
        let first_root = partition_path(first).0;
        if mutations
            .iter()
            .all(|mutation| partition_path(mutation).0 == first_root)
        {
            return self.apply_single_root(revision, first_root, mutations, on_applied);
        }

        let mut roots = StateRootSet::empty();
        for mutation in mutations.iter() {
            roots.insert(partition_path(mutation).0);
        }

        let mut guards = self.lock_write_roots(roots);

        let mut applied = Vec::with_capacity(mutations.len());
        for (mutation_index, mutation) in mutations.iter_mut().enumerate() {
            let NormalizedMutation {
                path,
                object,
                fields,
                ..
            } = mutation;
            let (root, relative_path) = partition_path_segments(path.segments());
            let partition = guards.get_mut(root);
            if let Some(changed) = apply_mutation_at_partition(
                root,
                partition,
                relative_path,
                mutation_index,
                object,
                fields,
            ) {
                applied.push(changed);
            }
        }

        if !applied.is_empty() {
            self.revision.store(revision.get(), Ordering::SeqCst);
            Some(on_applied(applied, mutations))
        } else {
            None
        }
    }

    /// Applies a market-only batch while preserving the generic multi-root commit contract.
    ///
    /// Market updates commonly span quotes, charts, and serial rows. Locking their known
    /// partitions directly avoids allocating a root set and searching the guard list for each
    /// mutation, while retaining the `StateRoot` lock order used by the generic path.
    pub(crate) fn apply_market_with<T, F>(
        &self,
        revision: Revision,
        mutations: &mut [NormalizedMutation],
        on_applied: F,
    ) -> Option<T>
    where
        F: FnOnce(Vec<AppliedChange>, &[NormalizedMutation]) -> T,
    {
        let first = mutations.first()?;
        let first_root = partition_path(first).0;
        if mutations
            .iter()
            .all(|mutation| partition_path(mutation).0 == first_root)
        {
            return self.apply_single_root(revision, first_root, mutations, on_applied);
        }

        let mut roots = StateRootSet::empty();
        let mut has_quotes = false;
        let mut has_trading_status = false;
        let mut has_charts = false;
        let mut has_klines = false;
        let mut has_ticks = false;
        let mut has_other = false;
        for mutation in mutations.iter() {
            let root = partition_path(mutation).0;
            roots.insert(root);
            match root {
                StateRoot::Quotes => has_quotes = true,
                StateRoot::TradingStatus => has_trading_status = true,
                StateRoot::Charts => has_charts = true,
                StateRoot::Klines => has_klines = true,
                StateRoot::Ticks => has_ticks = true,
                StateRoot::Other => has_other = true,
                _ => return self.apply_with(revision, mutations, on_applied),
            }
        }

        // Guard extraction cannot alter the canonical StateRoot::ALL acquisition order.
        let mut guards = self.lock_write_roots(roots);
        let mut quotes = has_quotes.then(|| guards.take(StateRoot::Quotes));
        let mut trading_status = has_trading_status.then(|| guards.take(StateRoot::TradingStatus));
        let mut charts = has_charts.then(|| guards.take(StateRoot::Charts));
        let mut klines = has_klines.then(|| guards.take(StateRoot::Klines));
        let mut ticks = has_ticks.then(|| guards.take(StateRoot::Ticks));
        let mut other = has_other.then(|| guards.take(StateRoot::Other));

        let mut applied = Vec::with_capacity(mutations.len());
        for (mutation_index, mutation) in mutations.iter_mut().enumerate() {
            let NormalizedMutation {
                path,
                object,
                fields,
                ..
            } = mutation;
            let (root, relative_path) = partition_path_segments(path.segments());
            let partition: &mut Value = match root {
                StateRoot::Quotes => Arc::make_mut(
                    &mut **quotes
                        .as_mut()
                        .expect("market quote root must have a write guard"),
                ),
                StateRoot::TradingStatus => Arc::make_mut(
                    &mut **trading_status
                        .as_mut()
                        .expect("market trading_status root must have a write guard"),
                ),
                StateRoot::Charts => Arc::make_mut(
                    &mut **charts
                        .as_mut()
                        .expect("market charts root must have a write guard"),
                ),
                StateRoot::Klines => Arc::make_mut(
                    &mut **klines
                        .as_mut()
                        .expect("market klines root must have a write guard"),
                ),
                StateRoot::Ticks => Arc::make_mut(
                    &mut **ticks
                        .as_mut()
                        .expect("market ticks root must have a write guard"),
                ),
                StateRoot::Other => Arc::make_mut(
                    &mut **other
                        .as_mut()
                        .expect("market fallback root must have a write guard"),
                ),
                _ => unreachable!("non-market root must use the generic state apply path"),
            };
            if let Some(changed) = apply_mutation_at_partition(
                root,
                partition,
                relative_path,
                mutation_index,
                object,
                fields,
            ) {
                applied.push(changed);
            }
        }

        if applied.is_empty() {
            None
        } else {
            self.revision.store(revision.get(), Ordering::SeqCst);
            Some(on_applied(applied, mutations))
        }
    }

    fn apply_single_root<T, F>(
        &self,
        revision: Revision,
        root: StateRoot,
        mutations: &mut [NormalizedMutation],
        on_applied: F,
    ) -> Option<T>
    where
        F: FnOnce(Vec<AppliedChange>, &[NormalizedMutation]) -> T,
    {
        let mut partition = rwlock_write(root.partition(self));
        let partition = Arc::make_mut(&mut *partition);
        let mut applied = Vec::with_capacity(mutations.len());
        for (mutation_index, mutation) in mutations.iter_mut().enumerate() {
            let NormalizedMutation {
                path,
                object,
                fields,
                ..
            } = mutation;
            let (_, relative_path) = partition_path_segments(path.segments());
            if let Some(changed) = apply_mutation_at_partition(
                root,
                partition,
                relative_path,
                mutation_index,
                object,
                fields,
            ) {
                applied.push(changed);
            }
        }

        if applied.is_empty() {
            None
        } else {
            self.revision.store(revision.get(), Ordering::SeqCst);
            Some(on_applied(applied, mutations))
        }
    }

    #[cfg(test)]
    pub(crate) fn partition_roots_for_test(&self) -> Vec<&'static str> {
        StateRoot::ALL
            .iter()
            .copied()
            .filter_map(StateRoot::visible_root)
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn poison_partition_for_test(&self, root: &str) {
        let root = StateRoot::from_segment(root).unwrap_or(StateRoot::Other);
        let _guard = root.partition(self).write().unwrap();
        panic!("poison state partition");
    }
}

fn apply_mutation_at_partition(
    partition_root: StateRoot,
    root: &mut Value,
    path: &[PathSegment],
    mutation_index: usize,
    object: &Option<ObjectKey>,
    fields: &mut [crate::events::FieldMutation],
) -> Option<AppliedChange> {
    let mut field_indexes = Vec::with_capacity(fields.len());
    let structural_changed = if is_partition_root_delete(partition_root, path, fields) {
        apply_partition_root_delete(root, &mut field_indexes)
    } else if is_direct_scalar_path(partition_root, path) {
        apply_direct_scalar(root, path, fields, &mut field_indexes)
    } else {
        apply_mutation_at_path(
            root,
            path,
            object.as_ref(),
            fields,
            &mut field_indexes,
            partition_root == StateRoot::Runtime,
        )
    };

    if field_indexes.is_empty() && !structural_changed {
        None
    } else {
        Some(AppliedChange::new(
            partition_root.as_str(),
            mutation_index,
            field_indexes,
        ))
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StateRoot {
    Quotes,
    TradingStatus,
    Charts,
    Klines,
    Ticks,
    Trade,
    Query,
    Schema,
    Replay,
    System,
    Runtime,
    Other,
}

impl StateRoot {
    const COUNT: usize = 12;

    /// The only root acquisition order for multi-partition reads and writes.
    const ALL: [Self; Self::COUNT] = [
        Self::Quotes,
        Self::TradingStatus,
        Self::Charts,
        Self::Klines,
        Self::Ticks,
        Self::Trade,
        Self::Query,
        Self::Schema,
        Self::Replay,
        Self::System,
        Self::Runtime,
        Self::Other,
    ];

    const MARKET: &'static [Self] = &[
        Self::Quotes,
        Self::TradingStatus,
        Self::Charts,
        Self::Klines,
        Self::Ticks,
        Self::Other,
    ];

    const MARKET_TRADE: &'static [Self] = &[
        Self::Quotes,
        Self::TradingStatus,
        Self::Charts,
        Self::Klines,
        Self::Ticks,
        Self::Trade,
        Self::Other,
    ];

    const fn index(self) -> usize {
        self as usize
    }

    fn from_segment(segment: &str) -> Option<Self> {
        match segment {
            "quotes" => Some(Self::Quotes),
            "trading_status" => Some(Self::TradingStatus),
            "charts" => Some(Self::Charts),
            "klines" => Some(Self::Klines),
            "ticks" => Some(Self::Ticks),
            "trade" => Some(Self::Trade),
            "query" => Some(Self::Query),
            "schema" => Some(Self::Schema),
            "replay" => Some(Self::Replay),
            "system" => Some(Self::System),
            "runtime" => Some(Self::Runtime),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Quotes => "quotes",
            Self::TradingStatus => "trading_status",
            Self::Charts => "charts",
            Self::Klines => "klines",
            Self::Ticks => "ticks",
            Self::Trade => "trade",
            Self::Query => "query",
            Self::Schema => "schema",
            Self::Replay => "replay",
            Self::System => "system",
            Self::Runtime => "runtime",
            Self::Other => "other",
        }
    }

    #[cfg(test)]
    fn visible_root(self) -> Option<&'static str> {
        match self {
            Self::Other => None,
            root => Some(root.as_str()),
        }
    }

    fn partition(self, store: &StateStore) -> &RwLock<Arc<Value>> {
        match self {
            Self::Quotes => &store.quotes,
            Self::TradingStatus => &store.trading_status,
            Self::Charts => &store.charts,
            Self::Klines => &store.klines,
            Self::Ticks => &store.ticks,
            Self::Trade => &store.trade,
            Self::Query => &store.query,
            Self::Schema => &store.schema,
            Self::Replay => &store.replay,
            Self::System => &store.system,
            Self::Runtime => &store.runtime,
            Self::Other => &store.other,
        }
    }
}

/// Fixed-size root selection which always iterates in [`StateRoot::ALL`] order.
///
/// It intentionally has no public `Ord` surface: callers select roots, while this
/// type owns sorting/deduplication and lock acquisition order.
#[derive(Debug, Clone, Copy)]
struct StateRootSet {
    selected: [bool; StateRoot::COUNT],
}

impl StateRootSet {
    fn empty() -> Self {
        Self {
            selected: [false; StateRoot::COUNT],
        }
    }

    fn all() -> Self {
        Self::from_roots(StateRoot::ALL.iter().copied())
    }

    fn from_roots(roots: impl IntoIterator<Item = StateRoot>) -> Self {
        let mut roots_set = Self::empty();
        for root in roots {
            roots_set.insert(root);
        }
        roots_set
    }

    fn insert(&mut self, root: StateRoot) {
        self.selected[root.index()] = true;
    }

    fn iter(&self) -> impl Iterator<Item = StateRoot> + '_ {
        StateRoot::ALL
            .iter()
            .copied()
            .filter(|root| self.selected[root.index()])
    }
}

fn partition_path(mutation: &NormalizedMutation) -> (StateRoot, &[PathSegment]) {
    partition_path_segments(mutation.path.segments())
}

fn partition_path_segments(segments: &[PathSegment]) -> (StateRoot, &[PathSegment]) {
    let Some(root) = segments.first() else {
        return (StateRoot::Other, segments);
    };

    match StateRoot::from_segment(root) {
        Some(root) => (root, &segments[1..]),
        None => (StateRoot::Other, segments),
    }
}

fn empty_partition() -> RwLock<Arc<Value>> {
    RwLock::new(Arc::new(Value::Object(Map::new())))
}

fn recover_poisoned_lock<G>(result: LockResult<G>) -> G {
    match result {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn rwlock_read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    recover_poisoned_lock(lock.read())
}

fn rwlock_write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    recover_poisoned_lock(lock.write())
}

fn is_empty_partition(value: &Value) -> bool {
    value.as_object().is_some_and(Map::is_empty)
}

fn merge_fallback_roots(root: &mut Map<String, Value>, fallback: &Value) {
    let Some(entries) = fallback.as_object() else {
        return;
    };
    for (key, value) in entries {
        if !is_empty_partition(value) {
            root.insert(key.clone(), value.clone());
        }
    }
}

fn apply_mutation_at_path(
    cursor: &mut Value,
    path: &[PathSegment],
    object: Option<&ObjectKey>,
    fields: &mut [crate::events::FieldMutation],
    field_indexes: &mut Vec<usize>,
    prune_empty_parents: bool,
) -> bool {
    if path.is_empty() {
        return apply_fields(cursor, object, fields, field_indexes);
    }

    let segment = &path[0];
    let (mut changed, child_empty_after) = {
        let (child, structural_changed) = ensure_child_object(cursor, segment);
        let child_changed = apply_mutation_at_path(
            child,
            &path[1..],
            object,
            fields,
            field_indexes,
            prune_empty_parents,
        );
        (
            structural_changed || child_changed,
            prune_empty_parents && is_empty_partition(child),
        )
    };

    if child_empty_after
        && let Value::Object(map) = cursor
        && map.remove(segment).is_some()
    {
        changed = true;
    }

    changed
}

fn apply_fields(
    cursor: &mut Value,
    object: Option<&ObjectKey>,
    fields: &mut [crate::events::FieldMutation],
    field_indexes: &mut Vec<usize>,
) -> bool {
    let mut changed = false;
    if !cursor.is_object() {
        *cursor = Value::Object(Map::new());
        changed = true;
    }

    let Value::Object(map) = cursor else {
        unreachable!("state snapshot path targets must always resolve to objects");
    };

    for (field_index, field) in fields.iter_mut().enumerate() {
        let preserve_null = preserves_null_field(object, &field.field);
        if field.value.is_null() && !preserve_null {
            if map.remove(&field.field).is_none() {
                continue;
            }
        } else if let Some(existing) = map.get_mut(&field.field) {
            if *existing == field.value {
                continue;
            }
            *existing = std::mem::replace(&mut field.value, Value::Null);
        } else {
            let value = std::mem::replace(&mut field.value, Value::Null);
            map.insert(field.field.clone(), value);
        }

        changed = true;
        field_indexes.push(field_index);
    }

    changed
}

fn preserves_null_field(object: Option<&ObjectKey>, field: &str) -> bool {
    matches!(object, Some(ObjectKey::SessionReconnect)) && field == "max_attempts"
}

fn is_partition_root_delete(
    partition_root: StateRoot,
    path: &[PathSegment],
    fields: &[crate::events::FieldMutation],
) -> bool {
    !matches!(partition_root, StateRoot::Other)
        && path.is_empty()
        && matches!(fields, [field] if field.field == "value" && field.value.is_null())
}

fn apply_partition_root_delete(root: &mut Value, field_indexes: &mut Vec<usize>) -> bool {
    if is_empty_partition(root) {
        return false;
    }

    *root = Value::Object(Map::new());
    field_indexes.push(0);
    true
}

fn is_direct_scalar_path(partition_root: StateRoot, path: &[PathSegment]) -> bool {
    match partition_root {
        StateRoot::Other => {
            matches!(path, [field] if matches!(field.as_str(), "ins_list" | "mdhis_more_data"))
        }
        StateRoot::Trade => {
            matches!(path, [_account_id, field] if field == "trade_more_data")
        }
        _ => false,
    }
}

fn apply_direct_scalar(
    root: &mut Value,
    path: &[PathSegment],
    fields: &mut [crate::events::FieldMutation],
    field_indexes: &mut Vec<usize>,
) -> bool {
    let [parent_path @ .., segment] = path else {
        return false;
    };
    let [field] = fields else {
        return false;
    };
    if field.field != "value" {
        return false;
    }

    if field.value.is_null() {
        let mut cursor = root;
        for parent in parent_path {
            let Some(map) = cursor.as_object_mut() else {
                return false;
            };
            let Some(child) = map.get_mut(parent) else {
                return false;
            };
            cursor = child;
        }
        let Some(map) = cursor.as_object_mut() else {
            return false;
        };
        if !map.contains_key(segment) {
            return false;
        }
        map.remove(segment);
        field_indexes.push(0);
        return true;
    }

    let mut cursor = root;
    for parent in parent_path {
        let (child, _) = ensure_child_object(cursor, parent);
        cursor = child;
    }
    if !cursor.is_object() {
        *cursor = Value::Object(Map::new());
    }
    let Value::Object(map) = cursor else {
        unreachable!("direct scalar parent must be an object");
    };

    if map.get(segment) == Some(&field.value) {
        return false;
    }

    let value = std::mem::replace(&mut field.value, Value::Null);
    map.insert(segment.clone(), value);
    field_indexes.push(0);
    true
}

fn ensure_child_object<'a>(root: &'a mut Value, segment: &PathSegment) -> (&'a mut Value, bool) {
    let mut changed = false;
    if !root.is_object() {
        *root = Value::Object(Map::new());
        changed = true;
    }

    let Value::Object(map) = root else {
        unreachable!("state snapshot intermediate nodes must always be objects");
    };

    if !map.contains_key(segment) {
        map.insert(segment.clone(), Value::Object(Map::new()));
        changed = true;
    }
    let child = map
        .get_mut(segment)
        .expect("child was inserted or already present");
    if !child.is_object() {
        *child = Value::Object(Map::new());
        changed = true;
    }

    (child, changed)
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::{Arc, mpsc};
    use std::thread;
    use std::time::Duration;

    use serde_json::json;

    use super::*;
    use crate::{
        events::{FieldMutation, MutationSource, NormalizedMutation},
        ids::{ChartId, Symbol},
        state::StatePath,
    };

    #[test]
    fn state_store_materializes_domain_partitions_as_compatible_snapshot() {
        let store = StateStore::new(Revision::new(0));
        assert!(store.partition_roots_for_test().contains(&"quotes"));
        assert!(store.partition_roots_for_test().contains(&"trade"));

        let market = NormalizedMutation {
            path: StatePath::new(["quotes", "SHFE.au2602"]),
            object: None,
            fields: vec![FieldMutation {
                field: "last_price".to_string(),
                value: json!(620.5),
            }],
            source: MutationSource::MarketDiff,
        };
        let trade = NormalizedMutation {
            path: StatePath::new(["trade", "simnow", "accounts", "CNY"]),
            object: None,
            fields: vec![FieldMutation {
                field: "balance".to_string(),
                value: json!(1000.0),
            }],
            source: MutationSource::TradeReply,
        };

        assert_eq!(
            store.apply(Revision::new(1), &[market]).len(),
            1,
            "market mutation should apply to its partition"
        );
        assert_eq!(
            store.apply(Revision::new(2), &[trade]).len(),
            1,
            "trade mutation should apply to its partition"
        );

        let snapshot = store.snapshot();
        assert_eq!(snapshot.revision(), Revision::new(2));
        assert_eq!(
            snapshot.get(["quotes", "SHFE.au2602", "last_price"]),
            Some(&json!(620.5))
        );
        assert_eq!(
            snapshot.get(["trade", "simnow", "accounts", "CNY", "balance"]),
            Some(&json!(1000.0))
        );
    }

    #[test]
    fn snapshots_share_unchanged_partition_roots_and_preserve_old_values() {
        let store = StateStore::new(Revision::new(0));
        let first = store.snapshot();
        let mutation = NormalizedMutation {
            path: StatePath::new(["quotes", "SHFE.au2602"]),
            object: None,
            fields: vec![FieldMutation {
                field: "last_price".to_string(),
                value: json!(620.5),
            }],
            source: MutationSource::MarketDiff,
        };
        store.apply(Revision::new(1), &[mutation]);
        let second = store.snapshot();

        let SnapshotData::Roots(first_roots) = first.data.as_ref() else {
            panic!("store snapshots must retain immutable roots");
        };
        let SnapshotData::Roots(second_roots) = second.data.as_ref() else {
            panic!("store snapshots must retain immutable roots");
        };
        assert!(!Arc::ptr_eq(&first_roots.quotes, &second_roots.quotes));
        assert!(Arc::ptr_eq(&first_roots.trade, &second_roots.trade));
        assert!(first.get(["quotes", "SHFE.au2602"]).is_none());
        assert_eq!(
            second.get(["quotes", "SHFE.au2602", "last_price"]),
            Some(&json!(620.5))
        );
    }

    #[test]
    fn state_store_recovers_from_poisoned_partition_lock() {
        let store = StateStore::new(Revision::new(0));

        let panic = catch_unwind(AssertUnwindSafe(|| {
            store.poison_partition_for_test("runtime");
        }));
        assert!(panic.is_err());

        assert_eq!(store.snapshot().revision(), Revision::new(0));
    }

    #[test]
    fn state_store_preserves_unbounded_session_reconnect_attempts_as_null() {
        let store = StateStore::new(Revision::new(0));
        let mutation = NormalizedMutation {
            path: StatePath::new(["system", "session", "reconnect"]),
            object: Some(ObjectKey::SessionReconnect),
            fields: vec![FieldMutation {
                field: "max_attempts".to_string(),
                value: Value::Null,
            }],
            source: MutationSource::SessionControl,
        };

        assert_eq!(
            store.apply(Revision::new(1), &[mutation]).len(),
            1,
            "session reconnect max_attempts=null is a visible unbounded policy"
        );
        assert_eq!(
            store
                .snapshot()
                .get(["system", "session", "reconnect", "max_attempts"]),
            Some(&Value::Null)
        );
    }

    #[test]
    fn state_store_still_treats_other_null_fields_as_deletes() {
        let store = StateStore::new(Revision::new(0));
        let insert = NormalizedMutation {
            path: StatePath::new(["system", "session", "reconnect"]),
            object: Some(ObjectKey::SessionReconnect),
            fields: vec![FieldMutation {
                field: "detail".to_string(),
                value: json!({ "reason": "test" }),
            }],
            source: MutationSource::SessionControl,
        };
        let delete = NormalizedMutation {
            path: StatePath::new(["system", "session", "reconnect"]),
            object: Some(ObjectKey::SessionReconnect),
            fields: vec![FieldMutation {
                field: "detail".to_string(),
                value: Value::Null,
            }],
            source: MutationSource::SessionControl,
        };

        assert_eq!(store.apply(Revision::new(1), &[insert]).len(), 1);
        assert_eq!(store.apply(Revision::new(2), &[delete]).len(), 1);
        assert_eq!(
            store
                .snapshot()
                .get(["system", "session", "reconnect", "detail"]),
            None
        );
    }

    #[test]
    fn state_store_updates_and_evicts_rolling_tick_rows() {
        let store = StateStore::new(Revision::new(0));
        let tick_path = StatePath::new(["ticks", "SHFE.au2606", "data", "7"]);
        let tick_data_path = StatePath::new(["ticks", "SHFE.au2606", "data"]);

        let insert = NormalizedMutation {
            path: tick_path.clone(),
            object: Some(ObjectKey::Tick {
                symbol: Symbol::new("SHFE.au2606"),
                tick_id: 7,
            }),
            fields: vec![FieldMutation {
                field: "last_price".to_string(),
                value: json!(610.0),
            }],
            source: MutationSource::MarketDiff,
        };
        let update = NormalizedMutation {
            path: tick_path,
            object: Some(ObjectKey::Tick {
                symbol: Symbol::new("SHFE.au2606"),
                tick_id: 7,
            }),
            fields: vec![FieldMutation {
                field: "last_price".to_string(),
                value: json!(611.0),
            }],
            source: MutationSource::MarketDiff,
        };
        let evict = NormalizedMutation {
            path: tick_data_path,
            object: None,
            fields: vec![FieldMutation {
                field: "7".to_string(),
                value: Value::Null,
            }],
            source: MutationSource::MarketDiff,
        };

        assert_eq!(store.apply(Revision::new(1), &[insert]).len(), 1);
        assert_eq!(store.apply(Revision::new(2), &[update]).len(), 1);
        assert_eq!(
            store
                .snapshot()
                .get(["ticks", "SHFE.au2606", "data", "7", "last_price"]),
            Some(&json!(611.0))
        );
        assert_eq!(store.apply(Revision::new(3), &[evict]).len(), 1);
        assert_eq!(
            store.snapshot().get(["ticks", "SHFE.au2606", "data", "7"]),
            None
        );
    }

    #[test]
    fn state_root_set_enforces_canonical_multi_partition_lock_order() {
        let reversed = StateRootSet::from_roots([StateRoot::Other, StateRoot::Trade]);
        assert_eq!(
            reversed.iter().collect::<Vec<_>>(),
            vec![StateRoot::Trade, StateRoot::Other]
        );

        let market = StateRootSet::from_roots(StateRoot::MARKET.iter().copied());
        assert_eq!(
            market.iter().collect::<Vec<_>>(),
            StateRoot::MARKET.to_vec()
        );

        let market_trade = StateRootSet::from_roots(StateRoot::MARKET_TRADE.iter().copied());
        assert_eq!(
            market_trade.iter().collect::<Vec<_>>(),
            StateRoot::MARKET_TRADE.to_vec()
        );
    }

    #[test]
    fn combined_reader_and_trade_other_writer_complete_without_lock_order_deadlock() {
        let store = Arc::new(StateStore::new(Revision::new(0)));
        let (reader_ready_tx, reader_ready_rx) = mpsc::channel();
        let (release_reader_tx, release_reader_rx) = mpsc::channel();
        let (writer_started_tx, writer_started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        let reader_store = Arc::clone(&store);
        let reader_done_tx = done_tx.clone();
        let reader = thread::spawn(move || {
            let guard = reader_store.read_market_trade_state();
            reader_ready_tx.send(()).unwrap();
            release_reader_rx.recv().unwrap();
            drop(guard);
            let _ = reader_done_tx.send("reader");
        });

        reader_ready_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("combined reader should acquire its canonical lock set");

        let writer_store = Arc::clone(&store);
        let writer_done_tx = done_tx.clone();
        let writer = thread::spawn(move || {
            writer_started_tx.send(()).unwrap();
            let mut mutations = vec![
                NormalizedMutation {
                    path: StatePath::new(["trade", "TQSIM", "CNY"]),
                    object: None,
                    fields: vec![FieldMutation {
                        field: "balance".to_string(),
                        value: json!(1_000_000.0),
                    }],
                    source: MutationSource::TradeReply,
                },
                NormalizedMutation {
                    path: StatePath::new(["custom", "lock-order"]),
                    object: None,
                    fields: vec![FieldMutation {
                        field: "value".to_string(),
                        value: json!(true),
                    }],
                    source: MutationSource::SessionControl,
                },
            ];
            writer_store
                .apply_with(Revision::new(1), &mut mutations, |applied, _| applied)
                .expect("trade and other roots should be written together");
            let _ = writer_done_tx.send("writer");
        });

        writer_started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("writer should start while the combined reader is live");
        release_reader_tx.send(()).unwrap();

        let mut completed = [
            done_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("concurrent reader and writer should not deadlock"),
            done_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("concurrent reader and writer should not deadlock"),
        ];
        completed.sort_unstable();
        assert_eq!(completed, ["reader", "writer"]);
        reader.join().unwrap();
        writer.join().unwrap();
    }

    #[test]
    fn state_root_table_is_complete_unique_and_indexed() {
        assert_eq!(StateRoot::ALL.len(), StateRoot::COUNT);
        for (index, root) in StateRoot::ALL.iter().copied().enumerate() {
            assert_eq!(root.index(), index);
            assert!(
                !StateRoot::ALL[..index].contains(&root),
                "each state root must have one canonical acquisition position"
            );
        }
    }

    #[test]
    fn generic_trade_other_commit_applies_all_partitions() {
        let store = StateStore::new(Revision::new(0));
        let mut mutations = vec![
            NormalizedMutation {
                path: StatePath::new(["trade", "TQSIM", "accounts", "CNY"]),
                object: None,
                fields: vec![FieldMutation {
                    field: "balance".to_string(),
                    value: json!(1_000_000.0),
                }],
                source: MutationSource::TradeReply,
            },
            NormalizedMutation {
                path: StatePath::new(["custom", "settings"]),
                object: None,
                fields: vec![FieldMutation {
                    field: "enabled".to_string(),
                    value: json!(true),
                }],
                source: MutationSource::SessionControl,
            },
        ];
        let applied = store
            .apply_with(Revision::new(1), &mut mutations, |applied, _| applied)
            .expect("multi-root mutation must publish applied changes");

        assert_eq!(applied.len(), 2);
        assert_eq!(store.revision(), Revision::new(1));
        let snapshot = store.snapshot();
        assert!(
            snapshot
                .get(["trade", "TQSIM", "accounts", "CNY", "balance"])
                .is_some(),
            "trade mutation must be committed"
        );
        assert!(
            snapshot.get(["custom", "settings", "enabled"]).is_some(),
            "fallback-root mutation must be committed"
        );
    }

    #[test]
    fn multi_root_write_recovers_from_a_poisoned_partition() {
        let store = StateStore::new(Revision::new(0));
        let panic = catch_unwind(AssertUnwindSafe(|| {
            store.poison_partition_for_test("trade");
        }));
        assert!(panic.is_err());

        let mut mutations = vec![
            NormalizedMutation {
                path: StatePath::new(["trade", "TQSIM", "accounts", "CNY"]),
                object: None,
                fields: vec![FieldMutation {
                    field: "balance".to_string(),
                    value: json!(1_000_000.0),
                }],
                source: MutationSource::TradeReply,
            },
            NormalizedMutation {
                path: StatePath::new(["custom", "settings"]),
                object: None,
                fields: vec![FieldMutation {
                    field: "enabled".to_string(),
                    value: json!(true),
                }],
                source: MutationSource::SessionControl,
            },
        ];
        assert!(
            store
                .apply_with(Revision::new(1), &mut mutations, |applied, _| applied)
                .is_some(),
            "multi-root write must recover poisoned locks through the shared helper"
        );
    }

    #[test]
    fn multi_root_commit_holds_state_locks_until_publication_callback_returns() {
        let store = std::sync::Arc::new(StateStore::new(Revision::new(0)));
        let writer_store = std::sync::Arc::clone(&store);
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let writer = std::thread::spawn(move || {
            let mut mutations = vec![
                NormalizedMutation {
                    path: StatePath::new(["trade", "TQSIM", "accounts", "CNY"]),
                    object: None,
                    fields: vec![FieldMutation {
                        field: "balance".to_string(),
                        value: json!(1_000_000.0),
                    }],
                    source: MutationSource::TradeReply,
                },
                NormalizedMutation {
                    path: StatePath::new(["custom", "settings"]),
                    object: None,
                    fields: vec![FieldMutation {
                        field: "enabled".to_string(),
                        value: json!(true),
                    }],
                    source: MutationSource::SessionControl,
                },
            ];
            writer_store
                .apply_with(Revision::new(1), &mut mutations, |applied, _| {
                    entered_tx.send(()).expect("test receiver must be alive");
                    release_rx
                        .recv()
                        .expect("test sender must release callback");
                    applied
                })
                .expect("multi-root mutation must publish applied changes");
        });
        entered_rx
            .recv()
            .expect("callback must run with locks held");

        let snapshot_store = std::sync::Arc::clone(&store);
        let (snapshot_tx, snapshot_rx) = std::sync::mpsc::sync_channel(1);
        let reader = std::thread::spawn(move || {
            snapshot_tx
                .send(snapshot_store.snapshot().revision())
                .expect("test receiver must be alive");
        });
        assert!(matches!(
            snapshot_rx.recv_timeout(std::time::Duration::from_millis(50)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));

        release_tx.send(()).expect("writer must be waiting");
        assert_eq!(
            snapshot_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .expect("snapshot must proceed after publication callback"),
            Revision::new(1)
        );
        writer.join().expect("writer must complete");
        reader.join().expect("reader must complete");
    }

    #[test]
    fn market_multi_root_fast_path_matches_generic_apply() {
        let generic = StateStore::new(Revision::new(0));
        let optimized = StateStore::new(Revision::new(0));
        let mut generic_mutations = multi_root_market_mutations();
        let mut optimized_mutations = generic_mutations.clone();

        let generic_applied = generic
            .apply_with(Revision::new(1), &mut generic_mutations, |applied, _| {
                applied
            })
            .expect("generic market batch should apply");
        let optimized_applied = optimized
            .apply_market_with(Revision::new(1), &mut optimized_mutations, |applied, _| {
                applied
            })
            .expect("optimized market batch should apply");

        assert_eq!(optimized_applied, generic_applied);
        assert_eq!(optimized.revision(), generic.revision());
        assert_eq!(optimized.snapshot(), generic.snapshot());
    }

    #[test]
    fn market_multi_root_fast_path_falls_back_for_non_market_partition() {
        let generic = StateStore::new(Revision::new(0));
        let optimized = StateStore::new(Revision::new(0));
        let mut generic_mutations = vec![
            NormalizedMutation {
                path: StatePath::new(["quotes", "SHFE.au2606"]),
                object: None,
                fields: vec![FieldMutation {
                    field: "last_price".to_string(),
                    value: json!(610.0),
                }],
                source: MutationSource::MarketDiff,
            },
            NormalizedMutation {
                path: StatePath::new(["trade", "TQSIM", "accounts", "CNY"]),
                object: None,
                fields: vec![FieldMutation {
                    field: "balance".to_string(),
                    value: json!(1_000_000.0),
                }],
                source: MutationSource::MarketDiff,
            },
        ];
        let mut optimized_mutations = generic_mutations.clone();

        let generic_applied = generic
            .apply_with(Revision::new(1), &mut generic_mutations, |applied, _| {
                applied
            })
            .expect("generic mixed batch should apply");
        let optimized_applied = optimized
            .apply_market_with(Revision::new(1), &mut optimized_mutations, |applied, _| {
                applied
            })
            .expect("market fast path should fall back to generic apply");

        assert_eq!(optimized_applied, generic_applied);
        assert_eq!(optimized.snapshot(), generic.snapshot());
    }

    fn multi_root_market_mutations() -> Vec<NormalizedMutation> {
        vec![
            NormalizedMutation {
                path: StatePath::new(["charts", "tick-chart"]),
                object: Some(ObjectKey::Chart {
                    chart_id: ChartId::new("tick-chart"),
                }),
                fields: vec![FieldMutation {
                    field: "right_id".to_string(),
                    value: json!(7),
                }],
                source: MutationSource::MarketDiff,
            },
            NormalizedMutation {
                path: StatePath::new(["quotes", "SHFE.au2606"]),
                object: Some(ObjectKey::Quote {
                    symbol: Symbol::new("SHFE.au2606"),
                }),
                fields: vec![FieldMutation {
                    field: "last_price".to_string(),
                    value: json!(610.0),
                }],
                source: MutationSource::MarketDiff,
            },
            NormalizedMutation {
                path: StatePath::new(["ticks", "SHFE.au2606", "data", "7"]),
                object: Some(ObjectKey::Tick {
                    symbol: Symbol::new("SHFE.au2606"),
                    tick_id: 7,
                }),
                fields: vec![FieldMutation {
                    field: "last_price".to_string(),
                    value: json!(610.0),
                }],
                source: MutationSource::MarketDiff,
            },
            NormalizedMutation {
                path: StatePath::new(["trading_status", "SHFE.au2606"]),
                object: Some(ObjectKey::TradingStatus {
                    symbol: Symbol::new("SHFE.au2606"),
                }),
                fields: vec![FieldMutation {
                    field: "tradeable".to_string(),
                    value: json!(true),
                }],
                source: MutationSource::MarketDiff,
            },
            NormalizedMutation {
                path: StatePath::new(["symbols", "SHFE.au2606"]),
                object: None,
                fields: vec![FieldMutation {
                    field: "instrument_name".to_string(),
                    value: json!("gold"),
                }],
                source: MutationSource::MarketDiff,
            },
        ]
    }
}
