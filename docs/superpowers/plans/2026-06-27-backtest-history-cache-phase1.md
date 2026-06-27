# Backtest History Cache Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make backtest persistent data reuse `HistorySeriesCache` as the single durable cache abstraction, with a replaceable storage backend and a tick-only backtest facade.

**Architecture:** `HistorySeriesCache` becomes a stable facade over `Arc<dyn HistorySeriesStore>`. The current binary segment implementation becomes `BinaryHistorySeriesStore`, while `BacktestTickCache` becomes a thin tick-only semantic wrapper over `HistorySeriesCache` rather than a separate file format. The `tqsdk` facade starts the breaking API migration by making `backtest(start, end)` mean local persistent-cache backtest and by moving official server-side backtest to `server_backtest(start, end)`.

**Tech Stack:** Rust 2024, Cargo workspace, `tqsdk-data`, `tqsdk-task`, `tqsdk`, existing `HistorySeriesCache` binary segment format, existing `StrategyBacktest` replay engine.

---

## Scope Check

This plan implements the foundation required by the approved design:

- `HistorySeriesCache` is the only durable history cache abstraction.
- The storage format can be replaced through a `HistorySeriesStore` trait.
- Backtest cache behavior is exposed through `BacktestTickCache`, which wraps `HistorySeriesCache`.
- The experimental JSONL `TickReplayCache` path is removed from public API and examples.
- The `tqsdk` facade gets the breaking API skeleton for `backtest`, `server_backtest`, and `replay_backtest`.

The following design requirements get their own follow-up plans after this plan lands:

- Extract relay universe selectors into `tqsdk-data`.
- Implement full-universe remote-on-miss preparation.
- Replace `ReplayMarketSource { Vec<_> }` with heap-merged streaming replay.
- Derive backtest kline serials incrementally from ticks.

## File Structure

- Modify `crates/tqsdk-data/src/history_series_cache.rs`
  - Keep the public `HistorySeriesCache` methods.
  - Change the internals to delegate to a store backend.
  - Keep `HistorySeriesCache::open(...)` backed by the binary store.

- Create `crates/tqsdk-data/src/history_series_cache/store.rs`
  - Define `HistorySeriesStore`, request/report types, row enums, and reader traits.

- Create `crates/tqsdk-data/src/history_series_cache/binary_store.rs`
  - Move current binary segment internals behind `BinaryHistorySeriesStore`.
  - Keep existing filename and row encoding behavior for compatibility with existing tests.

- Create `crates/tqsdk-data/src/backtest_tick_cache.rs`
  - Define `BacktestTickCache`, `BacktestCachePolicy`, `BacktestTickCoverage`, and `BacktestTickCacheWriteReport`.
  - Implement tick-only coverage, writes, and reads by delegating to `HistorySeriesCache`.

- Modify `crates/tqsdk-data/src/lib.rs`
  - Export the new shared cache abstractions and backtest tick facade.
  - Remove `TickReplayCache` exports.

- Delete `crates/tqsdk-data/src/tick_replay_cache.rs`
  - Remove the JSONL cache implementation.

- Modify `crates/tqsdk-data/tests/history_series_cache.rs`
  - Add tests for backend identity, store injection, and `BacktestTickCache` reuse.

- Modify `crates/tqsdk/src/lib.rs`
  - Change `TqBuilder::backtest(start, end)` to return a local `BacktestBuilder`.
  - Add `TqBuilder::server_backtest(start, end)`.
  - Add `TqBuilder::replay_backtest(source)`.
  - Remove public `local_backtest_*` helpers.

- Modify `crates/tqsdk/src/local_backtest.rs`
  - Keep internal replay conversion helpers needed by `replay_backtest`.
  - Remove helpers that only supported removed public `local_backtest_*` methods.

- Modify `crates/tqsdk/Cargo.toml`
  - Replace removed local-backtest contract examples with new breaking API contract examples.

- Create `crates/tqsdk/examples/api_contract_s43_facade_backtest_history_cache.rs`
  - Demonstrate cache-only local backtest through `HistorySeriesCache` and `BacktestTickCache`.

- Modify docs touched by this API:
  - `README.md`
  - `crates/tqsdk/README.md`
  - `crates/tqsdk-data/README.md`
  - `docs/architecture/api-data.md`
  - `docs/architecture/crate-boundaries.md`
  - `docs/architecture/validation.md`

## Task 1: Add Store Abstraction Tests

**Files:**
- Modify: `crates/tqsdk-data/tests/history_series_cache.rs`

- [ ] **Step 1: Add tests that describe the new cache abstraction**

Append these tests near the existing builder/cache tests:

```rust
#[test]
fn history_cache_open_uses_default_binary_store() {
    let dir = temp_dir("history-cache-default-store");
    let cache = HistorySeriesCache::open(&dir).unwrap();

    assert_eq!(cache.root_dir(), dir.as_path());
    assert_eq!(cache.format_id(), "tqsdk.binary-series.v1");
    assert_eq!(cache.schema_version(), HISTORY_SERIES_CACHE_SCHEMA_VERSION);
}

#[test]
fn backtest_tick_cache_reuses_history_series_cache_storage() {
    let dir = temp_dir("backtest-tick-cache-reuses-history-cache");
    let history = HistorySeriesCache::open(&dir).unwrap();
    let backtest_cache = BacktestTickCache::new(history.clone());

    let report = backtest_cache
        .store_ticks(
            "SHFE.rb2601",
            1_000,
            3_000,
            vec![
                tick(2, 2_000, 102.0),
                tick(1, 1_000, 101.0),
                tick(2, 2_000, 102.0),
            ],
        )
        .unwrap();

    assert_eq!(report.symbol, "SHFE.rb2601");
    assert_eq!(report.rows, 2);
    assert_eq!(report.range_start_ns, 1_000);
    assert_eq!(report.range_end_ns, 3_000);

    let rows = history
        .read_tick_window("SHFE.rb2601", 1_000, 3_000)
        .unwrap();
    assert_eq!(rows.iter().map(|row| row.id).collect::<Vec<_>>(), vec![1, 2]);
    assert!(dir.join("SHFE.rb2601.0.1.3").exists());
}

#[test]
fn backtest_tick_cache_reports_missing_ranges_from_history_cache() {
    let dir = temp_dir("backtest-tick-cache-missing-ranges");
    let history = HistorySeriesCache::open(&dir).unwrap();
    let backtest_cache = BacktestTickCache::new(history);

    backtest_cache
        .store_ticks("SHFE.rb2601", 1_000, 2_000, vec![tick(1, 1_000, 101.0)])
        .unwrap();

    let coverage = backtest_cache
        .coverage("SHFE.rb2601", 1_000, 4_000)
        .unwrap();

    assert_eq!(coverage.cached_ranges, vec![(1_000, 2_000)]);
    assert_eq!(coverage.missing_ranges, vec![(2_000, 4_000)]);
    assert!(!coverage.is_complete());
}
```

- [ ] **Step 2: Import the new symbols in the test file**

Change the existing `use tqsdk_data::{ ... }` block to include these names:

```rust
use tqsdk_data::{
    BacktestTickCache, DataClientBuilder, DataError, HISTORY_SERIES_CACHE_SCHEMA_VERSION,
    HistorySeriesCache, HistorySeriesCacheFileStatus, KlineDataSeriesRequest,
};
```

- [ ] **Step 3: Run the focused tests and confirm they fail**

Run:

```bash
rtk cargo test -p tqsdk-data history_cache_open_uses_default_binary_store
rtk cargo test -p tqsdk-data backtest_tick_cache_reuses_history_series_cache_storage
rtk cargo test -p tqsdk-data backtest_tick_cache_reports_missing_ranges_from_history_cache
```

Expected:

```text
error[E0432]: unresolved import `tqsdk_data::BacktestTickCache`
error[E0599]: no method named `format_id` found for struct `HistorySeriesCache`
```

## Task 2: Add Generic Cache Type Definitions

**Files:**
- Create: `crates/tqsdk-data/src/history_series_cache/store.rs`
- Modify: `crates/tqsdk-data/src/history_series_cache.rs`
- Modify: `crates/tqsdk-data/src/lib.rs`

- [ ] **Step 1: Create `store.rs` with the backend contracts**

Create `crates/tqsdk-data/src/history_series_cache/store.rs`:

```rust
use std::path::{Path, PathBuf};

use tqsdk_core::{Kline, Tick};

use crate::Result;

use super::{HistorySeriesCacheMaintenanceReport, HistorySeriesCacheScanReport};

pub const BINARY_HISTORY_SERIES_FORMAT_ID: &str = "tqsdk.binary-series.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistorySeriesKind {
    Kline { duration_ns: i64 },
    Tick,
}

impl HistorySeriesKind {
    #[must_use]
    pub fn duration_ns(self) -> i64 {
        match self {
            Self::Kline { duration_ns } => duration_ns,
            Self::Tick => 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistorySeriesCoverageRequest {
    pub symbol: String,
    pub kind: HistorySeriesKind,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistorySeriesCoverageReport {
    pub symbol: String,
    pub kind: HistorySeriesKind,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
    pub cached_ranges: Vec<(i64, i64)>,
    pub missing_ranges: Vec<(i64, i64)>,
}

impl HistorySeriesCoverageReport {
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.missing_ranges.is_empty()
    }
}

#[derive(Debug, Clone)]
pub enum HistorySeriesWriteRows<'a> {
    Klines(&'a [Kline]),
    Ticks(&'a [Tick]),
}

#[derive(Debug, Clone)]
pub struct HistorySeriesWriteSegment<'a> {
    pub symbol: &'a str,
    pub kind: HistorySeriesKind,
    pub rows: HistorySeriesWriteRows<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistorySeriesSegmentReport {
    pub path: PathBuf,
    pub symbol: String,
    pub kind: HistorySeriesKind,
    pub id_range: Option<(i64, i64)>,
    pub range_start_ns: Option<i64>,
    pub range_end_ns: Option<i64>,
    pub rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistorySeriesReadRequest {
    pub symbol: String,
    pub kind: HistorySeriesKind,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
}

pub enum HistorySeriesRow {
    Kline(Kline),
    Tick(Tick),
}

pub trait HistorySeriesReader: Send {
    fn next_row(&mut self) -> Result<Option<HistorySeriesRow>>;
}

pub trait HistorySeriesStore: Send + Sync {
    fn format_id(&self) -> &'static str;
    fn schema_version(&self) -> u32;
    fn root_dir(&self) -> &Path;
    fn uses_mmap_backend(&self) -> bool;
    fn scan(&self) -> Result<HistorySeriesCacheScanReport>;
    fn enforce_limits(
        &self,
        max_bytes: Option<u64>,
        retention_days: Option<u64>,
    ) -> Result<HistorySeriesCacheMaintenanceReport>;
    fn coverage(&self, request: HistorySeriesCoverageRequest)
        -> Result<HistorySeriesCoverageReport>;
    fn write_segment(&self, segment: HistorySeriesWriteSegment<'_>)
        -> Result<HistorySeriesSegmentReport>;
    fn open_reader(&self, request: HistorySeriesReadRequest)
        -> Result<Box<dyn HistorySeriesReader>>;
}
```

- [ ] **Step 2: Add the module declaration**

In `crates/tqsdk-data/src/history_series_cache.rs`, add this module declaration near the existing submodules:

```rust
mod store;
```

- [ ] **Step 3: Re-export the new abstraction types**

In `crates/tqsdk-data/src/history_series_cache.rs`, add:

```rust
pub use store::{
    BINARY_HISTORY_SERIES_FORMAT_ID, HistorySeriesCoverageReport,
    HistorySeriesCoverageRequest, HistorySeriesKind, HistorySeriesReadRequest,
    HistorySeriesReader, HistorySeriesRow, HistorySeriesSegmentReport, HistorySeriesStore,
    HistorySeriesWriteRows, HistorySeriesWriteSegment,
};
```

In `crates/tqsdk-data/src/lib.rs`, extend the `pub use history_series_cache::{ ... }` block with:

```rust
BINARY_HISTORY_SERIES_FORMAT_ID, HistorySeriesCoverageReport,
HistorySeriesCoverageRequest, HistorySeriesKind, HistorySeriesReadRequest,
HistorySeriesReader, HistorySeriesRow, HistorySeriesSegmentReport, HistorySeriesStore,
HistorySeriesWriteRows, HistorySeriesWriteSegment,
```

- [ ] **Step 4: Run check and confirm the new type definitions compile**

Run:

```bash
rtk cargo check -p tqsdk-data
```

Expected:

```text
exit code 0
```

## Task 3: Move Binary Behavior Behind `BinaryHistorySeriesStore`

**Files:**
- Create: `crates/tqsdk-data/src/history_series_cache/binary_store.rs`
- Modify: `crates/tqsdk-data/src/history_series_cache.rs`

- [ ] **Step 1: Create the binary store shell**

Create `crates/tqsdk-data/src/history_series_cache/binary_store.rs`:

```rust
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{
    BINARY_HISTORY_SERIES_FORMAT_ID, HISTORY_SERIES_CACHE_SCHEMA_VERSION,
    HistorySeriesCacheFileReport, HistorySeriesCacheMaintenanceReport,
    HistorySeriesCacheScanReport, HistorySeriesCoverageReport, HistorySeriesCoverageRequest,
    HistorySeriesKind, HistorySeriesReadRequest, HistorySeriesReader, HistorySeriesRow,
    HistorySeriesSegmentReport, HistorySeriesStore, HistorySeriesWriteRows,
    HistorySeriesWriteSegment,
};
use crate::Result;

#[derive(Clone)]
pub(super) struct BinaryHistorySeriesStore {
    inner: Arc<super::HistorySeriesCacheInner>,
}

impl BinaryHistorySeriesStore {
    pub(super) fn new(root_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&root_dir)?;
        Ok(Self {
            inner: Arc::new(super::HistorySeriesCacheInner::new(root_dir)),
        })
    }

    pub(super) fn inner(&self) -> &Arc<super::HistorySeriesCacheInner> {
        &self.inner
    }
}
```

- [ ] **Step 2: Add a constructor to the existing inner state**

In `crates/tqsdk-data/src/history_series_cache.rs`, add this impl block below `struct HistorySeriesCacheInner`:

```rust
impl HistorySeriesCacheInner {
    fn new(root_dir: PathBuf) -> Self {
        Self {
            root_dir,
            global_gate: RwLock::new(()),
            active_series: Mutex::new(HashSet::new()),
            active_series_changed: Condvar::new(),
            range_index: Mutex::new(RangeIndex::default()),
        }
    }
}
```

- [ ] **Step 3: Add the binary store module and delegate methods**

In `crates/tqsdk-data/src/history_series_cache.rs`, add:

```rust
mod binary_store;
```

Change `HistorySeriesCache` to store the trait object:

```rust
#[derive(Clone)]
pub struct HistorySeriesCache {
    store: Arc<dyn HistorySeriesStore>,
}
```

Add a private constructor:

```rust
impl HistorySeriesCache {
    #[must_use]
    pub fn from_store(store: Arc<dyn HistorySeriesStore>) -> Self {
        Self { store }
    }
}
```

Change `HistorySeriesCache::open(...)` to:

```rust
pub fn open(root_dir: impl AsRef<Path>) -> Result<Self> {
    let root_dir = canonical_or_original(root_dir.as_ref());
    let store = binary_store::BinaryHistorySeriesStore::new(root_dir)?;
    Ok(Self::from_store(Arc::new(store)))
}
```

Add these delegating methods:

```rust
#[must_use]
pub fn format_id(&self) -> &'static str {
    self.store.format_id()
}

#[must_use]
pub fn schema_version(&self) -> u32 {
    self.store.schema_version()
}

#[must_use]
pub fn root_dir(&self) -> &Path {
    self.store.root_dir()
}

#[must_use]
pub fn uses_mmap_backend(&self) -> bool {
    self.store.uses_mmap_backend()
}
```

- [ ] **Step 4: Implement `HistorySeriesStore` for the binary backend**

In `binary_store.rs`, add:

```rust
impl HistorySeriesStore for BinaryHistorySeriesStore {
    fn format_id(&self) -> &'static str {
        BINARY_HISTORY_SERIES_FORMAT_ID
    }

    fn schema_version(&self) -> u32 {
        HISTORY_SERIES_CACHE_SCHEMA_VERSION
    }

    fn root_dir(&self) -> &Path {
        self.inner.root_dir.as_path()
    }

    fn uses_mmap_backend(&self) -> bool {
        true
    }

    fn scan(&self) -> Result<HistorySeriesCacheScanReport> {
        super::scan_with_inner(&self.inner)
    }

    fn enforce_limits(
        &self,
        max_bytes: Option<u64>,
        retention_days: Option<u64>,
    ) -> Result<HistorySeriesCacheMaintenanceReport> {
        super::enforce_limits_with_inner(&self.inner, max_bytes, retention_days)
    }

    fn coverage(
        &self,
        request: HistorySeriesCoverageRequest,
    ) -> Result<HistorySeriesCoverageReport> {
        super::coverage_with_inner(&self.inner, request)
    }

    fn write_segment(
        &self,
        segment: HistorySeriesWriteSegment<'_>,
    ) -> Result<HistorySeriesSegmentReport> {
        super::write_segment_with_inner(&self.inner, segment)
    }

    fn open_reader(
        &self,
        request: HistorySeriesReadRequest,
    ) -> Result<Box<dyn HistorySeriesReader>> {
        super::open_reader_with_inner(&self.inner, request)
    }
}
```

- [ ] **Step 5: Extract helper functions from existing methods**

In `history_series_cache.rs`, extract each current `self.inner` method body into a helper that accepts `&Arc<HistorySeriesCacheInner>`.

Use this shape:

```rust
fn coverage_with_inner(
    inner: &Arc<HistorySeriesCacheInner>,
    request: HistorySeriesCoverageRequest,
) -> Result<HistorySeriesCoverageReport> {
    let cache = HistorySeriesCache {
        store: Arc::new(binary_store::BinaryHistorySeriesStore::from_inner(inner.clone())),
    };
    let duration_ns = request.kind.duration_ns();
    let missing_ranges = if duration_ns == 0 {
        cache.missing_tick_datetime_ranges(
            request.symbol.as_str(),
            request.range_start_ns,
            request.range_end_ns,
        )?
    } else {
        cache.missing_kline_datetime_ranges(
            request.symbol.as_str(),
            duration_ns,
            request.range_start_ns,
            request.range_end_ns,
        )?
    };
    let cached_ranges = invert_missing_ranges(
        (request.range_start_ns, request.range_end_ns),
        &missing_ranges,
    );
    Ok(HistorySeriesCoverageReport {
        symbol: request.symbol,
        kind: request.kind,
        range_start_ns: request.range_start_ns,
        range_end_ns: request.range_end_ns,
        cached_ranges,
        missing_ranges,
    })
}
```

Add `BinaryHistorySeriesStore::from_inner(...)`:

```rust
pub(super) fn from_inner(inner: Arc<super::HistorySeriesCacheInner>) -> Self {
    Self { inner }
}
```

Add `invert_missing_ranges(...)` near the existing range helpers:

```rust
fn invert_missing_ranges(request: (i64, i64), missing_ranges: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let mut cached = Vec::new();
    let mut cursor = request.0;
    for &(start, end) in missing_ranges {
        if cursor < start {
            cached.push((cursor, start));
        }
        cursor = cursor.max(end);
    }
    if cursor < request.1 {
        cached.push((cursor, request.1));
    }
    cached
}
```

- [ ] **Step 6: Run existing history cache tests**

Run:

```bash
rtk cargo test -p tqsdk-data history_series_cache
```

Expected:

```text
test result: ok
```

- [ ] **Step 7: Commit the store abstraction**

Run:

```bash
rtk git add crates/tqsdk-data/src/history_series_cache.rs crates/tqsdk-data/src/history_series_cache/store.rs crates/tqsdk-data/src/history_series_cache/binary_store.rs crates/tqsdk-data/src/lib.rs crates/tqsdk-data/tests/history_series_cache.rs
rtk git commit -m "refactor(data): abstract history series cache storage"
```

## Task 4: Implement `BacktestTickCache` as a Facade

**Files:**
- Create: `crates/tqsdk-data/src/backtest_tick_cache.rs`
- Modify: `crates/tqsdk-data/src/lib.rs`
- Delete: `crates/tqsdk-data/src/tick_replay_cache.rs`

- [ ] **Step 1: Add `backtest_tick_cache.rs`**

Create `crates/tqsdk-data/src/backtest_tick_cache.rs`:

```rust
use std::path::{Path, PathBuf};

use tqsdk_core::Tick;

use crate::{
    HistorySeriesCache, HistorySeriesCoverageRequest, HistorySeriesKind, Result,
    TickDataSeries, TickDataSeriesRequest,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktestCachePolicy {
    CacheOnly,
    RemoteOnMiss,
    RefreshMissing,
    RefreshAll,
}

impl Default for BacktestCachePolicy {
    fn default() -> Self {
        Self::RemoteOnMiss
    }
}

#[derive(Clone)]
pub struct BacktestTickCache {
    history: HistorySeriesCache,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestTickCoverage {
    pub cache_dir: PathBuf,
    pub symbol: String,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
    pub cached_ranges: Vec<(i64, i64)>,
    pub missing_ranges: Vec<(i64, i64)>,
}

impl BacktestTickCoverage {
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.missing_ranges.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestTickCacheWriteReport {
    pub cache_dir: PathBuf,
    pub symbol: String,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
    pub rows: usize,
}

impl BacktestTickCache {
    #[must_use]
    pub fn new(history: HistorySeriesCache) -> Self {
        Self { history }
    }

    pub fn open(root_dir: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::new(HistorySeriesCache::open(root_dir)?))
    }

    #[must_use]
    pub fn history_cache(&self) -> &HistorySeriesCache {
        &self.history
    }

    pub fn coverage(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
    ) -> Result<BacktestTickCoverage> {
        let symbol = symbol.as_ref();
        validate_range(symbol, range_start_ns, range_end_ns)?;
        let report = self.history.coverage(HistorySeriesCoverageRequest {
            symbol: symbol.to_owned(),
            kind: HistorySeriesKind::Tick,
            range_start_ns,
            range_end_ns,
        })?;
        Ok(BacktestTickCoverage {
            cache_dir: self.history.root_dir().to_path_buf(),
            symbol: report.symbol,
            range_start_ns: report.range_start_ns,
            range_end_ns: report.range_end_ns,
            cached_ranges: report.cached_ranges,
            missing_ranges: report.missing_ranges,
        })
    }

    pub fn require_coverage(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
    ) -> Result<BacktestTickCoverage> {
        let coverage = self.coverage(symbol, range_start_ns, range_end_ns)?;
        if coverage.is_complete() {
            Ok(coverage)
        } else {
            Err(crate::DataError::InvalidState("backtest tick cache coverage is incomplete"))
        }
    }

    pub fn store_ticks(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
        rows: impl IntoIterator<Item = Tick>,
    ) -> Result<BacktestTickCacheWriteReport> {
        let symbol = symbol.as_ref();
        validate_range(symbol, range_start_ns, range_end_ns)?;
        let mut rows = rows.into_iter().collect::<Vec<_>>();
        rows.retain(|row| row.datetime >= range_start_ns && row.datetime < range_end_ns);
        rows.sort_by_key(|row| (row.datetime, row.id, row.epoch));
        rows.dedup_by(|left, right| {
            left.datetime == right.datetime && left.id == right.id && left.epoch == right.epoch
        });
        self.history.write_tick_segment(symbol, rows.as_slice())?;
        Ok(BacktestTickCacheWriteReport {
            cache_dir: self.history.root_dir().to_path_buf(),
            symbol: symbol.to_owned(),
            range_start_ns,
            range_end_ns,
            rows: rows.len(),
        })
    }

    pub fn load_series(&self, request: TickDataSeriesRequest) -> Result<TickDataSeries> {
        self.require_coverage(
            request.symbol(),
            request.start_datetime_ns(),
            request.end_datetime_ns(),
        )?;
        self.history.read_tick_data_series(request)
    }
}

fn validate_range(symbol: &str, range_start_ns: i64, range_end_ns: i64) -> Result<()> {
    if symbol.is_empty() {
        return Err(crate::DataError::InvalidState("backtest tick cache symbol must not be empty"));
    }
    if range_start_ns >= range_end_ns {
        return Err(crate::DataError::InvalidState(
            "backtest tick cache range_start_ns must be less than range_end_ns",
        ));
    }
    Ok(())
}
```

- [ ] **Step 2: Export the facade and remove JSONL exports**

In `crates/tqsdk-data/src/lib.rs`, replace:

```rust
mod tick_replay_cache;
```

with:

```rust
mod backtest_tick_cache;
```

Remove the public `TickReplayCache` export block.

Add:

```rust
pub use backtest_tick_cache::{
    BacktestCachePolicy, BacktestTickCache, BacktestTickCacheWriteReport, BacktestTickCoverage,
};
```

- [ ] **Step 3: Delete the JSONL cache file**

Delete:

```text
crates/tqsdk-data/src/tick_replay_cache.rs
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
rtk cargo test -p tqsdk-data backtest_tick_cache
rtk cargo test -p tqsdk-data history_cache_open_uses_default_binary_store
```

Expected:

```text
test result: ok
```

- [ ] **Step 5: Run no-default-features coverage**

Run:

```bash
rtk cargo test -p tqsdk-data --no-default-features backtest_tick_cache
```

Expected:

```text
test result: ok
```

- [ ] **Step 6: Commit the facade**

Run:

```bash
rtk git add crates/tqsdk-data/src/backtest_tick_cache.rs crates/tqsdk-data/src/lib.rs crates/tqsdk-data/tests/history_series_cache.rs
rtk git add -u crates/tqsdk-data/src/tick_replay_cache.rs
rtk git commit -m "feat(data): add backtest tick cache facade"
```

## Task 5: Add Breaking Backtest API Skeleton

**Files:**
- Modify: `crates/tqsdk/src/lib.rs`
- Modify: `crates/tqsdk/src/local_backtest.rs`

- [ ] **Step 1: Add compile-failing contract example**

Create `crates/tqsdk/examples/api_contract_s43_facade_backtest_history_cache.rs`:

```rust
use std::time::Duration;

use tqsdk::{BacktestCachePolicy, Tq};
use tqsdk_core::Tick;
use tqsdk_data::{BacktestTickCache, TickDataSeriesRequest};

fn tick(id: i64, datetime: i64, last_price: f64) -> Tick {
    Tick {
        id,
        datetime,
        last_price,
        ask_price1: last_price + 0.5,
        ask_volume1: 1,
        bid_price1: last_price - 0.5,
        bid_volume1: 1,
        ..Tick::default()
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    let dir = std::env::temp_dir().join("tqsdk-backtest-history-cache-contract");
    let cache = BacktestTickCache::open(&dir)?;
    cache.store_ticks(
        "SHFE.rb2601",
        1_000,
        3_000,
        vec![tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
    )?;

    let mut tq = Tq::new()
        .backtest(1_000, 3_000)
        .cache(cache)
        .cache_policy(BacktestCachePolicy::CacheOnly)
        .symbol("SHFE.rb2601")
        .connect()
        .await?;

    let quote = tq.quote("SHFE.rb2601").await?;
    let mut events = 0;
    while tq.next().await? {
        events += 1;
        let loaded = quote.load()?;
        assert!(loaded.last_price >= 100.0);
    }

    assert_eq!(events, 2);

    let request = TickDataSeriesRequest::new(
        "SHFE.rb2601",
        1_000,
        3_000,
        Duration::from_millis(10),
    );
    assert_eq!(cache.load_series(request)?.len(), 2);

    Ok(())
}
```

- [ ] **Step 2: Register the example**

In `crates/tqsdk/Cargo.toml`, replace the old S43 example block with:

```toml
[[example]]
name = "api_contract_s43_facade_backtest_history_cache"
```

- [ ] **Step 3: Run the contract and confirm failure**

Run:

```bash
rtk cargo check -p tqsdk --example api_contract_s43_facade_backtest_history_cache
```

Expected:

```text
error[E0432]: unresolved import `tqsdk::BacktestCachePolicy`
error[E0599]: no method named `cache` found
```

- [ ] **Step 4: Add the new builder types**

In `crates/tqsdk/src/lib.rs`, add these public types near `TqBuilder`:

```rust
pub use tqsdk_data::BacktestCachePolicy;

#[derive(Debug, Clone)]
pub struct BacktestBuilder {
    base: TqBuilder,
    start_ns: i64,
    end_ns: i64,
    cache: Option<tqsdk_data::BacktestTickCache>,
    cache_policy: BacktestCachePolicy,
    symbols: Vec<String>,
}

pub struct PreparedBacktest {
    builder: BacktestBuilder,
    data_report: BacktestDataReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestDataReport {
    pub requested_range: (i64, i64),
    pub cache_policy: BacktestCachePolicy,
    pub cache_dir: Option<std::path::PathBuf>,
    pub resolved_symbols: usize,
    pub remote_used: bool,
}
```

- [ ] **Step 5: Make `TqBuilder::backtest` return the local builder**

Replace the current `TqBuilder::backtest` method with:

```rust
#[must_use]
pub fn backtest(self, start_ns: i64, end_ns: i64) -> BacktestBuilder {
    BacktestBuilder {
        base: self,
        start_ns,
        end_ns,
        cache: None,
        cache_policy: BacktestCachePolicy::RemoteOnMiss,
        symbols: Vec::new(),
    }
}
```

Add the server-side method:

```rust
#[must_use]
pub fn server_backtest(mut self, start_ns: i64, end_ns: i64) -> Self {
    self.backtest = Some(BacktestConfig::Server { start_ns, end_ns });
    self
}
```

Add the custom replay method:

```rust
#[must_use]
pub fn replay_backtest(mut self, replay: tqsdk_task::replay::ReplayMarketSource) -> Self {
    self.backtest = Some(BacktestConfig::Local { replay });
    self
}
```

- [ ] **Step 6: Implement `BacktestBuilder` cache-only connect**

Add:

```rust
impl BacktestBuilder {
    #[must_use]
    pub fn cache(mut self, cache: tqsdk_data::BacktestTickCache) -> Self {
        self.cache = Some(cache);
        self
    }

    #[must_use]
    pub fn cache_policy(mut self, policy: BacktestCachePolicy) -> Self {
        self.cache_policy = policy;
        self
    }

    #[must_use]
    pub fn symbol(mut self, symbol: impl Into<String>) -> Self {
        let symbol = symbol.into();
        if !self.symbols.iter().any(|existing| existing == &symbol) {
            self.symbols.push(symbol);
        }
        self
    }

    pub async fn prepare(self) -> Result<PreparedBacktest> {
        if self.end_ns <= self.start_ns {
            return Err(data_validation("backtest end_ns must be greater than start_ns"));
        }
        if self.symbols.is_empty() {
            return Err(data_validation("backtest requires at least one symbol in phase 1"));
        }
        let cache = self
            .cache
            .as_ref()
            .ok_or_else(|| data_validation("backtest cache is required in phase 1"))?;
        for symbol in &self.symbols {
            cache.require_coverage(symbol, self.start_ns, self.end_ns)?;
        }
        let data_report = BacktestDataReport {
            requested_range: (self.start_ns, self.end_ns),
            cache_policy: self.cache_policy,
            cache_dir: Some(cache.history_cache().root_dir().to_path_buf()),
            resolved_symbols: self.symbols.len(),
            remote_used: false,
        };
        Ok(PreparedBacktest {
            builder: self,
            data_report,
        })
    }

    pub async fn connect(self) -> Result<Tq> {
        self.prepare().await?.connect().await
    }
}

impl PreparedBacktest {
    #[must_use]
    pub fn data_report(&self) -> &BacktestDataReport {
        &self.data_report
    }

    pub async fn connect(self) -> Result<Tq> {
        let cache = self
            .builder
            .cache
            .as_ref()
            .ok_or_else(|| data_validation("prepared backtest cache missing"))?;
        let mut series = Vec::new();
        for symbol in &self.builder.symbols {
            series.push(cache.load_series(tqsdk_data::TickDataSeriesRequest::new(
                symbol,
                self.builder.start_ns,
                self.builder.end_ns,
                std::time::Duration::from_secs(5),
            ))?);
        }
        let replay = local_backtest::replay_from_ticks(series)?;
        self.builder.base.replay_backtest(replay).connect().await
    }
}
```

- [ ] **Step 7: Replace internal references to old local method**

In `crates/tqsdk/src/lib.rs`, replace internal calls to:

```rust
self.local_backtest(replay)
```

with:

```rust
self.replay_backtest(replay)
```

Then remove public methods whose names start with:

```text
local_backtest_
```

Keep `quote_symbol`, `price_tick`, `instrument_spec`, `instrument_specs`, and `default_price_tick` as builder configuration methods if current examples still use them.

- [ ] **Step 8: Run the contract example**

Run:

```bash
rtk cargo run -p tqsdk --example api_contract_s43_facade_backtest_history_cache
```

Expected:

```text
no output and exit code 0
```

- [ ] **Step 9: Commit the facade skeleton**

Run:

```bash
rtk git add crates/tqsdk/src/lib.rs crates/tqsdk/src/local_backtest.rs crates/tqsdk/Cargo.toml crates/tqsdk/examples/api_contract_s43_facade_backtest_history_cache.rs
rtk git add -u crates/tqsdk/examples/api_contract_s43_facade_tick_cache_backtest.rs
rtk git commit -m "feat(tqsdk): add persistent-cache backtest builder"
```

## Task 6: Update Public Documentation

**Files:**
- Modify: `README.md`
- Modify: `crates/tqsdk/README.md`
- Modify: `crates/tqsdk-data/README.md`
- Modify: `docs/architecture/api-data.md`
- Modify: `docs/architecture/crate-boundaries.md`
- Modify: `docs/architecture/validation.md`

- [ ] **Step 1: Update public README backtest wording**

Replace references to `local_backtest_*` with this user-facing shape:

```rust
let mut tq = Tq::new()
    .auth_env()?
    .backtest(start_ns, end_ns)
    .cache_dir(".tqsdk/history")
    .symbol("SHFE.rb2601")
    .connect()
    .await?;
```

Add this note:

```markdown
`backtest(start, end)` is the local simulated backtest path. It reuses the shared
`HistorySeriesCache` persistent history store. `server_backtest(start, end)` is
reserved for the official server-side market-data replay mode.
```

- [ ] **Step 2: Update `crates/tqsdk-data/README.md` cache section**

Add:

```markdown
`HistorySeriesCache` is the durable cache abstraction for both ordinary history
series and backtest acceleration. `BacktestTickCache` is a tick-only facade over
that cache; it does not own a second file format. The default store backend uses
the existing binary history segment layout.
```

- [ ] **Step 3: Update architecture boundary docs**

In `docs/architecture/crate-boundaries.md`, add the durable cache rule:

```markdown
`tqsdk-data` owns `HistorySeriesCache` and its `HistorySeriesStore` backends.
Backtest acceleration must reuse that abstraction. No SDK crate should add a
second durable tick-cache file format for backtest.
```

- [ ] **Step 4: Update validation matrix**

In `docs/architecture/validation.md`, add:

```bash
rtk cargo test -p tqsdk-data history_series_cache
rtk cargo test -p tqsdk-data --no-default-features backtest_tick_cache
rtk cargo run -p tqsdk --example api_contract_s43_facade_backtest_history_cache
rtk cargo check -p tqsdk --no-default-features --example api_contract_s43_facade_backtest_history_cache
```

- [ ] **Step 5: Run docs whitespace check**

Run:

```bash
rtk git diff --check README.md crates/tqsdk/README.md crates/tqsdk-data/README.md docs/architecture/api-data.md docs/architecture/crate-boundaries.md docs/architecture/validation.md
```

Expected:

```text
no output and exit code 0
```

- [ ] **Step 6: Commit docs**

Run:

```bash
rtk git add README.md crates/tqsdk/README.md crates/tqsdk-data/README.md docs/architecture/api-data.md docs/architecture/crate-boundaries.md docs/architecture/validation.md
rtk git commit -m "docs: document history-cache backed backtest"
```

## Task 7: Final Verification

**Files:**
- Verify current workspace only

- [ ] **Step 1: Run formatting**

Run:

```bash
rtk cargo fmt --all
```

Expected:

```text
exit code 0
```

- [ ] **Step 2: Run focused test suite**

Run:

```bash
rtk cargo test -p tqsdk-data history_series_cache
rtk cargo test -p tqsdk-data --no-default-features backtest_tick_cache
rtk cargo run -p tqsdk --example api_contract_s43_facade_backtest_history_cache
rtk cargo check -p tqsdk --no-default-features --example api_contract_s43_facade_backtest_history_cache
```

Expected:

```text
all commands exit code 0
```

- [ ] **Step 3: Run broader facade checks**

Run:

```bash
rtk cargo check -p tqsdk --examples
rtk cargo check -p tqsdk --no-default-features --examples
rtk cargo clippy -p tqsdk-data --all-targets -- -D warnings
rtk cargo clippy -p tqsdk --example api_contract_s43_facade_backtest_history_cache -- -D warnings
rtk cargo fmt --all --check
rtk git diff --check
```

Expected:

```text
all commands exit code 0
```

- [ ] **Step 4: Run GitNexus change detection**

Run:

```bash
rtk gitnexus detect-changes
```

Expected:

```text
reported changes are limited to tqsdk-data cache abstraction, tqsdk backtest facade, contract example, and docs
```

- [ ] **Step 5: Final commit if verification caused formatting changes**

If `cargo fmt` changed files after the last task commit, run:

```bash
rtk git add crates/tqsdk-data crates/tqsdk README.md docs/architecture
rtk git commit -m "style: format backtest history cache changes"
```

Expected:

```text
commit created only when formatting changed tracked files
```
