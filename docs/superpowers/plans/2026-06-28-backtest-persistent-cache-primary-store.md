# Backtest Persistent Cache Primary Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `backtest(start, end)` around a single-file persistent tick cache that accelerates repeat backtests and fills misses from official server-side backtest streams.

**Architecture:** `tqsdk` owns the user-facing builder and lazy mode selection. `tqsdk-data` owns `HistorySeriesCache`, the single-file `HistorySeriesStore`, backtest tick coverage, and shared universe selection. `tqsdk-task` owns bounded replay streams and local `TqSim` execution, with both cached ticks and remote ticks feeding the same backtest stream interface.

**Tech Stack:** Rust 2024, existing `fs2` locking, existing binary tick/kline row encoding helpers, `tqsdk-wait::TqApiBuilder::futures_backtest`, `tick_ready(symbol, 10_000)`, `step_until`, `changed_rows`, Cargo tests and contract examples.

---

## Scope And Commit Strategy

This plan covers the full spec in incremental commits. Each task should be completed, verified, and committed before starting the next task. Do not use professional history download APIs during implementation; remote cache fills must use the official server-side backtest market stream.

Before editing Rust symbols in each task, run the repository-required impact analysis for the symbols named in that task. If the risk is HIGH or CRITICAL, report the callers and affected flows before editing.

## File Structure

- `crates/tqsdk-data/src/history_series_cache/store.rs`: stable store trait, format IDs, request/report types.
- `crates/tqsdk-data/src/history_series_cache/series_file_store.rs`: new single-file `.tqseries` backend, chunk codec, same-file locking, coverage scan, row reader.
- `crates/tqsdk-data/src/history_series_cache.rs`: constructors and shared wrappers for `HistorySeriesCache`.
- `crates/tqsdk-data/src/backtest_tick_cache.rs`: tick-only facade, default single-file cache opening, partial write and coverage commit helpers.
- `crates/tqsdk-data/src/universe_expression.rs`: shared selector expression parser moved from relay.
- `crates/tqsdk-data/src/universe.rs`: shared futures universe resolver moved from relay.
- `crates/tqsdk-relay/src/universe_expression.rs`: removed; relay imports shared data parser.
- `crates/tqsdk-relay/src/universe.rs`: reduced to relay-specific adapters or re-exports from data.
- `crates/tqsdk-task/src/backtest_stream.rs`: async market stream trait and `ReplayMarketSource` adapter.
- `crates/tqsdk-task/src/history_tick_replay.rs`: bounded heap merge of `HistorySeriesReader` tick streams.
- `crates/tqsdk-task/src/backtest.rs`: `StrategyBacktest` consumes `BacktestMarketStream`.
- `crates/tqsdk/src/backtest_remote.rs`: remote-on-miss stream that reads official backtest ticks and writes cache.
- `crates/tqsdk/src/local_backtest.rs`: cache hit path uses streaming tick replay instead of materializing all events.
- `crates/tqsdk/src/lib.rs`: public API collapse, lazy auth, cache policy selection, builder wiring.
- `crates/tqsdk-data/tests/history_series_single_file_store.rs`: storage contract tests.
- `crates/tqsdk-data/tests/universe_selector.rs`: shared selector tests.
- `crates/tqsdk-task/tests/history_tick_replay.rs`: streaming replay tests.
- `crates/tqsdk/tests/facade_contract.rs`: updated facade tests.
- `crates/tqsdk/examples/api_contract_s43_facade_backtest_history_cache.rs`: updated cache-backed backtest contract.
- `crates/tqsdk/examples/api_contract_s44_facade_backtest_remote_on_miss.rs`: new remote-on-miss contract example.
- `README.md`, `crates/tqsdk/README.md`, `crates/tqsdk-data/README.md`, `docs/architecture/README.md`, `docs/architecture/crate-boundaries.md`, `docs/architecture/validation.md`: architecture and validation updates.

---

### Task 1: Add Failing Single-File Store Contract Tests

**Files:**
- Create: `crates/tqsdk-data/tests/history_series_single_file_store.rs`
- Modify: `crates/tqsdk-data/tests/history_series_cache.rs`

- [ ] **Step 1: Run impact analysis**

Run:

```bash
rtk git status --short
```

Expected: no unrelated work is staged. If unrelated files are dirty, leave them untouched.

Run GitNexus impact analysis for `HistorySeriesCache`, `BacktestTickCache`, and `HistorySeriesStore`.

- [ ] **Step 2: Write failing tests for the target store shape**

Create `crates/tqsdk-data/tests/history_series_single_file_store.rs` with:

```rust
use std::path::{Path, PathBuf};

use tqsdk_core::Tick;
use tqsdk_data::{
    BacktestTickCache, HistorySeriesCache, HistorySeriesCoverageRequest, HistorySeriesKind,
    TickDataSeriesRequest,
};

const SERIES_FILE_FORMAT_ID: &str = "tqsdk.series-file.v1";

#[test]
fn backtest_tick_cache_open_uses_single_file_store() {
    let dir = temp_dir("backtest-open-single-file");

    let cache = BacktestTickCache::open(&dir).unwrap();

    assert_eq!(cache.history_cache().root_dir(), dir.as_path());
    assert_eq!(cache.history_cache().format_id(), SERIES_FILE_FORMAT_ID);
}

#[test]
fn series_file_store_uses_one_final_file_per_symbol_period() {
    let dir = temp_dir("one-file-per-symbol-period");
    let cache = BacktestTickCache::open(&dir).unwrap();

    cache
        .store_ticks(
            "SHFE.rb2601",
            1_000,
            4_000,
            [tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
        )
        .unwrap();

    let tick_file = dir.join("series").join("SHFE.rb2601").join("tick.tqseries");
    assert!(tick_file.is_file(), "missing {}", tick_file.display());

    let files = regular_files(&dir);
    assert_eq!(files, vec![tick_file]);
    assert!(!dir.join(".SHFE.rb2601.0.coverage").exists());
}

#[test]
fn series_file_store_embeds_coverage_and_reopens_complete_range() {
    let dir = temp_dir("embedded-coverage-reopen");
    let cache = BacktestTickCache::open(&dir).unwrap();

    cache
        .store_ticks(
            "DCE.i2601",
            1_000,
            5_000,
            [tick(1, 1_000, 100.0), tick(2, 3_000, 101.0)],
        )
        .unwrap();

    let reopened = BacktestTickCache::open(&dir).unwrap();
    let coverage = reopened.coverage("DCE.i2601", 1_000, 5_000).unwrap();
    assert!(coverage.is_complete());
    assert_eq!(coverage.cached_ranges, vec![(1_000, 5_000)]);
    assert_eq!(coverage.missing_ranges, Vec::<(i64, i64)>::new());

    let rows = reopened
        .load_series(TickDataSeriesRequest::new("DCE.i2601", 1_000, 5_000))
        .unwrap();
    assert_eq!(rows.iter().map(|row| row.id).collect::<Vec<_>>(), vec![1, 2]);
}

#[test]
fn series_file_store_partial_rows_do_not_create_coverage() {
    let dir = temp_dir("partial-rows-no-coverage");
    let history = HistorySeriesCache::open_series_file(&dir).unwrap();

    history
        .write_tick_rows_without_coverage(
            "SHFE.au2608",
            &[tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
        )
        .unwrap();

    let coverage = history
        .coverage(HistorySeriesCoverageRequest {
            symbol: "SHFE.au2608".to_string(),
            kind: HistorySeriesKind::Tick,
            range_start_ns: 1_000,
            range_end_ns: 3_000,
        })
        .unwrap();
    assert_eq!(coverage.cached_ranges, Vec::<(i64, i64)>::new());
    assert_eq!(coverage.missing_ranges, vec![(1_000, 3_000)]);
}

fn tick(id: i64, datetime: i64, last_price: f64) -> Tick {
    Tick {
        id,
        datetime,
        last_price,
        highest: last_price,
        lowest: last_price,
        average: last_price,
        bid_price1: last_price - 1.0,
        bid_volume1: 1,
        ask_price1: last_price + 1.0,
        ask_volume1: 1,
        ..Tick::default()
    }
}

fn temp_dir(name: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "tqsdk-series-file-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn regular_files(root: &Path) -> Vec<PathBuf> {
    fn walk(dir: &Path, files: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, files);
            } else if path.is_file() {
                files.push(path);
            }
        }
    }

    let mut files = Vec::new();
    walk(root, &mut files);
    files.sort();
    files
}
```

- [ ] **Step 3: Preserve binary-store coverage test intent**

In `crates/tqsdk-data/tests/history_series_cache.rs`, rename the existing test `backtest_tick_cache_reuses_history_series_cache_storage` to:

```rust
fn backtest_tick_cache_can_wrap_binary_history_series_cache_storage()
```

Keep its explicit `HistorySeriesCache::open(&dir)` setup so it continues to test the binary compatibility backend.

- [ ] **Step 4: Verify failing tests**

Run:

```bash
rtk cargo test -p tqsdk-data --test history_series_single_file_store
```

Expected: FAIL because `HistorySeriesCache::open_series_file` and `write_tick_rows_without_coverage` do not exist, and `BacktestTickCache::open` still uses the binary backend.

- [ ] **Step 5: Commit the failing tests**

```bash
rtk git add crates/tqsdk-data/tests/history_series_single_file_store.rs crates/tqsdk-data/tests/history_series_cache.rs
rtk git commit -m "test(data): define single-file history store contract"
```

---

### Task 2: Add Store Trait Coverage Commit And Series File Skeleton

**Files:**
- Modify: `crates/tqsdk-data/src/history_series_cache/store.rs`
- Modify: `crates/tqsdk-data/src/history_series_cache.rs`
- Create: `crates/tqsdk-data/src/history_series_cache/series_file_store.rs`

- [ ] **Step 1: Run impact analysis**

Run GitNexus impact analysis for `HistorySeriesStore`, `HistorySeriesCache::open`, and `HistorySeriesCache::write_segment`.

- [ ] **Step 2: Extend store types**

In `crates/tqsdk-data/src/history_series_cache/store.rs`, add the format ID and coverage commit type:

```rust
pub const SERIES_FILE_HISTORY_SERIES_FORMAT_ID: &str = "tqsdk.series-file.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistorySeriesCoverageCommit {
    pub symbol: String,
    pub kind: HistorySeriesKind,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
    pub rows: usize,
    pub id_range: Option<(i64, i64)>,
}
```

Add this method to `HistorySeriesStore`:

```rust
fn commit_coverage(
    &self,
    commit: HistorySeriesCoverageCommit,
) -> Result<HistorySeriesCoverageReport>;
```

- [ ] **Step 3: Export new store items**

In `crates/tqsdk-data/src/history_series_cache.rs`, update the `pub use store::{ ... }` list to include:

```rust
HistorySeriesCoverageCommit, SERIES_FILE_HISTORY_SERIES_FORMAT_ID,
```

Add the module:

```rust
mod series_file_store;
```

- [ ] **Step 4: Add the series-file store skeleton**

Create `crates/tqsdk-data/src/history_series_cache/series_file_store.rs`:

```rust
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{DataError, Result};

use super::{
    HISTORY_SERIES_CACHE_SCHEMA_VERSION, HistorySeriesCacheMaintenanceReport,
    HistorySeriesCacheScanReport, HistorySeriesCoverageCommit, HistorySeriesCoverageReport,
    HistorySeriesCoverageRequest, HistorySeriesReadRequest, HistorySeriesReader,
    HistorySeriesSegmentReport, HistorySeriesStore, HistorySeriesWriteSegment,
    SERIES_FILE_HISTORY_SERIES_FORMAT_ID,
};

const ROOT_DIR_NAME: &str = "series";
const TICK_FILE_NAME: &str = "tick.tqseries";

#[derive(Debug, Clone)]
pub(super) struct SeriesFileHistoryStore {
    root_dir: Arc<PathBuf>,
}

impl SeriesFileHistoryStore {
    pub(super) fn new(root_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(root_dir.join(ROOT_DIR_NAME))?;
        Ok(Self {
            root_dir: Arc::new(root_dir),
        })
    }

    pub(super) fn series_path(&self, symbol: &str, duration_ns: i64) -> PathBuf {
        self.root_dir
            .join(ROOT_DIR_NAME)
            .join(escape_symbol_path_component(symbol))
            .join(if duration_ns == 0 {
                TICK_FILE_NAME.to_string()
            } else {
                format!("{duration_ns}.tqseries")
            })
    }
}

impl HistorySeriesStore for SeriesFileHistoryStore {
    fn format_id(&self) -> &'static str {
        SERIES_FILE_HISTORY_SERIES_FORMAT_ID
    }

    fn schema_version(&self) -> u32 {
        HISTORY_SERIES_CACHE_SCHEMA_VERSION
    }

    fn root_dir(&self) -> &Path {
        self.root_dir.as_path()
    }

    fn uses_mmap_backend(&self) -> bool {
        false
    }

    fn scan(&self) -> Result<HistorySeriesCacheScanReport> {
        super::empty_scan_report(self.root_dir.as_path())
    }

    fn enforce_limits(
        &self,
        _max_bytes: Option<u64>,
        _retention_days: Option<u64>,
    ) -> Result<HistorySeriesCacheMaintenanceReport> {
        Ok(HistorySeriesCacheMaintenanceReport::default())
    }

    fn coverage(
        &self,
        request: HistorySeriesCoverageRequest,
    ) -> Result<HistorySeriesCoverageReport> {
        Ok(HistorySeriesCoverageReport {
            cached_ranges: Vec::new(),
            missing_ranges: vec![(request.range_start_ns, request.range_end_ns)],
            symbol: request.symbol,
            kind: request.kind,
            range_start_ns: request.range_start_ns,
            range_end_ns: request.range_end_ns,
        })
    }

    fn write_segment(
        &self,
        _segment: HistorySeriesWriteSegment<'_>,
    ) -> Result<HistorySeriesSegmentReport> {
        Err(DataError::InvalidState("series-file store write path is not wired"))
    }

    fn commit_coverage(
        &self,
        commit: HistorySeriesCoverageCommit,
    ) -> Result<HistorySeriesCoverageReport> {
        Ok(HistorySeriesCoverageReport {
            symbol: commit.symbol,
            kind: commit.kind,
            range_start_ns: commit.range_start_ns,
            range_end_ns: commit.range_end_ns,
            cached_ranges: vec![(commit.range_start_ns, commit.range_end_ns)],
            missing_ranges: Vec::new(),
        })
    }

    fn open_reader(
        &self,
        _request: HistorySeriesReadRequest,
    ) -> Result<Box<dyn HistorySeriesReader>> {
        Err(DataError::InvalidState("series-file store reader path is not wired"))
    }
}

fn escape_symbol_path_component(symbol: &str) -> String {
    symbol.replace('/', "%2F")
}
```

- [ ] **Step 5: Add cache constructors and helper wrappers**

In `impl HistorySeriesCache` in `history_series_cache.rs`, add:

```rust
pub fn open_series_file(root_dir: impl AsRef<Path>) -> Result<Self> {
    let root_dir = canonical_or_original(root_dir.as_ref());
    let store = series_file_store::SeriesFileHistoryStore::new(root_dir)?;
    Ok(Self::from_store(Arc::new(store)))
}

pub fn commit_coverage(
    &self,
    commit: HistorySeriesCoverageCommit,
) -> Result<HistorySeriesCoverageReport> {
    self.store.commit_coverage(commit)
}

pub fn write_tick_rows_without_coverage(&self, symbol: &str, rows: &[Tick]) -> Result<()> {
    self.write_segment(HistorySeriesWriteSegment {
        symbol,
        kind: HistorySeriesKind::Tick,
        declared_range_ns: None,
        rows: HistorySeriesWriteRows::Ticks(rows),
    })?;
    Ok(())
}
```

Add this helper near scan helpers so the skeleton compiles:

```rust
fn empty_scan_report(root_dir: &Path) -> Result<HistorySeriesCacheScanReport> {
    Ok(HistorySeriesCacheScanReport {
        cache_dir: root_dir.to_path_buf(),
        schema_version: HISTORY_SERIES_CACHE_SCHEMA_VERSION,
        files: Vec::new(),
    })
}
```

- [ ] **Step 6: Make binary store implement the new trait method**

In `crates/tqsdk-data/src/history_series_cache/binary_store.rs`, add `HistorySeriesCoverageCommit` to imports and implement:

```rust
fn commit_coverage(
    &self,
    commit: HistorySeriesCoverageCommit,
) -> Result<HistorySeriesCoverageReport> {
    super::commit_coverage_with_inner(&self.inner, commit)
}
```

In `history_series_cache.rs`, implement `commit_coverage_with_inner` by reusing the existing declared coverage writer:

```rust
fn commit_coverage_with_inner(
    inner: &Arc<HistorySeriesCacheInner>,
    commit: HistorySeriesCoverageCommit,
) -> Result<HistorySeriesCoverageReport> {
    let cache = HistorySeriesCache::from_binary_inner(Arc::clone(inner));
    cache.write_segment(HistorySeriesWriteSegment {
        symbol: commit.symbol.as_str(),
        kind: commit.kind,
        declared_range_ns: Some((commit.range_start_ns, commit.range_end_ns)),
        rows: match commit.kind {
            HistorySeriesKind::Tick => HistorySeriesWriteRows::Ticks(&[]),
            HistorySeriesKind::Kline { .. } => HistorySeriesWriteRows::Klines(&[]),
        },
    })?;
    cache.coverage(HistorySeriesCoverageRequest {
        symbol: commit.symbol,
        kind: commit.kind,
        range_start_ns: commit.range_start_ns,
        range_end_ns: commit.range_end_ns,
    })
}
```

- [ ] **Step 7: Verify compilation fails only on store behavior tests**

Run:

```bash
rtk cargo test -p tqsdk-data --test history_series_single_file_store
```

Expected: compilation passes, tests still fail on write/read/store default behavior.

- [ ] **Step 8: Commit skeleton**

```bash
rtk git add crates/tqsdk-data/src/history_series_cache.rs crates/tqsdk-data/src/history_series_cache/store.rs crates/tqsdk-data/src/history_series_cache/binary_store.rs crates/tqsdk-data/src/history_series_cache/series_file_store.rs
rtk git commit -m "feat(data): add series-file history store skeleton"
```

---

### Task 3: Implement The Single-File Chunk Log

**Files:**
- Modify: `crates/tqsdk-data/src/history_series_cache/series_file_store.rs`
- Modify: `crates/tqsdk-data/src/history_series_cache.rs`
- Modify: `crates/tqsdk-data/src/history_series_cache/storage.rs`

- [ ] **Step 1: Run impact analysis**

Run GitNexus impact analysis for `SeriesFileHistoryStore`, `HistorySeriesStore::write_segment`, and `HistorySeriesStore::open_reader`.

- [ ] **Step 2: Make row encoding reusable**

In `storage.rs`, expose the existing row encoding helpers to sibling modules:

```rust
pub(super) fn write_kline_row(writer: &mut impl Write, row: &Kline) -> Result<()> {
    write_i64(writer, row.id)?;
    write_i64(writer, row.datetime)?;
    write_f64(writer, row.open)?;
    write_f64(writer, row.high)?;
    write_f64(writer, row.low)?;
    write_f64(writer, row.close)?;
    write_f64(writer, row.volume as f64)?;
    write_f64(writer, row.open_oi as f64)?;
    write_f64(writer, row.close_oi as f64)?;
    Ok(())
}

pub(super) fn write_tick_row(
    writer: &mut impl Write,
    row: &Tick,
    five_level: bool,
) -> Result<()> {
    write_i64(writer, row.id)?;
    write_i64(writer, row.datetime)?;
    write_f64(writer, row.last_price)?;
    write_f64(writer, row.highest)?;
    write_f64(writer, row.lowest)?;
    write_f64(writer, row.average)?;
    write_f64(writer, row.volume as f64)?;
    write_f64(writer, row.amount)?;
    write_f64(writer, row.open_interest as f64)?;
    write_tick_level(writer, row.bid_price1, row.bid_volume1, row.ask_price1, row.ask_volume1)?;
    if five_level {
        write_tick_level(writer, row.bid_price2, row.bid_volume2, row.ask_price2, row.ask_volume2)?;
        write_tick_level(writer, row.bid_price3, row.bid_volume3, row.ask_price3, row.ask_volume3)?;
        write_tick_level(writer, row.bid_price4, row.bid_volume4, row.ask_price4, row.ask_volume4)?;
        write_tick_level(writer, row.bid_price5, row.bid_volume5, row.ask_price5, row.ask_volume5)?;
    }
    Ok(())
}
```

- [ ] **Step 3: Add the chunk codec**

In `series_file_store.rs`, replace the skeleton body with chunk primitives:

```rust
const FILE_MAGIC: &[u8; 8] = b"TQHSF1\0\0";
const CHUNK_MAGIC: &[u8; 4] = b"TQSC";
const CHUNK_HEADER_LEN: usize = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkKind {
    Meta = 1,
    Rows = 2,
    Coverage = 3,
}

fn checksum64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn write_chunk(writer: &mut impl std::io::Write, kind: ChunkKind, payload: &[u8]) -> Result<()> {
    writer.write_all(CHUNK_MAGIC)?;
    writer.write_all(&[kind as u8, 0, 0, 0])?;
    writer.write_all(&(payload.len() as u64).to_le_bytes())?;
    writer.write_all(&checksum64(payload).to_le_bytes())?;
    writer.write_all(payload)?;
    Ok(())
}
```

Add payload helpers:

```rust
fn append_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn append_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn append_string(out: &mut Vec<u8>, value: &str) -> Result<()> {
    let len = u16::try_from(value.len()).map_err(|_| {
        DataError::InvalidResponse("history series symbol is too long for series-file metadata".to_string())
    })?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}
```

- [ ] **Step 4: Add file state scanning**

Add:

```rust
#[derive(Debug, Default)]
struct SeriesFileState {
    rows: Vec<HistorySeriesRow>,
    coverage: Vec<(i64, i64)>,
}

fn scan_series_file(path: &Path, kind: HistorySeriesKind) -> Result<SeriesFileState> {
    if !path.exists() {
        return Ok(SeriesFileState::default());
    }
    let bytes = std::fs::read(path)?;
    if bytes.len() < FILE_MAGIC.len() || &bytes[..FILE_MAGIC.len()] != FILE_MAGIC {
        return Err(DataError::InvalidResponse(format!(
            "invalid history series-file header: {}",
            path.display()
        )));
    }
    let mut offset = FILE_MAGIC.len();
    let mut state = SeriesFileState::default();
    while offset + CHUNK_HEADER_LEN <= bytes.len() {
        if &bytes[offset..offset + 4] != CHUNK_MAGIC {
            break;
        }
        let kind_byte = bytes[offset + 4];
        let len_offset = offset + 8;
        let payload_len = u64::from_le_bytes(bytes[len_offset..len_offset + 8].try_into().unwrap()) as usize;
        let checksum_offset = offset + 16;
        let checksum = u64::from_le_bytes(bytes[checksum_offset..checksum_offset + 8].try_into().unwrap());
        let payload_start = offset + CHUNK_HEADER_LEN;
        let payload_end = payload_start.saturating_add(payload_len);
        if payload_end > bytes.len() {
            break;
        }
        let payload = &bytes[payload_start..payload_end];
        if checksum64(payload) != checksum {
            break;
        }
        match kind_byte {
            2 => decode_rows_payload(payload, kind, &mut state.rows)?,
            3 => decode_coverage_payload(payload, &mut state.coverage)?,
            _ => {}
        }
        offset = payload_end;
    }
    Ok(state)
}
```

Implement `decode_rows_payload` and `decode_coverage_payload` by reading the row count and then using the same field order as `MappedSeriesFile::read_row`.

- [ ] **Step 5: Implement write, coverage, and reader**

In the `HistorySeriesStore for SeriesFileHistoryStore` implementation:

```rust
fn write_segment(
    &self,
    segment: HistorySeriesWriteSegment<'_>,
) -> Result<HistorySeriesSegmentReport> {
    validate_segment_rows(&segment)?;
    let duration_ns = segment.kind.duration_ns();
    let path = self.series_path(segment.symbol, duration_ns);
    std::fs::create_dir_all(path.parent().unwrap())?;

    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(&path)?;
    fs2::FileExt::lock_exclusive(&lock_file)?;
    let result = append_segment_to_file(&path, &segment);
    fs2::FileExt::unlock(&lock_file)?;
    result
}
```

`append_segment_to_file` must:

1. create the file with `FILE_MAGIC` if it does not exist;
2. append a `Meta` chunk when the file is new;
3. append a `Rows` chunk when rows are non-empty;
4. append a `Coverage` chunk only when `declared_range_ns` is `Some`;
5. flush the file before returning.

Implement `coverage` by scanning coverage chunks and computing:

```rust
let cached_ranges = super::merge_datetime_ranges(state.coverage);
let missing_ranges = super::rangeset_difference(
    &[(request.range_start_ns, request.range_end_ns)],
    &cached_ranges,
);
```

Implement `open_reader` with:

```rust
struct SeriesFileReader {
    rows: Vec<HistorySeriesRow>,
    index: usize,
}

impl HistorySeriesReader for SeriesFileReader {
    fn next_row(&mut self) -> Result<Option<HistorySeriesRow>> {
        let row = self.rows.get(self.index).cloned();
        if row.is_some() {
            self.index += 1;
        }
        Ok(row)
    }
}
```

Filter rows by `[range_start_ns, range_end_ns)` before constructing `SeriesFileReader`.

- [ ] **Step 6: Make the new tests pass**

Run:

```bash
rtk cargo test -p tqsdk-data --test history_series_single_file_store
```

Expected: PASS.

- [ ] **Step 7: Run the existing cache tests**

Run:

```bash
rtk cargo test -p tqsdk-data --test history_series_cache
```

Expected: PASS.

- [ ] **Step 8: Commit chunk log implementation**

```bash
rtk git add crates/tqsdk-data/src/history_series_cache.rs crates/tqsdk-data/src/history_series_cache/storage.rs crates/tqsdk-data/src/history_series_cache/series_file_store.rs
rtk git commit -m "feat(data): implement single-file history series store"
```

---

### Task 4: Make Backtest Tick Cache Default To Single-File Store

**Files:**
- Modify: `crates/tqsdk-data/src/backtest_tick_cache.rs`
- Modify: `crates/tqsdk-data/tests/history_series_cache.rs`
- Modify: `crates/tqsdk-data/tests/history_series_single_file_store.rs`
- Modify: `crates/tqsdk-data/src/lib.rs`

- [ ] **Step 1: Run impact analysis**

Run GitNexus impact analysis for `BacktestTickCache::open`, `BacktestTickCache::store_ticks`, and `BacktestCachePolicy`.

- [ ] **Step 2: Collapse cache policy names**

Replace `BacktestCachePolicy` with:

```rust
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum BacktestCachePolicy {
    Disabled,
    CacheOnly,
    #[default]
    RemoteOnMiss,
    Refresh,
}
```

- [ ] **Step 3: Switch `BacktestTickCache::open`**

Change `BacktestTickCache::open`:

```rust
pub fn open(root_dir: impl AsRef<Path>) -> Result<Self> {
    Ok(Self::new(HistorySeriesCache::open_series_file(root_dir)?))
}
```

Add an explicit binary helper for tests and compatibility tooling:

```rust
pub fn open_binary_compat(root_dir: impl AsRef<Path>) -> Result<Self> {
    Ok(Self::new(HistorySeriesCache::open(root_dir)?))
}
```

- [ ] **Step 4: Split complete writes from partial writes**

Add:

```rust
pub fn append_partial_ticks(
    &self,
    symbol: impl AsRef<str>,
    rows: impl IntoIterator<Item = Tick>,
) -> Result<BacktestTickCacheWriteReport> {
    let symbol = symbol.as_ref();
    if symbol.is_empty() {
        return Err(DataError::InvalidState("backtest tick cache symbol must not be empty"));
    }
    let mut rows = rows.into_iter().collect::<Vec<_>>();
    rows.sort_by_key(|row| (row.id, row.datetime, row.epoch));
    rows.dedup_by(|left, right| left.id == right.id && left.datetime == right.datetime);
    self.history.write_segment(HistorySeriesWriteSegment {
        symbol,
        kind: HistorySeriesKind::Tick,
        declared_range_ns: None,
        rows: HistorySeriesWriteRows::Ticks(rows.as_slice()),
    })?;
    Ok(BacktestTickCacheWriteReport {
        cache_dir: self.history.root_dir().to_path_buf(),
        symbol: symbol.to_string(),
        range_start_ns: rows.first().map_or(0, |row| row.datetime),
        range_end_ns: rows.last().map_or(0, |row| row.datetime.saturating_add(1)),
        rows: rows.len(),
    })
}

pub fn mark_complete(
    &self,
    symbol: impl AsRef<str>,
    range_start_ns: i64,
    range_end_ns: i64,
    rows: usize,
    id_range: Option<(i64, i64)>,
) -> Result<BacktestTickCoverage> {
    let symbol = symbol.as_ref();
    validate_range(symbol, range_start_ns, range_end_ns)?;
    let report = self.history.commit_coverage(HistorySeriesCoverageCommit {
        symbol: symbol.to_string(),
        kind: HistorySeriesKind::Tick,
        range_start_ns,
        range_end_ns,
        rows,
        id_range,
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
```

Then make `store_ticks` call `append_partial_ticks` followed by `mark_complete`.

- [ ] **Step 5: Update tests that expected binary paths**

For tests that use `BacktestTickCache::open(&dir)` and assert old segment paths, change either:

```rust
let cache = BacktestTickCache::open_binary_compat(&dir).unwrap();
```

or change the assertion to:

```rust
assert!(dir.join("series").join("SHFE.rb2601").join("tick.tqseries").exists());
```

Use `open_binary_compat` only when the test is explicitly checking legacy binary segment behavior.

- [ ] **Step 6: Verify data tests**

Run:

```bash
rtk cargo test -p tqsdk-data --test history_series_cache
rtk cargo test -p tqsdk-data --test history_series_single_file_store
```

Expected: PASS.

- [ ] **Step 7: Commit default switch**

```bash
rtk git add crates/tqsdk-data/src/backtest_tick_cache.rs crates/tqsdk-data/src/lib.rs crates/tqsdk-data/tests/history_series_cache.rs crates/tqsdk-data/tests/history_series_single_file_store.rs
rtk git commit -m "feat(data): default backtest tick cache to series files"
```

---

### Task 5: Add Tick Fill Integrity Accumulator

**Files:**
- Modify: `crates/tqsdk-data/src/backtest_tick_cache.rs`
- Modify: `crates/tqsdk-data/tests/history_series_single_file_store.rs`

- [ ] **Step 1: Run impact analysis**

Run GitNexus impact analysis for `BacktestTickCache`.

- [ ] **Step 2: Add failing integrity tests**

Append to `history_series_single_file_store.rs`:

```rust
#[test]
fn tick_fill_accumulator_marks_continuous_rows_complete() {
    let mut fill = tqsdk_data::BacktestTickFill::new("SHFE.rb2601", 1_000, 4_000);
    fill.push(tick(1, 1_000, 100.0)).unwrap();
    fill.push(tick(2, 2_000, 101.0)).unwrap();
    fill.push(tick(3, 3_500, 102.0)).unwrap();

    let report = fill.finish(1_000_000_000).unwrap();

    assert!(report.complete);
    assert_eq!(report.unique_rows, 3);
    assert_eq!(report.id_range, Some((1, 3)));
}

#[test]
fn tick_fill_accumulator_rejects_id_gap() {
    let mut fill = tqsdk_data::BacktestTickFill::new("SHFE.rb2601", 1_000, 4_000);
    fill.push(tick(1, 1_000, 100.0)).unwrap();
    fill.push(tick(3, 3_500, 102.0)).unwrap();

    let report = fill.finish(1_000_000_000).unwrap();

    assert!(!report.complete);
    assert_eq!(report.gap_summary.as_deref(), Some("tick id range 1..=3 contains 2 unique rows"));
}
```

- [ ] **Step 3: Implement accumulator**

In `backtest_tick_cache.rs`, add exports in `lib.rs` after implementation:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestTickFillReport {
    pub symbol: String,
    pub requested_range: (i64, i64),
    pub unique_rows: usize,
    pub id_range: Option<(i64, i64)>,
    pub first_datetime_ns: Option<i64>,
    pub last_datetime_ns: Option<i64>,
    pub complete: bool,
    pub gap_summary: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BacktestTickFill {
    symbol: String,
    range_start_ns: i64,
    range_end_ns: i64,
    rows_by_id: std::collections::BTreeMap<i64, Tick>,
}

impl BacktestTickFill {
    pub fn new(symbol: impl Into<String>, range_start_ns: i64, range_end_ns: i64) -> Self {
        Self {
            symbol: symbol.into(),
            range_start_ns,
            range_end_ns,
            rows_by_id: std::collections::BTreeMap::new(),
        }
    }

    pub fn push(&mut self, row: Tick) -> Result<bool> {
        if row.datetime < self.range_start_ns || row.datetime >= self.range_end_ns {
            return Ok(false);
        }
        Ok(self.rows_by_id.insert(row.id, row).is_none())
    }

    pub fn drain_rows(&self) -> Vec<Tick> {
        self.rows_by_id.values().cloned().collect()
    }

    pub fn finish(&self, end_tolerance_ns: i64) -> Result<BacktestTickFillReport> {
        let first = self.rows_by_id.values().next();
        let last = self.rows_by_id.values().next_back();
        let id_range = first.zip(last).map(|(first, last)| (first.id, last.id));
        let unique_rows = self.rows_by_id.len();
        let first_datetime_ns = first.map(|row| row.datetime);
        let last_datetime_ns = last.map(|row| row.datetime);
        let mut complete = first.is_some();
        let mut gap_summary = None;
        if let Some((first_id, last_id)) = id_range {
            let expected = last_id.saturating_sub(first_id).saturating_add(1);
            if expected != unique_rows as i64 {
                complete = false;
                gap_summary = Some(format!(
                    "tick id range {first_id}..={last_id} contains {unique_rows} unique rows"
                ));
            }
        } else {
            complete = false;
        }
        if last_datetime_ns.is_none_or(|last_ns| {
            last_ns < self.range_end_ns.saturating_sub(end_tolerance_ns)
        }) {
            complete = false;
        }
        Ok(BacktestTickFillReport {
            symbol: self.symbol.clone(),
            requested_range: (self.range_start_ns, self.range_end_ns),
            unique_rows,
            id_range,
            first_datetime_ns,
            last_datetime_ns,
            complete,
            gap_summary,
        })
    }
}
```

- [ ] **Step 4: Export accumulator**

In `crates/tqsdk-data/src/lib.rs`, add:

```rust
BacktestTickFill, BacktestTickFillReport,
```

to the `backtest_tick_cache` export list.

- [ ] **Step 5: Verify**

Run:

```bash
rtk cargo test -p tqsdk-data --test history_series_single_file_store
```

Expected: PASS.

- [ ] **Step 6: Commit accumulator**

```bash
rtk git add crates/tqsdk-data/src/backtest_tick_cache.rs crates/tqsdk-data/src/lib.rs crates/tqsdk-data/tests/history_series_single_file_store.rs
rtk git commit -m "feat(data): add backtest tick fill integrity checks"
```

---

### Task 6: Move Universe Selector Semantics To `tqsdk-data`

**Files:**
- Modify: `crates/tqsdk-data/Cargo.toml`
- Modify: `crates/tqsdk-relay/Cargo.toml`
- Create: `crates/tqsdk-data/src/universe_expression.rs`
- Create: `crates/tqsdk-data/src/universe.rs`
- Modify: `crates/tqsdk-data/src/lib.rs`
- Modify: `crates/tqsdk-relay/src/universe.rs`
- Modify: `crates/tqsdk-relay/src/lib.rs`
- Modify: `crates/tqsdk-relay/tests/universe.rs`
- Create: `crates/tqsdk-data/tests/universe_selector.rs`

- [ ] **Step 1: Run impact analysis**

Run GitNexus impact analysis for `UniverseExpression`, `UniverseSelector`, and `resolve_futures_universe_symbols`.

- [ ] **Step 2: Add `tqsdk-data` dependency to relay**

In `crates/tqsdk-relay/Cargo.toml`, add:

```toml
tqsdk-data = { path = "../tqsdk-data", version = "0.1.0", default-features = false }
```

- [ ] **Step 3: Move expression parser**

Move `crates/tqsdk-relay/src/universe_expression.rs` to `crates/tqsdk-data/src/universe_expression.rs`.

Replace relay-specific result/error uses with data error:

```rust
use crate::error::{DataError, Result};

fn invalid_universe(message: impl Into<String>) -> DataError {
    DataError::Validation(message.into())
}
```

Change parser returns from `RelayResult<T>` to `Result<T>`, and change each `RelayError::invalid_config(...)` to `invalid_universe(...)`.

- [ ] **Step 4: Move shared resolver**

Create `crates/tqsdk-data/src/universe.rs` by moving the shared types and resolver functions from relay:

```rust
pub struct FuturesProductCode { /* existing fields */ }
pub struct FuturesContract { /* existing fields */ }
pub trait FuturesUniverseResolver { /* existing trait methods */ }
pub struct StaticFuturesUniverseResolver { /* existing fields */ }
pub async fn resolve_futures_universe_symbols<R>(...) -> Result<Vec<String>>
pub async fn resolve_futures_contracts_with_expression<R>(...) -> Result<Vec<FuturesContract>>
```

Move pure helper functions used by selector matching. Keep relay-only runtime/session bootstrap code in relay if it requires relay config.

- [ ] **Step 5: Export shared universe API**

In `crates/tqsdk-data/src/lib.rs`, add:

```rust
mod universe;
mod universe_expression;

pub use universe::{
    FuturesContract, FuturesProductCode, FuturesUniverseResolver, StaticFuturesUniverseResolver,
    resolve_futures_contracts_with_expression, resolve_futures_universe_symbols,
};
pub use universe_expression::{
    UniverseClause, UniverseExpression, UniverseSelector, UniverseSelectorKind,
};
```

- [ ] **Step 6: Make relay import shared types**

In `crates/tqsdk-relay/src/universe.rs`, replace:

```rust
use crate::universe_expression::{
    UniverseClause, UniverseExpression, UniverseSelector, UniverseSelectorKind,
};
```

with:

```rust
pub use tqsdk_data::{
    FuturesContract, FuturesProductCode, FuturesUniverseResolver, StaticFuturesUniverseResolver,
    resolve_futures_contracts_with_expression, resolve_futures_universe_symbols,
};
use tqsdk_data::{UniverseClause, UniverseExpression, UniverseSelector, UniverseSelectorKind};
```

For relay code paths that need `RelayResult`, map data errors explicitly:

```rust
fn relay_data_error(error: tqsdk_data::DataError) -> RelayError {
    RelayError::invalid_config(error.to_string())
}
```

- [ ] **Step 7: Add shared selector tests**

Create `crates/tqsdk-data/tests/universe_selector.rs` with parser and resolver tests copied from relay expectations:

```rust
use tqsdk_data::{
    FuturesContract, StaticFuturesUniverseResolver, UniverseExpression,
    resolve_futures_universe_symbols,
};

#[tokio::test]
async fn selector_matches_relay_expression_semantics() {
    let expression = UniverseExpression::parse("active:all;!CFFEX").unwrap();
    let mut resolver = StaticFuturesUniverseResolver::new([
        FuturesContract::new("SHFE.rb2601", "SHFE", "rb", false).unwrap(),
        FuturesContract::new("CFFEX.IF2601", "CFFEX", "IF", false).unwrap(),
    ]);

    let symbols = resolve_futures_universe_symbols(&expression, &mut resolver)
        .await
        .unwrap();

    assert_eq!(symbols, vec!["SHFE.rb2601"]);
}
```

- [ ] **Step 8: Verify relay parity**

Run:

```bash
rtk cargo test -p tqsdk-data --test universe_selector
rtk cargo test -p tqsdk-relay --test universe
```

Expected: PASS.

- [ ] **Step 9: Commit shared universe selector**

```bash
rtk git add crates/tqsdk-data/Cargo.toml crates/tqsdk-relay/Cargo.toml crates/tqsdk-data/src/lib.rs crates/tqsdk-data/src/universe.rs crates/tqsdk-data/src/universe_expression.rs crates/tqsdk-data/tests/universe_selector.rs crates/tqsdk-relay/src/universe.rs crates/tqsdk-relay/src/lib.rs crates/tqsdk-relay/tests/universe.rs
rtk git commit -m "feat(data): share futures universe selector with relay"
```

---

### Task 7: Collapse Facade Backtest API Around Cache Policy

**Files:**
- Modify: `crates/tqsdk/src/lib.rs`
- Modify: `crates/tqsdk/tests/facade_contract.rs`
- Modify: `crates/tqsdk/examples/api_contract_s43_facade_backtest_history_cache.rs`

- [ ] **Step 1: Run impact analysis**

Run GitNexus impact analysis for `BacktestBuilder`, `PreparedBacktest`, `TqBuilder::backtest`, and `TqBuilder::replay_backtest`.

- [ ] **Step 2: Add failing facade tests**

In `facade_contract.rs`, add:

```rust
#[tokio::test]
async fn facade_backtest_remote_on_miss_requires_auth_only_when_cache_missing() {
    let symbol = "SHFE.rb2501";
    let cache_dir = temp_cache_dir();
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    cache
        .store_ticks(symbol, 1_000, 3_000, [tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)])
        .unwrap();

    let prepared = Tq::futures()
        .backtest(1_000, 3_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol(symbol)
        .cache(BacktestCachePolicy::RemoteOnMiss)
        .prepare()
        .await
        .unwrap();
    assert!(!prepared.data_report().remote_used);

    let missing = Tq::futures()
        .backtest(1_000, 4_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol(symbol)
        .cache(BacktestCachePolicy::RemoteOnMiss)
        .prepare()
        .await
        .unwrap_err();
    assert!(missing.to_string().contains("remote backtest cache fill requires auth"));
}
```

- [ ] **Step 3: Change builder cache methods**

In `BacktestBuilder`, replace the current `cache(self, BacktestTickCache)` with:

```rust
#[must_use]
pub fn cache(mut self, policy: BacktestCachePolicy) -> Self {
    self.cache_policy = policy;
    self
}

#[must_use]
pub fn cache_store(mut self, cache: tqsdk_data::BacktestTickCache) -> Self {
    self.cache = Some(cache);
    self
}
```

Keep `cache_dir` as the primary user path:

```rust
pub fn cache_dir(mut self, root_dir: impl AsRef<std::path::Path>) -> Result<Self> {
    self.cache = Some(tqsdk_data::BacktestTickCache::open(root_dir)?);
    Ok(self)
}
```

- [ ] **Step 4: Update policy methods**

Replace `refresh_missing` and `refresh_all` with:

```rust
#[must_use]
pub fn disabled_cache(self) -> Self {
    self.cache(BacktestCachePolicy::Disabled)
}

#[must_use]
pub fn cache_only(self) -> Self {
    self.cache(BacktestCachePolicy::CacheOnly)
}

#[must_use]
pub fn remote_on_miss(self) -> Self {
    self.cache(BacktestCachePolicy::RemoteOnMiss)
}

#[must_use]
pub fn refresh(self) -> Self {
    self.cache(BacktestCachePolicy::Refresh)
}
```

- [ ] **Step 5: Make auth lazy in `prepare`**

In `BacktestBuilder::prepare`, compute coverage first. Only check `self.base.auth` when missing ranges exist and the policy is `RemoteOnMiss` or `Refresh`:

```rust
let needs_remote = match self.cache_policy {
    BacktestCachePolicy::Disabled | BacktestCachePolicy::Refresh => true,
    BacktestCachePolicy::RemoteOnMiss => missing_symbols.iter().any(|entry| !entry.missing_ranges.is_empty()),
    BacktestCachePolicy::CacheOnly => false,
};
if needs_remote && self.base.auth.is_none() {
    return Err(data_validation("remote backtest cache fill requires auth"));
}
```

- [ ] **Step 6: Verify facade API tests**

Run:

```bash
rtk cargo test -p tqsdk --test facade_contract
```

Expected: PASS after updating old `.cache(...)` call sites to `.cache_store(...)` or `.cache(BacktestCachePolicy::...)`.

- [ ] **Step 7: Commit API collapse**

```bash
rtk git add crates/tqsdk/src/lib.rs crates/tqsdk/tests/facade_contract.rs crates/tqsdk/examples/api_contract_s43_facade_backtest_history_cache.rs
rtk git commit -m "feat(tqsdk): collapse backtest cache policy API"
```

---

### Task 8: Add Async Backtest Market Stream Abstraction

**Files:**
- Create: `crates/tqsdk-task/src/backtest_stream.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`
- Modify: `crates/tqsdk-task/src/backtest.rs`
- Modify: `crates/tqsdk-task/src/replay.rs`
- Modify: `crates/tqsdk-task/src/error.rs`
- Modify: `crates/tqsdk-task/tests/strategy_backtest.rs`

- [ ] **Step 1: Run impact analysis**

Run GitNexus impact analysis for `StrategyBacktest`, `StrategyBacktestBuilder`, `ReplayMarketSource`, and `TaskError`.

- [ ] **Step 2: Add an owned external error variant**

In `crates/tqsdk-task/src/error.rs`, add this variant to `TaskError`:

```rust
External(String),
```

Add the display arm:

```rust
Self::External(message) => write!(f, "{message}"),
```

Add it to the no-source branch in `impl std::error::Error for TaskError`:

```rust
Self::External(_)
```

- [ ] **Step 3: Add stream trait**

Create `crates/tqsdk-task/src/backtest_stream.rs`:

```rust
use std::future::Future;
use std::pin::Pin;

use crate::replay::{ReplayMarketEvent, ReplayMarketSource};
use crate::Result;

pub trait BacktestMarketStream {
    fn next_event<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ReplayMarketEvent>>> + 'a>>;
}

#[derive(Debug)]
pub struct ReplayMarketStream {
    source: ReplayMarketSource,
}

impl ReplayMarketStream {
    #[must_use]
    pub fn new(source: ReplayMarketSource) -> Self {
        Self { source }
    }
}

impl BacktestMarketStream for ReplayMarketStream {
    fn next_event<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ReplayMarketEvent>>> + 'a>> {
        Box::pin(async move { Ok(self.source.next()) })
    }
}
```

In `lib.rs`, add:

```rust
mod backtest_stream;
pub use backtest_stream::{BacktestMarketStream, ReplayMarketStream};
```

- [ ] **Step 4: Expose `ReplayMarketSource::next` to the stream module**

If `ReplayMarketSource::next` is private, change it in `replay.rs` to:

```rust
pub fn next(&mut self) -> Option<ReplayMarketEvent> {
    let event = self.events.get(self.index).cloned();
    if event.is_some() {
        self.index += 1;
    }
    event
}
```

- [ ] **Step 5: Make `StrategyBacktest` consume a stream**

In `backtest.rs`, change the replay field type:

```rust
replay: Box<dyn BacktestMarketStream>,
```

Change `StrategyBacktest::builder` to wrap the existing source:

```rust
pub fn builder(replay: ReplayMarketSource) -> StrategyBacktestBuilder {
    StrategyBacktestBuilder::new(Box::new(ReplayMarketStream::new(replay)))
}

pub fn builder_from_stream(stream: Box<dyn BacktestMarketStream>) -> StrategyBacktestBuilder {
    StrategyBacktestBuilder::new(stream)
}
```

Change `StrategyBacktestBuilder::new` to accept `Box<dyn BacktestMarketStream>`.

In `StrategyBacktest::next`, replace:

```rust
let Some(event) = self.replay.next() else {
```

with:

```rust
let Some(event) = self.replay.next_event().await? else {
```

- [ ] **Step 6: Verify existing task tests**

Run:

```bash
rtk cargo test -p tqsdk-task --test strategy_backtest
rtk cargo test -p tqsdk-task --test strategy_replay
```

Expected: PASS.

- [ ] **Step 7: Commit stream abstraction**

```bash
rtk git add crates/tqsdk-task/src/backtest_stream.rs crates/tqsdk-task/src/lib.rs crates/tqsdk-task/src/backtest.rs crates/tqsdk-task/src/replay.rs crates/tqsdk-task/src/error.rs crates/tqsdk-task/tests/strategy_backtest.rs
rtk git commit -m "feat(task): add streaming backtest market source"
```

---

### Task 9: Add Bounded Cached Tick Replay Merge

**Files:**
- Create: `crates/tqsdk-task/src/history_tick_replay.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`
- Modify: `crates/tqsdk/src/local_backtest.rs`
- Modify: `crates/tqsdk/src/lib.rs`
- Create: `crates/tqsdk-task/tests/history_tick_replay.rs`

- [ ] **Step 1: Run impact analysis**

Run GitNexus impact analysis for `HistorySeriesReader`, `StrategyBacktest::builder_from_stream`, and `PreparedBacktest::connect`.

- [ ] **Step 2: Add cached replay tests**

Create `crates/tqsdk-task/tests/history_tick_replay.rs`:

```rust
use tqsdk_core::Tick;
use tqsdk_data::{BacktestTickCache, TickDataSeriesRequest};
use tqsdk_task::{BacktestMarketStream, HistoryTickReplayStream, ReplayMarketPayload};

#[tokio::test]
async fn history_tick_replay_merges_symbols_by_datetime_and_tick_id() {
    let dir = temp_dir("history-tick-replay");
    let cache = BacktestTickCache::open(&dir).unwrap();
    cache
        .store_ticks("SHFE.rb2601", 1_000, 4_000, [tick(2, 2_000, 102.0)])
        .unwrap();
    cache
        .store_ticks("DCE.i2601", 1_000, 4_000, [tick(1, 1_000, 101.0)])
        .unwrap();

    let mut stream = HistoryTickReplayStream::new(
        cache.history_cache().clone(),
        [
            TickDataSeriesRequest::new("SHFE.rb2601", 1_000, 4_000),
            TickDataSeriesRequest::new("DCE.i2601", 1_000, 4_000),
        ],
    )
    .unwrap();

    let first = stream.next_event().await.unwrap().unwrap();
    let second = stream.next_event().await.unwrap().unwrap();
    assert_eq!(first.symbol(), "DCE.i2601");
    assert_eq!(second.symbol(), "SHFE.rb2601");
    assert!(matches!(first.payload(), ReplayMarketPayload::Tick(_)));
    assert!(stream.next_event().await.unwrap().is_none());
}

fn tick(id: i64, datetime: i64, last_price: f64) -> Tick {
    Tick {
        id,
        datetime,
        last_price,
        ..Tick::default()
    }
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push(format!("tqsdk-history-tick-replay-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
```

- [ ] **Step 3: Implement heap merge stream**

Create `crates/tqsdk-task/src/history_tick_replay.rs`:

```rust
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::future::Future;
use std::pin::Pin;

use tqsdk_data::{HistorySeriesCache, HistorySeriesReadRequest, HistorySeriesRow, TickDataSeriesRequest};

use crate::{BacktestMarketStream, ReplayMarketEvent, Result, TaskError};

pub struct HistoryTickReplayStream {
    readers: Vec<Box<dyn tqsdk_data::HistorySeriesReader>>,
    heap: BinaryHeap<HeapItem>,
}

#[derive(Debug, Clone)]
struct HeapItem {
    reader_index: usize,
    symbol: String,
    tick: tqsdk_core::Tick,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .tick
            .datetime
            .cmp(&self.tick.datetime)
            .then_with(|| other.symbol.cmp(&self.symbol))
            .then_with(|| other.tick.id.cmp(&self.tick.id))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.reader_index == other.reader_index && self.tick.id == other.tick.id
    }
}

impl Eq for HeapItem {}
```

Implement `new` by opening one reader per request and pushing the first row from each reader. Implement `next_event` by popping the heap, pushing that reader's next row, and returning:

```rust
ReplayMarketEvent::tick("history-cache", item.symbol, item.tick.datetime, Some(item.tick.datetime), item.tick)
    .map_err(TaskError::from)
```

- [ ] **Step 4: Export stream**

In `crates/tqsdk-task/src/lib.rs`, add:

```rust
mod history_tick_replay;
pub use history_tick_replay::HistoryTickReplayStream;
```

- [ ] **Step 5: Wire cache hits through streaming replay**

In `PreparedBacktest::connect` in `crates/tqsdk/src/lib.rs`, replace the `Vec<TickDataSeries>` loading loop with:

```rust
let requests = symbols
    .iter()
    .map(|symbol| tqsdk_data::TickDataSeriesRequest::new(symbol, start_ns, end_ns))
    .collect::<Vec<_>>();
let stream = tqsdk_task::HistoryTickReplayStream::new(cache.history_cache().clone(), requests)?;
base.replay_backtest_stream(Box::new(stream)).connect().await
```

Add `TqBuilder::replay_backtest_stream`:

```rust
pub fn replay_backtest_stream(
    mut self,
    stream: Box<dyn tqsdk_task::BacktestMarketStream>,
) -> Self {
    self.backtest = Some(BacktestConfig::LocalStream { stream });
    self
}
```

Add the `BacktestConfig` variant:

```rust
LocalStream {
    stream: Box<dyn tqsdk_task::BacktestMarketStream>,
},
```

Update the manual `Debug` implementation:

```rust
Self::LocalStream { .. } => f.debug_struct("LocalStream").finish_non_exhaustive(),
```

Update `BacktestConfig::is_server_side`:

```rust
Self::Local { .. } | Self::LocalStream { .. } => false,
```

Update `TqBuilder::connect`:

```rust
match backtest {
    Some(BacktestConfig::Local { replay }) => local_backtest_recipe.connect(replay).await,
    Some(BacktestConfig::LocalStream { stream }) => {
        local_backtest_recipe.connect_stream(stream).await
    }
    backtest => {
        // keep existing live/server connection branch
    }
}
```

Add `LocalBacktestRecipe::connect_stream` in `crates/tqsdk/src/local_backtest.rs`:

```rust
pub(super) async fn connect_stream(
    self,
    stream: Box<dyn tqsdk_task::BacktestMarketStream>,
) -> Result<Tq> {
    let mut builder = StrategyBacktest::builder_from_stream(stream);
    if let Some(default_price_tick) = self.default_price_tick {
        builder = builder.default_price_tick(default_price_tick);
    }
    builder = builder.instrument_specs(self.instrument_specs);
    for symbol in &self.quote_symbols {
        builder = builder.quote(symbol);
    }
    for (symbol, tick) in &self.price_ticks {
        builder = builder.price_tick(symbol, *tick);
    }
    let backtest = builder.build().await?;
    Ok(Tq::from_local_backtest(backtest))
}
```

- [ ] **Step 6: Verify**

Run:

```bash
rtk cargo test -p tqsdk-task --test history_tick_replay
rtk cargo test -p tqsdk --test facade_contract
```

Expected: PASS.

- [ ] **Step 7: Commit cached streaming replay**

```bash
rtk git add crates/tqsdk-task/src/history_tick_replay.rs crates/tqsdk-task/src/lib.rs crates/tqsdk-task/tests/history_tick_replay.rs crates/tqsdk/src/local_backtest.rs crates/tqsdk/src/lib.rs
rtk git commit -m "feat(task): stream cached tick replay"
```

---

### Task 10: Implement Remote-On-Miss Caching Stream

**Files:**
- Create: `crates/tqsdk/src/backtest_remote.rs`
- Modify: `crates/tqsdk/src/lib.rs`
- Modify: `crates/tqsdk/tests/facade_contract.rs`

- [ ] **Step 1: Run impact analysis**

Run GitNexus impact analysis for `BacktestBuilder::prepare`, `TqBuilder::connect`, and `Tq::next`.

- [ ] **Step 2: Add a non-network test for missing auth and report shape**

In `facade_contract.rs`, add:

```rust
#[tokio::test]
async fn remote_on_miss_report_lists_missing_symbol_ranges() {
    let cache_dir = temp_cache_dir();
    let result = Tq::futures()
        .backtest(1_000, 4_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol("SHFE.rb2601")
        .cache(BacktestCachePolicy::RemoteOnMiss)
        .prepare()
        .await;

    let err = result.unwrap_err();
    assert!(err.to_string().contains("remote backtest cache fill requires auth"));
}
```

- [ ] **Step 3: Add remote stream module**

Create `crates/tqsdk/src/backtest_remote.rs`:

```rust
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use tqsdk_data::{BacktestTickCache, BacktestTickFill};
use tqsdk_task::{BacktestMarketStream, ReplayMarketEvent};

use crate::{Error, Result};

const REMOTE_TICK_DATA_LENGTH: usize = 10_000;
const REMOTE_FILL_END_TOLERANCE_NS: i64 = 1_000_000_000;

pub(crate) struct RemoteBacktestCachingStream {
    api: tqsdk_wait::TqApi,
    handles: BTreeMap<String, tqsdk_wait::TickHandle>,
    cache: BacktestTickCache,
    fills: BTreeMap<String, BacktestTickFill>,
    pending: VecDeque<ReplayMarketEvent>,
    finalized: bool,
}
```

Add constructor:

```rust
impl RemoteBacktestCachingStream {
    pub(crate) async fn connect(
        user: String,
        pass: String,
        start_ns: i64,
        end_ns: i64,
        symbols: Vec<String>,
        cache: BacktestTickCache,
    ) -> Result<Self> {
        let mut api = tqsdk_wait::TqApiBuilder::new(user, pass)
            .futures_backtest(start_ns, end_ns)?
            .build()
            .await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut handles = BTreeMap::new();
        let mut fills = BTreeMap::new();
        for symbol in symbols {
            let handle = api
                .tick_ready(&symbol, REMOTE_TICK_DATA_LENGTH, Some(deadline))
                .await?;
            fills.insert(symbol.clone(), BacktestTickFill::new(symbol.clone(), start_ns, end_ns));
            handles.insert(symbol, handle);
        }
        Ok(Self {
            api,
            handles,
            cache,
            fills,
            pending: VecDeque::new(),
            finalized: false,
        })
    }
}
```

Implement `BacktestMarketStream`:

```rust
impl BacktestMarketStream for RemoteBacktestCachingStream {
    fn next_event<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = tqsdk_task::Result<Option<ReplayMarketEvent>>> + 'a>> {
        Box::pin(async move {
            self.next_remote_event()
                .await
                .map_err(|error| tqsdk_task::TaskError::External(error.to_string()))
        })
    }
}
```

- [ ] **Step 4: Implement remote loop without history APIs**

In `RemoteBacktestCachingStream`, implement:

```rust
async fn next_remote_event(&mut self) -> Result<Option<ReplayMarketEvent>> {
    if let Some(event) = self.pending.pop_front() {
        return Ok(Some(event));
    }
    while let Some(step) = self.api.step_until(None).await? {
        for (symbol, handle) in &self.handles {
            if !step.is_changing(handle) {
                continue;
            }
            for row in handle.changed_rows(&step)? {
                let Some(fill) = self.fills.get_mut(symbol) else {
                    continue;
                };
                if !fill.push(row.clone())? {
                    continue;
                }
                self.cache.append_partial_ticks(symbol, [row.clone()])?;
                self.pending.push_back(ReplayMarketEvent::tick(
                    "server-backtest",
                    symbol,
                    row.datetime,
                    Some(row.datetime),
                    row,
                )?);
            }
        }
        if let Some(event) = self.pending.pop_front() {
            return Ok(Some(event));
        }
    }
    self.finalize_cache()?;
    Ok(None)
}
```

Implement `finalize_cache`:

```rust
fn finalize_cache(&mut self) -> Result<()> {
    if self.finalized {
        return Ok(());
    }
    self.finalized = true;
    for (symbol, fill) in &self.fills {
        let report = fill.finish(REMOTE_FILL_END_TOLERANCE_NS)?;
        if report.complete {
            self.cache.mark_complete(
                symbol,
                report.requested_range.0,
                report.requested_range.1,
                report.unique_rows,
                report.id_range,
            )?;
        } else {
            return Err(crate::data_validation(format!(
                "incomplete remote backtest cache fill for {symbol}: {:?}",
                report.gap_summary
            )));
        }
    }
    Ok(())
}
```

- [ ] **Step 5: Wire remote branch in builder**

In `BacktestBuilder::prepare`, return an internal enum:

```rust
enum PreparedBacktestMode {
    CacheHit,
    RemoteCaching { symbols: Vec<String> },
}
```

Store it in `PreparedBacktest`.

In `PreparedBacktest::connect`, when mode is `RemoteCaching`, build:

```rust
let auth = base.auth.clone().ok_or(Error::MissingAuth)?;
let stream = backtest_remote::RemoteBacktestCachingStream::connect(
    auth.user,
    auth.pass,
    start_ns,
    end_ns,
    symbols,
    cache,
)
.await?;
base.replay_backtest_stream(Box::new(stream)).connect().await
```

- [ ] **Step 6: Add ignored live smoke for real remote fill**

In `facade_contract.rs`, add:

```rust
#[tokio::test]
#[ignore = "requires TQ_AUTH_USER/TQ_AUTH_PASS and remote backtest service"]
async fn facade_backtest_remote_on_miss_live_smoke() {
    let user = std::env::var("TQ_AUTH_USER").unwrap();
    let pass = std::env::var("TQ_AUTH_PASS").unwrap();
    let cache_dir = temp_cache_dir();
    let symbol = "SHFE.au2608";
    let start_ns = 1_781_172_000_000_000_000;
    let end_ns = 1_781_258_401_000_000_000;

    let mut tq = Tq::futures()
        .auth(user, pass)
        .backtest(start_ns, end_ns)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol(symbol)
        .cache(BacktestCachePolicy::RemoteOnMiss)
        .connect()
        .await
        .unwrap();
    let quote = tq.quote(symbol).await.unwrap();
    assert!(tq.next().await.unwrap());
    assert!(quote.load().unwrap().last_price.is_finite());
}
```

- [ ] **Step 7: Verify non-live tests**

Run:

```bash
rtk cargo test -p tqsdk --test facade_contract
```

Expected: PASS; ignored live smoke is not run.

- [ ] **Step 8: Commit remote stream**

```bash
rtk git add crates/tqsdk/src/backtest_remote.rs crates/tqsdk/src/lib.rs crates/tqsdk/tests/facade_contract.rs
rtk git commit -m "feat(tqsdk): fill backtest cache from server-side stream"
```

---

### Task 11: Connect Full-Universe Selection To Backtest Builder

**Files:**
- Modify: `crates/tqsdk/src/lib.rs`
- Modify: `crates/tqsdk/tests/facade_contract.rs`
- Modify: `crates/tqsdk/examples/api_contract_s43_facade_backtest_history_cache.rs`
- Create: `crates/tqsdk/examples/api_contract_s44_facade_backtest_remote_on_miss.rs`

- [ ] **Step 1: Run impact analysis**

Run GitNexus impact analysis for `BacktestBuilder::symbol`, `BacktestBuilder::prepare`, and `resolve_futures_universe_symbols`.

- [ ] **Step 2: Add builder universe state**

In `BacktestBuilder`, replace `symbols: Vec<String>` with:

```rust
symbols: Vec<String>,
universe_expression: Option<tqsdk_data::UniverseExpression>,
```

Keep `.symbol(...)` for explicit single-symbol tests.

Add:

```rust
pub fn universe(mut self, expression: impl AsRef<str>) -> Result<Self> {
    self.universe_expression = Some(tqsdk_data::UniverseExpression::parse(expression.as_ref())?);
    Ok(self)
}
```

- [ ] **Step 3: Resolve universe in prepare**

In `prepare`, if `universe_expression` is set, resolve it before coverage checks:

```rust
if let Some(expression) = &self.universe_expression {
    let mut resolver = tqsdk_data::StaticFuturesUniverseResolver::new(Vec::new());
    let resolved = tqsdk_data::resolve_futures_universe_symbols(expression, &mut resolver).await?;
    for symbol in resolved {
        if !self.symbols.iter().any(|existing| existing == &symbol) {
            self.symbols.push(symbol);
        }
    }
}
```

For dynamic selectors such as `active:all`, use a session-backed resolver when auth is present. Add a helper:

```rust
async fn resolve_backtest_universe(
    expression: &tqsdk_data::UniverseExpression,
    auth: Option<&Auth>,
) -> Result<Vec<String>> {
    if expression.is_static_symbol_only() {
        let mut resolver = tqsdk_data::StaticFuturesUniverseResolver::new(Vec::new());
        return Ok(tqsdk_data::resolve_futures_universe_symbols(expression, &mut resolver).await?);
    }
    let auth = auth.ok_or(Error::MissingAuth)?;
    let client = tqsdk_session::SessionClientBuilder::new(auth.user.clone(), auth.pass.clone())
        .enable_query()
        .build()?;
    let mut resolver = tqsdk_data::SessionFuturesUniverseResolver::new(client);
    Ok(tqsdk_data::resolve_futures_universe_symbols(expression, &mut resolver).await?)
}
```

If `SessionFuturesUniverseResolver` is not moved in Task 6, move it now with its tests.

- [ ] **Step 4: Add facade tests for static universe**

In `facade_contract.rs`, add:

```rust
#[tokio::test]
async fn facade_backtest_universe_accepts_static_selector_expression() {
    let symbol = "SHFE.rb2501";
    let cache_dir = temp_cache_dir();
    BacktestTickCache::open(&cache_dir)
        .unwrap()
        .store_ticks(symbol, 1_000, 3_000, [tick(1, 1_000, 100.0)])
        .unwrap();

    let prepared = Tq::futures()
        .backtest(1_000, 3_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .universe("symbol:SHFE.rb2501")
        .unwrap()
        .cache_only()
        .prepare()
        .await
        .unwrap();

    assert_eq!(prepared.data_report().resolved_symbols, 1);
}
```

- [ ] **Step 5: Add contract example**

Create `crates/tqsdk/examples/api_contract_s44_facade_backtest_remote_on_miss.rs`:

```rust
use tqsdk::prelude::*;

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    let start_ns = 1_781_172_000_000_000_000;
    let end_ns = 1_781_258_401_000_000_000;
    let mut tq = Tq::futures()
        .auth_env()?
        .backtest(start_ns, end_ns)
        .cache_dir(".tqsdk/backtest_ticks")?
        .universe("symbol:SHFE.au2608")?
        .remote_on_miss()
        .connect()
        .await?;

    let quote = tq.quote("SHFE.au2608").await?;
    while tq.next().await? {
        let _last_price = quote.load()?.last_price;
    }
    Ok(())
}
```

- [ ] **Step 6: Verify**

Run:

```bash
rtk cargo test -p tqsdk --test facade_contract
rtk cargo check -p tqsdk --example api_contract_s44_facade_backtest_remote_on_miss
```

Expected: PASS.

- [ ] **Step 7: Commit universe builder**

```bash
rtk git add crates/tqsdk/src/lib.rs crates/tqsdk/tests/facade_contract.rs crates/tqsdk/examples/api_contract_s43_facade_backtest_history_cache.rs crates/tqsdk/examples/api_contract_s44_facade_backtest_remote_on_miss.rs
rtk git commit -m "feat(tqsdk): support shared universe backtests"
```

---

### Task 12: Update Docs And Architecture Contracts

**Files:**
- Modify: `README.md`
- Modify: `crates/tqsdk/README.md`
- Modify: `crates/tqsdk-data/README.md`
- Modify: `docs/architecture/README.md`
- Modify: `docs/architecture/crate-boundaries.md`
- Modify: `docs/architecture/validation.md`

- [ ] **Step 1: Read architecture docs**

Run:

```bash
rtk read docs/architecture/README.md
rtk read docs/architecture/crate-boundaries.md
rtk read docs/architecture/validation.md
```

- [ ] **Step 2: Update README backtest section**

Document this user-facing shape:

```rust
let mut tq = Tq::futures()
    .auth_env()?
    .backtest(start_ns, end_ns)
    .cache_dir(".tqsdk/backtest_ticks")?
    .universe("active:all;!CFFEX")?
    .remote_on_miss()
    .connect()
    .await?;
```

State explicitly:

```text
Backtest cache fills use official server-side backtest market streams. They do
not require professional history download permissions. Cache hits do not require
auth.
```

- [ ] **Step 3: Update crate boundary docs**

Add these ownership statements:

```text
tqsdk-data owns HistorySeriesCache, HistorySeriesStore backends, the default
single-file backtest cache store, backtest tick coverage checks, and shared
futures universe selector semantics.

tqsdk-task owns streaming local backtest execution and bounded tick replay.

tqsdk owns the Python-style backtest builder, cache policy selection, lazy auth,
and remote-on-miss stream wiring through tqsdk-wait.
```

- [ ] **Step 4: Update validation matrix**

Add commands:

```bash
rtk cargo test -p tqsdk-data --test history_series_single_file_store
rtk cargo test -p tqsdk-data --test universe_selector
rtk cargo test -p tqsdk-task --test history_tick_replay
rtk cargo test -p tqsdk --test facade_contract
rtk cargo check -p tqsdk --example api_contract_s44_facade_backtest_remote_on_miss
```

- [ ] **Step 5: Verify docs formatting**

Run:

```bash
rtk git diff --check
```

Expected: PASS.

- [ ] **Step 6: Commit docs**

```bash
rtk git add README.md crates/tqsdk/README.md crates/tqsdk-data/README.md docs/architecture/README.md docs/architecture/crate-boundaries.md docs/architecture/validation.md
rtk git commit -m "docs: document cache-backed backtest flow"
```

---

### Task 13: Full Verification And Cleanup

**Files:**
- Modify only files required by failing verification.

- [ ] **Step 1: Run formatting**

```bash
rtk cargo fmt --all --check
```

Expected: PASS.

- [ ] **Step 2: Run focused tests**

```bash
rtk cargo test -p tqsdk-data --test history_series_single_file_store
rtk cargo test -p tqsdk-data --test history_series_cache
rtk cargo test -p tqsdk-data --test universe_selector
rtk cargo test -p tqsdk-task --test history_tick_replay
rtk cargo test -p tqsdk-task --test strategy_backtest
rtk cargo test -p tqsdk --test facade_contract
```

Expected: PASS.

- [ ] **Step 3: Run feature matrix**

```bash
rtk cargo check --no-default-features
rtk cargo check --no-default-features --examples
rtk cargo check --all-features --examples
```

Expected: PASS.

- [ ] **Step 4: Run full workspace tests**

```bash
rtk cargo test
```

Expected: PASS with existing ignored tests still ignored.

- [ ] **Step 5: Run clippy**

```bash
rtk cargo clippy --examples --all-targets -- -D warnings
```

Expected: PASS.

- [ ] **Step 6: Run GitNexus detect changes**

Run GitNexus `detect_changes` with scope `all`.

Expected: changed symbols and flows match the backtest/cache/universe/replay scope.

- [ ] **Step 7: Commit verification fixes**

If any verification fixes were needed:

```bash
rtk git add <changed-files>
rtk git commit -m "fix: satisfy backtest cache verification"
```

If no fixes were needed, do not create an empty commit.

---

## Acceptance Mapping

- Single final file per `(symbol, period)`: Tasks 1-4.
- Coverage metadata inside the same series file: Tasks 2-4.
- `HistorySeriesCache` remains the abstraction: Tasks 2-4.
- First miss uses official server-side backtest stream: Task 10.
- No professional history API for cache fill: Task 10 and docs in Task 12.
- Second run can use cache without auth: Tasks 7 and 10.
- Full-universe selector matches relay: Tasks 6 and 11.
- Full-universe replay avoids full vector materialization: Tasks 8 and 9.
- `local_backtest` user concept collapses into `backtest(...).cache(...)`: Task 7 and docs in Task 12.
- Klines derive from ticks: preserved by using tick replay as the only cache-backed backtest source in Tasks 9-11.
