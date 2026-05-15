# Live History Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix Kline/Tick live row projection correctness and add explicit live stream writes into the Python-compatible history series cache.

**Architecture:** Keep all live state reads on the existing runtime state tree. Extend `HistorySeriesCache` instead of adding a manifest or separate cache layer, preserving `symbol.duration_ns.start_id.end_id` mmap files. Add a narrow `tqsdk-data` stream-feature bridge that users opt into explicitly.

**Tech Stack:** Rust 2024, Cargo workspace, `tqsdk-core`, `tqsdk-stream`, `tqsdk-wait`, `tqsdk-data`, `memmap2`, existing `DataError`.

---

## File Structure

- Modify `crates/tqsdk-core/src/adapter/common.rs`: inject row `id` fields during diff flattening.
- Modify `crates/tqsdk-core/tests/runtime_contract_adapters.rs`: regression tests for Kline/Tick row id injection.
- Modify `crates/tqsdk-stream/src/window.rs`: read chart bounds and project only bounded rows.
- Modify `crates/tqsdk-stream/tests/stream_typed.rs`: window projection regression tests.
- Modify `crates/tqsdk-wait/src/refs/kline.rs`: Kline serial `load()` respects chart bounds.
- Modify `crates/tqsdk-wait/src/refs/tick.rs`: Tick serial `load()` respects chart bounds.
- Modify `crates/tqsdk-wait/tests/wait_facade.rs`: wait serial projection regression tests.
- Modify `crates/tqsdk-data/src/history_series_cache.rs`: append and latest-read APIs.
- Modify `crates/tqsdk-data/src/history_series_cache/ranges.rs`: reuse or add id dedup helpers if needed.
- Modify `crates/tqsdk-data/src/history_series_cache/storage.rs`: add row reads by id/index helpers if needed.
- Create `crates/tqsdk-data/src/live_history_cache.rs`: `LiveHistoryCacheWriter` and reports.
- Modify `crates/tqsdk-data/src/lib.rs`: always export `HistorySeriesCache` append/latest methods through the existing type, and export `LiveHistoryCacheWriter` types behind the `stream` feature.
- Modify `crates/tqsdk-data/tests/history_series_cache.rs`: append/latest tests.
- Create `crates/tqsdk-data/tests/live_history_cache.rs`: stream-feature live writer tests.
- Modify `crates/tqsdk-data/README.md`, `crates/tqsdk-data/src/lib.rs`, `docs/architecture/api-data.md`: document new opt-in bridge.
- Optionally modify `crates/tqsdk-stream/README.md` and `crates/tqsdk-wait/README.md`: mention chart-bound serial windows.

## Task 1: Core Row ID Normalization

**Files:**
- Modify: `crates/tqsdk-core/tests/runtime_contract_adapters.rs`
- Modify: `crates/tqsdk-core/src/adapter/common.rs`

- [ ] **Step 1: Write failing Kline/Tick adapter tests**

Add two tests near existing market diff adapter tests:

```rust
#[test]
fn market_diff_injects_kline_row_id_from_data_key() {
    let mut adapter = MarketAdapter::default();
    let mutations = adapter
        .decode(&RuntimeInput::Io(IoEvent::InboundFrame {
            route: Default::default(),
            frame: InboundFrame::Text(
                json!({
                    "aid": "rtn_data",
                    "data": [{
                        "klines": {
                            "SHFE.au2602": {
                                "60000000000": {
                                    "data": {
                                        "42": {
                                            "datetime": 1,
                                            "close": 612.0
                                        }
                                    }
                                }
                            }
                        }
                    }]
                })
                .to_string(),
            ),
        }))
        .expect("market diff should decode");

    let row = mutations
        .iter()
        .find(|mutation| mutation.path.as_slice() == ["klines", "SHFE.au2602", "60000000000", "data", "42"])
        .expect("kline row mutation should exist");
    assert!(row.fields.iter().any(|field| field.field == "id" && field.value == json!(42)));
}

#[test]
fn market_diff_injects_tick_row_id_from_data_key() {
    let mut adapter = MarketAdapter::default();
    let mutations = adapter
        .decode(&RuntimeInput::Io(IoEvent::InboundFrame {
            route: Default::default(),
            frame: InboundFrame::Text(
                json!({
                    "aid": "rtn_data",
                    "data": [{
                        "ticks": {
                            "SHFE.au2602": {
                                "data": {
                                    "7": {
                                        "datetime": 1,
                                        "last_price": 612.0
                                    }
                                }
                            }
                        }
                    }]
                })
                .to_string(),
            ),
        }))
        .expect("market diff should decode");

    let row = mutations
        .iter()
        .find(|mutation| mutation.path.as_slice() == ["ticks", "SHFE.au2602", "data", "7"])
        .expect("tick row mutation should exist");
    assert!(row.fields.iter().any(|field| field.field == "id" && field.value == json!(7)));
}
```

- [ ] **Step 2: Run the core tests and verify RED**

Run: `cargo test -p tqsdk-core market_diff_injects_ --test runtime_contract_adapters`

Expected: both tests fail because `id` is missing.

- [ ] **Step 3: Implement minimal row id injection**

In `flatten_object`, after collecting scalar fields and before pushing the `NormalizedMutation`, call a helper:

```rust
inject_market_row_id(&path, &mut fields);
```

Add helper:

```rust
fn inject_market_row_id(path: &[String], fields: &mut Vec<FieldMutation>) {
    if fields.iter().any(|field| field.field == "id") {
        return;
    }
    let Some(id) = market_data_row_id(path) else {
        return;
    };
    fields.push(FieldMutation {
        field: "id".to_string(),
        value: Value::from(id),
    });
    fields.sort_by(|left, right| left.field.cmp(&right.field));
}

fn market_data_row_id(path: &[String]) -> Option<i64> {
    match path {
        [root, _symbol, _duration, branch, row_id] if root == "klines" && branch == "data" => {
            row_id.parse().ok()
        }
        [root, _symbol, branch, row_id] if root == "ticks" && branch == "data" => {
            row_id.parse().ok()
        }
        _ => None,
    }
}
```

- [ ] **Step 4: Verify GREEN**

Run: `cargo test -p tqsdk-core market_diff_injects_ --test runtime_contract_adapters`

Expected: tests pass.

- [ ] **Step 5: Commit Task 1**

Run:

```bash
git add crates/tqsdk-core/src/adapter/common.rs crates/tqsdk-core/tests/runtime_contract_adapters.rs
npx gitnexus detect-changes --scope staged --repo tqsdk-rust
git commit -m "fix(core): inject market row ids from diff keys"
```

## Task 2: Stream Window Projection Uses Chart Bounds

**Files:**
- Modify: `crates/tqsdk-stream/src/window.rs`
- Modify: `crates/tqsdk-stream/tests/stream_typed.rs`

- [ ] **Step 1: Write failing stream window tests**

Add tests that seed rows outside chart bounds and assert only bounded rows appear:

```rust
#[test]
fn kline_window_projection_respects_chart_bounds() {
    let handle = RuntimeHandle::default();
    seed_kline_chart_with_rows(&handle, "chart-k", "SHFE.au2602", 60_000_000_000, 2, 3, [1, 2, 3, 4]);
    let market = handle.reader().read_market_state();
    let spec = KlineWindowSpec {
        symbol: "SHFE.au2602".to_string(),
        duration_ns: 60_000_000_000,
        view_width: 2,
        chart_id: "chart-k".to_string(),
    };

    let window = project_kline_window_from_market(&market, &spec)
        .expect("projection should succeed")
        .expect("window should be ready");

    assert_eq!(window.iter().map(|row| row.id).collect::<Vec<_>>(), vec![2, 3]);
}

#[test]
fn tick_window_projection_respects_chart_bounds() {
    let handle = RuntimeHandle::default();
    seed_tick_chart_with_rows(&handle, "chart-t", "SHFE.au2602", 11, 12, [10, 11, 12, 13]);
    let market = handle.reader().read_market_state();
    let spec = TickWindowSpec {
        symbol: "SHFE.au2602".to_string(),
        view_width: 2,
        chart_id: "chart-t".to_string(),
    };

    let window = project_tick_window_from_market(&market, &spec)
        .expect("projection should succeed")
        .expect("window should be ready");

    assert_eq!(window.iter().map(|row| row.id).collect::<Vec<_>>(), vec![11, 12]);
}
```

- [ ] **Step 2: Run stream tests and verify RED**

Run: `cargo test -p tqsdk-stream window_projection_respects_chart_bounds --test stream_typed`

Expected: tests fail with extra global latest rows.

- [ ] **Step 3: Implement chart bounds helpers**

In `window.rs`, add:

```rust
fn chart_bounds(market: &MarketStateReadGuard<'_>, chart_id: &str) -> Option<(i64, i64)> {
    let left_id = market.get_path(&["charts", chart_id, "left_id"])?.as_i64()?;
    let right_id = market.get_path(&["charts", chart_id, "right_id"])?.as_i64()?;
    (left_id <= right_id).then_some((left_id, right_id))
}
```

Change `read_kline_window` and `read_tick_window` to iterate `left_id..=right_id` instead of sorting all data keys and taking `view_width`.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test -p tqsdk-stream window_projection_respects_chart_bounds --test stream_typed`

Expected: tests pass.

- [ ] **Step 5: Commit Task 2**

Run:

```bash
git add crates/tqsdk-stream/src/window.rs crates/tqsdk-stream/tests/stream_typed.rs
npx gitnexus detect-changes --scope staged --repo tqsdk-rust
git commit -m "fix(stream): project serial windows by chart bounds"
```

## Task 3: Wait Window Projection Uses Chart Bounds

**Files:**
- Modify: `crates/tqsdk-wait/src/refs/kline.rs`
- Modify: `crates/tqsdk-wait/src/refs/tick.rs`
- Modify: `crates/tqsdk-wait/tests/wait_facade.rs`

- [ ] **Step 1: Write failing wait serial tests**

Add tests that use `WaitTestDriver` or existing seed helpers to create chart bounds plus out-of-bounds rows, then call `KlineSerialRef::load` and `TickSerialRef::load`:

```rust
#[test]
fn wait_kline_serial_load_respects_chart_bounds() {
    let (api, serial) = seeded_wait_kline_serial_with_rows(2, 3, [1, 2, 3, 4]);
    let window = serial.load(&api).expect("kline window should load");
    assert_eq!(window.iter().map(|row| row.id).collect::<Vec<_>>(), vec![2, 3]);
}

#[test]
fn wait_tick_serial_load_respects_chart_bounds() {
    let (api, serial) = seeded_wait_tick_serial_with_rows(11, 12, [10, 11, 12, 13]);
    let window = serial.load(&api).expect("tick window should load");
    assert_eq!(window.iter().map(|row| row.id).collect::<Vec<_>>(), vec![11, 12]);
}
```

- [ ] **Step 2: Run wait tests and verify RED**

Run: `cargo test -p tqsdk-wait serial_load_respects_chart_bounds --test wait_facade`

Expected: tests fail with global latest rows.

- [ ] **Step 3: Implement bounds reads in wait refs**

Add local helper in both files or a small shared helper if an existing module fits:

```rust
fn chart_bounds(
    guard: &tqsdk_core::MarketStateReadGuard<'_>,
    chart_id: &str,
) -> Option<(i64, i64)> {
    let left_id = guard.get_path(&["charts", chart_id, "left_id"])?.as_i64()?;
    let right_id = guard.get_path(&["charts", chart_id, "right_id"])?.as_i64()?;
    (left_id <= right_id).then_some((left_id, right_id))
}
```

Update `load()` to iterate bounds rather than global latest keys.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test -p tqsdk-wait serial_load_respects_chart_bounds --test wait_facade`

Expected: tests pass.

- [ ] **Step 5: Commit Task 3**

Run:

```bash
git add crates/tqsdk-wait/src/refs/kline.rs crates/tqsdk-wait/src/refs/tick.rs crates/tqsdk-wait/tests/wait_facade.rs
npx gitnexus detect-changes --scope staged --repo tqsdk-rust
git commit -m "fix(wait): project serial windows by chart bounds"
```

## Task 4: HistorySeriesCache Append And Latest Read

**Files:**
- Modify: `crates/tqsdk-data/src/history_series_cache.rs`
- Modify: `crates/tqsdk-data/src/history_series_cache/ranges.rs`
- Modify: `crates/tqsdk-data/src/history_series_cache/storage.rs`
- Modify: `crates/tqsdk-data/tests/history_series_cache.rs`

- [ ] **Step 1: Write failing append/latest tests**

Add tests:

```rust
#[test]
fn append_kline_rows_is_idempotent_for_duplicates_overlaps_adjacent_and_gaps() {
    let dir = temp_cache_dir("append-kline");
    let cache = HistorySeriesCache::open(&dir).unwrap();

    cache.append_kline_rows("SHFE.au2602", 60_000_000_000, &[kline(1, 10, 1.0), kline(2, 20, 2.0)]).unwrap();
    cache.append_kline_rows("SHFE.au2602", 60_000_000_000, &[kline(2, 20, 22.0), kline(3, 30, 3.0)]).unwrap();
    cache.append_kline_rows("SHFE.au2602", 60_000_000_000, &[kline(5, 50, 5.0)]).unwrap();
    cache.append_kline_rows("SHFE.au2602", 60_000_000_000, &[kline(3, 30, 3.0), kline(5, 50, 5.0)]).unwrap();

    let rows = cache.read_latest_kline_rows("SHFE.au2602", 60_000_000_000, 10).unwrap();
    assert_eq!(rows.iter().map(|row| row.id).collect::<Vec<_>>(), vec![1, 2, 3, 5]);
    assert_eq!(rows[1].close, 22.0);
}

#[test]
fn read_latest_tick_rows_returns_recent_rows_in_ascending_id_order() {
    let dir = temp_cache_dir("latest-tick");
    let cache = HistorySeriesCache::open(&dir).unwrap();

    cache.append_tick_rows("SHFE.au2602", &[tick(10, 100, 10.0), tick(12, 120, 12.0)]).unwrap();
    cache.append_tick_rows("SHFE.au2602", &[tick(11, 110, 11.0), tick(12, 120, 120.0)]).unwrap();

    let rows = cache.read_latest_tick_rows("SHFE.au2602", 2).unwrap();
    assert_eq!(rows.iter().map(|row| row.id).collect::<Vec<_>>(), vec![11, 12]);
    assert_eq!(rows[1].last_price, 120.0);
}
```

- [ ] **Step 2: Run data cache tests and verify RED**

Run: `cargo test -p tqsdk-data append_kline_rows_is_idempotent read_latest_tick_rows_returns_recent_rows`

Expected: tests fail because APIs do not exist.

- [ ] **Step 3: Implement append APIs**

Add public methods:

```rust
pub fn append_kline_rows(&self, symbol: &str, duration_ns: i64, rows: &[Kline]) -> Result<usize>;
pub fn append_tick_rows(&self, symbol: &str, rows: &[Tick]) -> Result<usize>;
```

Implementation outline:

```rust
let incoming = dedup_klines(rows.to_vec());
let incoming_range = range_from_ids(incoming.iter().map(|row| row.id))?;
let affected = self
    .cached_id_ranges(symbol, duration_ns)?
    .into_iter()
    .filter(|range| ranges_touch_or_overlap(*range, incoming_range))
    .collect::<Vec<_>>();
let mut merged_rows = read_rows_from_ranges(...affected...)?;
merged_rows.extend(incoming);
let merged_rows = dedup_klines(merged_rows);
remove affected files;
write_kline_segment_unlocked(symbol, duration_ns, &merged_rows)?;
Ok(rows.len())
```

For ticks, use duration `0`, `dedup_ticks`, and `write_tick_segment_unlocked`.

- [ ] **Step 4: Implement latest-read APIs**

Add public methods:

```rust
pub fn read_latest_kline_rows(&self, symbol: &str, duration_ns: i64, limit: usize) -> Result<Vec<Kline>>;
pub fn read_latest_tick_rows(&self, symbol: &str, limit: usize) -> Result<Vec<Tick>>;
```

Read segment ranges sorted descending by `end_id`, collect rows, dedup by id, then keep the highest `limit` ids and return ascending by id.

- [ ] **Step 5: Verify GREEN**

Run: `cargo test -p tqsdk-data append_kline_rows_is_idempotent read_latest_tick_rows_returns_recent_rows`

Expected: tests pass.

- [ ] **Step 6: Commit Task 4**

Run:

```bash
git add crates/tqsdk-data/src/history_series_cache.rs crates/tqsdk-data/src/history_series_cache/ranges.rs crates/tqsdk-data/src/history_series_cache/storage.rs crates/tqsdk-data/tests/history_series_cache.rs
npx gitnexus detect-changes --scope staged --repo tqsdk-rust
git commit -m "feat(data): append and read latest history cache rows"
```

## Task 5: LiveHistoryCacheWriter

**Files:**
- Create: `crates/tqsdk-data/src/live_history_cache.rs`
- Modify: `crates/tqsdk-data/src/lib.rs`
- Create: `crates/tqsdk-data/tests/live_history_cache.rs`

- [ ] **Step 1: Write failing live writer tests**

Add tests behind `#![cfg(feature = "stream")]`:

```rust
#[test]
fn kline_live_writer_skips_mutable_tail_and_writes_completed_bar_after_window_advances() {
    let dir = temp_cache_dir("live-kline");
    let cache = HistorySeriesCache::open(&dir).unwrap();
    let mut writer = LiveHistoryCacheWriter::new(cache.clone(), LiveHistoryCacheOptions::default());

    writer.write_kline_window(&kline_window([kline(1, 10, 1.0), kline(2, 20, 2.0)])).unwrap();
    assert_eq!(cache.read_latest_kline_rows("SHFE.au2602", 60_000_000_000, 10).unwrap().iter().map(|row| row.id).collect::<Vec<_>>(), vec![1]);

    writer.write_kline_window(&kline_window([kline(2, 20, 22.0), kline(3, 30, 3.0)])).unwrap();
    let rows = cache.read_latest_kline_rows("SHFE.au2602", 60_000_000_000, 10).unwrap();
    assert_eq!(rows.iter().map(|row| row.id).collect::<Vec<_>>(), vec![1, 2]);
    assert_eq!(rows[1].close, 22.0);
}

#[test]
fn tick_live_writer_dedups_repeated_windows() {
    let dir = temp_cache_dir("live-tick");
    let cache = HistorySeriesCache::open(&dir).unwrap();
    let mut writer = LiveHistoryCacheWriter::new(cache.clone(), LiveHistoryCacheOptions::default());
    let window = tick_window([tick(10, 100, 10.0), tick(11, 110, 11.0)]);

    writer.write_tick_window(&window).unwrap();
    writer.write_tick_window(&window).unwrap();

    let rows = cache.read_latest_tick_rows("SHFE.au2602", 10).unwrap();
    assert_eq!(rows.iter().map(|row| row.id).collect::<Vec<_>>(), vec![10, 11]);
}
```

- [ ] **Step 2: Run live writer tests and verify RED**

Run: `cargo test -p tqsdk-data --features stream live_writer`

Expected: tests fail because APIs do not exist.

- [ ] **Step 3: Implement live writer module**

Create:

```rust
#[derive(Debug, Clone, Default)]
pub struct LiveHistoryCacheOptions;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LiveHistoryCacheWriteReport {
    pub rows_seen: usize,
    pub rows_written: usize,
    pub skipped_mutable_tail: bool,
}

pub struct LiveHistoryCacheWriter {
    cache: HistorySeriesCache,
    options: LiveHistoryCacheOptions,
}
```

Implement methods:

```rust
pub fn write_kline_window(&mut self, window: &tqsdk_stream::KlineWindow) -> Result<LiveHistoryCacheWriteReport> {
    let rows_seen = window.len();
    let max_id = window.iter().map(|row| row.id).max();
    let rows = window
        .iter()
        .filter(|row| Some(row.id) != max_id)
        .cloned()
        .collect::<Vec<_>>();
    let rows_written = self.cache.append_kline_rows(window.symbol(), window.duration_ns(), &rows)?;
    Ok(LiveHistoryCacheWriteReport {
        rows_seen,
        rows_written,
        skipped_mutable_tail: max_id.is_some(),
    })
}
```

Tick writes all rows. `write_market_event` routes Kline/Tick windows and ignores Quote.

- [ ] **Step 4: Export APIs behind stream feature**

In `lib.rs`:

```rust
#[cfg(feature = "stream")]
mod live_history_cache;

#[cfg(feature = "stream")]
pub use live_history_cache::{
    LiveHistoryCacheOptions, LiveHistoryCacheWriteReport, LiveHistoryCacheWriter,
};
```

- [ ] **Step 5: Verify GREEN**

Run: `cargo test -p tqsdk-data --features stream live_writer`

Expected: tests pass.

- [ ] **Step 6: Commit Task 5**

Run:

```bash
git add crates/tqsdk-data/src/live_history_cache.rs crates/tqsdk-data/src/lib.rs crates/tqsdk-data/tests/live_history_cache.rs
npx gitnexus detect-changes --scope staged --repo tqsdk-rust
git commit -m "feat(data): write stream windows into history cache"
```

## Task 6: Documentation And Full Verification

**Files:**
- Modify: `docs/architecture/api-data.md`
- Modify: `crates/tqsdk-data/README.md`
- Modify: `crates/tqsdk-data/src/lib.rs`
- Optional: `crates/tqsdk-stream/README.md`
- Optional: `crates/tqsdk-wait/README.md`

- [ ] **Step 1: Update docs**

Document:

- `HistorySeriesCache::append_*`
- `HistorySeriesCache::read_latest_*`
- `LiveHistoryCacheWriter`
- Kline tail skip semantics
- `DataClient::from_session(...)` remains unchanged

- [ ] **Step 2: Run targeted tests**

Run:

```bash
cargo test -p tqsdk-core
cargo test -p tqsdk-stream
cargo test -p tqsdk-wait
cargo test -p tqsdk-data --features stream
```

Expected: all commands exit 0.

- [ ] **Step 3: Run formatting and lint checks**

Run:

```bash
cargo fmt --all --check
cargo check --workspace --examples
cargo clippy --workspace --examples --all-targets -- -D warnings
```

Expected: all commands exit 0.

- [ ] **Step 4: Run final GitNexus detect changes**

Run:

```bash
npx gitnexus detect-changes --scope all --repo tqsdk-rust
```

Expected: only intended symbols and flows are affected.

- [ ] **Step 5: Commit docs/final polish**

Run:

```bash
git add docs/architecture/api-data.md crates/tqsdk-data/README.md crates/tqsdk-data/src/lib.rs crates/tqsdk-stream/README.md crates/tqsdk-wait/README.md
npx gitnexus detect-changes --scope staged --repo tqsdk-rust
git commit -m "docs: describe live history cache bridge"
```
