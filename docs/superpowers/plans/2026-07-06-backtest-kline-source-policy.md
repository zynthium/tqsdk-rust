# Backtest Kline Source Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Implement Python-aligned cache-backed local backtest Kline data policy: synthesize `duration <= 60s` Klines from local ticks, use native Kline history for `duration > 60s`, and keep quote fallback behavior aligned with `tqsdk-python`.

**Architecture:** Keep ownership boundaries unchanged. `tqsdk-data` remains the durable `HistorySeriesCache` owner. `tqsdk-task` owns replay/backtest market event ingestion and local stream mechanics. `tqsdk` facade owns cache policy resolution, remote fill orchestration, and user-facing builder APIs. Synthesized Klines are transient replay events only and are never written to the durable cache.

**Tech Stack:** Rust 2024, Tokio, Cargo workspace, `tqsdk-data`, `tqsdk-task`, `tqsdk`, existing `HistorySeriesCache`, existing `BacktestTickCache`, existing `DataClient` history APIs.

---

## Scope Check

This plan implements:

- `duration <= 60s`: Kline serials are synthesized locally from cached tick data. This includes exactly `60s`.
- `duration > 60s`: Kline serials are read from native Kline history rows in `HistorySeriesCache`.
- Quote fallback:
  - If tick data is requested, quote updates come from ticks.
  - If only Kline data is requested and the smallest requested Kline duration is `<= 60s`, quote updates include that synthesized Kline.
  - If no tick/Kline is requested, or the smallest requested Kline duration is `> 60s`, auto-add a synthetic `60s` Kline requirement for quote fallback.
  - If multiple Kline durations are requested, every Kline update can produce quote checkpoints, matching Python's multi-period behavior.
- `CacheOnly`, `RemoteOnMiss`, and `Refresh` coverage handling for both required tick ranges and native `>60s` Kline ranges.
- No-cache official server-side backtest remains unchanged.

This plan does not implement:

- A new durable Kline cache format.
- Persistent storage for synthesized Klines.
- Core runtime/state-tree contract changes.
- Relay/dashboard changes.
- Trading-side behavior changes.

## Impact Analysis Gates

Before editing Rust symbols, run GitNexus impact analysis or CodeGraph exploration for the target symbol and record risk in the work log. At minimum, check:

- `BacktestBuilder::prepare`
- `PreparedBacktest::connect`
- `StrategyBacktestBuilder::build`
- `StrategyBacktest::ingest_replay_event`
- `seed_replay_serials`
- `ingest_replay_market_event`
- `HistoryTickReplayStream`

If impact is HIGH or CRITICAL, report the direct callers, affected flows, and risk before editing.

## File Structure

- Create `crates/tqsdk-task/src/replay_runtime.rs`
  - Move replay serial specs and chart-state update helpers out of `replay.rs`.
  - Share them between `StrategyReplay` and `StrategyBacktest`.

- Modify `crates/tqsdk-task/src/replay.rs`
  - Use `replay_runtime` for `ReplayKlineSpec`, `ReplayTickSpec`, `seed_replay_serials`, and `ingest_replay_market_event`.

- Modify `crates/tqsdk-task/src/backtest.rs`
  - Add Kline/tick serial registration to `StrategyBacktestBuilder`.
  - Seed Kline/tick chart serials during `build`.
  - Update Kline/tick serial state when replay events are ingested.

- Create `crates/tqsdk-task/src/kline_synth.rs`
  - Implement tick-to-Kline synthesis for `duration <= 60s`.
  - Keep synthesized rows transient and deterministic.

- Create `crates/tqsdk-task/src/history_backtest_replay.rs`
  - Implement a mixed local history replay stream over tick rows, native Kline rows, and synthesized Kline rows.
  - Preserve `HistoryTickReplayStream` as the existing tick-only public stream.

- Modify `crates/tqsdk-task/src/lib.rs`
  - Export `HistoryBacktestReplayStream`.
  - Keep existing `HistoryTickReplayStream` export.

- Modify `crates/tqsdk/src/lib.rs`
  - Add backtest Kline/tick requirements to `BacktestBuilder`.
  - Classify Kline source by duration.
  - Resolve coverage and remote fill for tick and native Kline inputs.
  - Build `HistoryBacktestReplayStream` for cache-backed local backtest.
  - Extend `BacktestDataReport` with tick/native/synth counts without breaking existing fields.

- Modify `crates/tqsdk/src/local_backtest.rs`
  - Carry facade Kline/tick declarations into `StrategyBacktestBuilder`.

- Modify or create `crates/tqsdk/src/backtest_history_remote.rs`
  - Add native Kline remote fill through `DataClientBuilder::with_session(...).history_cache_dir(cache.cache_dir())`.
  - Keep existing `backtest_remote.rs` tick fill behavior intact.

- Modify tests:
  - `crates/tqsdk-task/tests/history_tick_replay.rs`
  - Add `crates/tqsdk-task/tests/history_backtest_replay.rs`
  - Add or extend facade contract tests under `crates/tqsdk/tests/` or existing `crates/tqsdk/src/lib.rs` test modules.

- Modify docs/examples:
  - `crates/tqsdk/examples/api_contract_s43_facade_backtest_history_cache.rs`
  - `crates/tqsdk/examples/api_contract_s44_facade_backtest_remote_on_miss.rs`
  - `crates/tqsdk/README.md`
  - `crates/tqsdk-data/README.md`
  - `docs/architecture/api-data.md`
  - `docs/architecture/validation.md`

## Task 1: Add Source Policy Tests

**Files:**
- Modify: `crates/tqsdk/src/lib.rs`

- [x] **Step 1: Add pure policy helpers behind the facade**

Add internal helper types and functions near `BacktestBuilder`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BacktestKlineSource {
    SynthesizedFromTick,
    NativeKline,
}

fn backtest_kline_source(duration_ns: i64) -> Result<BacktestKlineSource> {
    if duration_ns <= 0 {
        return Err(data_validation("backtest kline duration must be greater than zero"));
    }
    if duration_ns <= 60_000_000_000 {
        Ok(BacktestKlineSource::SynthesizedFromTick)
    } else {
        Ok(BacktestKlineSource::NativeKline)
    }
}
```

- [x] **Step 2: Add boundary tests**

Add tests covering:

- `1s` => `SynthesizedFromTick`
- `59s` => `SynthesizedFromTick`
- `60s` => `SynthesizedFromTick`
- `61s` => `NativeKline`
- zero duration rejects

- [x] **Step 3: Add quote fallback planning tests**

Add a pure helper that receives declared tick symbols and Kline specs and returns:

- required tick symbols
- native Kline specs
- synthetic Kline specs
- auto-added quote fallback synthetic `60s` specs

Test cases:

- tick only: no auto `60s` Kline
- `30s` Kline only: requires tick data and synthetic `30s`, no extra `60s`
- `60s` Kline only: requires tick data and synthetic `60s`
- `300s` Kline only: requires native `300s` plus synthetic `60s`
- no tick/Kline but quote symbol exists: requires synthetic `60s`
- multiple Klines `30s` and `300s`: requires synthetic `30s` and native `300s`, no extra `60s`

- [x] **Step 4: Run focused tests**

Run:

```bash
rtk cargo test -p tqsdk backtest_kline_source
rtk cargo test -p tqsdk backtest_quote_fallback
```

Expected:

```text
all tests pass
```

## Task 2: Share Replay Serial Runtime Helpers

**Files:**
- Create: `crates/tqsdk-task/src/replay_runtime.rs`
- Modify: `crates/tqsdk-task/src/replay.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`

- [x] **Step 1: Move private replay specs**

Move `ReplayKlineSpec` and `ReplayTickSpec` from `replay.rs` into `replay_runtime.rs` as `pub(crate)` types with constructors:

```rust
pub(crate) struct ReplayKlineSpec {
    pub(crate) symbol: String,
    pub(crate) duration_ns: i64,
    pub(crate) view_width: usize,
}

pub(crate) struct ReplayTickSpec {
    pub(crate) symbol: String,
    pub(crate) view_width: usize,
}
```

Keep equality semantics unchanged so existing dedup logic keeps working.

- [x] **Step 2: Move chart update helpers**

Move these helpers from `replay.rs` to `replay_runtime.rs`:

- `seed_replay_serials`
- `ingest_replay_market_event`
- `quote_update`
- `kline_update`
- `tick_update`
- `insert_quote_underlying_update`
- `kline_value`
- `tick_value`
- `kline_chart_id`
- `tick_chart_id`
- `sanitize_chart_token`

Expose only what is needed as `pub(crate)`.

- [x] **Step 3: Update `StrategyReplayBuilder`**

Update `replay.rs` to import from `crate::replay_runtime`.

Behavior must remain unchanged:

- `StrategyReplayBuilder::kline(...)` still dedups specs.
- `StrategyReplayBuilder::tick(...)` still dedups specs.
- `StrategyReplayBuilder::build()` still seeds serials before `StrategyHostBuilder::build()`.
- `StrategyReplay::next()` still calls shared event ingestion before `next_once()`.

- [x] **Step 4: Run existing replay tests**

Run:

```bash
rtk cargo test -p tqsdk-task replay
rtk cargo check -p tqsdk-task --tests
```

Expected:

```text
all commands exit code 0
```

## Task 3: Add Kline and Tick Serial Support to StrategyBacktest

**Files:**
- Modify: `crates/tqsdk-task/src/backtest.rs`
- Modify: `crates/tqsdk-task/src/replay_runtime.rs`

- [x] **Step 1: Extend `StrategyBacktestBuilder` state**

Add builder fields:

```rust
klines: Vec<ReplayKlineSpec>,
ticks: Vec<ReplayTickSpec>,
```

Add builder methods:

```rust
pub fn kline(self, symbol: impl AsRef<str>, duration: Duration, view_width: usize) -> Self
pub fn tick(self, symbol: impl AsRef<str>, view_width: usize) -> Self
```

Use the same duration conversion and dedup semantics as `StrategyReplayBuilder`.

- [x] **Step 2: Seed serials in `StrategyBacktestBuilder::build`**

Before building the strategy host, call:

```rust
seed_replay_serials(&host, &self.klines, &self.ticks)?;
```

Then register the handles through `StrategyHostBuilder`:

```rust
for spec in &self.klines {
    builder = builder.kline(&spec.symbol, Duration::from_nanos(spec.duration_ns as u64), spec.view_width);
}
for spec in &self.ticks {
    builder = builder.tick(&spec.symbol, spec.view_width);
}
```

- [x] **Step 3: Store serial specs in `StrategyBacktest`**

Add fields:

```rust
klines: Vec<ReplayKlineSpec>,
ticks: Vec<ReplayTickSpec>,
```

Pass them from the builder into the constructed backtest.

- [x] **Step 4: Update serial state during backtest ingestion**

At the start of `StrategyBacktest::ingest_replay_event`, call:

```rust
ingest_replay_market_event(self.strategy.task_host(), event, &self.klines, &self.ticks)?;
```

Keep existing quote/sim ingestion logic after that call.

- [x] **Step 5: Add backtest serial tests**

Add tests that build a `StrategyBacktest` with:

- one tick serial and tick events
- one Kline serial and Kline events

Assert that strategy context can read the corresponding `TickWindow` and `KlineWindow` after `next()`.

- [x] **Step 6: Run focused tests**

Run:

```bash
rtk cargo test -p tqsdk-task backtest
rtk cargo check -p tqsdk-task --tests
```

Expected:

```text
all commands exit code 0
```

## Task 4: Implement Tick-to-Kline Synthesis

**Files:**
- Create: `crates/tqsdk-task/src/kline_synth.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`

- [x] **Step 1: Add the synthesizer**

Create `KlineSynthesizer` with:

- `symbol: String`
- `duration_ns: i64`
- current bar state
- previous cumulative tick volume and amount baseline

Rules:

- Reject `duration_ns <= 0`.
- Reject `duration_ns > 60_000_000_000` for synthetic use.
- Bar id is deterministic from `bar_start_ns / duration_ns`.
- Bar datetime is `bar_start_ns`.
- Open is the first tick last price in the bar.
- High/low/close update on every tick in the bar.
- `open_oi` is the first tick open interest in the bar.
- `close_oi` is the latest tick open interest in the bar.
- Volume uses non-negative cumulative tick volume delta since the bar baseline.
- If the first tick in the replay range has no prior baseline, start that first bar from zero volume.

- [x] **Step 2: Emit one Kline update per input tick**

Expose:

```rust
pub(crate) fn update(&mut self, tick: &Tick) -> Option<Kline>
```

The returned row is the current partial/final bar state after applying that tick. The event timestamp remains the tick timestamp. This keeps `wait_update()` stepping at real tick timestamps for synthesized short-period Klines.

- [x] **Step 3: Add unit tests**

Test:

- first tick starts a bar with open/high/low/close equal to tick price
- multiple ticks update high/low/close
- boundary tick at exactly next bar start creates a new row
- `60s` is accepted
- `61s` is rejected by the caller policy, not synthesized
- cumulative volume reset or decrease does not produce negative Kline volume

- [x] **Step 4: Run tests**

Run:

```bash
rtk cargo test -p tqsdk-task kline_synth
```

Expected:

```text
all tests pass
```

## Task 5: Add Mixed History Backtest Replay Stream

**Files:**
- Create: `crates/tqsdk-task/src/history_backtest_replay.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`
- Optionally modify: `crates/tqsdk-data/src/history_series_cache.rs`

- [x] **Step 1: Add request specs**

Add public task-level request types:

```rust
pub struct HistoryBacktestReplayRequest {
    pub cache: HistorySeriesCache,
    pub start_ns: i64,
    pub end_ns: i64,
    pub tick_symbols: Vec<String>,
    pub native_klines: Vec<HistoryBacktestKlineRequest>,
    pub synthetic_klines: Vec<HistoryBacktestKlineRequest>,
}

pub struct HistoryBacktestKlineRequest {
    pub symbol: String,
    pub duration_ns: i64,
}
```

Keep `view_width` out of the stream request. View width belongs to serial registration, not event generation.

- [x] **Step 2: Implement native Kline cursors**

Use `HistorySeriesCache::read_kline_data_series(...)` initially. If memory risk becomes material during implementation, add a `KlineDataSeriesReader` parallel to `TickDataSeriesReader`.

Native Kline event rules:

- For each native row, emit an open event at `row.datetime` with OHLC all set to `row.open`, volume `0`, `open_oi`, and `close_oi = open_oi`.
- Emit a final event at `row.datetime + duration_ns` with the original row.
- Skip events whose event timestamp is outside `[start_ns, end_ns)`.

- [x] **Step 3: Implement synthetic Kline cursors**

Each synthetic Kline request opens a tick reader for the same symbol and requested range. Feed ticks through `KlineSynthesizer` and emit Kline payload events with source `history-cache-synth-kline`.

Keep synthesized rows transient. Do not call `write_kline_range`.

- [x] **Step 4: Keep tick event cursors**

For declared tick symbols, reuse the `HistoryTickReplayStream` cursor behavior:

- Open `TickDataSeriesReader`.
- Emit `ReplayMarketEvent::tick("history-cache", ...)`.
- Preserve stable ordering by `(datetime, tick_id, symbol_rank)`.

- [x] **Step 5: Merge all cursor types**

Use a `BinaryHeap` similar to `HistoryTickReplayStream`.

Stable sort key:

1. `event_time_ns`
2. source rank: tick, synthetic Kline, native Kline open, native Kline close
3. symbol rank
4. row id
5. cursor index

`StrategyBacktest::next()` batches equal `event_time_ns`, so source ordering only affects same-timestamp last-quote tie breaking. Keep synthetic Kline close equal to tick price so tick/synthetic ties remain stable.

- [x] **Step 6: Export the stream**

In `crates/tqsdk-task/src/lib.rs`, add:

```rust
mod history_backtest_replay;
pub use history_backtest_replay::{
    HistoryBacktestKlineRequest, HistoryBacktestReplayRequest, HistoryBacktestReplayStream,
};
```

- [x] **Step 7: Add stream tests**

Add `crates/tqsdk-task/tests/history_backtest_replay.rs` covering:

- tick-only stream matches existing `HistoryTickReplayStream` event order
- `60s` synthetic Kline rows are emitted from ticks
- `61s` native Kline rows are emitted from cached Kline data
- mixed tick, synthetic Kline, and native Kline events are globally time ordered
- synthesized Klines are not written to cache

- [x] **Step 8: Run tests**

Run:

```bash
rtk cargo test -p tqsdk-task history_backtest_replay
rtk cargo test -p tqsdk-task history_tick_replay
```

Expected:

```text
all tests pass
```

## Task 6: Add Facade Backtest Serial APIs

**Files:**
- Modify: `crates/tqsdk/src/lib.rs`
- Modify: `crates/tqsdk/src/local_backtest.rs`

- [x] **Step 1: Extend `BacktestBuilder`**

Add fields:

```rust
kline_specs: Vec<BacktestKlineSpec>,
tick_specs: Vec<BacktestTickSpec>,
quote_symbols: Vec<String>,
```

If existing `symbols` already means replay quote/tick symbols, keep it but split internally into explicit planned requirements before coverage checks.

- [x] **Step 2: Add user-facing builder methods**

Add:

```rust
pub fn kline(self, symbol: impl AsRef<str>, duration: Duration, view_width: usize) -> Self
pub fn tick(self, symbol: impl AsRef<str>, view_width: usize) -> Self
```

Behavior:

- `.kline("X", 60s, n)` registers a serial and plans synthesized Kline from ticks.
- `.kline("X", 61s, n)` registers a serial and plans native Kline cache use.
- `.tick("X", n)` registers a tick serial and plans tick cache use.

- [x] **Step 3: Extend `TqBuilder` replay-backtest helpers**

Add matching local replay declarations for custom replay users:

```rust
pub fn kline_symbol(self, symbol: impl Into<String>, duration: Duration, view_width: usize) -> Self
pub fn tick_symbol(self, symbol: impl Into<String>, view_width: usize) -> Self
```

Apply them through `LocalBacktestRecipe` into `StrategyBacktestBuilder`.

- [x] **Step 4: Update `LocalBacktestRecipe`**

Add Kline/tick spec storage and apply them in `apply_to_builder(...)`:

```rust
for spec in &kline_specs {
    builder = builder.kline(&spec.symbol, spec.duration, spec.view_width);
}
for spec in &tick_specs {
    builder = builder.tick(&spec.symbol, spec.view_width);
}
```

- [x] **Step 5: Add facade API tests**

Test that:

- `Tq::new().replay_backtest(source).kline_symbol(...).connect()` exposes a `KlineWindow`.
- `Tq::new().replay_backtest(source).tick_symbol(...).connect()` exposes a `TickWindow`.
- `Tq::new().backtest(...).cache_only().kline(...60s...)` plans tick coverage, not native Kline coverage.
- `Tq::new().backtest(...).cache_only().kline(...61s...)` plans native Kline coverage.

- [x] **Step 6: Run focused facade checks**

Run:

```bash
rtk cargo test -p tqsdk backtest_kline
rtk cargo check -p tqsdk --examples
```

Expected:

```text
all commands exit code 0
```

## Task 7: Implement Cache Coverage and Remote Native Kline Fill

**Files:**
- Modify: `crates/tqsdk/src/lib.rs`
- Create or modify: `crates/tqsdk/src/backtest_history_remote.rs`
- Modify: `crates/tqsdk/src/backtest_remote.rs` only if shared request/report code is needed

- [x] **Step 1: Build a planned input set in `BacktestBuilder::prepare`**

After universe resolution and market cache policy application, compute:

- required tick symbols
- required synthetic Kline specs
- required native Kline specs
- auto quote fallback synthetic `60s` specs

Dedup by `(symbol, duration_ns, view_width)` for serials and by `(symbol, duration_ns)` for data requirements.

- [x] **Step 2: Keep `Refresh` semantics strict**

For `Refresh`:

- purge required tick symbols through `BacktestTickCache::purge_symbol_ticks`
- purge required native Kline series through `HistorySeriesCache::open(cache.cache_dir())?.purge_kline_series(symbol, duration_ns)`
- do not purge synthesized Kline series, because none exist

- [x] **Step 3: Check coverage**

For tick requirements:

```rust
cache.coverage(symbol, start_ns, end_ns)?
```

For native Kline requirements:

```rust
HistorySeriesCache::open(cache.cache_dir())?
    .kline_coverage(symbol, duration_ns, start_ns, end_ns)?
```

`CacheOnly` fails on the first missing tick or native Kline coverage with an error message that includes symbol, duration, and missing ranges.

- [x] **Step 4: Fill missing tick ranges**

Keep the existing `backtest_remote::fill_backtest_tick_cache(...)` path unchanged for tick requirements.

- [x] **Step 5: Fill missing native Kline ranges**

Implement helper:

```rust
async fn fill_backtest_kline_cache(
    auth: &Auth,
    cache_dir: &Path,
    requests: Vec<BacktestKlineFillRequest>,
) -> Result<BacktestKlineFillReport>
```

Implementation:

1. Build a session with `session_builder(Some(auth.clone()), false, Vec::new(), None, None)?`.
2. Build a `DataClient`:

```rust
let client = tqsdk_data::DataClientBuilder::new()
    .with_session(session)
    .history_cache_enabled(true)
    .history_cache_dir(cache_dir)
    .build()?;
```

3. For each missing `(symbol, duration_ns, start_ns, end_ns)`, call:

```rust
client
    .get_kline_data_series(
        KlineDataSeriesRequest::new(symbol, Duration::from_nanos(duration_ns as u64), start_ns, end_ns)
    )
    .await?;
```

The `DataClient` writes downloaded native Kline rows into the same `HistorySeriesCache`.

- [x] **Step 6: Extend `PreparedBacktestMode` and report**

Keep existing public fields and add fields that preserve old meaning:

```rust
pub struct BacktestDataReport {
    pub requested_range: (i64, i64),
    pub cache_policy: BacktestCachePolicy,
    pub cache_dir: PathBuf,
    pub resolved_symbols: usize,
    pub remote_used: bool,
    pub tick_symbols: usize,
    pub native_kline_series: usize,
    pub synthetic_kline_series: usize,
    pub remote_tick_used: bool,
    pub remote_kline_used: bool,
}
```

Set:

- `remote_used = remote_tick_used || remote_kline_used`
- `native_kline_series` counts `duration > 60s`
- `synthetic_kline_series` counts `duration <= 60s`, including auto `60s` fallback

If adding public fields is considered too breaking during implementation, replace these with accessor methods and keep struct fields unchanged.

- [x] **Step 7: Build the mixed stream in `PreparedBacktest::connect`**

Replace tick-only stream creation with:

```rust
let stream = tqsdk_task::HistoryBacktestReplayStream::new(HistoryBacktestReplayRequest {
    cache: HistorySeriesCache::open(cache.cache_dir())?,
    start_ns,
    end_ns,
    tick_symbols,
    native_klines,
    synthetic_klines,
})?;
```

Then connect through:

```rust
base.replay_backtest_stream(Box::new(stream)).connect().await
```

- [x] **Step 8: Run focused facade tests**

Run:

```bash
rtk cargo test -p tqsdk fill_requests_use_only_sparse_cache_missing_ranges
rtk cargo test -p tqsdk backtest_kline
rtk cargo check -p tqsdk --example api_contract_s43_facade_backtest_history_cache
```

Expected:

```text
all commands exit code 0
```

## Task 8: Update Contract Examples and Docs

**Files:**
- Modify: `crates/tqsdk/examples/api_contract_s43_facade_backtest_history_cache.rs`
- Modify: `crates/tqsdk/examples/api_contract_s44_facade_backtest_remote_on_miss.rs`
- Modify: `crates/tqsdk/examples/api_contract_s45_facade_backtest_cache_warmup.rs`
- Modify: `crates/tqsdk/README.md`
- Modify: `crates/tqsdk-data/README.md`
- Modify: `docs/architecture/api-data.md`
- Modify: `docs/architecture/validation.md`

- [x] **Step 1: Update examples**

Show both boundary cases:

```rust
.kline("SHFE.rb2601", Duration::from_secs(60), 200)  // synthesized from tick
.kline("SHFE.rb2601", Duration::from_secs(300), 200) // native Kline cache
```

Keep examples free of credentials unless they already require live/remote features and env gating.

- [x] **Step 2: Document source policy**

Add user-facing wording:

```markdown
For cache-backed local backtest, Klines at 60 seconds or below are synthesized
from cached tick data and are not persisted as Kline files. Klines above 60
seconds use native historical Kline rows in `HistorySeriesCache`. In
`RemoteOnMiss` or `Refresh`, missing native Kline rows are downloaded through
the normal history data path, while missing tick rows use the backtest tick
fill path.
```

- [x] **Step 3: Update validation matrix**

Add:

```bash
rtk cargo test -p tqsdk-task kline_synth
rtk cargo test -p tqsdk-task history_backtest_replay
rtk cargo test -p tqsdk backtest_kline
rtk cargo check -p tqsdk --examples
rtk cargo check -p tqsdk --no-default-features --examples
```

- [x] **Step 4: Run docs whitespace check**

Run:

```bash
rtk git diff --check crates/tqsdk/examples crates/tqsdk/README.md crates/tqsdk-data/README.md docs/architecture/api-data.md docs/architecture/validation.md
```

Expected:

```text
no output and exit code 0
```

## Task 9: Final Verification

**Files:**
- Verify current workspace only

- [x] **Step 1: Format**

Run:

```bash
rtk cargo fmt --all
rtk cargo fmt --all --check
```

Expected:

```text
both commands exit code 0
```

- [x] **Step 2: Focused tests**

Run:

```bash
rtk cargo test -p tqsdk-task kline_synth
rtk cargo test -p tqsdk-task history_backtest_replay
rtk cargo test -p tqsdk-task history_tick_replay
rtk cargo test -p tqsdk backtest_kline
rtk cargo check -p tqsdk --examples
```

Expected:

```text
all commands exit code 0
```

- [x] **Step 3: Feature coverage**

Run:

```bash
rtk cargo check -p tqsdk --no-default-features --examples
rtk cargo check -p tqsdk-task --no-default-features --tests
rtk cargo test -p tqsdk-data --no-default-features history_series_cache
```

Expected:

```text
all commands exit code 0
```

- [x] **Step 4: Lint**

Run:

```bash
rtk cargo clippy -p tqsdk-task --all-targets -- -D warnings
rtk cargo clippy -p tqsdk --all-targets -- -D warnings
rtk git diff --check
```

Expected:

```text
all commands exit code 0
```

- [x] **Step 5: Detect changes before any commit**

Run:

```bash
rtk gitnexus detect-changes
```

Expected:

```text
reported changes are limited to tqsdk-task replay/backtest history stream, tqsdk facade backtest planning/fill, examples, and docs
```

## Rollback Plan

If implementation exposes a large unexpected risk:

- Keep `StrategyBacktest` serial support, because it is useful and shared with custom replay.
- Revert `HistoryBacktestReplayStream` usage from `PreparedBacktest::connect` back to `HistoryTickReplayStream`.
- Keep the source policy tests as ignored or pending only if the user wants the design preserved.
- Do not change no-cache official server-side backtest behavior.

## Notes for Implementer

- Do not write synthesized Klines to `HistorySeriesCache`.
- Do not make `tqsdk-core` depend on data/backtest policy.
- Do not make `tqsdk-monitor` part of replay or cache preparation.
- Keep `duration == 60s` in the synthetic path.
- Prefer adding accessors over breaking public report struct construction if downstream breakage becomes visible.
- Use exact timestamps in errors and tests. Avoid relative date wording.
