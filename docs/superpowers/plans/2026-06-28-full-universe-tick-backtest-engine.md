# Full-Universe Tick Backtest Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `backtest(start, end)` a Python-minded but Rust-native full-universe backtest entry: tick is the canonical historical source, the persistent cache is the primary store, K lines are derived from ticks, and strategies advance by market-time frames across all selected symbols.

**Architecture:** This plan builds on `docs/superpowers/plans/2026-06-28-backtest-persistent-cache-primary-store.md`. That prerequisite plan establishes the persistent tick cache, remote-on-miss filling through official server-side backtest streams, shared universe selection, and bounded tick replay streams. This Phase 2 plan changes the execution model from one replay event per strategy step to one market-time frame per strategy step, adds tick-derived K line state, and separates fill logic behind a Rust trait so the default path remains tick-canonical.

**Tech Stack:** Rust 2024, `thiserror` crate-level errors, existing `tqsdk-core` `Tick`/`Kline`/`Quote` types, existing `tqsdk-task::StrategyBacktest`, existing `tqsdk-data::BacktestTickCache`, `BinaryHeap` tick merge, `VecDeque` rolling serial buffers, Cargo contract examples and integration tests.

---

## Scope And Ordering

This plan intentionally does not repeat the persistent-cache primary-store work. Before starting Task 1, verify that the following prerequisite symbols exist:

```bash
rtk rg -n "RemoteBacktestCachingStream" crates/tqsdk/src/backtest_remote.rs
rtk rg -n "BacktestMarketStream" crates/tqsdk-task/src/backtest_stream.rs
rtk rg -n "pub fn universe" crates/tqsdk/src/lib.rs
rtk rg -n "HistoryTickReplayStream" crates/tqsdk-task/src/history_tick_replay.rs
```

Expected: all commands find concrete Rust code. If any command fails, execute `docs/superpowers/plans/2026-06-28-backtest-persistent-cache-primary-store.md` through Task 11 first.

Hard constraints:

- Do not use professional history download APIs for backtest cache misses.
- Remote fills must keep using official server-side backtest market streams.
- Tick cache remains canonical; do not persist derived K lines as a second backtest source.
- Python mental model is preserved at the facade level, but Python K-line synthesized-fill quirks are not the default Rust execution model.
- Public library APIs return typed crate errors; reserve `anyhow` for examples and binaries.

## File Structure

- `crates/tqsdk-task/src/backtest_frame.rs`: market-time frame type, frame stream trait, frame grouping helpers.
- `crates/tqsdk-task/src/backtest_stream.rs`: adapt the existing market stream trait to expose frames.
- `crates/tqsdk-task/src/history_tick_replay.rs`: group merged cached ticks into `BacktestFrame`.
- `crates/tqsdk-task/src/kline_aggregator.rs`: tick-to-Kline rolling aggregation and subscription state.
- `crates/tqsdk-task/src/fill_model.rs`: fill model trait and default tick fill model adapter.
- `crates/tqsdk-task/src/backtest.rs`: consume frames, update quotes/K lines before one strategy step, delegate matching to fill model.
- `crates/tqsdk-task/src/lib.rs`: export new task-level types.
- `crates/tqsdk-task/tests/backtest_frame.rs`: frame grouping tests.
- `crates/tqsdk-task/tests/kline_aggregator.rs`: tick-derived K-line tests.
- `crates/tqsdk-task/tests/strategy_backtest.rs`: update behavior tests for one strategy step per market-time frame.
- `crates/tqsdk/src/backtest_remote.rs`: emit frames from official backtest ticks.
- `crates/tqsdk/src/lib.rs`: expose facade contract for universe-driven tick-canonical backtests.
- `crates/tqsdk/examples/api_contract_s45_facade_full_universe_backtest.rs`: public full-universe backtest example.
- `crates/tqsdk/tests/facade_contract.rs`: example-source contract checks.
- `crates/tqsdk/Cargo.toml`: register the new example.
- `README.md`, `crates/tqsdk/README.md`, `crates/tqsdk-task/README.md`, `docs/architecture/api-task.md`, `docs/architecture/api-data.md`, `docs/architecture/validation.md`: document tick-canonical semantics and validation.

---

### Task 1: Add Full-Universe Facade Contract Example

**Files:**
- Create: `crates/tqsdk/examples/api_contract_s45_facade_full_universe_backtest.rs`
- Modify: `crates/tqsdk/Cargo.toml`
- Modify: `crates/tqsdk/tests/facade_contract.rs`

- [ ] **Step 1: Write the public example first**

Create `crates/tqsdk/examples/api_contract_s45_facade_full_universe_backtest.rs`:

```rust
use tqsdk::prelude::*;

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    let start_ns = 1_781_172_000_000_000_000;
    let end_ns = 1_781_258_401_000_000_000;

    let mut tq = TqBuilder::new()
        .auth_env()?
        .backtest(start_ns, end_ns)
        .cache_dir(".tqsdk/backtest_ticks")?
        .universe("active:all;!CFFEX")?
        .prepare()
        .await?
        .connect()
        .await?;

    while tq.next().await? {
        let changed = tq.changed_symbols();
        for symbol in changed {
            let quote = tq.quote(symbol)?;
            if quote.last_price.is_finite() {
                println!("{symbol} {}", quote.last_price);
            }
        }
    }

    Ok(())
}
```

- [ ] **Step 2: Register the example**

In `crates/tqsdk/Cargo.toml`, add:

```toml
[[example]]
name = "api_contract_s45_facade_full_universe_backtest"
path = "examples/api_contract_s45_facade_full_universe_backtest.rs"
required-features = ["services", "live"]
```

- [ ] **Step 3: Add source contract assertions**

Append this test to `crates/tqsdk/tests/facade_contract.rs`:

```rust
#[test]
fn full_universe_backtest_contract_example_exposes_tick_canonical_flow() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/api_contract_s45_facade_full_universe_backtest.rs"
    );
    let source = std::fs::read_to_string(path).expect("read full-universe backtest example");
    for required in [
        ".backtest(start_ns, end_ns)",
        ".cache_dir(\".tqsdk/backtest_ticks\")?",
        ".universe(\"active:all;!CFFEX\")?",
        ".prepare()",
        "while tq.next().await?",
        "tq.changed_symbols()",
    ] {
        assert!(
            source.contains(required),
            "full-universe backtest example missing required flow fragment: {required}"
        );
    }
}
```

- [ ] **Step 4: Verify the contract test**

Run:

```bash
rtk cargo test -p tqsdk --test facade_contract full_universe_backtest_contract_example_exposes_tick_canonical_flow
```

Expected before implementation wiring: the source contract test passes once the file and example registration exist. Compilation of all examples may still fail until later tasks add `changed_symbols()`.

- [ ] **Step 5: Commit**

```bash
rtk git add crates/tqsdk/Cargo.toml crates/tqsdk/examples/api_contract_s45_facade_full_universe_backtest.rs crates/tqsdk/tests/facade_contract.rs
rtk git commit -m "test(tqsdk): add full-universe backtest contract"
```

---

### Task 2: Add `BacktestFrame` And Frame Grouping

**Files:**
- Create: `crates/tqsdk-task/src/backtest_frame.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`
- Create: `crates/tqsdk-task/tests/backtest_frame.rs`

- [ ] **Step 1: Run impact analysis**

Run GitNexus impact analysis for `ReplayMarketEvent`, `BacktestMarketStream`, and `StrategyBacktest::next`.

- [ ] **Step 2: Write failing frame tests**

Create `crates/tqsdk-task/tests/backtest_frame.rs`:

```rust
use tqsdk_core::Tick;
use tqsdk_task::{BacktestFrame, ReplayMarketEvent};

#[test]
fn backtest_frame_groups_events_by_market_time_and_sorts_symbols() {
    let rb = ReplayMarketEvent::tick(
        "cache",
        "SHFE.rb2601",
        2_000,
        Some(2_000),
        tick(2, 2_000, 101.0),
    )
    .unwrap();
    let au = ReplayMarketEvent::tick(
        "cache",
        "SHFE.au2608",
        2_000,
        Some(2_000),
        tick(7, 2_000, 501.0),
    )
    .unwrap();

    let frame = BacktestFrame::new(2_000, vec![rb, au]).unwrap();

    assert_eq!(frame.datetime_ns(), 2_000);
    assert_eq!(
        frame.changed_symbols(),
        &["SHFE.au2608".to_string(), "SHFE.rb2601".to_string()]
    );
    assert_eq!(frame.events().len(), 2);
}

#[test]
fn backtest_frame_rejects_mixed_market_time() {
    let left = ReplayMarketEvent::tick("cache", "SHFE.rb2601", 1_000, Some(1_000), tick(1, 1_000, 100.0)).unwrap();
    let right = ReplayMarketEvent::tick("cache", "SHFE.rb2601", 2_000, Some(2_000), tick(2, 2_000, 101.0)).unwrap();

    let err = BacktestFrame::new(1_000, vec![left, right]).unwrap_err();

    assert!(err.to_string().contains("mixed market time"));
}

fn tick(id: i64, datetime: i64, last_price: f64) -> Tick {
    Tick {
        id,
        datetime,
        last_price,
        bid_price1: last_price - 1.0,
        bid_volume1: 1,
        ask_price1: last_price + 1.0,
        ask_volume1: 1,
        ..Tick::default()
    }
}
```

- [ ] **Step 3: Implement the frame type**

Create `crates/tqsdk-task/src/backtest_frame.rs`:

```rust
use std::collections::BTreeSet;

use crate::{Error, ReplayMarketEvent, Result};

#[derive(Debug, Clone)]
pub struct BacktestFrame {
    datetime_ns: i64,
    events: Vec<ReplayMarketEvent>,
    changed_symbols: Vec<String>,
}

impl BacktestFrame {
    pub fn new(datetime_ns: i64, events: Vec<ReplayMarketEvent>) -> Result<Self> {
        if events
            .iter()
            .any(|event| event.event_time_ns() != datetime_ns)
        {
            return Err(Error::invalid_config("backtest frame contains mixed market time"));
        }
        let changed_symbols = events
            .iter()
            .map(|event| event.symbol().to_owned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Ok(Self {
            datetime_ns,
            events,
            changed_symbols,
        })
    }

    #[must_use]
    pub fn datetime_ns(&self) -> i64 {
        self.datetime_ns
    }

    #[must_use]
    pub fn events(&self) -> &[ReplayMarketEvent] {
        &self.events
    }

    #[must_use]
    pub fn changed_symbols(&self) -> &[String] {
        &self.changed_symbols
    }
}
```

- [ ] **Step 4: Export the type**

In `crates/tqsdk-task/src/lib.rs`, add:

```rust
mod backtest_frame;
pub use backtest_frame::BacktestFrame;
```

- [ ] **Step 5: Run tests**

```bash
rtk cargo test -p tqsdk-task --test backtest_frame
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
rtk git add crates/tqsdk-task/src/backtest_frame.rs crates/tqsdk-task/src/lib.rs crates/tqsdk-task/tests/backtest_frame.rs
rtk git commit -m "feat(task): add backtest market-time frames"
```

---

### Task 3: Convert Cached Tick Replay To Frame Stream

**Files:**
- Modify: `crates/tqsdk-task/src/backtest_stream.rs`
- Modify: `crates/tqsdk-task/src/history_tick_replay.rs`
- Modify: `crates/tqsdk-task/tests/history_tick_replay.rs`

- [ ] **Step 1: Add frame-stream behavior tests**

In `crates/tqsdk-task/tests/history_tick_replay.rs`, add a test that writes two symbols with the same `datetime` and asserts the stream yields one `BacktestFrame` with both symbols:

```rust
#[tokio::test]
async fn history_tick_replay_groups_same_datetime_ticks_into_one_frame() {
    let cache = populated_tick_cache([
        ("SHFE.rb2601", vec![tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)]),
        ("SHFE.au2608", vec![tick(9, 2_000, 500.0)]),
    ]);
    let mut stream = HistoryTickReplayStream::from_cache(
        &cache,
        ["SHFE.rb2601".to_string(), "SHFE.au2608".to_string()],
        1_000,
        3_000,
    )
    .unwrap();

    let first = stream.next_frame().await.unwrap().unwrap();
    assert_eq!(first.datetime_ns(), 1_000);
    assert_eq!(first.changed_symbols(), &["SHFE.rb2601".to_string()]);

    let second = stream.next_frame().await.unwrap().unwrap();
    assert_eq!(second.datetime_ns(), 2_000);
    assert_eq!(
        second.changed_symbols(),
        &["SHFE.au2608".to_string(), "SHFE.rb2601".to_string()]
    );
    assert_eq!(second.events().len(), 2);
}
```

- [ ] **Step 2: Extend the stream trait**

In `crates/tqsdk-task/src/backtest_stream.rs`, change the trait to expose frames:

```rust
#[async_trait::async_trait]
pub trait BacktestMarketStream: Send {
    async fn next_frame(&mut self) -> Result<Option<BacktestFrame>>;
}
```

Keep the existing `ReplayMarketStream` adapter by grouping one replay event into a one-event frame.

- [ ] **Step 3: Group heap output by datetime**

In `crates/tqsdk-task/src/history_tick_replay.rs`, implement `next_frame()` so it pops the first heap item, then continues popping all heap heads with the same `datetime`. Convert each row to `ReplayMarketEvent::tick("cache", symbol, row.datetime, Some(row.datetime), row)`.

- [ ] **Step 4: Run focused tests**

```bash
rtk cargo test -p tqsdk-task --test history_tick_replay
rtk cargo test -p tqsdk-task --test backtest_frame
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
rtk git add crates/tqsdk-task/src/backtest_stream.rs crates/tqsdk-task/src/history_tick_replay.rs crates/tqsdk-task/tests/history_tick_replay.rs
rtk git commit -m "feat(task): replay cached ticks as market-time frames"
```

---

### Task 4: Make `StrategyBacktest` Advance Once Per Frame

**Files:**
- Modify: `crates/tqsdk-task/src/backtest.rs`
- Modify: `crates/tqsdk-task/tests/strategy_backtest.rs`

- [ ] **Step 1: Add behavior test**

In `crates/tqsdk-task/tests/strategy_backtest.rs`, add:

```rust
#[tokio::test]
async fn strategy_backtest_runs_one_strategy_step_for_same_datetime_frame() {
    let frame = BacktestFrame::new(
        2_000,
        vec![
            ReplayMarketEvent::tick("test", "SHFE.rb2601", 2_000, Some(2_000), tick(1, 2_000, 100.0)).unwrap(),
            ReplayMarketEvent::tick("test", "SHFE.au2608", 2_000, Some(2_000), tick(2, 2_000, 500.0)).unwrap(),
        ],
    )
    .unwrap();
    let replay = ReplayMarketStream::from_frames([frame]);
    let mut backtest = StrategyBacktest::builder_stream(Box::new(replay))
        .quote("SHFE.rb2601")
        .quote("SHFE.au2608")
        .build()
        .await
        .unwrap();

    let ctx = backtest.next().await.unwrap().unwrap();

    assert_eq!(ctx.event().event_time_ns(), 2_000);
    assert_eq!(ctx.changed_symbols(), &["SHFE.au2608".to_string(), "SHFE.rb2601".to_string()]);
    assert!(backtest.next().await.unwrap().is_none());
    assert_eq!(backtest.summary().event_count(), 1);
    assert_eq!(backtest.summary().tick_count(), 2);
}
```

- [ ] **Step 2: Change `StrategyBacktest::next`**

Update `StrategyBacktest::next` so it calls `self.replay.next_frame().await?`, ingests every event in the frame, records payload counts for every event, records one snapshot at `frame.datetime_ns()`, and then calls `strategy.next_once().await?` exactly once.

- [ ] **Step 3: Expose changed symbols on context**

Add to `StrategyBacktestContext`:

```rust
changed_symbols: Vec<String>,
```

and expose:

```rust
#[must_use]
pub fn changed_symbols(&self) -> &[String] {
    &self.changed_symbols
}
```

- [ ] **Step 4: Preserve single-event replay compatibility**

Keep `StrategyBacktest::builder(replay: ReplayMarketSource)` by wrapping `ReplayMarketSource` in `ReplayMarketStream::from_replay_source(replay)`.

- [ ] **Step 5: Run regression tests**

```bash
rtk cargo test -p tqsdk-task --test strategy_backtest
```

Expected: existing local replay behavior still passes after adjusting event-count assertions that intentionally change from per-event to per-frame.

- [ ] **Step 6: Commit**

```bash
rtk git add crates/tqsdk-task/src/backtest.rs crates/tqsdk-task/tests/strategy_backtest.rs
rtk git commit -m "feat(task): advance backtests by market-time frame"
```

---

### Task 5: Add Tick-Derived K-Line Aggregator

**Files:**
- Create: `crates/tqsdk-task/src/kline_aggregator.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`
- Create: `crates/tqsdk-task/tests/kline_aggregator.rs`

- [ ] **Step 1: Write aggregation tests**

Create `crates/tqsdk-task/tests/kline_aggregator.rs`:

```rust
use tqsdk_core::Tick;
use tqsdk_task::{KlineAggregationRequest, KlineAggregator};

#[test]
fn kline_aggregator_builds_ohlc_from_ticks() {
    let mut agg = KlineAggregator::default();
    agg.subscribe(KlineAggregationRequest::new("SHFE.rb2601", 60_000_000_000, 3)).unwrap();

    agg.on_tick("SHFE.rb2601", &tick(1, 60_000_000_000, 100.0)).unwrap();
    agg.on_tick("SHFE.rb2601", &tick(2, 61_000_000_000, 105.0)).unwrap();
    agg.on_tick("SHFE.rb2601", &tick(3, 62_000_000_000, 99.0)).unwrap();

    let rows = agg.rows("SHFE.rb2601", 60_000_000_000).unwrap();
    let row = rows.back().unwrap();
    assert_eq!(row.datetime, 60_000_000_000);
    assert_eq!(row.open, 100.0);
    assert_eq!(row.high, 105.0);
    assert_eq!(row.low, 99.0);
    assert_eq!(row.close, 99.0);
}

#[test]
fn kline_aggregator_rolls_to_next_bucket() {
    let mut agg = KlineAggregator::default();
    agg.subscribe(KlineAggregationRequest::new("SHFE.rb2601", 60_000_000_000, 3)).unwrap();

    agg.on_tick("SHFE.rb2601", &tick(1, 60_000_000_000, 100.0)).unwrap();
    agg.on_tick("SHFE.rb2601", &tick(2, 120_000_000_000, 101.0)).unwrap();

    let rows = agg.rows("SHFE.rb2601", 60_000_000_000).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].close, 100.0);
    assert_eq!(rows[1].open, 101.0);
}

fn tick(id: i64, datetime: i64, last_price: f64) -> Tick {
    Tick {
        id,
        datetime,
        last_price,
        volume: id,
        open_interest: 100 + id,
        ..Tick::default()
    }
}
```

- [ ] **Step 2: Implement request and aggregator**

Create `crates/tqsdk-task/src/kline_aggregator.rs` with `KlineAggregationRequest`, `KlineAggregator`, and a `HashMap<(String, i64), VecDeque<Kline>>`. Use `duration_ns` bucket start: `datetime - datetime.rem_euclid(duration_ns)`.

- [ ] **Step 3: Validate inputs**

Return `Error::invalid_config("kline duration_ns must be positive")` for non-positive durations, `Error::invalid_config("kline data_length must be positive")` for zero length, and `Error::invalid_config("kline symbol must not be empty")` for empty symbols.

- [ ] **Step 4: Export the aggregator**

In `crates/tqsdk-task/src/lib.rs`, add:

```rust
mod kline_aggregator;
pub use kline_aggregator::{KlineAggregationRequest, KlineAggregator};
```

- [ ] **Step 5: Run tests**

```bash
rtk cargo test -p tqsdk-task --test kline_aggregator
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
rtk git add crates/tqsdk-task/src/kline_aggregator.rs crates/tqsdk-task/src/lib.rs crates/tqsdk-task/tests/kline_aggregator.rs
rtk git commit -m "feat(task): derive kline serials from ticks"
```

---

### Task 6: Wire Tick-Derived K Lines Into Backtest Runtime

**Files:**
- Modify: `crates/tqsdk-task/src/backtest.rs`
- Modify: `crates/tqsdk-task/tests/strategy_backtest.rs`

- [ ] **Step 1: Add strategy-level K-line test**

In `crates/tqsdk-task/tests/strategy_backtest.rs`, add:

```rust
#[tokio::test]
async fn strategy_backtest_updates_kline_serials_from_tick_frames() {
    let replay = ReplayMarketSource::default()
        .tick_row("SHFE.rb2601", tick(1, 60_000_000_000, 100.0), "test")
        .unwrap()
        .tick_row("SHFE.rb2601", tick(2, 61_000_000_000, 101.0), "test")
        .unwrap();
    let mut backtest = StrategyBacktest::builder(replay)
        .quote("SHFE.rb2601")
        .kline("SHFE.rb2601", 60_000_000_000, 10)
        .build()
        .await
        .unwrap();

    let _ = backtest.next().await.unwrap().unwrap();
    let ctx = backtest.next().await.unwrap().unwrap();
    let rows = ctx.kline_rows("SHFE.rb2601", 60_000_000_000).unwrap();

    assert_eq!(rows.back().unwrap().close, 101.0);
}
```

- [ ] **Step 2: Add builder subscription method**

Add to `StrategyBacktestBuilder`:

```rust
pub fn kline(mut self, symbol: impl AsRef<str>, duration_ns: i64, data_length: usize) -> Self {
    self.kline_requests.push(KlineAggregationRequest::new(symbol.as_ref(), duration_ns, data_length));
    self
}
```

- [ ] **Step 3: Update frame ingest**

When a frame event payload is `ReplayMarketPayload::Tick(tick)`, update quote from tick and then call `self.kline_aggregator.on_tick(event.symbol(), tick)?`.

- [ ] **Step 4: Add context accessor**

Add to `StrategyBacktestContext`:

```rust
pub fn kline_rows(&self, symbol: impl AsRef<str>, duration_ns: i64) -> Result<&std::collections::VecDeque<tqsdk_core::Kline>> {
    self.context.kline_rows(symbol, duration_ns)
}
```

If `StrategyContext` does not yet expose this shape, add a focused accessor that reads from the backtest-owned aggregator rather than the live runtime chart store.

- [ ] **Step 5: Run tests**

```bash
rtk cargo test -p tqsdk-task --test strategy_backtest strategy_backtest_updates_kline_serials_from_tick_frames
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
rtk git add crates/tqsdk-task/src/backtest.rs crates/tqsdk-task/tests/strategy_backtest.rs
rtk git commit -m "feat(task): expose tick-derived kline state in backtests"
```

---

### Task 7: Introduce `FillModel` And Default Tick Fill Model

**Files:**
- Create: `crates/tqsdk-task/src/fill_model.rs`
- Modify: `crates/tqsdk-task/src/backtest.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`
- Modify: `crates/tqsdk-task/tests/strategy_backtest.rs`

- [ ] **Step 1: Write fill-model behavior test**

Add a test that places a limit order before a later tick frame and asserts the default fill model fills only when the tick quote crosses the order price. Keep the existing `strategy_backtest_fills_alive_limit_order_on_later_quote` as the baseline and add:

```rust
#[tokio::test]
async fn tick_fill_model_fills_pending_order_on_crossing_tick_frame() {
    let replay = ReplayMarketSource::default()
        .tick_row("SHFE.rb2601", tick(1, 1_000, 100.0), "test")
        .unwrap()
        .tick_row("SHFE.rb2601", tick(2, 2_000, 105.0), "test")
        .unwrap();
    let mut backtest = StrategyBacktest::builder(replay)
        .quote("SHFE.rb2601")
        .build()
        .await
        .unwrap();

    let mut ctx = backtest.next().await.unwrap().unwrap();
    ctx.insert_order("SHFE.rb2601", Direction::Buy, Offset::Open, 106.0, 1).unwrap();

    let _ = backtest.next().await.unwrap().unwrap();

    assert_eq!(backtest.summary().trades().len(), 1);
}
```

- [ ] **Step 2: Define the trait**

Create `crates/tqsdk-task/src/fill_model.rs`:

```rust
use crate::{BacktestFrame, Result, TqSim};

pub trait FillModel: Send {
    fn on_frame_before_strategy(&mut self, sim: &mut TqSim, frame: &BacktestFrame) -> Result<()>;
    fn on_order_inserted(&mut self, sim: &mut TqSim, symbol: &str) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct TickFillModel;

impl FillModel for TickFillModel {
    fn on_frame_before_strategy(&mut self, sim: &mut TqSim, frame: &BacktestFrame) -> Result<()> {
        for symbol in frame.changed_symbols() {
            sim.try_match_symbol(symbol)?;
        }
        Ok(())
    }

    fn on_order_inserted(&mut self, sim: &mut TqSim, symbol: &str) -> Result<()> {
        sim.try_match_symbol(symbol)
    }
}
```

If `TqSim::try_match_symbol` does not exist, add the smallest public-within-crate method that delegates to the current matching path without duplicating matching logic.

- [ ] **Step 3: Use the trait in `StrategyBacktest`**

Store `fill_model: Box<dyn FillModel>` in `StrategyBacktest`, default it to `Box::new(TickFillModel)`, and call `on_frame_before_strategy` after all tick quote updates and before `strategy.next_once()`.

- [ ] **Step 4: Export types**

In `crates/tqsdk-task/src/lib.rs`, add:

```rust
mod fill_model;
pub use fill_model::{FillModel, TickFillModel};
```

- [ ] **Step 5: Run tests**

```bash
rtk cargo test -p tqsdk-task --test strategy_backtest tick_fill_model_fills_pending_order_on_crossing_tick_frame
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
rtk git add crates/tqsdk-task/src/fill_model.rs crates/tqsdk-task/src/backtest.rs crates/tqsdk-task/src/lib.rs crates/tqsdk-task/tests/strategy_backtest.rs
rtk git commit -m "feat(task): isolate backtest fill model"
```

---

### Task 8: Expose Changed Symbols On The `Tq` Facade

**Files:**
- Modify: `crates/tqsdk/src/lib.rs`
- Modify: `crates/tqsdk/tests/facade_contract.rs`

- [ ] **Step 1: Add facade test**

Add to `crates/tqsdk/tests/facade_contract.rs`:

```rust
#[tokio::test]
async fn facade_backtest_exposes_changed_symbols_for_current_frame() {
    let replay = ReplayMarketSource::default()
        .tick_row("SHFE.rb2601", tick(1, 1_000, 100.0), "test")
        .unwrap()
        .tick_row("SHFE.au2608", tick(2, 1_000, 500.0), "test")
        .unwrap();
    let mut tq = TqBuilder::new()
        .replay_backtest(replay)
        .quote_symbol("SHFE.rb2601")
        .quote_symbol("SHFE.au2608")
        .connect()
        .await
        .unwrap();

    assert!(tq.next().await.unwrap());
    assert_eq!(
        tq.changed_symbols(),
        &["SHFE.au2608".to_string(), "SHFE.rb2601".to_string()]
    );
}
```

- [ ] **Step 2: Store last changed symbols**

In `TqInner::LocalBacktest`, retain the last context changed symbol list after each `next()` call.

- [ ] **Step 3: Add public accessor**

Add to `impl Tq`:

```rust
#[must_use]
pub fn changed_symbols(&self) -> &[String] {
    match &self.inner {
        TqInner::LocalBacktest(inner) => inner.changed_symbols(),
        _ => &[],
    }
}
```

Use an empty static slice helper if the current enum shape cannot return `&[]` with the desired lifetime.

- [ ] **Step 4: Run tests and example check**

```bash
rtk cargo test -p tqsdk --test facade_contract facade_backtest_exposes_changed_symbols_for_current_frame
rtk cargo check -p tqsdk --example api_contract_s45_facade_full_universe_backtest --features services,live
```

Expected: both pass once previous tasks are complete.

- [ ] **Step 5: Commit**

```bash
rtk git add crates/tqsdk/src/lib.rs crates/tqsdk/tests/facade_contract.rs
rtk git commit -m "feat(tqsdk): expose changed symbols for backtest frames"
```

---

### Task 9: Update Remote-On-Miss Stream To Emit Frames

**Files:**
- Modify: `crates/tqsdk/src/backtest_remote.rs`
- Modify: `crates/tqsdk/tests/facade_contract.rs`

- [ ] **Step 1: Add remote stream unit boundary test**

If `RemoteBacktestCachingStream` has test seams, add a pure helper test that feeds changed rows with the same datetime and asserts one frame is emitted. If it does not, extract a pure helper:

```rust
fn ticks_to_frames(source: &str, rows: Vec<(String, Tick)>) -> Result<Vec<BacktestFrame>>
```

and test that helper.

- [ ] **Step 2: Group changed rows before emission**

In `RemoteBacktestCachingStream::next_frame`, gather all changed rows from the current official backtest `step`, sort by `(datetime, id, symbol)`, deduplicate by `(symbol, id)`, append rows to cache-fill accumulators, and return the earliest complete same-datetime frame.

- [ ] **Step 3: Preserve completeness validation**

Keep final completion checks:

```rust
last_tick_near_end_ns(end_ns, tolerance_ns)
last_id - first_id + 1 == rows.len() as i64
```

Do not mark cache coverage complete until the fill accumulator passes integrity checks for every requested symbol.

- [ ] **Step 4: Run focused tests**

```bash
rtk cargo test -p tqsdk --test facade_contract backtest
```

Expected: cache-hit and remote-miss contract tests pass. Tests that require real auth remain ignored or env-gated.

- [ ] **Step 5: Commit**

```bash
rtk git add crates/tqsdk/src/backtest_remote.rs crates/tqsdk/tests/facade_contract.rs
rtk git commit -m "feat(tqsdk): emit remote backtest ticks as frames"
```

---

### Task 10: Document The Final Semantics And Verification

**Files:**
- Modify: `README.md`
- Modify: `crates/tqsdk/README.md`
- Modify: `crates/tqsdk-task/README.md`
- Modify: `docs/architecture/api-task.md`
- Modify: `docs/architecture/api-data.md`
- Modify: `docs/architecture/validation.md`

- [ ] **Step 1: Document user-facing semantics**

Add this core wording to `crates/tqsdk/README.md`:

```markdown
### Full-universe backtest

`TqBuilder::backtest(start_ns, end_ns)` is the default strategy backtest entry. It keeps the same strategy body across live, paper, and backtest modes, while the backtest data path is tick-canonical:

- the persistent tick cache is the primary historical store;
- cache misses are filled through official server-side backtest market streams, not professional history-query APIs;
- K lines are derived from ticks during replay;
- each `next()` advances one market-time frame and `changed_symbols()` returns the symbols changed in that frame.
```

- [ ] **Step 2: Document crate boundaries**

In `docs/architecture/api-task.md`, state that `tqsdk-task` owns `BacktestFrame`, tick-derived K-line aggregation, and fill-model abstractions. In `docs/architecture/api-data.md`, state that `tqsdk-data` owns persistent tick storage and universe resolution but not strategy execution.

- [ ] **Step 3: Document validation commands**

In `docs/architecture/validation.md`, add:

```bash
rtk cargo test -p tqsdk-task --test backtest_frame
rtk cargo test -p tqsdk-task --test history_tick_replay
rtk cargo test -p tqsdk-task --test kline_aggregator
rtk cargo test -p tqsdk-task --test strategy_backtest
rtk cargo test -p tqsdk --test facade_contract
rtk cargo check -p tqsdk --example api_contract_s45_facade_full_universe_backtest --features services,live
```

- [ ] **Step 4: Run doc hygiene**

```bash
rtk git diff --check
```

Expected: no whitespace errors.

- [ ] **Step 5: Commit**

```bash
rtk git add README.md crates/tqsdk/README.md crates/tqsdk-task/README.md docs/architecture/api-task.md docs/architecture/api-data.md docs/architecture/validation.md
rtk git commit -m "docs: document tick-canonical full-universe backtests"
```

---

### Task 11: Full Verification

**Files:**
- No file edits unless verification finds defects.

- [ ] **Step 1: Run task-level tests**

```bash
rtk cargo test -p tqsdk-task --test backtest_frame
rtk cargo test -p tqsdk-task --test history_tick_replay
rtk cargo test -p tqsdk-task --test kline_aggregator
rtk cargo test -p tqsdk-task --test strategy_backtest
```

Expected: all pass.

- [ ] **Step 2: Run facade tests**

```bash
rtk cargo test -p tqsdk --test facade_contract
```

Expected: all pass.

- [ ] **Step 3: Run example check**

```bash
rtk cargo check -p tqsdk --example api_contract_s45_facade_full_universe_backtest --features services,live
```

Expected: pass.

- [ ] **Step 4: Run broader compile check**

```bash
rtk cargo check --examples
```

Expected: pass.

- [ ] **Step 5: Run formatting and diff checks**

```bash
rtk cargo fmt --all --check
rtk git diff --check
```

Expected: both pass.

- [ ] **Step 6: Run impact detection before final commit or merge**

Run GitNexus detect changes.

Expected: changed symbols and flows are limited to backtest/cache/universe/frame/K-line/fill-model scope.

---

## Self-Review

- Spec coverage: the plan covers the 10 requested improvements by mapping them to canonical tick storage, a single `backtest()` entry, universe planning, prepared reports from the prerequisite plan, market-time frames, tick-derived K lines, fill-model separation, Rust ownership/error/dispatch boundaries, cache-primary invariants, and semantic contract tests.
- Placeholder scan: no step uses placeholder-marker or fill-later language. Helper extraction is conditional only where the current code seam determines the exact file-local helper shape.
- Type consistency: `BacktestFrame`, `KlineAggregator`, `KlineAggregationRequest`, `FillModel`, `TickFillModel`, and `changed_symbols()` are introduced before later tasks consume them.
- Deliberate divergence from Python: Python-style facade and one-market-time advancement are preserved; Python K-line synthesized execution is not the default because Rust backtest execution is tick-canonical.
