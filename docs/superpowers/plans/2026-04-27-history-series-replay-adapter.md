# History Series Replay Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let history data users turn owned `KlineDataSeries` / `TickDataSeries` into standard `MarketCacheEvent` / `MarketCacheReplay` without writing event conversion loops.

**Architecture:** Keep the adapter in `tqsdk-data`, because history series and cache events both belong to the research/offline data layer. `tqsdk-task::StrategyReplay` continues to consume only `MarketCacheReplay`; task does not learn how to fetch history and data does not learn how to execute strategies. Replay clock/checkpoint and live/sim/replay environment adapters remain out of scope.

**Tech Stack:** Rust 2024, `tqsdk-data`, `tqsdk-core` schema types, Cargo examples, existing API contract docs.

---

## File Structure

- Modify `crates/tqsdk-data/src/client.rs`
  - Add public conversion methods on `KlineDataSeries` and `TickDataSeries`.
  - Add unit tests using private `KlineDataSeries::new` / `TickDataSeries::new`.
- Create `crates/tqsdk-data/examples/api_contract_s16_history_series_replay_adapter.rs`
  - Formal scenario contract showing `DataClient` history series feeding `StrategyReplay` through `MarketCacheReplay`.
- Modify `docs/public-api-scenario-review.md`
  - Update S16 evidence from “history adapter still gap” to “history series adapter foundation works”.
- Modify `docs/scenarios/api_gaps/api_contract_s16_history_replay_strategy.rs`
  - Narrow remaining gap to replay clock/checkpoint and full live/sim/replay environment.
- Modify `docs/scenarios/user-layer-iteration-plan.md`
  - Move DataClient history series adapter to landed status.
- Modify `crates/tqsdk-data/README.md`, `crates/tqsdk-task/README.md`, `docs/architecture/api-data.md`, `docs/architecture/api-task.md`
  - Update boundary notes to reflect the adapter while preserving task/data separation.

## Task 1: TDD Data Adapter

**Files:**
- Modify: `crates/tqsdk-data/src/client.rs`

- [ ] **Step 1: Write failing tests**

Add tests to the existing `#[cfg(test)] mod tests` in `client.rs`:

```rust
#[test]
fn kline_data_series_converts_to_market_cache_replay() {
    let series = KlineDataSeries::new(
        "SHFE.au2602".to_string(),
        60_000_000_000,
        1_000,
        3_000,
        vec![
            Kline {
                id: 2,
                datetime: 2_000,
                close: 481.0,
                ..Kline::default()
            },
            Kline {
                id: 1,
                datetime: 1_000,
                close: 480.0,
                ..Kline::default()
            },
        ],
    );

    let events: Vec<_> = series
        .into_market_cache_replay("history")
        .unwrap()
        .collect();

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].source, "history");
    assert_eq!(events[0].symbol, "SHFE.au2602");
    assert_eq!(events[0].received_at_ns, 1_000);
    assert_eq!(events[0].exchange_time_ns, Some(1_000));
    match &events[0].payload {
        MarketCachePayload::Kline { duration_ns, row } => {
            assert_eq!(*duration_ns, 60_000_000_000);
            assert_eq!(row.close, 480.0);
        }
        _ => panic!("expected kline payload"),
    }
}

#[test]
fn tick_data_series_converts_to_market_cache_events() {
    let series = TickDataSeries::new(
        "SHFE.au2602".to_string(),
        1_000,
        3_000,
        vec![
            Tick {
                id: 1,
                datetime: 1_000,
                last_price: 480.5,
                ..Tick::default()
            },
            Tick {
                id: 2,
                datetime: 2_000,
                last_price: 481.5,
                ..Tick::default()
            },
        ],
    );

    let events = series.into_market_cache_events("history").unwrap();

    assert_eq!(events.len(), 2);
    assert_eq!(events[1].source, "history");
    assert_eq!(events[1].symbol, "SHFE.au2602");
    assert_eq!(events[1].received_at_ns, 2_000);
    assert_eq!(events[1].exchange_time_ns, Some(2_000));
    match &events[1].payload {
        MarketCachePayload::Tick(row) => assert_eq!(row.last_price, 481.5),
        _ => panic!("expected tick payload"),
    }
}
```

- [ ] **Step 2: Run red tests**

Run:

```bash
cargo test -p tqsdk-data kline_data_series_converts_to_market_cache_replay tick_data_series_converts_to_market_cache_events -- --nocapture
```

Expected: fail because `into_market_cache_replay` and `into_market_cache_events` do not exist.

- [ ] **Step 3: Implement minimal adapter**

Add imports in `client.rs`:

```rust
use crate::market_cache::{MarketCacheEvent, MarketCacheReplay};
```

Add methods:

```rust
impl KlineDataSeries {
    pub fn into_market_cache_events(self, source: impl AsRef<str>) -> Result<Vec<MarketCacheEvent>> {
        let source = source.as_ref();
        let symbol = self.symbol;
        let duration_ns = self.duration_ns;
        self.rows
            .into_iter()
            .map(|row| {
                MarketCacheEvent::kline(
                    source,
                    symbol.as_str(),
                    row.datetime,
                    Some(row.datetime),
                    duration_ns,
                    row,
                )
            })
            .collect()
    }

    pub fn into_market_cache_replay(self, source: impl AsRef<str>) -> Result<MarketCacheReplay> {
        Ok(MarketCacheReplay::new(self.into_market_cache_events(source)?))
    }
}

impl TickDataSeries {
    pub fn into_market_cache_events(self, source: impl AsRef<str>) -> Result<Vec<MarketCacheEvent>> {
        let source = source.as_ref();
        let symbol = self.symbol;
        self.rows
            .into_iter()
            .map(|row| {
                MarketCacheEvent::tick(source, symbol.as_str(), row.datetime, Some(row.datetime), row)
            })
            .collect()
    }

    pub fn into_market_cache_replay(self, source: impl AsRef<str>) -> Result<MarketCacheReplay> {
        Ok(MarketCacheReplay::new(self.into_market_cache_events(source)?))
    }
}
```

- [ ] **Step 4: Run green tests**

Run:

```bash
cargo test -p tqsdk-data kline_data_series_converts_to_market_cache_replay tick_data_series_converts_to_market_cache_events -- --nocapture
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add crates/tqsdk-data/src/client.rs
git commit -m "feat: add history series replay adapters"
```

## Task 2: Scenario Contract And Docs

**Files:**
- Create: `crates/tqsdk-data/examples/api_contract_s16_history_series_replay_adapter.rs`
- Modify: `docs/public-api-scenario-review.md`
- Modify: `docs/scenarios/api_gaps/api_contract_s16_history_replay_strategy.rs`
- Modify: `docs/scenarios/user-layer-iteration-plan.md`
- Modify: `crates/tqsdk-data/README.md`
- Modify: `crates/tqsdk-task/README.md`
- Modify: `docs/architecture/api-data.md`
- Modify: `docs/architecture/api-task.md`

- [ ] **Step 1: Add formal example**

Create an API contract example that:

- Uses `DataClient::get_kline_data_series(...)`.
- Calls `series.into_market_cache_replay("history")?`.
- Feeds the replay into `tqsdk_task::StrategyReplay`.
- Avoids runtime internals, protocol JSON, channels, and `Arc<Mutex<_>>`.

- [ ] **Step 2: Update docs**

Update review and architecture docs to say:

- S16 history series -> replay adapter foundation is landed.
- Remaining gaps are replay speed/clock, checkpoint/resume, full live/sim/replay environment, and multi-series convenience builders.
- `tqsdk-data` owns history/cache materialization; `tqsdk-task` owns strategy replay consumption.

- [ ] **Step 3: Verify examples and commit**

Run:

```bash
cargo check -p tqsdk-data --example api_contract_s16_history_series_replay_adapter
scripts/check_api_contract_examples.sh
```

Expected: pass.

Commit:

```bash
git add crates/tqsdk-data/examples/api_contract_s16_history_series_replay_adapter.rs docs/public-api-scenario-review.md docs/scenarios/api_gaps/api_contract_s16_history_replay_strategy.rs docs/scenarios/user-layer-iteration-plan.md crates/tqsdk-data/README.md crates/tqsdk-task/README.md docs/architecture/api-data.md docs/architecture/api-task.md
git commit -m "docs: promote history series replay adapter"
```

## Task 3: Full Verification

**Files:** none unless verification finds a defect.

- [ ] **Step 1: Run required checks**

Run:

```bash
scripts/check_api_contract_examples.sh
cargo check --workspace --examples
cargo test --workspace
cargo clippy --workspace --examples --all-targets -- -D warnings
```

Expected: all pass.

- [ ] **Step 2: Final status**

Run:

```bash
git status --short
git log --oneline -10
```

Expected: only unrelated untracked files remain.

## Self-Review

- Spec coverage: covers the next recommended S16 bottom-layer gap only.
- Placeholder scan: no placeholders; replay clock/checkpoint and environment adapter are explicitly out of scope.
- Type consistency: adapter methods are on owned `KlineDataSeries` / `TickDataSeries` and return existing `MarketCacheEvent` / `MarketCacheReplay` types.
