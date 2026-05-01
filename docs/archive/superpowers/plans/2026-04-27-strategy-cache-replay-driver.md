# Strategy Cache Replay Driver Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the next S16 foundation: drive a `StrategyHost` from ordered `tqsdk-data::MarketCacheReplay` events so replayed quote/kline/tick data reaches the same strategy context shape as live/test execution.

**Architecture:** Keep replay orchestration in `tqsdk-task`, because the user-facing contract is strategy execution, not data storage. Reuse `tqsdk-data` only as the offline cache event source and keep the runtime state path unchanged: replay events are converted into normal market DIFF commits that `tqsdk-wait` / `StrategyContext` already know how to read. Do not add live/sim/replay environment switching, wall-clock replay speed, durable sink runtime, or a second state tree in this batch.

**Tech Stack:** Rust 2024, Tokio tests, existing `tqsdk-core` runtime ingestion, `tqsdk-wait` serial refs/windows, `tqsdk-data` `MarketCacheReplay`, `tqsdk-task` `StrategyHost` / `StrategyTestHarness`.

---

## Batch Scope

This batch promotes a **foundation subset** of S16:

- strategy code can receive ordered offline cache events through a `StrategyReplay` host;
- replayed quote/kline/tick payloads materialize into normal runtime market state;
- `StrategyContext` can read quote, kline window, tick window, account, position and submit fake-broker orders during replay;
- S16 remains `勉强`, not `自然`, because there is still no live/sim/replay environment abstraction, replay speed controller, deterministic clock, or direct history-series-to-strategy adapter.

This batch must not:

- add `tqsdk-data` dependency to `tqsdk-core`, `tqsdk-session`, `tqsdk-wait`, or `tqsdk-stream`;
- expose provider protocol or `RuntimeInput` in examples;
- require users to call `handle_for_test`, create channels, or manage `Arc<Mutex<_>>`;
- move cache/replay state into a facade-private tree;
- claim full S15 live/sim/replay switching.

## File Structure

- Modify `crates/tqsdk-task/Cargo.toml`
  - Add `tqsdk-data = { path = "../tqsdk-data", default-features = false }`.
- Modify `crates/tqsdk-task/src/strategy.rs`
  - Add kline/tick subscription specs to `StrategyHostBuilder`.
  - Store public wait-layer `KlineSerialRef` / `TickSerialRef` handles inside `StrategyHost`.
  - Add `StrategyContext::kline(...)` and `StrategyContext::tick(...)`.
- Create `crates/tqsdk-task/src/replay.rs`
  - Define `StrategyReplayBuilder`, `StrategyReplay`, `StrategyReplayEvent`, and `StrategyReplayContext`.
  - Convert `MarketCacheEvent` payloads into runtime market commits.
- Modify `crates/tqsdk-task/src/lib.rs`
  - Re-export replay types.
- Create `crates/tqsdk-task/tests/strategy_replay.rs`
  - Cover quote replay into context, kline replay into context, event ordering, and fake broker order submission during replay.
- Create `crates/tqsdk-task/examples/api_contract_s16_history_replay_strategy.rs`
  - Formal S16 foundation example.
- Modify `docs/reviews/public-api-scenario-review.md`
  - Update S16 from `不自然` to `勉强` with foundation evidence and remaining gaps.
- Modify `docs/scenarios/api_gaps/api_contract_s16_history_replay_strategy.rs`
  - Narrow remaining gap to history-series adapter, replay clock/speed, and live/sim/replay environment switching.
- Modify `docs/scenarios/user-layer-iteration-plan.md`
  - Add this batch under P1 strategy runtime / testability and P2 cache/replay.
- Modify `crates/tqsdk-task/README.md`
  - Document `StrategyReplay` as an offline cache replay foundation.
- Modify architecture docs if implementation confirms the new dependency:
  - `docs/architecture/ai-workflow.md`
  - `docs/architecture/README.md`
  - `docs/architecture/crate-boundaries.md`
  - `docs/architecture/api-task.md`

## Public API Shape

Target user code:

```rust
use std::time::Duration;

use tqsdk_core::{Kline, Quote};
use tqsdk_data::{MarketCacheEvent, MarketCacheReplay};
use tqsdk_task::testing::{FakeBroker, FakeMarket};
use tqsdk_task::StrategyReplay;

# async fn run() -> tqsdk_task::Result<()> {
let quote = Quote {
    last_price: 480.5,
    ..Quote::default()
};
let kline = Kline {
    id: 1,
    datetime: 1_000,
    close: 481.0,
    ..Kline::default()
};

let replay = MarketCacheReplay::new(vec![
    MarketCacheEvent::quote("cache", "SHFE.au2602", 1_000, Some(900), quote)?,
    MarketCacheEvent::kline(
        "cache",
        "SHFE.au2602",
        2_000,
        Some(1_900),
        60_000_000_000,
        kline,
    )?,
]);

let mut strategy = StrategyReplay::builder(replay)
    .market(
        FakeMarket::new()
            .account("sim", 100_000.0)
            .position("sim", "SHFE.au2602", 0),
    )
    .broker(FakeBroker::new().fill_all())
    .account("sim")
    .quote("SHFE.au2602")
    .kline("SHFE.au2602", Duration::from_secs(60), 32)
    .build()
    .await?;

while let Some(mut ctx) = strategy.next().await? {
    if let Some(row) = ctx.kline("SHFE.au2602", Duration::from_secs(60))?.last() {
        if row.close > 480.0 {
            ctx.orders("sim")
                .buy_open("SHFE.au2602", 1)
                .limit(row.close)
                .send_once(format!("entry-{}", row.datetime))
                .await?;
            let _report = ctx.finish_test_step().await?;
        }
    }
}
# Ok(())
# }
```

## Task 1: Extend Strategy Context With Kline/Tick Windows

**Files:**
- Modify: `crates/tqsdk-task/src/strategy.rs`
- Test: `crates/tqsdk-task/tests/strategy_host.rs`

- [ ] **Step 1: Add failing kline/tick context test**

Append to `crates/tqsdk-task/tests/strategy_host.rs`:

```rust
use std::time::Duration;

use tqsdk_core::{CommitScope, InputPayload, IoEvent, ProtocolDomain, RuntimeInput};

fn seed_ready_kline_and_tick(host: &TaskHost, symbol: &str) {
    host.api()
        .handle_for_test()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "charts": {
                            "wait-kline-SHFE.rb2601-60000000000-16": {
                                "state": {
                                    "ins_list": symbol,
                                    "duration": 60_000_000_000_i64
                                },
                                "left_id": 1,
                                "right_id": 1,
                                "more_data": false,
                                "ready": true
                            },
                            "wait-tick-SHFE.rb2601-16": {
                                "state": {
                                    "ins_list": symbol,
                                    "duration": 0
                                },
                                "left_id": 1,
                                "right_id": 1,
                                "more_data": false,
                                "ready": true
                            }
                        },
                        "klines": {
                            symbol: {
                                "60000000000": {
                                    "data": {
                                        "1": {
                                            "id": 1,
                                            "datetime": 1_000_i64,
                                            "open": 3670.0,
                                            "high": 3680.0,
                                            "low": 3660.0,
                                            "close": 3678.0
                                        }
                                    }
                                }
                            }
                        },
                        "ticks": {
                            symbol: {
                                "data": {
                                    "1": {
                                        "id": 1,
                                        "datetime": 1_100_i64,
                                        "last_price": 3679.0
                                    }
                                }
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("seed serial market commit should produce a commit");
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_context_reads_kline_and_tick_windows() {
    let host = seeded_host();
    seed_account_position_quote(&host, "sim", "SHFE.rb2601", 80_000.0, 0, 3_678.0);
    seed_ready_kline_and_tick(&host, "SHFE.rb2601");

    let mut strategy = StrategyHost::builder(host)
        .account("sim")
        .quote("SHFE.rb2601")
        .kline("SHFE.rb2601", Duration::from_secs(60), 16)
        .tick("SHFE.rb2601", 16)
        .build()
        .await
        .unwrap();
    let ctx = strategy.next_once().await.unwrap();

    let klines = ctx.kline("SHFE.rb2601", Duration::from_secs(60)).unwrap();
    let ticks = ctx.tick("SHFE.rb2601").unwrap();

    assert_eq!(klines.len(), 1);
    assert_eq!(klines.last().unwrap().close, 3_678.0);
    assert_eq!(ticks.len(), 1);
    assert_eq!(ticks.last().unwrap().last_price, 3_679.0);
}
```

- [ ] **Step 2: Run the failing focused test**

Run:

```bash
cargo test -p tqsdk-task --test strategy_host strategy_context_reads_kline_and_tick_windows -- --nocapture
```

Expected: compile failure because `StrategyHostBuilder::kline`, `StrategyHostBuilder::tick`, `StrategyContext::kline`, and `StrategyContext::tick` do not exist.

- [ ] **Step 3: Implement strategy serial specs and context readers**

Modify `crates/tqsdk-task/src/strategy.rs`.

Add imports:

```rust
use std::time::Duration;

use tqsdk_wait::{KlineSerialRef, KlineWindow, TickSerialRef, TickWindow};
```

Add private spec/ref structs near `StrategyHost`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyKlineSpec {
    symbol: String,
    duration_ns: i64,
    view_width: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyTickSpec {
    symbol: String,
    view_width: usize,
}

struct StrategyKlineHandle {
    spec: StrategyKlineSpec,
    serial: KlineSerialRef,
}

struct StrategyTickHandle {
    spec: StrategyTickSpec,
    serial: TickSerialRef,
}
```

Extend `StrategyHostBuilder`, `StrategyHost`, and `StrategyContext` fields:

```rust
pub struct StrategyHostBuilder {
    host: TaskHost,
    accounts: Vec<String>,
    quotes: Vec<String>,
    klines: Vec<StrategyKlineSpec>,
    ticks: Vec<StrategyTickSpec>,
}

pub struct StrategyHost {
    host: TaskHost,
    accounts: Vec<String>,
    quotes: Vec<String>,
    klines: Vec<StrategyKlineHandle>,
    ticks: Vec<StrategyTickHandle>,
}

pub struct StrategyContext<'a> {
    host: &'a mut TaskHost,
    update: StrategyUpdate,
    klines: &'a [StrategyKlineHandle],
    ticks: &'a [StrategyTickHandle],
}
```

Update `StrategyHost::next`:

```rust
Ok(Some(StrategyContext {
    host: &mut self.host,
    update: StrategyUpdate { updated },
    klines: &self.klines,
    ticks: &self.ticks,
}))
```

Update `StrategyHostBuilder::new`:

```rust
Self {
    host,
    accounts: Vec::new(),
    quotes: Vec::new(),
    klines: Vec::new(),
    ticks: Vec::new(),
}
```

Add builder methods:

```rust
#[must_use]
pub fn kline(
    mut self,
    symbol: impl AsRef<str>,
    duration: Duration,
    view_width: usize,
) -> Self {
    let duration_ns = duration_to_ns(duration);
    let spec = StrategyKlineSpec {
        symbol: symbol.as_ref().to_owned(),
        duration_ns,
        view_width,
    };
    if !self.klines.iter().any(|existing| existing == &spec) {
        self.klines.push(spec);
    }
    self
}

#[must_use]
pub fn tick(mut self, symbol: impl AsRef<str>, view_width: usize) -> Self {
    let spec = StrategyTickSpec {
        symbol: symbol.as_ref().to_owned(),
        view_width,
    };
    if !self.ticks.iter().any(|existing| existing == &spec) {
        self.ticks.push(spec);
    }
    self
}
```

Update `StrategyHostBuilder::build` before constructing `StrategyHost`:

```rust
let mut kline_handles = Vec::new();
for spec in &self.klines {
    let serial = self
        .host
        .api_mut()
        .get_kline_serial(
            &spec.symbol,
            Duration::from_nanos(spec.duration_ns as u64),
            spec.view_width,
        )
        .await?;
    kline_handles.push(StrategyKlineHandle {
        spec: spec.clone(),
        serial,
    });
}

let mut tick_handles = Vec::new();
for spec in &self.ticks {
    let serial = self
        .host
        .api_mut()
        .get_tick_serial(&spec.symbol, spec.view_width)
        .await?;
    tick_handles.push(StrategyTickHandle {
        spec: spec.clone(),
        serial,
    });
}
```

Construct `StrategyHost` with:

```rust
Ok(StrategyHost {
    host: self.host,
    accounts: self.accounts,
    quotes: self.quotes,
    klines: kline_handles,
    ticks: tick_handles,
})
```

Add context readers:

```rust
pub fn kline(
    &self,
    symbol: impl AsRef<str>,
    duration: Duration,
) -> Result<KlineWindow> {
    let duration_ns = duration_to_ns(duration);
    let symbol = symbol.as_ref();
    let Some(handle) = self
        .klines
        .iter()
        .find(|handle| handle.spec.symbol == symbol && handle.spec.duration_ns == duration_ns)
    else {
        return Err(TaskError::InvalidState("strategy kline serial is not configured"));
    };
    handle.serial.load(self.host.api()).map_err(Into::into)
}

pub fn tick(&self, symbol: impl AsRef<str>) -> Result<TickWindow> {
    let symbol = symbol.as_ref();
    let Some(handle) = self
        .ticks
        .iter()
        .find(|handle| handle.spec.symbol == symbol)
    else {
        return Err(TaskError::InvalidState("strategy tick serial is not configured"));
    };
    handle.serial.load(self.host.api()).map_err(Into::into)
}
```

Add helper:

```rust
fn duration_to_ns(duration: Duration) -> i64 {
    (duration.as_secs() as i64) * 1_000_000_000 + i64::from(duration.subsec_nanos())
}
```

- [ ] **Step 4: Run the focused test**

Run:

```bash
cargo test -p tqsdk-task --test strategy_host strategy_context_reads_kline_and_tick_windows -- --nocapture
```

Expected: test passes.

- [ ] **Step 5: Run task strategy tests**

Run:

```bash
cargo test -p tqsdk-task --test strategy_host -- --nocapture
cargo test -p tqsdk-task --test strategy_testing -- --nocapture
```

Expected: both pass.

- [ ] **Step 6: Commit**

```bash
git add crates/tqsdk-task/src/strategy.rs crates/tqsdk-task/tests/strategy_host.rs
git commit -m "feat: expose strategy serial context readers"
```

## Task 2: Add Strategy Replay Driver Over Market Cache Events

**Files:**
- Modify: `crates/tqsdk-task/Cargo.toml`
- Create: `crates/tqsdk-task/src/replay.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`
- Test: `crates/tqsdk-task/tests/strategy_replay.rs`

- [ ] **Step 1: Add failing replay tests**

Create `crates/tqsdk-task/tests/strategy_replay.rs`:

```rust
use std::time::Duration;

use tqsdk_core::{Kline, Quote};
use tqsdk_data::{MarketCacheEvent, MarketCacheReplay};
use tqsdk_task::testing::{FakeBroker, FakeMarket};
use tqsdk_task::StrategyReplay;

#[tokio::test(flavor = "current_thread")]
async fn strategy_replay_drives_quote_events_into_strategy_context() {
    let quote = Quote {
        last_price: 480.5,
        ..Quote::default()
    };
    let replay = MarketCacheReplay::new(vec![
        MarketCacheEvent::quote("cache", "SHFE.au2602", 1_000, Some(900), quote).unwrap(),
    ]);

    let mut strategy = StrategyReplay::builder(replay)
        .market(
            FakeMarket::new()
                .account("sim", 100_000.0)
                .position("sim", "SHFE.au2602", 0),
        )
        .broker(FakeBroker::new().fill_all())
        .account("sim")
        .quote("SHFE.au2602")
        .build()
        .await
        .unwrap();

    let mut ctx = strategy.next().await.unwrap().unwrap();
    assert_eq!(ctx.event().source(), "cache");
    assert_eq!(ctx.event().symbol(), "SHFE.au2602");
    assert_eq!(ctx.event().event_time_ns(), 900);
    assert_eq!(ctx.quote("SHFE.au2602").unwrap().last_price, 480.5);

    ctx.orders("sim")
        .buy_open("SHFE.au2602", 1)
        .limit(480.5)
        .send_once("replay-entry-1")
        .await
        .unwrap();
    let report = ctx.finish_test_step().await.unwrap();
    assert_eq!(report.orders().len(), 1);
    assert_eq!(report.trades().len(), 1);
    assert_eq!(report.position("sim", "SHFE.au2602").unwrap().pos_long, 1);

    assert!(strategy.next().await.unwrap().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_replay_drives_kline_events_into_strategy_context() {
    let older = Kline {
        id: 1,
        datetime: 1_000,
        close: 480.0,
        ..Kline::default()
    };
    let newer = Kline {
        id: 2,
        datetime: 2_000,
        close: 481.0,
        ..Kline::default()
    };
    let replay = MarketCacheReplay::new(vec![
        MarketCacheEvent::kline(
            "cache",
            "SHFE.au2602",
            2_100,
            Some(2_000),
            60_000_000_000,
            newer,
        )
        .unwrap(),
        MarketCacheEvent::kline(
            "cache",
            "SHFE.au2602",
            1_100,
            Some(1_000),
            60_000_000_000,
            older,
        )
        .unwrap(),
    ]);

    let mut strategy = StrategyReplay::builder(replay)
        .market(FakeMarket::new().account("sim", 100_000.0))
        .broker(FakeBroker::new().fill_all())
        .account("sim")
        .kline("SHFE.au2602", Duration::from_secs(60), 16)
        .build()
        .await
        .unwrap();

    let ctx = strategy.next().await.unwrap().unwrap();
    assert_eq!(ctx.event().event_time_ns(), 1_000);
    assert_eq!(
        ctx.kline("SHFE.au2602", Duration::from_secs(60))
            .unwrap()
            .last()
            .unwrap()
            .close,
        480.0
    );
    drop(ctx);

    let ctx = strategy.next().await.unwrap().unwrap();
    assert_eq!(ctx.event().event_time_ns(), 2_000);
    assert_eq!(
        ctx.kline("SHFE.au2602", Duration::from_secs(60))
            .unwrap()
            .last()
            .unwrap()
            .close,
        481.0
    );
}
```

- [ ] **Step 2: Run the failing replay tests**

Run:

```bash
cargo test -p tqsdk-task --test strategy_replay -- --nocapture
```

Expected: compile failure because `tqsdk-task` does not depend on `tqsdk-data` and `StrategyReplay` does not exist.

- [ ] **Step 3: Add the data dependency**

Modify `crates/tqsdk-task/Cargo.toml`:

```toml
tqsdk-data = { path = "../tqsdk-data", default-features = false }
```

- [ ] **Step 4: Add replay module export**

Modify `crates/tqsdk-task/src/lib.rs`:

```rust
mod replay;
```

and:

```rust
pub use replay::{
    StrategyReplay, StrategyReplayBuilder, StrategyReplayContext, StrategyReplayEvent,
};
```

- [ ] **Step 5: Implement replay driver**

Create `crates/tqsdk-task/src/replay.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use std::time::Duration;

use serde_json::{Value, json};
use tqsdk_core::{
    CommitScope, InputPayload, IoEvent, Kline, ProtocolDomain, Quote, RuntimeInput, Tick,
};
use tqsdk_data::{MarketCacheEvent, MarketCachePayload, MarketCacheReplay};

use crate::strategy::StrategyHostBuilder;
use crate::testing::{FakeBroker, FakeMarket, StrategyTestReport};
use crate::{
    Result, StrategyContext, StrategyHost, TaskError, TaskHost,
};

/// Offline strategy replay builder backed by ordered market cache events.
pub struct StrategyReplayBuilder {
    replay: MarketCacheReplay,
    market: FakeMarket,
    broker: FakeBroker,
    accounts: Vec<String>,
    quotes: Vec<String>,
    klines: Vec<ReplayKlineSpec>,
    ticks: Vec<ReplayTickSpec>,
}

/// Offline strategy replay host.
pub struct StrategyReplay {
    replay: MarketCacheReplay,
    strategy: StrategyHost,
    klines: Vec<ReplayKlineSpec>,
    ticks: Vec<ReplayTickSpec>,
}

/// Metadata for the market event that produced a replay strategy context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyReplayEvent {
    source: String,
    symbol: String,
    received_at_ns: i64,
    event_time_ns: i64,
}

/// Strategy context plus the replay event that triggered it.
pub struct StrategyReplayContext<'a> {
    event: StrategyReplayEvent,
    context: StrategyContext<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplayKlineSpec {
    symbol: String,
    duration_ns: i64,
    view_width: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplayTickSpec {
    symbol: String,
    view_width: usize,
}

impl StrategyReplay {
    #[must_use]
    pub fn builder(replay: MarketCacheReplay) -> StrategyReplayBuilder {
        StrategyReplayBuilder::new(replay)
    }

    pub async fn next(&mut self) -> Result<Option<StrategyReplayContext<'_>>> {
        let Some(event) = self.replay.next() else {
            return Ok(None);
        };
        let replay_event = StrategyReplayEvent::from_cache_event(&event);
        ingest_market_cache_event(
            self.strategy.task_host(),
            &event,
            &self.klines,
            &self.ticks,
        )?;
        let context = self.strategy.next_once().await?;
        Ok(Some(StrategyReplayContext {
            event: replay_event,
            context,
        }))
    }

    #[must_use]
    pub fn strategy(&self) -> &StrategyHost {
        &self.strategy
    }

    #[must_use]
    pub fn strategy_mut(&mut self) -> &mut StrategyHost {
        &mut self.strategy
    }

    #[must_use]
    pub fn into_strategy(self) -> StrategyHost {
        self.strategy
    }
}

impl StrategyReplayBuilder {
    #[must_use]
    pub fn new(replay: MarketCacheReplay) -> Self {
        Self {
            replay,
            market: FakeMarket::new(),
            broker: FakeBroker::new(),
            accounts: Vec::new(),
            quotes: Vec::new(),
            klines: Vec::new(),
            ticks: Vec::new(),
        }
    }

    #[must_use]
    pub fn market(mut self, market: FakeMarket) -> Self {
        self.market = market;
        self
    }

    #[must_use]
    pub fn broker(mut self, broker: FakeBroker) -> Self {
        self.broker = broker;
        self
    }

    #[must_use]
    pub fn account(mut self, account_id: impl AsRef<str>) -> Self {
        push_unique(&mut self.accounts, account_id.as_ref());
        self
    }

    #[must_use]
    pub fn quote(mut self, symbol: impl AsRef<str>) -> Self {
        push_unique(&mut self.quotes, symbol.as_ref());
        self
    }

    #[must_use]
    pub fn kline(
        mut self,
        symbol: impl AsRef<str>,
        duration: Duration,
        view_width: usize,
    ) -> Self {
        let spec = ReplayKlineSpec {
            symbol: symbol.as_ref().to_owned(),
            duration_ns: duration_to_ns(duration),
            view_width,
        };
        if !self.klines.iter().any(|existing| existing == &spec) {
            self.klines.push(spec);
        }
        self
    }

    #[must_use]
    pub fn tick(mut self, symbol: impl AsRef<str>, view_width: usize) -> Self {
        let spec = ReplayTickSpec {
            symbol: symbol.as_ref().to_owned(),
            view_width,
        };
        if !self.ticks.iter().any(|existing| existing == &spec) {
            self.ticks.push(spec);
        }
        self
    }

    pub async fn build(self) -> Result<StrategyReplay> {
        let harness = crate::testing::StrategyTestHarness::new()
            .market(self.market)
            .broker(self.broker)
            .build()?;
        let mut builder = StrategyHostBuilder::new(harness.into_task_host());
        for account in &self.accounts {
            builder = builder.account(account);
        }
        for quote in &self.quotes {
            builder = builder.quote(quote);
        }
        for spec in &self.klines {
            builder = builder.kline(
                &spec.symbol,
                Duration::from_nanos(spec.duration_ns as u64),
                spec.view_width,
            );
        }
        for spec in &self.ticks {
            builder = builder.tick(&spec.symbol, spec.view_width);
        }
        let strategy = builder.build().await?;
        Ok(StrategyReplay {
            replay: self.replay,
            strategy,
            klines: self.klines,
            ticks: self.ticks,
        })
    }
}

impl StrategyReplayEvent {
    fn from_cache_event(event: &MarketCacheEvent) -> Self {
        Self {
            source: event.source.clone(),
            symbol: event.symbol.clone(),
            received_at_ns: event.received_at_ns,
            event_time_ns: event.event_time_ns(),
        }
    }

    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub fn received_at_ns(&self) -> i64 {
        self.received_at_ns
    }

    #[must_use]
    pub fn event_time_ns(&self) -> i64 {
        self.event_time_ns
    }
}

impl StrategyReplayContext<'_> {
    #[must_use]
    pub fn event(&self) -> &StrategyReplayEvent {
        &self.event
    }

    pub fn quote(&self, symbol: impl AsRef<str>) -> Result<Quote> {
        self.context.quote(symbol)
    }

    pub fn kline(
        &self,
        symbol: impl AsRef<str>,
        duration: Duration,
    ) -> Result<tqsdk_wait::KlineWindow> {
        self.context.kline(symbol, duration)
    }

    pub fn tick(&self, symbol: impl AsRef<str>) -> Result<tqsdk_wait::TickWindow> {
        self.context.tick(symbol)
    }

    pub fn account(&self, account_id: impl AsRef<str>) -> Result<tqsdk_core::Account> {
        self.context.account(account_id)
    }

    pub fn position(
        &self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> Result<tqsdk_core::Position> {
        self.context.position(account_id, symbol)
    }

    #[must_use]
    pub fn orders(&mut self, account_id: impl AsRef<str>) -> crate::TaskOrderBuilder<'_> {
        self.context.orders(account_id)
    }

    pub async fn finish_test_step(&mut self) -> Result<StrategyTestReport> {
        self.context.finish_test_step().await
    }
}

fn ingest_market_cache_event(
    host: &TaskHost,
    event: &MarketCacheEvent,
    klines: &[ReplayKlineSpec],
    ticks: &[ReplayTickSpec],
) -> Result<()> {
    let body = match &event.payload {
        MarketCachePayload::Quote(quote) => quote_update(&event.symbol, quote)?,
        MarketCachePayload::Kline { duration_ns, row } => {
            kline_update(&event.symbol, *duration_ns, row, klines)?
        }
        MarketCachePayload::Tick(tick) => tick_update(&event.symbol, tick, ticks)?,
    };

    host.api()
        .handle_for_test()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market-replay".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [body]
                })),
            }),
            vec![],
            CommitScope::ReplayStep,
        )?;
    Ok(())
}

fn quote_update(symbol: &str, quote: &Quote) -> Result<Value> {
    Ok(json!({
        "quotes": {
            symbol: serde_json::to_value(quote)
                .map_err(|_| TaskError::InvalidState("replay quote payload is invalid"))?
        }
    }))
}

fn kline_update(
    symbol: &str,
    duration_ns: i64,
    row: &Kline,
    klines: &[ReplayKlineSpec],
) -> Result<Value> {
    let row_id = row.id.to_string();
    let mut charts = serde_json::Map::new();
    for spec in klines
        .iter()
        .filter(|spec| spec.symbol == symbol && spec.duration_ns == duration_ns)
    {
        let chart_id = kline_chart_id(symbol, duration_ns, spec.view_width);
        charts.insert(
            chart_id,
            json!({
                "state": {
                    "ins_list": symbol,
                    "duration": duration_ns
                },
                "left_id": row.id,
                "right_id": row.id,
                "more_data": false,
                "ready": true
            }),
        );
    }

    Ok(json!({
        "charts": charts,
        "klines": {
            symbol: {
                duration_ns.to_string(): {
                    "data": {
                        row_id: serde_json::to_value(row)
                            .map_err(|_| TaskError::InvalidState("replay kline payload is invalid"))?
                    }
                }
            }
        }
    }))
}

fn tick_update(symbol: &str, tick: &Tick, ticks: &[ReplayTickSpec]) -> Result<Value> {
    let row_id = tick.id.to_string();
    let mut charts = serde_json::Map::new();
    for spec in ticks.iter().filter(|spec| spec.symbol == symbol) {
        let chart_id = tick_chart_id(symbol, spec.view_width);
        charts.insert(
            chart_id,
            json!({
                "state": {
                    "ins_list": symbol,
                    "duration": 0
                },
                "left_id": tick.id,
                "right_id": tick.id,
                "more_data": false,
                "ready": true
            }),
        );
    }

    Ok(json!({
        "charts": charts,
        "ticks": {
            symbol: {
                "data": {
                    row_id: serde_json::to_value(tick)
                        .map_err(|_| TaskError::InvalidState("replay tick payload is invalid"))?
                }
            }
        }
    }))
}

fn kline_chart_id(symbol: &str, duration_ns: i64, view_width: usize) -> String {
    format!("wait-kline-{symbol}-{duration_ns}-{view_width}")
}

fn tick_chart_id(symbol: &str, view_width: usize) -> String {
    format!("wait-tick-{symbol}-{view_width}")
}

fn duration_to_ns(duration: Duration) -> i64 {
    (duration.as_secs() as i64) * 1_000_000_000 + i64::from(duration.subsec_nanos())
}

fn push_unique(values: &mut Vec<String>, value: &str) {
    if !values.iter().any(|existing| existing == value) {
        values.push(value.to_owned());
    }
}
```

- [ ] **Step 6: Run replay tests**

Run:

```bash
cargo test -p tqsdk-task --test strategy_replay -- --nocapture
```

Expected: tests pass.

- [ ] **Step 7: Run task tests touched by new dependency**

Run:

```bash
cargo test -p tqsdk-task --test strategy_replay -- --nocapture
cargo test -p tqsdk-task --test strategy_host -- --nocapture
cargo test -p tqsdk-task --test strategy_testing -- --nocapture
```

Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add crates/tqsdk-task/Cargo.toml crates/tqsdk-task/src/lib.rs \
  crates/tqsdk-task/src/replay.rs crates/tqsdk-task/tests/strategy_replay.rs
git commit -m "feat: add strategy cache replay driver"
```

## Task 3: Promote S16 Foundation Example and Update Scenario Docs

**Files:**
- Create: `crates/tqsdk-task/examples/api_contract_s16_history_replay_strategy.rs`
- Modify: `docs/reviews/public-api-scenario-review.md`
- Modify: `docs/scenarios/api_gaps/api_contract_s16_history_replay_strategy.rs`
- Modify: `docs/scenarios/user-layer-iteration-plan.md`
- Modify: `crates/tqsdk-task/README.md`
- Modify: `docs/architecture/ai-workflow.md`
- Modify: `docs/architecture/README.md`
- Modify: `docs/architecture/crate-boundaries.md`
- Modify: `docs/architecture/api-task.md`

- [ ] **Step 1: Add formal S16 foundation example**

Create `crates/tqsdk-task/examples/api_contract_s16_history_replay_strategy.rs`:

```rust
//! Scenario: 历史行情回放（cache event -> strategy context 子集）
//!
//! User goal:
//! - 历史/cache 行情按时间顺序驱动同一套策略 context
//! - 策略读取 quote/kline，不手写 runtime state
//! - replay 中的订单走 fake broker，便于测试策略逻辑
//!
//! API contract:
//! - 使用 `MarketCacheReplay` 作为离线事件源
//! - 使用 `StrategyReplay` 驱动 `StrategyContext`
//! - replay event 进入 SDK runtime commit，不维护第二棵状态树
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - provider 私有 protocol type
//! - `RuntimeInput` / `RuntimeHandle` 泄漏到用户策略
//! - 用户自己排序事件或改写 state tree dump
//! - 把 replay driver 下沉到 `tqsdk-core` / `tqsdk-session`
//!
//! Regression signal:
//! - 策略代码必须区分 live context 和 replay context
//! - 用户需要手动把 K线转成 quote 或 runtime mutation
//! - cache replay 无法复用 typed order / fake broker
//!
//! Review questions:
//! - 当前 API 是否自然表达 replay foundation？
//! - 剩余 live/sim/replay environment gap 是否被明确排除？
//! - 是否暴露内部协议或手动异步编排？
//!
//! Current API note:
//! 本示例只验证 cache event -> StrategyContext 的离线 replay foundation。
//! 历史序列直接拉取、replay speed/clock、live/sim/replay 统一 environment
//! 仍保留在 `docs/scenarios/api_gaps/`。

use std::time::Duration;

use tqsdk_core::{Kline, Quote};
use tqsdk_data::{MarketCacheEvent, MarketCacheReplay};
use tqsdk_task::StrategyReplay;
use tqsdk_task::testing::{FakeBroker, FakeMarket};

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk_task::Result<()> {
    let quote = Quote {
        last_price: 480.5,
        ..Quote::default()
    };
    let kline = Kline {
        id: 1,
        datetime: 1_000,
        close: 481.0,
        ..Kline::default()
    };

    let replay = MarketCacheReplay::new(vec![
        MarketCacheEvent::quote("cache", "SHFE.au2602", 1_000, Some(900), quote)
            .map_err(|_| tqsdk_task::TaskError::InvalidState("invalid replay quote"))?,
        MarketCacheEvent::kline(
            "cache",
            "SHFE.au2602",
            2_000,
            Some(1_900),
            60_000_000_000,
            kline,
        )
        .map_err(|_| tqsdk_task::TaskError::InvalidState("invalid replay kline"))?,
    ]);

    let mut strategy = StrategyReplay::builder(replay)
        .market(
            FakeMarket::new()
                .account("sim", 100_000.0)
                .position("sim", "SHFE.au2602", 0),
        )
        .broker(FakeBroker::new().fill_all())
        .account("sim")
        .quote("SHFE.au2602")
        .kline("SHFE.au2602", Duration::from_secs(60), 32)
        .build()
        .await?;

    while let Some(mut ctx) = strategy.next().await? {
        println!(
            "source={} symbol={} event_time_ns={}",
            ctx.event().source(),
            ctx.event().symbol(),
            ctx.event().event_time_ns()
        );

        let last_price = ctx.quote("SHFE.au2602")?.last_price;
        if let Some(row) = ctx.kline("SHFE.au2602", Duration::from_secs(60))?.last() {
            if row.close > last_price {
                ctx.orders("sim")
                    .buy_open("SHFE.au2602", 1)
                    .limit(row.close)
                    .send_once(format!("replay-entry-{}", row.datetime))
                    .await?;
                let report = ctx.finish_test_step().await?;
                println!("orders={} trades={}", report.orders().len(), report.trades().len());
            }
        }
    }

    Ok(())
}
```

- [ ] **Step 2: Update scenario review S16 row**

In `docs/reviews/public-api-scenario-review.md`, update S16:

```markdown
| 16. 历史行情回放 | 勉强 | 中 | 无 | 无 | 低 | 中 | 局部重构 | `crates/tqsdk-task/examples/api_contract_s16_history_replay_strategy.rs`; `docs/scenarios/api_gaps/api_contract_s16_history_replay_strategy.rs`; `StrategyReplay`; `MarketCacheReplay`; cache event -> strategy context foundation works; replay speed/history adapter/live-sim-replay environment still gap |
```

- [ ] **Step 3: Narrow S16 gap note**

In `docs/scenarios/api_gaps/api_contract_s16_history_replay_strategy.rs`, replace the remaining gap paragraph:

```rust
//! Remaining API gap:
//! `tqsdk-task::StrategyReplay` can drive `StrategyHost` from ordered
//! `tqsdk-data::MarketCacheReplay` events. Remaining gaps are direct
//! `DataClient` history-series-to-replay adapters, replay speed/clock control,
//! resumable replay checkpoints, and a unified live/sim/replay environment
//! abstraction for production strategy deployment.
```

- [ ] **Step 4: Update iteration plan**

In `docs/scenarios/user-layer-iteration-plan.md`, under P1 strategy runtime / testability, add:

```markdown
- `StrategyReplay` foundation 已接入 `MarketCacheReplay`，cache quote/kline/tick
  event 可以按时间顺序进入同一 `StrategyContext`。
```

Under remaining gaps, keep:

```markdown
- S15 完整 live / sim / replay environment adapter。
- `DataClient` history series 直接转 replay event 的 adapter。
- replay speed / deterministic clock / resumable checkpoint。
```

- [ ] **Step 5: Update task README**

Add to `crates/tqsdk-task/README.md` current capabilities:

```markdown
- `StrategyReplay`
  - 使用 `tqsdk-data::MarketCacheReplay` 作为离线 market event source
  - 将 cache quote/kline/tick 转成正常 runtime market commit
  - 让 replay strategy 复用 `StrategyContext`、typed order builder 和 fake broker
  - 当前不包含 replay speed controller、deterministic clock 或 live/sim/replay environment adapter
```

- [ ] **Step 6: Update architecture docs**

Update architecture docs to say `tqsdk-task` may depend on `tqsdk-data` for strategy replay integration, while `tqsdk-data` remains independent and never depends on task:

```markdown
`tqsdk-task` may consume `tqsdk-data` cache/history events when building
strategy replay drivers. This is an upper-layer integration path; it must not
move cache storage into task or move strategy execution into data.
```

Apply that wording in:

- `docs/architecture/ai-workflow.md`
- `docs/architecture/README.md`
- `docs/architecture/crate-boundaries.md`
- `docs/architecture/api-task.md`

- [ ] **Step 7: Run example and scenario checks**

Run:

```bash
cargo check -p tqsdk-task --example api_contract_s16_history_replay_strategy
scripts/check_api_contract_examples.sh
```

Expected: both pass. The script may print the existing locale warning; exit code must be 0.

- [ ] **Step 8: Commit**

```bash
git add crates/tqsdk-task/examples/api_contract_s16_history_replay_strategy.rs \
  docs/reviews/public-api-scenario-review.md \
  docs/scenarios/api_gaps/api_contract_s16_history_replay_strategy.rs \
  docs/scenarios/user-layer-iteration-plan.md \
  crates/tqsdk-task/README.md \
  docs/architecture/ai-workflow.md \
  docs/architecture/README.md \
  docs/architecture/crate-boundaries.md \
  docs/architecture/api-task.md
git commit -m "docs: promote strategy cache replay foundation"
```

## Task 4: Full Verification

**Files:**
- No source changes unless verification exposes issues.

- [ ] **Step 1: Run scenario guardrail**

Run:

```bash
scripts/check_api_contract_examples.sh
```

Expected: exits 0. The known locale warning is acceptable only if the exit code is 0.

- [ ] **Step 2: Run workspace examples check**

Run:

```bash
cargo check --workspace --examples
```

Expected: exits 0.

- [ ] **Step 3: Run workspace tests**

Run:

```bash
cargo test --workspace
```

Expected: exits 0.

- [ ] **Step 4: Run Clippy**

Run:

```bash
cargo clippy --workspace --examples --all-targets -- -D warnings
```

Expected: exits 0.

- [ ] **Step 5: Feature flag verification**

This plan adds a dependency but does not add or modify feature flags. If implementation changes feature flags anyway, also run:

```bash
cargo check --workspace --no-default-features
cargo check --workspace --all-features --examples
```

- [ ] **Step 6: Final status check**

Run:

```bash
git status --short
git log --oneline -10
```

Expected: only unrelated untracked files remain untouched; all intentional tracked changes are committed.

## Self-Review

- Spec coverage: This plan advances S16 from gap-only to cache event replay foundation, while preserving S15 environment switching and replay clock as explicit gaps. It also makes kline/tick windows readable from `StrategyContext`, which is required for historical replay strategies to use more than quote snapshots.
- Placeholder scan: No open-ended implementation placeholders and no hidden “write tests” step.
- Type consistency: Public names are consistently `StrategyReplay`, `StrategyReplayBuilder`, `StrategyReplayContext`, `StrategyReplayEvent`, `MarketCacheReplay`, `StrategyContext::kline`, and `StrategyContext::tick`.
