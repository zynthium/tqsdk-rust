# Scenario Low-Level Foundations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Advance the next scenario-driven Public API batch by strengthening low-level foundations for S19 contract-rule risk checks, S21 sink configuration ergonomics, and S18 live market cache writing while keeping S14 paused.

**Architecture:** Keep the changes above `tqsdk-core`: risk and execution semantics stay in `tqsdk-task`, managed sink ergonomics stay in `tqsdk-stream`, and cache record / writer semantics stay in `tqsdk-data`. Do not introduce a second runtime state tree, private facade revisions, provider protocol exposure, or HTTP/GUI daemon requirements.

**Tech Stack:** Rust 2024, Tokio, futures streams, existing `tqsdk-core` runtime contract, `tqsdk-session::InstrumentSpec`, `tqsdk-stream::MarketEvent`, `tqsdk-data::MarketCacheWriter`.

---

## Scope

In scope:

- S19: add contract metadata driven risk rules using `InstrumentSpec` without moving metadata APIs out of `tqsdk-session`.
- S21: add a reusable `StreamSinkProfile` so production sink examples stop hand-assembling every WAL / retry / journal option.
- S18: add a single-process live market event to cache writer bridge; do not promise cross-process locks, durable daemon queue, or runtime state snapshot recovery.
- Update formal examples and scenario review docs.

Out of scope:

- S14 multi-provider aggregation.
- Durable daemon queue and cross-process cache locking.
- Built-in HTTP health/metrics endpoint or GUI.
- Core/session crate boundary changes.

## File Structure

- Modify `crates/tqsdk-task/src/risk.rs`
  - Add instrument spec storage, tick-size validation, and contract notional projection.
- Modify `crates/tqsdk-task/src/lib.rs`
  - Re-export any new public risk report/rule type only if it is user-facing.
- Modify `crates/tqsdk-task/tests/risk_orders.rs`
  - Add TDD coverage for tick-size rejection and contract multiplier projection.
- Modify `crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs`
  - Show metadata-backed risk rule usage.
- Modify `crates/tqsdk-stream/src/sink.rs`
  - Add `StreamSinkProfile` and missing read-only getters on `StreamSinkOptions`.
- Modify `crates/tqsdk-stream/src/lib.rs`
  - Re-export `StreamSinkProfile`.
- Modify `crates/tqsdk-stream/tests/stream_commit_flow.rs`
  - Add profile tests.
- Modify `crates/tqsdk-stream/examples/api_contract_s21_slow_consumer_isolation.rs`
  - Use the profile to reduce user boilerplate.
- Modify `crates/tqsdk-data/Cargo.toml`
  - Add optional `stream` feature for the bridge: `stream = ["dep:futures", "dep:tqsdk-stream"]`.
- Create `crates/tqsdk-data/src/stream_cache.rs`
  - Convert `tqsdk_stream::MarketEvent` into cache records and pipe a stream into `MarketCacheWriter`.
- Modify `crates/tqsdk-data/src/lib.rs`
  - Re-export the stream cache bridge behind `#[cfg(feature = "stream")]`.
- Create or modify `crates/tqsdk-data/tests/market_cache_stream.rs`
  - Test quote/kline/tick conversion and bounded pipe behavior.
- Add `crates/tqsdk-data/examples/api_contract_s18_live_market_cache_pipe.rs`
  - Formal example for the live pipe foundation, gated by `required-features = ["live", "stream"]`.
- Modify `crates/tqsdk-data/Cargo.toml`
  - Add the S18 live pipe example with required features.
- Modify `docs/public-api-scenario-review.md`
  - Update S18/S19/S21 status evidence.
- Modify relevant gap sketches:
  - `docs/scenarios/api_gaps/api_contract_s18_local_market_cache.rs`
  - `docs/scenarios/api_gaps/api_contract_s19_pre_trade_risk.rs`
  - `docs/scenarios/api_gaps/api_contract_s21_slow_consumer_isolation.rs`
- Modify `docs/scenarios/user-layer-iteration-plan.md`
  - Mark the new foundations landed and keep remaining gaps explicit.

## Task 1: S19 Contract Metadata Risk Rules

**Files:**
- Modify: `crates/tqsdk-task/src/risk.rs`
- Modify: `crates/tqsdk-task/tests/risk_orders.rs`
- Modify: `crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs`

- [ ] **Step 1: Write failing tests**

Add tests to `crates/tqsdk-task/tests/risk_orders.rs`:

```rust
#[tokio::test(flavor = "current_thread")]
async fn risk_engine_rejects_limit_price_not_aligned_to_instrument_tick() {
    let api = seeded_api_with_account_position_quote("sim", "SHFE.au2602", 10_000.0, 0, 480.0);
    let spec = tqsdk_session::InstrumentSpec {
        symbol: tqsdk_core::Symbol::new("SHFE.au2602"),
        exchange_id: "SHFE".to_string(),
        product_id: "au".to_string(),
        class: tqsdk_session::InstrumentClass::Future,
        price_tick: 0.2,
        volume_multiple: 1_000,
        expire_datetime_ns: None,
        underlying_symbol: None,
    };
    let risk = RiskEngine::new().instrument_specs([spec]);
    let intent = TaskOrderIntent {
        account_id: "sim".to_string(),
        symbol: "SHFE.au2602".to_string(),
        direction: TradeDirection::Buy,
        offset: Some(TradeOffset::Open),
        volume: 1,
        limit_price: Some(480.15),
    };

    let report = risk.check_report(&api, &intent).unwrap();

    assert!(matches!(
        report.decision().rejection(),
        Some(RiskRejection::PriceNotOnTick {
            symbol,
            limit_price,
            price_tick
        }) if symbol == "SHFE.au2602" && *limit_price == 480.15 && *price_tick == 0.2
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn risk_projection_uses_instrument_volume_multiple_for_notional() {
    let api = seeded_api_with_account_position_quote("sim", "SHFE.au2602", 10_000.0, 2, 480.0);
    let spec = tqsdk_session::InstrumentSpec {
        symbol: tqsdk_core::Symbol::new("SHFE.au2602"),
        exchange_id: "SHFE".to_string(),
        product_id: "au".to_string(),
        class: tqsdk_session::InstrumentClass::Future,
        price_tick: 0.2,
        volume_multiple: 1_000,
        expire_datetime_ns: None,
        underlying_symbol: None,
    };
    let risk = RiskEngine::new().instrument_specs([spec]);
    let intent = TaskOrderIntent {
        account_id: "sim".to_string(),
        symbol: "SHFE.au2602".to_string(),
        direction: TradeDirection::Buy,
        offset: Some(TradeOffset::Open),
        volume: 3,
        limit_price: Some(480.2),
    };

    let projection = risk.project_order(&api, &intent).unwrap();

    assert_eq!(projection.contract_multiplier(), Some(1_000));
    assert_eq!(projection.estimated_notional(), Some(480.2 * 3.0 * 1_000.0));
}
```

- [ ] **Step 2: Run tests and verify red**

Run:

```bash
cargo test -p tqsdk-task --test risk_orders risk_engine_rejects_limit_price_not_aligned_to_instrument_tick -- --exact
cargo test -p tqsdk-task --test risk_orders risk_projection_uses_instrument_volume_multiple_for_notional -- --exact
```

Expected: compile failure for missing `instrument_specs`, `PriceNotOnTick`, `contract_multiplier`, or `estimated_notional`.

- [ ] **Step 3: Implement minimal API**

In `crates/tqsdk-task/src/risk.rs`, add a private rule map and public additive methods:

```rust
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
struct InstrumentRiskRule {
    price_tick: f64,
    volume_multiple: i64,
}

#[derive(Debug, Clone, Default)]
pub struct RiskEngine {
    max_order_volume: Option<i64>,
    min_available: Option<f64>,
    max_abs_net_position: Option<i64>,
    max_abs_price_deviation: Option<f64>,
    instrument_rules: HashMap<String, InstrumentRiskRule>,
}
```

Add builder and report getters:

```rust
pub fn instrument_specs<I>(mut self, specs: I) -> Self
where
    I: IntoIterator<Item = tqsdk_session::InstrumentSpec>,
{
    for spec in specs {
        self.instrument_rules.insert(
            spec.symbol.as_str().to_string(),
            InstrumentRiskRule {
                price_tick: spec.price_tick,
                volume_multiple: spec.volume_multiple,
            },
        );
    }
    self
}

#[must_use]
pub fn contract_multiplier(&self) -> Option<i64> {
    self.contract_multiplier
}

#[must_use]
pub fn estimated_notional(&self) -> Option<f64> {
    self.estimated_notional
}
```

Add rejection:

```rust
PriceNotOnTick {
    symbol: String,
    limit_price: f64,
    price_tick: f64,
},
```

Add helpers:

```rust
fn price_is_on_tick(price: f64, price_tick: f64) -> bool {
    if !price.is_finite() || !price_tick.is_finite() || price_tick <= 0.0 {
        return false;
    }
    let ticks = (price / price_tick).round();
    (price - ticks * price_tick).abs() <= price_tick * 1e-9
}
```

In `check_report`, before price deviation acceptance, reject finite limit prices that are not aligned to a known instrument tick.

In `project_order`, set:

```rust
let contract_multiplier = self
    .instrument_rules
    .get(&intent.symbol)
    .map(|rule| rule.volume_multiple);
let estimated_notional = price_basis.zip(contract_multiplier).map(|(price, multiplier)| {
    price * intent.volume as f64 * multiplier as f64
});
```

- [ ] **Step 4: Run focused tests and commit**

Run:

```bash
cargo test -p tqsdk-task --test risk_orders
```

Expected: all `risk_orders` tests pass.

Commit:

```bash
git add crates/tqsdk-task/src/risk.rs crates/tqsdk-task/tests/risk_orders.rs
git commit -m "feat(task): add instrument-backed risk rules"
```

## Task 2: S21 Stream Sink Profile

**Files:**
- Modify: `crates/tqsdk-stream/src/sink.rs`
- Modify: `crates/tqsdk-stream/src/lib.rs`
- Modify: `crates/tqsdk-stream/tests/stream_commit_flow.rs`
- Modify: `crates/tqsdk-stream/examples/api_contract_s21_slow_consumer_isolation.rs`

- [ ] **Step 1: Write failing tests**

Add to `crates/tqsdk-stream/tests/stream_commit_flow.rs`:

```rust
#[test]
fn stream_sink_profile_builds_reliable_jsonl_options() {
    let wal = std::env::temp_dir().join("profile-wal.jsonl");
    let journal = std::env::temp_dir().join("profile-journal.jsonl");

    let options = StreamSinkProfile::reliable_jsonl(wal.clone(), journal.clone())
        .retry_policy(StreamSinkRetryPolicy::limited(5).unwrap())
        .fsync_policy(StreamSinkWalFsyncPolicy::EveryRecord)
        .into_options();

    assert_eq!(options.wal_path(), Some(wal.as_path()));
    assert_eq!(options.commit_journal_path(), Some(journal.as_path()));
    assert_eq!(options.retry_policy().max_attempts(), 5);
    assert_eq!(options.fsync_policy(), StreamSinkWalFsyncPolicy::EveryRecord);
}
```

- [ ] **Step 2: Run test and verify red**

Run:

```bash
cargo test -p tqsdk-stream --test stream_commit_flow stream_sink_profile_builds_reliable_jsonl_options -- --exact
```

Expected: compile failure for missing `StreamSinkProfile`, `wal_path`, or `retry_policy` getters.

- [ ] **Step 3: Implement profile**

Add to `crates/tqsdk-stream/src/sink.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSinkProfile {
    options: StreamSinkOptions,
}

impl StreamSinkProfile {
    #[must_use]
    pub fn memory() -> Self {
        Self {
            options: StreamSinkOptions::new(),
        }
    }

    #[must_use]
    pub fn reliable_jsonl(wal_path: impl Into<PathBuf>, journal_path: impl Into<PathBuf>) -> Self {
        Self {
            options: StreamSinkOptions::new()
                .jsonl_wal(wal_path)
                .jsonl_commit_journal(journal_path),
        }
    }

    #[must_use]
    pub fn retry_policy(mut self, retry_policy: StreamSinkRetryPolicy) -> Self {
        self.options = self.options.retry_policy(retry_policy);
        self
    }

    #[must_use]
    pub fn fsync_policy(mut self, policy: StreamSinkWalFsyncPolicy) -> Self {
        self.options = self.options.wal_fsync_policy(policy);
        self
    }

    #[must_use]
    pub fn into_options(self) -> StreamSinkOptions {
        self.options
    }
}
```

Add getters on `StreamSinkOptions`:

```rust
#[must_use]
pub fn retry_policy(&self) -> StreamSinkRetryPolicy {
    self.retry_policy
}

#[must_use]
pub fn wal_path(&self) -> Option<&Path> {
    self.wal_path.as_deref()
}
```

Re-export `StreamSinkProfile` in `crates/tqsdk-stream/src/lib.rs`.

- [ ] **Step 4: Update S21 example**

In `crates/tqsdk-stream/examples/api_contract_s21_slow_consumer_isolation.rs`, replace manual option assembly:

```rust
let warehouse_options = StreamSinkOptions::new()
    .retry_policy(StreamSinkRetryPolicy::limited(3)?)
    .jsonl_wal(wal_path.clone())
    .jsonl_commit_journal(journal_path.clone())
    .wal_fsync_policy(StreamSinkWalFsyncPolicy::EveryRecord);
```

with:

```rust
let warehouse_options = StreamSinkProfile::reliable_jsonl(wal_path.clone(), journal_path.clone())
    .retry_policy(StreamSinkRetryPolicy::limited(3)?)
    .fsync_policy(StreamSinkWalFsyncPolicy::EveryRecord)
    .into_options();
```

- [ ] **Step 5: Run focused verification and commit**

Run:

```bash
cargo test -p tqsdk-stream --test stream_commit_flow stream_sink_profile_builds_reliable_jsonl_options -- --exact
cargo check -p tqsdk-stream --examples
```

Expected: both pass.

Commit:

```bash
git add crates/tqsdk-stream/src/sink.rs crates/tqsdk-stream/src/lib.rs crates/tqsdk-stream/tests/stream_commit_flow.rs crates/tqsdk-stream/examples/api_contract_s21_slow_consumer_isolation.rs
git commit -m "feat(stream): add reusable sink profiles"
```

## Task 3: S18 Live Market Cache Pipe Foundation

**Files:**
- Modify: `crates/tqsdk-data/Cargo.toml`
- Create: `crates/tqsdk-data/src/stream_cache.rs`
- Modify: `crates/tqsdk-data/src/lib.rs`
- Create: `crates/tqsdk-data/tests/market_cache_stream.rs`
- Add: `crates/tqsdk-data/examples/api_contract_s18_live_market_cache_pipe.rs`

- [ ] **Step 1: Add optional stream feature and write failing tests**

In `crates/tqsdk-data/Cargo.toml`, add optional dependencies:

```toml
[features]
default = ["live", "services"]
live = ["tqsdk-session/live"]
services = ["dep:reqwest", "tqsdk-session/services"]
stream = ["dep:futures", "dep:tqsdk-stream"]

[dependencies]
futures = { workspace = true, optional = true }
tqsdk-stream = { path = "../tqsdk-stream", default-features = false, optional = true }
```

Create `crates/tqsdk-data/tests/market_cache_stream.rs`:

```rust
#![cfg(feature = "stream")]

use futures::stream;
use tqsdk_core::{CommitResult, CommitScope, ProtocolDomain, Quote, Revision};
use tqsdk_data::{MarketCacheReader, MarketCacheStreamWriter, MarketCacheWriter};
use tqsdk_stream::{MarketEvent, ValueUpdate};

#[tokio::test(flavor = "current_thread")]
async fn market_cache_stream_writer_pipes_quote_events() {
    let path = std::env::temp_dir().join("tqsdk-market-cache-stream-test.jsonl");
    let _ = std::fs::remove_file(&path);
    let writer = MarketCacheWriter::create(&path).unwrap();
    let mut cache = MarketCacheStreamWriter::new("live", writer).unwrap();
    let quote = Quote {
        instrument_id: "SHFE.au2602".to_string(),
        last_price: 480.0,
        ..Quote::default()
    };
    let commit = CommitResult::new(
        Revision::new(7),
        vec![ProtocolDomain::Market],
        Default::default(),
        Vec::new(),
        CommitScope::RealtimeUpdate,
    );

    let written = cache
        .pipe_market_events(
            stream::iter([Ok(MarketEvent::Quote(ValueUpdate { commit, value: quote }))]),
            Some(1),
        )
        .await
        .unwrap();

    assert_eq!(written, 1);
    let events: Vec<_> = MarketCacheReader::open(&path).unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].source, "live");
    assert_eq!(events[0].symbol, "SHFE.au2602");
}
```

- [ ] **Step 2: Run test and verify red**

Run:

```bash
cargo test -p tqsdk-data --features stream --test market_cache_stream market_cache_stream_writer_pipes_quote_events -- --exact
```

Expected: compile failure for missing `MarketCacheStreamWriter`.

- [ ] **Step 3: Implement the bridge**

Create `crates/tqsdk-data/src/stream_cache.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::{Stream, StreamExt};
use tqsdk_stream::MarketEvent;

use crate::{DataError, MarketCacheEvent, MarketCacheWriter, Result};

pub struct MarketCacheStreamWriter<W: Write> {
    source: String,
    writer: MarketCacheWriter<W>,
}

impl<W: Write> MarketCacheStreamWriter<W> {
    pub fn new(source: impl Into<String>, writer: MarketCacheWriter<W>) -> Result<Self> {
        let source = source.into();
        if source.trim().is_empty() {
            return Err(DataError::Validation(
                "cache stream source must not be empty".into(),
            ));
        }
        Ok(Self { source, writer })
    }

    pub fn write_market_event(&mut self, event: MarketEvent) -> Result<usize> {
        let received_at_ns = system_time_ns()?;
        let cache_events = market_event_to_cache_events(&self.source, received_at_ns, event)?;
        let count = cache_events.len();
        for event in cache_events {
            self.writer.write_event(&event)?;
        }
        Ok(count)
    }

    pub async fn pipe_market_events<S>(
        &mut self,
        mut events: S,
        max_events: Option<usize>,
    ) -> Result<usize>
    where
        S: Stream<Item = tqsdk_stream::Result<MarketEvent>> + Unpin,
    {
        let mut written = 0usize;
        while max_events.is_none_or(|max| written < max) {
            let Some(event) = events.next().await else {
                break;
            };
            let event = event.map_err(|error| DataError::Validation(error.to_string()))?;
            written += self.write_market_event(event)?;
        }
        self.writer.flush()?;
        Ok(written)
    }
}
```

Implement `market_event_to_cache_events` for:

- `MarketEvent::Quote(update)`: write one quote event using `update.value.instrument_id`.
- `MarketEvent::KlineWindow(update)`: write only `update.value.last()` to avoid duplicating the whole rolling window.
- `MarketEvent::TickWindow(update)`: write only `update.value.last()` to avoid duplicating the whole rolling window.

Reject empty quote instrument ids with `DataError::Validation("market event quote is missing instrument_id".into())`.

Re-export in `crates/tqsdk-data/src/lib.rs`:

```rust
#[cfg(feature = "stream")]
mod stream_cache;

#[cfg(feature = "stream")]
pub use stream_cache::MarketCacheStreamWriter;
```

- [ ] **Step 4: Add formal S18 live pipe example**

Create `crates/tqsdk-data/examples/api_contract_s18_live_market_cache_pipe.rs` with the standard scenario header. The example should:

```rust
use std::time::Duration;

use tqsdk_data::{MarketCacheStreamWriter, MarketCacheWriter};
use tqsdk_stream::TqStreamBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_CACHE_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".into());
    let path = std::env::temp_dir().join("tqsdk-live-cache-example.jsonl");

    let stream = TqStreamBuilder::new(user, pass).futures_market().build().await?;
    let events = stream
        .market_events()
        .quote(symbol)
        .kline("SHFE.au2602", Duration::from_secs(60), 16)
        .build()
        .await?;

    let writer = MarketCacheWriter::create(&path)?;
    let mut cache = MarketCacheStreamWriter::new("live", writer)?;
    let written = cache.pipe_market_events(events, Some(10)).await?;

    println!("wrote {written} market cache events to {}", path.display());
    Ok(())
}
```

Add to `crates/tqsdk-data/Cargo.toml`:

```toml
[[example]]
name = "api_contract_s18_live_market_cache_pipe"
required-features = ["live", "stream"]
```

- [ ] **Step 5: Run focused verification and commit**

Run:

```bash
cargo test -p tqsdk-data --features stream --test market_cache_stream
cargo check -p tqsdk-data --all-features --examples
```

Expected: both pass.

Commit:

```bash
git add crates/tqsdk-data/Cargo.toml crates/tqsdk-data/src/lib.rs crates/tqsdk-data/src/stream_cache.rs crates/tqsdk-data/tests/market_cache_stream.rs crates/tqsdk-data/examples/api_contract_s18_live_market_cache_pipe.rs
git commit -m "feat(data): add live market cache stream pipe"
```

## Task 4: Scenario Docs and Review Status

**Files:**
- Modify: `docs/public-api-scenario-review.md`
- Modify: `docs/scenarios/user-layer-iteration-plan.md`
- Modify: `docs/scenarios/api_gaps/api_contract_s18_local_market_cache.rs`
- Modify: `docs/scenarios/api_gaps/api_contract_s19_pre_trade_risk.rs`
- Modify: `docs/scenarios/api_gaps/api_contract_s21_slow_consumer_isolation.rs`

- [ ] **Step 1: Update docs**

Update `docs/public-api-scenario-review.md`:

- S18 remains `勉强`, but evidence includes `MarketCacheStreamWriter` and live single-process pipe.
- S19 remains `勉强`, but evidence includes `InstrumentSpec` backed tick-size validation and contract notional projection.
- S21 remains `自然` for the current bounded fan-out / managed sink / WAL foundation, with `StreamSinkProfile` added as ergonomics evidence.

Update gap sketches:

- S18 remaining gap becomes cross-process lock/index, durable daemon queue, and cache compaction beyond local JSONL maintenance.
- S19 remaining gap becomes portfolio margin model, price limit bands, risk hot reload, and durable audit.
- S21 remaining gap remains durable daemon queue and runtime state snapshot recovery.

- [ ] **Step 2: Run contract check**

Run:

```bash
scripts/check_api_contract_examples.sh
cargo check --workspace --examples
cargo check --workspace --all-features --examples
```

Expected: all pass. The locale warning from `scripts/check_api_contract_examples.sh` is acceptable if exit code is 0.

- [ ] **Step 3: Commit docs**

Commit:

```bash
git add docs/public-api-scenario-review.md docs/scenarios/user-layer-iteration-plan.md docs/scenarios/api_gaps/api_contract_s18_local_market_cache.rs docs/scenarios/api_gaps/api_contract_s19_pre_trade_risk.rs docs/scenarios/api_gaps/api_contract_s21_slow_consumer_isolation.rs
git commit -m "docs: update low-level scenario foundation status"
```

## Task 5: Full Verification

**Files:**
- No code edits unless verification finds an issue.

- [ ] **Step 1: Run full required checks**

Run:

```bash
cargo fmt --check
cargo check --workspace --examples
cargo test --workspace
cargo clippy --workspace --examples --all-targets -- -D warnings
cargo check --workspace --no-default-features
cargo check --workspace --all-features --examples
```

Expected: all pass.

- [ ] **Step 2: If feature flags fail**

If `cargo check --workspace --no-default-features` fails because `tqsdk-data` references `tqsdk-stream`, ensure all stream bridge imports and re-exports are behind `#[cfg(feature = "stream")]` and that the new example has `required-features = ["live", "stream"]`.

- [ ] **Step 3: Final status**

Run:

```bash
git status --short --branch
git log --oneline origin/main..HEAD
```

Report commits and verification results. Do not touch unrelated untracked files such as `rrr.md`.

## Self-Review

Spec coverage:

- S14 remains explicitly out of scope.
- S19 gets a bottom-up metadata-backed rule foundation.
- S21 gets reusable sink configuration without changing sink runtime semantics.
- S18 gets a single-process live stream pipe without pretending to solve durable cross-process cache.

Placeholder scan:

- No task contains TBD/TODO placeholders.
- Each task has concrete files, API names, commands, and expected outcomes.

Type consistency:

- `RiskEngine::instrument_specs` consumes `tqsdk_session::InstrumentSpec`.
- `StreamSinkProfile::into_options` returns existing `StreamSinkOptions`.
- `MarketCacheStreamWriter` lives in `tqsdk-data` behind a `stream` feature and consumes `tqsdk_stream::MarketEvent`.
