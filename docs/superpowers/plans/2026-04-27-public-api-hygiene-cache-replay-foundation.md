# Public API Hygiene Cache Replay Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tighten scenario-driven public API contract status and build the next lower-level `tqsdk-data` foundation for local market cache records and deterministic event replay.

**Architecture:** Keep `tqsdk-core` and `tqsdk-session` unchanged as protocol/runtime substrate. Put cache file format, cache reader/writer, and ordered replay event iteration in `tqsdk-data`; do not add live sink runtime, provider aggregation, strategy environment switching, or durable queue orchestration in this batch. Treat S21/S22 as natural only for their low-level sub-contracts and keep full sink/retry orchestration as explicit gaps.

**Tech Stack:** Rust 2024, `serde`/`serde_json`, standard `std::io` reader/writer APIs, existing core market schema types (`Quote`, `Kline`, `Tick`), Cargo workspace examples, scenario review docs.

---

## Batch Scope

This batch is intentionally lower-level and contract-oriented:

- Correct scenario status drift for S21/S22 so formal examples do not imply complete durable sink or retry orchestration support.
- Add a lightweight scenario-contract audit script so future examples keep the required public API header.
- Add `tqsdk-data` market cache record types for quote/kline/tick payloads.
- Add newline-delimited JSON cache writer/reader using SDK standard schema types.
- Add deterministic ordered replay iteration over cached market events.
- Promote only the S18 cache record reader/writer foundation to a formal compiled example.
- Keep S16 history replay strategy, S14 multi-provider aggregation, live stream durable sink, and strategy environment adapters as gaps.

This batch must not:

- Move provider aggregation into `tqsdk-core` or `tqsdk-session`.
- Add a second runtime state tree or private facade revision.
- Add live sink supervisor tasks, channels, WAL, metrics endpoint, or retry policy orchestration.
- Make strategy replay claim parity with live/sim execution before a common `StrategyEnvironment` exists.

## File Structure

- Modify `docs/public-api-scenario-review.md`
  - Change S21/S22 table rows from full “自然” to “勉强” or “底层子集自然，完整场景仍勉强”.
  - Update summary bullets to distinguish formal sub-contract examples from remaining gaps.
- Modify formal examples:
  - `crates/tqsdk-stream/examples/api_contract_s21_slow_consumer_isolation.rs`
  - `crates/tqsdk-stream/examples/api_contract_s22_error_diagnosis_retry.rs`
  - Rename header scenario text to explicitly say “底层子集”.
- Create `scripts/check_api_contract_examples.sh`
  - Check every formal `api_contract_s*.rs` example has the required header sections.
  - Check `docs/scenarios/api_gaps/api_contract_s*.rs` files also carry scenario/review metadata.
- Modify `crates/tqsdk-data/Cargo.toml`
  - Add `serde.workspace = true`.
- Create `crates/tqsdk-data/src/market_cache.rs`
  - Define `MarketCachePayload`, `MarketCacheEvent`, `MarketCacheWriter`, `MarketCacheReader`, and `MarketCacheReplay`.
- Modify `crates/tqsdk-data/src/error.rs`
  - Add JSON serialization/deserialization error support.
- Modify `crates/tqsdk-data/src/lib.rs`
  - Re-export cache/replay types.
- Create `crates/tqsdk-data/tests/market_cache.rs`
  - Test record constructors, JSONL roundtrip, validation, and replay ordering.
- Create `crates/tqsdk-data/examples/api_contract_s18_local_market_cache.rs`
  - Formal S18 foundation example: write cache records, read them, replay in event-time order.
- Modify `docs/scenarios/api_gaps/api_contract_s16_history_replay_strategy.rs`
  - Note that cache event replay foundation exists but strategy driver remains gap.
- Modify `docs/scenarios/api_gaps/api_contract_s18_local_market_cache.rs`
  - Narrow remaining gap to live stream pipe, durable sink runtime, cross-process locking/indexing, and cache replay into strategy context.
- Modify `docs/scenarios/user-layer-iteration-plan.md`
  - Add this batch under P2 local cache / replay.
- Modify `crates/tqsdk-data/README.md`
  - Document cache record reader/writer as offline/research data layer, not live daemon sink.

## Public API Shape

### Cache Record

```rust
use tqsdk_core::{Kline, Quote, Tick};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum MarketCachePayload {
    Quote(Box<Quote>),
    Kline { duration_ns: i64, row: Kline },
    Tick(Tick),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MarketCacheEvent {
    pub source: String,
    pub symbol: String,
    pub received_at_ns: i64,
    pub exchange_time_ns: Option<i64>,
    pub payload: MarketCachePayload,
}
```

### Cache Writer / Reader

```rust
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

pub struct MarketCacheWriter<W: Write> {
    inner: BufWriter<W>,
}

pub struct MarketCacheReader<R: BufRead> {
    lines: std::io::Lines<R>,
}
```

### Ordered Replay

```rust
pub struct MarketCacheReplay {
    events: Vec<MarketCacheEvent>,
    index: usize,
}

impl Iterator for MarketCacheReplay {
    type Item = MarketCacheEvent;
}
```

`MarketCacheReplay` is a deterministic offline iterator, not a live runtime driver. It does not advance `RuntimeHandle`, does not create `CommitResult`, and does not feed `StrategyHost` in this batch.

## Task 1: Tighten S21/S22 Scenario Status

**Files:**
- Modify: `docs/public-api-scenario-review.md`
- Modify: `crates/tqsdk-stream/examples/api_contract_s21_slow_consumer_isolation.rs`
- Modify: `crates/tqsdk-stream/examples/api_contract_s22_error_diagnosis_retry.rs`

- [ ] **Step 1: Update S21 formal example header**

Change the first lines of `crates/tqsdk-stream/examples/api_contract_s21_slow_consumer_isolation.rs`:

```rust
//! Scenario: 慢消费者隔离（bounded fan-out / lag 诊断子集）
//!
//! User goal:
//! - 写库 / 日志不能拖慢核心行情循环
//! - 慢消费者 lag 可见
//! - 核心策略消费者不受影响
//!
//! API contract:
//! - fan-out/backpressure 的底层 capacity 是 public config
//! - fan-out buffer capacity 可显式配置
//! - 慢消费者 lag 通过 typed diagnostic 暴露
//! - durable sink lifecycle / per-sink retry/storage policy 仍是 gap
//! - 不要求用户自建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
```

- [ ] **Step 2: Update S22 formal example header**

Change the first lines of `crates/tqsdk-stream/examples/api_contract_s22_error_diagnosis_retry.rs`:

```rust
//! Scenario: 错误诊断与重试（error diagnostic / retry hint 子集）
//!
//! User goal:
//! - 区分连接错误、登录错误、业务拒单、交易错误
//! - 对可重试错误读取 typed retry hint
//! - 对不可重试错误给出明确诊断
//!
//! API contract:
//! - public error enum 有稳定分类和 retry hint
//! - trade reject 与 transport failure 不混在一个字符串里
//! - retry hint 可读取；完整 retry policy orchestration 保留为 gap
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
```

- [ ] **Step 3: Update review table rows**

In `docs/public-api-scenario-review.md`, update S21/S22 rows to make the full scenario status conservative:

```markdown
| 21. 慢消费者隔离 | 勉强 | 中 | 无 | 少量 | 低 | 低 | 局部重构 | `crates/tqsdk-stream/examples/api_contract_s21_slow_consumer_isolation.rs`; `docs/scenarios/api_gaps/api_contract_s21_slow_consumer_isolation.rs`; bounded fan-out / typed lag diagnostic 子集自然；durable sink runtime 仍是 gap |
| 22. 错误诊断与重试 | 勉强 | 中 | 无 | 少量 | 低 | 低 | 局部重构 | `crates/tqsdk-stream/examples/api_contract_s22_error_diagnosis_retry.rs`; `docs/scenarios/api_gaps/api_contract_s22_error_diagnosis_retry.rs`; error kind / retry hint 子集自然；retry orchestration 仍是 gap |
```

- [ ] **Step 4: Run example check**

Run:

```bash
cargo check -p tqsdk-stream --example api_contract_s21_slow_consumer_isolation
cargo check -p tqsdk-stream --example api_contract_s22_error_diagnosis_retry
```

Expected: both pass.

- [ ] **Step 5: Commit**

```bash
git add docs/public-api-scenario-review.md \
  crates/tqsdk-stream/examples/api_contract_s21_slow_consumer_isolation.rs \
  crates/tqsdk-stream/examples/api_contract_s22_error_diagnosis_retry.rs
git commit -m "docs: clarify stream scenario subcontracts"
```

## Task 2: Add Scenario Contract Audit Script

**Files:**
- Create: `scripts/check_api_contract_examples.sh`
- Modify: `docs/scenarios/user-layer-iteration-plan.md`

- [ ] **Step 1: Create script directory**

Run:

```bash
mkdir -p scripts
```

- [ ] **Step 2: Add audit script**

Create `scripts/check_api_contract_examples.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

required_headers=(
  "Scenario:"
  "User goal:"
  "API contract:"
  "Forbidden:"
  "Regression signal:"
  "Review questions:"
)

check_file() {
  local file="$1"
  local missing=0
  for header in "${required_headers[@]}"; do
    if ! rg -q "^//! ${header}" "$file"; then
      printf 'missing header "%s" in %s\n' "$header" "$file" >&2
      missing=1
    fi
  done
  return "$missing"
}

failed=0
while IFS= read -r file; do
  check_file "$file" || failed=1
done < <(rg --files crates | rg 'examples/api_contract_s[0-9].*\.rs$' | sort)

while IFS= read -r file; do
  check_file "$file" || failed=1
done < <(rg --files docs/scenarios/api_gaps | rg 'api_contract_s[0-9].*\.rs$' | sort)

exit "$failed"
```

- [ ] **Step 3: Make script executable**

Run:

```bash
chmod +x scripts/check_api_contract_examples.sh
```

- [ ] **Step 4: Document the guardrail**

Add to `docs/scenarios/user-layer-iteration-plan.md` under “验收原则”:

```markdown
- 新增或提升场景 example 后运行 `scripts/check_api_contract_examples.sh`，
  确认正式 examples 和 gap sketches 都保留完整场景契约头。
```

- [ ] **Step 5: Run guardrail**

Run:

```bash
scripts/check_api_contract_examples.sh
```

Expected: exits 0.

- [ ] **Step 6: Commit**

```bash
git add scripts/check_api_contract_examples.sh docs/scenarios/user-layer-iteration-plan.md
git commit -m "chore: add scenario contract audit"
```

## Task 3: Add Market Cache Event Contract

**Files:**
- Modify: `crates/tqsdk-data/Cargo.toml`
- Create: `crates/tqsdk-data/src/market_cache.rs`
- Modify: `crates/tqsdk-data/src/lib.rs`
- Modify: `crates/tqsdk-data/src/error.rs`
- Test: `crates/tqsdk-data/tests/market_cache.rs`

- [ ] **Step 1: Add failing tests for cache event construction**

Create `crates/tqsdk-data/tests/market_cache.rs`:

```rust
use tqsdk_core::{Kline, Quote, Tick};
use tqsdk_data::{MarketCacheEvent, MarketCachePayload};

#[test]
fn market_cache_event_constructors_preserve_standard_payloads() {
    let mut quote = Quote::default();
    quote.last_price = 480.5;
    let quote_event = MarketCacheEvent::quote(
        "live",
        "SHFE.au2602",
        1_000,
        Some(900),
        quote.clone(),
    )
    .unwrap();
    assert_eq!(quote_event.source, "live");
    assert_eq!(quote_event.symbol, "SHFE.au2602");
    assert_eq!(quote_event.event_time_ns(), 900);
    match quote_event.payload {
        MarketCachePayload::Quote(payload) => assert_eq!(payload.last_price, 480.5),
        _ => panic!("expected quote payload"),
    }

    let mut kline = Kline::default();
    kline.datetime = 2_000;
    let kline_event = MarketCacheEvent::kline(
        "history",
        "SHFE.au2602",
        2_100,
        Some(2_000),
        60_000_000_000,
        kline,
    )
    .unwrap();
    assert_eq!(kline_event.event_time_ns(), 2_000);

    let mut tick = Tick::default();
    tick.datetime = 3_000;
    let tick_event = MarketCacheEvent::tick("history", "SHFE.au2602", 3_100, None, tick).unwrap();
    assert_eq!(tick_event.event_time_ns(), 3_100);
}

#[test]
fn market_cache_event_rejects_invalid_identity_and_times() {
    assert!(MarketCacheEvent::quote("live", "", 1, None, Quote::default()).is_err());
    assert!(MarketCacheEvent::quote("", "SHFE.au2602", 1, None, Quote::default()).is_err());
    assert!(MarketCacheEvent::quote("live", "SHFE.au2602", -1, None, Quote::default()).is_err());
    assert!(
        MarketCacheEvent::kline("history", "SHFE.au2602", 1, None, 0, Kline::default()).is_err()
    );
}
```

- [ ] **Step 2: Run failing test**

Run:

```bash
cargo test -p tqsdk-data --test market_cache -- --nocapture
```

Expected: compile failure because `MarketCacheEvent` and `MarketCachePayload` do not exist.

- [ ] **Step 3: Add serde dependency**

In `crates/tqsdk-data/Cargo.toml`, add:

```toml
serde.workspace = true
```

- [ ] **Step 4: Add JSON error variant**

Modify `crates/tqsdk-data/src/error.rs`:

```rust
pub enum DataError {
    Session(tqsdk_session::SessionFacadeError),
    PermissionDenied(String),
    Validation(String),
    InvalidState(&'static str),
    InvalidResponse(String),
    Timeout(Duration),
    #[cfg(feature = "services")]
    Http(reqwest::Error),
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl From<serde_json::Error> for DataError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}
```

Update `Display` and `source()` matches so `Json` prints and returns the serde error.

- [ ] **Step 5: Implement cache event contract**

Create `crates/tqsdk-data/src/market_cache.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Lines, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use tqsdk_core::{Kline, Quote, Tick};

use crate::{DataError, Result};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data")]
pub enum MarketCachePayload {
    Quote(Box<Quote>),
    Kline { duration_ns: i64, row: Kline },
    Tick(Tick),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketCacheEvent {
    pub source: String,
    pub symbol: String,
    pub received_at_ns: i64,
    pub exchange_time_ns: Option<i64>,
    pub payload: MarketCachePayload,
}

impl MarketCacheEvent {
    pub fn quote(
        source: impl Into<String>,
        symbol: impl Into<String>,
        received_at_ns: i64,
        exchange_time_ns: Option<i64>,
        quote: Quote,
    ) -> Result<Self> {
        Self::new(
            source,
            symbol,
            received_at_ns,
            exchange_time_ns,
            MarketCachePayload::Quote(Box::new(quote)),
        )
    }

    pub fn kline(
        source: impl Into<String>,
        symbol: impl Into<String>,
        received_at_ns: i64,
        exchange_time_ns: Option<i64>,
        duration_ns: i64,
        row: Kline,
    ) -> Result<Self> {
        if duration_ns <= 0 {
            return Err(DataError::Validation("kline duration must be positive".into()));
        }
        Self::new(
            source,
            symbol,
            received_at_ns,
            exchange_time_ns,
            MarketCachePayload::Kline { duration_ns, row },
        )
    }

    pub fn tick(
        source: impl Into<String>,
        symbol: impl Into<String>,
        received_at_ns: i64,
        exchange_time_ns: Option<i64>,
        tick: Tick,
    ) -> Result<Self> {
        Self::new(
            source,
            symbol,
            received_at_ns,
            exchange_time_ns,
            MarketCachePayload::Tick(tick),
        )
    }

    fn new(
        source: impl Into<String>,
        symbol: impl Into<String>,
        received_at_ns: i64,
        exchange_time_ns: Option<i64>,
        payload: MarketCachePayload,
    ) -> Result<Self> {
        let source = source.into();
        let symbol = symbol.into();
        if source.trim().is_empty() {
            return Err(DataError::Validation("cache event source must not be empty".into()));
        }
        if symbol.trim().is_empty() {
            return Err(DataError::Validation("cache event symbol must not be empty".into()));
        }
        if received_at_ns < 0 {
            return Err(DataError::Validation("received_at_ns must be non-negative".into()));
        }
        if exchange_time_ns.is_some_and(|time| time < 0) {
            return Err(DataError::Validation("exchange_time_ns must be non-negative".into()));
        }
        Ok(Self {
            source,
            symbol,
            received_at_ns,
            exchange_time_ns,
            payload,
        })
    }

    #[must_use]
    pub fn event_time_ns(&self) -> i64 {
        self.exchange_time_ns.unwrap_or(self.received_at_ns)
    }
}
```

- [ ] **Step 6: Re-export types**

Modify `crates/tqsdk-data/src/lib.rs`:

```rust
mod market_cache;

pub use market_cache::{MarketCacheEvent, MarketCachePayload};
```

- [ ] **Step 7: Run test**

Run:

```bash
cargo test -p tqsdk-data --test market_cache -- --nocapture
```

Expected: tests pass.

- [ ] **Step 8: Commit**

```bash
git add crates/tqsdk-data/Cargo.toml crates/tqsdk-data/src/error.rs \
  crates/tqsdk-data/src/lib.rs crates/tqsdk-data/src/market_cache.rs \
  crates/tqsdk-data/tests/market_cache.rs
git commit -m "feat: add market cache event contract"
```

## Task 4: Add JSONL Cache Reader and Writer

**Files:**
- Modify: `crates/tqsdk-data/src/market_cache.rs`
- Test: `crates/tqsdk-data/tests/market_cache.rs`

- [ ] **Step 1: Add failing JSONL roundtrip test**

Append to `crates/tqsdk-data/tests/market_cache.rs`:

```rust
use std::io::Cursor;

use tqsdk_data::{MarketCacheReader, MarketCacheWriter};

#[test]
fn market_cache_writer_and_reader_roundtrip_jsonl_events() {
    let mut quote = Quote::default();
    quote.last_price = 481.0;
    let event = MarketCacheEvent::quote("live", "SHFE.au2602", 1_000, Some(900), quote).unwrap();

    let mut bytes = Vec::new();
    {
        let mut writer = MarketCacheWriter::new(&mut bytes);
        writer.write_event(&event).unwrap();
        writer.flush().unwrap();
    }

    let decoded: Vec<_> = MarketCacheReader::new(Cursor::new(bytes))
        .collect::<tqsdk_data::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(decoded, vec![event]);
}
```

- [ ] **Step 2: Run failing test**

Run:

```bash
cargo test -p tqsdk-data --test market_cache -- --nocapture
```

Expected: compile failure because reader/writer do not exist.

- [ ] **Step 3: Implement reader/writer**

Append to `crates/tqsdk-data/src/market_cache.rs`:

```rust
pub struct MarketCacheWriter<W: Write> {
    inner: BufWriter<W>,
}

impl MarketCacheWriter<File> {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::new(File::create(path)?))
    }
}

impl<W: Write> MarketCacheWriter<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner: BufWriter::new(inner),
        }
    }

    pub fn write_event(&mut self, event: &MarketCacheEvent) -> Result<()> {
        serde_json::to_writer(&mut self.inner, event)?;
        self.inner.write_all(b"\n")?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.inner.flush()?;
        Ok(())
    }
}

pub struct MarketCacheReader<R: BufRead> {
    lines: Lines<R>,
}

impl MarketCacheReader<BufReader<File>> {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::new(BufReader::new(File::open(path)?)))
    }
}

impl<R: BufRead> MarketCacheReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            lines: inner.lines(),
        }
    }
}

impl<R: BufRead> Iterator for MarketCacheReader<R> {
    type Item = Result<MarketCacheEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        let line = self.lines.next()?;
        Some(line.map_err(DataError::from).and_then(|line| {
            if line.trim().is_empty() {
                Err(DataError::InvalidResponse("empty market cache line".into()))
            } else {
                serde_json::from_str(&line).map_err(DataError::from)
            }
        }))
    }
}
```

- [ ] **Step 4: Re-export reader/writer**

Modify `crates/tqsdk-data/src/lib.rs`:

```rust
pub use market_cache::{
    MarketCacheEvent, MarketCachePayload, MarketCacheReader, MarketCacheWriter,
};
```

- [ ] **Step 5: Run focused test**

Run:

```bash
cargo test -p tqsdk-data --test market_cache -- --nocapture
```

Expected: tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/tqsdk-data/src/lib.rs crates/tqsdk-data/src/market_cache.rs \
  crates/tqsdk-data/tests/market_cache.rs
git commit -m "feat: add market cache jsonl reader writer"
```

## Task 5: Add Deterministic Market Cache Replay

**Files:**
- Modify: `crates/tqsdk-data/src/market_cache.rs`
- Modify: `crates/tqsdk-data/src/lib.rs`
- Test: `crates/tqsdk-data/tests/market_cache.rs`

- [ ] **Step 1: Add failing replay ordering test**

Append to `crates/tqsdk-data/tests/market_cache.rs`:

```rust
use tqsdk_data::MarketCacheReplay;

#[test]
fn market_cache_replay_orders_events_by_event_time_then_receive_time() {
    let late_received_early_exchange =
        MarketCacheEvent::quote("live", "SHFE.au2602", 2_000, Some(1_000), Quote::default())
            .unwrap();
    let early_received_late_exchange =
        MarketCacheEvent::quote("live", "SHFE.au2602", 1_000, Some(3_000), Quote::default())
            .unwrap();
    let no_exchange_time =
        MarketCacheEvent::quote("live", "SHFE.au2602", 1_500, None, Quote::default()).unwrap();

    let replay = MarketCacheReplay::new(vec![
        early_received_late_exchange.clone(),
        no_exchange_time.clone(),
        late_received_early_exchange.clone(),
    ]);
    let ordered: Vec<_> = replay.collect();

    assert_eq!(
        ordered,
        vec![
            late_received_early_exchange,
            no_exchange_time,
            early_received_late_exchange,
        ]
    );
}
```

- [ ] **Step 2: Run failing test**

Run:

```bash
cargo test -p tqsdk-data --test market_cache -- --nocapture
```

Expected: compile failure because `MarketCacheReplay` does not exist.

- [ ] **Step 3: Implement replay iterator**

Append to `crates/tqsdk-data/src/market_cache.rs`:

```rust
pub struct MarketCacheReplay {
    events: Vec<MarketCacheEvent>,
    index: usize,
}

impl MarketCacheReplay {
    #[must_use]
    pub fn new(mut events: Vec<MarketCacheEvent>) -> Self {
        events.sort_by_key(|event| (event.event_time_ns(), event.received_at_ns));
        Self { events, index: 0 }
    }

    pub fn from_reader<R: BufRead>(reader: MarketCacheReader<R>) -> Result<Self> {
        let events = reader.collect::<Result<Vec<_>>>()?;
        Ok(Self::new(events))
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.events.len().saturating_sub(self.index)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Iterator for MarketCacheReplay {
    type Item = MarketCacheEvent;

    fn next(&mut self) -> Option<Self::Item> {
        let event = self.events.get(self.index)?.clone();
        self.index += 1;
        Some(event)
    }
}
```

- [ ] **Step 4: Re-export replay**

Modify `crates/tqsdk-data/src/lib.rs`:

```rust
pub use market_cache::{
    MarketCacheEvent, MarketCachePayload, MarketCacheReader, MarketCacheReplay,
    MarketCacheWriter,
};
```

- [ ] **Step 5: Run focused test**

Run:

```bash
cargo test -p tqsdk-data --test market_cache -- --nocapture
```

Expected: tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/tqsdk-data/src/lib.rs crates/tqsdk-data/src/market_cache.rs \
  crates/tqsdk-data/tests/market_cache.rs
git commit -m "feat: add market cache replay iterator"
```

## Task 6: Promote S18 Cache Foundation Example and Update Gaps

**Files:**
- Create: `crates/tqsdk-data/examples/api_contract_s18_local_market_cache.rs`
- Modify: `docs/public-api-scenario-review.md`
- Modify: `docs/scenarios/api_gaps/api_contract_s18_local_market_cache.rs`
- Modify: `docs/scenarios/api_gaps/api_contract_s16_history_replay_strategy.rs`
- Modify: `docs/scenarios/user-layer-iteration-plan.md`
- Modify: `crates/tqsdk-data/README.md`

- [ ] **Step 1: Add formal S18 foundation example**

Create `crates/tqsdk-data/examples/api_contract_s18_local_market_cache.rs`:

```rust
//! Scenario: 本地行情缓存读写（cache record / replay 子集）
//!
//! User goal:
//! - 将标准行情对象写入本地缓存文件
//! - 从缓存文件读取标准行情对象
//! - 按事件时间顺序回放缓存记录
//!
//! API contract:
//! - cache writer/reader 是明确 public API
//! - 缓存 payload 使用 SDK 标准 `Quote` / `Kline` / `Tick`
//! - replay ordering 由 SDK 提供
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 用户自己定义缓存文件格式
//! - 业务代码直接写 state tree dump
//! - provider 私有 protocol type
//! - cache 读写污染 `tqsdk-core` / `tqsdk-session`
//!
//! Regression signal:
//! - 多进程共享只能靠用户手写 CSV/JSON
//! - cache replay 无法复用标准 market schema
//! - 用户需要自己处理排序
//!
//! Review questions:
//! - 当前 API 是否自然表达本地缓存 foundation？
//! - 剩余 live sink / durable queue gap 是否被明确排除？
//! - 是否存在热路径性能风险？
//!
//! Current API note:
//! 本示例只验证离线 cache record、JSONL reader/writer 和 deterministic replay。
//! live stream pipe、durable sink runtime、跨进程锁/index 和 strategy replay driver
//! 仍保留在 `docs/scenarios/api_gaps/`。

use tqsdk_core::Quote;
use tqsdk_data::{MarketCacheEvent, MarketCacheReader, MarketCacheReplay, MarketCacheWriter};

fn main() -> tqsdk_data::Result<()> {
    let path = std::env::temp_dir().join("tqsdk-cache-example.jsonl");

    let mut quote = Quote::default();
    quote.last_price = 480.5;

    let mut writer = MarketCacheWriter::create(&path)?;
    writer.write_event(&MarketCacheEvent::quote(
        "example",
        "SHFE.au2602",
        1_000,
        Some(900),
        quote,
    )?)?;
    writer.flush()?;

    let replay = MarketCacheReplay::from_reader(MarketCacheReader::open(&path)?)?;
    for event in replay {
        println!(
            "source={} symbol={} event_time_ns={}",
            event.source,
            event.symbol,
            event.event_time_ns()
        );
    }

    Ok(())
}
```

- [ ] **Step 2: Update scenario review S18/S16 rows**

In `docs/public-api-scenario-review.md`:

```markdown
| 16. 历史行情回放 | 不自然 | 高 | 少量 | 少量 | 中 | 中 | 局部重构 | `docs/scenarios/api_gaps/api_contract_s16_history_replay_strategy.rs`; `MarketCacheReplay` provides ordered offline events, but no strategy replay driver |
| 18. 本地行情缓存读写 | 勉强 | 中 | 无 | 无 | 低 | 中 | 局部重构 | `crates/tqsdk-data/examples/api_contract_s18_local_market_cache.rs`; `docs/scenarios/api_gaps/api_contract_s18_local_market_cache.rs`; `MarketCacheWriter`; `MarketCacheReader`; `MarketCacheReplay`; live durable sink still gap |
```

- [ ] **Step 3: Update S18 gap note**

In `docs/scenarios/api_gaps/api_contract_s18_local_market_cache.rs`, replace the API gap paragraph:

```rust
//! API gap:
//! `tqsdk-data` 已提供离线 cache record、JSONL reader/writer 和 ordered
//! replay iterator。剩余 gap 是 live market stream pipe、可靠 sink runtime、
//! 跨进程锁/index、cache compaction 和将 cache replay 接入 `StrategyHost`
//! 同构 context。
```

- [ ] **Step 4: Update S16 gap note**

In `docs/scenarios/api_gaps/api_contract_s16_history_replay_strategy.rs`, replace the remaining gap paragraph:

```rust
//! Remaining API gap:
//! `tqsdk-data` 能拉取历史序列，并提供 cache event replay iterator；
//! `tqsdk-session` 有 replay control-plane helper。但还没有把历史/cache
//! event 转成 `StrategyHost` 同构 update/context 的 public replay driver。
```

- [ ] **Step 5: Update data README**

Add a short section to `crates/tqsdk-data/README.md`:

```markdown
## Market Cache Foundation

`MarketCacheEvent` / `MarketCacheWriter` / `MarketCacheReader` /
`MarketCacheReplay` define the offline cache record and replay foundation for
standard `Quote` / `Kline` / `Tick` payloads.

This is not a live durable sink runtime: it does not spawn tasks, isolate slow
consumers, manage WAL compaction, or drive `StrategyHost`. Those remain
scenario gaps above this data-layer foundation.
```

- [ ] **Step 6: Run example check**

Run:

```bash
cargo check -p tqsdk-data --example api_contract_s18_local_market_cache
scripts/check_api_contract_examples.sh
```

Expected: both pass.

- [ ] **Step 7: Commit**

```bash
git add crates/tqsdk-data/examples/api_contract_s18_local_market_cache.rs \
  docs/public-api-scenario-review.md \
  docs/scenarios/api_gaps/api_contract_s18_local_market_cache.rs \
  docs/scenarios/api_gaps/api_contract_s16_history_replay_strategy.rs \
  docs/scenarios/user-layer-iteration-plan.md \
  crates/tqsdk-data/README.md
git commit -m "docs: promote local market cache foundation"
```

## Task 7: Full Verification

**Files:**
- No source changes unless verification exposes issues.

- [ ] **Step 1: Run scenario guardrail**

Run:

```bash
scripts/check_api_contract_examples.sh
```

Expected: exits 0.

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
git log --oneline -8
```

Expected: only intentional tracked changes are committed; unrelated untracked files remain untouched.

## Self-Review

- Spec coverage: The plan addresses the review findings by correcting S21/S22 status, adding a guardrail for scenario headers, and advancing S18/S16 lower-level cache/replay foundation without overclaiming full live sink or strategy replay.
- Completion scan: No task uses unresolved wording; code snippets define concrete types and commands.
- Type consistency: Public types are consistently named `MarketCacheEvent`, `MarketCachePayload`, `MarketCacheWriter`, `MarketCacheReader`, and `MarketCacheReplay`; docs and examples use the same names.
