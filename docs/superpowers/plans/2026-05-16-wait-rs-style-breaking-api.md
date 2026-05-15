# Wait Rs-Style Breaking API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current `tqsdk-wait` public API with a `tqsdk-rs`-style handle/step API in the first version, deleting the old `get_*`, `load(&api)`, and API-level change-checking surface instead of preserving compatibility aliases.

**Architecture:** `tqsdk-wait` remains the single-owner Python-like strategy facade, but its public shape changes to handles that carry their own read context and a `step()` loop that returns a commit-backed `WaitStep`. The implementation must keep the existing single runtime state source: every visible change still flows through `RuntimeHandle -> StateStore -> CommitResult -> RuntimeReader`; do not introduce a second `MarketDataState`, watch/broadcast tree, or local order overlay.

**Tech Stack:** Rust 2024, existing `tqsdk-core` runtime reader/commit log, `tqsdk-session` shared session, `tqsdk-wait` facade, `tqsdk-task` strategy/task integration, existing contract examples and architecture docs.

---

## Scope

This is a breaking first release. Do not add deprecated aliases for the old wait API.

Delete these public `tqsdk-wait` methods:

- `TqApi::get_quote`
- `TqApi::quote_snapshot`
- `TqApi::get_kline_serial`
- `TqApi::get_tick_serial`
- `TqApi::is_changing`
- `TqApi::is_changing_fields`
- `TqApi::is_serial_ready`
- `QuoteRef::snapshot(&TqApi)`
- `QuoteRef::load(&TqApi)`
- `KlineSerialRef::load(&TqApi)`
- `TickSerialRef::load(&TqApi)`
- every other wait ref `load(&TqApi)` method touched by public examples

Replace with:

- `TqApi::quote(symbol).await? -> QuoteRef`
- `TqApi::kline(symbol, duration, data_length).await? -> KlineHandle`
- `TqApi::tick(symbol, data_length).await? -> TickHandle`
- `TqApi::trading_status(symbol).await? -> TradingStatusRef`
- `TqApi::step().await? -> Option<WaitStep>`
- `TqApi::step_until(deadline).await? -> Option<WaitStep>`
- `WaitStep::is_changing(&handle_or_ref)`
- `WaitStep::is_changing_fields(&handle_or_ref, fields)`
- `WaitStep::current_dt()`
- `QuoteRef::snapshot()`
- `QuoteRef::load()`
- `KlineHandle::window()`
- `KlineHandle::rows()`
- `KlineHandle::completed_rows()`
- `TickHandle::window()`
- `TickHandle::rows()`

Keep these APIs only if they are already `tqsdk-rs`-style enough or are outside wait facade:

- `TqApiBuilder`
- `TqApi::session()`
- `TqApi::into_session()`
- order command methods such as `insert_order`, `insert_limit_order`, and `cancel_order`, unless a task explicitly renames them
- `tqsdk-session` direct query APIs
- `tqsdk-data` history/offline APIs
- `tqsdk-stream` event-stream APIs

## File Map

Modify:

- `crates/tqsdk-wait/src/api.rs`: replace old methods with `quote/kline/tick/step/step_until`, construct refs with a cloned `RuntimeReader`, remove API-level `is_changing*`.
- `crates/tqsdk-wait/src/driver.rs`: keep single-owner wait state; add step bookkeeping if needed.
- `crates/tqsdk-wait/src/change.rs`: keep `ChangeTrackedRef`, make it consumed by `WaitStep` rather than `TqApi`.
- `crates/tqsdk-wait/src/refs/*.rs`: make refs self-reading via a shared `WaitReadHandle`; remove `load(&TqApi)` and `snapshot(&TqApi)` signatures.
- `crates/tqsdk-wait/src/views/kline_window.rs`: expose row helpers used by `KlineHandle`.
- `crates/tqsdk-wait/src/views/tick_window.rs`: expose row helpers used by `TickHandle`.
- `crates/tqsdk-wait/src/lib.rs`: export new types and stop exporting deleted names where applicable.
- `crates/tqsdk-wait/src/builder.rs`: add breaking builder tests and later backtest config wiring.
- `crates/tqsdk-wait/README.md`: rewrite examples to new handle/step API.
- `crates/tqsdk-wait/examples/*.rs`: update all wait contract examples.
- `crates/tqsdk-wait/tests/*.rs`: update tests to new API and add source-level deletion guards.
- `crates/tqsdk-task/src/strategy.rs`: switch strategy context market reads to new wait handles.
- `crates/tqsdk-task/src/target_pos/machine.rs`: switch quote subscription/read calls to `quote()` and self-reading refs.
- `crates/tqsdk-task/examples/*.rs`: update wait API usage.
- `skills/tqsdk-rust/assets/templates/wait-quote-loop/src/main.rs`: rewrite starter template.
- `skills/tqsdk-rust/references/*.md`: rewrite routing and code patterns.
- `docs/architecture/api-wait.md`: update wait facade architecture.
- `docs/architecture/crate-boundaries.md`: update public wait API names.
- `docs/architecture/facade-paradigms.md`: update wait facade model.
- `docs/architecture/README.md`: update public surface table.
- `docs/reviews/public-api-scenario-review.md`: update scenario matrix.

Create:

- `crates/tqsdk-wait/src/step.rs`: `WaitStep` and `WaitReadHandle`.
- `crates/tqsdk-wait/src/backtest.rs`: `TqBacktest` config, once API surface is stable.
- `crates/tqsdk-wait/examples/api_contract_s32_wait_live_backtest_same_body.rs`: same strategy body for live/backtest builders.

## Task 1: Add Breaking Surface Tests

**Files:**
- Modify: `crates/tqsdk-wait/tests/wait_api_surface.rs`
- Modify: `crates/tqsdk-wait/tests/wait_api_market.rs`

- [ ] **Step 1: Write source-level deletion guards**

Add this test to `crates/tqsdk-wait/tests/wait_api_surface.rs`:

```rust
#[test]
fn wait_api_removes_legacy_get_and_api_bound_ref_surface() {
    let api_source = std::fs::read_to_string("src/api.rs").expect("read api.rs");
    let quote_source = std::fs::read_to_string("src/refs/quote.rs").expect("read quote.rs");
    let kline_source = std::fs::read_to_string("src/refs/kline.rs").expect("read kline.rs");
    let tick_source = std::fs::read_to_string("src/refs/tick.rs").expect("read tick.rs");

    for legacy in [
        "pub async fn get_quote",
        "pub async fn quote_snapshot",
        "pub async fn get_kline_serial",
        "pub async fn get_tick_serial",
        "pub fn is_changing(",
        "pub fn is_changing_fields",
        "pub fn is_serial_ready",
    ] {
        assert!(
            !api_source.contains(legacy),
            "legacy wait API still present: {legacy}"
        );
    }

    for source in [quote_source, kline_source, tick_source] {
        assert!(
            !source.contains("&TqApi"),
            "wait refs must not require &TqApi for snapshot/load"
        );
        assert!(
            !source.contains("load(&self, api:"),
            "wait refs must not expose load(&api)"
        );
    }
}
```

- [ ] **Step 2: Run deletion guard and verify it fails**

Run:

```bash
cargo test -p tqsdk-wait wait_api_removes_legacy_get_and_api_bound_ref_surface
```

Expected: FAIL, because the old API still exists.

- [ ] **Step 3: Write new handle/step behavior tests**

In `crates/tqsdk-wait/tests/wait_api_market.rs`, replace tests that call `get_quote`, `get_kline_serial`, and `get_tick_serial` with tests shaped like this:

```rust
#[tokio::test]
async fn quote_handle_reads_snapshot_without_api_argument_after_step() {
    let mut api = seeded_market_api();
    let quote = api.quote("SHFE.au2602").await.unwrap();

    let step = api.step().await.unwrap().expect("seed commit should produce step");

    assert!(step.is_changing(&quote));
    let snapshot = quote.load().unwrap();
    assert_eq!(snapshot.instrument_id, "au2602");
}

#[tokio::test]
async fn kline_handle_reads_bounded_window_without_api_argument_after_step() {
    let mut api = seeded_kline_api_with_outer_rows();
    let bars = api
        .kline("SHFE.au2602", std::time::Duration::from_secs(60), 64)
        .await
        .unwrap();

    let step = api.step().await.unwrap().expect("chart commit should produce step");

    assert!(step.is_changing(&bars));
    let window = bars.window().unwrap();
    assert_eq!(window.view_width(), 64);
    assert!(window.rows().iter().all(|row| row.id >= 10 && row.id <= 12));
}

#[tokio::test]
async fn tick_handle_reads_bounded_window_without_api_argument_after_step() {
    let mut api = seeded_tick_api_with_outer_rows();
    let ticks = api.tick("SHFE.au2602", 32).await.unwrap();

    let step = api.step().await.unwrap().expect("chart commit should produce step");

    assert!(step.is_changing(&ticks));
    let window = ticks.window().unwrap();
    assert_eq!(window.view_width(), 32);
    assert!(window.rows().iter().all(|row| row.id >= 20 && row.id <= 22));
}
```

Use existing support helpers from `crates/tqsdk-wait/tests/support/core_seed.rs`; rename helper functions only if the new names make the new API tests clearer.

- [ ] **Step 4: Run new tests and verify they fail**

Run:

```bash
cargo test -p tqsdk-wait quote_handle_reads_snapshot_without_api_argument_after_step
cargo test -p tqsdk-wait kline_handle_reads_bounded_window_without_api_argument_after_step
cargo test -p tqsdk-wait tick_handle_reads_bounded_window_without_api_argument_after_step
```

Expected: FAIL with missing `quote`, `kline`, `tick`, `step`, or no-argument handle methods.

- [ ] **Step 5: Commit tests**

```bash
git add crates/tqsdk-wait/tests/wait_api_surface.rs crates/tqsdk-wait/tests/wait_api_market.rs
git commit -m "test(wait): define breaking handle step api"
```

## Task 2: Introduce `WaitReadHandle` and `WaitStep`

**Files:**
- Create: `crates/tqsdk-wait/src/step.rs`
- Modify: `crates/tqsdk-wait/src/lib.rs`
- Modify: `crates/tqsdk-wait/src/api.rs`
- Modify: `crates/tqsdk-wait/src/change.rs`

- [ ] **Step 1: Add the step module**

Create `crates/tqsdk-wait/src/step.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use std::sync::Arc;

use tqsdk_core::{CommitResult, RuntimeReader, Revision};

use crate::change::{ChangeTrackedRef, matches_any, matches_fields};

#[derive(Clone)]
pub(crate) struct WaitReadHandle {
    reader: RuntimeReader,
}

impl WaitReadHandle {
    pub(crate) fn new(reader: RuntimeReader) -> Self {
        Self { reader }
    }

    pub(crate) fn reader(&self) -> &RuntimeReader {
        &self.reader
    }
}

#[derive(Debug, Clone)]
pub struct WaitStep {
    commit: Arc<CommitResult>,
    current_dt: Option<i64>,
}

impl WaitStep {
    pub(crate) fn new(commit: Arc<CommitResult>, current_dt: Option<i64>) -> Self {
        Self { commit, current_dt }
    }

    pub fn revision(&self) -> Revision {
        self.commit.revision
    }

    pub fn current_dt(&self) -> Option<i64> {
        self.current_dt
    }

    pub fn is_changing(&self, target: &impl ChangeTrackedRef) -> bool {
        matches_any(&self.commit.changes, target)
    }

    pub fn is_changing_fields(&self, target: &impl ChangeTrackedRef, fields: &[&str]) -> bool {
        matches_fields(&self.commit.changes, target, fields)
    }
}
```

Add a private helper in `crates/tqsdk-wait/src/api.rs` so `WaitStep` does not need to read runtime state itself:

```rust
fn current_dt_from_reader(reader: &tqsdk_core::RuntimeReader) -> Option<i64> {
    reader
        .decode_value_at_path::<serde_json::Value>(&["_tqsdk_backtest", "current_dt"])
        .ok()
        .flatten()
        .and_then(|value| value.as_i64())
        .or_else(|| {
            reader
                .decode_value_at_path::<serde_json::Value>(&["replay"])
                .ok()
                .flatten()
                .and_then(|value| value.pointer("/cursor/dt").and_then(serde_json::Value::as_i64))
        })
}
```

If the exact `RuntimeReader` method name differs, use the existing path decode helper from `crates/tqsdk-core/src/runtime/reader.rs`; the implementation must read `_tqsdk_backtest/current_dt` or a typed replay cursor path from the runtime state tree.

- [ ] **Step 2: Export the step type**

Modify `crates/tqsdk-wait/src/lib.rs`:

```rust
mod step;

pub use step::WaitStep;
```

Keep `WaitReadHandle` crate-private.

- [ ] **Step 3: Add `TqApi::step` and `step_until`**

Modify `crates/tqsdk-wait/src/api.rs`:

```rust
use crate::step::{WaitReadHandle, WaitStep};

impl TqApi {
    pub async fn step(&mut self) -> crate::error::Result<Option<WaitStep>> {
        self.step_until(None).await
    }

    pub async fn step_until(
        &mut self,
        deadline: Option<tokio::time::Instant>,
    ) -> crate::error::Result<Option<WaitStep>> {
        let _guard = WaitGuard::new(&self.driver.waiting)?;

        if let Some(commit) = self.driver.deferred_commits.pop_front() {
            self.driver.last_commit = Some(commit.clone());
            return Ok(Some(WaitStep::new(
                commit,
                current_dt_from_reader(&self.driver.reader),
            )));
        }

        loop {
            if let Some(commit) = self.driver.reader.next(&mut self.driver.cursor) {
                self.driver.last_commit = Some(commit.clone());
                return Ok(Some(WaitStep::new(
                    commit,
                    current_dt_from_reader(&self.driver.reader),
                )));
            }

            let progress = self
                .driver
                .session
                .progress_once(deadline)
                .await
                .map_err(crate::error::WaitFacadeError::Session)?;
            if !progress.is_progress() {
                return Ok(None);
            }
        }
    }

    pub(crate) fn read_handle(&self) -> WaitReadHandle {
        WaitReadHandle::new(self.driver.reader.clone())
    }
}
```

Keep `last_commit()` for diagnostics if tests still need it; do not use it as the recommended change API.

- [ ] **Step 4: Run focused tests**

Run:

```bash
cargo test -p tqsdk-wait quote_handle_reads_snapshot_without_api_argument_after_step
```

Expected: still FAIL because refs have not been refactored.

- [ ] **Step 5: Commit step infrastructure**

```bash
git add crates/tqsdk-wait/src/step.rs crates/tqsdk-wait/src/lib.rs crates/tqsdk-wait/src/api.rs
git commit -m "feat(wait): add commit backed wait step"
```

## Task 3: Make Wait Refs Self-Reading

**Files:**
- Modify: `crates/tqsdk-wait/src/refs/quote.rs`
- Modify: `crates/tqsdk-wait/src/refs/kline.rs`
- Modify: `crates/tqsdk-wait/src/refs/tick.rs`
- Modify: `crates/tqsdk-wait/src/refs/trade.rs`
- Modify: `crates/tqsdk-wait/src/refs/security.rs`
- Modify: `crates/tqsdk-wait/src/refs/trading_status.rs`
- Modify: `crates/tqsdk-wait/src/refs/extensions.rs`

- [ ] **Step 1: Refactor `QuoteRef`**

Change `crates/tqsdk-wait/src/refs/quote.rs` to store `WaitReadHandle`:

```rust
use crate::step::WaitReadHandle;

#[derive(Clone)]
pub struct QuoteRef {
    reader: WaitReadHandle,
    symbol: Symbol,
}

impl QuoteRef {
    pub(crate) fn new(reader: WaitReadHandle, symbol: impl Into<String>) -> Self {
        Self {
            reader,
            symbol: Symbol::new(symbol.into()),
        }
    }

    pub fn symbol(&self) -> &str {
        self.symbol.as_str()
    }

    pub fn is_ready(&self) -> crate::error::Result<bool> {
        Ok(self.snapshot()?.is_some())
    }

    pub fn snapshot(&self) -> crate::error::Result<Option<Quote>> {
        self.reader
            .reader()
            .read_market_state()
            .quote(&self.symbol)
            .map_err(Into::into)
    }

    pub fn load(&self) -> crate::error::Result<Quote> {
        self.snapshot()?
            .ok_or(crate::error::WaitFacadeError::InvalidState("quote not ready"))
    }
}
```

Keep the existing `ChangeTrackedRef` implementation; it should still only depend on symbol identity.

- [ ] **Step 2: Refactor Kline and Tick handles**

Rename `KlineSerialRef` to `KlineHandle` and `TickSerialRef` to `TickHandle` if this does not create excessive churn in one patch. If the rename is too noisy, keep internal file names and public type aliases out of the first patch; the public exported name must be the new one.

The new kline handle shape:

```rust
#[derive(Clone)]
pub struct KlineHandle {
    reader: WaitReadHandle,
    symbol: String,
    duration_ns: i64,
    view_width: usize,
    chart_id: String,
}

impl KlineHandle {
    pub(crate) fn new(
        reader: WaitReadHandle,
        symbol: String,
        duration_ns: i64,
        view_width: usize,
        chart_id: String,
    ) -> Self {
        Self {
            reader,
            symbol,
            duration_ns,
            view_width,
            chart_id,
        }
    }

    pub fn is_ready(&self) -> crate::error::Result<bool> {
        let guard = self.reader.reader().read_market_state();
        let ready = guard
            .get_path(&["charts", self.chart_id.as_str(), "ready"])
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        let more_data = guard
            .get_path(&["charts", self.chart_id.as_str(), "more_data"])
            .and_then(|value| value.as_bool())
            .unwrap_or(true);

        Ok(ready && !more_data && !self.window()?.is_empty())
    }

    pub fn window(&self) -> crate::error::Result<KlineWindow> {
        let guard = self.reader.reader().read_market_state();
        let mut rows = Vec::new();
        let duration_key = self.duration_ns.to_string();

        if let Some((left_id, right_id)) = chart_bounds(&guard, self.chart_id.as_str()) {
            for id in left_id..=right_id {
                let id_key = id.to_string();
                if let Some(row) = guard.decode_path::<tqsdk_core::Kline>(&[
                    "klines",
                    self.symbol.as_str(),
                    duration_key.as_str(),
                    "data",
                    id_key.as_str(),
                ])? {
                    rows.push(row);
                }
            }
        }

        Ok(KlineWindow::new(
            self.symbol.clone(),
            self.duration_ns,
            self.view_width,
            self.chart_id.clone(),
            rows,
        ))
    }

    pub fn rows(&self) -> crate::error::Result<Vec<tqsdk_core::Kline>> {
        Ok(self.window()?.into_rows())
    }

    pub fn completed_rows(&self) -> crate::error::Result<Vec<tqsdk_core::Kline>> {
        Ok(self.window()?.completed_rows().to_vec())
    }
}
```

The tick handle mirrors kline:

```rust
pub fn window(&self) -> crate::error::Result<TickWindow>;
pub fn rows(&self) -> crate::error::Result<Vec<tqsdk_core::Tick>>;
```

- [ ] **Step 3: Refactor trade/security/extension refs**

For every wait ref currently exposing `load(&api)`, store `WaitReadHandle` and expose:

```rust
pub fn load(&self) -> crate::error::Result<TypedObject>;
pub fn snapshot(&self) -> crate::error::Result<Option<TypedObject>>;
```

Use the same domain read guard as the old method. For example, account-like refs must continue using `read_trade_state()` or the existing partition-specific helper, not a full snapshot clone.

- [ ] **Step 4: Run ref tests**

Run:

```bash
cargo test -p tqsdk-wait quote_handle_reads_snapshot_without_api_argument_after_step
cargo test -p tqsdk-wait kline_handle_reads_bounded_window_without_api_argument_after_step
cargo test -p tqsdk-wait tick_handle_reads_bounded_window_without_api_argument_after_step
```

Expected: still FAIL until API constructors are renamed.

- [ ] **Step 5: Commit self-reading refs**

```bash
git add crates/tqsdk-wait/src/refs crates/tqsdk-wait/tests
git commit -m "refactor(wait): make refs self reading"
```

## Task 4: Replace `TqApi` Public Methods and Delete Legacy Surface

**Files:**
- Modify: `crates/tqsdk-wait/src/api.rs`
- Modify: `crates/tqsdk-wait/src/refs/mod.rs`
- Modify: `crates/tqsdk-wait/src/lib.rs`

- [ ] **Step 1: Replace market subscription methods**

In `crates/tqsdk-wait/src/api.rs`, delete old methods and add:

```rust
pub async fn quote(&mut self, symbol: &str) -> crate::error::Result<QuoteRef> {
    self.driver
        .session
        .submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
            symbols: vec![Symbol::new(symbol)],
        }))
        .await
        .map_err(crate::error::WaitFacadeError::Session)?;

    Ok(QuoteRef::new(self.read_handle(), symbol))
}

pub async fn trading_status(&mut self, symbol: &str) -> crate::error::Result<TradingStatusRef> {
    self.driver
        .session
        .submit(RuntimeCommand::Market(
            MarketCommand::SubscribeTradingStatus {
                symbols: vec![Symbol::new(symbol)],
            },
        ))
        .await
        .map_err(crate::error::WaitFacadeError::Session)?;

    Ok(TradingStatusRef::new(self.read_handle(), symbol))
}

pub async fn kline(
    &mut self,
    symbol: &str,
    duration: Duration,
    data_length: usize,
) -> crate::error::Result<KlineHandle> {
    let data_length = normalize_serial_data_length(data_length)?;
    let duration_ns = duration_to_ns(duration)?;
    let chart_id = format!("wait-kline-{symbol}-{duration_ns}-{data_length}");

    if !self.driver.serial_charts.contains(&chart_id) {
        self.driver
            .session
            .submit(RuntimeCommand::Market(MarketCommand::SetChart(
                MarketChartCommand {
                    chart_id: chart_id.clone(),
                    symbols: vec![Symbol::new(symbol)],
                    duration_ns,
                    view_width: data_length,
                    left_kline_id: None,
                    focus_datetime_ns: None,
                    focus_position: None,
                },
            )))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;
        self.driver.serial_charts.insert(chart_id.clone());
    }

    let handle = KlineHandle::new(
        self.read_handle(),
        symbol.to_string(),
        duration_ns,
        data_length,
        chart_id,
    );
    self.wait_until_ready_for_step_api(|_| handle.is_ready()).await?;
    Ok(handle)
}

pub async fn tick(&mut self, symbol: &str, data_length: usize) -> crate::error::Result<TickHandle> {
    let data_length = normalize_serial_data_length(data_length)?;
    let chart_id = format!("wait-tick-{symbol}-{data_length}");

    if !self.driver.serial_charts.contains(&chart_id) {
        self.driver
            .session
            .submit(RuntimeCommand::Market(MarketCommand::SetChart(
                MarketChartCommand {
                    chart_id: chart_id.clone(),
                    symbols: vec![Symbol::new(symbol)],
                    duration_ns: 0,
                    view_width: data_length,
                    left_kline_id: None,
                    focus_datetime_ns: None,
                    focus_position: None,
                },
            )))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;
        self.driver.serial_charts.insert(chart_id.clone());
    }

    let handle = TickHandle::new(self.read_handle(), symbol.to_string(), data_length, chart_id);
    self.wait_until_ready_for_step_api(|_| handle.is_ready()).await?;
    Ok(handle)
}
```

Use the existing readiness helper but rename it so it no longer says `for_test`.

- [ ] **Step 2: Replace simple ref constructors**

Rename wait ref constructors:

```rust
pub fn account(&self, account_id: &str) -> AccountRef;
pub fn position(&self, account_id: &str, symbol: &str) -> PositionRef;
pub fn order(&self, account_id: &str, order_id: &str) -> OrderRef;
pub fn pre_insert_order(&self, account_id: &str, order_id: &str) -> PreInsertOrderRef;
pub fn trade(&self, account_id: &str, trade_id: &str) -> TradeRef;
pub fn notification(&self, notification_id: &str) -> NotificationRef;
pub fn security_account(&self, account_id: &str) -> SecurityAccountRef;
pub fn security_position(&self, account_id: &str, symbol: &str) -> SecurityPositionRef;
pub fn security_order(&self, account_id: &str, order_id: &str) -> SecurityOrderRef;
pub fn security_trade(&self, account_id: &str, trade_id: &str) -> SecurityTradeRef;
```

Delete the old `get_*` versions in the same patch.

- [ ] **Step 3: Run deletion and behavior tests**

Run:

```bash
cargo test -p tqsdk-wait wait_api_removes_legacy_get_and_api_bound_ref_surface
cargo test -p tqsdk-wait quote_handle_reads_snapshot_without_api_argument_after_step
cargo test -p tqsdk-wait kline_handle_reads_bounded_window_without_api_argument_after_step
cargo test -p tqsdk-wait tick_handle_reads_bounded_window_without_api_argument_after_step
```

Expected: PASS.

- [ ] **Step 4: Commit breaking wait API**

```bash
git add crates/tqsdk-wait/src crates/tqsdk-wait/tests
git commit -m "refactor(wait): replace legacy get api with step handles"
```

## Task 5: Update `tqsdk-task` to the New Wait API

**Files:**
- Modify: `crates/tqsdk-task/src/strategy.rs`
- Modify: `crates/tqsdk-task/src/target_pos/machine.rs`
- Modify: `crates/tqsdk-task/examples/api_contract_s11_simple_strategy.rs`
- Modify: `crates/tqsdk-task/examples/api_contract_s12_spread_arbitrage.rs`
- Modify: `crates/tqsdk-task/examples/api_contract_s13_multi_account_ordering.rs`
- Modify: `crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs`

- [ ] **Step 1: Replace market handle calls in task code**

Apply these mechanical rewrites:

```text
api.get_quote(symbol).await?        -> api.quote(symbol).await?
api.get_kline_serial(s, d, n).await? -> api.kline(s, d, n).await?
api.get_tick_serial(s, n).await?     -> api.tick(s, n).await?
quote.load(&api)?                   -> quote.load()?
api.quote_snapshot(symbol, deadline).await? -> api.quote(symbol).await? plus step loop until quote.load() succeeds
```

For strategy context methods, use self-reading handles rather than storing an API borrow inside the context.

- [ ] **Step 2: Update task tests for new API**

Where task tests currently inspect wait state via `load(&api)`, change them to:

```rust
let step = host.api_mut().step().await.unwrap().expect("expected update");
assert!(step.is_changing(&quote));
let snapshot = quote.load().unwrap();
```

If the test uses `TaskHost::wait_update`, keep `TaskHost::wait_update` for task-level scheduling, but update the underlying wait facade calls inside `TaskHost`.

- [ ] **Step 3: Run task tests**

Run:

```bash
cargo test -p tqsdk-task
```

Expected: PASS.

- [ ] **Step 4: Commit task migration**

```bash
git add crates/tqsdk-task/src crates/tqsdk-task/examples crates/tqsdk-task/tests
git commit -m "refactor(task): adopt wait step handle api"
```

## Task 6: Add Breaking Backtest Facade Surface

**Files:**
- Create: `crates/tqsdk-wait/src/backtest.rs`
- Modify: `crates/tqsdk-wait/src/lib.rs`
- Modify: `crates/tqsdk-wait/src/builder.rs`
- Modify: `crates/tqsdk-wait/src/api.rs`
- Modify: `crates/tqsdk-wait/tests/wait_api_surface.rs`

- [ ] **Step 1: Add `TqBacktest` config**

Create `crates/tqsdk-wait/src/backtest.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktestMarketKind {
    Futures,
    Stock,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TqBacktest {
    start_datetime_ns: i64,
    end_datetime_ns: i64,
    market_kind: BacktestMarketKind,
}

impl TqBacktest {
    pub fn new(start_datetime_ns: i64, end_datetime_ns: i64) -> crate::error::Result<Self> {
        Self::futures(start_datetime_ns, end_datetime_ns)
    }

    pub fn futures(start_datetime_ns: i64, end_datetime_ns: i64) -> crate::error::Result<Self> {
        Self::with_market_kind(start_datetime_ns, end_datetime_ns, BacktestMarketKind::Futures)
    }

    pub fn stock(start_datetime_ns: i64, end_datetime_ns: i64) -> crate::error::Result<Self> {
        Self::with_market_kind(start_datetime_ns, end_datetime_ns, BacktestMarketKind::Stock)
    }

    fn with_market_kind(
        start_datetime_ns: i64,
        end_datetime_ns: i64,
        market_kind: BacktestMarketKind,
    ) -> crate::error::Result<Self> {
        if start_datetime_ns >= end_datetime_ns {
            return Err(crate::error::WaitFacadeError::InvalidState(
                "backtest start_datetime_ns must be less than end_datetime_ns",
            ));
        }
        Ok(Self {
            start_datetime_ns,
            end_datetime_ns,
            market_kind,
        })
    }

    pub fn start_datetime_ns(&self) -> i64 {
        self.start_datetime_ns
    }

    pub fn end_datetime_ns(&self) -> i64 {
        self.end_datetime_ns
    }

    pub fn market_kind(&self) -> BacktestMarketKind {
        self.market_kind
    }
}
```

- [ ] **Step 2: Export `TqBacktest`**

Modify `crates/tqsdk-wait/src/lib.rs`:

```rust
mod backtest;

pub use backtest::{BacktestMarketKind, TqBacktest};
```

- [ ] **Step 3: Wire builder methods**

Change `TqApiBuilder` from a one-field wrapper to:

```rust
#[derive(Debug, Clone)]
pub struct TqApiBuilder {
    inner: tqsdk_session::SessionClientBuilder,
    backtest: Option<TqBacktest>,
}
```

Add methods:

```rust
pub fn backtest(mut self, backtest: TqBacktest) -> Self {
    self.inner = match backtest.market_kind() {
        BacktestMarketKind::Futures => self.inner.futures_backtest_market(),
        BacktestMarketKind::Stock => self.inner.stock_backtest_market(),
    };
    self.backtest = Some(backtest);
    self
}

pub fn futures_backtest(mut self, start_datetime_ns: i64, end_datetime_ns: i64) -> crate::error::Result<Self> {
    Ok(self.backtest(TqBacktest::futures(start_datetime_ns, end_datetime_ns)?))
}

pub fn stock_backtest(mut self, start_datetime_ns: i64, end_datetime_ns: i64) -> crate::error::Result<Self> {
    Ok(self.backtest(TqBacktest::stock(start_datetime_ns, end_datetime_ns)?))
}
```

In `build`, pass the optional backtest config into `TqApi::new_with_options`.

- [ ] **Step 4: Add builder tests**

Add to `crates/tqsdk-wait/src/builder.rs` tests:

```rust
#[test]
fn backtest_builder_sets_backtest_market_target_and_config() {
    let backtest = TqBacktest::futures(1_000, 2_000).unwrap();
    let builder = TqApiBuilder::new("demo-user", "demo-pass").backtest(backtest.clone());

    assert_eq!(
        builder.inner.market_target_ref(),
        &tqsdk_core::MarketSessionTarget::futures_backtest()
    );
    assert_eq!(builder.backtest, Some(backtest));
}
```

- [ ] **Step 5: Run builder tests**

Run:

```bash
cargo test -p tqsdk-wait backtest_builder_sets_backtest_market_target_and_config
```

Expected: PASS.

- [ ] **Step 6: Commit backtest facade config**

```bash
git add crates/tqsdk-wait/src/backtest.rs crates/tqsdk-wait/src/lib.rs crates/tqsdk-wait/src/builder.rs crates/tqsdk-wait/src/api.rs
git commit -m "feat(wait): add breaking backtest facade config"
```

## Task 7: Implement Backtest Step Completion Semantics

**Files:**
- Modify: `crates/tqsdk-wait/src/api.rs`
- Modify: `crates/tqsdk-wait/src/driver.rs`
- Modify: `crates/tqsdk-wait/src/step.rs`
- Modify: `crates/tqsdk-wait/tests/wait_api_market.rs`

- [ ] **Step 1: Extend `WaitDriver`**

Add:

```rust
pub(crate) backtest: Option<TqBacktest>,
pub(crate) backtest_finished: bool,
```

Initialize these from `TqApiBuilder::build`.

- [ ] **Step 2: Add backtest finish detection**

In `TqApi::step_until`, after creating a `WaitStep`, detect current time:

```rust
if let Some(backtest) = &self.driver.backtest {
    if let Some(current_dt) = step.current_dt()
        && current_dt >= backtest.end_datetime_ns()
    {
        self.driver.backtest_finished = true;
    }
}
```

At the beginning of `step_until`, return `Ok(None)` if `backtest_finished` is true.

- [ ] **Step 3: Add deterministic test using injected commits**

Add to `crates/tqsdk-wait/tests/wait_api_market.rs`:

```rust
#[tokio::test]
async fn backtest_step_returns_none_after_end_datetime() {
    let mut api = backtest_api_for_test(1_000, 2_000);
    api.testing()
        .push_market_json(serde_json::json!({
            "_tqsdk_backtest": {
                "start_dt": 1_000,
                "current_dt": 2_000,
                "end_dt": 2_000
            }
        }))
        .unwrap();

    let first = api.step().await.unwrap();
    assert!(first.is_some());

    let second = api.step().await.unwrap();
    assert!(second.is_none());
}
```

Use the existing `testing` helper style in `crates/tqsdk-wait/src/testing.rs`. If no helper accepts raw market JSON, add one that calls the runtime handle ingestion path; do not bypass the runtime state store.

- [ ] **Step 4: Run the backtest test**

Run:

```bash
cargo test -p tqsdk-wait backtest_step_returns_none_after_end_datetime
```

Expected: PASS.

- [ ] **Step 5: Commit backtest step completion**

```bash
git add crates/tqsdk-wait/src crates/tqsdk-wait/tests
git commit -m "feat(wait): end backtest via step none"
```

## Task 8: Update Wait Contract Examples

**Files:**
- Modify: `crates/tqsdk-wait/examples/api_contract_s01_zero_barrier_quote.rs`
- Modify: `crates/tqsdk-wait/examples/api_contract_s03_quote_snapshot.rs`
- Modify: `crates/tqsdk-wait/examples/api_contract_s06_limit_order.rs`
- Modify: `crates/tqsdk-wait/examples/api_contract_s08_account_position_updates.rs`
- Modify: `crates/tqsdk-wait/examples/api_contract_s09_startup_state_recovery.rs`
- Modify: `crates/tqsdk-wait/examples/api_contract_s25_wait_serial_trading_status.rs`
- Modify: `crates/tqsdk-wait/examples/api_contract_s26_security_trade_refs.rs`
- Modify: `crates/tqsdk-wait/examples/api_contract_s26_trade_system_refs.rs`
- Create: `crates/tqsdk-wait/examples/api_contract_s32_wait_live_backtest_same_body.rs`

- [ ] **Step 1: Rewrite S01 quote example**

Use this main loop shape:

```rust
let mut api = TqApiBuilder::new(user, pass).futures_market().build().await?;
let quote = api.quote(&symbol).await?;
let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

while let Some(step) = api.step_until(Some(deadline)).await? {
    if step.is_changing(&quote) {
        let snapshot = quote.load()?;
        println!(
            "quote symbol={} datetime={} last_price={}",
            symbol, snapshot.datetime, snapshot.last_price
        );
        break;
    }
}
```

- [ ] **Step 2: Replace S03 `quote_snapshot`**

Delete usage of `quote_snapshot`. The example should demonstrate a helper function local to the example:

```rust
async fn wait_quote_ready(
    api: &mut tqsdk_wait::TqApi,
    quote: &tqsdk_wait::QuoteRef,
    deadline: tokio::time::Instant,
) -> Result<tqsdk_core::Quote, Box<dyn std::error::Error>> {
    while let Some(step) = api.step_until(Some(deadline)).await? {
        if step.is_changing(quote)
            && let Ok(snapshot) = quote.load()
            && !snapshot.datetime.is_empty()
        {
            return Ok(snapshot);
        }
    }
    Err("quote snapshot not ready".into())
}
```

- [ ] **Step 3: Rewrite S25 serial/status example**

Use:

```rust
let trading_status = api.trading_status(&symbol).await?;
let kline = api.kline(&symbol, Duration::from_secs(kline_seconds), serial_length).await?;
let tick = api.tick(&symbol, serial_length).await?;

while let Some(step) = api.step_until(Some(deadline)).await? {
    if step.is_changing_fields(&trading_status, &["trade_status"]) {
        let status = trading_status.load()?;
        println!("trade_status={}", status.trade_status);
    }

    if step.is_changing_fields(&kline, &["close"]) {
        let window = kline.window()?;
        println!("completed={}", window.completed_rows().len());
    }

    if step.is_changing_fields(&tick, &["last_price"]) {
        let window = tick.window()?;
        println!("ticks={}", window.len());
    }
}
```

- [ ] **Step 4: Add live/backtest same-body contract**

Create `crates/tqsdk-wait/examples/api_contract_s32_wait_live_backtest_same_body.rs` with two builder functions and one shared strategy function:

```rust
async fn run_strategy(mut api: tqsdk_wait::TqApi, symbol: &str) -> Result<(), Box<dyn std::error::Error>> {
    let quote = api.quote(symbol).await?;
    let bars = api.kline(symbol, Duration::from_secs(60), 32).await?;

    while let Some(step) = api.step().await? {
        if step.is_changing(&quote) {
            let snapshot = quote.load()?;
            println!("last_price={}", snapshot.last_price);
        }
        if step.is_changing(&bars) {
            let window = bars.window()?;
            println!("bars={}", window.len());
        }
    }
    Ok(())
}
```

The live and backtest builders must be separate functions; the strategy body must not branch on live/backtest mode.

- [ ] **Step 5: Run example checks**

Run:

```bash
cargo check -p tqsdk-wait --examples
```

Expected: PASS.

- [ ] **Step 6: Commit examples**

```bash
git add crates/tqsdk-wait/examples
git commit -m "docs(wait): rewrite contracts for step handles"
```

## Task 9: Update Docs and Skill References

**Files:**
- Modify: `crates/tqsdk-wait/README.md`
- Modify: `docs/architecture/api-wait.md`
- Modify: `docs/architecture/crate-boundaries.md`
- Modify: `docs/architecture/facade-paradigms.md`
- Modify: `docs/architecture/README.md`
- Modify: `docs/reviews/public-api-scenario-review.md`
- Modify: `skills/tqsdk-rust/SKILL.md`
- Modify: `skills/tqsdk-rust/references/scenario-router.md`
- Modify: `skills/tqsdk-rust/references/code-patterns.md`
- Modify: `skills/tqsdk-rust/references/quant-workflows.md`
- Modify: `skills/tqsdk-rust/references/scenario-contracts.md`
- Modify: `skills/tqsdk-rust/assets/templates/wait-quote-loop/src/main.rs`

- [ ] **Step 1: Replace wait routing language**

Use this wording in skill and architecture docs:

```text
Single-owner strategy loops use `tqsdk-wait`: construct `TqApi`, create handles with
`quote`, `kline`, `tick`, or `trading_status`, then drive state with `step()` /
`step_until(...)`. `WaitStep` owns the latest commit boundary and answers
`is_changing` / `is_changing_fields`; handles read snapshots without taking `&TqApi`.
```

- [ ] **Step 2: Replace starter template**

Use this body in `skills/tqsdk-rust/assets/templates/wait-quote-loop/src/main.rs`:

```rust
use std::time::Duration;

use tqsdk_wait::TqApiBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;

    let mut api = TqApiBuilder::new(user, pass).futures_market().build().await?;
    let quote = api.quote("{{SYMBOL}}").await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    while let Some(step) = api.step_until(Some(deadline)).await? {
        if step.is_changing(&quote) {
            let snapshot = quote.load()?;
            println!("{} {}", snapshot.datetime, snapshot.last_price);
        }
    }

    Ok(())
}
```

- [ ] **Step 3: Run documentation checks**

Run:

```bash
git diff --check
```

Expected: PASS.

- [ ] **Step 4: Commit docs and skill updates**

```bash
git add crates/tqsdk-wait/README.md docs skills
git commit -m "docs: document breaking wait step api"
```

## Task 10: Remove Remaining Legacy References

**Files:**
- All workspace files from `rg`.

- [ ] **Step 1: Search for old API names**

Run:

```bash
rg "get_quote|get_kline_serial|get_tick_serial|quote_snapshot|is_serial_ready|is_changing\\(|is_changing_fields\\(|load\\(&api\\)|snapshot\\(&api\\)" crates docs skills
```

Expected: no matches outside archived docs under `docs/archive/`.

- [ ] **Step 2: Fix non-archive matches**

For each non-archive match:

```text
get_quote(...)              -> quote(...)
get_kline_serial(...)       -> kline(...)
get_tick_serial(...)        -> tick(...)
quote_snapshot(...)         -> quote(...) + step loop
api.is_changing(&handle)?   -> step.is_changing(&handle)
api.is_changing_fields(...) -> step.is_changing_fields(...)
handle.load(&api)?          -> handle.load()?
handle.snapshot(&api)?      -> handle.snapshot()?
```

- [ ] **Step 3: Run full workspace check**

Run:

```bash
cargo check --workspace --examples
```

Expected: PASS.

- [ ] **Step 4: Commit cleanup**

```bash
git add crates docs skills
git commit -m "refactor: remove legacy wait api references"
```

## Task 11: Full Verification

**Files:**
- No source edits unless verification fails.

- [ ] **Step 1: Format check**

Run:

```bash
cargo fmt --all --check
```

Expected: PASS.

- [ ] **Step 2: Wait tests**

Run:

```bash
cargo test -p tqsdk-wait
```

Expected: PASS.

- [ ] **Step 3: Task tests**

Run:

```bash
cargo test -p tqsdk-task
```

Expected: PASS.

- [ ] **Step 4: Workspace examples**

Run:

```bash
cargo check --workspace --examples
```

Expected: PASS.

- [ ] **Step 5: Clippy**

Run:

```bash
cargo clippy --workspace --examples --all-targets -- -D warnings
```

Expected: PASS.

- [ ] **Step 6: Diff whitespace**

Run:

```bash
git diff --check
```

Expected: PASS.

- [ ] **Step 7: GitNexus change detection**

Run the configured code graph change detector if available:

```text
mcp__code_review_graph__detect_changes_tool(base="HEAD", detail_level="minimal")
```

Expected: risk is reviewed and any HIGH/CRITICAL warning is explicitly acknowledged before final commit.

- [ ] **Step 8: Final commit if verification required fixes**

```bash
git add crates docs skills
git commit -m "test: verify breaking wait api migration"
```

Skip this commit if no files changed during verification.

## Acceptance Checklist

- [ ] No non-archive source, docs, skill, or examples mention `TqApi::get_quote`.
- [ ] No non-archive source, docs, skill, or examples mention `TqApi::get_kline_serial`.
- [ ] No non-archive source, docs, skill, or examples mention `TqApi::get_tick_serial`.
- [ ] No non-archive source, docs, skill, or examples mention `TqApi::quote_snapshot`.
- [ ] No wait ref public method takes `&TqApi`.
- [ ] Strategy examples use `api.step()` or `api.step_until(...)`.
- [ ] Change checks are performed through `WaitStep`.
- [ ] Handles read from `RuntimeReader`, not a private state tree.
- [ ] Live/backtest same-body contract example compiles.
- [ ] `tqsdk-data` remains offline/history/replay foundation and is not used as a live serial cache.
- [ ] `tqsdk-stream` remains multi-consumer event stream and does not gain mmap cache behavior.
