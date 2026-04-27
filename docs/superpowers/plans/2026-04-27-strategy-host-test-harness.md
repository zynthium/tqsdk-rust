# Strategy Host and Test Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the next scenario-driven public API batch for S11/S24 by adding a minimal `tqsdk-task` strategy host and public fake test harness, while preparing S15/S16 without freezing a broad live/sim/replay abstraction too early.

**Architecture:** Keep strategy execution in `tqsdk-task` because it composes live state, task ownership, typed orders, target-pos tasks, risk checks, and execution reports. Do not add strategy, fake broker, or replay-driver semantics to `tqsdk-core`, `tqsdk-session`, `tqsdk-wait`, or `tqsdk-stream`. The host must keep the existing single runtime commit/revision model by driving `TaskHost::wait_update()` and reading through wait/task public surfaces; the public fake harness may use internal test handles inside the crate but must not expose runtime handles, provider protocol, channels, or `Arc<Mutex<_>>` to users.

**Tech Stack:** Rust 2024, Tokio tests, existing `tqsdk-task`, `tqsdk-wait`, `tqsdk-session`, `tqsdk-core` runtime test substrate, scenario contract examples.

---

## Batch Scope

This batch should promote:

- S11 简单策略: from “勉强” to “自然” if the final formal example can express quote-triggered entry, position-aware exit, typed orders, and risk access through one context.
- S24 最小可测试策略: from “无法表达” to “勉强” if users can run a strategy against public fake market/broker APIs without hidden `*_for_test` calls.

This batch should only narrow, not close:

- S15 实盘 / 模拟 / 回放切换: the same strategy loop should work against live `StrategyHost` and the fake harness, but full live/sim/replay provider switching remains a later batch.
- S16 历史行情回放: no full history replay driver in this batch; design names must leave room for `tqsdk-data` backed replay later.

Non-goals:

- No new crate in this batch.
- No `async_trait` dependency or boxed user strategy trait.
- No background Tokio task, channel, or user-visible shared mutable state.
- No historical cache/replay file format.
- No provider aggregation.
- No cross-process order intent persistence.

## File Structure

- Create `crates/tqsdk-task/src/strategy.rs`
  - Owns `StrategyHostBuilder`, `StrategyHost`, `StrategyContext`, `StrategyUpdate`, watched quote/account registration, and convenience accessors.
  - Wraps `TaskHost` instead of duplicating wait/task behavior.
- Create `crates/tqsdk-task/src/testing.rs`
  - Owns public `StrategyTestHarness`, `FakeMarket`, `FakeBroker`, fake order policies, and `StrategyTestReport`.
  - Internally may seed runtime state and consume test dispatches, but the public API must expose only domain concepts.
- Modify `crates/tqsdk-task/src/host.rs`
  - Adds `TaskHost::strategy()` returning `StrategyHostBuilder`.
  - Adds only the minimal helper hooks needed by strategy/testing modules.
- Modify `crates/tqsdk-task/src/lib.rs`
  - Re-exports strategy and testing public types.
- Modify `crates/tqsdk-task/src/error.rs`
  - Adds typed strategy/test harness errors if existing `TaskError::InvalidState` is too vague.
- Create `crates/tqsdk-task/tests/strategy_host.rs`
  - Covers context creation, quote/position access, order submission through `TaskHost::orders`, and target-pos/risk integration.
- Create `crates/tqsdk-task/tests/strategy_testing.rs`
  - Covers fake quote seeding, fake order fill/reject/partial fill, report collection, and absence of hidden user APIs.
- Modify `crates/tqsdk-task/examples/api_contract_s11_simple_strategy.rs`
  - Replace the gap-style sketch with the new formal `StrategyHost` contract if it compiles naturally.
- Create `crates/tqsdk-task/examples/api_contract_s24_testable_strategy.rs`
  - Formal public API contract example for the minimal test harness.
- Modify `docs/scenarios/api_gaps/api_contract_s24_testable_strategy.rs`
  - Narrow the remaining gap after the fake harness exists.
- Modify `docs/scenarios/api_gaps/api_contract_s15_live_sim_replay_switch.rs`
  - Note that `StrategyHost` establishes the common loop shape, while full environment switching remains open.
- Modify `docs/scenarios/api_gaps/api_contract_s16_history_replay_strategy.rs`
  - Note that history replay should later implement the same strategy step/context shape.
- Modify `docs/public-api-scenario-review.md`
  - Update S11/S24 statuses only after examples compile and tests pass.
- Modify `docs/scenarios/user-layer-iteration-plan.md`
  - Mark this batch as completed only after implementation and verification.
- Modify `docs/architecture/api-task.md`, `docs/architecture/ai-workflow.md`, `docs/architecture/README.md`, root `README.md`, and `crates/tqsdk-task/README.md`
  - Document strategy host/testing as task-layer user tooling if the public API lands.

## Public API Shape

The intended S11 user-facing shape:

```rust
use std::time::Duration;

use tqsdk_task::{RiskEngine, StrategyHost, TaskHost};

# async fn run(host: TaskHost) -> tqsdk_task::Result<()> {
let mut strategy = StrategyHost::builder(host)
    .account("sim")
    .quote("SHFE.au2602")
    .build()
    .await?;

while let Some(mut ctx) = strategy.next(None).await? {
    let quote = ctx.quote("SHFE.au2602")?;
    let position = ctx.position("sim", "SHFE.au2602")?;

    if quote.last_price > 480.0 && position.pos_long == 0 {
        ctx.orders("sim")
            .buy_open("SHFE.au2602", 1)
            .limit(quote.last_price)
            .send_once("breakout-entry-1")
            .await?;
    }

    if position.pos_long > 0 && quote.last_price < 470.0 {
        ctx.target_pos("sim", "SHFE.au2602")
            .build()?
            .set_target_volume(0);
    }
}
# Ok(())
# }
```

The intended S24 user-facing shape:

```rust
use tqsdk_task::testing::{FakeBroker, FakeMarket, StrategyTestHarness};
use tqsdk_task::StrategyHost;

#[tokio::test]
async fn strategy_buys_when_breakout() -> tqsdk_task::Result<()> {
    let harness = StrategyTestHarness::new()
        .market(FakeMarket::new().quote("SHFE.au2602", 481.0))
        .broker(FakeBroker::new().fill_all())
        .build()?;

    let mut strategy = StrategyHost::builder(harness.into_task_host())
        .account("sim")
        .quote("SHFE.au2602")
        .build()
        .await?;

    let mut ctx = strategy.next_once().await?;
    ctx.orders("sim")
        .buy_open("SHFE.au2602", 1)
        .limit(481.0)
        .send_once("entry-1")
        .await?;

    let report = ctx.finish_test_step().await?;
    assert_eq!(report.orders().len(), 1);
    assert_eq!(report.position("sim", "SHFE.au2602")?.pos_long, 1);
    Ok(())
}
```

During implementation, if borrow-checker pressure shows `finish_test_step()` should live on the harness rather than context, keep the public user shape equally explicit and do not expose runtime handles.

## Task 1: Strategy Host Contract Tests

**Files:**
- Create: `crates/tqsdk-task/tests/strategy_host.rs`
- Modify later: `crates/tqsdk-task/src/strategy.rs`, `crates/tqsdk-task/src/lib.rs`, `crates/tqsdk-task/src/host.rs`

- [ ] **Step 1: Add compile-failing tests for the intended host/context surface**

Cover these behaviors:

- `StrategyHost::builder(host).account(...).quote(...).build().await` creates a strategy host.
- `next_once().await` returns a context over one stable task/wait update boundary.
- `ctx.quote(symbol)` returns typed `tqsdk_core::Quote`.
- `ctx.position(account, symbol)` returns typed `tqsdk_core::Position`.
- `ctx.orders(account)` delegates to existing typed task order builder.
- `ctx.target_pos(account, symbol)` delegates to existing target-pos builder.

- [ ] **Step 2: Run the focused test and verify it fails at compile time**

Run:

```bash
cargo test -p tqsdk-task --test strategy_host -- --nocapture
```

Expected: compile failure because strategy types do not exist.

## Task 2: Minimal Strategy Host Implementation

**Files:**
- Create: `crates/tqsdk-task/src/strategy.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`
- Modify: `crates/tqsdk-task/src/host.rs`
- Test: `crates/tqsdk-task/tests/strategy_host.rs`

- [ ] **Step 1: Add strategy module and public exports**

Define these public types:

- `StrategyHostBuilder`
- `StrategyHost`
- `StrategyContext<'a>`
- `StrategyUpdate`

Do not define a public async strategy trait in this task.

- [ ] **Step 2: Implement builder registration**

`StrategyHostBuilder` should accept:

- `account(account_id)`
- `quote(symbol)`
- `build().await`

The build step should subscribe to requested quotes through the underlying wait facade and preserve account ids for context access. It should not login trade accounts implicitly unless the existing `TaskHost`/`TqApi` public API already has enough information to do so safely.

- [ ] **Step 3: Implement stable update stepping**

`StrategyHost::next(deadline)` should:

- call `TaskHost::wait_update(deadline).await`
- return `Ok(None)` only when the underlying facade is closed or a future explicit close API exists
- otherwise return `Ok(Some(StrategyContext))`

`StrategyHost::next_once()` should be a test-friendly/public convenience that advances one step without a deadline.

- [ ] **Step 4: Implement context accessors**

`StrategyContext` should expose:

- `quote(symbol) -> Result<tqsdk_core::Quote>`
- `position(account_id, symbol) -> Result<tqsdk_core::Position>`
- `account(account_id) -> Result<tqsdk_core::Account>`
- `orders(account_id) -> TaskOrderBuilder<'_>`
- `target_pos(account_id, symbol) -> TargetPosBuilder`
- `risk() -> Option<&RiskEngine>`
- `task_host()` / `task_host_mut()` only if needed, and only if it does not make examples degenerate into manual host plumbing.

- [ ] **Step 5: Verify focused strategy host tests**

Run:

```bash
cargo test -p tqsdk-task --test strategy_host -- --nocapture
```

Expected: all strategy host tests pass.

## Task 3: Public Fake Market/Broker Test Harness

**Files:**
- Create: `crates/tqsdk-task/src/testing.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`
- Modify if needed: `crates/tqsdk-task/src/error.rs`
- Test: `crates/tqsdk-task/tests/strategy_testing.rs`

- [ ] **Step 1: Add compile-failing harness tests**

Cover these behaviors:

- `StrategyTestHarness::new().market(FakeMarket::new().quote(...)).broker(FakeBroker::new().fill_all()).build()?`
- `harness.into_task_host()` returns a public `TaskHost` ready for `StrategyHost`.
- Fake market quotes are readable through `StrategyContext::quote`.
- Fake broker can materialize at least:
  - all-filled order
  - rejected order
  - partial fill with remaining volume
- Public test code never calls `new_for_test`, `handle_for_test`, `RuntimeHandle`, `RuntimeInput`, `serde_json::Value`, channel APIs, or `Arc<Mutex<_>>`.

- [ ] **Step 2: Run focused harness tests and verify failure**

Run:

```bash
cargo test -p tqsdk-task --test strategy_testing -- --nocapture
```

Expected: compile failure because harness types do not exist.

- [ ] **Step 3: Implement minimal harness**

Define public types:

- `testing::StrategyTestHarness`
- `testing::StrategyTestHarnessBuilder`
- `testing::FakeMarket`
- `testing::FakeBroker`
- `testing::FakeBrokerPolicy`
- `testing::StrategyTestReport`

Implementation constraints:

- Internally build a `TaskHost` using existing runtime/session/wait test substrate.
- Hide all internal handles.
- Seed quote/account/position state through domain-level helper methods.
- Translate submitted task/wait order dispatches into fake order/trade/account/position updates.
- Keep fake behavior deterministic and single-threaded.

- [ ] **Step 4: Verify focused harness tests**

Run:

```bash
cargo test -p tqsdk-task --test strategy_testing -- --nocapture
```

Expected: all harness tests pass.

## Task 4: Scenario Examples and Gap Updates

**Files:**
- Modify: `crates/tqsdk-task/examples/api_contract_s11_simple_strategy.rs`
- Create: `crates/tqsdk-task/examples/api_contract_s24_testable_strategy.rs`
- Modify: `docs/scenarios/api_gaps/api_contract_s24_testable_strategy.rs`
- Modify: `docs/scenarios/api_gaps/api_contract_s15_live_sim_replay_switch.rs`
- Modify: `docs/scenarios/api_gaps/api_contract_s16_history_replay_strategy.rs`

- [ ] **Step 1: Promote S11 example to the new strategy host**

The example must keep the required scenario header and must not contain:

- `RuntimeCommand`
- provider protocol/session internals
- user-created Tokio tasks/channels
- `Arc<Mutex<_>>`
- manual local position cache as funding truth

- [ ] **Step 2: Add formal S24 example**

The example must demonstrate a minimal test that:

- seeds fake market data
- configures fake broker behavior
- runs the same strategy host/context path a live user would use
- asserts order and final position through public report/state APIs

- [ ] **Step 3: Narrow S15/S16 gap sketches**

Do not promote S15/S16 yet. Update them to say:

- `StrategyHost` now provides the common strategy loop/context shape.
- full live/sim/replay environment switching remains open.
- history replay should later implement this same step/context contract, likely with `tqsdk-data` as the history source.

- [ ] **Step 4: Check examples**

Run:

```bash
cargo check -p tqsdk-task --examples
```

Expected: S11/S24 examples compile.

## Task 5: Review Matrix and Architecture Docs

**Files:**
- Modify: `docs/public-api-scenario-review.md`
- Modify: `docs/scenarios/user-layer-iteration-plan.md`
- Modify: `docs/architecture/api-task.md`
- Modify: `docs/architecture/ai-workflow.md`
- Modify: `docs/architecture/README.md`
- Modify: `README.md`
- Modify: `crates/tqsdk-task/README.md`

- [ ] **Step 1: Update scenario review statuses**

If examples compile naturally:

- S11: `自然`, boilerplate `低/中`, internal leak `无`, manual async `无`, consistency risk `低/中`.
- S24: `勉强`, boilerplate `低/中`, internal leak `无`, manual async `无`, consistency risk `低`.

Keep S15/S16 as gaps unless their formal examples compile without replay/provider-specific branches.

- [ ] **Step 2: Update iteration plan**

Mark this batch as landed and list remaining gaps:

- full environment switch
- history replay driver
- cache replay
- fake reconnect/latency scenarios
- cross-process persistence

- [ ] **Step 3: Update architecture docs**

Document that:

- strategy host/testing belong to `tqsdk-task`
- fake/test harness is user-level test support, not core runtime API
- replay/history remains data/session substrate until a dedicated strategy replay driver lands

- [ ] **Step 4: Verify doc references**

Run:

```bash
rg -n "StrategyHost|StrategyTestHarness|api_contract_s24|S24|最小可测试策略" docs crates/tqsdk-task
```

Expected: references are consistent and no status claims contradict examples.

## Task 6: Full Verification and Commit

**Files:**
- All files touched above.

- [ ] **Step 1: Run required checks**

Run:

```bash
cargo check --workspace --examples
cargo test --workspace
cargo clippy --workspace --examples --all-targets -- -D warnings
```

Expected: all pass.

- [ ] **Step 2: Run feature checks only if feature flags changed**

If this batch modifies feature flags or optional dependencies, also run:

```bash
cargo check --workspace --no-default-features
cargo check --workspace --all-features --examples
```

Expected: all pass.

- [ ] **Step 3: Commit in small logical commits**

Recommended commit split:

```bash
git add crates/tqsdk-task/src/strategy.rs crates/tqsdk-task/src/lib.rs crates/tqsdk-task/src/host.rs crates/tqsdk-task/tests/strategy_host.rs
git commit -m "feat: add task strategy host"

git add crates/tqsdk-task/src/testing.rs crates/tqsdk-task/src/error.rs crates/tqsdk-task/tests/strategy_testing.rs
git commit -m "feat: add public strategy test harness"

git add crates/tqsdk-task/examples/api_contract_s11_simple_strategy.rs crates/tqsdk-task/examples/api_contract_s24_testable_strategy.rs docs/scenarios/api_gaps/api_contract_s15_live_sim_replay_switch.rs docs/scenarios/api_gaps/api_contract_s16_history_replay_strategy.rs docs/scenarios/api_gaps/api_contract_s24_testable_strategy.rs docs/public-api-scenario-review.md docs/scenarios/user-layer-iteration-plan.md docs/architecture/api-task.md docs/architecture/ai-workflow.md docs/architecture/README.md README.md crates/tqsdk-task/README.md
git commit -m "docs: promote strategy host testing scenarios"
```

Do not commit unrelated untracked files.

## Acceptance Criteria

- S11 formal example uses `StrategyHost` and no longer needs a gap-style sketch.
- S24 has a formal compiling example using public fake market/broker APIs.
- User strategy code does not reference provider protocol, runtime command, hidden test constructors, channels, or `Arc<Mutex<_>>`.
- Strategy/test APIs stay in `tqsdk-task`.
- `tqsdk-core` public surface does not grow.
- `tqsdk-session` remains one-shot request/response and replay control substrate.
- `tqsdk-wait` remains continuous consumption facade and does not absorb strategy/test tooling.
- `cargo check --workspace --examples`, `cargo test --workspace`, and `cargo clippy --workspace --examples --all-targets -- -D warnings` pass.

## Deferred Follow-Up Batches

1. S15 environment switch: `StrategyEnvironment` / live-sim-replay adapter layer after `StrategyHost` is stable.
2. S16 history replay: `HistoryReplay` driver backed by `tqsdk-data` series and emitting the same strategy context/update shape.
3. S18 local cache: cache writer/reader/replay driver after history replay event shape is frozen.
4. Advanced S24 testing: fake reconnect, latency, partial fills over multiple steps, and deterministic clock.
