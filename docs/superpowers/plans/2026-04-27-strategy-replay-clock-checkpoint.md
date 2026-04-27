# Strategy Replay Clock Checkpoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a deterministic replay clock and resumable checkpoint foundation to `tqsdk-task::StrategyReplay`.

**Architecture:** Keep this in `tqsdk-task`, because it is strategy replay runtime metadata, not data storage. `MarketCacheReplay` remains the ordered event source; `StrategyReplay` tracks the next event index and last replay event time. This does not add real-time sleep/speed control, persistent checkpoint storage, or a live/sim/replay environment adapter.

**Tech Stack:** Rust 2024, `tqsdk-task`, `tqsdk-data::MarketCacheReplay`, Tokio tests, existing scenario contract docs.

---

## File Structure

- Modify `crates/tqsdk-task/src/replay.rs`
  - Add `StrategyReplayCheckpoint`.
  - Add builder `resume_from(...)`.
  - Track `next_event_index` and `replay_time_ns` inside `StrategyReplay`.
  - Expose replay time/checkpoint from both `StrategyReplay` and `StrategyReplayContext`.
- Modify `crates/tqsdk-task/src/lib.rs`
  - Re-export `StrategyReplayCheckpoint`.
- Modify `crates/tqsdk-task/tests/strategy_replay.rs`
  - Add TDD coverage for replay clock/checkpoint and resume.
- Modify `crates/tqsdk-task/examples/api_contract_s16_history_replay_strategy.rs`
  - Print replay time/checkpoint to lock the public contract.
- Modify scenario/architecture docs
  - Update S16 from “clock/checkpoint gap” to “clock/checkpoint foundation landed”.

## Task 1: TDD Replay Clock And Checkpoint

- [ ] **Step 1: Write failing tests**

Add tests:

```rust
#[tokio::test(flavor = "current_thread")]
async fn strategy_replay_exposes_replay_clock_and_checkpoint() {
    let replay = two_kline_replay();
    let mut strategy = replay_strategy(replay).await;

    let ctx = strategy.next().await.unwrap().unwrap();
    assert_eq!(ctx.replay_time_ns(), 1_000);
    assert_eq!(ctx.checkpoint().next_event_index(), 1);
    assert_eq!(ctx.checkpoint().replay_time_ns(), Some(1_000));
    drop(ctx);

    assert_eq!(strategy.replay_time_ns(), Some(1_000));
    assert_eq!(strategy.checkpoint().next_event_index(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_replay_resume_from_checkpoint_skips_processed_events() {
    let mut first = replay_strategy(two_kline_replay()).await;
    let ctx = first.next().await.unwrap().unwrap();
    let checkpoint = ctx.checkpoint();
    drop(ctx);

    let mut resumed = StrategyReplay::builder(two_kline_replay())
        .market(FakeMarket::new().account("sim", 100_000.0))
        .broker(FakeBroker::new().fill_all())
        .account("sim")
        .kline("SHFE.au2602", Duration::from_secs(60), 16)
        .resume_from(checkpoint)
        .build()
        .await
        .unwrap();

    assert_eq!(resumed.replay_time_ns(), Some(1_000));
    let ctx = resumed.next().await.unwrap().unwrap();
    assert_eq!(ctx.replay_time_ns(), 2_000);
    assert_eq!(ctx.checkpoint().next_event_index(), 2);
}
```

- [ ] **Step 2: Verify red**

Run:

```bash
cargo test -p tqsdk-task --test strategy_replay strategy_replay_exposes_replay_clock_and_checkpoint -- --nocapture
```

Expected: fail because the methods/types do not exist.

- [ ] **Step 3: Implement minimal code**

Add:

```rust
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StrategyReplayCheckpoint {
    next_event_index: usize,
    replay_time_ns: Option<i64>,
}
```

Track the checkpoint in builder/host/context. `next()` captures current index before consuming the event, increments after successful commit/context creation, and returns a context checkpoint pointing to the next event.

- [ ] **Step 4: Verify green**

Run:

```bash
cargo test -p tqsdk-task --test strategy_replay -- --nocapture
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add crates/tqsdk-task/src/replay.rs crates/tqsdk-task/src/lib.rs crates/tqsdk-task/tests/strategy_replay.rs
git commit -m "feat: add strategy replay checkpoints"
```

## Task 2: Scenario Contract And Docs

- [ ] **Step 1: Update S16 example**

Add `ctx.replay_time_ns()` and `ctx.checkpoint().next_event_index()` to the printed replay status.

- [ ] **Step 2: Update docs**

Update:

- `docs/public-api-scenario-review.md`
- `docs/scenarios/api_gaps/api_contract_s16_history_replay_strategy.rs`
- `docs/scenarios/user-layer-iteration-plan.md`
- `crates/tqsdk-task/README.md`
- `docs/architecture/api-task.md`

Remaining gap should be replay speed/sleep policy, durable checkpoint persistence, multi-series builder, and full live/sim/replay environment.

- [ ] **Step 3: Verify and commit**

Run:

```bash
cargo check -p tqsdk-task --example api_contract_s16_history_replay_strategy
scripts/check_api_contract_examples.sh
```

Commit:

```bash
git add crates/tqsdk-task/examples/api_contract_s16_history_replay_strategy.rs docs/public-api-scenario-review.md docs/scenarios/api_gaps/api_contract_s16_history_replay_strategy.rs docs/scenarios/user-layer-iteration-plan.md crates/tqsdk-task/README.md docs/architecture/api-task.md
git commit -m "docs: promote strategy replay checkpoint contract"
```

## Task 3: Full Verification

Run:

```bash
scripts/check_api_contract_examples.sh
cargo check --workspace --examples
cargo test --workspace
cargo clippy --workspace --examples --all-targets -- -D warnings
```

Then run:

```bash
git status --short
git log --oneline -12
```

## Self-Review

- Scope stays inside `tqsdk-task::StrategyReplay`.
- No real-time sleep/speed controller is included.
- No persistent checkpoint storage is included.
- No environment adapter is included.
