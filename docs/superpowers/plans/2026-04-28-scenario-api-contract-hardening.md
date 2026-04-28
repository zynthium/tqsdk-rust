# Scenario API Contract Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修正当前场景驱动 Public API 契约中已经暴露的语义过度承诺与类型安全退化，让 S12/S13/S19/S24 的正式 examples 更准确地代表 SDK public API 能力。

**Architecture:** 本批只在 `tqsdk-task` 与对应 examples/docs 内演进，不下沉到 `tqsdk-core` / `tqsdk-session`。`tqsdk-core` 继续保持 protocol-complete runtime substrate；task 层只消费既有 `TqApi` / `SessionClient` / `RuntimeReader` 能力，不新增第二棵状态树、不新增旁路通知、不内置 GUI 或 HTTP endpoint。

**Tech Stack:** Rust, Tokio, Cargo workspace examples, `tqsdk-task`, `tqsdk-wait`, `tqsdk-core::OrderLifecycle`, scenario contract docs.

---

## File Structure

- Modify: `crates/tqsdk-task/src/execution_group.rs`
  - Make `max_unhedged` participate in `ExecutionGroupTicket::wait_finished`.
  - Preserve current behavior of `HedgePolicy::ReportExposure`: typed report only, no automatic hedge order.
- Modify: `crates/tqsdk-task/src/account_group.rs`
  - Make `max_unhedged` participate in `MultiAccountOrderTicket::wait_finished`.
  - Preserve current behavior of `AccountFailurePolicy::ReportExposure`: typed report only, no automatic flatten.
- Modify: `crates/tqsdk-task/src/risk.rs`
  - Add revision-bound `RiskCheckReport`.
  - Keep `RiskEngine::check` source-compatible by delegating to the report path.
- Modify: `crates/tqsdk-task/src/lib.rs`
  - Re-export new risk report type.
- Modify: `crates/tqsdk-task/examples/api_contract_s12_spread_arbitrage.rs`
  - Keep `max_unhedged` only after it has real wait semantics.
  - Clarify that automatic hedge remains unsupported.
- Modify: `crates/tqsdk-task/examples/api_contract_s13_multi_account_ordering.rs`
  - Keep `max_unhedged` only after it has real wait semantics.
  - Clarify that automatic account flatten remains unsupported.
- Modify: `crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs`
  - Demonstrate typed `RiskCheckReport` / revision-aware preflight.
- Modify: `crates/tqsdk-task/examples/api_contract_s24_testable_strategy.rs`
  - Replace string status assertions with `OrderLifecycle` assertions.
- Modify: `docs/public-api-scenario-review.md`
  - Update S12/S13/S19/S24 status and remaining gaps after implementation.
- Modify: `docs/scenarios/api_gaps/api_contract_s12_spread_arbitrage.rs`
  - Remove `max_unhedged` from remaining gap once enforced.
- Modify: `docs/scenarios/api_gaps/api_contract_s13_multi_account_ordering.rs`
  - Remove `max_unhedged` from remaining gap once enforced.
- Modify: `docs/scenarios/api_gaps/api_contract_s19_pre_trade_risk.rs`
  - Narrow remaining gap to advanced portfolio what-if / persistent risk audit if revision report lands.

---

## Task 1: Make S12 `max_unhedged` Real

**Files:**
- Modify: `crates/tqsdk-task/src/execution_group.rs`
- Test: `crates/tqsdk-task/tests/execution_group.rs`
- Example: `crates/tqsdk-task/examples/api_contract_s12_spread_arbitrage.rs`

- [ ] **Step 1: Write failing test for exposure timeout**

Add a test named `execution_group_wait_finished_returns_needs_hedge_after_max_unhedged_exposure` in `crates/tqsdk-task/tests/execution_group.rs`.

Test shape:
- Build a two-leg execution group with `.max_unhedged(Duration::from_millis(10))`.
- Seed one leg as partially or fully filled.
- Leave the other leg live/pending.
- Call `wait_finished` with a much longer deadline.
- Assert the result is `ExecutionGroupOutcome::NeedsHedge`.
- Assert the returned exposure contains the filled symbol and the unfilled symbol.

Expected before implementation: the test hangs until the outer deadline or returns only after global timeout, proving `max_unhedged` is not enforced.

- [ ] **Step 2: Implement observed-exposure timer**

In `ExecutionGroupTicket::wait_finished`:
- Track `exposure_started_at: Option<tokio::time::Instant>`.
- Each loop reads `leg_reports`.
- If reports show open exposure and `max_unhedged` is set:
  - set `exposure_started_at` when exposure is first observed;
  - compute `exposure_deadline = exposure_started_at + max_unhedged`;
  - use `min(global_deadline, exposure_deadline)` as the wait deadline;
  - if the exposure deadline is reached first, return `NeedsHedge`.
- If exposure disappears, clear `exposure_started_at`.
- Do not submit hedge/flatten orders.

Exposure predicate:
- `has_filled = any leg.filled_volume > 0`
- `has_unfilled = any leg.volume_left > 0 && leg.state is not Rejected/Failed/Cancelled`
- open exposure exists when both are true.

- [ ] **Step 3: Run focused test**

Run:

```bash
cargo test -p tqsdk-task execution_group_wait_finished_returns_needs_hedge_after_max_unhedged_exposure
```

Expected: PASS.

- [ ] **Step 4: Update S12 example and gap sketch**

Update the S12 example header to say:
- `max_unhedged` triggers typed `NeedsHedge` report when observed exposure lasts longer than the configured duration.
- `HedgePolicy::ReportExposure` does not submit hedge orders.

Update the S12 gap sketch so remaining gaps are:
- automatic hedge / flatten;
- timed cancel / replace;
- group resume / persistent audit log.

- [ ] **Step 5: Commit**

```bash
git add crates/tqsdk-task/src/execution_group.rs crates/tqsdk-task/tests/execution_group.rs crates/tqsdk-task/examples/api_contract_s12_spread_arbitrage.rs docs/scenarios/api_gaps/api_contract_s12_spread_arbitrage.rs
git commit -m "feat(task): enforce execution group exposure timeout"
```

---

## Task 2: Make S13 `max_unhedged` Real

**Files:**
- Modify: `crates/tqsdk-task/src/account_group.rs`
- Test: `crates/tqsdk-task/tests/account_group.rs`
- Example: `crates/tqsdk-task/examples/api_contract_s13_multi_account_ordering.rs`

- [ ] **Step 1: Write failing test for account allocation exposure timeout**

Add a test named `multi_account_order_wait_finished_returns_attention_after_max_unhedged_exposure` in `crates/tqsdk-task/tests/account_group.rs`.

Test shape:
- Build a multi-account order with `.max_unhedged(Duration::from_millis(10))`.
- Seed one account as filled and another account as live/pending.
- Call `wait_finished` with a longer deadline.
- Assert the result is `MultiAccountOrderOutcome::NeedsAttention`.
- Assert filled and unfilled account lists are populated separately.

Expected before implementation: the test waits until the outer deadline, proving `max_unhedged` is not enforced.

- [ ] **Step 2: Implement observed account-exposure timer**

In `MultiAccountOrderTicket::wait_finished`:
- Track `exposure_started_at: Option<tokio::time::Instant>`.
- Reuse account reports each loop.
- If at least one account has filled volume and at least one account still has unfilled live/pending volume, start or continue exposure timer.
- Use the earlier of caller deadline and exposure deadline.
- On exposure timeout, return `needs_attention_from_reports(&accounts)`.
- Do not submit account flatten orders.

- [ ] **Step 3: Run focused test**

Run:

```bash
cargo test -p tqsdk-task multi_account_order_wait_finished_returns_attention_after_max_unhedged_exposure
```

Expected: PASS.

- [ ] **Step 4: Update S13 example and gap sketch**

Update the S13 example header to say:
- `max_unhedged` triggers typed `NeedsAttention` when account allocation exposure lasts longer than configured.
- `AccountFailurePolicy::ReportExposure` does not submit flatten orders.

Update the S13 gap sketch so remaining gaps are:
- automatic account flatten / rebalance;
- persistent resume;
- durable execution audit.

- [ ] **Step 5: Commit**

```bash
git add crates/tqsdk-task/src/account_group.rs crates/tqsdk-task/tests/account_group.rs crates/tqsdk-task/examples/api_contract_s13_multi_account_ordering.rs docs/scenarios/api_gaps/api_contract_s13_multi_account_ordering.rs
git commit -m "feat(task): enforce multi-account exposure timeout"
```

---

## Task 3: Add Revision-Bound Risk Check Report for S19

**Files:**
- Modify: `crates/tqsdk-task/src/risk.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`
- Modify: `crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs`
- Test: `crates/tqsdk-task/tests/risk_orders.rs`

- [ ] **Step 1: Write failing test for report revision**

Add a test named `risk_engine_report_exposes_revision_bound_decision` in `crates/tqsdk-task/tests/risk_orders.rs`.

Test shape:
- Seed quote/account/position.
- Build a `TaskOrderIntent`.
- Call `RiskEngine::new().max_price_deviation(...).check_report(host.api(), &intent)`.
- Assert `report.revision().get() > 0`.
- Assert `report.decision().is_accepted()` for an in-band price.
- Call again with an out-of-band price and assert `report.decision().rejection()` is `Some(RiskRejection::PriceDeviationExceeded { .. })`.

Expected before implementation: the test does not compile because `RiskCheckReport` and `check_report` do not exist.

- [ ] **Step 2: Add `RiskCheckReport`**

In `crates/tqsdk-task/src/risk.rs`, add:
- `pub struct RiskCheckReport { revision: tqsdk_core::Revision, decision: RiskDecision }`
- `pub fn revision(&self) -> tqsdk_core::Revision`
- `pub fn decision(&self) -> &RiskDecision`
- `pub fn into_decision(self) -> RiskDecision`

- [ ] **Step 3: Implement `RiskEngine::check_report`**

Implementation requirements:
- Acquire one revision-bound read through `api.session().reader().read()`.
- Decode account, position and quote from that single snapshot view.
- Return a `RiskCheckReport` carrying the snapshot revision and final `RiskDecision`.
- Preserve all existing rejection variants.
- Keep `RiskEngine::check` public and source-compatible by returning `self.check_report(api, intent)?.into_decision()`.

If direct typed decode from `StateReadView` is not ergonomic enough, keep the helper private to `risk.rs`; do not add new `tqsdk-core` convenience unless a core read API gap is proven by implementation.

- [ ] **Step 4: Re-export report**

In `crates/tqsdk-task/src/lib.rs`, export `RiskCheckReport` next to `RiskDecision`, `RiskEngine`, and `RiskRejection`.

- [ ] **Step 5: Update S19 example**

Update `crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs` to demonstrate:
- typed order intent construction or builder preflight;
- risk report revision printing;
- existing guarded order submit path still rejects through `TaskError::RiskRejected`.

Do not require users to manually manage `RuntimeReader`, channel, or `Arc<Mutex<_>>`.

- [ ] **Step 6: Run focused tests**

Run:

```bash
cargo test -p tqsdk-task risk_engine_report_exposes_revision_bound_decision
cargo test -p tqsdk-task risk_orders
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/tqsdk-task/src/risk.rs crates/tqsdk-task/src/lib.rs crates/tqsdk-task/tests/risk_orders.rs crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs
git commit -m "feat(task): add revision-bound risk check report"
```

---

## Task 4: Make S24 Strategy Test Assertions Typed

**Files:**
- Modify: `crates/tqsdk-task/examples/api_contract_s24_testable_strategy.rs`
- Test: `crates/tqsdk-task/tests/strategy_testing.rs`

- [ ] **Step 1: Update tests to use `OrderLifecycle`**

In `crates/tqsdk-task/tests/strategy_testing.rs`, replace public-facing assertions of raw `"ALIVE"` / `"FINISHED"` order status with assertions on `order.lifecycle` where the test is verifying user-visible lifecycle.

Keep raw status assertions only in tests that explicitly validate adapter status normalization.

- [ ] **Step 2: Update S24 example**

In `crates/tqsdk-task/examples/api_contract_s24_testable_strategy.rs`:
- import `tqsdk_core::OrderLifecycle`;
- replace:
  - `assert_eq!(order.status, "ALIVE")`
  - `assert_eq!(order.status, "FINISHED")`
- with:
  - `assert_eq!(order.lifecycle, OrderLifecycle::PartiallyFilled)`
  - `assert_eq!(order.lifecycle, OrderLifecycle::Filled)`

- [ ] **Step 3: Run focused tests and example check**

Run:

```bash
cargo test -p tqsdk-task strategy_testing
cargo check -p tqsdk-task --example api_contract_s24_testable_strategy
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/tqsdk-task/tests/strategy_testing.rs crates/tqsdk-task/examples/api_contract_s24_testable_strategy.rs
git commit -m "test(task): use typed lifecycle in strategy test contract"
```

---

## Task 5: Update Scenario Review Status

**Files:**
- Modify: `docs/public-api-scenario-review.md`
- Modify: `docs/scenarios/api_gaps/api_contract_s19_pre_trade_risk.rs`

- [ ] **Step 1: Update S12/S13 rows**

In `docs/public-api-scenario-review.md`:
- Keep S12 and S13 as `勉强` unless automatic hedge/flatten and durable audit are implemented.
- Lower the stated timeout/exposure gap after `max_unhedged` is enforced.
- Keep state consistency risk as `中` or `高` depending on remaining automatic compensation gap.

- [ ] **Step 2: Update S19 row**

If `RiskCheckReport` lands:
- Change S19 evidence to include `RiskCheckReport`.
- Keep S19 as `勉强` if guarded insert still does not return a durable risk audit id.
- Narrow remaining gap to portfolio what-if, persistent audit, and richer exchange rule validation.

- [ ] **Step 3: Update S24 row**

If examples no longer assert raw order status:
- Note that typed lifecycle is now used in the public test contract.
- Keep S24 as `勉强` if durable fixtures and richer broker behavior remain incomplete.

- [ ] **Step 4: Run doc consistency checks**

Run:

```bash
rg -n 'max_unhedged|RiskCheckReport|OrderLifecycle|ALIVE|FINISHED' docs/public-api-scenario-review.md docs/scenarios/api_gaps crates/tqsdk-task/examples/api_contract_s*.rs
```

Expected:
- S12/S13 mention `max_unhedged` as implemented typed exposure timeout, not as remaining gap.
- S19 mentions `RiskCheckReport`.
- S24 example does not assert raw `"ALIVE"` / `"FINISHED"` status.

- [ ] **Step 5: Commit**

```bash
git add docs/public-api-scenario-review.md docs/scenarios/api_gaps/api_contract_s19_pre_trade_risk.rs
git commit -m "docs: update scenario api contract status"
```

---

## Task 6: Full Verification

**Files:**
- No source edits unless verification exposes a regression.

- [ ] **Step 1: Format**

Run:

```bash
cargo fmt --check
```

Expected: PASS.

- [ ] **Step 2: Check examples**

Run:

```bash
cargo check --workspace --examples
```

Expected: PASS.

- [ ] **Step 3: Run tests**

Run:

```bash
cargo test --workspace
```

Expected: PASS.

- [ ] **Step 4: Run clippy**

Run:

```bash
cargo clippy --workspace --examples --all-targets -- -D warnings
```

Expected: PASS.

- [ ] **Step 5: Check feature matrices**

Run:

```bash
cargo check --workspace --no-default-features
cargo check --workspace --all-features --examples
```

Expected: PASS.

- [ ] **Step 6: Commit verification fixes if needed**

If verification required code or docs fixes:

```bash
git add <changed-files>
git commit -m "fix: address scenario api hardening verification"
```

If no fixes were needed, do not create an empty commit.

---

## Deferred Next Batch

The following work is intentionally deferred until this contract-hardening batch is complete:

- S18 live market cache pipe:
  - likely landing split: `tqsdk-data` owns cache record/index/reader/writer; `tqsdk-stream` owns live sink adapter; `tqsdk-task` only consumes replay events.
- S21 durable daemon queue / runtime state snapshot recovery:
  - needs a separate design pass because commit metadata journal is not equivalent to runtime state recovery.
- S20 cross-process daemon orchestration:
  - keep transport-neutral; no Rust GUI, web helper, or built-in HTTP health/metrics endpoint.
- S14 multi-provider market aggregation:
  - remains paused by decision; do not start unless explicitly resumed.

---

## Self-Review

- Spec coverage: This plan covers the review findings for S12/S13 `max_unhedged`, S19 revision-bound risk reporting, S24 typed lifecycle examples, and scenario status docs.
- Architecture boundary: No task moves task/data/stream semantics into `tqsdk-core` or `tqsdk-session`.
- Verification: The required post-refactor command set is included.
- Commit strategy: Each behavior change has a focused commit boundary, with docs committed after behavior lands.
