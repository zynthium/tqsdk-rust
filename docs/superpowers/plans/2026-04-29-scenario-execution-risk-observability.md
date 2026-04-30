# Scenario Execution/Risk Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 暂缓 S14，把下一批场景驱动 Public API 迭代聚焦在 S12/S13/S19 的 revision-bound execution/risk report、轻量 what-if projection 和文档状态同步。

**Architecture:** 本批只在 `tqsdk-task` 扩展执行层 public contract，并同步 `docs/public-api-scenario-review.md` 与 gap sketches。不得把 execution group、account group、risk what-if、audit/report 能力下沉到 `tqsdk-core` 或 `tqsdk-session`；所有 report 必须从同一 `RuntimeReader` revision-bound snapshot 读取，不新增第二棵状态树。

**Tech Stack:** Rust 2024, Tokio, `tqsdk-task`, `tqsdk-wait`, `tqsdk-core` typed state, existing scenario examples and tests.

---

## Scope

### Included

- S19：新增轻量 `RiskProjectionReport`，让用户在下单前获得 revision-bound projected net position / price basis / notional estimate foundation。
- S12：新增 revision-bound `ExecutionGroupReport`，让 execution group 的 status/outcome 与 runtime revision 绑定，作为 audit/resume 前置基础。
- S13：新增 revision-bound `MultiAccountOrderGroupReport`，让 multi-account order 的 status/outcome 与 runtime revision 绑定，作为 audit/resume 前置基础。
- 文档：同步 public API scenario review、S12/S13/S19 gap sketches、`docs/scenarios/user-layer-iteration-plan.md`。

### Excluded

- S14 多 provider 行情聚合，继续暂缓。
- 自动 hedge / flatten。
- timed cancel / replace。
- 跨进程 durable audit log。
- 跨进程 intent ledger persistence。
- HTTP health/metrics endpoint、GUI 或 web helper。

---

## File Structure

- Modify: `crates/tqsdk-task/src/risk.rs`
  - Add revision-bound risk projection type and `RiskEngine::project_order`.
  - Keep `RiskEngine::check_report` as the pre-trade decision API.
- Modify: `crates/tqsdk-task/src/execution_group.rs`
  - Add revision-bound group report type and `ExecutionGroupTicket::report`.
  - Keep existing `status`, `outcome`, and `wait_finished` compatibility.
- Modify: `crates/tqsdk-task/src/account_group.rs`
  - Add revision-bound multi-account group report type and `MultiAccountOrderTicket::report`.
  - Keep existing `status`, `outcome`, and `wait_finished` compatibility.
- Modify: `crates/tqsdk-task/src/lib.rs`
  - Re-export new public report/projection types.
- Modify: `crates/tqsdk-task/tests/risk_orders.rs`
  - Add TDD tests for `RiskProjectionReport`.
- Modify: `crates/tqsdk-task/tests/execution_group.rs`
  - Add TDD tests for `ExecutionGroupReport`.
- Modify: `crates/tqsdk-task/tests/account_group.rs`
  - Add TDD tests for `MultiAccountOrderGroupReport`.
- Modify: `crates/tqsdk-task/examples/api_contract_s12_spread_arbitrage.rs`
  - Demonstrate revision-bound execution group report.
- Modify: `crates/tqsdk-task/examples/api_contract_s13_multi_account_ordering.rs`
  - Demonstrate revision-bound multi-account group report.
- Modify: `crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs`
  - Demonstrate `project_order` before guarded submit.
- Modify: `docs/scenarios/api_gaps/api_contract_s12_spread_arbitrage.rs`
  - Mark revision-bound report as supported; keep hedge/flatten/resume/durable audit as gaps.
- Modify: `docs/scenarios/api_gaps/api_contract_s13_multi_account_ordering.rs`
  - Mark revision-bound report as supported; keep advanced failure policy/resume/durable audit as gaps.
- Modify: `docs/scenarios/api_gaps/api_contract_s19_pre_trade_risk.rs`
  - Mark lightweight what-if projection as supported; keep margin/rule/audit/hot-update as gaps.
- Modify: `docs/public-api-scenario-review.md`
  - Update status rows for S12/S13/S19.
- Modify: `docs/scenarios/user-layer-iteration-plan.md`
  - Sync the already-landed `max_unhedged`, `RiskCheckReport`, and new projection/report foundation.

---

## Task 1: Add Revision-Bound Risk Projection

**Files:**
- Modify: `crates/tqsdk-task/src/risk.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`
- Test: `crates/tqsdk-task/tests/risk_orders.rs`

- [ ] **Step 1: Write failing test for projected net and revision**

Add this test to `crates/tqsdk-task/tests/risk_orders.rs`:

```rust
#[test]
fn risk_engine_project_order_exposes_revision_bound_position_projection() {
    let mut host = seeded_risk_host("ACC1", "SHFE.au2602", 100_000.0, 2, 1, 480.0);
    let intent = TaskOrderIntent {
        account_id: "ACC1".to_owned(),
        symbol: "SHFE.au2602".to_owned(),
        direction: TradeDirection::Buy,
        offset: Some(TradeOffset::Open),
        volume: 3,
        limit_price: Some(481.0),
    };

    let report = RiskEngine::new()
        .project_order(host.api(), &intent)
        .expect("projection should read one runtime snapshot");

    assert_eq!(report.account_id(), "ACC1");
    assert_eq!(report.symbol(), "SHFE.au2602");
    assert_eq!(report.current_net(), Some(1));
    assert_eq!(report.projected_net(), Some(4));
    assert_eq!(report.price_basis(), Some(481.0));
    assert_eq!(report.estimated_price_volume(), Some(1443.0));
    assert_eq!(report.revision(), host.api().session().reader().read().revision());
}
```

Run:

```bash
cargo test -p tqsdk-task risk_engine_project_order_exposes_revision_bound_position_projection
```

Expected: FAIL with missing `project_order` / `RiskProjectionReport`.

- [ ] **Step 2: Implement `RiskProjectionReport`**

Add to `crates/tqsdk-task/src/risk.rs` near `RiskCheckReport`:

```rust
/// Revision-bound lightweight what-if projection for one task order intent.
#[derive(Debug, Clone, PartialEq)]
pub struct RiskProjectionReport {
    revision: Revision,
    account_id: String,
    symbol: String,
    current_net: Option<i64>,
    projected_net: Option<i64>,
    price_basis: Option<f64>,
    estimated_price_volume: Option<f64>,
}

impl RiskProjectionReport {
    #[must_use]
    pub fn revision(&self) -> Revision {
        self.revision
    }

    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub fn current_net(&self) -> Option<i64> {
        self.current_net
    }

    #[must_use]
    pub fn projected_net(&self) -> Option<i64> {
        self.projected_net
    }

    #[must_use]
    pub fn price_basis(&self) -> Option<f64> {
        self.price_basis
    }

    #[must_use]
    pub fn estimated_price_volume(&self) -> Option<f64> {
        self.estimated_price_volume
    }
}
```

Add this method to `impl RiskEngine`:

```rust
pub fn project_order(
    &self,
    api: &tqsdk_wait::TqApi,
    intent: &TaskOrderIntent,
) -> Result<RiskProjectionReport> {
    let snapshot = api.session().reader().read();
    let revision = snapshot.revision();
    let view = snapshot.view();
    let trade = view.trade_state();
    let market = view.market_state();
    let account_id = AccountId::new(intent.account_id.clone());
    let symbol = Symbol::new(intent.symbol.clone());

    let current_net = trade
        .position(&account_id, &symbol)?
        .map(|position| position.volume_long - position.volume_short);
    let projected_net = current_net.map(|current| project_net_position(current, intent));
    let price_basis = intent.limit_price.or_else(|| {
        market
            .quote(&symbol)
            .ok()
            .flatten()
            .and_then(|quote| quote.last_price.is_finite().then_some(quote.last_price))
    });
    let estimated_price_volume = price_basis.map(|price| price * intent.volume as f64);

    Ok(RiskProjectionReport {
        revision,
        account_id: intent.account_id.clone(),
        symbol: intent.symbol.clone(),
        current_net,
        projected_net,
        price_basis,
        estimated_price_volume,
    })
}
```

Update `crates/tqsdk-task/src/lib.rs`:

```rust
pub use risk::{RiskCheckReport, RiskDecision, RiskEngine, RiskProjectionReport, RiskRejection};
```

- [ ] **Step 3: Run focused tests**

Run:

```bash
cargo test -p tqsdk-task --test risk_orders
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/tqsdk-task/src/risk.rs crates/tqsdk-task/src/lib.rs crates/tqsdk-task/tests/risk_orders.rs
git commit -m "feat(task): add revision-bound risk projection"
```

---

## Task 2: Add Revision-Bound Execution Group Report

**Files:**
- Modify: `crates/tqsdk-task/src/execution_group.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`
- Test: `crates/tqsdk-task/tests/execution_group.rs`

- [ ] **Step 1: Write failing test for S12 report revision**

Add this test to `crates/tqsdk-task/tests/execution_group.rs`:

```rust
#[test]
fn execution_group_report_binds_status_to_runtime_revision() {
    let mut host = seeded_execution_group_host();
    let ticket = submit_two_leg_group(&mut host, "spread-report-1");

    let report = ticket
        .report(host.api())
        .expect("group report should read one runtime snapshot");

    assert_eq!(report.revision(), host.api().session().reader().read().revision());
    assert_eq!(report.group_id(), "spread-report-1");
    assert_eq!(report.account_id(), "ACC1");
    assert_eq!(report.legs().len(), 2);
    assert!(matches!(report.status(), ExecutionGroupStatus::Pending { .. }));
}
```

If existing helpers have different names, add local helper functions in the same test module rather than changing production API.

Run:

```bash
cargo test -p tqsdk-task execution_group_report_binds_status_to_runtime_revision
```

Expected: FAIL with missing `ExecutionGroupTicket::report` / `ExecutionGroupReport`.

- [ ] **Step 2: Implement `ExecutionGroupReport`**

Add this type to `crates/tqsdk-task/src/execution_group.rs`:

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionGroupReport {
    revision: tqsdk_core::Revision,
    group_id: String,
    account_id: String,
    status: ExecutionGroupStatus,
}

impl ExecutionGroupReport {
    #[must_use]
    pub fn revision(&self) -> tqsdk_core::Revision {
        self.revision
    }

    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub fn status(&self) -> &ExecutionGroupStatus {
        &self.status
    }

    #[must_use]
    pub fn legs(&self) -> &[ExecutionLegReport] {
        match &self.status {
            ExecutionGroupStatus::Pending { legs } => legs,
            ExecutionGroupStatus::Finished(outcome) => outcome.legs(),
        }
    }
}
```

Add this helper to `impl ExecutionGroupOutcome`:

```rust
impl ExecutionGroupOutcome {
    #[must_use]
    pub fn legs(&self) -> &[ExecutionLegReport] {
        match self {
            Self::AllFilled { legs }
            | Self::Cancelled { legs }
            | Self::Rejected { legs }
            | Self::Failed { legs }
            | Self::NeedsHedge { legs, .. } => legs,
        }
    }
}
```

Add this method to `impl ExecutionGroupTicket`:

```rust
pub fn report(&self, api: &tqsdk_wait::TqApi) -> Result<ExecutionGroupReport> {
    let snapshot = api.session().reader().read();
    let revision = snapshot.revision();
    let status = self.status(api)?;
    Ok(ExecutionGroupReport {
        revision,
        group_id: self.group_id.clone(),
        account_id: self.account_id.clone(),
        status,
    })
}
```

Update `crates/tqsdk-task/src/lib.rs` re-export list:

```rust
ExecutionGroupReport,
```

- [ ] **Step 3: Run focused tests**

Run:

```bash
cargo test -p tqsdk-task --test execution_group
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/tqsdk-task/src/execution_group.rs crates/tqsdk-task/src/lib.rs crates/tqsdk-task/tests/execution_group.rs
git commit -m "feat(task): add revision-bound execution group report"
```

---

## Task 3: Add Revision-Bound Multi-Account Group Report

**Files:**
- Modify: `crates/tqsdk-task/src/account_group.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`
- Test: `crates/tqsdk-task/tests/account_group.rs`

- [ ] **Step 1: Write failing test for S13 report revision**

Add this test to `crates/tqsdk-task/tests/account_group.rs`:

```rust
#[test]
fn multi_account_order_report_binds_status_to_runtime_revision() {
    let mut host = seeded_multi_account_host();
    let ticket = submit_multi_account_order(&mut host, "acct-report-1");

    let report = ticket
        .report(host.api())
        .expect("multi-account report should read one runtime snapshot");

    assert_eq!(report.revision(), host.api().session().reader().read().revision());
    assert_eq!(report.group_id(), "acct-report-1");
    assert_eq!(report.accounts().len(), 2);
    assert!(matches!(report.status(), MultiAccountOrderStatus::Pending { .. }));
}
```

If existing helpers have different names, add local helper functions in the same test module rather than changing production API.

Run:

```bash
cargo test -p tqsdk-task multi_account_order_report_binds_status_to_runtime_revision
```

Expected: FAIL with missing `MultiAccountOrderTicket::report` / `MultiAccountOrderGroupReport`.

- [ ] **Step 2: Implement `MultiAccountOrderGroupReport`**

Add this type to `crates/tqsdk-task/src/account_group.rs`:

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct MultiAccountOrderGroupReport {
    revision: tqsdk_core::Revision,
    group_id: String,
    status: MultiAccountOrderStatus,
}

impl MultiAccountOrderGroupReport {
    #[must_use]
    pub fn revision(&self) -> tqsdk_core::Revision {
        self.revision
    }

    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    #[must_use]
    pub fn status(&self) -> &MultiAccountOrderStatus {
        &self.status
    }

    #[must_use]
    pub fn accounts(&self) -> &[MultiAccountOrderReport] {
        match &self.status {
            MultiAccountOrderStatus::Pending { accounts } => accounts,
            MultiAccountOrderStatus::Finished(outcome) => outcome.accounts(),
        }
    }
}
```

Add this helper to `impl MultiAccountOrderOutcome`:

```rust
impl MultiAccountOrderOutcome {
    #[must_use]
    pub fn accounts(&self) -> &[MultiAccountOrderReport] {
        match self {
            Self::AllFilled { accounts }
            | Self::Cancelled { accounts }
            | Self::Rejected { accounts }
            | Self::Failed { accounts }
            | Self::NeedsAttention { accounts, .. } => accounts,
        }
    }
}
```

Add this method to `impl MultiAccountOrderTicket`:

```rust
pub fn report(&self, api: &tqsdk_wait::TqApi) -> Result<MultiAccountOrderGroupReport> {
    let snapshot = api.session().reader().read();
    let revision = snapshot.revision();
    let status = self.status(api)?;
    Ok(MultiAccountOrderGroupReport {
        revision,
        group_id: self.group_id.clone(),
        status,
    })
}
```

Update `crates/tqsdk-task/src/lib.rs` re-export list:

```rust
MultiAccountOrderGroupReport,
```

- [ ] **Step 3: Run focused tests**

Run:

```bash
cargo test -p tqsdk-task --test account_group
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/tqsdk-task/src/account_group.rs crates/tqsdk-task/src/lib.rs crates/tqsdk-task/tests/account_group.rs
git commit -m "feat(task): add revision-bound multi-account report"
```

---

## Task 4: Promote New Contracts Into Scenario Examples

**Files:**
- Modify: `crates/tqsdk-task/examples/api_contract_s12_spread_arbitrage.rs`
- Modify: `crates/tqsdk-task/examples/api_contract_s13_multi_account_ordering.rs`
- Modify: `crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs`

- [ ] **Step 1: Update S19 example to show projection before risk decision**

In `crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs`, after building the order intent and before submitting, add:

```rust
let projection = risk.project_order(host.api(), &intent)?;
println!(
    "risk projection rev={:?} account={} symbol={} current_net={:?} projected_net={:?} price_basis={:?} price_volume={:?}",
    projection.revision(),
    projection.account_id(),
    projection.symbol(),
    projection.current_net(),
    projection.projected_net(),
    projection.price_basis(),
    projection.estimated_price_volume()
);
```

- [ ] **Step 2: Update S12 example to show revision-bound group report**

In `crates/tqsdk-task/examples/api_contract_s12_spread_arbitrage.rs`, after `send_once()` returns:

```rust
let report = ticket.report(host.api())?;
println!(
    "execution group rev={:?} group={} account={} legs={} status={:?}",
    report.revision(),
    report.group_id(),
    report.account_id(),
    report.legs().len(),
    report.status()
);
```

- [ ] **Step 3: Update S13 example to show revision-bound account report**

In `crates/tqsdk-task/examples/api_contract_s13_multi_account_ordering.rs`, after `send_once()` returns:

```rust
let report = ticket.report(host.api())?;
println!(
    "multi-account rev={:?} group={} accounts={} status={:?}",
    report.revision(),
    report.group_id(),
    report.accounts().len(),
    report.status()
);
```

- [ ] **Step 4: Check examples**

Run:

```bash
cargo check -p tqsdk-task --examples
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/tqsdk-task/examples/api_contract_s12_spread_arbitrage.rs crates/tqsdk-task/examples/api_contract_s13_multi_account_ordering.rs crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs
git commit -m "docs(task): update scenario examples for execution risk reports"
```

---

## Task 5: Sync Scenario Review and Gap Documentation

**Files:**
- Modify: `docs/public-api-scenario-review.md`
- Modify: `docs/scenarios/user-layer-iteration-plan.md`
- Modify: `docs/scenarios/api_gaps/api_contract_s12_spread_arbitrage.rs`
- Modify: `docs/scenarios/api_gaps/api_contract_s13_multi_account_ordering.rs`
- Modify: `docs/scenarios/api_gaps/api_contract_s19_pre_trade_risk.rs`

- [ ] **Step 1: Update S12 review status**

In `docs/public-api-scenario-review.md`, update S12 text to include:

```markdown
revision-bound `ExecutionGroupReport`
```

Keep S12 as `勉强` because automatic hedge / flatten, timed cancel / replace, resume and durable audit remain gaps.

- [ ] **Step 2: Update S13 review status**

In `docs/public-api-scenario-review.md`, update S13 text to include:

```markdown
revision-bound `MultiAccountOrderGroupReport`
```

Keep S13 as `勉强` because advanced failure policy, resume and durable audit remain gaps.

- [ ] **Step 3: Update S19 review status**

In `docs/public-api-scenario-review.md`, update S19 text to include:

```markdown
`RiskProjectionReport`
```

Keep S19 as `勉强` because contract metadata driven checks, portfolio margin what-if, hot updates and durable audit remain gaps.

- [ ] **Step 4: Update user-layer plan**

In `docs/scenarios/user-layer-iteration-plan.md`, sync the already landed state:

```markdown
- `ExecutionGroup` foundation 支持 observed `max_unhedged` exposure timeout 和 revision-bound `ExecutionGroupReport`。
- `AccountGroup` foundation 支持 observed `max_unhedged` account exposure timeout 和 revision-bound `MultiAccountOrderGroupReport`。
- `RiskEngine::check_report` 返回 revision-bound `RiskCheckReport`；`RiskEngine::project_order` 返回 lightweight revision-bound `RiskProjectionReport`。
```

- [ ] **Step 5: Update gap sketches**

In each relevant gap sketch, add comments that the following are now supported:

```rust
// Supported foundation:
// - revision-bound report/projection is available in the formal task example.
```

Do not move the gap sketches out of `docs/scenarios/api_gaps/` because each scenario still has remaining unsupported behavior.

- [ ] **Step 6: Run contract header check**

Run:

```bash
scripts/check_api_contract_examples.sh
```

Expected: PASS. A local `LC_ALL` warning is acceptable only if the script exits `0`.

- [ ] **Step 7: Commit**

```bash
git add docs/public-api-scenario-review.md docs/scenarios/user-layer-iteration-plan.md docs/scenarios/api_gaps/api_contract_s12_spread_arbitrage.rs docs/scenarios/api_gaps/api_contract_s13_multi_account_ordering.rs docs/scenarios/api_gaps/api_contract_s19_pre_trade_risk.rs
git commit -m "docs: update execution risk scenario status"
```

---

## Task 6: Full Verification

**Files:**
- No source changes expected unless verification finds a bug.

- [ ] **Step 1: Format**

Run:

```bash
cargo fmt --check
```

Expected: PASS. If it fails, run `cargo fmt`, review the diff, and commit formatting as:

```bash
git add crates/tqsdk-task/src/risk.rs crates/tqsdk-task/src/execution_group.rs crates/tqsdk-task/src/account_group.rs crates/tqsdk-task/src/lib.rs crates/tqsdk-task/tests/risk_orders.rs crates/tqsdk-task/tests/execution_group.rs crates/tqsdk-task/tests/account_group.rs crates/tqsdk-task/examples/api_contract_s12_spread_arbitrage.rs crates/tqsdk-task/examples/api_contract_s13_multi_account_ordering.rs crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs
git commit -m "fix: format execution risk observability changes"
```

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

- [ ] **Step 5: Verify feature flags**

Run:

```bash
cargo check --workspace --no-default-features
cargo check --workspace --all-features --examples
```

Expected: PASS.

- [ ] **Step 6: Final status**

Run:

```bash
git status --short --branch
git log --oneline -8
```

Expected: only pre-existing unrelated files may remain untracked, and all plan changes are committed.

---

## Acceptance Criteria

- S14 remains untouched except being explicitly excluded from this plan.
- S12 formal example uses `ExecutionGroupReport` and remains compilable.
- S13 formal example uses `MultiAccountOrderGroupReport` and remains compilable.
- S19 formal example uses `RiskProjectionReport` and `RiskCheckReport`.
- No user-facing example uses provider private modules, `RuntimeCommand`, `RuntimeHandle`, manual channel orchestration, or `Arc<Mutex<_>>`.
- `docs/public-api-scenario-review.md` and `docs/scenarios/user-layer-iteration-plan.md` agree on S12/S13/S19 status.
- Full required verification passes:
  - `cargo check --workspace --examples`
  - `cargo test --workspace`
  - `cargo clippy --workspace --examples --all-targets -- -D warnings`
  - `cargo check --workspace --no-default-features`
  - `cargo check --workspace --all-features --examples`
