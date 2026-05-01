# Task Account Group Allocation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the S13 multi-account ordering foundation in `tqsdk-task` so a strategy can submit one typed order intent across an account group with deterministic allocation, preflight, idempotent client ids, and per-account outcomes.

**Architecture:** Keep the capability in `tqsdk-task`; do not add multi-account semantics to `tqsdk-core`, `tqsdk-session`, `tqsdk-wait`, or `tqsdk-stream`. Reuse the existing task order substrate (`TaskOrderIntent`, `TaskHost::preflight_task_order`, `TaskHost::submit_prechecked_task_order_once`, wait-layer `OrderTicket`) so runtime commit, command lifecycle, risk checks, and session-scoped intent idempotency remain single-sourced. The new layer is a task execution helper, not a provider aggregation layer and not a new state tree.

**Tech Stack:** Rust 2024, Tokio tests, existing `tqsdk-task`, `tqsdk-wait`, `tqsdk-session`, `tqsdk-core` test helpers, serde_json test seeding.

---

## File Structure

- Create `crates/tqsdk-task/src/account_group.rs`
  - Owns `Ratio`, `AccountGroup`, `AccountGroupBuilder`, allocation plan types, `MultiAccountOrderBuilder`, `MultiAccountOrderTicket`, per-account reports, and outcome classification.
  - Has no runtime state tree and no Tokio task/channel creation.
- Modify `crates/tqsdk-task/src/lib.rs`
  - Adds `mod account_group;` and public re-exports.
- Modify `crates/tqsdk-task/src/error.rs`
  - Adds a multi-account partial-submit error so account execution does not reuse execution-group leg wording.
- Modify `crates/tqsdk-task/src/host.rs`
  - Adds `TaskHost::account_group()` and `TaskHost::multi_account_order(group)`.
  - Keeps order submission through existing preflight and prechecked submit helpers.
- Create `crates/tqsdk-task/tests/account_group.rs`
  - Covers allocation, preflight-before-dispatch, idempotent retry, mismatched retry rejection, and per-account outcomes.
- Create `crates/tqsdk-task/examples/api_contract_s13_multi_account_ordering.rs`
  - Formal public API contract example for the S13 foundation.
- Modify `docs/scenarios/api_gaps/api_contract_s13_multi_account_ordering.rs`
  - Narrow the remaining gap to advanced failure policy, cross-account target-pos orchestration, persistent resume, and audit log.
- Modify `docs/reviews/public-api-scenario-review.md`
  - Move S13 from “无法表达” to “勉强” after the formal example compiles.
- Modify `docs/scenarios/user-layer-iteration-plan.md`
  - Mark account group foundation as landed and preserve remaining gaps.
- Modify `docs/architecture/api-task.md` and `crates/tqsdk-task/README.md`
  - Document the new task-layer account group boundary and non-goals.

## Public API Shape

The intended user-facing code after this plan:

```rust
use std::time::Duration;

use tqsdk_task::{AccountFailurePolicy, Ratio, TaskHost};

# async fn run(mut host: TaskHost) -> tqsdk_task::Result<()> {
let accounts = host
    .account_group()
    .add("sim-a", Ratio::new(7, 10)?)
    .add("sim-b", Ratio::new(3, 10)?)
    .min_volume_per_account(1)
    .build()?;

let ticket = host
    .multi_account_order(accounts)
    .client_group_id("alloc-au-001")
    .max_unhedged(Duration::from_secs(2))
    .on_account_failed(AccountFailurePolicy::ReportExposure)
    .buy_open("SHFE.au2602", 10)
    .limit(480.0)
    .send_once()
    .await?;

let outcome = ticket.wait_finished(&mut host, None).await?;
println!("{outcome:?}");
# Ok(())
# }
```

Non-goals for this batch:

- No automatic hedge / flatten across accounts.
- No cross-account target position scheduler.
- No cross-process persistent resume.
- No audit-log storage backend.
- No provider-specific account discovery.

## Task 1: Account Group Allocation Model

**Files:**
- Create: `crates/tqsdk-task/src/account_group.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`
- Test: `crates/tqsdk-task/tests/account_group.rs`

- [ ] **Step 1: Write allocation model tests**

Add the test file with these initial tests:

```rust
use tqsdk_task::{AccountGroup, Ratio, TaskError};

#[test]
fn account_group_allocates_ratio_volume_with_largest_remainder() {
    let group = AccountGroup::builder()
        .add("sim-a", Ratio::new(2, 3).unwrap())
        .add("sim-b", Ratio::new(1, 3).unwrap())
        .min_volume_per_account(1)
        .build()
        .unwrap();

    let plan = group.allocate(5).unwrap();

    let allocations: Vec<_> = plan
        .allocations()
        .iter()
        .map(|allocation| (allocation.account_id(), allocation.volume()))
        .collect();
    assert_eq!(allocations, vec![("sim-a", 3), ("sim-b", 2)]);
}

#[test]
fn account_group_rejects_empty_and_duplicate_accounts() {
    let empty = AccountGroup::builder().build().unwrap_err();
    assert_eq!(empty, TaskError::InvalidState("account group cannot be empty"));

    let duplicate = AccountGroup::builder()
        .add("sim-a", Ratio::new(1, 1).unwrap())
        .add("sim-a", Ratio::new(1, 1).unwrap())
        .build()
        .unwrap_err();
    assert_eq!(
        duplicate,
        TaskError::InvalidState("duplicate account id in account group")
    );
}

#[test]
fn account_group_rejects_invalid_ratio_and_impossible_minimum() {
    assert_eq!(
        Ratio::new(0, 10).unwrap_err(),
        TaskError::InvalidState("account allocation ratio numerator must be positive")
    );
    assert_eq!(
        Ratio::new(1, 0).unwrap_err(),
        TaskError::InvalidState("account allocation ratio denominator must be positive")
    );

    let group = AccountGroup::builder()
        .add("sim-a", Ratio::new(1, 2).unwrap())
        .add("sim-b", Ratio::new(1, 2).unwrap())
        .min_volume_per_account(1)
        .build()
        .unwrap();

    assert_eq!(
        group.allocate(1).unwrap_err(),
        TaskError::InvalidState("total volume cannot satisfy account minimum volume")
    );
}
```

- [ ] **Step 2: Run the tests and verify failure**

Run:

```bash
cargo test -p tqsdk-task --test account_group account_group_ -- --nocapture
```

Expected: compile failure because `AccountGroup` and `Ratio` are not defined.

- [ ] **Step 3: Implement allocation model**

Create `account_group.rs` with these types and methods:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashSet;

use crate::{Result, TaskError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ratio {
    numerator: u32,
    denominator: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountAllocation {
    account_id: String,
    ratio: Ratio,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountGroup {
    accounts: Vec<AccountAllocation>,
    min_volume_per_account: i64,
}

#[derive(Debug, Default)]
pub struct AccountGroupBuilder {
    accounts: Vec<AccountAllocation>,
    min_volume_per_account: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocatedAccountOrder {
    account_id: String,
    volume: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountAllocationPlan {
    allocations: Vec<AllocatedAccountOrder>,
}

impl Ratio {
    pub fn new(numerator: u32, denominator: u32) -> Result<Self> {
        if numerator == 0 {
            return Err(TaskError::InvalidState(
                "account allocation ratio numerator must be positive",
            ));
        }
        if denominator == 0 {
            return Err(TaskError::InvalidState(
                "account allocation ratio denominator must be positive",
            ));
        }
        Ok(Self {
            numerator,
            denominator,
        })
    }
}

impl AccountAllocation {
    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub fn ratio(&self) -> Ratio {
        self.ratio
    }
}

impl AccountGroup {
    #[must_use]
    pub fn builder() -> AccountGroupBuilder {
        AccountGroupBuilder::default()
    }

    #[must_use]
    pub fn accounts(&self) -> &[AccountAllocation] {
        &self.accounts
    }

    pub fn allocate(&self, total_volume: i64) -> Result<AccountAllocationPlan> {
        if total_volume <= 0 {
            return Err(TaskError::InvalidState("total volume must be positive"));
        }
        if self.accounts.is_empty() {
            return Err(TaskError::InvalidState("account group cannot be empty"));
        }
        if self.min_volume_per_account > 0
            && total_volume < self.min_volume_per_account * self.accounts.len() as i64
        {
            return Err(TaskError::InvalidState(
                "total volume cannot satisfy account minimum volume",
            ));
        }

        let common_denominator: u128 = self
            .accounts
            .iter()
            .map(|allocation| allocation.ratio.denominator as u128)
            .product();
        let weights: Vec<u128> = self
            .accounts
            .iter()
            .map(|allocation| {
                allocation.ratio.numerator as u128
                    * (common_denominator / allocation.ratio.denominator as u128)
            })
            .collect();
        let total_weight: u128 = weights.iter().sum();
        let mut rows: Vec<(usize, i64, u128)> = self
            .accounts
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let weighted = total_volume as u128 * weights[index];
                let whole = (weighted / total_weight) as i64;
                let remainder = weighted % total_weight;
                (index, whole, remainder)
            })
            .collect();

        let mut allocated: i64 = rows.iter().map(|(_, volume, _)| *volume).sum();
        rows.sort_by(|left, right| {
            right
                .2
                .cmp(&left.2)
                .then_with(|| left.0.cmp(&right.0))
        });
        for row in rows.iter_mut() {
            if allocated >= total_volume {
                break;
            }
            row.1 += 1;
            allocated += 1;
        }
        rows.sort_by_key(|row| row.0);

        if self.min_volume_per_account > 0
            && rows.iter().any(|(_, volume, _)| *volume < self.min_volume_per_account)
        {
            return Err(TaskError::InvalidState(
                "total volume cannot satisfy account minimum volume",
            ));
        }

        Ok(AccountAllocationPlan {
            allocations: rows
                .into_iter()
                .map(|(index, volume, _)| AllocatedAccountOrder {
                    account_id: self.accounts[index].account_id.clone(),
                    volume,
                })
                .collect(),
        })
    }
}

impl AccountGroupBuilder {
    #[must_use]
    pub fn add(mut self, account_id: impl Into<String>, ratio: Ratio) -> Self {
        self.accounts.push(AccountAllocation {
            account_id: account_id.into(),
            ratio,
        });
        self
    }

    #[must_use]
    pub fn min_volume_per_account(mut self, min_volume: i64) -> Self {
        self.min_volume_per_account = min_volume;
        self
    }

    pub fn build(self) -> Result<AccountGroup> {
        if self.accounts.is_empty() {
            return Err(TaskError::InvalidState("account group cannot be empty"));
        }
        if self.min_volume_per_account < 0 {
            return Err(TaskError::InvalidState(
                "account minimum volume cannot be negative",
            ));
        }
        let mut seen = HashSet::new();
        for account in &self.accounts {
            if account.account_id.is_empty() {
                return Err(TaskError::InvalidState("account id cannot be empty"));
            }
            if !seen.insert(account.account_id.as_str()) {
                return Err(TaskError::InvalidState(
                    "duplicate account id in account group",
                ));
            }
        }
        Ok(AccountGroup {
            accounts: self.accounts,
            min_volume_per_account: self.min_volume_per_account,
        })
    }
}

impl AccountAllocationPlan {
    #[must_use]
    pub fn allocations(&self) -> &[AllocatedAccountOrder] {
        &self.allocations
    }
}

impl AllocatedAccountOrder {
    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub fn volume(&self) -> i64 {
        self.volume
    }
}
```

Update `lib.rs`:

```rust
mod account_group;

pub use account_group::{
    AccountAllocation, AccountAllocationPlan, AccountGroup, AccountGroupBuilder,
    AllocatedAccountOrder, Ratio,
};
```

- [ ] **Step 4: Run allocation tests**

Run:

```bash
cargo test -p tqsdk-task --test account_group account_group_ -- --nocapture
```

Expected: the three allocation tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/tqsdk-task/src/account_group.rs crates/tqsdk-task/src/lib.rs crates/tqsdk-task/tests/account_group.rs
git commit -m "feat: add account group allocation model"
```

## Task 2: Multi-Account Order Builder

**Files:**
- Modify: `crates/tqsdk-task/src/account_group.rs`
- Modify: `crates/tqsdk-task/src/error.rs`
- Modify: `crates/tqsdk-task/src/host.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`
- Test: `crates/tqsdk-task/tests/account_group.rs`

- [ ] **Step 1: Add submission tests**

Append these tests to `crates/tqsdk-task/tests/account_group.rs`. Copy the existing
`seeded_host()`, `transport_payload()`, and `seed_account_position_quote()`
helpers from `tests/execution_group.rs` into this test file.

```rust
use tqsdk_task::{
    AccountFailurePolicy, AccountGroup, Ratio, RiskEngine, RiskRejection, TaskError, TaskHost,
};

#[tokio::test(flavor = "current_thread")]
async fn multi_account_order_submits_allocated_orders_with_deterministic_ids() {
    let mut host = seeded_host();
    let accounts = AccountGroup::builder()
        .add("sim-a", Ratio::new(7, 10).unwrap())
        .add("sim-b", Ratio::new(3, 10).unwrap())
        .min_volume_per_account(1)
        .build()
        .unwrap();

    let ticket = host
        .multi_account_order(accounts)
        .client_group_id("alloc-au-001")
        .max_unhedged(std::time::Duration::from_secs(2))
        .on_account_failed(AccountFailurePolicy::ReportExposure)
        .buy_open("SHFE.au2602", 10)
        .limit(480.0)
        .send_once()
        .await
        .unwrap();

    assert_eq!(ticket.group_id(), "alloc-au-001");
    assert_eq!(ticket.orders().len(), 2);
    assert_eq!(ticket.orders()[0].account_id(), "sim-a");
    assert_eq!(ticket.orders()[0].client_order_id(), "alloc-au-001:acct:0");
    assert_eq!(ticket.orders()[0].intent().volume, 7);
    assert_eq!(ticket.orders()[1].account_id(), "sim-b");
    assert_eq!(ticket.orders()[1].client_order_id(), "alloc-au-001:acct:1");
    assert_eq!(ticket.orders()[1].intent().volume, 3);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 2);

    let first = transport_payload(&dispatches[0].request);
    assert_eq!(first["aid"], "insert_order");
    assert_eq!(first["user_id"], "sim-a");
    assert_eq!(first["order_id"], "alloc-au-001:acct:0");
    assert_eq!(first["volume"], 7);

    let second = transport_payload(&dispatches[1].request);
    assert_eq!(second["aid"], "insert_order");
    assert_eq!(second["user_id"], "sim-b");
    assert_eq!(second["order_id"], "alloc-au-001:acct:1");
    assert_eq!(second["volume"], 3);
}

#[tokio::test(flavor = "current_thread")]
async fn multi_account_order_preflights_all_accounts_before_dispatch() {
    let mut host = seeded_host().with_risk(RiskEngine::new().max_price_deviation(10.0));
    seed_account_position_quote(&host, "sim-a", "SHFE.au2602", 2_000.0, 0, 480.0);
    seed_account_position_quote(&host, "sim-b", "SHFE.au2602", 2_000.0, 0, 480.0);

    let accounts = AccountGroup::builder()
        .add("sim-a", Ratio::new(1, 2).unwrap())
        .add("sim-b", Ratio::new(1, 2).unwrap())
        .build()
        .unwrap();

    let err = host
        .multi_account_order(accounts)
        .client_group_id("alloc-risk-001")
        .buy_open("SHFE.au2602", 2)
        .limit(500.5)
        .send_once()
        .await
        .unwrap_err();

    assert_eq!(
        err,
        TaskError::RiskRejected(RiskRejection::PriceDeviationExceeded {
            symbol: "SHFE.au2602".to_string(),
            limit_price: 500.5,
            reference_price: 480.0,
            max_abs_deviation: 10.0,
        })
    );
    assert!(host.api().handle_for_test().drain_dispatches().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn multi_account_order_retry_reuses_existing_account_intents() {
    let mut host = seeded_host();
    let accounts = AccountGroup::builder()
        .add("sim-a", Ratio::new(1, 2).unwrap())
        .add("sim-b", Ratio::new(1, 2).unwrap())
        .build()
        .unwrap();

    host.multi_account_order(accounts.clone())
        .client_group_id("alloc-retry-001")
        .sell_open("SHFE.au2602", 4)
        .limit(481.0)
        .send_once()
        .await
        .unwrap();
    assert_eq!(host.api().handle_for_test().drain_dispatches().unwrap().len(), 2);

    let retry = host
        .multi_account_order(accounts)
        .client_group_id("alloc-retry-001")
        .sell_open("SHFE.au2602", 4)
        .limit(481.0)
        .send_once()
        .await
        .unwrap();

    assert!(retry.orders().iter().all(|order| !order.ticket().was_submitted()));
    assert!(host.api().handle_for_test().drain_dispatches().unwrap().is_empty());
}
```

- [ ] **Step 2: Run submission tests and verify failure**

Run:

```bash
cargo test -p tqsdk-task --test account_group multi_account_order_ -- --nocapture
```

Expected: compile failure because `TaskHost::multi_account_order` and builder types are not defined.

- [ ] **Step 3: Implement builder and host entrypoints**

Add this variant to `TaskError` in `error.rs`, plus matching `Display` and `source()` arms:

```rust
MultiAccountPartialSubmit {
    group_id: String,
    submitted_accounts: usize,
    total_accounts: usize,
    reason: &'static str,
},
```

The `Display` arm should render:

```rust
Self::MultiAccountPartialSubmit {
    group_id,
    submitted_accounts,
    total_accounts,
    reason,
} => write!(
    f,
    "multi-account partial submit group_id={group_id} submitted_accounts={submitted_accounts} total_accounts={total_accounts}: {reason}"
),
```

Extend `account_group.rs` with:

```rust
use std::time::Duration;

use tqsdk_core::{TradeDirection, TradeOffset};
use tqsdk_wait::OrderTicket;

use crate::{TaskHost, TaskOrderIntent};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountFailurePolicy {
    ReportExposure,
    FlattenFilledAccounts,
}

pub struct MultiAccountOrderBuilder<'a> {
    host: &'a mut TaskHost,
    accounts: AccountGroup,
    group_id: Option<String>,
    max_unhedged: Option<Duration>,
    failure_policy: AccountFailurePolicy,
}

pub struct MultiAccountOrderDraft<'a> {
    builder: MultiAccountOrderBuilder<'a>,
    symbol: String,
    direction: TradeDirection,
    offset: TradeOffset,
    total_volume: i64,
    limit_price: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct MultiAccountOrderTicket {
    group_id: String,
    max_unhedged: Option<Duration>,
    failure_policy: AccountFailurePolicy,
    orders: Vec<MultiAccountOrderLegTicket>,
}

#[derive(Debug, Clone)]
pub struct MultiAccountOrderLegTicket {
    account_id: String,
    client_order_id: String,
    intent: TaskOrderIntent,
    ticket: OrderTicket,
}

impl<'a> MultiAccountOrderBuilder<'a> {
    pub(crate) fn new(host: &'a mut TaskHost, accounts: AccountGroup) -> Self {
        Self {
            host,
            accounts,
            group_id: None,
            max_unhedged: None,
            failure_policy: AccountFailurePolicy::ReportExposure,
        }
    }

    #[must_use]
    pub fn client_group_id(mut self, group_id: impl Into<String>) -> Self {
        self.group_id = Some(group_id.into());
        self
    }

    #[must_use]
    pub fn max_unhedged(mut self, duration: Duration) -> Self {
        self.max_unhedged = Some(duration);
        self
    }

    #[must_use]
    pub fn on_account_failed(mut self, policy: AccountFailurePolicy) -> Self {
        self.failure_policy = policy;
        self
    }

    #[must_use]
    pub fn buy_open(self, symbol: impl AsRef<str>, total_volume: i64) -> MultiAccountOrderDraft<'a> {
        self.intent(symbol, TradeDirection::Buy, TradeOffset::Open, total_volume)
    }

    #[must_use]
    pub fn sell_open(self, symbol: impl AsRef<str>, total_volume: i64) -> MultiAccountOrderDraft<'a> {
        self.intent(symbol, TradeDirection::Sell, TradeOffset::Open, total_volume)
    }

    fn intent(
        self,
        symbol: impl AsRef<str>,
        direction: TradeDirection,
        offset: TradeOffset,
        total_volume: i64,
    ) -> MultiAccountOrderDraft<'a> {
        MultiAccountOrderDraft {
            builder: self,
            symbol: symbol.as_ref().to_owned(),
            direction,
            offset,
            total_volume,
            limit_price: None,
        }
    }
}

impl MultiAccountOrderDraft<'_> {
    #[must_use]
    pub fn limit(mut self, price: f64) -> Self {
        self.limit_price = Some(price);
        self
    }

    pub async fn send_once(self) -> Result<MultiAccountOrderTicket> {
        submit_multi_account_order(self).await
    }
}
```

Add `submit_multi_account_order` in the same file:

```rust
async fn submit_multi_account_order(draft: MultiAccountOrderDraft<'_>) -> Result<MultiAccountOrderTicket> {
    let group_id = draft
        .builder
        .group_id
        .clone()
        .filter(|value| !value.is_empty())
        .ok_or(TaskError::InvalidState("multi-account group id is required"))?;
    if draft.builder.failure_policy == AccountFailurePolicy::FlattenFilledAccounts {
        return Err(TaskError::Unsupported(
            "automatic multi-account flatten policy is not implemented",
        ));
    }
    let limit_price = draft
        .limit_price
        .ok_or(TaskError::InvalidState("limit price is required"))?;
    let allocation_plan = draft.builder.accounts.allocate(draft.total_volume)?;
    let mut intents = Vec::new();
    for allocation in allocation_plan.allocations() {
        intents.push(TaskOrderIntent {
            account_id: allocation.account_id().to_owned(),
            symbol: draft.symbol.clone(),
            direction: draft.direction,
            offset: Some(draft.offset),
            volume: allocation.volume(),
            limit_price: Some(limit_price),
        });
    }
    for intent in &intents {
        draft.builder.host.preflight_task_order(intent)?;
    }

    let mut orders = Vec::new();
    let total_accounts = intents.len();
    for (index, intent) in intents.into_iter().enumerate() {
        let client_order_id = format!("{group_id}:acct:{index}");
        match draft
            .builder
            .host
            .submit_prechecked_task_order_once(intent.clone(), client_order_id.clone())
            .await
        {
            Ok(ticket) => orders.push(MultiAccountOrderLegTicket {
                account_id: intent.account_id.clone(),
                client_order_id,
                intent,
                ticket,
            }),
            Err(error) if orders.is_empty() => return Err(error),
            Err(_) => {
                return Err(TaskError::MultiAccountPartialSubmit {
                    group_id,
                    submitted_accounts: orders.len(),
                    total_accounts,
                    reason: "account submit failed after group preflight",
                });
            }
        }
    }
    Ok(MultiAccountOrderTicket {
        group_id,
        max_unhedged: draft.builder.max_unhedged,
        failure_policy: draft.builder.failure_policy,
        orders,
    })
}
```

Add accessors:

```rust
impl MultiAccountOrderTicket {
    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    #[must_use]
    pub fn orders(&self) -> &[MultiAccountOrderLegTicket] {
        &self.orders
    }
}

impl MultiAccountOrderLegTicket {
    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub fn client_order_id(&self) -> &str {
        &self.client_order_id
    }

    #[must_use]
    pub fn intent(&self) -> &TaskOrderIntent {
        &self.intent
    }

    #[must_use]
    pub fn ticket(&self) -> &OrderTicket {
        &self.ticket
    }
}
```

Update `host.rs`:

```rust
use crate::account_group::{AccountGroup, AccountGroupBuilder, MultiAccountOrderBuilder};

#[must_use]
pub fn account_group(&self) -> AccountGroupBuilder {
    AccountGroup::builder()
}

#[must_use]
pub fn multi_account_order(&mut self, accounts: AccountGroup) -> MultiAccountOrderBuilder<'_> {
    MultiAccountOrderBuilder::new(self, accounts)
}
```

Update `lib.rs` exports:

```rust
pub use account_group::{
    AccountAllocation, AccountAllocationPlan, AccountGroup, AccountGroupBuilder,
    AllocatedAccountOrder, MultiAccountOrderBuilder, MultiAccountOrderDraft,
    AccountFailurePolicy, MultiAccountOrderLegTicket, MultiAccountOrderTicket, Ratio,
};
```

- [ ] **Step 4: Run submission tests**

Run:

```bash
cargo test -p tqsdk-task --test account_group multi_account_order_ -- --nocapture
```

Expected: the three submission tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/tqsdk-task/src/account_group.rs crates/tqsdk-task/src/error.rs crates/tqsdk-task/src/host.rs crates/tqsdk-task/src/lib.rs crates/tqsdk-task/tests/account_group.rs
git commit -m "feat: submit multi-account task orders"
```

## Task 3: Per-Account Outcome Reporting

**Files:**
- Modify: `crates/tqsdk-task/src/account_group.rs`
- Test: `crates/tqsdk-task/tests/account_group.rs`

- [ ] **Step 1: Add outcome tests**

Append these tests. Copy `OrderStatusSeed` and `seed_order_status_commit()` from `tests/execution_group.rs` into `tests/account_group.rs`.

```rust
use tqsdk_task::MultiAccountOrderOutcome;

#[tokio::test(flavor = "current_thread")]
async fn multi_account_order_reports_all_accounts_filled() {
    let mut host = seeded_host();
    let accounts = AccountGroup::builder()
        .add("sim-a", Ratio::new(1, 2).unwrap())
        .add("sim-b", Ratio::new(1, 2).unwrap())
        .build()
        .unwrap();

    let ticket = host
        .multi_account_order(accounts)
        .client_group_id("alloc-filled-001")
        .buy_open("SHFE.au2602", 4)
        .limit(480.0)
        .send_once()
        .await
        .unwrap();
    host.api().handle_for_test().drain_dispatches().unwrap();

    seed_order_status_commit(&host, OrderStatusSeed {
        account_id: "sim-a",
        symbol: "SHFE.au2602",
        order_id: "alloc-filled-001:acct:0",
        direction: "BUY",
        offset: "OPEN",
        volume_orign: 2,
        volume_left: 0,
        status: "FINISHED",
    });
    seed_order_status_commit(&host, OrderStatusSeed {
        account_id: "sim-b",
        symbol: "SHFE.au2602",
        order_id: "alloc-filled-001:acct:1",
        direction: "BUY",
        offset: "OPEN",
        volume_orign: 2,
        volume_left: 0,
        status: "FINISHED",
    });

    let outcome = ticket.outcome(host.api()).unwrap().unwrap();
    match outcome {
        MultiAccountOrderOutcome::AllFilled { accounts } => {
            assert_eq!(accounts.len(), 2);
            assert!(accounts.iter().all(|account| account.filled_volume == account.requested_volume));
        }
        other => panic!("expected all-filled outcome, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn multi_account_order_reports_mixed_account_outcome() {
    let mut host = seeded_host();
    let accounts = AccountGroup::builder()
        .add("sim-a", Ratio::new(1, 2).unwrap())
        .add("sim-b", Ratio::new(1, 2).unwrap())
        .build()
        .unwrap();

    let ticket = host
        .multi_account_order(accounts)
        .client_group_id("alloc-mixed-001")
        .buy_open("SHFE.au2602", 4)
        .limit(480.0)
        .send_once()
        .await
        .unwrap();
    host.api().handle_for_test().drain_dispatches().unwrap();

    seed_order_status_commit(&host, OrderStatusSeed {
        account_id: "sim-a",
        symbol: "SHFE.au2602",
        order_id: "alloc-mixed-001:acct:0",
        direction: "BUY",
        offset: "OPEN",
        volume_orign: 2,
        volume_left: 0,
        status: "FINISHED",
    });
    seed_order_status_commit(&host, OrderStatusSeed {
        account_id: "sim-b",
        symbol: "SHFE.au2602",
        order_id: "alloc-mixed-001:acct:1",
        direction: "BUY",
        offset: "OPEN",
        volume_orign: 2,
        volume_left: 2,
        status: "FINISHED",
    });

    let outcome = ticket.outcome(host.api()).unwrap().unwrap();
    match outcome {
        MultiAccountOrderOutcome::NeedsAttention { filled_accounts, unfilled_accounts, accounts } => {
            assert_eq!(filled_accounts, vec!["sim-a".to_string()]);
            assert_eq!(unfilled_accounts, vec!["sim-b".to_string()]);
            assert_eq!(accounts.len(), 2);
        }
        other => panic!("expected needs-attention outcome, got {other:?}"),
    }
}
```

- [ ] **Step 2: Run outcome tests and verify failure**

Run:

```bash
cargo test -p tqsdk-task --test account_group multi_account_order_reports_ -- --nocapture
```

Expected: compile failure because outcome types and methods are not defined.

- [ ] **Step 3: Implement reports and outcome classification**

Add these public types:

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum MultiAccountOrderState {
    Unknown,
    CommandPending,
    Live,
    Filled,
    PartiallyFilled { filled_volume: i64, volume_left: i64 },
    Cancelled,
    Rejected,
    Failed,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MultiAccountOrderReport {
    pub account_id: String,
    pub client_order_id: String,
    pub symbol: String,
    pub requested_volume: i64,
    pub filled_volume: i64,
    pub volume_left: i64,
    pub state: MultiAccountOrderState,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MultiAccountOrderOutcome {
    AllFilled { accounts: Vec<MultiAccountOrderReport> },
    Cancelled { accounts: Vec<MultiAccountOrderReport> },
    Rejected { accounts: Vec<MultiAccountOrderReport> },
    Failed { accounts: Vec<MultiAccountOrderReport> },
    NeedsAttention {
        filled_accounts: Vec<String>,
        unfilled_accounts: Vec<String>,
        accounts: Vec<MultiAccountOrderReport>,
    },
}
```

Implement `MultiAccountOrderTicket::status(api)`, `outcome(api)`, and `wait_finished(host, deadline)` by following the shape already used by `ExecutionGroupTicket`. Map wait-layer `OrderTicketState` to account-level state; classify all filled as `AllFilled`, all terminal unfilled as `Cancelled` / `Rejected` / `Failed`, and mixed terminal with at least one fill as `NeedsAttention`.

Export:

```rust
pub use account_group::{
    MultiAccountOrderOutcome, MultiAccountOrderReport, MultiAccountOrderState,
};
```

- [ ] **Step 4: Run outcome tests**

Run:

```bash
cargo test -p tqsdk-task --test account_group multi_account_order_reports_ -- --nocapture
```

Expected: outcome tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/tqsdk-task/src/account_group.rs crates/tqsdk-task/src/lib.rs crates/tqsdk-task/tests/account_group.rs
git commit -m "feat: report multi-account order outcomes"
```

## Task 4: Formal S13 Example And Documentation

**Files:**
- Create: `crates/tqsdk-task/examples/api_contract_s13_multi_account_ordering.rs`
- Modify: `docs/scenarios/api_gaps/api_contract_s13_multi_account_ordering.rs`
- Modify: `docs/reviews/public-api-scenario-review.md`
- Modify: `docs/scenarios/user-layer-iteration-plan.md`
- Modify: `docs/architecture/api-task.md`
- Modify: `crates/tqsdk-task/README.md`

- [ ] **Step 1: Create the formal S13 example**

Create `crates/tqsdk-task/examples/api_contract_s13_multi_account_ordering.rs`:

```rust
//! Scenario: 多账户下单
//!
//! User goal:
//! - 同一策略按比例向多个账户下单
//! - 每个账户状态隔离
//! - 汇总执行结果
//!
//! API contract:
//! - 多账户是 typed account group，而不是业务代码里的字符串循环
//! - 比例拆单、最小手数和 deterministic client order id 由 task 层处理
//! - 每个账户订单、成交和错误隔离可追踪
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 在业务代码里循环多个 `insert_order`
//! - 用共享 `HashMap` 拼账户执行状态
//! - 字符串判断订单状态或错误类型
//! - `RuntimeCommand::Trade`
//!
//! Regression signal:
//! - 一个账户拒单导致其他账户 outcome 无法解释
//! - 比例拆单、尾差和风控散落在用户代码
//! - 多账户状态相互污染
//!
//! Review questions:
//! - 当前 API 是否自然表达多账户执行？
//! - 是否有状态隔离和资金安全风险？
//! - 多账户能力是否留在 task 层，而不是下沉到 core/session/wait？

use std::time::Duration;

use tqsdk_task::{AccountFailurePolicy, MultiAccountOrderOutcome, Ratio, TaskHost};
use tqsdk_wait::TqApi;

#[allow(dead_code)]
async fn run(mut host: TaskHost) -> tqsdk_task::Result<()> {
    let accounts = host
        .account_group()
        .add("sim-a", Ratio::new(7, 10)?)
        .add("sim-b", Ratio::new(3, 10)?)
        .min_volume_per_account(1)
        .build()?;

    let ticket = host
        .multi_account_order(accounts)
        .client_group_id("alloc-au-001")
        .max_unhedged(Duration::from_secs(2))
        .on_account_failed(AccountFailurePolicy::ReportExposure)
        .buy_open("SHFE.au2602", 10)
        .limit(480.0)
        .send_once()
        .await?;

    match ticket.wait_finished(&mut host, None).await? {
        MultiAccountOrderOutcome::AllFilled { accounts } => {
            for account in accounts {
                println!(
                    "{} filled {}/{}",
                    account.account_id, account.filled_volume, account.requested_volume
                );
            }
        }
        MultiAccountOrderOutcome::NeedsAttention {
            filled_accounts,
            unfilled_accounts,
            ..
        } => {
            println!("filled={filled_accounts:?}, unfilled={unfilled_accounts:?}");
        }
        other => {
            println!("terminal multi-account outcome: {other:?}");
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> tqsdk_task::Result<()> {
    let api = TqApi::builder()
        .with_account_from_env()
        .with_tqkq_future_trade_from_env()
        .build()
        .await?;
    let host = TaskHost::new(api);
    run(host).await
}
```

- [ ] **Step 2: Update docs**

Update `docs/reviews/public-api-scenario-review.md` row 13:

```markdown
| 13. 多账户下单 | 勉强 | 中 | 无 | 无 | 中 | 中 | 局部重构 | `crates/tqsdk-task/examples/api_contract_s13_multi_account_ordering.rs`; `docs/scenarios/api_gaps/api_contract_s13_multi_account_ordering.rs`; `AccountGroup`; `MultiAccountOrderTicket`; advanced failure policy/resume/audit remains gap |
```

Update `docs/scenarios/api_gaps/api_contract_s13_multi_account_ordering.rs` so its `API gap` section says:

```rust
//! API gap:
//! foundation 已支持 typed account group、比例拆单、per-account order ticket 和
//! per-account outcome。仍未支持自动补单/对冲、跨账户 TargetPos 编排、
//! 跨进程 resume 和审计日志落库。
```

Update `docs/scenarios/user-layer-iteration-plan.md` under P1 执行层抽象:

```markdown
- `AccountGroup` foundation 支持 typed account group、比例拆单、全账户 preflight、
  session-scoped retry idempotency 和 per-account outcome report。
```

Update `docs/architecture/api-task.md` current landed list:

```markdown
- `AccountGroup` / `MultiAccountOrderTicket` 多账户执行 foundation
```

Update `crates/tqsdk-task/README.md` with a short account group example using the same API as the formal example.

- [ ] **Step 3: Check the formal example**

Run:

```bash
cargo check -p tqsdk-task --example api_contract_s13_multi_account_ordering
```

Expected: the example compiles.

- [ ] **Step 4: Commit**

```bash
git add crates/tqsdk-task/examples/api_contract_s13_multi_account_ordering.rs docs/scenarios/api_gaps/api_contract_s13_multi_account_ordering.rs docs/reviews/public-api-scenario-review.md docs/scenarios/user-layer-iteration-plan.md docs/architecture/api-task.md crates/tqsdk-task/README.md
git commit -m "docs: promote multi-account ordering scenario"
```

## Task 5: Full Validation

**Files:**
- No code files unless validation uncovers a defect.

- [ ] **Step 1: Run workspace example check**

Run:

```bash
cargo check --workspace --examples
```

Expected: success.

- [ ] **Step 2: Run workspace tests**

Run:

```bash
cargo test --workspace
```

Expected: success.

- [ ] **Step 3: Run clippy**

Run:

```bash
cargo clippy --workspace --examples --all-targets -- -D warnings
```

Expected: success.

- [ ] **Step 4: Feature flag note**

No feature flags are planned to change. If implementation changes any feature flags, also run:

```bash
cargo check --workspace --no-default-features
cargo check --workspace --all-features --examples
```

Expected: success.

- [ ] **Step 5: Commit validation fixes only if needed**

If validation required code or docs changes:

```bash
git add <changed-files>
git commit -m "fix: stabilize multi-account order validation"
```

If validation passes without changes, do not create an empty commit.

## Self-Review

Spec coverage:

- S13 typed account group is covered by `AccountGroup`, `Ratio`, and formal example.
- Ratio allocation and minimum hand volume are covered by allocation tests.
- Per-account outcome and isolation are covered by report tests.
- Risk and task ownership preflight happen before dispatch through existing `TaskHost` helpers.
- The plan does not change crate boundaries or runtime commit semantics.

Placeholder scan:

- No task uses an unspecified file path.
- No task says to add generic tests without concrete test code.
- Remaining gaps are explicitly listed as non-goals and kept in the gap sketch.

Type consistency:

- Public names used by tests, examples, exports, and docs are aligned:
  `Ratio`, `AccountGroup`, `AccountFailurePolicy`, `MultiAccountOrderBuilder`, `MultiAccountOrderTicket`,
  `MultiAccountOrderLegTicket`, `MultiAccountOrderOutcome`, `MultiAccountOrderReport`,
  and `MultiAccountOrderState`.
