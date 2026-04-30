# Task Execution Group Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the next scenario-driven Public API batch for `tqsdk-task`: a typed two-leg `ExecutionGroup` foundation for S12 跨合约套利, with all-leg preflight, group-level idempotent order intents, and typed outcome/exposure reporting.

**Architecture:** Keep `tqsdk-core`, `tqsdk-session`, `tqsdk-wait`, and `tqsdk-stream` boundaries unchanged. Add a task-layer execution group that reuses `TaskOrderIntent`, `RiskEngine`, existing ownership guard, `tqsdk_wait::OrderTicket`, and the shared runtime state tree; do not create a second order state tree, private revision, or provider-specific protocol path. This batch reports desynchronized-leg exposure but does not auto-submit hedge/flatten orders yet.

**Tech Stack:** Rust, Tokio, `tqsdk-task`, `tqsdk-wait`, existing runtime reader/refs, existing task integration-test style, cargo workspace examples/tests/clippy.

---

## Scope

This batch implements the foundation needed to move S12 from `无法表达` to `勉强`:

- S12 跨合约套利 foundation:
  - typed `ExecutionGroupBuilder`;
  - typed two-leg order intents under one client group id;
  - all-leg ownership/risk/local validation before any leg dispatch;
  - idempotent retry through per-leg `ClientOrderId`;
  - `ExecutionGroupTicket` with group status/outcome;
  - typed `ExecutionExposure` when one leg is filled and another leg is rejected/failed/cancelled.

This batch explicitly does not implement:

- automated hedge/flatten order submission;
- time-driven cancel/replace loops;
- multi-account allocation;
- portfolio margin what-if;
- a full `StrategyHost`;
- a public fake broker/test harness.

S13 多账户下单 and S24 最小可测试策略 remain separate future batches. Do not expand this plan to cover `AccountGroup`, allocation policy, or `StrategyTestHarness`.

## Public API Target

The target user code for this batch is:

```rust
use std::time::Duration;
use tqsdk_task::{ExecutionGroupOutcome, HedgePolicy, TaskHost};

let group = host
    .execution_group("sim")
    .client_group_id("spread-entry-001")
    .max_unhedged(Duration::from_secs(2))
    .on_leg_failed(HedgePolicy::ReportExposure)
    .leg("SHFE.au2602")
    .buy_open(1)
    .limit(480.0)
    .leg("SHFE.ag2602")
    .sell_open(15)
    .limit(6500.0)
    .send_once()
    .await?;

let outcome = group.wait_finished(&mut host, tokio::time::Instant::now() + Duration::from_secs(30)).await?;
match outcome {
    ExecutionGroupOutcome::AllFilled { legs } => {
        println!("spread filled with {} legs", legs.len());
    }
    ExecutionGroupOutcome::NeedsHedge { exposure, legs } => {
        println!("manual hedge required exposure={exposure:?} legs={legs:?}");
    }
    ExecutionGroupOutcome::Rejected { legs }
    | ExecutionGroupOutcome::Failed { legs }
    | ExecutionGroupOutcome::Cancelled { legs } => {
        println!("spread did not complete legs={legs:?}");
    }
}
```

Important API limits:

- `HedgePolicy::ReportExposure` is the only supported policy in this batch.
- `HedgePolicy::FlattenFilledLegs` may be declared as a future enum variant only if it returns `TaskError::Unsupported("automatic hedge policy is not implemented")` before any dispatch.
- `wait_finished` may return `NeedsHedge`; it must not hide the risk by returning generic `Failed`.

## File Map

- Create `crates/tqsdk-task/src/execution_group.rs`
  - Owns `ExecutionGroupBuilder`, leg builder states, `ExecutionGroupTicket`, `ExecutionGroupStatus`, `ExecutionGroupOutcome`, `ExecutionLegReport`, `ExecutionLegState`, `ExecutionExposure`, and `HedgePolicy`.
  - Delegates actual leg submission to `TaskHost` using existing `TaskOrderIntent` and `tqsdk_wait::OrderTicket`.

- Modify `crates/tqsdk-task/src/host.rs`
  - Add `TaskHost::execution_group(account_id)`.
  - Refactor task order validation into reusable `preflight_task_order`.
  - Add `submit_prechecked_task_order_once` for group dispatch after all legs pass preflight.
  - Keep `orders(...).send_once(...)` behavior unchanged.

- Modify `crates/tqsdk-task/src/error.rs`
  - Add group-level validation errors:
    - missing group id;
    - insufficient legs;
    - unsupported hedge policy;
    - group submission failure after partial local dispatch, with submitted/total leg counts.

- Modify `crates/tqsdk-task/src/lib.rs`
  - Export execution group public types.

- Add tests in `crates/tqsdk-task/tests/execution_group.rs`
  - Cover typed builder payloads, all-leg preflight, risk rejection without partial dispatch, same group id retry, mixed filled/rejected outcome, and `wait_finished` timeout/exposure behavior.

- Create `crates/tqsdk-task/examples/api_contract_s12_spread_arbitrage.rs`
  - Formal scenario contract example for the foundation.
  - It must be honest that automated hedge execution remains future work.

- Modify `docs/scenarios/api_gaps/api_contract_s12_spread_arbitrage.rs`
  - Narrow the gap to automated hedge/flatten, timed cancel/replace, and richer hedge policies.

- Modify `docs/reviews/public-api-scenario-review.md`
  - Move S12 from `无法表达` to `勉强` only if the formal example compiles cleanly and does not require manual leg state maps.

- Modify `docs/scenarios/user-layer-iteration-plan.md`
  - Mark execution group foundation as landed.
  - Keep S13 and S24 as future batches.

- Modify `docs/architecture/api-task.md` and `crates/tqsdk-task/README.md`
  - Document execution group placement and the explicit non-goal of automatic hedging in this batch.

---

### Task 1: Execution Group Types And Builder Surface

**Files:**
- Create: `crates/tqsdk-task/src/execution_group.rs`
- Modify: `crates/tqsdk-task/src/host.rs`
- Modify: `crates/tqsdk-task/src/error.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`
- Test: `crates/tqsdk-task/tests/execution_group.rs`

- [ ] **Step 1: Write the failing builder test**

Add `crates/tqsdk-task/tests/execution_group.rs` with the first two tests and shared helpers:

```rust
use std::time::Duration;

use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, OutboundFrame, OutboundRequest,
    ProtocolDomain, RuntimeHandle, RuntimeInput,
};
use tqsdk_session::SessionClient;
use tqsdk_task::{ExecutionGroupOutcome, HedgePolicy, TaskError, TaskHost};
use tqsdk_wait::TqApi;

fn seeded_host() -> TaskHost {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    let handle = RuntimeHandle::with_adapters(adapters);
    let session = SessionClient::new_for_test_with_handle(handle);
    TaskHost::new(TqApi::new(session))
}

fn transport_payload(request: &OutboundRequest) -> serde_json::Value {
    match request {
        OutboundRequest::Transport(OutboundFrame::Text(text)) => {
            serde_json::from_str(text).expect("transport frame should contain valid json payload")
        }
        OutboundRequest::Transport(OutboundFrame::Binary(bytes)) => serde_json::from_slice(bytes)
            .expect("transport frame should contain valid json payload"),
        other => panic!("expected transport request, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn execution_group_submits_two_typed_legs_under_one_group_id() {
    let mut host = seeded_host();

    let group = host
        .execution_group("sim")
        .client_group_id("spread-entry-001")
        .max_unhedged(Duration::from_secs(2))
        .on_leg_failed(HedgePolicy::ReportExposure)
        .leg("SHFE.au2602")
        .buy_open(1)
        .limit(480.0)
        .leg("SHFE.ag2602")
        .sell_open(15)
        .limit(6500.0)
        .send_once()
        .await
        .unwrap();

    assert_eq!(group.group_id(), "spread-entry-001");
    assert_eq!(group.legs().len(), 2);
    assert_eq!(group.legs()[0].client_order_id(), "spread-entry-001:leg:0");
    assert_eq!(group.legs()[1].client_order_id(), "spread-entry-001:leg:1");
    assert!(group.legs()[0].ticket().was_submitted());
    assert!(group.legs()[1].ticket().was_submitted());

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 2);

    let leg0 = transport_payload(&dispatches[0].request);
    assert_eq!(leg0["aid"], "insert_order");
    assert_eq!(leg0["user_id"], "sim");
    assert_eq!(leg0["order_id"], "spread-entry-001:leg:0");
    assert_eq!(leg0["exchange_id"], "SHFE");
    assert_eq!(leg0["instrument_id"], "au2602");
    assert_eq!(leg0["direction"], "BUY");
    assert_eq!(leg0["offset"], "OPEN");
    assert_eq!(leg0["volume"], 1);
    assert_eq!(leg0["limit_price"], 480.0);

    let leg1 = transport_payload(&dispatches[1].request);
    assert_eq!(leg1["aid"], "insert_order");
    assert_eq!(leg1["user_id"], "sim");
    assert_eq!(leg1["order_id"], "spread-entry-001:leg:1");
    assert_eq!(leg1["exchange_id"], "SHFE");
    assert_eq!(leg1["instrument_id"], "ag2602");
    assert_eq!(leg1["direction"], "SELL");
    assert_eq!(leg1["offset"], "OPEN");
    assert_eq!(leg1["volume"], 15);
    assert_eq!(leg1["limit_price"], 6500.0);
}

#[tokio::test(flavor = "current_thread")]
async fn execution_group_rejects_missing_group_id_before_dispatch() {
    let mut host = seeded_host();

    let err = host
        .execution_group("sim")
        .leg("SHFE.au2602")
        .buy_open(1)
        .limit(480.0)
        .leg("SHFE.ag2602")
        .sell_open(15)
        .limit(6500.0)
        .send_once()
        .await
        .unwrap_err();

    assert_eq!(err, TaskError::InvalidState("execution group id is required"));
    assert!(host.api().handle_for_test().drain_dispatches().unwrap().is_empty());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p tqsdk-task --test execution_group execution_group_ -- --nocapture
```

Expected: FAIL because `TaskHost::execution_group`, `HedgePolicy`, and group types do not exist.

- [ ] **Step 3: Add the error variants**

Modify `crates/tqsdk-task/src/error.rs`:

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum TaskError {
    Wait(tqsdk_wait::WaitFacadeError),
    Session(tqsdk_session::SessionFacadeError),
    RiskRejected(RiskRejection),
    ExecutionGroupPartialSubmit {
        group_id: String,
        submitted_legs: usize,
        total_legs: usize,
        reason: &'static str,
    },
    OwnershipConflict {
        account_id: String,
        symbol: String,
        active_task_kind: TaskKind,
    },
    ManualOrderBlocked {
        account_id: String,
        symbol: String,
        active_task_kind: TaskKind,
    },
    OrderNotReady {
        account_id: String,
        order_id: String,
    },
    InvalidCalendarDate {
        date: String,
    },
    Unsupported(&'static str),
    InvalidState(&'static str),
}
```

Add the display/source arms:

```rust
Self::ExecutionGroupPartialSubmit {
    group_id,
    submitted_legs,
    total_legs,
    reason,
} => write!(
    f,
    "execution group partial submit group_id={group_id} submitted_legs={submitted_legs} total_legs={total_legs}: {reason}"
),
```

and:

```rust
Self::ExecutionGroupPartialSubmit { .. } => None,
```

- [ ] **Step 4: Implement the builder surface**

Create `crates/tqsdk-task/src/execution_group.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use std::time::Duration;

use tqsdk_core::{Order, OrderLifecycle, TradeDirection, TradeOffset};
use tqsdk_wait::{OrderTicket, OrderTicketState};

use crate::{Result, TaskError, TaskHost, TaskOrderIntent};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HedgePolicy {
    ReportExposure,
    FlattenFilledLegs,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionLegIntent {
    pub client_order_id: String,
    pub intent: TaskOrderIntent,
}

#[derive(Debug, Clone)]
pub struct ExecutionLegTicket {
    client_order_id: String,
    intent: TaskOrderIntent,
    ticket: OrderTicket,
}

impl ExecutionLegTicket {
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

pub struct ExecutionGroupBuilder<'a> {
    host: &'a mut TaskHost,
    account_id: String,
    group_id: Option<String>,
    max_unhedged: Option<Duration>,
    hedge_policy: HedgePolicy,
    legs: Vec<TaskOrderIntent>,
}

pub struct ExecutionLegBuilder<'a> {
    group: ExecutionGroupBuilder<'a>,
    symbol: String,
}

pub struct ExecutionLegDraft<'a> {
    group: ExecutionGroupBuilder<'a>,
    intent: TaskOrderIntent,
}

#[derive(Debug, Clone)]
pub struct ExecutionGroupTicket {
    group_id: String,
    account_id: String,
    hedge_policy: HedgePolicy,
    max_unhedged: Option<Duration>,
    legs: Vec<ExecutionLegTicket>,
}

impl<'a> ExecutionGroupBuilder<'a> {
    pub(crate) fn new(host: &'a mut TaskHost, account_id: String) -> Self {
        Self {
            host,
            account_id,
            group_id: None,
            max_unhedged: None,
            hedge_policy: HedgePolicy::ReportExposure,
            legs: Vec::new(),
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
    pub fn on_leg_failed(mut self, policy: HedgePolicy) -> Self {
        self.hedge_policy = policy;
        self
    }

    #[must_use]
    pub fn leg(self, symbol: impl AsRef<str>) -> ExecutionLegBuilder<'a> {
        ExecutionLegBuilder {
            group: self,
            symbol: symbol.as_ref().to_owned(),
        }
    }

    pub async fn send_once(self) -> Result<ExecutionGroupTicket> {
        submit_group(self).await
    }
}

impl<'a> ExecutionLegBuilder<'a> {
    #[must_use]
    pub fn buy_open(self, volume: i64) -> ExecutionLegDraft<'a> {
        self.intent(TradeDirection::Buy, Some(TradeOffset::Open), volume)
    }

    #[must_use]
    pub fn sell_open(self, volume: i64) -> ExecutionLegDraft<'a> {
        self.intent(TradeDirection::Sell, Some(TradeOffset::Open), volume)
    }

    #[must_use]
    pub fn buy_close(self, volume: i64) -> ExecutionLegDraft<'a> {
        self.intent(TradeDirection::Buy, Some(TradeOffset::Close), volume)
    }

    #[must_use]
    pub fn sell_close(self, volume: i64) -> ExecutionLegDraft<'a> {
        self.intent(TradeDirection::Sell, Some(TradeOffset::Close), volume)
    }

    fn intent(self, direction: TradeDirection, offset: Option<TradeOffset>, volume: i64) -> ExecutionLegDraft<'a> {
        ExecutionLegDraft {
            intent: TaskOrderIntent {
                account_id: self.group.account_id.clone(),
                symbol: self.symbol,
                direction,
                offset,
                volume,
                limit_price: None,
            },
            group: self.group,
        }
    }
}

impl<'a> ExecutionLegDraft<'a> {
    #[must_use]
    pub fn limit(mut self, price: f64) -> Self {
        self.intent.limit_price = Some(price);
        self
    }

    #[must_use]
    pub fn leg(mut self, symbol: impl AsRef<str>) -> ExecutionLegBuilder<'a> {
        self.group.legs.push(self.intent);
        self.group.leg(symbol)
    }

    pub async fn send_once(mut self) -> Result<ExecutionGroupTicket> {
        self.group.legs.push(self.intent);
        submit_group(self.group).await
    }
}

impl ExecutionGroupTicket {
    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub fn hedge_policy(&self) -> HedgePolicy {
        self.hedge_policy
    }

    #[must_use]
    pub fn max_unhedged(&self) -> Option<Duration> {
        self.max_unhedged
    }

    #[must_use]
    pub fn legs(&self) -> &[ExecutionLegTicket] {
        &self.legs
    }
}

async fn submit_group(mut builder: ExecutionGroupBuilder<'_>) -> Result<ExecutionGroupTicket> {
    let group_id = builder
        .group_id
        .take()
        .ok_or(TaskError::InvalidState("execution group id is required"))?;
    if group_id.trim().is_empty() {
        return Err(TaskError::InvalidState("execution group id must not be empty"));
    }
    if builder.legs.len() < 2 {
        return Err(TaskError::InvalidState("execution group requires at least two legs"));
    }
    if builder.hedge_policy == HedgePolicy::FlattenFilledLegs {
        return Err(TaskError::Unsupported("automatic hedge policy is not implemented"));
    }

    let leg_intents = builder
        .legs
        .into_iter()
        .enumerate()
        .map(|(index, intent)| ExecutionLegIntent {
            client_order_id: format!("{group_id}:leg:{index}"),
            intent,
        })
        .collect::<Vec<_>>();

    for leg in &leg_intents {
        builder.host.preflight_task_order(&leg.intent)?;
    }

    let mut submitted = Vec::with_capacity(leg_intents.len());
    let total_legs = leg_intents.len();
    for leg in leg_intents {
        match builder
            .host
            .submit_prechecked_task_order_once(leg.intent.clone(), leg.client_order_id.as_str())
            .await
        {
            Ok(ticket) => submitted.push(ExecutionLegTicket {
                client_order_id: leg.client_order_id,
                intent: leg.intent,
                ticket,
            }),
            Err(_) => {
                return Err(TaskError::ExecutionGroupPartialSubmit {
                    group_id,
                    submitted_legs: submitted.len(),
                    total_legs,
                    reason: "leg submit failed after group preflight",
                });
            }
        }
    }

    Ok(ExecutionGroupTicket {
        group_id,
        account_id: builder.account_id,
        hedge_policy: builder.hedge_policy,
        max_unhedged: builder.max_unhedged,
        legs: submitted,
    })
}
```

This initial code imports outcome-related names that will be used in later tasks. If Clippy reports unused imports before Task 4, remove unused imports in Task 1 and reintroduce them when outcome code is added.

- [ ] **Step 5: Wire `TaskHost` and exports**

Modify `crates/tqsdk-task/src/host.rs`:

```rust
use crate::execution_group::ExecutionGroupBuilder;
```

Add:

```rust
#[must_use]
pub fn execution_group(&mut self, account_id: impl AsRef<str>) -> ExecutionGroupBuilder<'_> {
    ExecutionGroupBuilder::new(self, account_id.as_ref().to_owned())
}
```

Modify `crates/tqsdk-task/src/lib.rs`:

```rust
mod execution_group;

pub use execution_group::{
    ExecutionExposure, ExecutionGroupBuilder, ExecutionGroupOutcome, ExecutionGroupStatus,
    ExecutionGroupTicket, ExecutionLegReport, ExecutionLegState, ExecutionLegTicket, HedgePolicy,
};
```

For Task 1 only, export just the types that exist. Add the remaining exports in Task 4.

- [ ] **Step 6: Run builder tests**

Run:

```bash
cargo test -p tqsdk-task --test execution_group execution_group_ -- --nocapture
```

Expected: PASS for the two Task 1 tests.

- [ ] **Step 7: Commit**

```bash
git add crates/tqsdk-task/src/execution_group.rs crates/tqsdk-task/src/host.rs crates/tqsdk-task/src/error.rs crates/tqsdk-task/src/lib.rs crates/tqsdk-task/tests/execution_group.rs
git commit -m "feat: add task execution group builder"
```

---

### Task 2: All-Leg Preflight Uses Ownership And Risk Before Dispatch

**Files:**
- Modify: `crates/tqsdk-task/src/host.rs`
- Modify: `crates/tqsdk-task/src/execution_group.rs`
- Test: `crates/tqsdk-task/tests/execution_group.rs`

- [ ] **Step 1: Add failing ownership preflight test**

Append to `crates/tqsdk-task/tests/execution_group.rs`:

```rust
#[tokio::test(flavor = "current_thread")]
async fn execution_group_preflights_all_legs_before_dispatching_any_leg() {
    let mut host = seeded_host();
    let _task = host.target_pos("sim", "SHFE.ag2602").build().unwrap();

    let err = host
        .execution_group("sim")
        .client_group_id("spread-preflight-001")
        .leg("SHFE.au2602")
        .buy_open(1)
        .limit(480.0)
        .leg("SHFE.ag2602")
        .sell_open(15)
        .limit(6500.0)
        .send_once()
        .await
        .unwrap_err();

    assert_eq!(
        err,
        TaskError::ManualOrderBlocked {
            account_id: "sim".to_string(),
            symbol: "SHFE.ag2602".to_string(),
            active_task_kind: tqsdk_task::TaskKind::TargetPos,
        }
    );
    assert!(host.api().handle_for_test().drain_dispatches().unwrap().is_empty());
}
```

- [ ] **Step 2: Add failing risk preflight test**

Add imports:

```rust
use tqsdk_task::{RiskEngine, RiskRejection};
```

Add seed helper:

```rust
fn seed_account_position_quote(
    host: &TaskHost,
    account_id: &str,
    symbol: &str,
    available: f64,
    net_position: i64,
    last_price: f64,
) {
    host.api()
        .handle_for_test()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "quotes": {
                            symbol: {
                                "datetime": "2026-04-27 09:30:00.000000",
                                "last_price": last_price
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("seed quote commit should produce a commit");

    let (exchange_id, instrument_id) = symbol
        .split_once('.')
        .expect("test symbol should contain an exchange prefix");
    host.api()
        .handle_for_test()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "trade".to_string(),
                domains: vec![ProtocolDomain::Trade],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "trade": {
                            account_id: {
                                "accounts": {
                                    "CNY": {
                                        "user_id": account_id,
                                        "available": available
                                    }
                                },
                                "positions": {
                                    symbol: {
                                        "user_id": account_id,
                                        "exchange_id": exchange_id,
                                        "instrument_id": instrument_id,
                                        "volume_long": net_position.max(0),
                                        "volume_short": (-net_position).max(0),
                                        "pos_long": net_position.max(0),
                                        "pos_short": (-net_position).max(0),
                                        "pos": net_position
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
        .expect("seed account/position commit should produce a commit");
}
```

Add test:

```rust
#[tokio::test(flavor = "current_thread")]
async fn execution_group_risk_rejection_prevents_partial_dispatch() {
    let mut host = seeded_host().with_risk(RiskEngine::new().max_price_deviation(10.0));
    seed_account_position_quote(&host, "sim", "SHFE.au2602", 2_000.0, 0, 480.0);
    seed_account_position_quote(&host, "sim", "SHFE.ag2602", 2_000.0, 0, 6500.0);

    let err = host
        .execution_group("sim")
        .client_group_id("spread-risk-001")
        .leg("SHFE.au2602")
        .buy_open(1)
        .limit(480.0)
        .leg("SHFE.ag2602")
        .sell_open(15)
        .limit(6520.0)
        .send_once()
        .await
        .unwrap_err();

    assert_eq!(
        err,
        TaskError::RiskRejected(RiskRejection::PriceDeviationExceeded {
            symbol: "SHFE.ag2602".to_string(),
            limit_price: 6520.0,
            reference_price: 6500.0,
            max_abs_deviation: 10.0,
        })
    );
    assert!(host.api().handle_for_test().drain_dispatches().unwrap().is_empty());
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run:

```bash
cargo test -p tqsdk-task --test execution_group preflight -- --nocapture
```

Expected: FAIL until `TaskHost::preflight_task_order` and group preflight are wired correctly.

- [ ] **Step 4: Refactor host preflight**

Modify `crates/tqsdk-task/src/host.rs` so single orders and groups share validation:

```rust
pub(crate) fn preflight_task_order(&self, intent: &TaskOrderIntent) -> Result<()> {
    if intent.volume <= 0 {
        return Err(TaskError::InvalidState("order volume must be positive"));
    }
    if intent.offset.is_none() {
        return Err(TaskError::Unsupported("task orders require explicit offset"));
    }
    let limit_price = intent
        .limit_price
        .ok_or(TaskError::InvalidState("limit price is required"))?;
    if !limit_price.is_finite() {
        return Err(TaskError::InvalidState("limit price must be finite"));
    }

    self.registry.with(|registry| {
        registry.check_manual_order_allowed(&intent.account_id, &intent.symbol)
    })?;
    self.check_risk(intent)?;
    Ok(())
}

pub(crate) async fn submit_prechecked_task_order_once(
    &mut self,
    intent: TaskOrderIntent,
    client_order_id: impl Into<tqsdk_wait::ClientOrderId>,
) -> Result<tqsdk_wait::OrderTicket> {
    let offset = intent.offset.ok_or(TaskError::Unsupported(
        "task orders require explicit offset",
    ))?;
    let limit_price = intent
        .limit_price
        .ok_or(TaskError::InvalidState("limit price is required"))?;

    self.api
        .limit_order(intent.account_id, intent.symbol)
        .client_intent(client_order_id)
        .side(intent.direction, offset, intent.volume)
        .at(limit_price)
        .send_once()
        .await
        .map_err(Into::into)
}
```

Then simplify existing `submit_task_order_once`:

```rust
pub(crate) async fn submit_task_order_once(
    &mut self,
    intent: TaskOrderIntent,
    client_order_id: ClientOrderId,
) -> Result<OrderTicket> {
    self.preflight_task_order(&intent)?;
    self.submit_prechecked_task_order_once(intent, client_order_id).await
}
```

Keep `insert_order_guarded` on its existing path, but ensure it still calls `self.check_risk(&intent)?`.

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -p tqsdk-task --test execution_group -- --nocapture
cargo test -p tqsdk-task --test risk_orders -- --nocapture
```

Expected: PASS. The second command proves the existing typed order/risk tests still pass after the host refactor.

- [ ] **Step 6: Commit**

```bash
git add crates/tqsdk-task/src/host.rs crates/tqsdk-task/src/execution_group.rs crates/tqsdk-task/tests/execution_group.rs
git commit -m "feat: preflight execution group legs"
```

---

### Task 3: Group Idempotency And Partial Submit Error Surface

**Files:**
- Modify: `crates/tqsdk-task/src/execution_group.rs`
- Modify: `crates/tqsdk-task/src/error.rs`
- Test: `crates/tqsdk-task/tests/execution_group.rs`

- [ ] **Step 1: Add same group retry test**

Append:

```rust
#[tokio::test(flavor = "current_thread")]
async fn execution_group_send_once_reuses_existing_leg_intents_on_retry() {
    let mut host = seeded_host();

    let first = host
        .execution_group("sim")
        .client_group_id("spread-retry-001")
        .leg("SHFE.au2602")
        .buy_open(1)
        .limit(480.0)
        .leg("SHFE.ag2602")
        .sell_open(15)
        .limit(6500.0)
        .send_once()
        .await
        .unwrap();
    assert!(first.legs().iter().all(|leg| leg.ticket().was_submitted()));
    assert_eq!(host.api().handle_for_test().drain_dispatches().unwrap().len(), 2);

    let retry = host
        .execution_group("sim")
        .client_group_id("spread-retry-001")
        .leg("SHFE.au2602")
        .buy_open(1)
        .limit(480.0)
        .leg("SHFE.ag2602")
        .sell_open(15)
        .limit(6500.0)
        .send_once()
        .await
        .unwrap();

    assert_eq!(retry.group_id(), "spread-retry-001");
    assert!(retry.legs().iter().all(|leg| !leg.ticket().was_submitted()));
    assert!(host.api().handle_for_test().drain_dispatches().unwrap().is_empty());
}
```

- [ ] **Step 2: Add mismatched retry test**

Append:

```rust
#[tokio::test(flavor = "current_thread")]
async fn execution_group_retry_with_different_leg_spec_is_rejected_by_intent_ledger() {
    let mut host = seeded_host();

    host.execution_group("sim")
        .client_group_id("spread-mismatch-001")
        .leg("SHFE.au2602")
        .buy_open(1)
        .limit(480.0)
        .leg("SHFE.ag2602")
        .sell_open(15)
        .limit(6500.0)
        .send_once()
        .await
        .unwrap();
    assert_eq!(host.api().handle_for_test().drain_dispatches().unwrap().len(), 2);

    let err = host
        .execution_group("sim")
        .client_group_id("spread-mismatch-001")
        .leg("SHFE.au2602")
        .buy_open(2)
        .limit(480.0)
        .leg("SHFE.ag2602")
        .sell_open(15)
        .limit(6500.0)
        .send_once()
        .await
        .unwrap_err();

    assert!(
        matches!(err, TaskError::Wait(_)),
        "mismatched retry should be rejected by the wait/session intent ledger, got {err:?}"
    );
}
```

- [ ] **Step 3: Run tests**

Run:

```bash
cargo test -p tqsdk-task --test execution_group retry -- --nocapture
```

Expected: PASS if group client ids are deterministic and still use the existing `OrderIntentRecord` ledger.

- [ ] **Step 4: Ensure partial submit error is explicit**

Review `submit_group`. If a leg submit fails after earlier legs were submitted, it must return:

```rust
TaskError::ExecutionGroupPartialSubmit {
    group_id,
    submitted_legs: submitted.len(),
    total_legs,
    reason: "leg submit failed after group preflight",
}
```

Do not return a bare `Wait` or `Session` error for this path, because the user needs to know a group may already have live exposure.

- [ ] **Step 5: Run focused tests**

Run:

```bash
cargo test -p tqsdk-task --test execution_group -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/tqsdk-task/src/execution_group.rs crates/tqsdk-task/src/error.rs crates/tqsdk-task/tests/execution_group.rs
git commit -m "feat: make execution group retries idempotent"
```

---

### Task 4: Group Status, Outcome, And Exposure Reporting

**Files:**
- Modify: `crates/tqsdk-task/src/execution_group.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`
- Test: `crates/tqsdk-task/tests/execution_group.rs`

- [ ] **Step 1: Add order status seed helper**

Append to `crates/tqsdk-task/tests/execution_group.rs`:

```rust
fn seed_order_status_commit(
    host: &TaskHost,
    account_id: &str,
    symbol: &str,
    order_id: &str,
    direction: &str,
    offset: &str,
    volume_orign: i64,
    volume_left: i64,
    status: &str,
) {
    let (exchange_id, instrument_id) = symbol
        .split_once('.')
        .expect("test symbol should contain an exchange prefix");
    host.api()
        .handle_for_test()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "trade".to_string(),
                domains: vec![ProtocolDomain::Trade],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "trade": {
                            account_id: {
                                "orders": {
                                    order_id: {
                                        "seqno": 1,
                                        "user_id": account_id,
                                        "order_id": order_id,
                                        "exchange_order_id": format!("exchange-{order_id}"),
                                        "exchange_id": exchange_id,
                                        "instrument_id": instrument_id,
                                        "direction": direction,
                                        "offset": offset,
                                        "volume_orign": volume_orign,
                                        "volume_left": volume_left,
                                        "limit_price": 1.0,
                                        "price_type": "LIMIT",
                                        "volume_condition": "ANY",
                                        "time_condition": "GFD",
                                        "insert_date_time": 1_713_660_000_000_000_000_i64,
                                        "last_msg": "",
                                        "status": status,
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
        .expect("seed order status commit should produce a commit");
}
```

- [ ] **Step 2: Add all-filled outcome test**

Append:

```rust
#[tokio::test(flavor = "current_thread")]
async fn execution_group_status_reports_all_filled_outcome() {
    let mut host = seeded_host();
    let group = host
        .execution_group("sim")
        .client_group_id("spread-filled-001")
        .leg("SHFE.au2602")
        .buy_open(1)
        .limit(480.0)
        .leg("SHFE.ag2602")
        .sell_open(15)
        .limit(6500.0)
        .send_once()
        .await
        .unwrap();
    host.api().handle_for_test().drain_dispatches().unwrap();

    seed_order_status_commit(
        &host,
        "sim",
        "SHFE.au2602",
        "spread-filled-001:leg:0",
        "BUY",
        "OPEN",
        1,
        0,
        "FINISHED",
    );
    seed_order_status_commit(
        &host,
        "sim",
        "SHFE.ag2602",
        "spread-filled-001:leg:1",
        "SELL",
        "OPEN",
        15,
        0,
        "FINISHED",
    );

    let outcome = group.outcome(host.api()).unwrap().unwrap();
    match outcome {
        ExecutionGroupOutcome::AllFilled { legs } => {
            assert_eq!(legs.len(), 2);
            assert!(legs.iter().all(|leg| leg.filled_volume == leg.requested_volume));
        }
        other => panic!("expected all filled outcome, got {other:?}"),
    }
}
```

- [ ] **Step 3: Add desynchronized exposure test**

Append:

```rust
#[tokio::test(flavor = "current_thread")]
async fn execution_group_status_reports_exposure_when_one_leg_fills_and_other_rejects() {
    let mut host = seeded_host();
    let group = host
        .execution_group("sim")
        .client_group_id("spread-exposure-001")
        .leg("SHFE.au2602")
        .buy_open(1)
        .limit(480.0)
        .leg("SHFE.ag2602")
        .sell_open(15)
        .limit(6500.0)
        .send_once()
        .await
        .unwrap();
    host.api().handle_for_test().drain_dispatches().unwrap();

    seed_order_status_commit(
        &host,
        "sim",
        "SHFE.au2602",
        "spread-exposure-001:leg:0",
        "BUY",
        "OPEN",
        1,
        0,
        "FINISHED",
    );
    seed_order_status_commit(
        &host,
        "sim",
        "SHFE.ag2602",
        "spread-exposure-001:leg:1",
        "SELL",
        "OPEN",
        15,
        15,
        "FINISHED",
    );

    let outcome = group.outcome(host.api()).unwrap().unwrap();
    match outcome {
        ExecutionGroupOutcome::NeedsHedge { exposure, legs } => {
            assert_eq!(legs.len(), 2);
            assert_eq!(exposure.filled_symbols, vec!["SHFE.au2602".to_string()]);
            assert_eq!(exposure.unfilled_symbols, vec!["SHFE.ag2602".to_string()]);
        }
        other => panic!("expected hedge exposure outcome, got {other:?}"),
    }
}
```

- [ ] **Step 4: Implement status and outcome types**

Modify `crates/tqsdk-task/src/execution_group.rs`:

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionLegState {
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
pub struct ExecutionLegReport {
    pub client_order_id: String,
    pub account_id: String,
    pub symbol: String,
    pub direction: TradeDirection,
    pub offset: Option<TradeOffset>,
    pub requested_volume: i64,
    pub filled_volume: i64,
    pub volume_left: i64,
    pub state: ExecutionLegState,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionExposure {
    pub filled_symbols: Vec<String>,
    pub unfilled_symbols: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionGroupStatus {
    Pending { legs: Vec<ExecutionLegReport> },
    Finished(ExecutionGroupOutcome),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionGroupOutcome {
    AllFilled { legs: Vec<ExecutionLegReport> },
    Cancelled { legs: Vec<ExecutionLegReport> },
    Rejected { legs: Vec<ExecutionLegReport> },
    Failed { legs: Vec<ExecutionLegReport> },
    NeedsHedge {
        exposure: ExecutionExposure,
        legs: Vec<ExecutionLegReport>,
    },
}
```

Add methods:

```rust
impl ExecutionGroupTicket {
    pub fn status(&self, api: &tqsdk_wait::TqApi) -> Result<ExecutionGroupStatus> {
        let legs = self.leg_reports(api)?;
        Ok(match outcome_from_reports(&legs) {
            Some(outcome) => ExecutionGroupStatus::Finished(outcome),
            None => ExecutionGroupStatus::Pending { legs },
        })
    }

    pub fn outcome(&self, api: &tqsdk_wait::TqApi) -> Result<Option<ExecutionGroupOutcome>> {
        let legs = self.leg_reports(api)?;
        Ok(outcome_from_reports(&legs))
    }

    pub async fn wait_finished(
        &self,
        host: &mut TaskHost,
        deadline: tokio::time::Instant,
    ) -> Result<ExecutionGroupOutcome> {
        loop {
            if let Some(outcome) = self.outcome(host.api())? {
                return Ok(outcome);
            }
            if !host.wait_update(Some(deadline)).await? {
                let legs = self.leg_reports(host.api())?;
                return Ok(ExecutionGroupOutcome::NeedsHedge {
                    exposure: exposure_from_reports(&legs),
                    legs,
                });
            }
        }
    }

    fn leg_reports(&self, api: &tqsdk_wait::TqApi) -> Result<Vec<ExecutionLegReport>> {
        self.legs
            .iter()
            .map(|leg| leg_report(api, leg))
            .collect()
    }
}
```

Add helper functions in the same file:

```rust
fn leg_report(api: &tqsdk_wait::TqApi, leg: &ExecutionLegTicket) -> Result<ExecutionLegReport> {
    let state = leg.ticket.status(api)?;
    let (state, filled_volume, volume_left) = match state {
        OrderTicketState::Unknown { .. } => (ExecutionLegState::Unknown, 0, leg.intent.volume),
        OrderTicketState::CommandPending { .. } => {
            (ExecutionLegState::CommandPending, 0, leg.intent.volume)
        }
        OrderTicketState::Live { order, .. } => live_leg_state(&order),
        OrderTicketState::Filled { order, .. } => {
            let volume_left = order.volume_left;
            let filled = (order.volume_orign - volume_left).max(0);
            (ExecutionLegState::Filled, filled, volume_left)
        }
        OrderTicketState::Cancelled { order, .. } => terminal_optional_order_state(
            order.as_ref(),
            ExecutionLegState::Cancelled,
            leg.intent.volume,
        ),
        OrderTicketState::Rejected { order, .. } => terminal_optional_order_state(
            order.as_ref(),
            ExecutionLegState::Rejected,
            leg.intent.volume,
        ),
        OrderTicketState::Failed { order, .. } => terminal_optional_order_state(
            order.as_ref(),
            ExecutionLegState::Failed,
            leg.intent.volume,
        ),
    };

    Ok(ExecutionLegReport {
        client_order_id: leg.client_order_id.clone(),
        account_id: leg.intent.account_id.clone(),
        symbol: leg.intent.symbol.clone(),
        direction: leg.intent.direction,
        offset: leg.intent.offset,
        requested_volume: leg.intent.volume,
        filled_volume,
        volume_left,
        state,
    })
}

fn live_leg_state(order: &Order) -> (ExecutionLegState, i64, i64) {
    let volume_left = order.volume_left;
    let filled = (order.volume_orign - volume_left).max(0);
    if filled > 0 {
        (
            ExecutionLegState::PartiallyFilled {
                filled_volume: filled,
                volume_left,
            },
            filled,
            volume_left,
        )
    } else {
        (ExecutionLegState::Live, 0, volume_left)
    }
}

fn terminal_optional_order_state(
    order: Option<&Order>,
    fallback: ExecutionLegState,
    requested_volume: i64,
) -> (ExecutionLegState, i64, i64) {
    let Some(order) = order else {
        return (fallback, 0, requested_volume);
    };
    let volume_left = order.volume_left;
    let filled = (order.volume_orign - volume_left).max(0);
    let state = match (order.lifecycle, filled > 0, volume_left == 0) {
        (OrderLifecycle::Filled, _, _) => ExecutionLegState::Filled,
        (_, true, false) => ExecutionLegState::PartiallyFilled {
            filled_volume: filled,
            volume_left,
        },
        _ => fallback,
    };
    (state, filled, volume_left)
}

fn outcome_from_reports(legs: &[ExecutionLegReport]) -> Option<ExecutionGroupOutcome> {
    if legs.iter().any(is_pending_state) {
        return None;
    }

    let any_filled = legs.iter().any(|leg| leg.filled_volume > 0);
    let all_filled = legs.iter().all(|leg| matches!(leg.state, ExecutionLegState::Filled));
    if all_filled {
        return Some(ExecutionGroupOutcome::AllFilled {
            legs: legs.to_vec(),
        });
    }

    if any_filled {
        return Some(ExecutionGroupOutcome::NeedsHedge {
            exposure: exposure_from_reports(legs),
            legs: legs.to_vec(),
        });
    }

    if legs.iter().any(|leg| matches!(leg.state, ExecutionLegState::Failed)) {
        return Some(ExecutionGroupOutcome::Failed {
            legs: legs.to_vec(),
        });
    }
    if legs.iter().any(|leg| matches!(leg.state, ExecutionLegState::Rejected)) {
        return Some(ExecutionGroupOutcome::Rejected {
            legs: legs.to_vec(),
        });
    }
    if legs.iter().any(|leg| matches!(leg.state, ExecutionLegState::Cancelled)) {
        return Some(ExecutionGroupOutcome::Cancelled {
            legs: legs.to_vec(),
        });
    }
    None
}

fn is_pending_state(leg: &&ExecutionLegReport) -> bool {
    matches!(
        leg.state,
        ExecutionLegState::Unknown | ExecutionLegState::CommandPending | ExecutionLegState::Live
    )
}

fn exposure_from_reports(legs: &[ExecutionLegReport]) -> ExecutionExposure {
    let filled_symbols = legs
        .iter()
        .filter(|leg| leg.filled_volume > 0)
        .map(|leg| leg.symbol.clone())
        .collect();
    let unfilled_symbols = legs
        .iter()
        .filter(|leg| leg.filled_volume < leg.requested_volume)
        .map(|leg| leg.symbol.clone())
        .collect();
    ExecutionExposure {
        filled_symbols,
        unfilled_symbols,
    }
}
```

This is intentionally conservative: mixed terminal states with any fill produce `NeedsHedge`, not generic `Failed`.

- [ ] **Step 5: Export outcome types**

Modify `crates/tqsdk-task/src/lib.rs`:

```rust
pub use execution_group::{
    ExecutionExposure, ExecutionGroupBuilder, ExecutionGroupOutcome, ExecutionGroupStatus,
    ExecutionGroupTicket, ExecutionLegReport, ExecutionLegState, ExecutionLegTicket, HedgePolicy,
};
```

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test -p tqsdk-task --test execution_group -- --nocapture
cargo test -p tqsdk-task --test wait_api_trade wait_reconnect_safe_terminal -- --nocapture
```

Expected: PASS. The second command proves the group layer still agrees with wait-layer ticket terminal semantics.

- [ ] **Step 7: Commit**

```bash
git add crates/tqsdk-task/src/execution_group.rs crates/tqsdk-task/src/lib.rs crates/tqsdk-task/tests/execution_group.rs
git commit -m "feat: report execution group outcomes"
```

---

### Task 5: Formal S12 Example And Scenario Docs

**Files:**
- Create: `crates/tqsdk-task/examples/api_contract_s12_spread_arbitrage.rs`
- Modify: `docs/scenarios/api_gaps/api_contract_s12_spread_arbitrage.rs`
- Modify: `docs/reviews/public-api-scenario-review.md`
- Modify: `docs/scenarios/user-layer-iteration-plan.md`
- Modify: `docs/architecture/api-task.md`
- Modify: `crates/tqsdk-task/README.md`

- [ ] **Step 1: Add formal S12 example**

Create `crates/tqsdk-task/examples/api_contract_s12_spread_arbitrage.rs`:

```rust
//! Scenario: 跨合约套利
//!
//! User goal:
//! - 两腿使用同一个 typed execution group 下单
//! - 处理成交不同步
//! - 在单腿裸露时得到 typed exposure report
//!
//! API contract:
//! - 两腿 order intent 有同一个 client group id
//! - 下单前所有腿统一经过 ownership guard 和 risk gate
//! - 用户读取 group-level outcome，而不是手写 `Vec<OrderTicket>` 状态机
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 两腿分别用普通 order ref 手动拼事务语义
//! - 本地 bool/Vec 追踪腿状态作为资金安全依据
//! - 字符串判断订单状态
//! - `RuntimeCommand::Trade`
//!
//! Regression signal:
//! - 单腿成交后另一腿失败只能靠业务代码临时补救
//! - 无法表达最大净敞口或 group-level outcome
//! - group outcome 无法审计
//!
//! Review questions:
//! - 当前 API 是否能安全表达跨合约套利 foundation？
//! - 是否存在 P0 级单腿裸露风险？
//! - 自动对冲应继续留在 task 层，还是拆成独立 execution policy？
//!
//! Current limitation:
//! - 本示例只要求 SDK 报告 typed exposure，不自动提交对冲/平仓单。

use std::time::Duration;

use tqsdk_core::TradeAccountType;
use tqsdk_task::{ExecutionGroupOutcome, HedgePolicy, RiskEngine, TaskHost};
use tqsdk_wait::TqApiBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let broker_id = std::env::var("TQ_BROKER_ID")?;
    let account_id = std::env::var("TQ_ACCOUNT_ID")?;
    let account_password = std::env::var("TQ_ACCOUNT_PASSWORD")?;
    let leg_a = std::env::var("TQ_SPREAD_LEG_A").unwrap_or_else(|_| "SHFE.au2602".into());
    let leg_b = std::env::var("TQ_SPREAD_LEG_B").unwrap_or_else(|_| "SHFE.ag2602".into());

    let mut api = TqApiBuilder::new(user, pass)
        .futures_market()
        .trade_target(broker_id.clone(), account_id.clone())
        .build()
        .await?;

    api.login_trade_account(
        broker_id.as_str(),
        account_id.as_str(),
        account_password.as_str(),
        TradeAccountType::Future,
        Some(tokio::time::Instant::now() + Duration::from_secs(30)),
    )
    .await?;
    api.get_quote(leg_a.as_str()).await?;
    api.get_quote(leg_b.as_str()).await?;

    let risk = RiskEngine::new()
        .max_order_volume(20)
        .min_available(1_000.0)
        .max_price_deviation(50.0);
    let mut host = TaskHost::new(api).with_risk(risk);

    let group = host
        .execution_group(account_id.as_str())
        .client_group_id("spread-example-001")
        .max_unhedged(Duration::from_secs(2))
        .on_leg_failed(HedgePolicy::ReportExposure)
        .leg(leg_a.as_str())
        .buy_open(1)
        .limit(480.0)
        .leg(leg_b.as_str())
        .sell_open(15)
        .limit(6500.0)
        .send_once()
        .await?;

    let outcome = group
        .wait_finished(
            &mut host,
            tokio::time::Instant::now() + Duration::from_secs(30),
        )
        .await?;

    match outcome {
        ExecutionGroupOutcome::AllFilled { legs } => {
            println!("spread filled legs={}", legs.len());
        }
        ExecutionGroupOutcome::NeedsHedge { exposure, legs } => {
            println!("spread needs manual hedge exposure={exposure:?} legs={legs:?}");
        }
        ExecutionGroupOutcome::Rejected { legs }
        | ExecutionGroupOutcome::Failed { legs }
        | ExecutionGroupOutcome::Cancelled { legs } => {
            println!("spread did not complete legs={legs:?}");
        }
    }

    Ok(())
}
```

- [ ] **Step 2: Narrow S12 gap doc**

Modify `docs/scenarios/api_gaps/api_contract_s12_spread_arbitrage.rs` so the `API gap` section becomes:

```rust
//! Remaining API gap:
//! `tqsdk-task` 已提供最小 `ExecutionGroup` foundation：typed group id、
//! all-leg preflight、idempotent leg order intents、group outcome 和 exposure report。
//!
//! 本文件保留的是更高阶执行缺口：
//! - 自动 hedge / flatten filled legs；
//! - timed cancel / replace；
//! - 最大裸露量驱动的自动撤补；
//! - 多账户或多腿组合的联合风控；
//! - 人工介入后的 group resume / audit。
```

- [ ] **Step 3: Update scenario review**

Modify the S12 row in `docs/reviews/public-api-scenario-review.md`:

```markdown
| 12. 跨合约套利 | 勉强 | 中 | 无 | 无 | 高 | 中 | 局部重构 | `crates/tqsdk-task/examples/api_contract_s12_spread_arbitrage.rs`; `docs/scenarios/api_gaps/api_contract_s12_spread_arbitrage.rs`; `ExecutionGroupBuilder`; `ExecutionGroupOutcome`; automatic hedge remains gap |
```

Update the conclusions sentence so it says:

```markdown
普通登录、限价单、部分成交撤单、session-scoped reconnect-safe order intent、最小前置风控和 execution group foundation 已具备薄 facade；跨进程持久恢复、自动对冲、组合级 what-if 风控和多账户组合执行仍需继续补齐。
```

- [ ] **Step 4: Update iteration plan**

Modify `docs/scenarios/user-layer-iteration-plan.md` under P1 执行层抽象:

```markdown
已落地：

- `ExecutionGroup` foundation 支持 typed group id、两腿订单、all-leg preflight、session-scoped retry idempotency 和 group outcome/exposure report。

仍未完成、不可伪装为已支持：

- 自动 hedge / flatten；
- timed cancel / replace；
- `AccountGroup` 与 allocation policy；
- group resume / audit log。
```

- [ ] **Step 5: Update architecture and crate README**

Modify `docs/architecture/api-task.md` and `crates/tqsdk-task/README.md` to state:

- execution group belongs in `tqsdk-task`;
- it reuses wait-layer `OrderTicket`;
- it does not create private order state;
- automatic hedging remains future work.

- [ ] **Step 6: Check formal example**

Run:

```bash
cargo check -p tqsdk-task --example api_contract_s12_spread_arbitrage
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/tqsdk-task/examples/api_contract_s12_spread_arbitrage.rs docs/scenarios/api_gaps/api_contract_s12_spread_arbitrage.rs docs/reviews/public-api-scenario-review.md docs/scenarios/user-layer-iteration-plan.md docs/architecture/api-task.md crates/tqsdk-task/README.md
git commit -m "docs: promote spread execution group scenario"
```

---

### Task 6: Workspace Validation

**Files:**
- No planned source edits unless validation exposes a regression.

- [ ] **Step 1: Run examples check**

Run:

```bash
cargo check --workspace --examples
```

Expected: PASS.

- [ ] **Step 2: Run workspace tests**

Run:

```bash
cargo test --workspace
```

Expected: PASS.

- [ ] **Step 3: Run clippy**

Run:

```bash
cargo clippy --workspace --examples --all-targets -- -D warnings
```

Expected: PASS.

- [ ] **Step 4: Feature flags**

This batch is not expected to modify feature flags. If implementation changes feature flags, also run:

```bash
cargo check --workspace --no-default-features
cargo check --workspace --all-features --examples
```

Expected: PASS.

- [ ] **Step 5: Final status check**

Run:

```bash
git status --short
```

Expected: only intentional changes are present. Do not stage unrelated local files such as `.claude/settings.local.json` or `rrr.md`.

- [ ] **Step 6: Commit validation fixes if needed**

If validation required fixes:

```bash
git add <fixed-files>
git commit -m "fix: validate execution group foundation"
```

If validation required no fixes, do not create an empty commit.

---

## Self-Review Checklist

- Spec coverage:
  - S12 two-leg typed execution group: covered by Tasks 1, 3, 4, and 5.
  - All-leg preflight before dispatch: covered by Task 2.
  - Group-level outcome and exposure report: covered by Task 4.
  - No manual channel / `Arc<Mutex<_>>` in user examples: covered by Task 5.
  - S13 and S24 remain future batches: stated in Scope and docs tasks.

- Placeholder scan:
  - No `TBD`, `TODO`, or "fill in details" instructions.
  - Future work is explicitly excluded or documented as remaining gap.

- Type consistency:
  - `ExecutionGroupBuilder`, `ExecutionLegBuilder`, `ExecutionLegDraft`, `ExecutionGroupTicket`, `ExecutionGroupOutcome`, `ExecutionLegReport`, `ExecutionLegState`, `ExecutionExposure`, and `HedgePolicy` are introduced before use.
  - `TaskHost::execution_group`, `preflight_task_order`, and `submit_prechecked_task_order_once` are defined before group code depends on them.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-27-task-execution-group-foundation.md`.

Recommended execution mode: Inline Execution in the current branch, because this plan refactors the same `tqsdk-task` ownership/order/risk files across multiple tasks and needs tight review between tasks.
