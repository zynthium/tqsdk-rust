# Task Risk And Order Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the next scenario-driven Public API batch for `tqsdk-task`: typed task order entry plus pre-trade risk gates, promoting S19 from API gap to a formal example and reducing S11 simple-strategy boilerplate.

**Architecture:** Keep `tqsdk-core` and `tqsdk-session` unchanged. Add a thin execution/risk layer in `tqsdk-task` that consumes the existing `tqsdk-wait` stable snapshot and `OrderTicket` APIs, reuses current task ownership guards, and does not create a second state tree or private order lifecycle.

**Tech Stack:** Rust, Tokio, `tqsdk-task`, `tqsdk-wait`, existing runtime reader/refs, cargo workspace examples/tests/clippy.

---

## Scope

This batch implements the foundation needed before two-leg arbitrage and multi-account execution:

- S19 风控前置: typed risk rules, typed rejection reasons, stable snapshot checks, formal example.
- S11 简单策略: typed task order entry with client intent, no `serde_json::Value` in user-facing strategy code.
- S12/S13: remain explicit gaps in this batch; do not implement `ExecutionGroup`, `HedgePolicy`, `AccountGroup`, or allocation policy here.

## File Map

- Create `crates/tqsdk-task/src/order.rs`
  - Owns typed task-level order builder and `TaskOrderIntent` snapshot used by risk checks.
  - Delegates actual submission to `tqsdk_wait::LimitOrderIntent::send_once()`.

- Create `crates/tqsdk-task/src/risk.rs`
  - Owns `RiskEngine`, typed rule constructors, `RiskDecision`, and `RiskRejection`.
  - Reads current account/position/quote through `TqApi` refs without storing private state.

- Modify `crates/tqsdk-task/src/host.rs`
  - Add optional `RiskEngine` to `TaskHost`.
  - Add `TaskHost::with_risk`, `TaskHost::set_risk`, `TaskHost::risk`, `TaskHost::orders`.
  - Ensure legacy `insert_order_guarded` also goes through risk when a risk engine is configured.

- Modify `crates/tqsdk-task/src/error.rs`
  - Add typed `TaskError::RiskRejected(RiskRejection)`.
  - Keep ownership errors separate from risk rejection errors.

- Modify `crates/tqsdk-task/src/lib.rs`
  - Export `RiskEngine`, `RiskDecision`, `RiskRejection`, `TaskOrderBuilder`, `TaskOrderIntent`.

- Add tests in `crates/tqsdk-task/tests/risk_orders.rs`
  - Cover typed order builder, ownership guard integration, risk rejection, same-snapshot quote/account/position reads, and legacy guarded insert risk enforcement.

- Create `crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs`
  - Formal scenario contract example for S19.

- Modify `crates/tqsdk-task/examples/api_contract_s11_simple_strategy.rs`
  - Update the Current API note to show the new typed order/risk entry and clarify that a full `StrategyHost` remains later work.

- Modify `docs/scenarios/api_gaps/api_contract_s19_pre_trade_risk.rs`
  - Replace the current broad gap with a narrower note: what-if margin simulation and portfolio-level limits remain future work.

- Modify `docs/reviews/public-api-scenario-review.md`
  - Move S19 from `不自然` to `自然` or `勉强` depending on final example shape.
  - Update S11 evidence to include `TaskHost::orders` and `RiskEngine`.

- Modify `docs/scenarios/user-layer-iteration-plan.md`
  - Mark the S19 foundation as landed.
  - Keep S12/S13 as next execution-group/account-group work.

- Modify `docs/architecture/api-task.md` and `crates/tqsdk-task/README.md`
  - Document the task-level order builder and risk gate placement.

---

### Task 1: Typed Task Order Builder

**Files:**
- Create: `crates/tqsdk-task/src/order.rs`
- Modify: `crates/tqsdk-task/src/host.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`
- Test: `crates/tqsdk-task/tests/risk_orders.rs`

- [ ] **Step 1: Write the failing builder test**

Add `crates/tqsdk-task/tests/risk_orders.rs`:

```rust
use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, OutboundFrame, OutboundRequest,
    ProtocolDomain, RuntimeHandle, RuntimeInput, TradeDirection, TradeOffset,
};
use tqsdk_session::SessionClient;
use tqsdk_task::{TaskError, TaskHost, TaskKind};
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
async fn task_order_builder_submits_typed_client_intent_without_json_price() {
    let mut host = seeded_host();

    let ticket = host
        .orders("sim")
        .buy_open("SHFE.rb2601", 2)
        .limit(3678.0)
        .send_once("strategy-entry-001")
        .await
        .unwrap();

    assert!(ticket.was_submitted());
    assert_eq!(ticket.client_order_id().as_str(), "strategy-entry-001");

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "insert_order");
    assert_eq!(payload["user_id"], "sim");
    assert_eq!(payload["exchange_id"], "SHFE");
    assert_eq!(payload["instrument_id"], "rb2601");
    assert_eq!(payload["volume"], 2);
    assert_eq!(payload["limit_price"], 3678.0);
}

#[tokio::test(flavor = "current_thread")]
async fn task_order_builder_uses_existing_task_ownership_guard() {
    let mut host = seeded_host();
    let _task = host.target_pos("sim", "SHFE.rb2601").build().unwrap();

    let err = host
        .orders("sim")
        .buy_open("SHFE.rb2601", 1)
        .limit(3678.0)
        .send_once("blocked-entry")
        .await
        .unwrap_err();

    assert_eq!(
        err,
        TaskError::ManualOrderBlocked {
            account_id: "sim".to_string(),
            symbol: "SHFE.rb2601".to_string(),
            active_task_kind: TaskKind::TargetPos,
        }
    );
    assert!(host.api().handle_for_test().drain_dispatches().unwrap().is_empty());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p tqsdk-task --test risk_orders task_order_builder -- --nocapture
```

Expected: FAIL because `risk_orders.rs` imports `TaskHost::orders`, `TaskOrderBuilder`, or `TaskOrderIntent` APIs that do not exist yet.

- [ ] **Step 3: Implement `order.rs`**

Create `crates/tqsdk-task/src/order.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk_core::{TradeDirection, TradeOffset};
use tqsdk_wait::{ClientOrderId, OrderTicket};

use crate::{Result, TaskError, TaskHost};

#[derive(Debug, Clone, PartialEq)]
pub struct TaskOrderIntent {
    pub account_id: String,
    pub symbol: String,
    pub direction: TradeDirection,
    pub offset: Option<TradeOffset>,
    pub volume: i64,
    pub limit_price: Option<f64>,
}

pub struct TaskOrderBuilder<'a> {
    host: &'a mut TaskHost,
    account_id: String,
}

pub struct TaskOrderDraft<'a> {
    host: &'a mut TaskHost,
    intent: TaskOrderIntent,
}

impl<'a> TaskOrderBuilder<'a> {
    pub(crate) fn new(host: &'a mut TaskHost, account_id: String) -> Self {
        Self { host, account_id }
    }

    pub fn buy_open(self, symbol: impl AsRef<str>, volume: i64) -> TaskOrderDraft<'a> {
        self.intent(symbol, TradeDirection::Buy, Some(TradeOffset::Open), volume)
    }

    pub fn sell_open(self, symbol: impl AsRef<str>, volume: i64) -> TaskOrderDraft<'a> {
        self.intent(symbol, TradeDirection::Sell, Some(TradeOffset::Open), volume)
    }

    pub fn buy_close(self, symbol: impl AsRef<str>, volume: i64) -> TaskOrderDraft<'a> {
        self.intent(symbol, TradeDirection::Buy, Some(TradeOffset::Close), volume)
    }

    pub fn sell_close(self, symbol: impl AsRef<str>, volume: i64) -> TaskOrderDraft<'a> {
        self.intent(symbol, TradeDirection::Sell, Some(TradeOffset::Close), volume)
    }

    fn intent(
        self,
        symbol: impl AsRef<str>,
        direction: TradeDirection,
        offset: Option<TradeOffset>,
        volume: i64,
    ) -> TaskOrderDraft<'a> {
        TaskOrderDraft {
            host: self.host,
            intent: TaskOrderIntent {
                account_id: self.account_id,
                symbol: symbol.as_ref().to_owned(),
                direction,
                offset,
                volume,
                limit_price: None,
            },
        }
    }
}

impl TaskOrderDraft<'_> {
    pub fn intent(&self) -> &TaskOrderIntent {
        &self.intent
    }

    pub fn limit(mut self, price: f64) -> Result<Self> {
        if !price.is_finite() {
            return Err(TaskError::InvalidState("limit price must be finite"));
        }
        self.intent.limit_price = Some(price);
        Ok(self)
    }

    pub async fn send_once(
        self,
        client_order_id: impl AsRef<str>,
    ) -> Result<OrderTicket> {
        self.host.submit_task_order_once(self.intent, ClientOrderId::new(client_order_id.as_ref()))
            .await
    }
}
```

- [ ] **Step 4: Wire builder into `TaskHost` and exports**

Modify `crates/tqsdk-task/src/lib.rs`:

```rust
mod order;
pub use order::{TaskOrderBuilder, TaskOrderIntent};
```

Modify `crates/tqsdk-task/src/host.rs`:

```rust
use crate::order::{TaskOrderBuilder, TaskOrderIntent};
```

Add methods inside `impl TaskHost`:

```rust
#[must_use]
pub fn orders(&mut self, account_id: impl AsRef<str>) -> TaskOrderBuilder<'_> {
    TaskOrderBuilder::new(self, account_id.as_ref().to_owned())
}

pub(crate) async fn submit_task_order_once(
    &mut self,
    intent: TaskOrderIntent,
    client_order_id: tqsdk_wait::ClientOrderId,
) -> Result<tqsdk_wait::OrderTicket> {
    self.registry
        .with(|registry| registry.check_manual_order_allowed(&intent.account_id, &intent.symbol))?;

    let mut builder = self
        .api
        .limit_order(&intent.account_id, &intent.symbol)
        .client_intent(client_order_id.as_str());

    builder = match (intent.direction, intent.offset) {
        (tqsdk_core::TradeDirection::Buy, Some(tqsdk_core::TradeOffset::Open)) => builder.buy_open(intent.volume),
        (tqsdk_core::TradeDirection::Sell, Some(tqsdk_core::TradeOffset::Open)) => builder.sell_open(intent.volume),
        (tqsdk_core::TradeDirection::Buy, Some(tqsdk_core::TradeOffset::Close)) => builder.buy_close(intent.volume),
        (tqsdk_core::TradeDirection::Sell, Some(tqsdk_core::TradeOffset::Close)) => builder.sell_close(intent.volume),
        _ => return Err(TaskError::Unsupported("task order builder requires explicit open/close offset")),
    };

    if let Some(limit_price) = intent.limit_price {
        builder = builder.at(limit_price);
    }

    builder.send_once().await.map_err(Into::into)
}
```

If the exact `LimitOrderIntent` method names differ, adapt this step to the names already used by `crates/tqsdk-wait/examples/api_contract_s06_limit_order.rs` and `api_contract_s10_reconnect_order_consistency.rs`.

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -p tqsdk-task --test risk_orders task_order_builder -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/tqsdk-task/src/order.rs crates/tqsdk-task/src/host.rs crates/tqsdk-task/src/lib.rs crates/tqsdk-task/tests/risk_orders.rs
git commit -m "feat(task): add typed task order builder"
```

---

### Task 2: Pre-Trade Risk Engine

**Files:**
- Create: `crates/tqsdk-task/src/risk.rs`
- Modify: `crates/tqsdk-task/src/error.rs`
- Modify: `crates/tqsdk-task/src/host.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`
- Test: `crates/tqsdk-task/tests/risk_orders.rs`

- [ ] **Step 1: Add failing risk tests**

Append to `crates/tqsdk-task/tests/risk_orders.rs`:

```rust
use tqsdk_task::{RiskEngine, RiskRejection};

fn seed_account_position_quote(
    host: &TaskHost,
    account_id: &str,
    symbol: &str,
    available: f64,
    long_volume: i64,
    short_volume: i64,
    last_price: f64,
) {
    let (exchange_id, instrument_id) = symbol
        .split_once('.')
        .expect("test symbol should contain exchange prefix");
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
                                        "currency": "CNY",
                                        "available": available
                                    }
                                },
                                "positions": {
                                    symbol: {
                                        "user_id": account_id,
                                        "exchange_id": exchange_id,
                                        "instrument_id": instrument_id,
                                        "volume_long": long_volume,
                                        "volume_short": short_volume,
                                        "pos": long_volume - short_volume,
                                        "pos_long": long_volume,
                                        "pos_short": short_volume
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
        .expect("seed trade state should commit");

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
                                "instrument_id": symbol,
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
        .expect("seed quote should commit");
}

#[tokio::test(flavor = "current_thread")]
async fn risk_engine_rejects_oversized_order_before_dispatch() {
    let mut host = seeded_host().with_risk(RiskEngine::new().max_order_volume(3));

    let err = host
        .orders("sim")
        .buy_open("SHFE.rb2601", 4)
        .limit(3678.0)
        .send_once("oversized-entry")
        .await
        .unwrap_err();

    assert_eq!(
        err,
        TaskError::RiskRejected(RiskRejection::MaxOrderVolume {
            volume: 4,
            max_volume: 3,
        })
    );
    assert!(host.api().handle_for_test().drain_dispatches().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn risk_engine_checks_account_position_and_quote_before_dispatch() {
    let mut host = seeded_host().with_risk(
        RiskEngine::new()
            .min_available(1_000.0)
            .max_net_position("SHFE.rb2601", 5)
            .max_price_deviation("SHFE.rb2601", 20.0),
    );
    seed_account_position_quote(&host, "sim", "SHFE.rb2601", 2_000.0, 4, 0, 3660.0);

    let ticket = host
        .orders("sim")
        .buy_open("SHFE.rb2601", 1)
        .limit(3678.0)
        .send_once("risk-ok-entry")
        .await
        .unwrap();

    assert!(ticket.was_submitted());
    assert_eq!(host.api().handle_for_test().drain_dispatches().unwrap().len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn risk_engine_rejects_price_outside_quote_band() {
    let mut host = seeded_host().with_risk(
        RiskEngine::new().max_price_deviation("SHFE.rb2601", 10.0),
    );
    seed_account_position_quote(&host, "sim", "SHFE.rb2601", 2_000.0, 0, 0, 3660.0);

    let err = host
        .orders("sim")
        .buy_open("SHFE.rb2601", 1)
        .limit(3678.0)
        .send_once("price-band-entry")
        .await
        .unwrap_err();

    assert_eq!(
        err,
        TaskError::RiskRejected(RiskRejection::PriceDeviation {
            symbol: "SHFE.rb2601".to_string(),
            limit_price: 3678.0,
            reference_price: 3660.0,
            max_deviation: 10.0,
        })
    );
    assert!(host.api().handle_for_test().drain_dispatches().unwrap().is_empty());
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test -p tqsdk-task --test risk_orders risk_engine -- --nocapture
```

Expected: FAIL because `RiskEngine`, `RiskRejection`, `TaskError::RiskRejected`, and `TaskHost::with_risk` do not exist.

- [ ] **Step 3: Implement `risk.rs`**

Create `crates/tqsdk-task/src/risk.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::BTreeMap;

use tqsdk_core::{TradeDirection, TradeOffset};

use crate::order::TaskOrderIntent;
use crate::{Result, TaskError};

#[derive(Debug, Clone, PartialEq)]
pub enum RiskRejection {
    MaxOrderVolume {
        volume: i64,
        max_volume: i64,
    },
    MissingAccount {
        account_id: String,
    },
    MinAvailable {
        account_id: String,
        available: f64,
        min_available: f64,
    },
    MissingPosition {
        account_id: String,
        symbol: String,
    },
    MaxNetPosition {
        account_id: String,
        symbol: String,
        projected_net: i64,
        max_abs_net: i64,
    },
    MissingQuote {
        symbol: String,
    },
    PriceDeviation {
        symbol: String,
        limit_price: f64,
        reference_price: f64,
        max_deviation: f64,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum RiskDecision {
    Accepted,
    Rejected(RiskRejection),
}

#[derive(Debug, Clone, Default)]
pub struct RiskEngine {
    max_order_volume: Option<i64>,
    min_available: Option<f64>,
    max_net_position_by_symbol: BTreeMap<String, i64>,
    max_price_deviation_by_symbol: BTreeMap<String, f64>,
}

impl RiskEngine {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn max_order_volume(mut self, max_volume: i64) -> Self {
        self.max_order_volume = Some(max_volume);
        self
    }

    #[must_use]
    pub fn min_available(mut self, min_available: f64) -> Self {
        self.min_available = Some(min_available);
        self
    }

    #[must_use]
    pub fn max_net_position(mut self, symbol: impl Into<String>, max_abs_net: i64) -> Self {
        self.max_net_position_by_symbol.insert(symbol.into(), max_abs_net);
        self
    }

    #[must_use]
    pub fn max_price_deviation(mut self, symbol: impl Into<String>, max_deviation: f64) -> Self {
        self.max_price_deviation_by_symbol
            .insert(symbol.into(), max_deviation);
        self
    }

    pub(crate) fn check(
        &self,
        api: &tqsdk_wait::TqApi,
        intent: &TaskOrderIntent,
    ) -> Result<RiskDecision> {
        if let Some(max_volume) = self.max_order_volume
            && intent.volume > max_volume
        {
            return Ok(RiskDecision::Rejected(RiskRejection::MaxOrderVolume {
                volume: intent.volume,
                max_volume,
            }));
        }

        if let Some(min_available) = self.min_available {
            let account = api
                .get_account(&intent.account_id)
                .snapshot(api)?
                .ok_or_else(|| TaskError::RiskRejected(RiskRejection::MissingAccount {
                    account_id: intent.account_id.clone(),
                }))?;
            if account.available < min_available {
                return Ok(RiskDecision::Rejected(RiskRejection::MinAvailable {
                    account_id: intent.account_id.clone(),
                    available: account.available,
                    min_available,
                }));
            }
        }

        if let Some(max_abs_net) = self.max_net_position_by_symbol.get(&intent.symbol).copied() {
            let position = api
                .get_position(&intent.account_id, &intent.symbol)
                .snapshot(api)?
                .ok_or_else(|| TaskError::RiskRejected(RiskRejection::MissingPosition {
                    account_id: intent.account_id.clone(),
                    symbol: intent.symbol.clone(),
                }))?;
            let projected_net = projected_net_position(position.pos, intent);
            if projected_net.abs() > max_abs_net {
                return Ok(RiskDecision::Rejected(RiskRejection::MaxNetPosition {
                    account_id: intent.account_id.clone(),
                    symbol: intent.symbol.clone(),
                    projected_net,
                    max_abs_net,
                }));
            }
        }

        if let Some(max_deviation) = self
            .max_price_deviation_by_symbol
            .get(&intent.symbol)
            .copied()
        {
            let quote = api
                .get_quote(&intent.symbol)
                .snapshot(api)?
                .ok_or_else(|| TaskError::RiskRejected(RiskRejection::MissingQuote {
                    symbol: intent.symbol.clone(),
                }))?;
            let Some(limit_price) = intent.limit_price else {
                return Ok(RiskDecision::Accepted);
            };
            if (limit_price - quote.last_price).abs() > max_deviation {
                return Ok(RiskDecision::Rejected(RiskRejection::PriceDeviation {
                    symbol: intent.symbol.clone(),
                    limit_price,
                    reference_price: quote.last_price,
                    max_deviation,
                }));
            }
        }

        Ok(RiskDecision::Accepted)
    }
}

fn projected_net_position(current_net: i64, intent: &TaskOrderIntent) -> i64 {
    match (intent.direction, intent.offset) {
        (TradeDirection::Buy, Some(TradeOffset::Open)) => current_net + intent.volume,
        (TradeDirection::Sell, Some(TradeOffset::Open)) => current_net - intent.volume,
        (TradeDirection::Buy, Some(TradeOffset::Close)) => current_net + intent.volume,
        (TradeDirection::Sell, Some(TradeOffset::Close)) => current_net - intent.volume,
        _ => current_net,
    }
}
```

- [ ] **Step 4: Wire risk into error, host, and exports**

Modify `crates/tqsdk-task/src/error.rs`:

```rust
use crate::risk::RiskRejection;

pub enum TaskError {
    RiskRejected(RiskRejection),
    // keep existing variants
}
```

Add to `Display`:

```rust
Self::RiskRejected(rejection) => write!(f, "pre-trade risk rejected order: {rejection:?}"),
```

Add to `source()` no-source group:

```rust
| Self::RiskRejected(_)
```

Modify `crates/tqsdk-task/src/lib.rs`:

```rust
mod risk;
pub use risk::{RiskDecision, RiskEngine, RiskRejection};
```

Modify `TaskHost` in `crates/tqsdk-task/src/host.rs`:

```rust
risk: Option<crate::risk::RiskEngine>,
```

Initialize in `TaskHost::new`:

```rust
risk: None,
```

Add methods:

```rust
#[must_use]
pub fn with_risk(mut self, risk: crate::risk::RiskEngine) -> Self {
    self.risk = Some(risk);
    self
}

pub fn set_risk(&mut self, risk: crate::risk::RiskEngine) {
    self.risk = Some(risk);
}

#[must_use]
pub fn risk(&self) -> Option<&crate::risk::RiskEngine> {
    self.risk.as_ref()
}
```

At the start of `submit_task_order_once`, after ownership check and before constructing wait order:

```rust
if let Some(risk) = self.risk.as_ref()
    && let crate::risk::RiskDecision::Rejected(rejection) = risk.check(&self.api, &intent)?
{
    return Err(TaskError::RiskRejected(rejection));
}
```

- [ ] **Step 5: Run risk tests**

Run:

```bash
cargo test -p tqsdk-task --test risk_orders risk_engine -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/tqsdk-task/src/risk.rs crates/tqsdk-task/src/error.rs crates/tqsdk-task/src/host.rs crates/tqsdk-task/src/lib.rs crates/tqsdk-task/tests/risk_orders.rs
git commit -m "feat(task): add pre-trade risk engine"
```

---

### Task 3: Apply Risk To Legacy Guarded Orders

**Files:**
- Modify: `crates/tqsdk-task/src/host.rs`
- Test: `crates/tqsdk-task/tests/risk_orders.rs`

- [ ] **Step 1: Add failing legacy guarded insert test**

Append:

```rust
#[tokio::test(flavor = "current_thread")]
async fn legacy_guarded_insert_uses_configured_risk_engine() {
    let mut host = seeded_host().with_risk(RiskEngine::new().max_order_volume(1));

    let err = host
        .insert_order_guarded(
            "sim",
            "SHFE.rb2601",
            TradeDirection::Buy,
            Some(TradeOffset::Open),
            2,
            Some(json!(3678.0)),
        )
        .await
        .unwrap_err();

    assert_eq!(
        err,
        TaskError::RiskRejected(RiskRejection::MaxOrderVolume {
            volume: 2,
            max_volume: 1,
        })
    );
    assert!(host.api().handle_for_test().drain_dispatches().unwrap().is_empty());
}
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
cargo test -p tqsdk-task --test risk_orders legacy_guarded_insert -- --nocapture
```

Expected: FAIL because `insert_order_guarded` bypasses `RiskEngine`.

- [ ] **Step 3: Update `insert_order_guarded`**

In `crates/tqsdk-task/src/host.rs`, after ownership check and before `self.api.insert_order(...)`, construct a `TaskOrderIntent`:

```rust
let limit_price = limit_price
    .as_ref()
    .and_then(serde_json::Value::as_f64);
let intent = TaskOrderIntent {
    account_id: account_id.clone(),
    symbol: symbol.clone(),
    direction,
    offset,
    volume,
    limit_price,
};

if let Some(risk) = self.risk.as_ref()
    && let crate::risk::RiskDecision::Rejected(rejection) = risk.check(&self.api, &intent)?
{
    return Err(TaskError::RiskRejected(rejection));
}
```

- [ ] **Step 4: Run guarded order tests**

Run:

```bash
cargo test -p tqsdk-task --test guarded_orders
cargo test -p tqsdk-task --test risk_orders legacy_guarded_insert -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/tqsdk-task/src/host.rs crates/tqsdk-task/tests/risk_orders.rs
git commit -m "feat(task): enforce risk on guarded orders"
```

---

### Task 4: Promote S19 Formal Example And Update Review Docs

**Files:**
- Create: `crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs`
- Modify: `docs/scenarios/api_gaps/api_contract_s19_pre_trade_risk.rs`
- Modify: `docs/reviews/public-api-scenario-review.md`
- Modify: `docs/scenarios/user-layer-iteration-plan.md`
- Modify: `docs/architecture/api-task.md`
- Modify: `crates/tqsdk-task/README.md`

- [ ] **Step 1: Create formal S19 example**

Create `crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs`:

```rust
//! Scenario: 风控前置
//!
//! User goal:
//! - 下单前检查资金、持仓、价格、限额
//! - 拒绝不安全订单
//! - 留下可审计的拒绝原因
//!
//! API contract:
//! - 风控规则是 typed public API
//! - 下单入口能强制经过 risk gate
//! - 风控读取账户/持仓/quote 时使用同一稳定截面
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 用户在策略里散写 if 判断作为唯一风控
//! - `serde_json::Value` 表达订单价格
//! - 字符串判断订单状态或风控拒绝原因
//! - `RuntimeCommand::Trade`
//!
//! Regression signal:
//! - 下单前资金/持仓读取不是同一 revision
//! - 规则拒绝原因不可审计
//! - guarded 和 unguarded order API 容易混用
//!
//! Review questions:
//! - 当前 API 是否自然表达前置风控？
//! - 是否存在资金安全风险？
//! - 后续 what-if 试算应进入 task 还是 data/tooling？

use std::time::Duration;

use tqsdk_core::TradeAccountType;
use tqsdk_task::{RiskEngine, TaskError, TaskHost};
use tqsdk_wait::TqApiBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let broker_id = std::env::var("TQ_BROKER_ID")?;
    let account_id = std::env::var("TQ_ACCOUNT_ID")?;
    let account_password = std::env::var("TQ_ACCOUNT_PASSWORD")?;
    let symbol = std::env::var("TQ_ORDER_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".into());
    let limit_price = std::env::var("TQ_ORDER_LIMIT_PRICE")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(480.0);

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

    let risk = RiskEngine::new()
        .max_order_volume(3)
        .min_available(1_000.0)
        .max_net_position(symbol.as_str(), 5)
        .max_price_deviation(symbol.as_str(), 20.0);
    let mut host = TaskHost::new(api).with_risk(risk);

    match host
        .orders(account_id.as_str())
        .buy_open(symbol.as_str(), 1)
        .limit(limit_price)?
        .send_once("risk-checked-entry-001")
        .await
    {
        Ok(ticket) => {
            let state = ticket.wait_reconnect_safe_terminal(host.api_mut()).await?;
            println!("order state: {state:?}");
        }
        Err(TaskError::RiskRejected(rejection)) => {
            println!("risk rejected: {rejection:?}");
        }
        Err(error) => return Err(error.into()),
    }

    Ok(())
}
```

- [ ] **Step 2: Update S19 gap file**

In `docs/scenarios/api_gaps/api_contract_s19_pre_trade_risk.rs`, change `API gap` section to:

```rust
//! API gap:
//! `tqsdk-task::RiskEngine` covers typed pre-trade checks for max order volume,
//! available funds snapshot, symbol net position and quote-relative price bands.
//! Remaining gap: portfolio-level limits, exchange contract metadata rules,
//! margin what-if simulation and cross-account aggregate risk.
```

- [ ] **Step 3: Update review matrix**

In `docs/reviews/public-api-scenario-review.md`, update S19 row to:

```markdown
| 19. 风控前置 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs`; `TaskHost::orders`; `RiskEngine`; `RiskRejection`; remaining what-if/portfolio risk stays in gap |
```

Update main conclusions to say task now has typed pre-trade guard foundation.

- [ ] **Step 4: Update iteration plan**

In `docs/scenarios/user-layer-iteration-plan.md`, under `P1：风控前置与 what-if 试算`, add:

```markdown
已落地：

- `tqsdk-task::RiskEngine` 提供 typed pre-trade rules：单笔手数、可用资金快照、symbol net position 和 quote-relative price band。
- `TaskHost::orders(...).buy_open(...).limit(...).send_once(...)` 会经过 ownership guard 和 risk gate，再委托 wait 层 `OrderTicket`。

仍未完成：

- portfolio-level aggregate risk。
- exchange contract metadata based rules。
- margin / position what-if simulation。
- cross-account aggregate risk。
```

- [ ] **Step 5: Update task docs**

In `docs/architecture/api-task.md` and `crates/tqsdk-task/README.md`, add a short section:

```markdown
### Pre-Trade Risk

`RiskEngine` belongs to `tqsdk-task` because it is execution tooling, not runtime protocol substrate. It reads account, position and quote through the wait facade's stable state surface, returns typed `RiskRejection`, and does not own a private state tree.

Prefer:

```rust
let risk = RiskEngine::new()
    .max_order_volume(3)
    .min_available(1_000.0)
    .max_net_position("SHFE.au2602", 5)
    .max_price_deviation("SHFE.au2602", 20.0);
let mut host = TaskHost::new(api).with_risk(risk);
host.orders("sim")
    .buy_open("SHFE.au2602", 1)
    .limit(480.0)?
    .send_once("entry-001")
    .await?;
```
```

- [ ] **Step 6: Validate examples**

Run:

```bash
cargo check -p tqsdk-task --examples
cargo test -p tqsdk-task --test risk_orders
cargo clippy -p tqsdk-task --examples --all-targets -- -D warnings
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs docs/scenarios/api_gaps/api_contract_s19_pre_trade_risk.rs docs/reviews/public-api-scenario-review.md docs/scenarios/user-layer-iteration-plan.md docs/architecture/api-task.md crates/tqsdk-task/README.md
git commit -m "docs(task): promote pre-trade risk scenario"
```

---

### Task 5: Reassess S11 Simple Strategy Contract

**Files:**
- Modify: `crates/tqsdk-task/examples/api_contract_s11_simple_strategy.rs`
- Modify: `docs/reviews/public-api-scenario-review.md`
- Modify: `docs/scenarios/user-layer-iteration-plan.md`

- [ ] **Step 1: Update S11 example note**

Replace the `Current API note` block in `crates/tqsdk-task/examples/api_contract_s11_simple_strategy.rs` with:

```rust
//! Current API note:
//! `TaskHost + TargetPosTask` covers target-position style simple strategies.
//! `TaskHost::orders` plus `RiskEngine` now covers signal-driven, risk-checked
//! one-shot order tickets. A full `StrategyHost::next()` context that unifies
//! quote/order/position/take-profit/stop-loss remains a later strategy runtime
//! layer and is not implemented in this batch.
```

- [ ] **Step 2: Update review row**

In `docs/reviews/public-api-scenario-review.md`, update S11 evidence to include:

```markdown
`TaskHost::orders`; `RiskEngine`; no full StrategyHost yet
```

Keep S11 as `勉强` unless a formal `StrategyHost` API is implemented in a later batch.

- [ ] **Step 3: Update iteration plan**

In `docs/scenarios/user-layer-iteration-plan.md`, under `P1：执行层抽象`, add:

```markdown
已落地的基础能力：

- signal-driven one-shot task order entry uses `TaskHost::orders`.
- pre-trade guard uses `RiskEngine`.

仍未完成：

- full `StrategyHost` context.
- take-profit / stop-loss helper.
- execution group and hedge policy.
- account group allocation.
```

- [ ] **Step 4: Validate docs/examples**

Run:

```bash
cargo check -p tqsdk-task --examples
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/tqsdk-task/examples/api_contract_s11_simple_strategy.rs docs/reviews/public-api-scenario-review.md docs/scenarios/user-layer-iteration-plan.md
git commit -m "docs(task): reassess simple strategy scenario"
```

---

### Task 6: Workspace Validation And Batch Closure

**Files:**
- No source changes unless validation exposes a bug.

- [ ] **Step 1: Run required workspace checks**

```bash
cargo check --workspace --examples
cargo test --workspace
cargo clippy --workspace --examples --all-targets -- -D warnings
```

Expected: all pass.

- [ ] **Step 2: Run formatting and whitespace checks**

```bash
cargo fmt -p tqsdk-task --check
git diff --check
git status --short
```

Expected:

- `cargo fmt` passes.
- `git diff --check` has no output.
- `git status --short` shows only intentionally untracked user files, if any.

- [ ] **Step 3: Final commit if validation required fixes**

If validation required any code/doc fix, commit only those files:

```bash
git add <changed-files-from-this-batch>
git commit -m "fix(task): stabilize risk order scenario"
```

If no validation fixes were required, do not create an empty commit.

---

## Success Criteria

At the end of this batch:

- `TaskHost::orders(...).buy_open(...).limit(...).send_once(...)` is public and compiles.
- `RiskEngine` is public and returns typed `RiskRejection`.
- S19 has a formal compiling example under `crates/tqsdk-task/examples/`.
- `docs/scenarios/api_gaps/api_contract_s19_pre_trade_risk.rs` is narrowed to remaining what-if/portfolio-level gaps.
- S11 review is updated to reflect improved order/risk foundation but still no full `StrategyHost`.
- S12/S13 are not partially implemented; they remain clear next-batch gaps.
- Required workspace validation passes.

## Follow-Up Batch After This Plan

After this foundation lands, the next scenario-driven batch should choose one:

1. **Execution Group Foundation for S12**
   - `ExecutionGroup`
   - `ExecutionLeg`
   - `HedgePolicy::FlattenFilledLegs`
   - group-level `ExecutionOutcome`

2. **Account Group Foundation for S13**
   - `AccountGroup`
   - `AllocationPolicy`
   - per-account `OrderTicketState`
   - aggregate `MultiAccountOutcome`

3. **Strategy Runtime/Testability for S11/S15/S24**
   - `StrategyHost`
   - fake market/fake broker public test support
   - deterministic strategy tick loop

