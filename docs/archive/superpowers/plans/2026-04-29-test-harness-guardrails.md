# Test Harness Guardrails Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the missing P0 characterization tests needed before the later structural refactors in the audit remediation roadmap.

**Architecture:** This batch only adds test helpers and tests. It must not change runtime behavior, public API shape, crate responsibilities, or facade ownership. New tests must reinforce the single runtime state tree, revision-bound wait facade semantics, and task-layer risk/ownership routing.

**Tech Stack:** Rust integration tests, existing `tokio` test runtime, `serde_json`, `cargo test -p tqsdk-core`, `cargo test -p tqsdk-task`, `cargo test -p tqsdk-wait`, `cargo test -p tqsdk-session`.

---

## Execution Context

- Worktree: `.worktrees/audit-guardrails`
- Branch: `audit-guardrails`
- Depends on Task 2 commit: `3109ff3 chore: add audit guardrails and safety documentation`

## Existing Coverage To Keep

- `crates/tqsdk-task/tests/target_pos.rs::host_wait_update_timeout_still_advances_target_pos_with_existing_quote` already covers `TaskHost` advancing local tasks when the inner wait facade returns no fresh diff.
- `crates/tqsdk-task/tests/strategy_host.rs::strategy_context_reads_quote_account_and_position` already covers same-step quote/account/position reads.
- `crates/tqsdk-task/tests/risk_orders.rs::task_order_builder_uses_existing_task_ownership_guard` and `legacy_guarded_insert_uses_configured_risk_engine` already cover direct task order ownership/risk paths.
- `crates/tqsdk-wait/tests/wait_api_market.rs::market_refs_read_market_partitions_instead_of_full_snapshot` already guards partition reads for market refs.

## File Structure

- Modify: `crates/tqsdk-core/tests/support/mod.rs`
  - Export a runtime helper module.
- Create: `crates/tqsdk-core/tests/support/runtime.rs`
  - Provide `TestRuntimeBuilder` for integration tests.
- Modify: `crates/tqsdk-core/tests/runtime_contract_order_lifecycle.rs`
  - Use `TestRuntimeBuilder`.
  - Make test mutations use the payload account/order id.
  - Add forward transition, terminal idempotency, and terminal branch tests.
- Modify: `crates/tqsdk-wait/tests/support/core_seed.rs`
  - Add a `TestTqApi` wrapper and keep `seeded_api()` as the compatibility helper.
- Modify: `crates/tqsdk-wait/tests/wait_api_surface.rs`
  - Add a source-level single-state-tree guard for `WaitDriver`.
- Modify: `crates/tqsdk-wait/tests/wait_api_is_changing.rs`
  - Add tests for unconsumed and unrelated commits.
- Modify: `crates/tqsdk-task/tests/strategy_host.rs`
  - Add a StrategyContext order submission risk-gate test.
- Verify: `crates/tqsdk-session/tests/session_direct_query.rs`
  - No code change required in this batch; existing test-only session client coverage remains the session-side fixture guard.

---

## Task 1: Add Core Runtime Test Builder

**Files:**
- Modify: `crates/tqsdk-core/tests/support/mod.rs`
- Create: `crates/tqsdk-core/tests/support/runtime.rs`

- [x] **Step 1: Export `runtime` from the core support module**

Add to `crates/tqsdk-core/tests/support/mod.rs`:

```rust
pub mod runtime;
```

- [x] **Step 2: Create `TestRuntimeBuilder`**

Create `crates/tqsdk-core/tests/support/runtime.rs`:

```rust
use tqsdk_core::{AdapterRegistry, ProtocolAdapter, RuntimeHandle};

pub struct TestRuntimeBuilder {
    adapters: AdapterRegistry,
}

impl Default for TestRuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TestRuntimeBuilder {
    pub fn new() -> Self {
        Self {
            adapters: AdapterRegistry::new(),
        }
    }

    pub fn with_default_adapters(mut self) -> Self {
        self.adapters.register_default_adapters();
        self
    }

    pub fn with_adapter<A>(mut self, adapter: A) -> Self
    where
        A: ProtocolAdapter + 'static,
    {
        self.adapters.register_adapter(adapter);
        self
    }

    pub fn build(self) -> RuntimeHandle {
        RuntimeHandle::with_adapters(self.adapters)
    }
}
```

---

## Task 2: Extend Core Order Lifecycle Tests

**Files:**
- Modify: `crates/tqsdk-core/tests/runtime_contract_order_lifecycle.rs`

- [x] **Step 1: Import support and use `TestRuntimeBuilder`**

Add near the top:

```rust
mod support;

use support::runtime::TestRuntimeBuilder;
```

Replace `runtime_with_order_lifecycle_adapter()` with:

```rust
fn runtime_with_order_lifecycle_adapter() -> RuntimeHandle {
    TestRuntimeBuilder::new()
        .with_adapter(OrderLifecycleAdapter)
        .build()
}
```

- [x] **Step 2: Make `order_mutation` honor payload identity**

Replace the fixed path/object construction with payload-derived account and order ids:

```rust
let account_id = order
    .get("user_id")
    .and_then(Value::as_str)
    .unwrap_or("simnow");
let order_id = order
    .get("order_id")
    .and_then(Value::as_str)
    .unwrap_or("order-1");

NormalizedMutation {
    path: StatePath::new(["trade", account_id, "orders", order_id]),
    object: Some(ObjectKey::Order {
        account_id: AccountId::new(account_id),
        order_id: OrderId::new(order_id),
    }),
    fields,
    source: MutationSource::TradeReply,
}
```

- [x] **Step 3: Add `runtime_accepts_order_lifecycle_forward_progression`**

Add this test:

```rust
#[test]
fn runtime_accepts_order_lifecycle_forward_progression() {
    let handle = runtime_with_order_lifecycle_adapter();

    for order in [
        json!({
            "exchange_id": "SHFE",
            "instrument_id": "au2602",
            "order_id": "order-forward",
            "status": "ALIVE",
            "user_id": "simnow",
            "volume_left": 2,
            "volume_orign": 2
        }),
        json!({
            "exchange_id": "SHFE",
            "instrument_id": "au2602",
            "order_id": "order-forward",
            "status": "ALIVE",
            "user_id": "simnow",
            "volume_left": 1,
            "volume_orign": 2
        }),
        json!({
            "exchange_id": "SHFE",
            "instrument_id": "au2602",
            "is_dead": true,
            "order_id": "order-forward",
            "status": "FINISHED",
            "user_id": "simnow",
            "volume_left": 0,
            "volume_orign": 2
        }),
    ] {
        ingest_order(&handle, order)
            .unwrap()
            .expect("forward lifecycle update should publish a commit");
    }

    assert_eq!(
        lifecycle_value(&handle, "order-forward"),
        Some(json!("filled"))
    );
}
```

- [x] **Step 4: Add `runtime_keeps_duplicate_terminal_order_lifecycle_idempotent`**

Add this test:

```rust
#[test]
fn runtime_keeps_duplicate_terminal_order_lifecycle_idempotent() {
    let handle = runtime_with_order_lifecycle_adapter();
    let terminal = json!({
        "exchange_id": "SHFE",
        "instrument_id": "au2602",
        "is_dead": true,
        "order_id": "order-duplicate-terminal",
        "status": "FINISHED",
        "user_id": "simnow",
        "volume_left": 0,
        "volume_orign": 2
    });

    ingest_order(&handle, terminal.clone())
        .unwrap()
        .expect("first terminal lifecycle update should publish a commit");
    assert!(
        ingest_order(&handle, terminal)
            .unwrap()
            .is_none(),
        "duplicate terminal update should not publish a visible change"
    );
    assert_eq!(
        lifecycle_value(&handle, "order-duplicate-terminal"),
        Some(json!("filled"))
    );
}
```

- [x] **Step 5: Add `runtime_materializes_rejected_failed_and_cancelled_order_lifecycles`**

Add this test:

```rust
#[test]
fn runtime_materializes_rejected_failed_and_cancelled_order_lifecycles() {
    let handle = runtime_with_order_lifecycle_adapter();

    for (order_id, status, expected) in [
        ("order-rejected", "REJECTED", "rejected"),
        ("order-failed", "ERROR", "failed"),
        ("order-cancelled", "CANCELLED", "cancelled"),
    ] {
        ingest_order(
            &handle,
            json!({
                "exchange_id": "SHFE",
                "instrument_id": "au2602",
                "is_dead": true,
                "order_id": order_id,
                "status": status,
                "user_id": "simnow",
                "volume_left": 2,
                "volume_orign": 2
            }),
        )
        .unwrap()
        .expect("terminal branch update should publish a commit");

        assert_eq!(lifecycle_value(&handle, order_id), Some(json!(expected)));
    }
}
```

- [x] **Step 6: Add `lifecycle_value` helper**

Add this helper:

```rust
fn lifecycle_value(handle: &RuntimeHandle, order_id: &str) -> Option<Value> {
    handle
        .latest_snapshot()
        .get(["trade", "simnow", "orders", order_id, "lifecycle"])
        .cloned()
}
```

- [x] **Step 7: Run focused core test**

Run:

```bash
cargo test -p tqsdk-core order_lifecycle
```

Expected: all order lifecycle tests pass.

---

## Task 3: Add Wait Facade Fixture And Change Semantics Tests

**Files:**
- Modify: `crates/tqsdk-wait/tests/support/core_seed.rs`
- Modify: `crates/tqsdk-wait/tests/wait_api_surface.rs`
- Modify: `crates/tqsdk-wait/tests/wait_api_is_changing.rs`

- [x] **Step 1: Add `TestTqApi` while preserving `seeded_api()`**

In `core_seed.rs`, add:

```rust
#[allow(dead_code)]
pub struct TestTqApi {
    api: TqApi,
}

#[allow(dead_code)]
impl TestTqApi {
    pub fn new() -> Self {
        let mut adapters = AdapterRegistry::new();
        adapters.register_default_adapters();

        let handle = RuntimeHandle::with_adapters(adapters);
        let session = SessionClient::new_for_test_with_handle(handle.clone());

        Self {
            api: TqApi::new_for_test(handle, session),
        }
    }

    pub fn api(&self) -> &TqApi {
        &self.api
    }

    pub fn api_mut(&mut self) -> &mut TqApi {
        &mut self.api
    }

    pub fn into_api(self) -> TqApi {
        self.api
    }
}
```

Then change `seeded_api()` to:

```rust
pub fn seeded_api() -> TqApi {
    TestTqApi::new().into_api()
}
```

- [x] **Step 2: Add WaitDriver single-state-tree source guard**

Add to `wait_api_surface.rs`:

```rust
#[test]
fn wait_driver_keeps_single_runtime_reader_without_snapshot_cache() {
    let driver = include_str!("../src/driver.rs");

    assert!(driver.contains("pub(crate) reader: tqsdk_core::RuntimeReader"));
    assert!(driver.contains("pub(crate) cursor: tqsdk_core::UpdateCursor"));
    assert!(!driver.contains("StateSnapshot"));
    assert!(!driver.contains("StateStore"));
}
```

- [x] **Step 3: Add unconsumed commit change test**

Add to `wait_api_is_changing.rs`:

```rust
#[tokio::test(flavor = "current_thread")]
async fn is_changing_is_false_until_a_commit_is_consumed() {
    let mut api = support::seeded_api();
    let quote = api.quote_ref("SHFE.au2602");

    support::seed_quote_commit(&mut api, "SHFE.au2602", 619.0);

    assert!(api.last_commit().is_none());
    assert!(!api.is_changing(&quote).unwrap());
    assert!(!api.is_changing_fields(&quote, &["last_price"]).unwrap());

    assert!(api.wait_update(None).await.unwrap());
    assert!(api.is_changing(&quote).unwrap());
    assert!(api.is_changing_fields(&quote, &["last_price"]).unwrap());
}
```

- [x] **Step 4: Add unrelated commit change test**

Add to `wait_api_is_changing.rs`:

```rust
#[tokio::test(flavor = "current_thread")]
async fn is_changing_ignores_unrelated_commit_paths() {
    let mut api = support::seeded_api();
    let quote = api.quote_ref("SHFE.au2602");

    support::seed_quote_commit(&mut api, "SHFE.ag2602", 8_000.0);

    assert!(api.wait_update(None).await.unwrap());
    assert!(!api.is_changing(&quote).unwrap());
    assert!(!api.is_changing_fields(&quote, &["last_price"]).unwrap());
}
```

- [x] **Step 5: Run focused wait tests**

Run:

```bash
cargo test -p tqsdk-wait --test wait_api_surface
cargo test -p tqsdk-wait --test wait_api_is_changing
```

Expected: targeted wait facade tests pass.

---

## Task 4: Add StrategyContext Risk Gate Coverage

**Files:**
- Modify: `crates/tqsdk-task/tests/strategy_host.rs`

- [x] **Step 1: Extend imports**

Change the task imports to include risk types:

```rust
use tqsdk_task::{RiskEngine, RiskRejection, StrategyHost, TaskError, TaskHost, TaskKind};
```

- [x] **Step 2: Add `strategy_context_orders_apply_risk_gate_before_dispatch`**

Add this test:

```rust
#[tokio::test(flavor = "current_thread")]
async fn strategy_context_orders_apply_risk_gate_before_dispatch() {
    let host = seeded_host().with_risk(RiskEngine::new().max_order_volume(1));
    seed_account_position_quote(&host, "sim", "SHFE.rb2601", 80_000.0, 0, 3_678.0);

    let mut strategy = StrategyHost::builder(host)
        .account("sim")
        .quote("SHFE.rb2601")
        .build()
        .await
        .unwrap();
    let mut ctx = strategy.next_once().await.unwrap();
    ctx.task_host()
        .api()
        .handle_for_test()
        .drain_dispatches()
        .unwrap();

    let err = ctx
        .orders("sim")
        .buy_open("SHFE.rb2601", 2)
        .limit(3_678.0)
        .send_once("strategy-risk-rejected")
        .await
        .unwrap_err();

    assert_eq!(
        err,
        TaskError::RiskRejected(RiskRejection::MaxOrderVolumeExceeded {
            account_id: "sim".to_string(),
            symbol: "SHFE.rb2601".to_string(),
            requested: 2,
            max: 1,
        })
    );
    assert!(
        ctx.task_host()
            .api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
}
```

- [x] **Step 3: Run focused task test**

Run:

```bash
cargo test -p tqsdk-task strategy_context_orders_apply_risk_gate_before_dispatch
```

Expected: focused task test passes.

---

## Task 5: Run Full Targeted Validation And Commit

**Files:**
- Verify: `crates/tqsdk-core/tests`
- Verify: `crates/tqsdk-task/tests`
- Verify: `crates/tqsdk-wait/tests`
- Verify: `crates/tqsdk-session/tests`

- [x] **Step 1: Run targeted high-risk test suites**

Run:

```bash
cargo test -p tqsdk-core
cargo test -p tqsdk-task
cargo test -p tqsdk-wait
cargo test -p tqsdk-session
```

Expected: all four crate test suites pass.

- [x] **Step 2: Commit the test harness guardrails**

Run:

```bash
git add crates/tqsdk-core/tests crates/tqsdk-task/tests crates/tqsdk-wait/tests
git commit -m "test: add guardrails for high-risk runtime and facade modules"
```

Expected: commit succeeds on branch `audit-guardrails`.

---

## Self-Review

- Spec coverage: Core order lifecycle branches, wait facade change semantics, task strategy risk gate, and reusable test fixtures are covered.
- Existing coverage was not duplicated unnecessarily; child plan names exact existing tests that already cover TaskHost no-diff advancement and same-step StrategyContext reads.
- Session crate is verified but not modified because its existing `new_for_test_with_handle` alignment test already covers the session-side helper contract needed for this batch.
