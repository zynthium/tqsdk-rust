# Session Public API Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the newly reviewed public API footguns around session outbox draining and hidden runtime leakage, and make TqKq numbered account validation fail at builder time.

**Architecture:** Keep `tqsdk-session` as the shared session + one-shot request/response layer. Live session driving must continue through `progress_once()` / `flush_outbound()` and the internal `SessionRuntime`; users must not be encouraged to drain the runtime outbox manually on a live client. Keep `tqsdk_core::internal` as a sibling-crate bridge only, and do not move TqKq auth material into `tqsdk-core` public API.

**Tech Stack:** Rust 2024, Cargo workspace, `tqsdk-session` builder/client tests, `tqsdk-wait` internal driver cleanup, architecture README alignment.

---

## Scope

Included in this batch:

- Guard `SessionClient::drain_dispatches()` so it cannot silently consume outbox work from a live session client.
- Hide `drain_dispatches()` from rustdoc because it is only for test/manual no-IO clients.
- Remove the public `SessionClient::runtime()` and `SessionClient::runtime_clone()` methods that leak `tqsdk_core::internal::SessionRuntime`.
- Refactor `tqsdk-wait` so it no longer stores or needs a cloned `SessionRuntime`.
- Validate TqKq numbered trade targets during `SessionClientBuilder::build()`, before live session construction.
- Update docs that currently imply `SessionRuntime` is a stable public user-facing surface.

Excluded from this batch:

- `tqsdk-data` MarketCache API narrowing. That remains a separate S18 cache API redesign.
- `tqsdk-stream` WAL/journal API narrowing. That remains a separate S21 durability API redesign.
- `tqsdk-task` strategy/report/status reshaping. That remains a separate task API compatibility plan.
- Changing `TradeSessionTarget::tqkq_numbered()` to return `Result`. This plan keeps the core transport DTO infallible and validates at the session builder boundary.
- Adding a new public `try_tqkq_numbered()` API. Do this only if a later compatibility review asks for constructor-time validation.

## File Structure

- Modify: `crates/tqsdk-session/src/client.rs`
  - Add the live-client guard in `drain_dispatches()`.
  - Mark `drain_dispatches()` as `#[doc(hidden)]`.
  - Remove `runtime()` and `runtime_clone()` public methods after `tqsdk-wait` no longer uses them.
- Modify: `crates/tqsdk-session/src/builder.rs`
  - Add private validation for auth-derived TqKq numbered targets.
  - Call validation at the top of `SessionClientBuilder::build()`.
- Modify: `crates/tqsdk-session/tests/session_builder.rs`
  - Add regression tests for invalid numbered TqKq targets and live-client outbox drain rejection.
- Modify: `crates/tqsdk-session/tests/session_market_command_helpers.rs`
  - Keep coverage proving no-IO test/manual clients can still drain dispatches.
- Modify: `crates/tqsdk-wait/src/driver.rs`
  - Remove the `SessionRuntime` import and `WaitDriver::runtime` field.
- Modify: `crates/tqsdk-wait/src/api.rs`
  - Stop calling `SessionClient::runtime_clone()`.
  - Change `handle_for_test()` to return `self.driver.session.handle().clone()`.
- Modify: `crates/tqsdk-core/README.md`
  - Remove `SessionRuntime` from the stable public surface table.
  - Keep the note that `tqsdk_core::internal` is a hidden sibling-crate bridge.
- Optional modify: `docs/archive/reviews/2026-04-29/public-api-overdesign-audit.md`
  - Only update if implementation changes the closure status of a listed public API finding.
- Do not modify: `docs/architecture/*`
  - This plan hardens API visibility without changing crate responsibilities or runtime architecture.

---

## Task 1: Add Public API Guardrail Tests

**Files:**
- Modify: `crates/tqsdk-session/tests/session_builder.rs`
- Verify: `crates/tqsdk-session/tests/session_market_command_helpers.rs`

- [ ] **Step 1: Add a live-client manual-drain rejection test**

Append this test to `crates/tqsdk-session/tests/session_builder.rs`:

```rust
#[test]
fn live_session_client_rejects_manual_dispatch_drain() {
    let client = SessionClientBuilder::new("user", "pass")
        .build()
        .expect("building a live client should not perform network IO");

    let err = client
        .drain_dispatches()
        .expect_err("live clients must not expose manual outbox draining");

    assert_eq!(
        err.to_string(),
        "invalid session facade state: drain_dispatches is only available for test/manual sessions without live IO; use progress_once() to drive live sessions"
    );
}
```

- [ ] **Step 2: Add invalid TqKq numbered target tests**

Append this test to `crates/tqsdk-session/tests/session_builder.rs`:

```rust
#[test]
fn builder_rejects_invalid_tqkq_numbered_targets_before_live_session_build() {
    let zero = match SessionClientBuilder::new("user", "pass")
        .trade_target_tqkq_numbered(0)
        .build()
    {
        Ok(_) => panic!("number 0 should be rejected before live session construction"),
        Err(err) => err,
    };
    assert_eq!(
        zero.to_string(),
        "validation error: TqKq assistant account number must be within 1..=99, got 0"
    );

    let too_large = match SessionClientBuilder::new("user", "pass")
        .trade_target_tqkq_stock_numbered(100)
        .build()
    {
        Ok(_) => panic!("number 100 should be rejected before live session construction"),
        Err(err) => err,
    };
    assert_eq!(
        too_large.to_string(),
        "validation error: TqKq assistant account number must be within 1..=99, got 100"
    );
}
```

- [ ] **Step 3: Run the new tests and verify they fail**

Run:

```bash
cargo test -p tqsdk-session --test session_builder live_session_client_rejects_manual_dispatch_drain -- --nocapture
cargo test -p tqsdk-session --test session_builder builder_rejects_invalid_tqkq_numbered_targets_before_live_session_build -- --nocapture
```

Expected:

```text
Both tests fail before implementation.
The first failure shows `drain_dispatches()` returned `Ok`.
The second failure shows invalid TqKq numbers were not rejected at builder time.
```

- [ ] **Step 4: Confirm existing manual no-IO dispatch tests still describe the allowed path**

Run:

```bash
cargo test -p tqsdk-session --test session_market_command_helpers -- --nocapture
```

Expected:

```text
Existing tests pass before implementation and continue proving `new_for_test_with_handle()` style clients can use manual dispatch draining.
```

---

## Task 2: Guard Manual Dispatch Draining On Live Session Clients

**Files:**
- Modify: `crates/tqsdk-session/src/client.rs`
- Test: `crates/tqsdk-session/tests/session_builder.rs`
- Verify: `crates/tqsdk-session/tests/session_market_command_helpers.rs`

- [ ] **Step 1: Hide and guard `SessionClient::drain_dispatches()`**

In `crates/tqsdk-session/src/client.rs`, replace the current method:

```rust
pub fn drain_dispatches(&self) -> crate::error::Result<Vec<OutboundDispatch>> {
    Ok(self.handle.drain_dispatches()?)
}
```

With:

```rust
#[doc(hidden)]
pub fn drain_dispatches(&self) -> crate::error::Result<Vec<OutboundDispatch>> {
    if self.io.is_some() {
        return Err(crate::error::SessionFacadeError::InvalidState(
            "drain_dispatches is only available for test/manual sessions without live IO; use progress_once() to drive live sessions",
        ));
    }

    Ok(self.handle.drain_dispatches()?)
}
```

- [ ] **Step 2: Run the live-drain test and verify it passes**

Run:

```bash
cargo test -p tqsdk-session --test session_builder live_session_client_rejects_manual_dispatch_drain -- --nocapture
```

Expected:

```text
test live_session_client_rejects_manual_dispatch_drain ... ok
```

- [ ] **Step 3: Verify manual no-IO tests still pass**

Run:

```bash
cargo test -p tqsdk-session --test session_market_command_helpers -- --nocapture
cargo test -p tqsdk-session --test session_direct_query -- --nocapture
```

Expected:

```text
Both tests pass. `new_for_test_with_handle()` clients still have `io: None`, so they can drain dispatches for contract tests.
```

- [ ] **Step 4: Commit the outbox guard**

Run:

```bash
git add crates/tqsdk-session/src/client.rs crates/tqsdk-session/tests/session_builder.rs
git commit -m "fix(session): reject manual outbox drain on live clients"
```

Expected:

```text
Commit succeeds with only the live drain guard and its test.
```

---

## Task 3: Remove `SessionRuntime` Leakage From `SessionClient` Public API

**Files:**
- Modify: `crates/tqsdk-wait/src/driver.rs`
- Modify: `crates/tqsdk-wait/src/api.rs`
- Modify: `crates/tqsdk-session/src/client.rs`
- Test: `crates/tqsdk-wait/tests/wait_api_surface.rs`
- Verify: `crates/tqsdk-session/tests/session_builder.rs`

- [ ] **Step 1: Remove `SessionRuntime` from `WaitDriver`**

In `crates/tqsdk-wait/src/driver.rs`, remove this import:

```rust
use tqsdk_core::internal::SessionRuntime;
```

And remove this field from `WaitDriver`:

```rust
pub(crate) runtime: SessionRuntime,
```

- [ ] **Step 2: Stop cloning the runtime in `TqApi::new_for_test()`**

In `crates/tqsdk-wait/src/api.rs`, remove this line:

```rust
let runtime = session.runtime_clone();
```

And remove this field initialization from the `WaitDriver` literal:

```rust
runtime,
```

- [ ] **Step 3: Use the session handle in `handle_for_test()`**

In `crates/tqsdk-wait/src/api.rs`, replace:

```rust
self.driver.runtime.handle()
```

With:

```rust
self.driver.session.handle().clone()
```

- [ ] **Step 4: Remove public runtime accessors from `SessionClient`**

In `crates/tqsdk-session/src/client.rs`, delete these methods:

```rust
#[must_use]
pub fn runtime(&self) -> &SessionRuntime {
    &self.runtime
}

#[must_use]
pub fn runtime_clone(&self) -> SessionRuntime {
    self.runtime.clone()
}
```

Keep the private `runtime: SessionRuntime` field in `SessionClient`; only the public accessors are removed.

- [ ] **Step 5: Verify no user-facing code references the removed accessors**

Run:

```bash
rg "runtime\\(\\)|runtime_clone\\(" crates docs README.md
```

Expected:

```text
No references to `SessionClient::runtime()` or `SessionClient::runtime_clone()` remain.
References to the private `runtime` field or architecture term `SessionRuntime` may remain.
```

- [ ] **Step 6: Run wait/session focused tests**

Run:

```bash
cargo test -p tqsdk-wait --test wait_api_surface -- --nocapture
cargo test -p tqsdk-session --test session_builder -- --nocapture
```

Expected:

```text
Both tests pass.
```

- [ ] **Step 7: Commit the runtime accessor removal**

Run:

```bash
git add crates/tqsdk-wait/src/driver.rs crates/tqsdk-wait/src/api.rs crates/tqsdk-session/src/client.rs
git commit -m "refactor(session): stop exposing internal session runtime"
```

Expected:

```text
Commit succeeds with no public dependency on `tqsdk_core::internal::SessionRuntime` from `tqsdk-session`.
```

---

## Task 4: Validate TqKq Numbered Trade Targets At Builder Time

**Files:**
- Modify: `crates/tqsdk-session/src/builder.rs`
- Test: `crates/tqsdk-session/tests/session_builder.rs`

- [ ] **Step 1: Import `AuthDerivedTradeTarget`**

In `crates/tqsdk-session/src/builder.rs`, update the `tqsdk_core` import block to include `AuthDerivedTradeTarget`:

```rust
use tqsdk_core::{
    AccountId, AdapterRegistry, AuthDerivedTradeTarget, EndpointConfig, MarketSessionTarget,
    ProtocolDomain, RuntimeHandle, SessionConfig, TradeSessionTarget,
};
```

- [ ] **Step 2: Add private TqKq validation helpers**

In `crates/tqsdk-session/src/builder.rs`, add these helpers near `session_config(...)` or before it:

```rust
fn validate_trade_targets(trade_targets: &[TradeSessionTarget]) -> Result<()> {
    for target in trade_targets {
        match target.auth_derived {
            Some(AuthDerivedTradeTarget::TqKqFuture {
                number: Some(number),
            })
            | Some(AuthDerivedTradeTarget::TqKqStock {
                number: Some(number),
            }) => validate_tqkq_number(number)?,
            Some(
                AuthDerivedTradeTarget::TqKqFuture { number: None }
                | AuthDerivedTradeTarget::TqKqStock { number: None },
            )
            | None => {}
        }
    }

    Ok(())
}

fn validate_tqkq_number(number: u8) -> Result<()> {
    if (1..=99).contains(&number) {
        Ok(())
    } else {
        Err(tqsdk_core::ContractError::validation(format!(
            "TqKq assistant account number must be within 1..=99, got {number}"
        ))
        .into())
    }
}
```

- [ ] **Step 3: Call validation before live client construction**

In `SessionClientBuilder::build()`, after destructuring `self` and before `session_config(...)`, add:

```rust
validate_trade_targets(&trade_targets)?;
```

The resulting shape should be:

```rust
let Self {
    auth_user,
    auth_pass,
    endpoints,
    query_enabled,
    market_target,
    trade_targets,
} = self;
validate_trade_targets(&trade_targets)?;
let mut adapters = AdapterRegistry::new();
```

- [ ] **Step 4: Run the TqKq validation test**

Run:

```bash
cargo test -p tqsdk-session --test session_builder builder_rejects_invalid_tqkq_numbered_targets_before_live_session_build -- --nocapture
```

Expected:

```text
test builder_rejects_invalid_tqkq_numbered_targets_before_live_session_build ... ok
```

- [ ] **Step 5: Verify builder tests**

Run:

```bash
cargo test -p tqsdk-session --test session_builder -- --nocapture
```

Expected:

```text
All session builder tests pass, including existing valid TqKq numbered target assertions.
```

- [ ] **Step 6: Commit the TqKq validation**

Run:

```bash
git add crates/tqsdk-session/src/builder.rs crates/tqsdk-session/tests/session_builder.rs
git commit -m "fix(session): validate tqkq numbered targets at build time"
```

Expected:

```text
Commit succeeds with builder-time validation only.
```

---

## Task 5: Align Public API Documentation And Run Final Verification

**Files:**
- Modify: `crates/tqsdk-core/README.md`
- Verify: `crates/tqsdk-session/README.md`
- Verify: `docs/architecture/ai-workflow.md`
- Verify: `docs/architecture/crate-boundaries.md`

- [ ] **Step 1: Update the core README stable public surface table**

In `crates/tqsdk-core/README.md`, remove this row from the “核心公开面” table:

```markdown
| `SessionRuntime` | auth / bootstrap / connect / recover / flush / pump 的统一编排器 |
```

Keep the existing note below the table that explains `tqsdk_core::internal` is a hidden sibling-crate bridge.

- [ ] **Step 2: Verify no session README entry promises manual outbox draining**

Run:

```bash
rg "drain_dispatches|runtime\\(\\)|runtime_clone\\(" crates/tqsdk-session/README.md README.md docs/architecture
```

Expected:

```text
No README or architecture docs advertise `drain_dispatches`, `runtime()`, or `runtime_clone()` as user-facing APIs.
```

- [ ] **Step 3: Run focused package tests**

Run:

```bash
cargo test -p tqsdk-session
cargo test -p tqsdk-wait
```

Expected:

```text
Both package test suites pass.
```

- [ ] **Step 4: Run no-default feature builds for touched crates**

Run:

```bash
cargo build -p tqsdk-session --no-default-features
cargo build -p tqsdk-wait --no-default-features
```

Expected:

```text
Both no-default-feature builds pass.
```

- [ ] **Step 5: Run workspace verification**

Run:

```bash
cargo test --workspace
cargo clippy --workspace --examples --all-targets -- -D warnings
```

Expected:

```text
Workspace tests pass.
Clippy exits successfully with no warnings promoted to errors.
```

- [ ] **Step 6: Commit docs and final verification note**

Run:

```bash
git add crates/tqsdk-core/README.md
git commit -m "docs(core): clarify stable public session surface"
```

Expected:

```text
Commit succeeds. If no README changes were required after review, skip this commit and record that no architecture boundary changed.
```

---

## Self-Review Checklist

- [ ] The plan addresses all three review findings:
  - live `drain_dispatches()` outbox footgun,
  - public `SessionRuntime` accessor leak,
  - delayed TqKq numbered target validation.
- [ ] The plan does not narrow documented `tqsdk-data`, `tqsdk-stream`, or `tqsdk-task` scenario-backed public APIs.
- [ ] Every code change has a focused test or verification command.
- [ ] The plan keeps runtime state changes on the existing `RuntimeHandle -> StateStore -> CommitResult -> RuntimeReader/UpdateCursor` path.
- [ ] The plan avoids adding new user-facing APIs under `tqsdk_core::internal`.
