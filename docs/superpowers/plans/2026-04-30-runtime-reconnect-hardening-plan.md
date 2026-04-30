# Runtime Reconnect And Coverage Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 完成下一批不改 public API 的审查修复：继续拆分 `tqsdk-core` session runtime 的 reconnect/timer/transport/detail 逻辑，并补齐对应回归护栏。

**Architecture:** 本批只修改 `tqsdk-core` 内部 runtime 编排代码，保持 `RuntimeHandle -> StateStore -> CommitResult -> RuntimeReader/UpdateCursor` 单一提交路径不变。所有新模块放在 `crates/tqsdk-core/src/session_runtime/` 下，不新增面向用户的 core public API，不移动 direct-query、wait、stream、task、data 职责边界。`docs/review-2026-04-29-pending.md` 是输入清单，`docs/architecture/*` 仍是架构权威。

**Tech Stack:** Rust, Cargo workspace, `tqsdk-core` integration tests, Tokio test runtime for reconnect backoff, Superpowers execution workflow.

---

## Scope

Included in this batch:

- Refactor `SessionRuntime::recover_with_policy`, `recover_internal`, `reconnect_backoff_ms`, and reconnect backoff sleep into a focused reconnect module.
- Refactor `drive_timer_once`, `timer_route_label`, `handle_disconnect`, `handle_transport_signal`, and `handle_transport_error` into a focused transport/timer module.
- Refactor command detail extraction helpers away from the main `session_runtime.rs` orchestration file.
- Add characterization tests before extraction for timer validation, reconnect exhaustion, and command detail preservation.
- Update review docs to mark the `3.2` deeper runtime refactor sub-scope as completed for this batch.

Excluded from this batch:

- Workspace `license` metadata. This still requires an explicit project license decision.
- `cargo audit` / dependency review automation. This should be a separate security tooling batch.
- `tqsdk-data` MarketCache public API redesign, `tqsdk-stream` WAL/journal public API redesign, and `tqsdk-task` report/status reshaping. These require architecture/docs/examples compatibility plans.
- Broad 80% coverage target. This batch adds targeted guardrails only.

## File Structure

- Modify: `crates/tqsdk-core/src/session_runtime.rs`
  - Keep the public internal orchestrator type definitions and high-level methods.
  - Remove reconnect, transport/timer, and command-detail helper implementations after extracting them.
- Create: `crates/tqsdk-core/src/session_runtime/reconnect.rs`
  - Own reconnect policy attempt loop, recovery internals, backoff calculation, and sleep helper.
- Create: `crates/tqsdk-core/src/session_runtime/transport.rs`
  - Own route pump transport signal/error handling and timer driving.
- Create: `crates/tqsdk-core/src/session_runtime/detail.rs`
  - Own command detail lookup, dispatch-derived detail fields, and path touch helpers used by status derivation.
- Modify: `crates/tqsdk-core/src/session_runtime/command_status.rs`
  - Import helper functions from `detail` instead of from the parent `session_runtime` module.
- Modify: `crates/tqsdk-core/tests/runtime_contract_session_heartbeat.rs`
  - Add timer validation guardrails.
- Modify: `crates/tqsdk-core/tests/runtime_contract_session_reconnect.rs`
  - Add reconnect exhaustion guardrails.
- Modify: `crates/tqsdk-core/tests/runtime_contract_session_cycle.rs`
  - Add command detail preservation guardrail.
- Modify: `docs/review-2026-04-29-pending.md`
  - Update only the status table/closure note for `3.2` after verification.
- Modify: `docs/public-api-overdesign-audit.md`
  - No change expected. Only update if implementation proves a documented public API decision changes.
- Do not modify: `docs/architecture/*`, unless code changes alter an architecture boundary. This plan is designed not to.

---

## Task 1: Add Characterization Tests Before Runtime Extraction

**Files:**
- Modify: `crates/tqsdk-core/tests/runtime_contract_session_heartbeat.rs`
- Modify: `crates/tqsdk-core/tests/runtime_contract_session_reconnect.rs`
- Modify: `crates/tqsdk-core/tests/runtime_contract_session_cycle.rs`

- [ ] **Step 1: Add a timer payload validation test**

Append this test to `crates/tqsdk-core/tests/runtime_contract_session_heartbeat.rs`:

```rust
#[test]
fn session_runtime_drive_timer_once_rejects_heartbeat_due_without_route_payload() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let connector = HeartbeatConnector::new(vec![RecvBehavior::Frame(RawFrame::Pong)]);
    let adapters = adapter_registry();
    let config = session_config();
    let mut run = block_on(runtime.establish(
        &TestAuthProvider,
        &MarketTopologyResolver,
        &connector,
        &config,
        &adapters,
    ))
    .unwrap();

    let err = block_on(runtime.drive_timer_once(
        &mut run,
        TimerEvent {
            label: "heartbeat-due",
            payload: None,
        },
        vec![],
        SessionRuntimeDeps::new(
            &TestAuthProvider,
            &MarketTopologyResolver,
            &connector,
            &config,
            &adapters,
        ),
    ))
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        "validation error: timer event 'heartbeat-due' requires payload.route string"
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "timers", "heartbeat-due"]),
        None
    );
}
```

- [ ] **Step 2: Add an unknown timer route validation test**

Append this test to `crates/tqsdk-core/tests/runtime_contract_session_heartbeat.rs`:

```rust
#[test]
fn session_runtime_drive_timer_once_rejects_unknown_heartbeat_route_before_timer_commit() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let connector = HeartbeatConnector::new(vec![RecvBehavior::Frame(RawFrame::Pong)]);
    let adapters = adapter_registry();
    let config = session_config();
    let mut run = block_on(runtime.establish(
        &TestAuthProvider,
        &MarketTopologyResolver,
        &connector,
        &config,
        &adapters,
    ))
    .unwrap();

    let err = block_on(runtime.drive_timer_once(
        &mut run,
        TimerEvent {
            label: "heartbeat-timeout",
            payload: Some(json!({ "route": "trade:missing" })),
        },
        vec![],
        SessionRuntimeDeps::new(
            &TestAuthProvider,
            &MarketTopologyResolver,
            &connector,
            &config,
            &adapters,
        ),
    ))
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        "validation error: unknown connected route for timer event: trade:missing"
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "timers", "heartbeat-timeout"]),
        None
    );
}
```

- [ ] **Step 3: Add reconnect exhaustion attempt detail test**

Append this test to `crates/tqsdk-core/tests/runtime_contract_session_reconnect.rs`. If the file already has an equivalent exhaustion test, extend the existing test with the exact assertions below instead of adding a duplicate.

```rust
#[test]
fn session_runtime_reconnect_exhaustion_records_attempt_error_and_closed_phase() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let connector = ControlledConnector::with_outcomes(vec![
        ConnectOutcome::Connected(RecvBehavior::Frame(RawFrame::Close)),
        ConnectOutcome::Error(ContractError::auth("first reconnect refused")),
        ConnectOutcome::Error(ContractError::auth("second reconnect refused")),
    ]);
    let adapters = adapter_registry();
    let config = SessionConfig::default()
        .with_endpoint_config(EndpointConfig::default().with_market_url("ws://market.example"))
        .with_reconnect_policy(
            ReconnectPolicy::default()
                .with_initial_backoff(Duration::from_millis(0))
                .with_max_backoff(Duration::from_millis(0))
                .with_max_attempts(Some(2)),
        );

    let mut run = block_on(runtime.establish(
        &TestAuthProvider,
        &MarketTopologyResolver,
        &connector,
        &config,
        &adapters,
    ))
    .unwrap();

    let err = block_on(runtime.drive_route_once(
        &mut run,
        "market",
        vec![],
        CommitScope::RealtimeUpdate,
        SessionRuntimeDeps::new(
            &TestAuthProvider,
            &MarketTopologyResolver,
            &connector,
            &config,
            &adapters,
        ),
    ))
    .unwrap_err();

    assert_eq!(err.to_string(), "auth error: second reconnect refused");
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "lifecycle", "phase"]),
        Some(&json!("closed"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "internal", "session-recovery-error", "attempt"]),
        Some(&json!(2))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "reconnect", "attempt"]),
        Some(&json!(2))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "reconnect", "exhausted"]),
        Some(&json!(true))
    );
}
```

- [ ] **Step 4: Add command detail preservation test**

Append this test to `crates/tqsdk-core/tests/runtime_contract_session_cycle.rs`:

```rust
#[test]
fn session_runtime_trade_order_status_preserves_seed_detail_over_dispatch_json() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let sent_frames = Arc::new(Mutex::new(Vec::new()));
    let connector = TestRouteConnector {
        sent_frames: Arc::clone(&sent_frames),
        recv_frames: vec![RawFrame::Text(
            json!({
                "aid": "rtn_data",
                "data": [{
                    "trade": {
                        "simnow": {
                            "orders": {
                                "ORDER_SEED": {
                                    "order_id": "ORDER_SEED",
                                    "status": "ALIVE",
                                    "exchange_order_id": "EX_SEED",
                                    "volume_left": 1
                                }
                            }
                        }
                    }
                }]
            })
            .to_string(),
        )],
    };
    let topology = SessionTopology::default().with_route(SessionRoute {
        label: "trade:simnow".to_string(),
        target: SessionTarget::Shared,
        domains: vec![ProtocolDomain::Trade],
        endpoint: SessionRouteEndpoint::WebSocket {
            url: "ws://trade.example".to_string(),
            connect: Default::default(),
        },
    });
    let connected =
        block_on(SessionBootstrap::new().connect_topology(&topology, &connector)).unwrap();
    let mut run = SessionRun {
        bootstrap: BootstrapResult::new(
            tqsdk_core::AuthContext::new("token"),
            vec![ProtocolDomain::Trade],
        )
        .with_topology(topology),
        connected,
    };

    let command_id = block_on(handle.submit(RuntimeCommand::Trade(TradeCommand::InsertOrder(
        TradeInsertOrderCommand {
            account_id: AccountId::new("simnow"),
            order_id: OrderId::new("ORDER_SEED"),
            symbol: Symbol::new("SHFE.au2602"),
            direction: TradeDirection::Buy,
            offset: TradeOffset::Open,
            volume: 1,
            price_type: TradePriceType::Limit,
            limit_price: Some(618.5),
            time_condition: TradeTimeCondition::Gfd,
            volume_condition: TradeVolumeCondition::Any,
        },
    ))))
    .unwrap();

    let _receipts = block_on(runtime.flush_outbound(&mut run)).unwrap();
    let commit = block_on(runtime.recv_route_and_ingest(
        &mut run,
        "trade:simnow",
        vec![command_id],
        CommitScope::RealtimeUpdate,
    ))
    .unwrap()
    .unwrap();

    assert_eq!(commit.caused_by, vec![command_id]);
    let command_segment = command_id.get().to_string();
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            command_segment.as_str(),
            "detail",
            "order_id",
        ]),
        Some(&json!("ORDER_SEED"))
    );
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            command_segment.as_str(),
            "detail",
            "exchange_order_id",
        ]),
        Some(&json!("EX_SEED"))
    );
}
```

- [ ] **Step 5: Run the new characterization tests**

Run:

```bash
cargo test -p tqsdk-core --test runtime_contract_session_heartbeat -- --nocapture
cargo test -p tqsdk-core --test runtime_contract_session_reconnect -- --nocapture
cargo test -p tqsdk-core --test runtime_contract_session_cycle -- --nocapture
```

Expected:

```text
test result: ok
```

- [ ] **Step 6: Commit characterization tests**

```bash
git add crates/tqsdk-core/tests/runtime_contract_session_heartbeat.rs crates/tqsdk-core/tests/runtime_contract_session_reconnect.rs crates/tqsdk-core/tests/runtime_contract_session_cycle.rs
git commit -m "test(core): characterize session runtime reconnect and timer behavior"
```

---

## Task 2: Extract Reconnect Policy And Recovery Flow

**Files:**
- Create: `crates/tqsdk-core/src/session_runtime/reconnect.rs`
- Modify: `crates/tqsdk-core/src/session_runtime.rs`

- [ ] **Step 1: Declare the reconnect module**

In `crates/tqsdk-core/src/session_runtime.rs`, keep `mod command_status;` and add:

```rust
mod reconnect;
```

- [ ] **Step 2: Move `RecoveryOutcome` into `reconnect.rs`**

Create `crates/tqsdk-core/src/session_runtime/reconnect.rs` with this initial structure:

```rust
use std::time::Duration;

use serde_json::json;

use crate::{
    Result,
    events::{InternalEvent, RuntimeInput},
    ids::CommandId,
    state::{CommitResult, CommitScope},
    transport::{ConnectedTopology, SessionPhase},
};

use super::{SessionRun, SessionRuntime, SessionRuntimeDeps};

pub(super) struct RecoveryOutcome {
    pub(super) run: SessionRun,
    pub(super) commits: Vec<CommitResult>,
}

impl SessionRuntime {
}
```

Remove this struct from `crates/tqsdk-core/src/session_runtime.rs`:

```rust
struct RecoveryOutcome {
    run: SessionRun,
    commits: Vec<CommitResult>,
}
```

- [ ] **Step 3: Move reconnect methods without behavior changes**

Move these methods from the main `impl SessionRuntime` block into `reconnect.rs` under `impl SessionRuntime`:

```rust
async fn recover_with_policy(
    &self,
    route_label: &str,
    reason: &'static str,
    caused_by: Vec<CommandId>,
    deps: SessionRuntimeDeps<'_>,
) -> Result<RecoveryOutcome>

async fn recover_internal(
    &self,
    deps: SessionRuntimeDeps<'_>,
    record_reconnecting: bool,
) -> Result<RecoveryOutcome>
```

The moved code must remain byte-for-byte equivalent except for imports and path qualification.

- [ ] **Step 4: Move reconnect helper functions**

Move these free functions into `reconnect.rs`:

```rust
fn reconnect_backoff_ms(config: &crate::SessionConfig, attempt: u32) -> u64

async fn sleep_reconnect_backoff(scheduled_backoff_ms: u64) -> Result<()>
```

Keep both functions private to `reconnect.rs`. Do not export them from `tqsdk-core`.

- [ ] **Step 5: Compile only `tqsdk-core` tests**

Run:

```bash
cargo test -p tqsdk-core --test runtime_contract_session_reconnect
```

Expected:

```text
test result: ok
```

- [ ] **Step 6: Commit reconnect extraction**

```bash
git add crates/tqsdk-core/src/session_runtime.rs crates/tqsdk-core/src/session_runtime/reconnect.rs
git commit -m "refactor(core): extract session reconnect flow"
```

---

## Task 3: Extract Transport And Timer Driving

**Files:**
- Create: `crates/tqsdk-core/src/session_runtime/transport.rs`
- Modify: `crates/tqsdk-core/src/session_runtime.rs`

- [ ] **Step 1: Declare the transport module**

In `crates/tqsdk-core/src/session_runtime.rs`, add:

```rust
mod transport;
```

- [ ] **Step 2: Move route pump transport handlers**

Create `crates/tqsdk-core/src/session_runtime/transport.rs` with imports and an `impl SessionRuntime` block:

```rust
use serde_json::{Value, json};

use crate::{
    Result,
    commands::OutboundFrame,
    events::{InternalEvent, RuntimeInput, TimerEvent},
    ids::CommandId,
    state::CommitScope,
    transport::SessionPhase,
};

use super::{RoutePumpOutcome, SessionRuntime, SessionRuntimeDeps, SessionRun};

impl SessionRuntime {
}
```

Move these methods into that `impl SessionRuntime` block:

```rust
pub async fn pump_route_once(
    &self,
    run: &mut SessionRun,
    route_label: &str,
    caused_by: Vec<CommandId>,
    scope: CommitScope,
) -> Result<RoutePumpOutcome>

pub async fn drive_timer_once(
    &self,
    run: &mut SessionRun,
    timer: TimerEvent,
    caused_by: Vec<CommandId>,
    deps: SessionRuntimeDeps<'_>,
) -> Result<crate::internal::SessionStepOutcome>

fn handle_disconnect(...)
fn handle_transport_signal(...)
fn handle_transport_error(...)
```

If referring to `SessionStepOutcome` from inside `transport.rs`, import it from `super` instead of using `crate::internal`.

- [ ] **Step 3: Move `timer_route_label`**

Move this free function into `transport.rs`:

```rust
fn timer_route_label(timer: &TimerEvent) -> Result<&str>
```

Keep it private to `transport.rs`.

- [ ] **Step 4: Merge duplicate timer route validation while preserving behavior**

After moving `drive_timer_once`, replace the two separate `match timer.label` passes with a single precomputed route:

```rust
let heartbeat_route = match timer.label {
    "heartbeat-due" | "heartbeat-timeout" => {
        let route_label = timer_route_label(&timer)?;
        if !run.connected.has_route(route_label) {
            return Err(crate::ContractError::validation(format!(
                "unknown connected route for timer event: {route_label}"
            )));
        }
        Some(route_label.to_string())
    }
    _ => None,
};
```

Use `heartbeat_route.as_deref().expect("heartbeat timer route was validated")` inside the `"heartbeat-due"` and `"heartbeat-timeout"` arms. This preserves the current invariant that invalid heartbeat timers do not commit a timer event.

- [ ] **Step 5: Run route and timer tests**

Run:

```bash
cargo test -p tqsdk-core --test runtime_contract_session_heartbeat
cargo test -p tqsdk-core --test runtime_contract_session_reconnect
cargo test -p tqsdk-core --test runtime_contract_session_cycle
```

Expected:

```text
test result: ok
```

- [ ] **Step 6: Commit transport/timer extraction**

```bash
git add crates/tqsdk-core/src/session_runtime.rs crates/tqsdk-core/src/session_runtime/transport.rs
git commit -m "refactor(core): extract session transport and timer driving"
```

---

## Task 4: Extract Command Detail Helpers

**Files:**
- Create: `crates/tqsdk-core/src/session_runtime/detail.rs`
- Modify: `crates/tqsdk-core/src/session_runtime.rs`
- Modify: `crates/tqsdk-core/src/session_runtime/command_status.rs`

- [ ] **Step 1: Declare the detail module**

In `crates/tqsdk-core/src/session_runtime.rs`, add:

```rust
mod detail;
```

- [ ] **Step 2: Move detail and path helper functions**

Create `crates/tqsdk-core/src/session_runtime/detail.rs` and move these functions from `session_runtime.rs`:

```rust
pub(super) fn command_detail_map_from_snapshot(
    snapshot: crate::state::StateReadView<'_>,
    command_id: crate::ids::CommandId,
) -> Option<serde_json::Map<String, serde_json::Value>>

pub(super) fn command_detail_from_seed(
    seed: serde_json::Map<String, serde_json::Value>,
    command_id: crate::ids::CommandId,
    route_label: Option<&str>,
    dispatch: Option<&crate::commands::OutboundDispatch>,
    extra: serde_json::Map<String, serde_json::Value>,
) -> Option<serde_json::Value>

pub(super) fn command_detail_fields_from_dispatch(
    dispatch: &crate::commands::OutboundDispatch,
) -> serde_json::Map<String, serde_json::Value>

pub(super) fn is_terminal_command_status(status: Option<&str>) -> bool

pub(super) fn commit_touches_path<I, S>(commit: &crate::state::CommitResult, path: I) -> bool
where
    I: IntoIterator<Item = S>,
    S: Into<String>

pub(super) fn commit_touches_path_prefix<I, S>(commit: &crate::state::CommitResult, path: I) -> bool
where
    I: IntoIterator<Item = S>,
    S: Into<String>
```

The moved functions must preserve current behavior. Keep `command_id` in `command_detail_from_seed` even if it is unused, so the call signature stays compatible with current status derivation call sites.

- [ ] **Step 3: Update imports in `command_status.rs`**

Replace:

```rust
use super::{command_detail_from_seed, commit_touches_path, commit_touches_path_prefix};
```

with:

```rust
use super::detail::{command_detail_from_seed, commit_touches_path, commit_touches_path_prefix};
```

- [ ] **Step 4: Update call sites in `session_runtime.rs`**

Add this import near the module declarations:

```rust
use detail::{
    command_detail_fields_from_dispatch, command_detail_map_from_snapshot,
    is_terminal_command_status,
};
```

Keep `command_detail_from_seed` used from `command_status.rs`, not from `session_runtime.rs`, unless compilation requires it.

- [ ] **Step 5: Run command detail and full core tests**

Run:

```bash
cargo test -p tqsdk-core --test runtime_contract_session_cycle
cargo test -p tqsdk-core
```

Expected:

```text
test result: ok
```

- [ ] **Step 6: Commit detail extraction**

```bash
git add crates/tqsdk-core/src/session_runtime.rs crates/tqsdk-core/src/session_runtime/detail.rs crates/tqsdk-core/src/session_runtime/command_status.rs
git commit -m "refactor(core): extract session command detail helpers"
```

---

## Task 5: Update Review Docs And Verify The Batch

**Files:**
- Modify: `docs/review-2026-04-29-pending.md`
- Verify: workspace commands

- [ ] **Step 1: Update `3.2` closure note**

In the top status table of `docs/review-2026-04-29-pending.md`, change the `3.2` row from:

```markdown
| 3.2 `tqsdk-core/src/session_runtime.rs` 大文件与重复 | `done` + `moved to breaking-change batch` | command status derivation 已拆分（`7e43df8`）。更深的 reconnect/transport 分拆未纳入本批，需后续 runtime plan。 |
```

to:

```markdown
| 3.2 `tqsdk-core/src/session_runtime.rs` 大文件与重复 | `done` | command status derivation 已拆分（`7e43df8`），reconnect/timer/transport/detail helpers 已在下一批 runtime hardening 中拆入 `session_runtime/*` 子模块并由 session runtime 回归测试覆盖。 |
```

- [ ] **Step 2: Add a batch note under final verification**

Under the existing “最终验证” command list in `docs/review-2026-04-29-pending.md`, add:

```markdown
Next runtime hardening batch verification:

- `cargo test -p tqsdk-core`
- `cargo test --workspace`
- `cargo clippy --workspace --examples --all-targets -- -D warnings`
```

- [ ] **Step 3: Run focused verification**

Run:

```bash
cargo test -p tqsdk-core
```

Expected:

```text
test result: ok
```

- [ ] **Step 4: Run full workspace verification**

Run:

```bash
cargo test --workspace
cargo clippy --workspace --examples --all-targets -- -D warnings
```

Expected:

```text
test result: ok
```

and:

```text
Finished `dev` profile
```

with no clippy warnings.

- [ ] **Step 5: Commit docs and final verification record**

```bash
git add docs/review-2026-04-29-pending.md
git commit -m "docs: close session runtime refactor audit item"
```

---

## Completion Checklist

- [ ] `session_runtime.rs` is smaller and no longer owns reconnect policy loop, timer transport handling, or command detail helper internals.
- [ ] No public re-exports in `crates/tqsdk-core/src/lib.rs` changed.
- [ ] No architecture boundary docs changed unless a real boundary change occurred.
- [ ] New characterization tests pass before and after extraction.
- [ ] `cargo test -p tqsdk-core` passes.
- [ ] `cargo test --workspace` passes.
- [ ] `cargo clippy --workspace --examples --all-targets -- -D warnings` passes.
- [ ] `docs/review-2026-04-29-pending.md` accurately reflects that only the `3.2` runtime-refactor remainder was closed by this batch.

## Follow-Up Batches

- Security tooling batch: add or document `cargo audit` / dependency review workflow for `yawc` and other network/security dependencies.
- License metadata batch: add `[workspace.package].license` only after the project license is explicitly chosen.
- Coverage expansion batch: define measurable focused coverage targets for `tqsdk-wait`, `tqsdk-task`, and `tqsdk-session` helpers instead of a broad 80% mandate.
- Public API redesign batches: S18 `tqsdk-data` cache API, S21 `tqsdk-stream` WAL/journal API, and `tqsdk-task` report/status shape each need separate compatibility plans.
