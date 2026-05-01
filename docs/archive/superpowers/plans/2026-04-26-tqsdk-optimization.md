# tqsdk-rust Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce maintenance risk in the current codebase without disturbing the already-stabilized core/session/wait/stream/data architecture boundaries.

**Architecture:** Keep `tqsdk-core` as the protocol-complete runtime substrate, keep one-shot direct-query ownership in `tqsdk-session`, and keep continuous consumption in `tqsdk-wait` / `tqsdk-stream`. Optimize in-place: first lock the validation/documentation baseline, then split `tqsdk-task` internals, then continue DIFF protocol-model consolidation, then gradually shrink `tqsdk_core::internal`.

**Tech Stack:** Rust 2024, Cargo workspace resolver 3, Tokio, Serde/serde_json, feature-gated `reqwest` / `base64`, existing runtime contract tests.

---

## Scope

This plan covers four independent optimization workstreams:

- Workstream A: validation and architecture documentation baseline
- Workstream B: `tqsdk-task` internal decomposition
- Workstream C: DIFF protocol model consolidation
- Workstream D: `tqsdk_core::internal` surface control

Do not merge unrelated semantic changes into these tasks. Each task should compile and test independently.

## Guardrails

- Read `docs/architecture/ai-workflow.md` before any code change.
- Do not move direct query APIs from `tqsdk-session` into wait/stream.
- Do not move task/data capabilities into core/session/wait/stream.
- Do not add a second facade-owned state tree or private revision.
- Do not bypass `RuntimeHandle -> StateStore -> CommitResult -> RuntimeReader/UpdateCursor`.
- Do not restore `ContractFuture` or widen core root re-exports.
- Do not remove the compatible runtime state tree; domain partitions and the compatible tree intentionally coexist.

---

## File Map

### Documentation and validation

- Modify: `docs/architecture/validation.md`
- Modify: `docs/architecture/ai-workflow.md`
- Modify: `crates/tqsdk-core/README.md`
- Modify: `crates/tqsdk-session/README.md`
- Modify: `report.md`

### Task decomposition

- Modify: `crates/tqsdk-task/src/target_pos.rs`
- Create: `crates/tqsdk-task/src/target_pos/state.rs`
- Create: `crates/tqsdk-task/src/target_pos/planner.rs`
- Create: `crates/tqsdk-task/src/target_pos/executor.rs`
- Create: `crates/tqsdk-task/src/target_pos/report.rs`
- Modify: `crates/tqsdk-task/src/scheduler.rs`
- Create: `crates/tqsdk-task/src/scheduler/state.rs`
- Create: `crates/tqsdk-task/src/scheduler/planner.rs`
- Create: `crates/tqsdk-task/src/scheduler/runner.rs`
- Existing tests to preserve: `crates/tqsdk-task/tests/*`

### DIFF protocol model

- Move: `crates/tqsdk-core/src/diff_protocol.rs` -> `crates/tqsdk-core/src/diff_protocol/mod.rs`
- Create: `crates/tqsdk-core/src/diff_protocol/outbound.rs`
- Create: `crates/tqsdk-core/src/diff_protocol/inbound.rs`
- Modify: `crates/tqsdk-core/src/adapter/common.rs`
- Optional follow-up split: `crates/tqsdk-core/src/adapter/common/query.rs`
- Optional follow-up split: `crates/tqsdk-core/src/adapter/common/trade.rs`
- Optional follow-up split: `crates/tqsdk-core/src/adapter/common/replay.rs`
- Existing tests to preserve: `crates/tqsdk-core/tests/runtime_contract_adapters.rs`

### Core internal surface

- Modify: `crates/tqsdk-core/src/lib.rs`
- Modify: `crates/tqsdk-session/src/*` only where internal imports can be narrowed
- Existing tests to preserve: `crates/tqsdk-core/tests/runtime_contract_surface.rs`

---

## Task 1: Add feature/no-default validation baseline

**Files:**

- Modify: `docs/architecture/validation.md`
- Modify: `report.md`

- [ ] **Step 1: Add a feature matrix section to `docs/architecture/validation.md`**

Add a section named `Feature / no-default build matrix` with these commands:

```bash
cargo build -p tqsdk-core
cargo build -p tqsdk-session --no-default-features
cargo build -p tqsdk-session --features live
cargo build -p tqsdk-session --features services
cargo build -p tqsdk-wait --no-default-features
cargo build -p tqsdk-stream --no-default-features
cargo build -p tqsdk-task --no-default-features
cargo build -p tqsdk-data --no-default-features
cargo test -p tqsdk-core
cargo test -p tqsdk-session --no-default-features
```

Expected documentation text:

```markdown
## Feature / no-default build matrix

The workspace intentionally keeps `tqsdk-core` free of live HTTP/auth dependencies.
Session and facade crates expose live/service capabilities through features. Any
change touching Cargo features or optional dependencies must preserve this matrix:

| Command | Purpose |
| --- | --- |
| `cargo build -p tqsdk-core` | Verify core remains standalone substrate |
| `cargo build -p tqsdk-session --no-default-features` | Verify session substrate without live/services |
| `cargo build -p tqsdk-session --features live` | Verify live/auth path |
| `cargo build -p tqsdk-session --features services` | Verify service/query path |
| `cargo build -p tqsdk-wait --no-default-features` | Verify wait facade without live defaults |
| `cargo build -p tqsdk-stream --no-default-features` | Verify stream facade without live defaults |
| `cargo build -p tqsdk-task --no-default-features` | Verify task crate without live defaults |
| `cargo build -p tqsdk-data --no-default-features` | Verify data crate without services defaults |
```

- [ ] **Step 2: Update `report.md` if command list differs**

Keep `report.md` aligned with the exact validation commands added above. Do not introduce a separate, conflicting matrix.

- [ ] **Step 3: Run validation commands**

Run each command from the matrix after editing docs.

Expected: all commands complete successfully.

- [ ] **Step 4: Commit**

```bash
git add docs/architecture/validation.md report.md
git commit -m "docs: add feature build validation matrix"
```

---

## Task 2: Document `tqsdk_core::internal` as unstable bridge

**Files:**

- Modify: `crates/tqsdk-core/README.md`
- Modify: `docs/architecture/ai-workflow.md`

- [ ] **Step 1: Add an internal-surface note to `crates/tqsdk-core/README.md`**

Add this text near the public API / crate role section:

```markdown
### Internal bridge surface

`tqsdk_core::internal` is a `#[doc(hidden)]` bridge used by sibling crates while
the shared session layer continues to absorb runtime assembly details. It is not
part of the stable user-facing contract. External users should prefer the root
exports such as `RuntimeHandle`, `RuntimeReader`, `UpdateCursor`, protocol
commands, schema types, and transport/session contracts.
```

- [ ] **Step 2: Add the same rule to `docs/architecture/ai-workflow.md`**

Add a short prohibition under the `tqsdk-core` section:

```markdown
- Do not add new user-facing APIs under `tqsdk_core::internal`; it is a temporary
  sibling-crate bridge and must not become a second public surface.
```

- [ ] **Step 3: Run docs-neutral compile check**

Run:

```bash
cargo test -p tqsdk-core --test runtime_contract_surface
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/tqsdk-core/README.md docs/architecture/ai-workflow.md
git commit -m "docs: clarify core internal bridge stability"
```

---

## Task 3: Split `TargetPosTask` state from behavior

**Files:**

- Modify: `crates/tqsdk-task/src/target_pos.rs`
- Create: `crates/tqsdk-task/src/target_pos/state.rs`

- [ ] **Step 1: Identify state-only types in `target_pos.rs`**

Move only private or crate-private data structs/enums that do not perform API calls into `target_pos/state.rs`.

Allowed state module shape:

```rust
use crate::config::TargetPosConfig;

#[derive(Debug, Clone)]
pub(crate) struct TargetPosState {
    pub(crate) config: TargetPosConfig,
    pub(crate) target_volume: i64,
    pub(crate) last_known_position: i64,
}

impl TargetPosState {
    pub(crate) fn new(config: TargetPosConfig, target_volume: i64) -> Self {
        Self {
            config,
            target_volume,
            last_known_position: 0,
        }
    }
}
```

If existing field names differ, preserve existing names and visibility. Do not change public behavior.

- [ ] **Step 2: Declare the submodule from `target_pos.rs`**

At the top of `target_pos.rs`, add:

```rust
mod state;

use state::TargetPosState;
```

If the existing type name is not `TargetPosState`, import the moved type under its existing name.

- [ ] **Step 3: Run focused task tests**

Run:

```bash
cargo test -p tqsdk-task target_pos
```

Expected: all target-pos related tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/tqsdk-task/src/target_pos.rs crates/tqsdk-task/src/target_pos/state.rs
git commit -m "refactor(task): split target position state"
```

---

## Task 4: Split target-position planning from execution

**Files:**

- Modify: `crates/tqsdk-task/src/target_pos.rs`
- Create: `crates/tqsdk-task/src/target_pos/planner.rs`
- Create: `crates/tqsdk-task/src/target_pos/executor.rs`

- [ ] **Step 1: Create a planner module**

Move pure calculation logic into `target_pos/planner.rs`. The planner must not call `wait_update()`, `insert_order()`, `cancel_order()`, or any network/session-driving method.

Target shape:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TargetPosAction {
    Noop,
    Insert { volume: i64 },
    CancelStale,
}

pub(crate) fn plan_target_position(current_volume: i64, target_volume: i64) -> TargetPosAction {
    let delta = target_volume - current_volume;
    if delta == 0 {
        TargetPosAction::Noop
    } else {
        TargetPosAction::Insert { volume: delta }
    }
}
```

Adapt the names and fields to the existing target-pos semantics. Preserve existing order direction/offset/price behavior.

- [ ] **Step 2: Create an executor module**

Move code that performs order insertion/cancel/wait interactions into `target_pos/executor.rs`.

Target boundary:

```rust
pub(crate) struct TargetPosExecutor;

impl TargetPosExecutor {
    pub(crate) fn new() -> Self {
        Self
    }
}
```

Keep async behavior and existing public `TargetPosTask` methods in `target_pos.rs`; delegate internals to `TargetPosExecutor`.

- [ ] **Step 3: Add planner unit tests**

Add tests either inside `target_pos/planner.rs` or an existing task test file:

```rust
#[test]
fn plan_target_position_returns_noop_when_already_at_target() {
    assert_eq!(plan_target_position(3, 3), TargetPosAction::Noop);
}

#[test]
fn plan_target_position_returns_insert_delta_when_target_differs() {
    assert_eq!(
        plan_target_position(1, 4),
        TargetPosAction::Insert { volume: 3 }
    );
}
```

If actual action fields differ, keep the same assertions using the real action type.

- [ ] **Step 4: Run focused tests**

Run:

```bash
cargo test -p tqsdk-task target_pos
```

Expected: all target-pos related tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/tqsdk-task/src/target_pos.rs crates/tqsdk-task/src/target_pos/planner.rs crates/tqsdk-task/src/target_pos/executor.rs
git commit -m "refactor(task): split target position planning and execution"
```

---

## Task 5: Split target-position reporting

**Files:**

- Modify: `crates/tqsdk-task/src/target_pos.rs`
- Create: `crates/tqsdk-task/src/target_pos/report.rs`

- [ ] **Step 1: Move report/event aggregation types**

Move execution report aggregation and event formatting logic into `target_pos/report.rs`.

Target shape:

```rust
#[derive(Debug, Default, Clone)]
pub(crate) struct TargetPosReportBuilder {
    event_count: usize,
}

impl TargetPosReportBuilder {
    pub(crate) fn record_event(&mut self) {
        self.event_count += 1;
    }

    pub(crate) fn event_count(&self) -> usize {
        self.event_count
    }
}
```

Use the existing report/event types and preserve public report output.

- [ ] **Step 2: Keep public exports stable**

If `target_pos.rs` currently exposes report-related public types, re-export them from the same public path:

```rust
pub use report::TargetPosReport;
```

Only add this if the public type exists today.

- [ ] **Step 3: Run focused tests**

Run:

```bash
cargo test -p tqsdk-task target_pos
```

Expected: all target-pos related tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/tqsdk-task/src/target_pos.rs crates/tqsdk-task/src/target_pos/report.rs
git commit -m "refactor(task): split target position reporting"
```

---

## Task 6: Split scheduler state, planning, and runner

**Files:**

- Modify: `crates/tqsdk-task/src/scheduler.rs`
- Create: `crates/tqsdk-task/src/scheduler/state.rs`
- Create: `crates/tqsdk-task/src/scheduler/planner.rs`
- Create: `crates/tqsdk-task/src/scheduler/runner.rs`

- [ ] **Step 1: Move scheduler state types**

Create `scheduler/state.rs` and move scheduler-owned store/state structs there.

Target shape:

```rust
#[derive(Debug, Default)]
pub(crate) struct SchedulerState {
    pub(crate) active_task_count: usize,
}
```

Preserve existing fields and names where they already exist.

- [ ] **Step 2: Move pure scheduling calculations**

Create `scheduler/planner.rs` for pure decisions that do not call session/wait APIs.

Target shape:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SchedulerAction {
    Noop,
    StartTask,
    StopTask,
}
```

Use real existing action names if they already exist.

- [ ] **Step 3: Move async driving loop**

Create `scheduler/runner.rs` for loop/drive logic that invokes task execution or wait/session operations.

Target shape:

```rust
pub(crate) struct SchedulerRunner;

impl SchedulerRunner {
    pub(crate) fn new() -> Self {
        Self
    }
}
```

Keep public scheduler methods in `scheduler.rs`.

- [ ] **Step 4: Run focused tests**

Run:

```bash
cargo test -p tqsdk-task scheduler
```

Expected: all scheduler related tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/tqsdk-task/src/scheduler.rs crates/tqsdk-task/src/scheduler/state.rs crates/tqsdk-task/src/scheduler/planner.rs crates/tqsdk-task/src/scheduler/runner.rs
git commit -m "refactor(task): split scheduler internals"
```

---

## Task 7: Move DIFF protocol file into a module directory

**Files:**

- Move: `crates/tqsdk-core/src/diff_protocol.rs` -> `crates/tqsdk-core/src/diff_protocol/mod.rs`
- Create: `crates/tqsdk-core/src/diff_protocol/outbound.rs`
- Modify: `crates/tqsdk-core/src/diff_protocol/mod.rs`

- [ ] **Step 1: Move existing file without semantic changes**

Run:

```bash
mkdir -p crates/tqsdk-core/src/diff_protocol
git mv crates/tqsdk-core/src/diff_protocol.rs crates/tqsdk-core/src/diff_protocol/mod.rs
```

Expected: `mod diff_protocol;` in `crates/tqsdk-core/src/lib.rs` continues to resolve.

- [ ] **Step 2: Extract outbound protocol message definitions**

Move outbound message enum and constructors into `diff_protocol/outbound.rs`.

In `diff_protocol/mod.rs`, add:

```rust
mod outbound;

pub(crate) use outbound::DiffProtocolMessage;
```

Keep existing `pub(crate)` visibility unless current code requires narrower visibility.

- [ ] **Step 3: Run core adapter tests**

Run:

```bash
cargo test -p tqsdk-core --test runtime_contract_adapters
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/tqsdk-core/src/diff_protocol
git commit -m "refactor(core): split diff protocol outbound model"
```

---

## Task 8: Centralize inbound DIFF `aid` parsing

**Files:**

- Create: `crates/tqsdk-core/src/diff_protocol/inbound.rs`
- Modify: `crates/tqsdk-core/src/diff_protocol/mod.rs`
- Modify: `crates/tqsdk-core/src/adapter/common.rs`

- [ ] **Step 1: Add inbound message classifier**

Create `diff_protocol/inbound.rs`:

```rust
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DiffInboundAid {
    RtnData,
    QrySettlementInfo,
    Unknown,
}

impl DiffInboundAid {
    pub(crate) fn from_value(value: &Value) -> Self {
        match value.get("aid").and_then(Value::as_str) {
            Some("rtn_data") => Self::RtnData,
            Some("qry_settlement_info") => Self::QrySettlementInfo,
            _ => Self::Unknown,
        }
    }
}
```

- [ ] **Step 2: Export classifier inside `diff_protocol/mod.rs`**

Add:

```rust
mod inbound;

pub(crate) use inbound::DiffInboundAid;
```

- [ ] **Step 3: Replace direct `aid` string checks in `adapter/common.rs`**

Replace checks like:

```rust
value.get("aid").and_then(Value::as_str) == Some("rtn_data")
```

with:

```rust
DiffInboundAid::from_value(value) == DiffInboundAid::RtnData
```

Import:

```rust
use crate::diff_protocol::DiffInboundAid;
```

- [ ] **Step 4: Add classifier unit tests**

Add tests in `diff_protocol/inbound.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn classifies_rtn_data() {
        assert_eq!(
            DiffInboundAid::from_value(&json!({"aid": "rtn_data"})),
            DiffInboundAid::RtnData
        );
    }

    #[test]
    fn classifies_unknown_when_aid_missing() {
        assert_eq!(
            DiffInboundAid::from_value(&json!({})),
            DiffInboundAid::Unknown
        );
    }
}
```

- [ ] **Step 5: Run adapter tests**

Run:

```bash
cargo test -p tqsdk-core --test runtime_contract_adapters
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/tqsdk-core/src/diff_protocol crates/tqsdk-core/src/adapter/common.rs
git commit -m "refactor(core): centralize inbound diff aid parsing"
```

---

## Task 9: Audit `tqsdk_core::internal` imports

**Files:**

- Inspect: `crates/tqsdk-session/src/*`
- Inspect: `crates/tqsdk-wait/src/*`
- Inspect: `crates/tqsdk-stream/src/*`
- Inspect: `crates/tqsdk-data/src/*`
- Inspect: `crates/tqsdk-task/src/*`
- Modify only if a safe narrowing is obvious: `crates/tqsdk-core/src/lib.rs`

- [ ] **Step 1: List current internal imports**

Run:

```bash
rg -n "tqsdk_core::internal|crate::internal|internal::" crates -S
```

Expected: a finite list of sibling-crate internal bridge usages.

- [ ] **Step 2: Categorize each internal type**

Use this table in a temporary note or PR description:

```markdown
| Type | Used by | Reason | Can move now? |
| --- | --- | --- | --- |
| SessionRuntime | tqsdk-session | session runtime assembly | no |
| WebSocketTransport | tqsdk-session | live route connector | maybe later |
```

- [ ] **Step 3: Remove only unused internal re-exports**

If a symbol in `tqsdk_core::internal` has no sibling-crate usage, remove only that symbol from `crates/tqsdk-core/src/lib.rs`.

Do not move files or change public root exports in this task.

- [ ] **Step 4: Run surface tests**

Run:

```bash
cargo test -p tqsdk-core --test runtime_contract_surface
cargo test -p tqsdk-session
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/tqsdk-core/src/lib.rs
git commit -m "refactor(core): trim unused internal bridge exports"
```

If no exports can be removed, do not commit code; update the PR description with the audit table.

---

## Task 10: Final workspace verification

**Files:**

- No code changes unless verification reveals an issue.

- [ ] **Step 1: Run core tests**

Run:

```bash
cargo test -p tqsdk-core
```

Expected: PASS.

- [ ] **Step 2: Run session tests**

Run:

```bash
cargo test -p tqsdk-session
```

Expected: PASS.

- [ ] **Step 3: Run facade/task/data tests**

Run:

```bash
cargo test -p tqsdk-wait
cargo test -p tqsdk-stream
cargo test -p tqsdk-task
cargo test -p tqsdk-data
```

Expected: PASS.

- [ ] **Step 4: Run feature matrix**

Run the commands added in Task 1.

Expected: PASS.

- [ ] **Step 5: Update final report status**

Modify `report.md` only if completed tasks change the current-state conclusions.

- [ ] **Step 6: Commit final report update if needed**

```bash
git add report.md
git commit -m "docs: update optimization report status"
```

Skip this commit if `report.md` did not change.

---

## Execution Order

Recommended order:

1. Task 1
2. Task 2
3. Task 3
4. Task 4
5. Task 5
6. Task 6
7. Task 7
8. Task 8
9. Task 9
10. Task 10

Safe parallelization:

- Task 1 and Task 2 can run in parallel.
- Task 3, Task 4, and Task 5 must run sequentially.
- Task 6 can run in parallel with Tasks 3-5 if a separate worker owns only `scheduler.rs` and `scheduler/*`.
- Task 7 must precede Task 8.
- Task 9 should run after Tasks 1-8.

## Success Criteria

- `report.md` and architecture docs no longer promote obsolete P0/P1 work.
- `tqsdk-task` keeps public API stable while reducing large-file responsibility concentration.
- DIFF `aid` parsing is centralized in protocol-model code.
- `tqsdk_core::internal` is documented as unstable and trimmed where safely possible.
- Default and no-default feature builds are documented and verified.
- No task/data capability is moved into core/session/wait/stream.
- No direct-query API is copied into wait/stream.
