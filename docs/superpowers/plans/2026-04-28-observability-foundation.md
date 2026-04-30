# Observability Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Advance S20/S21/S22 without S14 by adding transport-neutral production observability hooks and keeping HTTP/GUI out of the Rust SDK scope.

**Architecture:** S20 telemetry lives in `tqsdk-task` above `StrategySupervisor` and exports typed snapshots without changing runtime state or creating a second status tree. S21/S22 follow later as stream/task tooling layers over existing diagnostics, not as core/session changes.

**Tech Stack:** Rust, Tokio tests, existing `tqsdk-task` deployment/supervisor types, existing scenario contract examples and docs.

---

### Task 1: Strategy Supervisor Telemetry Reporter

**Files:**
- Modify: `crates/tqsdk-task/src/deployment.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`
- Modify: `crates/tqsdk-task/tests/strategy_environment.rs`
- Modify: `crates/tqsdk-task/examples/api_contract_s20_strategy_supervisor.rs`
- Modify: `docs/public-api-scenario-review.md`
- Modify: `docs/scenarios/user-layer-iteration-plan.md`
- Modify: `docs/scenarios/api_gaps/api_contract_s20_production_daemon.rs`

- [x] **Step 1: Write failing tests**

Add tests that use `StrategySupervisor::telemetry_reporter(...)` and assert typed telemetry events expose health status, metrics, and final stop reason without parsing logs.

Run:

```bash
cargo test -p tqsdk-task supervisor_reports_typed_telemetry_events -- --nocapture
```

Expected: fail because `telemetry_reporter` and telemetry event types do not exist.

- [x] **Step 2: Implement minimal telemetry types and reporter hook**

Add `StrategyTelemetryEvent`, `StrategyTelemetryEventKind`, and `StrategyTelemetryReporter` in `deployment.rs`; re-export them from `lib.rs`; emit events when supervisor status/metrics changes and when a run report is produced.

- [x] **Step 3: Update S20 example contract**

Update `api_contract_s20_strategy_supervisor.rs` to configure a reporter and print typed telemetry, while keeping HTTP/GUI endpoint wording out of the user code.

- [x] **Step 4: Verify task surface**

Run:

```bash
cargo test -p tqsdk-task strategy_environment
cargo check -p tqsdk-task --example api_contract_s20_strategy_supervisor
```

Expected: pass.

### Task 2: Scenario Review Update

**Files:**
- Modify: `docs/public-api-scenario-review.md`
- Modify: `docs/scenarios/user-layer-iteration-plan.md`
- Modify: `docs/scenarios/api_gaps/api_contract_s20_production_daemon.rs`

- [x] **Step 1: Update S20 status**

Record that stable telemetry/export hook foundation is now covered by `StrategySupervisor`, while durable sink isolation, complete reconnect orchestration, and cross-process daemon management remain gaps.

- [x] **Step 2: Verify wording**

Run:

```bash
rg -n "HTTP metrics|HTTP health|metrics_endpoint|health_endpoint|web_gui" docs/public-api-scenario-review.md docs/scenarios crates/tqsdk-task/examples/api_contract_s20_strategy_supervisor.rs
```

Expected: no endpoint implementation request appears outside explicit out-of-scope wording.

### Task 3: Workspace Verification

**Files:** no source edits.

- [x] **Step 1: Run scenario CI commands**

Run:

```bash
cargo check --workspace --examples
cargo test --workspace
cargo clippy --workspace --examples --all-targets -- -D warnings
cargo check --workspace --no-default-features
cargo check --workspace --all-features --examples
```

Expected: all pass.

- [ ] **Step 2: Commit**

Commit only files changed for this observability foundation batch.
