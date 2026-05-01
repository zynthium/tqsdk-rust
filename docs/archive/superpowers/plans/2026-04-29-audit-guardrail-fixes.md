# Audit Guardrail Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the no-API-change guardrail fixes from the 2026-04-29 audit: safety comments, OAuth constant intent comments, and the `serde(skip)` merge intent comment.

**Architecture:** This batch is intentionally source-compatible and must not change crate boundaries, public API shape, runtime state flow, command semantics, or auth configuration behavior. Workspace license metadata is excluded from this plan because the repository license has not been explicitly chosen; handle that later as a separate decision task.

**Tech Stack:** Rust, Cargo workspace, `rg`, `cargo test -p tqsdk-core`, `cargo check --workspace`.

---

## Execution Context

- Worktree: `.worktrees/audit-guardrails`
- Branch: `audit-guardrails`
- Baseline already verified before code edits:
  - `cargo build`
  - `cargo test`

## File Structure

- Modify: `crates/tqsdk-core/tests/runtime_contract_endpoint_config.rs`
  - Add `// SAFETY:` comments for test-scoped environment mutation blocks.
- Modify: `crates/tqsdk-core/tests/runtime_contract_command_ledger.rs`
  - Add `// SAFETY:` comment before `Waker::from_raw`.
- Modify: `crates/tqsdk-core/tests/runtime_contract_pending_route_executor.rs`
  - Add `// SAFETY:` comment before `Waker::from_raw`.
- Modify: `crates/tqsdk-core/tests/runtime_contract_route_connector.rs`
  - Add `// SAFETY:` comment before `Waker::from_raw`.
- Modify: `crates/tqsdk-core/tests/runtime_contract_route_dispatch.rs`
  - Add `// SAFETY:` comment before `Waker::from_raw`.
- Modify: `crates/tqsdk-core/tests/runtime_contract_runtime_core.rs`
  - Add `// SAFETY:` comment before `Waker::from_raw`.
- Modify: `crates/tqsdk-core/tests/runtime_contract_session.rs`
  - Add `// SAFETY:` comment before `Waker::from_raw`.
- Modify: `crates/tqsdk-core/tests/runtime_contract_session_cycle.rs`
  - Add `// SAFETY:` comment before `Waker::from_raw`.
- Modify: `crates/tqsdk-core/tests/runtime_contract_session_heartbeat.rs`
  - Add `// SAFETY:` comment before `Waker::from_raw`.
- Modify: `crates/tqsdk-core/tests/runtime_contract_session_reconnect.rs`
  - Add `// SAFETY:` comment before `Waker::from_raw`.
- Modify: `crates/tqsdk-core/tests/runtime_contract_session_runtime.rs`
  - Add `// SAFETY:` comment before `Waker::from_raw`.
- Modify: `crates/tqsdk-core/tests/runtime_contract_v1_capability.rs`
  - Add `// SAFETY:` comment before `Waker::from_raw`.
- Modify: `crates/tqsdk-core/tests/runtime_contract_ws_transport.rs`
  - Add `// SAFETY:` comment before `Waker::from_raw`.
- Modify: `crates/tqsdk-core/src/runtime/handle.rs`
  - Add `// SAFETY:` comment before the internal test helper `Waker::from_raw`.
- Modify: `crates/tqsdk-core/src/diff_protocol/outbound.rs`
  - Add intent comment for the skipped `rule` field.
- Modify: `crates/tqsdk-session/src/tq_auth.rs`
  - Add intent comment for the public OAuth client constants.
- Do not modify: `Cargo.toml`
  - License metadata is a separate repository decision, not part of this no-API-change batch.

---

## Task 1: Add Safety Comments For Environment Mutation

**Files:**
- Modify: `crates/tqsdk-core/tests/runtime_contract_endpoint_config.rs`

- [x] **Step 1: Add a comment before the first environment setup block**

Insert this comment immediately before the first `unsafe {` that calls `std::env::set_var`:

```rust
// SAFETY: endpoint config tests hold ENV_MUTEX while mutating process-wide
// environment variables, so no other test in this module observes partial
// updates during the scoped setup.
```

- [x] **Step 2: Add a comment before the second environment setup block**

Insert this comment immediately before the second `unsafe {` that calls `std::env::set_var`:

```rust
// SAFETY: endpoint config tests hold ENV_MUTEX while mutating process-wide
// environment variables, so these ignored keys are scoped to this serialized
// test section.
```

- [x] **Step 3: Add a comment inside `clear_env`**

Insert this comment immediately before the `unsafe { std::env::remove_var(key); }` block:

```rust
// SAFETY: callers hold ENV_MUTEX, serializing process-wide environment
// mutation for the duration of each endpoint config test.
```

- [x] **Step 4: Add comments inside `restore_env`**

Insert this comment before the `Some(value) => unsafe { ... }` arm:

```rust
// SAFETY: callers hold ENV_MUTEX, so restoring saved variables cannot race
// with another endpoint config test in this module.
```

Insert this comment before the `None => unsafe { ... }` arm:

```rust
// SAFETY: callers hold ENV_MUTEX, so removing variables absent in the saved
// snapshot is serialized with all other test environment mutation here.
```

---

## Task 2: Add Safety Comments For Noop Wakers

**Files:**
- Modify: `crates/tqsdk-core/src/runtime/handle.rs`
- Modify: `crates/tqsdk-core/tests/runtime_contract_command_ledger.rs`
- Modify: `crates/tqsdk-core/tests/runtime_contract_pending_route_executor.rs`
- Modify: `crates/tqsdk-core/tests/runtime_contract_route_connector.rs`
- Modify: `crates/tqsdk-core/tests/runtime_contract_route_dispatch.rs`
- Modify: `crates/tqsdk-core/tests/runtime_contract_runtime_core.rs`
- Modify: `crates/tqsdk-core/tests/runtime_contract_session.rs`
- Modify: `crates/tqsdk-core/tests/runtime_contract_session_cycle.rs`
- Modify: `crates/tqsdk-core/tests/runtime_contract_session_heartbeat.rs`
- Modify: `crates/tqsdk-core/tests/runtime_contract_session_reconnect.rs`
- Modify: `crates/tqsdk-core/tests/runtime_contract_session_runtime.rs`
- Modify: `crates/tqsdk-core/tests/runtime_contract_v1_capability.rs`
- Modify: `crates/tqsdk-core/tests/runtime_contract_ws_transport.rs`

- [x] **Step 1: Add the standard noop waker safety comment**

Insert this comment immediately before every uncovered `unsafe { Waker::from_raw(noop_raw_waker()) }` in the files above:

```rust
// SAFETY: the raw waker uses a static vtable with null data, does not own
// resources, and is only used to poll test futures that are expected to
// complete synchronously.
```

- [x] **Step 2: Confirm every matching unsafe has a safety comment**

Run:

```bash
rg -n -B 2 "unsafe \\{ Waker::from_raw" crates/tqsdk-core/src/runtime/handle.rs crates/tqsdk-core/tests
```

Expected: every match has a nearby preceding `// SAFETY:` comment.

---

## Task 3: Document OAuth Client Constants

**Files:**
- Modify: `crates/tqsdk-session/src/tq_auth.rs`

- [x] **Step 1: Add the OAuth constant comment**

Insert this comment immediately before `const CLIENT_ID`:

```rust
// These are ShinnyTech's public OAuth2 client identifiers, not user
// credentials. User passwords and access tokens still come from the runtime
// authentication flow; if the platform rotates this public client, a builder
// injection point can be considered in a separate API design.
```

---

## Task 4: Document `serde(skip)` Risk Rule Merge Intent

**Files:**
- Modify: `crates/tqsdk-core/src/diff_protocol/outbound.rs`

- [x] **Step 1: Add the reserved-field merge comment**

Insert this comment immediately before `#[serde(skip)]` on `SetRiskManagementRule::rule`:

```rust
// Merged manually in `into_value()` so caller-provided rule fields cannot
// override reserved protocol fields such as `aid` and `user_id`.
```

---

## Task 5: Verify And Commit

**Files:**
- Verify: all files modified in Tasks 1-4.
- Do not stage: `Cargo.toml`.

- [x] **Step 1: Verify no license metadata was added**

Run:

```bash
git diff -- Cargo.toml
```

Expected: no output.

- [x] **Step 2: Run targeted core tests**

Run:

```bash
cargo test -p tqsdk-core
```

Expected: all `tqsdk-core` tests pass.

- [x] **Step 3: Run workspace check**

Run:

```bash
cargo check --workspace
```

Expected: workspace check succeeds.

- [x] **Step 4: Commit the guardrail fixes**

Run:

```bash
git add crates/tqsdk-core/src/runtime/handle.rs crates/tqsdk-core/src/diff_protocol/outbound.rs crates/tqsdk-core/tests crates/tqsdk-session/src/tq_auth.rs
git commit -m "chore: add audit guardrails and safety documentation"
```

Expected: commit succeeds on branch `audit-guardrails`.

---

## Self-Review

- Spec coverage: Covers Task 2 safety comments, OAuth constant documentation, and `serde(skip)` intent documentation.
- License exclusion: `Cargo.toml` is explicitly out of scope until the repository license is chosen.
- API safety: No public API, runtime behavior, command shape, auth behavior, or crate boundary should change.
