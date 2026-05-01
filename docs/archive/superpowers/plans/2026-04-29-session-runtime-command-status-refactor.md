# Session Runtime Command Status Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract repeated command-status derivation helpers from `crates/tqsdk-core/src/session_runtime.rs` without changing command status semantics.

**Architecture:** Keep all runtime status transitions behind `RuntimeHandle::record_command_status()`. Do not change `RuntimeHandle -> StateStore -> CommitResult -> RuntimeReader`, command status strings, status detail shape, or public exports.

**Tech Stack:** Rust private modules, existing core integration tests.

---

## Files

- Modify: `crates/tqsdk-core/src/session_runtime.rs`
- Create: `crates/tqsdk-core/src/session_runtime/command_status.rs`
- Verify: `crates/tqsdk-core/tests/runtime_contract_session_cycle.rs`
- Verify: `crates/tqsdk-core/tests/runtime_contract_pending_route_executor.rs`

## Task 1: Characterize Existing Status Semantics

- [x] Run `cargo test -p tqsdk-core session_runtime_trade_order_diff_marks_transport_command_acked`.
- [x] Run `cargo test -p tqsdk-core session_runtime_trade_order_finish_diff_marks_transport_command_completed`.
- [x] Run `cargo test -p tqsdk-core session_runtime_trade_reject_diff_marks_transport_command_rejected`.
- [x] Run `cargo test -p tqsdk-core session_runtime_trade_login_snapshot_marks_transport_command_completed`.
- [x] Run `cargo test -p tqsdk-core session_runtime_trade_account_info_diff_marks_transport_command_completed`.
- [x] Run `cargo test -p tqsdk-core session_runtime_trade_pre_insert_order_diff_marks_transport_command_completed`.
- [x] Run `cargo test -p tqsdk-core session_runtime_trade_risk_management_rule_diff_marks_transport_command_completed`.
- [x] Run `cargo test -p tqsdk-core session_runtime_trade_settlement_reply_marks_transport_command_completed`.

Expected: every command passes before extraction.

## Task 2: Extract Private Command Status Helpers

- [x] Add `mod command_status;` near the other `session_runtime.rs` module-level items.
- [x] Create `crates/tqsdk-core/src/session_runtime/command_status.rs`.
- [x] Move only pure helper logic into the new module:
  - path-backed completed status detail construction
  - path-prefix-backed query status detail construction
  - trade order `ALIVE` / `FINISHED` status mapping
- [x] Keep `SessionRuntime::record_transport_commit_statuses()` and `RuntimeHandle::record_command_status()` calls in `session_runtime.rs`.
- [x] Do not move reconnect, heartbeat, or transport recovery code in this child plan.

## Task 3: Rewire Derivation Methods

- [x] Update `derive_query_command_status()` to call the query path-prefix helper.
- [x] Update `derive_trade_login_command_status()` to call a helper but preserve the `trade_more_data == false` guard and extra detail.
- [x] Update `derive_trade_account_info_command_status()` to call the path-backed completed helper with `currency = "CNY"`.
- [x] Update `derive_trade_pre_insert_order_command_status()` to call the path-backed completed helper and preserve optional `pre_margin`.
- [x] Update `derive_trade_risk_management_rule_command_status()` to call the path-backed completed helper with `exchange_id`.
- [x] Update `derive_trade_settlement_query_command_status()` to call the path-backed completed helper with `trading_day`.
- [x] Update `derive_trade_order_command_status()` to call the order helper and preserve `order_status`, `exchange_order_id`, `last_msg`, and `volume_left`.

## Task 4: Verify

- [x] Run `cargo test -p tqsdk-core session_runtime_trade`.
- [x] Run `cargo test -p tqsdk-core runtime_contract_pending_route_executor`.
- [x] Run `cargo test -p tqsdk-core`.

Expected: all core tests pass with no public API changes.

## Task 5: Commit

- [x] Run `git add crates/tqsdk-core/src/session_runtime.rs crates/tqsdk-core/src/session_runtime/command_status.rs`.
- [x] Run `git commit -m "refactor: extract session command status derivation"`.

Output:

- Committed as `7e43df8 refactor: extract session command status derivation`.
