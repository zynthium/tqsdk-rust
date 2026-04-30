# Target Position Control Flow Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Simplify `TargetPosTaskInner::process_wait_update()` and related cancellation paths without changing target-position behavior.

**Architecture:** Keep target-position semantics in `tqsdk-task`. Do not move task behavior into core/session/wait, do not change planner output, and do not change `wait_target_reached()` or `wait_finished()` completion conditions.

**Tech Stack:** Rust private methods, existing `tqsdk-task` target-pos tests.

---

## Files

- Modify: `crates/tqsdk-task/src/target_pos.rs`
- Verify: `crates/tqsdk-task/tests/target_pos.rs`
- Verify: `crates/tqsdk-task/tests/scheduler.rs`

## Task 1: Characterize Existing TargetPos Behavior

- [x] Run `cargo test -p tqsdk-task host_wait_update_timeout_still_advances_target_pos_with_existing_quote`.
- [x] Run `cargo test -p tqsdk-task target_pos_cancel_waits_for_live_order_to_finish_before_releasing_ownership`.
- [x] Run `cargo test -p tqsdk-task open_only_target_pos_retarget_cancels_unmaterialized_live_order_before_reaching_target`.
- [x] Run `cargo test -p tqsdk-task default_target_pos_replan_keeps_live_orders_after_stale_subset_finishes`.
- [x] Run `cargo test -p tqsdk-task target_pos_wait_finished_returns_error_when_insert_order_submission_fails`.

Note:

- The fifth planned test name did not exist in the workspace; the matching existing characterization test `target_pos_wait_target_reached_returns_error_when_insert_order_submission_fails` was run and passed.

Expected: all focused tests pass before refactor.

## Task 2: Split `process_wait_update()`

- [x] Extract cancel handling into `process_cancel_requested(api) -> impl Future<Output = Result<ProcessStep>>`.
- [x] Extract target planning into `desired_batch_for_current_state(api, target_volume)`.
- [x] Extract target reached check into `mark_reached_if_current_position_matches(current_seq, current_net_position, target_volume) -> bool`.
- [x] Keep exactly one top-level call to `record_commit_trades(api)`.

## Task 3: Simplify Cancellation Bookkeeping

- [x] In `cancel_pending_orders_filtered()`, replace manual `contains` plus `insert` with `HashSet::insert` return value.
- [x] Ensure failed cancel submission removes the order id from `cancel_requested_order_ids`.
- [x] Keep `record_cancel_order()` only after successful cancel submission.

## Task 4: Reuse `finish()` In Drop

- [x] Change `Drop for TargetPosTaskInner` to call `self.finish()`.
- [x] Preserve idempotency through the existing `finished.swap(true, Ordering::SeqCst)` in `finish()`.
- [x] Verify managed host unregister behavior remains unchanged.

## Task 5: Verify

- [x] Run `cargo test -p tqsdk-task --test target_pos`.
- [x] Run `cargo test -p tqsdk-task --test scheduler`.
- [x] Run `cargo test -p tqsdk-task`.

Expected: all task tests pass with no public API changes.

## Task 6: Commit

- [x] Run `git add crates/tqsdk-task/src/target_pos.rs`.
- [x] Run `git commit -m "refactor: simplify target position control flow"`.

Output:

- Committed as `49ff822 refactor: simplify target position control flow`.
