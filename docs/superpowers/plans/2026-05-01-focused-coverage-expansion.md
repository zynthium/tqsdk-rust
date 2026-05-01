# Focused Coverage Expansion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the residual unit-test coverage gap called out by `docs/archive/reviews/2026-04-29/review-2026-04-29-pending.md` with focused, measurable helper/module tests instead of a broad unbounded 80% mandate.

**Architecture:** This plan does not change crate boundaries or public API ownership. It only adds characterization/unit tests around the existing runtime state helpers, session direct-query helper logic, wait change/window primitives, and task shared/deployment helper behavior. All tests must preserve the single runtime state/commit model and must not introduce hidden public `_for_test` APIs.

**Tech Stack:** Rust, Cargo workspace tests, crate unit tests, integration tests, existing `tqsdk_session::testing::ManualSession`, `tqsdk_wait::testing::WaitTestDriver`, and `tqsdk_task::testing` fixtures.

---

## Scope

This is a coverage expansion batch, not a feature batch and not a public API redesign batch.

Covered review leftovers:

- `tqsdk-core/src/state/changes.rs`
- `tqsdk-core/src/state/domain.rs`
- `tqsdk-core/src/state/path.rs`
- `tqsdk-core/src/state/read.rs`
- `tqsdk-session/src/metadata_helpers.rs`
- `tqsdk-session/src/services_helpers.rs`
- `tqsdk-wait/src/change.rs`
- `tqsdk-wait/src/views/kline_window.rs`
- `tqsdk-wait/src/views/tick_window.rs`
- `tqsdk-task/src/deployment.rs`
- `tqsdk-task/src/shared.rs`

Already covered enough for this batch:

- `tqsdk-core/src/order_lifecycle.rs`: P0 guardrails exist in `crates/tqsdk-core/tests/runtime_contract_order_lifecycle.rs`.
- `tqsdk-task/src/host.rs` and `tqsdk-task/src/strategy.rs`: P0 guardrails exist in `crates/tqsdk-task/tests/strategy_host.rs` and `crates/tqsdk-task/tests/strategy_testing.rs`.
- `tqsdk-wait/src/driver.rs`: P0 facade/driver guardrails exist in `crates/tqsdk-wait/tests/wait_api_*`.
- `tqsdk-core/src/aggregation.rs`: private unit coverage was moved into the module during the core safe-surface narrowing plan.
- `tqsdk-task/src/execution_group.rs`: integration coverage exists in `crates/tqsdk-task/tests/execution_group.rs`; this batch only adds missing helper-edge tests if a concrete uncovered branch is identified while executing Task 4.

Out of scope:

- Reaching global 80% coverage in one pass.
- Adding `cargo-tarpaulin`/coverage tooling.
- Changing public API, crate boundaries, or architecture docs.
- Adding new hidden test-only public constructors.

## File Structure

- Modify: `docs/superpowers/plans/2026-04-29-review-remediation-plan.md`
  - Link this plan as the coverage-expansion child plan and remove ambiguity that test coverage is fully closed.
- Modify: `docs/archive/reviews/2026-04-29/review-2026-04-29-pending.md`
  - Update only the status table after this plan is executed; do not rewrite the historical finding text.
- Modify: `crates/tqsdk-core/src/state/changes.rs`
  - Add private unit tests for change deduplication, field hit formation, and cursor tracker notification.
- Modify: `crates/tqsdk-core/src/state/path.rs`
  - Add private unit tests for segment preservation and object-key equality/hash behavior where useful.
- Modify: `crates/tqsdk-core/src/state/read.rs`
  - Add private unit tests for nested path reads, missing paths, decode success, and decode error path text.
- Modify: `crates/tqsdk-core/src/state/domain.rs`
  - Add private unit tests for market/trade partition reads returning typed values and missing partition behavior.
- Modify: `crates/tqsdk-session/src/metadata_helpers.rs`
  - Add private unit tests for option parsing/filtering/sorting and validation edge cases.
- Modify: `crates/tqsdk-session/src/services_helpers.rs`
  - Add private unit tests for symbol split, ranking value selection, service URL/date parsing, numeric coercion, and body truncation.
- Modify: `crates/tqsdk-wait/src/change.rs`
  - Add private unit tests for object match precedence, prefix path matching, unrelated path rejection, and field filtering.
- Modify: `crates/tqsdk-wait/src/views/kline_window.rs`
  - Add private unit tests for owned metadata and row access.
- Modify: `crates/tqsdk-wait/src/views/tick_window.rs`
  - Add private unit tests for owned metadata and row access.
- Modify: `crates/tqsdk-task/src/shared.rs`
  - Add private unit tests for shared state wrapper mutation/access behavior and quote/calendar deduplication.
- Modify: `crates/tqsdk-task/src/deployment.rs`
  - Add private unit tests for lifecycle defaults, retry policy decisions, shutdown signal behavior, supervisor health/report helpers, and market mode application.

## Task 1: Core State Helper Coverage

**Files:**
- Modify: `crates/tqsdk-core/src/state/changes.rs`
- Modify: `crates/tqsdk-core/src/state/path.rs`
- Modify: `crates/tqsdk-core/src/state/read.rs`
- Modify: `crates/tqsdk-core/src/state/domain.rs`

- [ ] **Step 1: Add failing tests for `ChangeSet` and `UpdateCursor`**

Add a `#[cfg(test)] mod tests` to `crates/tqsdk-core/src/state/changes.rs` containing tests named:

- `change_set_deduplicates_path_object_and_field_hits`
- `update_cursor_notifies_tracker_when_revision_advances`
- `update_cursor_clone_keeps_identity_and_next_revision`

The tests must construct repeated `NormalizedMutation` values with the same `StatePath`, `ObjectKey`, and field names, then assert that `ChangeSet::from_mutations()` keeps one path hit, one object hit, and one field hit per distinct field.

- [ ] **Step 2: Verify the new `changes.rs` tests fail before implementation**

Run:

```bash
cargo test -p tqsdk-core state::changes::tests -- --nocapture
```

Expected:

```text
FAIL or compile error because the new tests have just been added and may expose missing imports/test helpers.
```

- [ ] **Step 3: Add tests for state path/read/domain behavior**

Add private unit tests with these names:

- `state_path_preserves_segment_order`
- `state_read_get_at_path_returns_nested_values`
- `state_read_decode_value_reports_path_on_type_error`
- `market_state_view_reads_quote_and_returns_none_for_missing_symbol`
- `trade_state_view_reads_account_position_order_and_trade`

Use `serde_json::json!` fixtures and existing typed schema structs. Do not add new public helpers.

- [ ] **Step 4: Run core state tests**

Run:

```bash
cargo test -p tqsdk-core state:: -- --nocapture
```

Expected:

```text
All core state helper tests pass.
```

- [ ] **Step 5: Commit core coverage**

```bash
git add crates/tqsdk-core/src/state/changes.rs crates/tqsdk-core/src/state/path.rs crates/tqsdk-core/src/state/read.rs crates/tqsdk-core/src/state/domain.rs
git commit -m "test: cover core state helper behavior"
```

## Task 2: Session Metadata And Services Helper Coverage

**Files:**
- Modify: `crates/tqsdk-session/src/metadata_helpers.rs`
- Modify: `crates/tqsdk-session/src/services_helpers.rs`

- [ ] **Step 1: Add failing metadata helper tests**

Add private tests named:

- `non_empty_str_rejects_empty_input`
- `parse_query_quotes_result_extracts_symbols`
- `parse_query_cont_quotes_result_ignores_non_array_payload`
- `parse_option_nodes_flattens_nested_option_payload`
- `bisect_value_index_respects_left_and_right_priority`
- `filter_option_nodes_applies_class_expire_and_price_filters`
- `sort_options_and_get_atm_index_orders_by_strike`
- `timestamp_nano_to_datetime_converts_nanoseconds`
- `validate_finance_nearbys_rejects_empty_or_negative_nearby`

Use compact JSON fixtures local to the tests. Keep tests in the helper module so `pub(super)` helpers remain non-public.

- [ ] **Step 2: Add failing services helper tests**

Add private tests named:

- `split_symbol_splits_exchange_and_instrument`
- `ranking_value_returns_selected_numeric_field`
- `parse_service_url_reports_label_on_invalid_url`
- `parse_iso_date_rejects_invalid_dates`
- `next_day_rejects_overflow`
- `json_value_to_f64_handles_numbers_strings_and_invalid_values`
- `truncate_body_limits_error_payload_size`

- [ ] **Step 3: Run session helper tests**

Run:

```bash
cargo test -p tqsdk-session metadata_helpers services_helpers -- --nocapture
```

Expected:

```text
All session helper tests pass.
```

- [ ] **Step 4: Commit session coverage**

```bash
git add crates/tqsdk-session/src/metadata_helpers.rs crates/tqsdk-session/src/services_helpers.rs
git commit -m "test: cover session query helper behavior"
```

## Task 3: Wait Change And Window Helper Coverage

**Files:**
- Modify: `crates/tqsdk-wait/src/change.rs`
- Modify: `crates/tqsdk-wait/src/views/kline_window.rs`
- Modify: `crates/tqsdk-wait/src/views/tick_window.rs`

- [ ] **Step 1: Add failing `change.rs` tests**

Add private tests named:

- `matches_any_prefers_object_hits`
- `matches_any_accepts_changed_child_path`
- `matches_any_rejects_unrelated_parent_or_sibling_path`
- `matches_fields_requires_object_key`
- `matches_fields_matches_only_requested_fields`

Create a tiny test-only `Tracked` struct implementing `ChangeTrackedRef`. Use `ChangeSet`, `ChangeHit`, `StatePath`, and `ObjectKey` directly.

- [ ] **Step 2: Add failing window tests**

Add private tests named:

- `kline_window_exposes_owned_metadata_and_rows`
- `kline_window_empty_reports_no_last_row`
- `tick_window_exposes_owned_metadata_and_rows`
- `tick_window_empty_reports_no_last_row`

Use `Kline::default()` and `Tick::default()` rows. Assert `len`, `is_empty`, `last`, `get`, `iter().count()`, and metadata accessors.

- [ ] **Step 3: Run wait helper tests**

Run:

```bash
cargo test -p tqsdk-wait change views -- --nocapture
```

Expected:

```text
All wait change/window tests pass.
```

- [ ] **Step 4: Commit wait coverage**

```bash
git add crates/tqsdk-wait/src/change.rs crates/tqsdk-wait/src/views/kline_window.rs crates/tqsdk-wait/src/views/tick_window.rs
git commit -m "test: cover wait change and window helpers"
```

## Task 4: Task Shared And Deployment Helper Coverage

**Files:**
- Modify: `crates/tqsdk-task/src/shared.rs`
- Modify: `crates/tqsdk-task/src/deployment.rs`
- Verify: `crates/tqsdk-task/src/execution_group.rs`
- Verify: `crates/tqsdk-task/tests/execution_group.rs`

- [ ] **Step 1: Add failing shared-state tests**

Add private tests to `crates/tqsdk-task/src/shared.rs` named:

- `shared_task_state_read_and_update_round_trips_value`
- `shared_quote_subscriptions_deduplicates_symbols`
- `shared_trading_calendar_replaces_days_atomically`

Do not expose the wrappers publicly. Test through existing crate-private methods only.

- [ ] **Step 2: Add failing deployment helper tests**

Add private tests to `crates/tqsdk-task/src/deployment.rs` named:

- `strategy_lifecycle_defaults_are_idle_without_started_revision`
- `strategy_retry_policy_allows_only_configured_attempts`
- `strategy_shutdown_signal_records_reason_once`
- `strategy_supervisor_health_reflects_failure_counts`
- `strategy_supervisor_report_marks_shutdown_reason`
- `apply_market_mode_maps_provider_sim_and_replay_modes`

Use existing public builders and crate-private helpers. If a test requires async strategy execution, use `#[tokio::test]` and the existing manual session/test fixtures rather than new hidden public APIs.

- [ ] **Step 3: Reconcile execution group coverage**

Read `crates/tqsdk-task/tests/execution_group.rs` and verify it still covers:

- multi-leg submit
- revision-bound report
- missing group id rejection
- all-leg preflight before dispatch
- risk rejection without partial dispatch
- retry intent reuse
- incompatible retry rejection
- all-filled outcome
- partial fill exposure
- hedge timeout outcome

If any item is missing, add one focused integration test to `crates/tqsdk-task/tests/execution_group.rs`. If all items are present, make no code change for `execution_group.rs` and record that in the final note.

- [ ] **Step 4: Run task helper tests**

Run:

```bash
cargo test -p tqsdk-task shared deployment execution_group -- --nocapture
```

Expected:

```text
All task helper and execution group tests pass.
```

- [ ] **Step 5: Commit task coverage**

```bash
git add crates/tqsdk-task/src/shared.rs crates/tqsdk-task/src/deployment.rs crates/tqsdk-task/tests/execution_group.rs
git commit -m "test: cover task shared and deployment helpers"
```

## Task 5: Documentation Closure And Workspace Verification

**Files:**
- Modify: `docs/superpowers/plans/2026-05-01-focused-coverage-expansion.md`
- Modify: `docs/superpowers/plans/2026-04-29-review-remediation-plan.md`
- Modify: `docs/archive/reviews/2026-04-29/review-2026-04-29-pending.md`

- [ ] **Step 1: Mark completed tasks in this plan**

Update each completed checkbox in this file. Add a short execution note listing commits produced by Tasks 1-4.

- [ ] **Step 2: Update the umbrella remediation roadmap**

In `docs/superpowers/plans/2026-04-29-review-remediation-plan.md`, update the remaining-items section to say that focused coverage expansion has been executed and only global numeric coverage tooling remains out of scope if no coverage tool was added.

- [ ] **Step 3: Update the archived review status table**

In `docs/archive/reviews/2026-04-29/review-2026-04-29-pending.md`, change:

- `2.1 总体覆盖率不足` to `focused batch done; global threshold out of scope`
- `2.2 缺少测试的关键模块` to `focused batch done`
- `2.3 测试基础设施利用不足` to `focused batch done; cross-crate fake expansion out of scope`

Do not remove the original historical finding text below the table.

- [ ] **Step 4: Run workspace verification**

Run:

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace --tests
cargo clippy --workspace --all-targets -- -D warnings
cargo check --workspace --examples
git diff --check
```

Expected:

```text
All commands pass.
```

- [ ] **Step 5: Commit documentation closure**

```bash
git add docs/superpowers/plans/2026-05-01-focused-coverage-expansion.md docs/superpowers/plans/2026-04-29-review-remediation-plan.md docs/archive/reviews/2026-04-29/review-2026-04-29-pending.md
git commit -m "docs: close focused coverage expansion plan"
```

## Completion Criteria

- Every module listed in this plan has focused helper/unit coverage or an explicit no-change rationale backed by existing tests.
- No new hidden public `_for_test` API was added.
- No public API or crate boundary changed.
- The umbrella remediation roadmap no longer implies test coverage is fully closed before this batch has executed.
- Workspace verification passes.

## Self-Review

- Spec coverage: This plan addresses the residual coverage findings from `review-2026-04-29-pending.md` without reopening already-closed P0 guardrails or module split work.
- Placeholder scan: There are no `TBD` or unspecified implementation placeholders; each task names concrete files, test names, commands, and commit boundaries.
- Type consistency: Test targets use existing crate-private modules and public fixture crates; no new test-only public constructors are required.
