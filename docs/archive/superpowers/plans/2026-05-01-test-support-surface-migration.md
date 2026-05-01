# Test Support Surface Migration Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` and `superpowers:receiving-code-review`. Use `superpowers:test-driven-development` before production code changes. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace hidden `_for_test` public APIs with explicit, stable test/support surfaces where they are legitimate, and with ordinary production APIs where the behavior is useful outside tests.

**Architecture:** This plan must not move runtime/session/wait/stream responsibilities across crate boundaries. Test support may live in existing `testing` modules or facade-owned support APIs, but it must not expose raw runtime mutation, private channel handles, or alternate state trees as a normal user API.

**Review Source:** `docs/reviews/comprehensive-review-2026-04-30.md` retained `_for_test` feature-gating as an independent item because `tqsdk-task::testing` and integration contracts still depend on injected runtimes and manual commit control.

## Current Findings

- `TaskHost::check_manual_order_allowed_for_test()` is not inherently test-only; it is a task-layer dry-run for the same ownership guard used by `insert_order_guarded()`.
- `TaskHost::register_target_owner_for_test()` and `unregister_task_for_test()` are no longer used by integration tests and should not remain hidden public ABI.
- `TaskHost::register_scheduler_owner_for_test()` is only used to simulate a scheduler ownership conflict; tests can use the real scheduler builder instead.
- `TqApi::handle_for_test()`, `push_deferred_commit_for_test()`, and wait/stream/session dispatch controls originally backed many integration fixtures; they have now been replaced by explicit testing modules or the public session/runtime substrate.
- `StrategyTestHarness` already covers user-facing fake market/fake broker strategy tests; the next migration should extend this direction rather than exposing lower-level runtime mutation.

## Task 1: Retire TaskHost Hidden Ownership Hooks

**Files:**
- Modify: `crates/tqsdk-task/src/host.rs`
- Modify: `crates/tqsdk-task/tests/target_pos.rs`
- Modify: `crates/tqsdk-task/tests/scheduler.rs`

- [x] Add a public `TaskHost::check_manual_order_allowed()` dry-run method that delegates to the registry ownership guard.
- [x] Replace task tests that call `check_manual_order_allowed_for_test()` with `check_manual_order_allowed()`.
- [x] Replace the one `register_scheduler_owner_for_test()` conflict assertion with a real scheduler build attempt.
- [x] Remove unused hidden public `register_target_owner_for_test()`, `register_scheduler_owner_for_test()`, and `unregister_task_for_test()`.
- [x] Verify `cargo test -p tqsdk-task --test target_pos --test scheduler`.

**Verification:** `cargo test -p tqsdk-task --test target_pos --test scheduler` passed with 61 tests.

## Task 2: Inventory Remaining Wait/Stream/Session Hidden Hooks

**Files:**
- Modify: `docs/superpowers/plans/2026-05-01-test-support-surface-migration.md`

- [x] Record every remaining `#[doc(hidden)]` `_for_test` item after Task 1.
- [x] Classify each item as `replace with normal public API`, `move behind explicit testing API`, or `keep sibling/internal bridge`.
- [x] Identify the smallest next code slice that can reduce hidden surface without blocking integration tests.

**Inventory after Task 1:**

| Item | Current role | Classification |
|------|--------------|----------------|
| `SessionClient::new_for_test_with_handle()` | Removed; callers now use `tqsdk_session::testing::ManualSession` | Done |
| `SessionClient::drain_dispatches()` | Removed; manual/no-IO outbox inspection now belongs to `ManualSession::drain_dispatches()` | Done |
| `TqApi::handle_for_test()` | Removed; task fixture/test callers use the public `api.session().handle()` substrate | Done |
| `TqApi::begin_wait_for_test()` | Removed; wait concurrency characterization now belongs to `tqsdk_wait::testing::WaitTestDriver` | Done |
| `TqApi::push_deferred_commit_for_test()` | Removed; deferred commit fixture control now belongs to `tqsdk_wait::testing::WaitTestDriver` | Done |
| `TqStream::handle_for_test()` | Removed; seeded stream fixtures use `stream.session().handle()` | Done |
| `TqStream::emit_session_error_for_test()` / `emit_closed_for_test()` / `close_driver_for_test()` | Removed; lifecycle/error characterization now belongs to `tqsdk_stream::testing::StreamTestDriver` | Done |
| `TargetPosTask::applied_target_volume_for_test()` | Removed; `applied_target_volume()` is now the documented public API | Done |
| `TargetPosTask::track_order_for_test()` | Internal unit-test-only helper | Keep crate-internal cfg(test) bridge |
| `__tqsdk_impl_session_builder_forwarders!` | Macro exported only for sibling facade builders | Keep sibling/internal bridge |

Task-side hidden ownership hooks removed in this batch:

- `TaskHost::register_target_owner_for_test()`
- `TaskHost::register_scheduler_owner_for_test()`
- `TaskHost::check_manual_order_allowed_for_test()`
- `TaskHost::unregister_task_for_test()`

Additional task-side duplicate hidden API removed:

- `TargetPosTask::applied_target_volume_for_test()`

**Verification:** `cargo test -p tqsdk-task --test target_pos --test live_smoke --no-run` and `cargo test -p tqsdk-task --test target_pos` passed after replacing callers with `applied_target_volume()`.

**Next code slice:** Re-run hidden API inventory and reassess feature gating now that session/wait/stream public hidden test hooks have been removed.

## Task 3: Stabilize Low-Level Test Driver Entry Points

**Candidate files:**
- `crates/tqsdk-session/src/client.rs`
- `crates/tqsdk-wait/src/api.rs`
- `crates/tqsdk-stream/src/api.rs`
- test support modules under each crate

- [x] Add or refine explicit test-driver APIs for manual/no-IO sessions and facade fixtures.
- [x] Migrate integration tests away from direct `handle_for_test()`/`drain_dispatches()` where a facade-owned test driver can express the same behavior.
- [x] Keep raw runtime ingest confined to explicit testing support or the public session/runtime substrate, without hidden facade hooks.

### Session Subtask

- [x] Add `tqsdk_session::testing::ManualSession` as the explicit no-IO/manual fixture wrapper.
- [x] Refactor session manual construction/dispatch draining to internal manual-session helpers, preventing behavior drift while downstream callers migrate.
- [x] Migrate session integration tests that need manual construction/dispatch draining to `ManualSession`.
- [x] Migrate downstream session construction in wait/stream/task/data tests and helpers to `ManualSession`.
- [x] Remove `SessionClient::new_for_test_with_handle()`.
- [x] Remove `SessionClient::drain_dispatches()` after moving manual outbox inspection to `ManualSession::drain_dispatches()` and core-handle based stream tests.

**Verification:**

- `cargo test -p tqsdk-session --test session_direct_query --test session_recovery --test session_order_intent --test session_market_command_helpers` passed after adding `ManualSession`.
- `cargo fmt --all --check`, `cargo check --workspace`, and `cargo test --workspace --tests` passed after migrating downstream wait/stream/task/data construction callers and removing `SessionClient::new_for_test_with_handle()`.

### Wait/Stream Handle Subtask

- [x] Migrate wait crate tests/support off `TqApi::handle_for_test()` to `api.session().handle()`.
- [x] Migrate stream crate tests/support off `TqStream::handle_for_test()` to `stream.session().handle()`.
- [x] Remove `TqStream::handle_for_test()`.
- [x] Remove `TqApi::handle_for_test()` after task fixture/test callers are migrated.

**Verification:** `cargo fmt --all --check`, `cargo check --workspace`, `cargo test -p tqsdk-wait --tests`, and `cargo test -p tqsdk-stream --tests` passed.

### Wait Fixture Subtask

- [x] Add `tqsdk_wait::testing::WaitTestDriver` as the explicit fixture entry point for wait guard and deferred commit characterization.
- [x] Migrate wait tests/support off `begin_wait_for_test()` and `push_deferred_commit_for_test()`.
- [x] Migrate task fixture/test callers off `TqApi::handle_for_test()` to the public `api.session().handle()` runtime substrate.
- [x] Remove `TqApi::begin_wait_for_test()`, `TqApi::push_deferred_commit_for_test()`, and `TqApi::handle_for_test()`.

**Verification:** `cargo test -p tqsdk-wait --tests` and `cargo test -p tqsdk-task --tests` passed.

### Stream Lifecycle Subtask

- [x] Add `tqsdk_stream::testing::StreamTestDriver` as the explicit fixture entry point for synthetic stream driver lifecycle events.
- [x] Migrate stream lifecycle tests off `emit_session_error_for_test()`, `emit_closed_for_test()`, and `close_driver_for_test()`.
- [x] Remove the `TqStream` lifecycle `_for_test` methods.

**Verification:** `cargo test -p tqsdk-stream --test stream_session --test stream_surface --test stream_diagnostics` passed.

## Task 4: Reassess Feature Gating

- [x] After callers are migrated, re-run the hidden API inventory.
- [x] Feature-gate or privatize any remaining `_for_test` functions that are no longer externally required.
- [x] Update `docs/reviews/comprehensive-review-2026-04-30.md` and `docs/architecture/*` only if API boundaries changed.

## Exit Criteria

- `TaskHost` no longer has hidden public ownership hooks.
- Remaining hidden test hooks have an explicit migration class and owner.
- Tests prove the new public dry-run path uses the same ownership guard as guarded order submission.
- No architecture boundary is changed without corresponding architecture documentation.
