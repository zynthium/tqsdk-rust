# Session Route Driving Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce duplicated route and pending-route driving logic in `tqsdk-session` without changing public session APIs.

**Architecture:** Keep `tqsdk-session` as shared session + one-shot request/response. Do not add wait/stream consumer configuration, do not expand public surface, and keep all runtime driving through `tqsdk_core::internal::SessionRuntime`.

**Tech Stack:** Rust private helpers, existing session integration tests.

---

## Files

- Modify: `crates/tqsdk-session/src/client/io.rs`
- Verify: `crates/tqsdk-session/tests/session_direct_query.rs`
- Verify: `crates/tqsdk-session/tests/session_market_command_helpers.rs`

## Task 1: Characterize Existing Route Driving

- [x] Run `cargo test -p tqsdk-session session_direct_query`.
- [x] Run `cargo test -p tqsdk-session session_market_command_helpers`.
- [x] Run `cargo test -p tqsdk-session live_client_progress_once`.

Expected: all targeted tests pass before extraction.

## Task 2: Extract Deadline Handling Helper

- [x] Add a private async helper in `io.rs` that runs a future with `Option<Instant>` and returns `Ok(None)` when the deadline is elapsed or times out.
- [x] Use this helper in `drive_route_label_once()` and `drive_route_once_locked()`.
- [x] Preserve the current zero-budget behavior: return `Ok(false)` without driving the runtime.

## Task 3: Extract Pending Route Execution Helper

- [x] Add a private helper that resolves the route executor for `Http`, `Internal`, and `Replay` endpoints.
- [x] Use that helper from `drive_pending_route_label_once()` and `drive_pending_once_locked()`.
- [x] Preserve current behavior for `WebSocket` pending routes: return `Ok(false)`.

## Task 4: Verify

- [x] Run `cargo test -p tqsdk-session`.

Expected: all session tests pass with no public API changes.

## Task 5: Commit

- [x] Run `git add crates/tqsdk-session/src/client/io.rs`.
- [x] Run `git commit -m "refactor: deduplicate session route driving"`.

Output:

- Committed as `a7c42e8 refactor: deduplicate session route driving`.
