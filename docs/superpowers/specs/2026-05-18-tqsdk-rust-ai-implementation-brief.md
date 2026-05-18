# TQSDK Rust Delta Spec

Use this brief inside the `tqsdk-rust` repository. It only describes the remaining delta against the current codebase.

## What is already in place

Do not re-specify these as new work in this brief:

- `tqsdk-stream` already has dynamic quote batch subscriptions with add/remove/close.
- `tqsdk-stream` already emits lag / closed / recovery diagnostics on its stream surface.
- `tqsdk-stream` already has K-line and tick row stream wrappers with `InitialSnapshot`, `Delta`, and `ResyncSnapshot`.
- `tqsdk-task` already has private trading-time elapsed logic built on `tqsdk_core::TradingTime`.

This brief is only for the missing public helper that can be reused by both core and wait-oriented consumers.

## Objective

Ship a pure trading-session status helper in `tqsdk-core` without expanding stream, UI, or data responsibilities.

## Must keep crate boundaries

- `tqsdk-core`: pure domain helpers and runtime substrate only.
- `tqsdk-wait`: single-owner live refs and Python-style `step()` workflows.
- `tqsdk-stream`: multi-consumer event pipelines, fan-out, sinks, lag diagnostics.
- `tqsdk-session`: one-shot metadata and service queries.
- `tqsdk-data`: historical/offline rows, downloads, cache, replay materialization.

Do not move an application gateway into the SDK. Do not add live-to-history-cache bridging.

## Delta scope

### In scope

1. Add a pure trading-session status helper in `tqsdk-core`.

### Out of scope

- New `quote_batches` or row-stream functionality in `tqsdk-stream`.
- Latest-only coalescing adapters.
- Recorder / persistence / history-cache bridging.
- UI, Tauri, or actor lifecycle integration.
- Any API expansion beyond the minimum needed for the status helper.

## Design target

Implement a small helper that can answer:

- what session phase is active at a given timestamp
- how long remains until the next boundary
- whether the schedule is currently open, in pre-close, or closed

Suggested shape:

```rust
let schedule = TradingSessionSchedule::from_segments(segments);
let status = schedule.status_at(now);
```

The helper should stay deterministic and reusable from both `tqsdk-core` and `tqsdk-wait` consumers.

## Requirements

- no network dependency
- no runtime side effects
- deterministic unit tests for open, pre-close, closed, and boundary rollover cases
- explicit handling of malformed or empty schedule input
- reuse existing schedule semantics instead of inventing a second interpretation in task code

## Acceptance criteria

- The helper is public and documented in the appropriate `tqsdk-core` surface.
- Status calculation is pure and testable with fixed timestamps.
- Existing stream quote/row behavior remains unchanged.
- The task-layer private trading-time logic can remain as-is for now; this brief does not require refactoring it.

## Validation checklist

Run the relevant checks after the helper lands:

- `cargo fmt --all --check`
- `cargo check --workspace --examples`
- crate-specific `cargo test`

## Recommended execution sequence

1. Implement `TradingSessionSchedule`.
2. Add unit tests for phase and countdown rollover.
3. Verify no stream/task API drift was introduced.
