# Strategy Test Partial Fill Steps Plan

## Scope

Advance S24 testability by letting public fake broker outcomes progress partial
fills across deterministic strategy test steps.

## Tasks

1. Add a failing strategy harness test for cross-step partial fills.
2. Add public `FakeBroker::partial_fills`.
3. Keep fake broker updates inside `tqsdk-task::testing`; do not change runtime
   command/state transition contracts.
4. Ensure each fake partial fill has a unique trade id.
5. Update S24 example and scenario review docs.
6. Run API contract checks, workspace examples, workspace tests, and clippy.

## Non-Goals

- No live/sim/replay environment adapter.
- No durable test fixture or cross-process intent persistence.
- No runtime transport reconnect model changes.
