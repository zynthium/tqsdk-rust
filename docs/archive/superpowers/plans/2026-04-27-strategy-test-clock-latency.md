# Strategy Test Clock And Latency Plan

## Scope

Advance S24 testability without changing runtime/core boundaries.

## Tasks

1. Add public `StrategyTestClock` to `tqsdk-task::testing`.
2. Add fake broker step latency via `FakeBroker::latency_steps`.
3. Expose pending fake orders on `StrategyTestReport`.
4. Update S24 example and scenario review docs.
5. Verify task tests, API contract examples, workspace examples, tests, and clippy.

## Non-Goals

- No fake reconnect implementation in this batch.
- No live/sim/replay environment adapter in this batch.
- No changes to `tqsdk-core` runtime commit or state model.
