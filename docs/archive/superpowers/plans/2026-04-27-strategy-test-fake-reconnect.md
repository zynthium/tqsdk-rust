# Strategy Test Fake Reconnect Plan

## Scope

Advance S24 testability by adding fake broker disconnect/reconnect injection to
`tqsdk-task::testing`.

## Tasks

1. Add a failing strategy harness test for broker disconnect and reconnect.
2. Add public `FakeBroker::disconnect_for_steps`.
3. Add public `FakeBrokerConnectionStatus` and expose it from `StrategyTestReport`.
4. Keep fake reconnect inside `tqsdk-task::testing`; do not change runtime/session reconnect contracts.
5. Update S24 example and scenario review docs.
6. Run API contract checks, workspace examples, workspace tests, and clippy.

## Non-Goals

- No cross-process recovery or durable intent store.
- No live/sim/replay environment adapter.
- No runtime transport reconnect model changes.
