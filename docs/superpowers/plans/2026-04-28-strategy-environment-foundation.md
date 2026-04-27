# Strategy Environment Foundation Plan

## Scope

Advance S15 by adding a minimal task-layer strategy environment adapter that
lets one strategy step run on task-host live/sim, public fake harness, and
cache replay contexts.

## Tasks

1. Add a failing public API test for a shared strategy step over simulated and
   replay environments.
2. Add `StrategyEnvironment`, `StrategyEnvironmentBuilder`,
   `StrategyEnvironmentContext`, and `StrategyEnvironmentKind` in `tqsdk-task`.
3. Delegate common context reads and typed execution entrypoints to existing
   `StrategyContext` / `StrategyReplayContext`.
4. Add a formal S15 API contract example.
5. Update scenario review, gap notes, crate README, and task architecture docs.
6. Run API contract checks, workspace examples, workspace tests, and clippy.

## Non-Goals

- No provider aggregation facade.
- No provider-backed sim environment implementation.
- No deployment config loader, lifecycle supervisor, or graceful shutdown model.
- No changes to core/session runtime commit contracts.
