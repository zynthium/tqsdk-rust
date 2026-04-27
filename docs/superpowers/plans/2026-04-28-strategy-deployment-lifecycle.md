# Strategy Deployment Lifecycle Plan

## Scope

Advance S15 beyond the environment foundation by adding task-layer deployment
configuration and lifecycle wrappers for provider-backed sim, live trade, fake
sim, and replay usage.

## Tasks

1. Add failing public API tests for provider-backed TQKQ sim config, deployment
   lifecycle max-step stop, and graceful shutdown reporting.
2. Extract reusable strategy environment subscription config.
3. Add `StrategyDeploymentConfig`, `StrategyDeployment`, `StrategyLifecycle`,
   typed run stop reasons, and shutdown reports in `tqsdk-task`.
4. Wire `StrategyEnvironment::from_config(...)` for live trade and TQKQ sim
   provider-backed deployments without exposing provider protocol details to
   strategy code.
5. Update the formal S15 example to use deployment config and lifecycle.
6. Update scenario review, gap notes, crate README, and task architecture docs.
7. Run API contract checks, examples, workspace tests, clippy, and diff checks.

## Non-Goals

- No production supervisor or metrics endpoint.
- No ctrl-c shutdown hook.
- No retry orchestration beyond existing lower-level reconnect behavior.
- No multi-provider market aggregation.
- No changes to core/session runtime commit contracts.
