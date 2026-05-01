# Strategy Supervisor Foundation

Goal: advance S15/S20 by adding a task-layer supervisor on top of
`StrategyDeployment` without changing core/session/wait/stream runtime
contracts.

Scope:

- Add `StrategySupervisor` with typed stop reason, health snapshot and metrics.
- Add explicit `StrategyRetryPolicy`, defaulting to no hidden retries.
- Add `StrategyShutdownSignal` with manual and ctrl-c shutdown modes.
- Update S15 to run provider-backed/fake/replay deployments through the
  supervisor.
- Add a formal S20 task supervisor example.
- Update public scenario review and task architecture docs.

Non-goals:

- HTTP metrics or health endpoint.
- Persistent daemon process manager.
- Durable sink isolation or per-sink retry/storage policy.
- Full reconnect orchestration beyond the underlying stream/session health
  foundation.

Verification:

- [x] `cargo test -p tqsdk-task --test strategy_environment -- --nocapture`
- [x] `scripts/check_api_contract_examples.sh`
- [x] `cargo check --workspace --examples`
- [x] `cargo test --workspace`
- [x] `cargo clippy --workspace --examples --all-targets -- -D warnings`
- [x] `cargo check --workspace --no-default-features`
- [x] `cargo check --workspace --all-features --examples`
- [x] `git diff --check`
- [x] `cargo run --example api_contract_s20_strategy_supervisor`
- [x] `cargo run --example api_contract_s15_live_sim_replay_switch`
