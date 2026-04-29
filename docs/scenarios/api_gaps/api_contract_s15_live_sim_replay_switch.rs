//! Scenario: 实盘 / 模拟 / 回放切换
//!
//! User goal:
//! - 同一套策略代码在实盘、模拟、回放之间切换
//! - provider 差异只出现在构建配置
//! - 策略逻辑只依赖标准事件和执行接口
//!
//! API contract:
//! - public API 提供稳定 StrategyRuntime/Environment trait
//! - live/sim/replay 都实现同一策略输入输出契约
//! - replay 不要求策略改写成底层 replay command loop
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 策略中写 `if live { ... } else if replay { ... }`
//! - `ReplayCommand` 泄漏到策略主逻辑
//! - provider 内部 session / protocol type
//! - 多套状态读取模型
//!
//! Regression signal:
//! - 策略从实盘迁到回放需要改事件循环
//! - 回放无法复用同一 execution/risk 接口
//! - 模拟账户和实盘账户状态类型不同
//!
//! Review questions:
//! - 当前 API 是否自然表达运行环境切换？
//! - 是否保持同一策略 contract？
//! - 是否需要新增 strategy/runtime facade？
//!
//! Current API note:
//! `tqsdk-task::StrategyEnvironment` / `StrategyEnvironmentContext` 已提供统一
//! strategy context；`StrategyDeploymentConfig` / `StrategyDeployment` /
//! `StrategyLifecycle` 已覆盖 provider-backed TQKQ sim config、live trade config、
//! fake/replay deployment wrapper、typed run stop reason 和 graceful shutdown
//! report；`StrategySupervisor` / `StrategyRetryPolicy` / `StrategyShutdownSignal`
//! 已覆盖 task-layer supervisor、typed health/metrics snapshot、typed telemetry/export
//! hook、有限 retry 和 ctrl-c shutdown hook foundation。正式 example 已更新到
//! `crates/tqsdk-task/examples/api_contract_s15_live_sim_replay_switch.rs`。
//!
//! Remaining API gap:
//! 当前仍是 deployment/supervisor foundation：配置文件反序列化、完整 reconnect
//! orchestration 和多 provider environment 尚未冻结。Rust SDK 不规划 GUI 或内置 HTTP
//! health/metrics endpoint 作为 S15/S20 完成标准。
//!
//! Boundary decision:
//! 官方 `tqsdk-python` 支持实盘、模拟、回测和复盘共享策略心智，但没有把部署平台、
//! 多 provider environment 或生产运维框架作为核心 API。`tqsdk-rust` 核心边界
//! 是让同一策略 context 可在 live / sim / replay / test 间切换；配置文件读取可
//! 作为薄便利能力评估，完整 deployment platform 和多 provider environment 随
//! S14 暂缓。
//!
//! 理想用户代码草案：
//! ```ignore
//! let config = StrategyDeploymentConfig::from_file("strategy.toml")?
//!     .lifecycle(StrategyLifecycle::new().without_step_limit());
//! let deployment = StrategyEnvironment::from_config(config)
//!     .build()
//!     .await?;
//! let mut supervisor = StrategySupervisor::new(deployment)
//!     .shutdown_signal(StrategyShutdownSignal::ctrl_c())
//!     .retry_policy(StrategyRetryPolicy::new().max_retries(3))
//!     .telemetry_reporter(report_telemetry);
//! supervisor.run(MyStrategy::default()).await?;
//! supervisor.shutdown().await?;
//! ```

fn main() {}
