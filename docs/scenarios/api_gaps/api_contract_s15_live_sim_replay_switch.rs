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
//! `tqsdk-task::StrategyEnvironment` / `StrategyEnvironmentContext` 已提供最小
//! environment foundation。task-host live/sim、public fake harness 和 replay builder
//! 可收敛到同一套 quote/position/orders/target-pos/risk context 方法；正式
//! example 已提升到
//! `crates/tqsdk-task/examples/api_contract_s15_live_sim_replay_switch.rs`。
//!
//! Remaining API gap:
//! 当前 adapter 仍是 foundation：provider-backed sim、部署级 config loader、
//! 生命周期管理、graceful shutdown、health/retry 组合和多 provider environment
//! 尚未冻结。
//!
//! 理想用户代码草案：
//! ```ignore
//! let mut env = StrategyEnvironment::from_config(config)
//!     .live_task_host(live_host)
//!     .simulated_broker(sim_host)
//!     .replay_builder(replay_builder)
//!     .build()
//!     .await?;
//! run_strategy(&mut env, MyStrategy::default()).await?;
//! ```

fn main() {}
