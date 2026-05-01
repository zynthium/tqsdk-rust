//! Archived gap sketch: 2026-05-02
//!
//! This scenario is now naturally expressible by the formal compiled example
//! `crates/tqsdk-task/examples/api_contract_s12_spread_arbitrage.rs`.
//! The remaining automatic hedge / flatten / replenish / persistent audit
//! scope was explicitly downgraded to user-side execution tooling, so this
//! sketch is kept only as historical context.
//!
//! Scenario: 跨合约套利
//!
//! User goal:
//! - 两腿同时或有序下单
//! - 处理成交不同步
//! - 撤单 / 补单 / 对冲剩余敞口
//!
//! API contract:
//! - 两腿 order intent 有同一个 typed execution group
//! - 部分成交、单腿失败、撤补和对冲由 execution layer 显式表达
//! - 用户能读取 group-level 风险和最终 outcome
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 两腿分别用普通 order ref 手动拼事务语义
//! - 本地 bool/Vec 追踪腿状态作为资金安全依据
//! - 字符串判断订单状态
//! - `RuntimeCommand::Trade`
//!
//! Regression signal:
//! - 单腿成交后另一腿失败只能靠业务代码临时补救
//! - 无法表达自动 hedge / flatten 或超时撤补规则
//! - group outcome 无法审计
//!
//! Review questions:
//! - 当前 API 是否能安全表达跨合约套利？
//! - 是否存在 P0 级单腿裸露风险？
//! - 应通过局部 task 扩展还是新增 execution group abstraction？
//!
//! Remaining API gap:
//! `tqsdk-task` 已提供最小 `ExecutionGroup` foundation：typed group id、
//! all-leg preflight、idempotent leg order intents、group outcome、observed
//! `max_unhedged` exposure timeout、exposure report 和 revision-bound
//! `ExecutionGroupReport`。
//!
//! 本文件保留的是更高阶执行缺口：
//! - 自动 hedge / flatten filled legs；
//! - timed cancel / replace；
//! - 最大裸露量驱动的自动撤补；
//! - 多账户或多腿组合的联合风控；
//! - 人工介入后的 group resume / persistent audit log。
//!
//! Boundary decision:
//! 官方 `tqsdk-python` 提供 `TargetPosTask` / `InsertOrderTask` 这类基础执行任务，
//! 但没有把跨合约自动对冲、自动补单或持久审计作为核心 API。`tqsdk-rust`
//! 当前核心边界止于 typed execution group、状态/裸露 report 和用户可审计的
//! outcome；自动 hedge / flatten / 补单引擎应由用户策略或上层执行系统实现。
//!
//! 理想用户代码草案：
//! ```ignore
//! let group = host
//!     .execution_group(account.id())
//!     .client_group_id("spread-entry-001")
//!     .max_unhedged(Duration::from_secs(2))
//!     .on_leg_failed(HedgePolicy::ReportExposure)
//!     .leg("SHFE.au2602").buy_open(1).limit(480.0)
//!     .leg("SHFE.ag2602").sell_open(15).limit(6500.0)
//!     .send_once()
//!     .await?;
//! let report = group.report(host.api())?;
//! println!(
//!     "group revision={} status={:?}",
//!     report.revision().get(),
//!     report.status()
//! );
//! let outcome = group.wait_finished(&mut host, deadline).await?;
//! println!("{:?}", outcome);
//! ```

fn main() {}
