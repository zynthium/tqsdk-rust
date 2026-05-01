//! Archived gap sketch: 2026-05-02
//!
//! This scenario is now naturally expressible by the formal compiled example
//! `crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs`.
//! The remaining portfolio margin / global risk service / durable audit scope
//! was explicitly downgraded to user-side risk tooling, so this sketch is kept
//! only as historical context.
//!
//! Scenario: 风控前置
//!
//! User goal:
//! - 下单前检查资金、持仓、价格、合约、限额和订单频率
//! - 拒绝不安全订单
//! - 留下可审计的拒绝原因
//!
//! API contract:
//! - 风控规则是 typed public API
//! - 下单入口能强制经过 risk gate
//! - 风控读取账户/持仓/quote 时使用同一 revision-bound snapshot
//! - 风控检查能返回 typed `RiskCheckReport` 供审计
//! - 风控投影能返回 typed `RiskProjectionReport` 供下单前估算
//! - 合约 tick size / multiplier 可通过 `InstrumentSpec` 接入风控
//! - 官方同类基础开仓次数、开仓手数、合约组累计开仓手数和订单频率规则可由
//!   `TaskHost` 本进程内计数表达
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 用户在策略里散写 if 判断作为唯一风控
//! - `serde_json::Value` 表达订单价格
//! - 字符串判断合约/交易所规则
//! - 旁路下单绕过 guard
//!
//! Regression signal:
//! - 下单前资金/持仓读取不是同一 revision
//! - 规则拒绝原因不可审计
//! - guarded 和 unguarded order API 容易混用
//!
//! Review questions:
//! - 当前 API 是否自然表达前置风控？
//! - 是否存在资金安全风险？
//! - 应通过 task 层局部扩展还是独立 risk facade？
//!
//! Remaining API gap:
//! `tqsdk-task` 已提供最小 `RiskEngine`、typed rejection reason 和
//! `RiskCheckReport`；`RiskEngine::project_order(...)` 已提供 revision-bound
//! `RiskProjectionReport`，用于单笔订单当前净持仓、投影净持仓和轻量
//! price-volume estimate；`RiskEngine::instrument_specs(...)` 已提供
//! `InstrumentSpec` backed tick-size 校验和 contract multiplier notional
//! projection；`daily_open_count_limit(...)`、`daily_open_volume_limit(...)`、
//! `accumulated_open_volume_limit(...)` 和 `order_rate_limit_per_second(...)`
//! 已对齐官方 Python SDK 的基础风控规则形态；`TaskHost::orders(...).limit(...).send_once(...)`
//! 和 guarded insert/cancel 也会经过 task-layer risk gate。
//!
//! 本文件保留的是更高阶风控缺口：
//! - 组合级资金/保证金 what-if simulation；
//! - 多账户 / 多腿订单组的联合限额；
//! - 涨跌停、品种级限额和更完整交易所规则；
//! - 策略级风控策略热更新、跨进程持久用量恢复与审计日志落库。
//!
//! Boundary decision:
//! 官方 `tqsdk-python` 的本地风控核心规则是开仓次数、开仓手数、合约组累计开仓
//! 手数和订单操作频率。`tqsdk-rust` 当前已对齐这一基础边界；组合保证金引擎、
//! 全局风控服务、跨进程持久用量恢复、热更新和 durable audit 不进入核心 SDK，
//! 应由用户风控系统或上层工具实现。
//!
//! 理想用户代码草案：
//! ```ignore
//! let risk = RiskEngine::new()
//!     .max_order_volume(3)
//!     .daily_open_count_limit(10, ["SHFE.au2602"])
//!     .daily_open_volume_limit(30, ["SHFE.au2602"])
//!     .accumulated_open_volume_limit(50, ["SHFE.au2602", "SHFE.ag2602"])
//!     .order_rate_limit_per_second(20, ["SHFE"])
//!     .min_available(1000.0)
//!     .max_net_position(5)
//!     .max_price_deviation(20.0)
//!     .instrument_specs(instrument_specs)
//!     .with_portfolio_margin_simulation(margin_model)
//!     .with_contract_rules(contract_catalog);
//! let mut host = TaskHost::new(api).with_risk(risk);
//! let projection = host.risk().unwrap().project_order(host.api(), &intent)?;
//! println!(
//!     "risk projection revision={} projected_net={:?}",
//!     projection.revision().get(),
//!     projection.projected_net()
//! );
//! let report = host.risk().unwrap().check_report(host.api(), &intent)?;
//! println!("risk revision={} decision={:?}", report.revision().get(), report.decision());
//! host.orders("sim").buy_open("SHFE.au2602", 1).limit(480.0).send_once("entry-1").await?;
//! ```

fn main() {}
