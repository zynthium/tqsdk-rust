//! Scenario: 风控前置
//!
//! User goal:
//! - 下单前检查资金、持仓、价格、合约、限额
//! - 拒绝不安全订单
//! - 留下可审计的拒绝原因
//!
//! API contract:
//! - 风控规则是 typed public API
//! - 下单入口能强制经过 risk gate
//! - 风控读取账户/持仓/quote 时使用同一稳定截面
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
//! API gap:
//! `TaskHost::insert_order_guarded` 现在只做 task ownership guard，不是资金、
//! 持仓、价格、合约和限额的通用 pre-trade risk engine。
//!
//! 理想用户代码草案：
//! ```ignore
//! let risk = RiskEngine::new()
//!     .max_order_volume(3)
//!     .max_symbol_position("SHFE.au2602", 5)
//!     .price_band_from_quote(5)
//!     .require_available_margin()
//!     .build();
//! let mut host = TaskHost::new(api).with_risk(risk);
//! host.orders().buy_open("SHFE.au2602", 1).limit(480.0).send().await?;
//! ```

fn main() {}
