//! Scenario: 简单策略
//!
//! User goal:
//! - 行情触发下单
//! - 成交后更新持仓
//! - 触发止盈/止损后平仓
//!
//! API contract:
//! - 策略循环能在一个稳定状态截面内读取 quote/order/position
//! - 下单和平仓通过 typed execution API 表达
//! - 成交回报和持仓更新由 SDK 对齐，不要求用户维护第二份状态
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 手写本地 position cache 作为真实资金依据
//! - 字符串解析订单状态
//! - `RuntimeCommand::Trade`
//! - provider 内部 session / protocol type
//!
//! Regression signal:
//! - 策略必须在 quote stream 和 trade stream 之间自行同步状态
//! - 止盈止损可能基于过期持仓
//! - 用户必须手动防止重复下单
//!
//! Review questions:
//! - 当前 API 是否自然表达最小策略？
//! - 状态截面是否一致？
//! - 资金安全风险应由 task 层还是 strategy facade 处理？
//!
//! Current API note:
//! `TaskHost + TargetPosTask` 可以覆盖“目标持仓”类简单策略。
//! signal-driven order ticket 现在可以通过 `TaskHost::orders(...).buy_open(...).limit(...).send_once(...)`
//! 表达，并可配置 `RiskEngine` 做前置风控。
//!
//! 仍未落地的是完整 `StrategyHost`：统一行情触发、订单 terminal wait、
//! 成交后持仓确认、止盈止损平仓和策略级 cursor 的单一上下文。
//!
//! 理想用户代码草案：
//! ```ignore
//! let mut strategy = StrategyHost::new(api)
//!     .quote("SHFE.au2602")
//!     .account(account.id())
//!     .build()
//!     .await?;
//!
//! while let Some(ctx) = strategy.next().await? {
//!     if ctx.quote("SHFE.au2602").last_price > entry && !ctx.position("SHFE.au2602").is_long() {
//!         ctx.orders().buy_open("SHFE.au2602", 1).limit(entry).send_once("entry-1").await?;
//!     }
//!     ctx.risk().close_on_take_profit_or_stop_loss("SHFE.au2602", take_profit, stop_loss).await?;
//! }
//! ```

fn main() {}
