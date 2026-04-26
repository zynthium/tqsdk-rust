//! Scenario: 撤单与部分成交
//!
//! User goal:
//! - 观察订单部分成交
//! - 撤掉剩余未成交量
//! - 确认最终订单状态
//!
//! API contract:
//! - public API 暴露 typed order lifecycle 和剩余量
//! - 撤单可直接作用于订单 handle
//! - 最终状态等待不要求用户写重复状态机
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 字符串解析 `status`
//! - `RuntimeCommand::Trade`
//! - 业务代码自行推断 terminal state
//!
//! Regression signal:
//! - 用户必须在循环里组合 `is_dead` / `status` / `volume_left`
//! - 撤单只接受裸 order id，丢失订单归属上下文
//! - 部分成交和撤单终态没有 typed helper
//!
//! Review questions:
//! - 当前 API 是否自然表达部分成交撤单？
//! - 订单生命周期是否类型安全？
//! - 是否存在漏撤或误判终态的资金安全风险？
//!
//! API gap:
//! 当前 `Order` 已有 typed `lifecycle` 和 `volume_left`，但 wait facade
//! 缺少 `order.cancel(&mut api).await?`、`wait_partially_filled()`、
//! `wait_terminal()` 这类用户级 helper。
//!
//! 理想用户代码草案：
//! ```ignore
//! let order = api.limit_order(account.id(), "SHFE.au2602").buy_open(3).at(480.0).send().await?;
//! let partial = order.wait_partially_filled(&mut api).await?;
//! assert!(partial.volume_left > 0);
//! order.cancel_remaining(&mut api).await?;
//! let final_state = order.wait_terminal(&mut api).await?;
//! assert!(final_state.lifecycle.is_terminal());
//! ```

fn main() {}
