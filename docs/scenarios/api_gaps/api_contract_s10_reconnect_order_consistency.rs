//! Scenario: 断线重连中的订单一致性
//!
//! User goal:
//! - 断线重连时不重复下单
//! - 不漏掉已提交订单的最终状态
//! - 不把旧状态误判成当前订单状态
//!
//! API contract:
//! - 用户能传入稳定 client order id / intent id
//! - SDK 能在重连后按 intent 对齐 command、order、trade
//! - 订单状态等待能区分 rejected/failed/cancelled/completed
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 业务代码用本地 bool 记录“是否已经下单”
//! - 字符串判断 runtime command status
//! - provider 内部 reconnect event
//! - 手写订单去重表
//!
//! Regression signal:
//! - 重连后需要用户自己扫订单表和本地 intent 表
//! - 相同策略信号可能提交第二笔订单
//! - command status 和 order lifecycle 无法关联
//!
//! Review questions:
//! - 当前 API 是否能自然表达重连订单一致性？
//! - 是否存在 P0 级重复下单风险？
//! - 需要 API 微调、局部重构还是新增执行一致性层？
//!
//! API gap:
//! runtime command ledger 存在，但 wait/stream/task public API 还没有
//! client intent id、reconnect-safe order ticket、command/order/trade correlation
//! 的终端用户契约。
//!
//! 理想用户代码草案：
//! ```ignore
//! let ticket = api
//!     .limit_order(account.id(), "SHFE.au2602")
//!     .client_intent("strategy-a-open-20260426-001")
//!     .buy_open(1)
//!     .at(480.0)
//!     .send_once()
//!     .await?;
//!
//! let state = ticket.wait_reconnect_safe_terminal(&mut api).await?;
//! println!("{:?}", state);
//! ```

fn main() {}
