//! Scenario: 普通限价下单
//!
//! User goal:
//! - 登录交易账户
//! - 提交普通限价单
//! - 等待订单状态变化并打印成交结果
//!
//! API contract:
//! - 下单参数是 typed order request，而不是 `serde_json::Value`
//! - 登录、账户 ready、订单状态等待是用户级 API
//! - 订单状态用 typed lifecycle 表达
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - `serde_json::Value` 作为价格参数
//! - 手写 `TradeLoginCommand`
//! - `RuntimeCommand::Trade`
//! - 字符串判断订单状态
//!
//! Regression signal:
//! - 用户必须手动提交 login command
//! - 价格和 offset 需要靠 loosely typed JSON/string 表达
//! - 等待成交只能写状态轮询模板
//!
//! Review questions:
//! - 当前 API 是否自然表达普通限价单？
//! - 是否暴露交易协议细节？
//! - 是否存在资金安全或重复下单风险？
//!
//! API gap:
//! 当前 `TqApi::insert_limit_order` 已经能用 typed `f64` 价格提交限价单，
//! 但 trade login 仍通常需要经 `session().submit(...)` 手动发送底层命令；
//! 订单 ticket / wait finished helper 也还没有冻结成终端用户 contract。
//!
//! 理想用户代码草案：
//! ```ignore
//! let mut api = TqApiBuilder::new(user, pass)
//!     .futures_market()
//!     .trade_target_tqkq()
//!     .build()
//!     .await?;
//! let account = api.login_default_trade_account().await?;
//!
//! let order = api
//!     .insert_limit_order(
//!         account.id(),
//!         "SHFE.au2602",
//!         TradeDirection::Buy,
//!         Some(TradeOffset::Open),
//!         1,
//!         480.0,
//!     )
//!     .await?;
//!
//! let finished = order.wait_finished(&mut api).await?;
//! println!("{:?}", finished.lifecycle);
//! ```

fn main() {}
