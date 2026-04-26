//! Scenario: 行情快照读取
//!
//! User goal:
//! - 获取某合约当前 quote
//! - 不编写持续监听循环
//! - 用 typed `Quote` 结果继续业务逻辑
//!
//! API contract:
//! - 一次调用返回 typed quote snapshot
//! - SDK 内部处理订阅、等待 ready、超时和清理
//! - 不要求用户理解 chart / diff / commit path
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - `serde_json::Value`
//! - `RuntimeCommand`
//! - `StatePath`
//! - 手写 `wait_update()` 循环只为取一次快照
//!
//! Regression signal:
//! - 用户必须先创建 live ref 再自己循环等待
//! - 用户必须手动判断 quote 是否 ready
//! - 快照读取需要访问底层 state tree
//!
//! Review questions:
//! - 当前 API 是否自然表达一次性 quote snapshot？
//! - 是否暴露内部提交模型？
//! - 是否存在订阅泄漏或快照不一致风险？
//!
//! API gap:
//! 当前可以用 `get_quote().await?` 加 `wait_update()` 循环再 `quote.load(&api)?`
//! 拼出结果，但这不是“只读当前快照”的自然 API。
//!
//! 理想用户代码草案：
//! ```ignore
//! let mut api = TqApiBuilder::new(user, pass).futures_market().build().await?;
//! let quote = api.quote_snapshot("SHFE.au2602").await?;
//! println!("{} {}", quote.datetime, quote.last_price);
//! ```

fn main() {}
