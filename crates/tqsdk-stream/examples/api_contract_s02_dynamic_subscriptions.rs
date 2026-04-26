//! Scenario: 多合约动态订阅
//!
//! User goal:
//! - 运行中增加订阅
//! - 运行中取消订阅
//! - 断线重连后自动恢复当前订阅集合
//!
//! API contract:
//! - 动态订阅是用户级 API，而不是要求用户提交底层 `RuntimeCommand`
//! - subscription handle 能表达 add/remove/current symbols
//! - reconnect/resync 后由 SDK 恢复订阅意图
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - `MarketCommand::SubscribeQuotes`
//! - `MarketCommand::UnsubscribeQuotes`
//! - provider 内部 session / protocol type
//! - 手写 Tokio 后台任务编排
//!
//! Regression signal:
//! - 用户需要维护两份订阅集合
//! - 取消订阅只能靠底层 protocol command
//! - 重连后需要业务代码重新提交所有订阅
//!
//! Review questions:
//! - 当前 API 是否自然表达动态订阅？
//! - 订阅意图是否能跨重连保持？
//! - 取消订阅是否有类型安全的 public API？
//!
//! API gap:
//! 当前 `tqsdk-stream` 有 `quote_stream()`，但 quote 订阅仍需要用户通过
//! `stream.session().submit(RuntimeCommand::Market(...))` 手动提交协议命令；
//! 没有 subscription handle，也没有 public restore contract。
//!
//! 理想用户代码草案：
//! ```ignore
//! let stream = TqStreamBuilder::new(user, pass).futures_market().build().await?;
//! let mut quotes = stream.quotes(["SHFE.au2602"]).await?;
//!
//! quotes.add("SHFE.ag2602").await?;
//! quotes.remove("SHFE.au2602").await?;
//!
//! while let Some(update) = quotes.next().await.transpose()? {
//!     println!("{} {}", update.symbol, update.quote.last_price);
//! }
//! ```

fn main() {}
