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
//! Current API note:
//! `QuoteSubscription` 已能表达 add/remove/current symbols；重连恢复依赖
//! runtime/session 保持的底层订阅意图，后续还需要专门的 reconnect contract test。

use futures::StreamExt;
use tqsdk_stream::TqStreamBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let stream = TqStreamBuilder::new(user, pass)
        .futures_market()
        .build()
        .await?;
    let mut quotes = stream.quotes(["SHFE.au2602"]).await?;

    quotes.add("SHFE.ag2606").await?;
    quotes.remove("SHFE.au2602").await?;

    while let Some(update) = quotes.next().await.transpose()? {
        println!(
            "{} {} revision={}",
            update.value.instrument_id,
            update.value.last_price,
            update.commit.revision.get()
        );
    }

    Ok(())
}
