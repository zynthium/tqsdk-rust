//! Scenario: Tick / Quote / K线混合订阅
//!
//! User goal:
//! - 同时订阅 quote、tick window、kline window
//! - 在同一个事件循环处理不同 market data 类型
//! - 不为每种数据手动维护独立任务和共享状态
//!
//! API contract:
//! - public API 提供统一 market event stream
//! - quote/tick/kline 的订阅建立和取消都在 facade 内完成
//! - 每个事件携带 typed payload 和 commit/revision 信息
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - `RuntimeCommand`
//! - `MarketCommand`
//! - `StatePath`
//! - `serde_json::Value`
//!
//! Regression signal:
//! - quote 需要底层 submit，而 kline/tick 使用 facade 方法
//! - 用户必须 `tokio::select!` 多个异构 stream 并自行归一化事件
//! - 用户必须理解 path filter 才能混合消费
//!
//! Review questions:
//! - 当前 API 是否自然表达统一 market event loop？
//! - 是否泄漏底层协议命令？
//! - 是否有热路径多余 decode 或 full snapshot 风险？

use std::time::Duration;

use futures::StreamExt;
use tqsdk_stream::{MarketEvent, TqStreamBuilder};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let stream = TqStreamBuilder::new(user, pass)
        .futures_market()
        .build()
        .await?;

    let mut events = stream
        .market_events()
        .quote("SHFE.au2602")
        .tick("SHFE.au2602", 200)
        .kline("SHFE.au2602", Duration::from_secs(60), 200)
        .build()
        .await?;

    while let Some(event) = events.next().await.transpose()? {
        match event {
            MarketEvent::Quote(update) => {
                println!(
                    "quote {} {}",
                    update.value.instrument_id, update.value.last_price
                );
            }
            MarketEvent::TickWindow(update) => {
                println!(
                    "tick window {} rows={} revision={}",
                    update.value.symbol(),
                    update.value.len(),
                    update.commit.revision.get()
                );
            }
            MarketEvent::KlineWindow(update) => {
                println!(
                    "kline window {} rows={} revision={}",
                    update.value.symbol(),
                    update.value.len(),
                    update.commit.revision.get()
                );
            }
        }
    }

    Ok(())
}
