//! Scenario: 本地行情缓存读写（live stream pipe foundation）
//!
//! User goal:
//! - 实盘进程将标准 market events 写入本地 cache
//! - cache payload 继续使用 SDK 标准 Quote/Kline/Tick
//! - 写缓存不要求策略用户手写 channel 或 provider protocol
//!
//! API contract:
//! - live stream 到 cache writer 的 bridge 是明确 public API
//! - 只承诺单进程 pipe foundation，不伪装成 durable daemon queue
//! - cache writer/reader 仍属于 `tqsdk-data`
//! - stream consumption 仍属于 `tqsdk-stream`
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 用户自己定义缓存文件格式
//! - 业务代码直接写 state tree dump
//! - provider 私有 protocol type
//! - cache 读写污染 `tqsdk-core` / `tqsdk-session`
//!
//! Regression signal:
//! - live cache pipe 只能靠用户手写 stream loop
//! - cache replay 无法复用标准 market schema
//! - pipe API 暗示已经支持跨进程 durable queue
//!
//! Review questions:
//! - 当前 API 是否自然表达单进程 live cache pipe？
//! - 剩余 durable queue / 跨进程锁 gap 是否被明确排除？
//! - 是否存在热路径性能风险？

use std::time::Duration;

use tqsdk_data::{MarketCacheStreamWriter, MarketCacheWriter};
use tqsdk_stream::TqStreamBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_CACHE_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".into());
    let path = std::env::temp_dir().join("tqsdk-live-cache-example.jsonl");

    let stream = TqStreamBuilder::new(user, pass)
        .futures_market()
        .build()
        .await?;
    let events = stream
        .market_events()
        .quote(symbol.as_str())
        .kline(symbol.as_str(), Duration::from_secs(60), 16)
        .build()
        .await?;

    let writer = MarketCacheWriter::create(&path)?;
    let mut cache = MarketCacheStreamWriter::new("live", writer)?;
    let written = cache.pipe_market_events(events, Some(10)).await?;

    println!("wrote {written} market cache events to {}", path.display());
    Ok(())
}
