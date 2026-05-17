//! Scenario: 高性能批量实时 quote stream
//!
//! User goal:
//! - 一次订阅几十个合约的实时 quote
//! - 每个 runtime commit 只处理本轮变化的合约
//! - 不把每个 quote 变化拆成独立 stream item
//!
//! API contract:
//! - `TqStream::quote_batches([...]).await` 返回动态 batch subscription
//! - 每个 yielded `QuoteBatch` 对应一个 commit
//! - batch 内只包含 subscribed 且本轮 changed 的 quote
//! - `quotes([...]).await` 仍保留为逐 quote item 兼容入口

use futures::StreamExt;
use tqsdk_stream::TqStreamBuilder;

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbols = std::env::var("TQ_TEST_SYMBOLS")
        .unwrap_or_else(|_| "SHFE.au2602,DCE.m2609,CZCE.PF607".to_string())
        .split(',')
        .map(str::trim)
        .filter(|symbol| !symbol.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();

    let stream = TqStreamBuilder::new(user, pass)
        .futures_market()
        .build()
        .await?;
    let mut batches = stream
        .quote_batches(symbols.iter().map(String::as_str))
        .await?;

    while let Some(batch) = batches.next().await.transpose()? {
        for update in batch.quotes {
            println!(
                "revision={} symbol={} datetime={} last_price={}",
                batch.commit.revision.get(),
                update.symbol,
                update.value.datetime,
                update.value.last_price
            );
        }
    }

    Ok(())
}
