//! Scenario: 高频裸行情直通
//!
//! User goal:
//! - 绕过厚 facade、缓存、研究层
//! - 低延迟消费 tick / quote
//! - 保持 revision / commit 一致性
//!
//! API contract:
//! - 使用 `tqsdk-session + RuntimeReader` 作为低层 public substrate
//! - 只消费 runtime commit，不建立第二棵状态树
//! - hot read 优先走 market partition read surface
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - provider 私有模块
//! - facade 私有 cache
//! - research/data layer
//! - full snapshot hot-loop decode
//!
//! Regression signal:
//! - 低层用户必须进入 `tqsdk-wait` 或 `tqsdk-data`
//! - quote hot path 只能 `reader.read()` 全量 state tree
//! - 多消费者需要自建共享状态
//!
//! Review questions:
//! - 当前 API 是否允许低延迟裸消费？
//! - 是否仍遵守单一 commit/revision？
//! - 热路径是否避开全量 snapshot？

use std::time::Duration;

use tokio::time::Instant;
use tqsdk_core::Symbol;
use tqsdk_session::SessionClientBuilder;

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_TEST_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".to_string());
    let symbol_key = Symbol::new(symbol.clone());

    let session = SessionClientBuilder::new(user, pass)
        .futures_market()
        .build()?;
    session.subscribe_quotes([symbol.as_str()]).await?;

    let reader = session.reader().clone();
    let mut cursor = reader.cursor();
    let deadline = Instant::now() + Duration::from_secs(30);

    loop {
        while let Some(commit) = reader.next(&mut cursor) {
            if let Some(quote) = reader.read_market_state().quote(&symbol_key)?
                && !quote.datetime.is_empty()
            {
                println!(
                    "revision={:?} symbol={} datetime={} last_price={}",
                    commit.revision, symbol, quote.datetime, quote.last_price
                );
                return Ok(());
            }
        }

        let now = Instant::now();
        if now >= deadline {
            return Err("timed out waiting for quote".into());
        }
        session
            .progress_once(Some(now + Duration::from_millis(250)))
            .await?;
    }
}
