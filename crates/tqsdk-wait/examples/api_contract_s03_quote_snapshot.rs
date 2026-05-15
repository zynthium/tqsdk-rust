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

use std::time::Duration;

use tqsdk_wait::{QuoteRef, TqApi, TqApiBuilder};

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_TEST_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".to_string());

    let mut api = TqApiBuilder::new(user, pass)
        .futures_market()
        .build()
        .await?;
    let quote = api.quote(&symbol).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let quote = wait_quote_ready(&mut api, &quote, deadline).await?;

    println!(
        "symbol={} datetime={} last_price={}",
        quote.instrument_id, quote.datetime, quote.last_price
    );

    Ok(())
}

async fn wait_quote_ready(
    api: &mut TqApi,
    quote: &QuoteRef,
    deadline: tokio::time::Instant,
) -> Result<tqsdk_core::Quote, Box<dyn std::error::Error>> {
    while let Some(step) = api.step_until(Some(deadline)).await? {
        if step.is_changing(quote)
            && let Ok(snapshot) = quote.load()
            && !snapshot.datetime.is_empty()
        {
            return Ok(snapshot);
        }
    }

    Err("quote snapshot not ready".into())
}
