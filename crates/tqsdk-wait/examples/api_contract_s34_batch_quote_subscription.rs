//! Scenario: 批量实时行情订阅
//!
//! User goal:
//! - 一次订阅一批合约的实时 quote
//! - 用 wait_update/step 循环只处理本轮变化的合约
//! - 不手动提交 RuntimeCommand，也不逐个 quote 造成重复订阅帧
//!
//! API contract:
//! - `TqApi::quotes([...]).await` 返回 symbol-indexed `QuoteSet`
//! - `QuoteSet::changed_snapshots(&step)` 只读取本轮变化的已订阅 quote
//! - 底层订阅意图由 shared session 统一去重和恢复

use std::time::Duration;

use tqsdk_wait::{QuoteSet, TqApi, TqApiBuilder};

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbols = std::env::var("TQ_TEST_SYMBOLS")
        .unwrap_or_else(|_| "SHFE.au2602,DCE.m2609".to_string())
        .split(',')
        .map(str::trim)
        .filter(|symbol| !symbol.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();

    let mut api = TqApiBuilder::new(user, pass)
        .futures_market()
        .build()
        .await?;
    let quotes = api.quotes(symbols.iter().map(String::as_str)).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    print_first_batch(&mut api, &quotes, deadline).await?;
    Ok(())
}

async fn print_first_batch(
    api: &mut TqApi,
    quotes: &QuoteSet,
    deadline: tokio::time::Instant,
) -> Result<(), Box<dyn std::error::Error>> {
    while let Some(step) = api.step_until(Some(deadline)).await? {
        let mut printed = false;
        for snapshot in quotes.changed_snapshots(&step)? {
            if !snapshot.datetime.is_empty() {
                println!(
                    "symbol={} datetime={} last_price={}",
                    snapshot.instrument_id, snapshot.datetime, snapshot.last_price
                );
                printed = true;
            }
        }
        if printed {
            return Ok(());
        }
    }

    Err("no quote update before deadline".into())
}
