//! Scenario: wait live/backtest same strategy body
//!
//! User goal:
//! - 同一段 wait 策略主体可用于实盘和回测
//! - 策略主体只依赖 `quote` / `kline` handles 和 `step()` 推进
//! - live/backtest 差异收敛在 builder 配置，而不是散落在策略逻辑里
//!
//! API contract:
//! - live builder 与 backtest builder 是独立函数
//! - shared strategy body 不分支判断 live/backtest mode
//! - `step()` 在 live 模式持续推进，在 backtest 结束后返回 `None`
//! - handles 自带读取上下文，不需要 `&TqApi` 参数

use std::time::Duration;

use tqsdk_wait::{TqApi, TqApiBuilder};

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

fn read_env_i64(key: &str, default: i64) -> i64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

async fn live_api(user: String, pass: String) -> Result<TqApi, Box<dyn std::error::Error>> {
    Ok(TqApiBuilder::new(user, pass)
        .futures_market()
        .build()
        .await?)
}

async fn backtest_api(user: String, pass: String) -> Result<TqApi, Box<dyn std::error::Error>> {
    let start_datetime_ns = read_env_i64("TQ_BACKTEST_START_NS", 1_713_660_000_000_000_000);
    let end_datetime_ns = read_env_i64("TQ_BACKTEST_END_NS", 1_713_746_400_000_000_000);

    Ok(TqApiBuilder::new(user, pass)
        .futures_backtest(start_datetime_ns, end_datetime_ns)?
        .build()
        .await?)
}

async fn run_strategy(mut api: TqApi, symbol: &str) -> Result<(), Box<dyn std::error::Error>> {
    let quote = api.quote(symbol).await?;
    let bars = api.kline(symbol, Duration::from_secs(60), 32).await?;

    while let Some(step) = api.step().await? {
        if step.is_changing(&quote) {
            let snapshot = quote.load()?;
            println!(
                "quote symbol={} datetime={} last_price={}",
                snapshot.instrument_id, snapshot.datetime, snapshot.last_price
            );
        }

        if step.is_changing(&bars) {
            let window = bars.window()?;
            println!(
                "bars symbol={} len={} completed={}",
                window.symbol(),
                window.len(),
                window.completed_rows().len()
            );
        }
    }

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_TEST_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".to_string());
    let mode = std::env::var("TQ_WAIT_MODE").unwrap_or_else(|_| "live".to_string());

    let api = if mode == "backtest" {
        backtest_api(user, pass).await?
    } else {
        live_api(user, pass).await?
    };

    run_strategy(api, &symbol).await
}
