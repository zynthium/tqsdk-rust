//! Scenario: 实盘 / 模拟 / 回放切换（environment foundation）
//!
//! User goal:
//! - 同一套策略步骤代码在 live task host、simulated fake broker 和 replay 之间复用
//! - provider 差异只出现在 environment 构建配置
//! - 策略逻辑只依赖标准 context 读取和 typed execution API
//!
//! API contract:
//! - public API 提供 `StrategyEnvironment` / `StrategyEnvironmentContext`
//! - task-host live/sim 和 replay 都暴露同一套 quote/position/orders context 方法
//! - replay 不要求策略改写成底层 replay command loop
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 策略中写 `if live { ... } else if replay { ... }`
//! - `ReplayCommand` 泄漏到策略主逻辑
//! - provider 内部 session / protocol type
//! - 多套状态读取模型
//!
//! Regression signal:
//! - 策略从 live/sim 迁到 replay 需要改事件循环
//! - replay 无法复用同一 typed order/risk 接口
//! - fake broker 和 replay 不能复用同一策略步骤函数
//!
//! Review questions:
//! - 当前 API 是否自然表达最小运行环境切换？
//! - 是否保持同一策略 context contract？
//! - 完整 provider-backed sim / deployment config 是否仍应作为后续 gap？

use std::time::Duration;

use tqsdk_core::{Quote, TradeAccountType};
use tqsdk_data::MarketCacheEvent;
use tqsdk_task::testing::{FakeBroker, FakeMarket, StrategyTestHarness};
use tqsdk_task::{
    RiskEngine, StrategyEnvironment, StrategyEnvironmentContext, StrategyReplay,
    StrategyReplaySpeed, TaskHost,
};
use tqsdk_wait::TqApiBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mode = std::env::var("TQ_STRATEGY_ENV").unwrap_or_else(|_| "sim".into());
    let account_id = std::env::var("TQ_STRATEGY_ACCOUNT").unwrap_or_else(|_| "sim".into());
    let symbol = std::env::var("TQ_STRATEGY_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".into());
    let mut environment =
        build_environment(mode.as_str(), account_id.as_str(), symbol.as_str()).await?;

    run_breakout_once(&mut environment, account_id.as_str(), symbol.as_str()).await?;
    Ok(())
}

async fn run_breakout_once(
    environment: &mut StrategyEnvironment,
    account_id: &str,
    symbol: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(mut ctx) = environment.next().await? else {
        return Ok(());
    };
    breakout_step(&mut ctx, account_id, symbol).await?;

    if let Some(event) = ctx.replay_event() {
        println!(
            "replay source={} symbol={} replay_time_ns={}",
            event.source(),
            event.symbol(),
            ctx.replay_time_ns().unwrap_or_default()
        );
    }
    Ok(())
}

async fn breakout_step(
    ctx: &mut StrategyEnvironmentContext<'_>,
    account_id: &str,
    symbol: &str,
) -> tqsdk_task::Result<()> {
    let quote = ctx.quote(symbol)?;
    let position = ctx.position(account_id, symbol)?;
    if quote.last_price > 480.0 && position.pos_long == 0 {
        ctx.orders(account_id)
            .buy_open(symbol, 1)
            .limit(quote.last_price)
            .send_once("env-breakout-entry")
            .await?;
    }
    Ok(())
}

async fn build_environment(
    mode: &str,
    account_id: &str,
    symbol: &str,
) -> Result<StrategyEnvironment, Box<dyn std::error::Error>> {
    match mode {
        "live" => live_environment(account_id, symbol).await,
        "replay" => replay_environment(account_id, symbol).await,
        _ => simulated_environment(account_id, symbol).await,
    }
}

async fn live_environment(
    account_id: &str,
    symbol: &str,
) -> Result<StrategyEnvironment, Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let broker_id = std::env::var("TQ_BROKER_ID")?;
    let account_password = std::env::var("TQ_ACCOUNT_PASSWORD")?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    let mut api = TqApiBuilder::new(user, pass)
        .futures_market()
        .trade_target(broker_id.clone(), account_id.to_owned())
        .build()
        .await?;
    api.login_trade_account(
        broker_id.as_str(),
        account_id,
        account_password.as_str(),
        TradeAccountType::Future,
        Some(deadline),
    )
    .await?;
    api.quote_snapshot(symbol, Some(deadline)).await?;

    let risk = RiskEngine::new()
        .max_order_volume(1)
        .min_available(1_000.0)
        .max_net_position(1)
        .max_price_deviation(50.0);
    StrategyEnvironment::from_task_host(TaskHost::new(api).with_risk(risk))
        .account(account_id)
        .quote(symbol)
        .build()
        .await
        .map_err(Into::into)
}

async fn simulated_environment(
    account_id: &str,
    symbol: &str,
) -> Result<StrategyEnvironment, Box<dyn std::error::Error>> {
    let harness = StrategyTestHarness::new()
        .market(
            FakeMarket::new()
                .quote(symbol, 481.0)
                .account(account_id, 100_000.0)
                .position(account_id, symbol, 0),
        )
        .broker(FakeBroker::new().fill_all())
        .build()?;

    StrategyEnvironment::from_test_harness(harness)
        .account(account_id)
        .quote(symbol)
        .build()
        .await
        .map_err(Into::into)
}

async fn replay_environment(
    account_id: &str,
    symbol: &str,
) -> Result<StrategyEnvironment, Box<dyn std::error::Error>> {
    let replay = StrategyReplay::source_builder()
        .event(MarketCacheEvent::quote(
            "inline-cache",
            symbol,
            1_000,
            Some(900),
            Quote {
                last_price: 481.0,
                ..Quote::default()
            },
        )?)
        .build();
    let replay_builder = StrategyReplay::builder(replay)
        .market(
            FakeMarket::new()
                .account(account_id, 100_000.0)
                .position(account_id, symbol, 0),
        )
        .broker(FakeBroker::new().fill_all())
        .speed(StrategyReplaySpeed::FASTEST);

    StrategyEnvironment::from_replay_builder(replay_builder)
        .account(account_id)
        .quote(symbol)
        .build()
        .await
        .map_err(Into::into)
}
