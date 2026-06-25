//! Scenario: 实盘 / 模拟 / 回放切换（deployment config + supervisor lifecycle）
//!
//! User goal:
//! - 同一套策略步骤代码在 live trade、provider-backed TQKQ sim、fake sim 和 replay 之间复用
//! - provider 差异只出现在 deployment config / environment construction
//! - 策略逻辑只依赖标准 context 读取和 typed execution API
//! - 运行生命周期、ctrl-c shutdown、retry 和 metrics snapshot 由 SDK wrapper 管理
//!
//! API contract:
//! - public API 提供 `StrategyDeploymentConfig` / `StrategyDeployment`
//! - provider-backed sim 使用 typed config，不暴露 TQKQ 内部账号派生协议
//! - live/sim/replay 都暴露同一套 quote/position/orders context 方法
//! - replay 不要求策略改写成底层 replay command loop
//! - supervisor 提供 typed stop reason、retry policy、health/metrics snapshot 和 graceful shutdown report
//!
//! Forbidden:
//! - 策略中写 `if live { ... } else if replay { ... }`
//! - `ReplayCommand` 泄漏到策略主逻辑
//! - provider 内部 session / protocol type
//! - 手写 Tokio task、channel 或 `Arc<Mutex<_>>`
//! - 多套状态读取模型
//!
//! Regression signal:
//! - 策略从 live/sim 迁到 replay 需要改事件循环
//! - provider-backed sim 需要用户手动派生账号或提交登录协议命令
//! - replay 无法复用同一 typed order/risk 接口
//! - lifecycle/shutdown/retry 退化成用户手写后台任务编排
//!
//! Review questions:
//! - 当前 API 是否自然表达 provider-backed sim / live / replay 切换？
//! - 是否保持同一策略 context contract？
//! - supervisor lifecycle 是否避免用户手动管理 Tokio task / channel？

use std::time::Duration;

use tqsdk_core::{Quote, TradeAccountType};
use tqsdk_task::RiskEngine;
use tqsdk_task::deployment::{
    StrategyDeployment, StrategyDeploymentConfig, StrategyLifecycle, StrategyRetryPolicy,
    StrategyShutdownSignal, StrategySupervisor, StrategySupervisorReport,
};
use tqsdk_task::environment::{StrategyEnvironment, StrategyEnvironmentContext};
use tqsdk_task::replay::{ReplayMarketEvent, StrategyReplay, StrategyReplaySpeed};
use tqsdk_task::testing::{FakeBroker, FakeMarket, StrategyTestHarness};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mode = std::env::var("TQ_STRATEGY_ENV").unwrap_or_else(|_| "fake".into());
    let configured_account = std::env::var("TQ_STRATEGY_ACCOUNT").unwrap_or_else(|_| "sim".into());
    let symbol = std::env::var("TQ_STRATEGY_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".into());
    let deployment =
        build_deployment(mode.as_str(), configured_account.as_str(), symbol.as_str()).await?;
    let account_id = deployment
        .account_id()
        .unwrap_or(configured_account.as_str())
        .to_owned();
    let mut supervisor = StrategySupervisor::new(deployment)
        .shutdown_signal(StrategyShutdownSignal::ctrl_c())
        .retry_policy(StrategyRetryPolicy::new().max_retries(1));

    let run_report =
        run_breakout_once(&mut supervisor, account_id.as_str(), symbol.as_str()).await?;
    let health = supervisor.health().clone();
    let shutdown = supervisor.shutdown().await?;

    println!(
        "provider={:?} stop={:?} steps={} retries={} errors={} health={:?} shutdown_kind={:?} graceful={}",
        mode,
        run_report.stop_reason(),
        run_report.metrics().steps(),
        run_report.metrics().retries(),
        run_report.metrics().errors(),
        health.status(),
        shutdown.kind(),
        shutdown.graceful()
    );
    Ok(())
}

async fn run_breakout_once(
    supervisor: &mut StrategySupervisor,
    account_id: &str,
    symbol: &str,
) -> tqsdk_task::Result<StrategySupervisorReport> {
    let account_id = account_id.to_owned();
    let symbol = symbol.to_owned();
    supervisor
        .run(move |ctx| {
            let account_id = account_id.clone();
            let symbol = symbol.clone();
            Box::pin(async move { breakout_step(ctx, &account_id, &symbol).await })
        })
        .await
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

async fn build_deployment(
    mode: &str,
    account_id: &str,
    symbol: &str,
) -> Result<StrategyDeployment, Box<dyn std::error::Error>> {
    match mode {
        "live" => live_deployment(account_id, symbol).await,
        "tqkq-sim" => tqkq_sim_deployment(symbol).await,
        "replay" => replay_deployment(account_id, symbol).await,
        _ => fake_deployment(account_id, symbol).await,
    }
}

async fn live_deployment(
    account_id: &str,
    symbol: &str,
) -> Result<StrategyDeployment, Box<dyn std::error::Error>> {
    let config = StrategyDeploymentConfig::live_trade(
        read_env("TQ_AUTH_USER")?,
        read_env("TQ_AUTH_PASS")?,
        read_env("TQ_BROKER_ID")?,
        account_id.to_owned(),
        read_env("TQ_ACCOUNT_PASSWORD")?,
        TradeAccountType::Future,
    )
    .futures_market()
    .account(account_id)
    .quote(symbol)
    .startup_timeout(Duration::from_secs(30))
    .lifecycle(StrategyLifecycle::new().max_steps(1))
    .risk(example_risk());

    StrategyEnvironment::from_config(config)
        .build()
        .await
        .map_err(Into::into)
}

async fn tqkq_sim_deployment(
    symbol: &str,
) -> Result<StrategyDeployment, Box<dyn std::error::Error>> {
    let mut config =
        StrategyDeploymentConfig::tqkq_sim(read_env("TQ_AUTH_USER")?, read_env("TQ_AUTH_PASS")?)
            .futures_market()
            .quote(symbol)
            .startup_timeout(Duration::from_secs(30))
            .lifecycle(StrategyLifecycle::new().max_steps(1))
            .risk(example_risk());

    if let Some(number) = read_optional_u8_env("TQ_TRADE_ACCOUNT_NO")? {
        config = config.account_number(number);
    }

    StrategyEnvironment::from_config(config)
        .build()
        .await
        .map_err(Into::into)
}

async fn fake_deployment(
    account_id: &str,
    symbol: &str,
) -> Result<StrategyDeployment, Box<dyn std::error::Error>> {
    let harness = StrategyTestHarness::new()
        .market(
            FakeMarket::new()
                .quote(symbol, 481.0)
                .account(account_id, 100_000.0)
                .position(account_id, symbol, 0),
        )
        .broker(FakeBroker::new().fill_all())
        .build()?;
    let environment = StrategyEnvironment::from_test_harness(harness)
        .account(account_id)
        .quote(symbol)
        .build()
        .await?;

    StrategyDeployment::from_environment(environment)
        .account_id(account_id)
        .lifecycle(StrategyLifecycle::new().max_steps(1))
        .build()
        .await
        .map_err(Into::into)
}

async fn replay_deployment(
    account_id: &str,
    symbol: &str,
) -> Result<StrategyDeployment, Box<dyn std::error::Error>> {
    let replay = StrategyReplay::source_builder()
        .event(ReplayMarketEvent::quote(
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
    let environment = StrategyEnvironment::from_replay_builder(replay_builder)
        .account(account_id)
        .quote(symbol)
        .build()
        .await?;

    StrategyDeployment::from_environment(environment)
        .account_id(account_id)
        .lifecycle(StrategyLifecycle::new().max_steps(1))
        .build()
        .await
        .map_err(Into::into)
}

fn example_risk() -> RiskEngine {
    RiskEngine::new()
        .max_order_volume(1)
        .min_available(1_000.0)
        .max_net_position(1)
        .max_price_deviation(50.0)
}

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

fn read_optional_u8_env(key: &str) -> Result<Option<u8>, Box<dyn std::error::Error>> {
    let Some(raw) = std::env::var(key)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    Ok(Some(raw.parse()?))
}
