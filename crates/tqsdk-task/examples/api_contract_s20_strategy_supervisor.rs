//! Scenario: 生产守护进程（strategy supervisor foundation）
//!
//! User goal:
//! - 用 SDK wrapper 运行策略 deployment
//! - 读取 health / metrics snapshot
//! - 导出 typed telemetry event 到用户自己的日志/指标系统
//! - 配置 ctrl-c graceful shutdown 和有限 retry
//! - 结束后执行 typed shutdown
//!
//! API contract:
//! - 只使用面向终端用户的 public API
//! - supervisor 暴露 typed stop reason、health status 和 metrics
//! - telemetry reporter 使用 typed event，不要求 SDK 内置 HTTP endpoint
//! - retry policy 默认不隐藏启用，必须由用户显式配置
//! - ctrl-c shutdown 不要求用户手写 Tokio task / channel
//! - shutdown 返回 typed report
//!
//! Forbidden:
//! - provider 私有 driver handle
//! - 用户自行追踪心跳和 route phase
//! - 通过 drop 隐式完成 async shutdown
//! - 手写 Tokio 后台任务编排
//! - 手动使用 `Arc<Mutex<_>>`
//!
//! Regression signal:
//! - 生产部署只能从日志字符串判断健康状态
//! - ctrl-c / retry / metrics 退化成用户手写后台任务
//! - shutdown 可能丢命令或悬挂订阅
//!
//! Review questions:
//! - 当前 API 是否自然表达最小 strategy supervisor？
//! - 错误/健康状态是否类型安全？
//! - 哪些完整 daemon 能力仍是 API gap？
//!
//! Current API note:
//! 本示例验证 task-layer supervisor foundation 和稳定 telemetry/export hook。
//! 持久化 sink isolation、完整 reconnect orchestration 和跨进程 daemon 管理
//! 仍属于 `docs/scenarios/api_gaps/` 中的生产 daemon gap；Rust SDK 不规划 GUI
//! 或内置 HTTP health/metrics endpoint 作为 S20 完成标准。

use tqsdk_core::Quote;
use tqsdk_data::MarketCacheEvent;
use tqsdk_task::testing::{FakeBroker, FakeMarket};
use tqsdk_task::{
    StrategyDeployment, StrategyEnvironment, StrategyEnvironmentContext, StrategyLifecycle,
    StrategyReplay, StrategyRetryPolicy, StrategyShutdownSignal, StrategySupervisor,
    StrategyTelemetryEvent,
};

const ACCOUNT_ID: &str = "sim";
const SYMBOL: &str = "SHFE.au2602";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let deployment = replay_deployment().await?;
    let mut supervisor = StrategySupervisor::new(deployment)
        .shutdown_signal(StrategyShutdownSignal::ctrl_c())
        .retry_policy(StrategyRetryPolicy::new().max_retries(1))
        .telemetry_reporter(print_telemetry);

    let report = supervisor
        .run(|ctx| Box::pin(async move { print_quote_step(ctx).await }))
        .await?;
    let health = supervisor.health().clone();
    let shutdown = supervisor.shutdown().await?;

    println!(
        "stop={:?} status={:?} provider={:?} kind={:?} steps={} retries={} errors={} graceful={}",
        report.stop_reason(),
        health.status(),
        health.provider(),
        health.kind(),
        report.metrics().steps(),
        report.metrics().retries(),
        report.metrics().errors(),
        shutdown.graceful()
    );

    Ok(())
}

fn print_telemetry(event: StrategyTelemetryEvent) {
    println!(
        "telemetry kind={:?} status={:?} provider={:?} kind={:?} steps={} retries={} errors={} stop={:?}",
        event.kind(),
        event.health().status(),
        event.health().provider(),
        event.health().kind(),
        event.metrics().steps(),
        event.metrics().retries(),
        event.metrics().errors(),
        event.stop_reason()
    );
}

async fn print_quote_step(ctx: &mut StrategyEnvironmentContext<'_>) -> tqsdk_task::Result<()> {
    let quote = ctx.quote(SYMBOL)?;
    println!(
        "symbol={} last_price={} replay_time_ns={}",
        SYMBOL,
        quote.last_price,
        ctx.replay_time_ns().unwrap_or_default()
    );
    Ok(())
}

async fn replay_deployment() -> Result<StrategyDeployment, Box<dyn std::error::Error>> {
    let replay = StrategyReplay::source_builder()
        .event(MarketCacheEvent::quote(
            "inline-cache",
            SYMBOL,
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
                .account(ACCOUNT_ID, 100_000.0)
                .position(ACCOUNT_ID, SYMBOL, 0),
        )
        .broker(FakeBroker::new().fill_all());
    let environment = StrategyEnvironment::from_replay_builder(replay_builder)
        .account(ACCOUNT_ID)
        .quote(SYMBOL)
        .build()
        .await?;

    StrategyDeployment::from_environment(environment)
        .account_id(ACCOUNT_ID)
        .lifecycle(StrategyLifecycle::new().max_steps(1))
        .build()
        .await
        .map_err(Into::into)
}
