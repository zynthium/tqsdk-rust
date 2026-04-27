use std::time::Duration;

use tqsdk_core::Quote;
use tqsdk_data::{MarketCacheEvent, MarketCacheReplay};
use tqsdk_task::testing::{FakeBroker, FakeMarket, StrategyTestHarness};
use tqsdk_task::{
    RiskEngine, StrategyDeployment, StrategyDeploymentConfig, StrategyEnvironment,
    StrategyEnvironmentContext, StrategyEnvironmentKind, StrategyEnvironmentProvider,
    StrategyLifecycle, StrategyRunStopReason,
};

const SYMBOL: &str = "SHFE.au2602";

async fn buy_breakout_once(ctx: &mut StrategyEnvironmentContext<'_>) -> tqsdk_task::Result<()> {
    let quote = ctx.quote(SYMBOL)?;
    let position = ctx.position("sim", SYMBOL)?;
    if quote.last_price > 480.0 && position.pos_long == 0 {
        ctx.orders("sim")
            .buy_open(SYMBOL, 1)
            .limit(quote.last_price)
            .send_once("env-breakout-entry")
            .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_environment_runs_same_strategy_step_on_test_harness() {
    let harness = StrategyTestHarness::new()
        .market(
            FakeMarket::new()
                .quote(SYMBOL, 481.0)
                .account("sim", 100_000.0)
                .position("sim", SYMBOL, 0),
        )
        .broker(FakeBroker::new().fill_all())
        .build()
        .unwrap();

    let mut environment = StrategyEnvironment::from_test_harness(harness)
        .account("sim")
        .quote(SYMBOL)
        .build()
        .await
        .unwrap();
    assert_eq!(environment.kind(), StrategyEnvironmentKind::TaskHost);

    let mut ctx = environment.next_once().await.unwrap();
    assert!(ctx.replay_event().is_none());
    buy_breakout_once(&mut ctx).await.unwrap();
    let report = ctx.finish_test_step().await.unwrap();

    assert_eq!(report.orders().len(), 1);
    assert_eq!(report.trades().len(), 1);
    assert_eq!(report.position("sim", SYMBOL).unwrap().pos_long, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_environment_runs_same_strategy_step_on_replay() {
    let replay = MarketCacheReplay::new(vec![
        MarketCacheEvent::quote(
            "cache",
            SYMBOL,
            1_000,
            Some(900),
            Quote {
                last_price: 481.0,
                ..Quote::default()
            },
        )
        .unwrap(),
    ]);

    let replay_builder = tqsdk_task::StrategyReplay::builder(replay)
        .market(
            FakeMarket::new()
                .account("sim", 100_000.0)
                .position("sim", SYMBOL, 0),
        )
        .broker(FakeBroker::new().fill_all());
    let mut environment = StrategyEnvironment::from_replay_builder(replay_builder)
        .account("sim")
        .quote(SYMBOL)
        .build()
        .await
        .unwrap();
    assert_eq!(environment.kind(), StrategyEnvironmentKind::Replay);

    let mut ctx = environment.next_once().await.unwrap();
    assert_eq!(ctx.replay_event().unwrap().source(), "cache");
    buy_breakout_once(&mut ctx).await.unwrap();
    let report = ctx.finish_test_step().await.unwrap();

    assert_eq!(report.orders().len(), 1);
    assert_eq!(report.trades().len(), 1);
    assert_eq!(report.position("sim", SYMBOL).unwrap().pos_long, 1);
}

#[test]
fn deployment_config_describes_provider_backed_tqkq_sim_without_protocol_leaks() {
    let config = StrategyDeploymentConfig::tqkq_sim("demo-user", "demo-pass")
        .account_number(7)
        .futures_market()
        .quote(SYMBOL)
        .startup_timeout(Duration::from_secs(5))
        .lifecycle(StrategyLifecycle::new().max_steps(3))
        .risk(RiskEngine::new().max_order_volume(1));

    assert_eq!(config.provider(), StrategyEnvironmentProvider::TqKqSim);
    assert_eq!(config.subscriptions().quote_symbols(), &[SYMBOL.to_owned()]);
    assert_eq!(config.lifecycle_policy().max_steps_limit(), Some(3));
    assert_eq!(config.startup_timeout_value(), Duration::from_secs(5));
    assert!(config.risk_engine().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn deployment_lifecycle_runs_replay_until_max_steps() {
    let replay = MarketCacheReplay::new(vec![
        MarketCacheEvent::quote(
            "cache",
            SYMBOL,
            1_000,
            Some(900),
            Quote {
                last_price: 481.0,
                ..Quote::default()
            },
        )
        .unwrap(),
        MarketCacheEvent::quote(
            "cache",
            SYMBOL,
            2_000,
            Some(1_900),
            Quote {
                last_price: 482.0,
                ..Quote::default()
            },
        )
        .unwrap(),
    ]);
    let replay_builder = tqsdk_task::StrategyReplay::builder(replay).market(
        FakeMarket::new()
            .account("sim", 100_000.0)
            .position("sim", SYMBOL, 0),
    );
    let environment = StrategyEnvironment::from_replay_builder(replay_builder)
        .account("sim")
        .quote(SYMBOL)
        .build()
        .await
        .unwrap();
    let mut deployment = StrategyDeployment::from_environment(environment)
        .account_id("sim")
        .lifecycle(StrategyLifecycle::new().max_steps(1))
        .build()
        .await
        .unwrap();

    let report = deployment
        .run(|ctx| {
            Box::pin(async move {
                assert_eq!(ctx.kind(), StrategyEnvironmentKind::Replay);
                assert!(ctx.replay_event().is_some());
                Ok(())
            })
        })
        .await
        .unwrap();

    assert_eq!(deployment.account_id(), Some("sim"));
    assert_eq!(report.steps(), 1);
    assert_eq!(report.stop_reason(), StrategyRunStopReason::MaxSteps);
}

#[tokio::test(flavor = "current_thread")]
async fn deployment_runs_async_strategy_step_and_reports_shutdown() {
    let harness = StrategyTestHarness::new()
        .market(
            FakeMarket::new()
                .quote(SYMBOL, 481.0)
                .account("sim", 100_000.0)
                .position("sim", SYMBOL, 0),
        )
        .broker(FakeBroker::new().fill_all())
        .build()
        .unwrap();
    let environment = StrategyEnvironment::from_test_harness(harness)
        .account("sim")
        .quote(SYMBOL)
        .build()
        .await
        .unwrap();
    let mut deployment = StrategyDeployment::from_environment(environment)
        .account_id("sim")
        .lifecycle(StrategyLifecycle::new().max_steps(1))
        .build()
        .await
        .unwrap();

    let run_report = deployment
        .run(|ctx| {
            Box::pin(async move {
                buy_breakout_once(ctx).await?;
                let test_report = ctx.finish_test_step().await?;
                assert_eq!(test_report.trades().len(), 1);
                assert_eq!(test_report.position("sim", SYMBOL).unwrap().pos_long, 1);
                Ok(())
            })
        })
        .await
        .unwrap();
    let shutdown_report = deployment.shutdown().await.unwrap();

    assert_eq!(run_report.steps(), 1);
    assert_eq!(shutdown_report.kind(), StrategyEnvironmentKind::TaskHost);
    assert!(shutdown_report.graceful());
}
