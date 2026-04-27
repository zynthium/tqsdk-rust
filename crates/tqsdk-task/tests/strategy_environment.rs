use tqsdk_core::Quote;
use tqsdk_data::{MarketCacheEvent, MarketCacheReplay};
use tqsdk_task::testing::{FakeBroker, FakeMarket, StrategyTestHarness};
use tqsdk_task::{StrategyEnvironment, StrategyEnvironmentContext, StrategyEnvironmentKind};

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
