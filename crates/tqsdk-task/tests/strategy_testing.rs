use tqsdk_task::StrategyHost;
use tqsdk_task::testing::{FakeBroker, FakeMarket, StrategyTestHarness};

#[tokio::test(flavor = "current_thread")]
async fn strategy_test_harness_seeds_market_and_fills_orders() {
    let harness = StrategyTestHarness::new()
        .market(
            FakeMarket::new()
                .quote("SHFE.rb2601", 3_678.0)
                .account("sim", 80_000.0)
                .position("sim", "SHFE.rb2601", 0),
        )
        .broker(FakeBroker::new().fill_all())
        .build()
        .unwrap();

    let mut strategy = StrategyHost::builder(harness.into_task_host())
        .account("sim")
        .quote("SHFE.rb2601")
        .build()
        .await
        .unwrap();
    let mut ctx = strategy.next_once().await.unwrap();
    assert_eq!(ctx.quote("SHFE.rb2601").unwrap().last_price, 3_678.0);

    ctx.orders("sim")
        .buy_open("SHFE.rb2601", 1)
        .limit(3_678.0)
        .send_once("entry-1")
        .await
        .unwrap();

    let report = ctx.finish_test_step().await.unwrap();
    assert_eq!(report.orders().len(), 1);
    assert_eq!(report.orders()[0].status, "FINISHED");
    assert_eq!(report.orders()[0].volume_left, 0);
    assert_eq!(report.trades().len(), 1);
    assert_eq!(report.position("sim", "SHFE.rb2601").unwrap().pos_long, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_test_harness_can_reject_orders() {
    let harness = StrategyTestHarness::new()
        .market(
            FakeMarket::new()
                .quote("SHFE.rb2601", 3_678.0)
                .account("sim", 80_000.0)
                .position("sim", "SHFE.rb2601", 0),
        )
        .broker(FakeBroker::new().reject_all("test rejection"))
        .build()
        .unwrap();

    let mut strategy = StrategyHost::builder(harness.into_task_host())
        .account("sim")
        .quote("SHFE.rb2601")
        .build()
        .await
        .unwrap();
    let mut ctx = strategy.next_once().await.unwrap();

    ctx.orders("sim")
        .buy_open("SHFE.rb2601", 1)
        .limit(3_678.0)
        .send_once("entry-rejected")
        .await
        .unwrap();

    let report = ctx.finish_test_step().await.unwrap();
    assert_eq!(report.orders().len(), 1);
    assert_eq!(report.orders()[0].status, "FINISHED");
    assert_eq!(report.orders()[0].last_msg, "test rejection");
    assert!(report.trades().is_empty());
    assert_eq!(report.position("sim", "SHFE.rb2601").unwrap().pos_long, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_test_harness_can_partial_fill_orders() {
    let harness = StrategyTestHarness::new()
        .market(
            FakeMarket::new()
                .quote("SHFE.rb2601", 3_678.0)
                .account("sim", 80_000.0)
                .position("sim", "SHFE.rb2601", 0),
        )
        .broker(FakeBroker::new().partial_fill(2))
        .build()
        .unwrap();

    let mut strategy = StrategyHost::builder(harness.into_task_host())
        .account("sim")
        .quote("SHFE.rb2601")
        .build()
        .await
        .unwrap();
    let mut ctx = strategy.next_once().await.unwrap();

    ctx.orders("sim")
        .buy_open("SHFE.rb2601", 5)
        .limit(3_678.0)
        .send_once("entry-partial")
        .await
        .unwrap();

    let report = ctx.finish_test_step().await.unwrap();
    assert_eq!(report.orders().len(), 1);
    assert_eq!(report.orders()[0].status, "ALIVE");
    assert_eq!(report.orders()[0].volume_left, 3);
    assert_eq!(report.trades().len(), 1);
    assert_eq!(report.trades()[0].volume, 2);
    assert_eq!(report.position("sim", "SHFE.rb2601").unwrap().pos_long, 2);
}
