use std::time::Duration;

use tqsdk_core::OrderLifecycle;
use tqsdk_task::StrategyHost;
use tqsdk_task::testing::{
    FakeBroker, FakeBrokerConnectionStatus, FakeMarket, StrategyTestClock, StrategyTestHarness,
};

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
    assert_eq!(report.orders()[0].lifecycle, OrderLifecycle::Filled);
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
    assert_eq!(report.orders()[0].lifecycle, OrderLifecycle::Rejected);
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
    assert_eq!(
        report.orders()[0].lifecycle,
        OrderLifecycle::PartiallyFilled
    );
    assert_eq!(report.orders()[0].volume_left, 3);
    assert_eq!(report.trades().len(), 1);
    assert_eq!(report.trades()[0].volume, 2);
    assert_eq!(report.position("sim", "SHFE.rb2601").unwrap().pos_long, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_test_harness_can_advance_partial_fills_across_steps() {
    let harness = StrategyTestHarness::new()
        .market(
            FakeMarket::new()
                .quote("SHFE.rb2601", 3_678.0)
                .account("sim", 80_000.0)
                .position("sim", "SHFE.rb2601", 0),
        )
        .broker(FakeBroker::new().partial_fills([2, 2, 1]))
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
        .send_once("entry-step-partial")
        .await
        .unwrap();

    let first = ctx.finish_test_step().await.unwrap();
    assert_eq!(first.orders().len(), 1);
    assert_eq!(first.orders()[0].lifecycle, OrderLifecycle::PartiallyFilled);
    assert_eq!(first.orders()[0].volume_left, 3);
    assert_eq!(first.trades().len(), 1);
    assert_eq!(first.trades()[0].volume, 2);
    assert_eq!(first.pending_orders(), 1);
    assert_eq!(first.position("sim", "SHFE.rb2601").unwrap().pos_long, 2);

    let second = ctx.finish_test_step().await.unwrap();
    assert_eq!(second.orders().len(), 1);
    assert_eq!(
        second.orders()[0].lifecycle,
        OrderLifecycle::PartiallyFilled
    );
    assert_eq!(second.orders()[0].volume_left, 1);
    assert_eq!(second.trades().len(), 1);
    assert_eq!(second.trades()[0].volume, 2);
    assert_eq!(second.pending_orders(), 1);
    assert_eq!(second.position("sim", "SHFE.rb2601").unwrap().pos_long, 4);

    let third = ctx.finish_test_step().await.unwrap();
    assert_eq!(third.orders().len(), 1);
    assert_eq!(third.orders()[0].lifecycle, OrderLifecycle::Filled);
    assert_eq!(third.orders()[0].volume_left, 0);
    assert_eq!(third.trades().len(), 1);
    assert_eq!(third.trades()[0].volume, 1);
    assert_eq!(third.pending_orders(), 0);
    assert_eq!(third.position("sim", "SHFE.rb2601").unwrap().pos_long, 5);
    assert_ne!(first.trades()[0].trade_id, second.trades()[0].trade_id);
    assert_ne!(second.trades()[0].trade_id, third.trades()[0].trade_id);
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_test_harness_uses_deterministic_clock_for_fake_broker_events() {
    let start_ns = 1_800_000_000_000_000_000;
    let harness = StrategyTestHarness::new()
        .market(
            FakeMarket::new()
                .quote("SHFE.rb2601", 3_678.0)
                .account("sim", 80_000.0)
                .position("sim", "SHFE.rb2601", 0),
        )
        .broker(FakeBroker::new().fill_all())
        .clock(StrategyTestClock::new(start_ns).step_by(Duration::from_millis(250)))
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
        .send_once("entry-clocked")
        .await
        .unwrap();

    let report = ctx.finish_test_step().await.unwrap();
    assert_eq!(report.orders()[0].insert_date_time, start_ns);
    assert_eq!(report.trades()[0].trade_date_time, start_ns + 250_000_000);
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_test_harness_can_delay_fake_broker_outcomes_by_test_steps() {
    let harness = StrategyTestHarness::new()
        .market(
            FakeMarket::new()
                .quote("SHFE.rb2601", 3_678.0)
                .account("sim", 80_000.0)
                .position("sim", "SHFE.rb2601", 0),
        )
        .broker(FakeBroker::new().fill_all().latency_steps(1))
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
        .send_once("entry-delayed")
        .await
        .unwrap();

    let first_step = ctx.finish_test_step().await.unwrap();
    assert!(first_step.orders().is_empty());
    assert!(first_step.trades().is_empty());
    assert_eq!(first_step.pending_orders(), 1);
    assert_eq!(
        first_step.position("sim", "SHFE.rb2601").unwrap().pos_long,
        0
    );

    let second_step = ctx.finish_test_step().await.unwrap();
    assert_eq!(second_step.orders().len(), 1);
    assert_eq!(second_step.orders()[0].lifecycle, OrderLifecycle::Filled);
    assert_eq!(second_step.trades().len(), 1);
    assert_eq!(second_step.pending_orders(), 0);
    assert_eq!(
        second_step.position("sim", "SHFE.rb2601").unwrap().pos_long,
        1
    );
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_test_harness_defers_orders_until_fake_broker_reconnects() {
    let harness = StrategyTestHarness::new()
        .market(
            FakeMarket::new()
                .quote("SHFE.rb2601", 3_678.0)
                .account("sim", 80_000.0)
                .position("sim", "SHFE.rb2601", 0),
        )
        .broker(FakeBroker::new().fill_all().disconnect_for_steps(1))
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
        .send_once("entry-reconnect")
        .await
        .unwrap();

    let disconnected = ctx.finish_test_step().await.unwrap();
    assert_eq!(
        disconnected.broker_connection_status(),
        FakeBrokerConnectionStatus::Disconnected
    );
    assert!(disconnected.orders().is_empty());
    assert!(disconnected.trades().is_empty());
    assert_eq!(disconnected.pending_orders(), 1);
    assert_eq!(
        disconnected
            .position("sim", "SHFE.rb2601")
            .unwrap()
            .pos_long,
        0
    );

    let reconnected = ctx.finish_test_step().await.unwrap();
    assert_eq!(
        reconnected.broker_connection_status(),
        FakeBrokerConnectionStatus::Reconnected
    );
    assert_eq!(reconnected.orders().len(), 1);
    assert_eq!(reconnected.orders()[0].lifecycle, OrderLifecycle::Filled);
    assert_eq!(reconnected.trades().len(), 1);
    assert_eq!(reconnected.pending_orders(), 0);
    assert_eq!(
        reconnected.position("sim", "SHFE.rb2601").unwrap().pos_long,
        1
    );
}
