use std::time::Duration;

use tqsdk_core::{Kline, Quote};
use tqsdk_data::{MarketCacheEvent, MarketCacheReplay};
use tqsdk_task::StrategyReplay;
use tqsdk_task::testing::{FakeBroker, FakeMarket};

#[tokio::test(flavor = "current_thread")]
async fn strategy_replay_drives_quote_events_into_strategy_context() {
    let quote = Quote {
        last_price: 480.5,
        ..Quote::default()
    };
    let replay = MarketCacheReplay::new(vec![
        MarketCacheEvent::quote("cache", "SHFE.au2602", 1_000, Some(900), quote).unwrap(),
    ]);

    let mut strategy = StrategyReplay::builder(replay)
        .market(
            FakeMarket::new()
                .account("sim", 100_000.0)
                .position("sim", "SHFE.au2602", 0),
        )
        .broker(FakeBroker::new().fill_all())
        .account("sim")
        .quote("SHFE.au2602")
        .build()
        .await
        .unwrap();

    let mut ctx = strategy.next().await.unwrap().unwrap();
    assert_eq!(ctx.event().source(), "cache");
    assert_eq!(ctx.event().symbol(), "SHFE.au2602");
    assert_eq!(ctx.event().event_time_ns(), 900);
    assert_eq!(ctx.quote("SHFE.au2602").unwrap().last_price, 480.5);

    ctx.orders("sim")
        .buy_open("SHFE.au2602", 1)
        .limit(480.5)
        .send_once("replay-entry-1")
        .await
        .unwrap();
    let report = ctx.finish_test_step().await.unwrap();
    assert_eq!(report.orders().len(), 1);
    assert_eq!(report.trades().len(), 1);
    assert_eq!(
        report.position("sim", "SHFE.au2602").unwrap().pos_long,
        1
    );

    drop(ctx);
    assert!(strategy.next().await.unwrap().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_replay_drives_kline_events_into_strategy_context() {
    let older = Kline {
        id: 1,
        datetime: 1_000,
        close: 480.0,
        ..Kline::default()
    };
    let newer = Kline {
        id: 2,
        datetime: 2_000,
        close: 481.0,
        ..Kline::default()
    };
    let replay = MarketCacheReplay::new(vec![
        MarketCacheEvent::kline(
            "cache",
            "SHFE.au2602",
            2_100,
            Some(2_000),
            60_000_000_000,
            newer,
        )
        .unwrap(),
        MarketCacheEvent::kline(
            "cache",
            "SHFE.au2602",
            1_100,
            Some(1_000),
            60_000_000_000,
            older,
        )
        .unwrap(),
    ]);

    let mut strategy = StrategyReplay::builder(replay)
        .market(FakeMarket::new().account("sim", 100_000.0))
        .broker(FakeBroker::new().fill_all())
        .account("sim")
        .kline("SHFE.au2602", Duration::from_secs(60), 16)
        .build()
        .await
        .unwrap();

    let ctx = strategy.next().await.unwrap().unwrap();
    assert_eq!(ctx.event().event_time_ns(), 1_000);
    assert_eq!(
        ctx.kline("SHFE.au2602", Duration::from_secs(60))
            .unwrap()
            .last()
            .unwrap()
            .close,
        480.0
    );
    drop(ctx);

    let ctx = strategy.next().await.unwrap().unwrap();
    assert_eq!(ctx.event().event_time_ns(), 2_000);
    assert_eq!(
        ctx.kline("SHFE.au2602", Duration::from_secs(60))
            .unwrap()
            .last()
            .unwrap()
            .close,
        481.0
    );
}
