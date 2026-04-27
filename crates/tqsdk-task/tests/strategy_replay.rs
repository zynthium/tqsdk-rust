use std::path::PathBuf;
use std::time::Duration;

use tqsdk_core::{Kline, Quote};
use tqsdk_data::{MarketCacheEvent, MarketCacheReplay};
use tqsdk_task::testing::{FakeBroker, FakeMarket};
use tqsdk_task::{StrategyReplay, StrategyReplayCheckpointStore, StrategyReplaySpeed};

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
    assert_eq!(report.position("sim", "SHFE.au2602").unwrap().pos_long, 1);

    drop(ctx);
    assert!(strategy.next().await.unwrap().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_replay_drives_kline_events_into_strategy_context() {
    let mut strategy = replay_strategy(two_kline_replay()).await;

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

#[tokio::test(flavor = "current_thread")]
async fn strategy_replay_exposes_replay_clock_and_checkpoint() {
    let mut strategy = replay_strategy(two_kline_replay()).await;

    let ctx = strategy.next().await.unwrap().unwrap();
    assert_eq!(ctx.replay_time_ns(), 1_000);
    assert_eq!(ctx.checkpoint().next_event_index(), 1);
    assert_eq!(ctx.checkpoint().replay_time_ns(), Some(1_000));
    let checkpoint = ctx.checkpoint();
    drop(ctx);

    assert_eq!(strategy.replay_time_ns(), Some(1_000));
    assert_eq!(strategy.checkpoint(), checkpoint);
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_replay_resume_from_checkpoint_skips_processed_events() {
    let mut first = replay_strategy(two_kline_replay()).await;
    let ctx = first.next().await.unwrap().unwrap();
    let checkpoint = ctx.checkpoint();
    drop(ctx);

    let mut resumed = StrategyReplay::builder(two_kline_replay())
        .market(FakeMarket::new().account("sim", 100_000.0))
        .broker(FakeBroker::new().fill_all())
        .account("sim")
        .kline("SHFE.au2602", Duration::from_secs(60), 16)
        .resume_from(checkpoint)
        .build()
        .await
        .unwrap();

    assert_eq!(resumed.replay_time_ns(), Some(1_000));
    assert_eq!(resumed.checkpoint(), checkpoint);

    let ctx = resumed.next().await.unwrap().unwrap();
    assert_eq!(ctx.replay_time_ns(), 2_000);
    assert_eq!(ctx.checkpoint().next_event_index(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_replay_checkpoint_store_persists_and_resumes() {
    let path = temp_checkpoint_path("persist-resume");
    let store = StrategyReplayCheckpointStore::json_file(&path);
    store.clear().unwrap();

    assert_eq!(store.path(), path.as_path());
    assert_eq!(store.load().unwrap(), None);

    let mut first = replay_strategy(two_kline_replay()).await;
    let ctx = first.next().await.unwrap().unwrap();
    store.save(ctx.checkpoint()).unwrap();
    drop(ctx);

    let checkpoint = store.load().unwrap().unwrap();
    assert_eq!(checkpoint.next_event_index(), 1);
    assert_eq!(checkpoint.replay_time_ns(), Some(1_000));

    let mut resumed = StrategyReplay::builder(two_kline_replay())
        .market(FakeMarket::new().account("sim", 100_000.0))
        .broker(FakeBroker::new().fill_all())
        .account("sim")
        .kline("SHFE.au2602", Duration::from_secs(60), 16)
        .resume_from_store(&store)
        .unwrap()
        .build()
        .await
        .unwrap();

    assert_eq!(resumed.checkpoint(), checkpoint);
    let ctx = resumed.next().await.unwrap().unwrap();
    assert_eq!(ctx.replay_time_ns(), 2_000);
    drop(ctx);

    store.clear().unwrap();
    assert_eq!(store.load().unwrap(), None);
}

#[test]
fn strategy_replay_checkpoint_store_rejects_invalid_file() {
    let path = temp_checkpoint_path("invalid");
    let store = StrategyReplayCheckpointStore::json_file(&path);
    store.clear().unwrap();
    std::fs::write(&path, "{\"version\":1,\"next_event_index\":-1}").unwrap();

    assert!(store.load().is_err());

    store.clear().unwrap();
}

#[test]
fn strategy_replay_speed_rejects_invalid_multiplier() {
    assert!(StrategyReplaySpeed::scaled(0.0).is_err());
    assert!(StrategyReplaySpeed::scaled(f64::NAN).is_err());
    assert!(StrategyReplaySpeed::scaled(f64::INFINITY).is_err());
    assert_eq!(
        StrategyReplaySpeed::scaled(10.0).unwrap(),
        StrategyReplaySpeed::scaled(10.0).unwrap()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_replay_real_time_speed_waits_between_event_times() {
    let mut strategy =
        StrategyReplay::builder(two_kline_replay_with_times(1_000_000_000, 1_020_000_000))
            .market(FakeMarket::new().account("sim", 100_000.0))
            .broker(FakeBroker::new().fill_all())
            .account("sim")
            .kline("SHFE.au2602", Duration::from_secs(60), 16)
            .speed(StrategyReplaySpeed::REAL_TIME)
            .build()
            .await
            .unwrap();

    assert_eq!(strategy.speed(), StrategyReplaySpeed::REAL_TIME);

    let ctx = strategy.next().await.unwrap().unwrap();
    assert_eq!(ctx.replay_time_ns(), 1_000_000_000);
    drop(ctx);

    let started = std::time::Instant::now();
    let ctx = strategy.next().await.unwrap().unwrap();
    assert_eq!(ctx.replay_time_ns(), 1_020_000_000);
    assert!(
        started.elapsed() >= Duration::from_millis(10),
        "expected real-time replay to wait before ingesting the next event, elapsed={:?}",
        started.elapsed()
    );
}

fn two_kline_replay() -> MarketCacheReplay {
    two_kline_replay_with_times(1_000, 2_000)
}

fn two_kline_replay_with_times(older_time_ns: i64, newer_time_ns: i64) -> MarketCacheReplay {
    let older = Kline {
        id: 1,
        datetime: older_time_ns,
        close: 480.0,
        ..Kline::default()
    };
    let newer = Kline {
        id: 2,
        datetime: newer_time_ns,
        close: 481.0,
        ..Kline::default()
    };
    MarketCacheReplay::new(vec![
        MarketCacheEvent::kline(
            "cache",
            "SHFE.au2602",
            newer_time_ns + 100,
            Some(newer_time_ns),
            60_000_000_000,
            newer,
        )
        .unwrap(),
        MarketCacheEvent::kline(
            "cache",
            "SHFE.au2602",
            older_time_ns + 100,
            Some(older_time_ns),
            60_000_000_000,
            older,
        )
        .unwrap(),
    ])
}

async fn replay_strategy(replay: MarketCacheReplay) -> StrategyReplay {
    StrategyReplay::builder(replay)
        .market(FakeMarket::new().account("sim", 100_000.0))
        .broker(FakeBroker::new().fill_all())
        .account("sim")
        .kline("SHFE.au2602", Duration::from_secs(60), 16)
        .build()
        .await
        .unwrap()
}

fn temp_checkpoint_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "tqsdk-rust-strategy-replay-{name}-{}.json",
        std::process::id()
    ))
}
