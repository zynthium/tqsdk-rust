use tqsdk_core::{Kline, Quote, Tick};
use tqsdk_data::{MarketCacheEvent, MarketCacheReplay};
use tqsdk_task::{StrategyBacktest, TaskError, TqSim};
use tqsdk_wait::OrderTicketState;

#[tokio::test]
async fn strategy_backtest_fills_limit_order_against_replayed_quote() {
    let replay = MarketCacheReplay::new(vec![
        MarketCacheEvent::quote(
            "fixture",
            "SHFE.rb2501",
            1_000,
            Some(1_000),
            Quote {
                datetime: "2026-05-15 09:30:00.000000".to_string(),
                last_price: 100.0,
                ask_price1: 100.0,
                ask_volume1: 10,
                bid_price1: 99.0,
                bid_volume1: 8,
                ..Quote::default()
            },
        )
        .unwrap(),
    ]);

    let mut backtest = StrategyBacktest::builder(replay)
        .sim(TqSim::new().with_margin("SHFE.rb2501", 1_000.0))
        .quote("SHFE.rb2501")
        .build()
        .await
        .unwrap();

    let mut ctx = backtest.next().await.unwrap().unwrap();
    assert_eq!(ctx.quote("SHFE.rb2501").unwrap().ask_price1, 100.0);
    assert_eq!(ctx.position("TQSIM", "SHFE.rb2501").unwrap().pos, 0);

    let ticket = ctx
        .orders("TQSIM")
        .buy_open("SHFE.rb2501", 2)
        .limit(101.0)
        .send_once("order-1")
        .await
        .unwrap();
    let report = ctx.finish_sim_step().unwrap();

    assert_eq!(report.trades().len(), 1);
    assert_eq!(report.trades()[0].price, 101.0);
    assert_eq!(ctx.position("TQSIM", "SHFE.rb2501").unwrap().pos_long, 2);
    assert!(matches!(
        ticket.status(ctx.task_host().api()).unwrap(),
        OrderTicketState::Filled { .. }
    ));

    assert!(backtest.next().await.unwrap().is_none());
}

#[tokio::test]
async fn strategy_backtest_fills_alive_limit_order_on_later_quote() {
    let replay = MarketCacheReplay::new(vec![
        quote_event("SHFE.rb2501", 1_000, 100.0, 10, 99.0, 8),
        quote_event("SHFE.rb2501", 2_000, 98.0, 10, 97.0, 8),
    ]);

    let mut backtest = StrategyBacktest::builder(replay)
        .sim(TqSim::new())
        .quote("SHFE.rb2501")
        .build()
        .await
        .unwrap();

    let mut ctx = backtest.next().await.unwrap().unwrap();
    let ticket = ctx
        .orders("TQSIM")
        .buy_open("SHFE.rb2501", 1)
        .limit(99.0)
        .send_once("order-2")
        .await
        .unwrap();
    let report = ctx.finish_sim_step().unwrap();
    assert_eq!(report.orders()[0].status, "ALIVE");
    assert!(report.trades().is_empty());

    let ctx = backtest.next().await.unwrap().unwrap();
    assert_eq!(ctx.position("TQSIM", "SHFE.rb2501").unwrap().pos_long, 1);
    assert!(matches!(
        ticket.status(ctx.task_host().api()).unwrap(),
        OrderTicketState::Filled { .. }
    ));
}

#[tokio::test]
async fn strategy_backtest_synthesizes_tick_as_quote_and_fills_pending_order() {
    let replay = MarketCacheReplay::new(vec![
        tick_event("SHFE.rb2501", 1_000, 100.0, 10, 99.0, 8),
        tick_event("SHFE.rb2501", 2_000, 98.0, 10, 97.0, 8),
    ]);

    let mut backtest = StrategyBacktest::builder(replay)
        .sim(TqSim::new())
        .quote("SHFE.rb2501")
        .build()
        .await
        .unwrap();

    let mut ctx = backtest.next().await.unwrap().unwrap();
    assert_eq!(ctx.quote("SHFE.rb2501").unwrap().last_price, 100.0);
    let ticket = ctx
        .orders("TQSIM")
        .buy_open("SHFE.rb2501", 1)
        .limit(99.0)
        .send_once("tick-order")
        .await
        .unwrap();
    assert!(ctx.finish_sim_step().unwrap().trades().is_empty());

    let ctx = backtest.next().await.unwrap().unwrap();
    assert_eq!(ctx.quote("SHFE.rb2501").unwrap().last_price, 98.0);
    assert_eq!(ctx.position("TQSIM", "SHFE.rb2501").unwrap().pos_long, 1);
    assert!(matches!(
        ticket.status(ctx.task_host().api()).unwrap(),
        OrderTicketState::Filled { .. }
    ));
}

#[tokio::test]
async fn strategy_backtest_synthesizes_kline_close_quote_once_for_strategy() {
    let replay = MarketCacheReplay::new(vec![kline_event(
        "SHFE.rb2501",
        1_000,
        101.0,
        105.0,
        97.0,
        99.0,
    )]);

    let mut backtest = StrategyBacktest::builder(replay)
        .sim(TqSim::new())
        .quote("SHFE.rb2501")
        .price_tick("SHFE.rb2501", 1.0)
        .build()
        .await
        .unwrap();

    let ctx = backtest.next().await.unwrap().unwrap();
    let quote = ctx.quote("SHFE.rb2501").unwrap();
    assert_eq!(quote.last_price, 99.0);
    assert_eq!(quote.ask_price1, 100.0);
    assert_eq!(quote.bid_price1, 98.0);
    assert!(backtest.next().await.unwrap().is_none());
    assert_eq!(backtest.summary().event_count(), 1);
    assert_eq!(backtest.summary().kline_count(), 1);
}

#[tokio::test]
async fn strategy_backtest_kline_checkpoints_fill_pending_orders_without_extra_strategy_steps() {
    let replay = MarketCacheReplay::new(vec![
        quote_event("SHFE.rb2501", 1_000, 120.0, 10, 90.0, 10),
        kline_event("SHFE.rb2501", 2_000, 100.0, 110.0, 95.0, 102.0),
    ]);

    let mut backtest = StrategyBacktest::builder(replay)
        .sim(TqSim::new())
        .quote("SHFE.rb2501")
        .price_tick("SHFE.rb2501", 1.0)
        .build()
        .await
        .unwrap();

    let mut ctx = backtest.next().await.unwrap().unwrap();
    let buy_ticket = ctx
        .orders("TQSIM")
        .buy_open("SHFE.rb2501", 1)
        .limit(96.0)
        .send_once("kline-buy")
        .await
        .unwrap();
    let sell_ticket = ctx
        .orders("TQSIM")
        .sell_open("SHFE.rb2501", 1)
        .limit(109.0)
        .send_once("kline-sell")
        .await
        .unwrap();
    let report = ctx.finish_sim_step().unwrap();
    assert_eq!(report.orders().len(), 2);
    assert!(report.trades().is_empty());

    let ctx = backtest.next().await.unwrap().unwrap();
    assert_eq!(ctx.quote("SHFE.rb2501").unwrap().last_price, 102.0);
    assert!(matches!(
        buy_ticket.status(ctx.task_host().api()).unwrap(),
        OrderTicketState::Filled { .. }
    ));
    assert!(matches!(
        sell_ticket.status(ctx.task_host().api()).unwrap(),
        OrderTicketState::Filled { .. }
    ));
    assert!(backtest.next().await.unwrap().is_none());
    assert_eq!(backtest.summary().event_count(), 2);
    assert_eq!(backtest.summary().kline_count(), 1);
    assert_eq!(backtest.summary().trades().len(), 2);
}

#[tokio::test]
async fn strategy_backtest_rejects_kline_without_price_tick() {
    let replay = MarketCacheReplay::new(vec![kline_event(
        "SHFE.rb2501",
        1_000,
        101.0,
        105.0,
        97.0,
        99.0,
    )]);
    let mut backtest = StrategyBacktest::builder(replay)
        .quote("SHFE.rb2501")
        .build()
        .await
        .unwrap();

    let err = match backtest.next().await {
        Ok(_) => panic!("kline without price_tick should fail"),
        Err(error) => error,
    };
    assert!(matches!(err, TaskError::Unsupported(message) if message.contains("price_tick")));
}

#[tokio::test]
async fn strategy_backtest_rejects_invalid_price_tick_config() {
    for price_tick in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        let replay = MarketCacheReplay::new(Vec::new());
        let err = match StrategyBacktest::builder(replay)
            .quote("SHFE.rb2501")
            .price_tick("SHFE.rb2501", price_tick)
            .build()
            .await
        {
            Ok(_) => panic!("invalid price_tick should fail"),
            Err(error) => error,
        };
        assert!(matches!(err, TaskError::InvalidState(message) if message.contains("price_tick")));
    }
}

#[tokio::test]
async fn strategy_backtest_summary_tracks_counts_and_final_snapshots() {
    let replay = MarketCacheReplay::new(vec![
        quote_event("SHFE.rb2501", 1_000, 100.0, 10, 99.0, 8),
        tick_event("SHFE.rb2501", 2_000, 101.0, 10, 100.0, 8),
    ]);

    let mut backtest = StrategyBacktest::builder(replay)
        .sim(TqSim::new().with_margin("SHFE.rb2501", 1_000.0))
        .quote("SHFE.rb2501")
        .build()
        .await
        .unwrap();

    let mut ctx = backtest.next().await.unwrap().unwrap();
    ctx.orders("TQSIM")
        .buy_open("SHFE.rb2501", 1)
        .limit(101.0)
        .send_once("summary-order")
        .await
        .unwrap();
    ctx.finish_sim_step().unwrap();
    let _ctx = backtest.next().await.unwrap().unwrap();

    let summary = backtest.summary();
    assert_eq!(summary.event_count(), 2);
    assert_eq!(summary.quote_count(), 1);
    assert_eq!(summary.tick_count(), 1);
    assert_eq!(summary.kline_count(), 0);
    assert_eq!(summary.orders().len(), backtest.sim().orders().len());
    assert_eq!(summary.trades().len(), backtest.sim().trades().len());
    assert_eq!(summary.final_account().user_id, "TQSIM");
    assert_eq!(summary.final_positions()[0].pos_long, 1);
}

fn quote_event(
    symbol: &str,
    datetime: i64,
    ask_price1: f64,
    ask_volume1: i64,
    bid_price1: f64,
    bid_volume1: i64,
) -> MarketCacheEvent {
    MarketCacheEvent::quote(
        "fixture",
        symbol,
        datetime,
        Some(datetime),
        Quote {
            datetime: "2026-05-15 09:30:00.000000".to_string(),
            last_price: ask_price1,
            ask_price1,
            ask_volume1,
            bid_price1,
            bid_volume1,
            ..Quote::default()
        },
    )
    .unwrap()
}

fn tick_event(
    symbol: &str,
    datetime: i64,
    ask_price1: f64,
    ask_volume1: i64,
    bid_price1: f64,
    bid_volume1: i64,
) -> MarketCacheEvent {
    MarketCacheEvent::tick(
        "fixture",
        symbol,
        datetime,
        Some(datetime),
        Tick {
            datetime,
            last_price: ask_price1,
            highest: ask_price1,
            lowest: bid_price1,
            ask_price1,
            ask_volume1,
            bid_price1,
            bid_volume1,
            volume: 100,
            amount: ask_price1 * 100.0,
            open_interest: 50,
            ..Tick::default()
        },
    )
    .unwrap()
}

fn kline_event(
    symbol: &str,
    datetime: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
) -> MarketCacheEvent {
    MarketCacheEvent::kline(
        "fixture",
        symbol,
        datetime,
        Some(datetime),
        60_000_000_000,
        Kline {
            id: datetime,
            datetime,
            open,
            high,
            low,
            close,
            volume: 100,
            open_oi: 40,
            close_oi: 50,
            ..Kline::default()
        },
    )
    .unwrap()
}
