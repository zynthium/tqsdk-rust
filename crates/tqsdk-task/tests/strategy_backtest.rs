use tqsdk_core::Quote;
use tqsdk_data::{MarketCacheEvent, MarketCacheReplay};
use tqsdk_task::{StrategyBacktest, TqSim};
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
