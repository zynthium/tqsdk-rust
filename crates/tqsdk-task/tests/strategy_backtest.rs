use chrono::NaiveDate;
use tqsdk_core::{Kline, Quote, Tick};
use tqsdk_task::{ReplayMarketEvent, ReplayMarketSource, StrategyBacktest, TaskError, TqSim};
use tqsdk_wait::OrderTicketState;

#[tokio::test]
async fn strategy_backtest_fills_limit_order_against_replayed_quote() {
    let replay = ReplayMarketSource::new(vec![
        ReplayMarketEvent::quote(
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
async fn strategy_backtest_tracks_symbols_from_replay_without_quote_preset() {
    let replay =
        ReplayMarketSource::new(vec![quote_event("SHFE.rb2501", 1_000, 100.0, 10, 99.0, 8)]);

    let mut backtest = StrategyBacktest::builder(replay)
        .sim(TqSim::new())
        .build()
        .await
        .unwrap();

    let ctx = backtest.next().await.unwrap().unwrap();
    assert_eq!(ctx.quote("SHFE.rb2501").unwrap().ask_price1, 100.0);
    assert_eq!(ctx.position("TQSIM", "SHFE.rb2501").unwrap().pos, 0);
    let summary = backtest.summary();
    let final_position = &summary.final_positions()[0];
    assert_eq!(final_position.exchange_id, "SHFE");
    assert_eq!(final_position.instrument_id, "rb2501");
}

#[tokio::test]
async fn strategy_backtest_fills_alive_limit_order_on_later_quote() {
    let replay = ReplayMarketSource::new(vec![
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
    let replay = ReplayMarketSource::new(vec![
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
    let replay = ReplayMarketSource::new(vec![kline_event(
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
async fn strategy_backtest_applies_replay_underlying_to_synthesized_kline_quote() {
    let alias = "KQ.m@SHFE.rb";
    let underlying = "SHFE.rb2501";
    let replay = ReplayMarketSource::new(vec![
        kline_event(alias, 1_000, 101.0, 105.0, 97.0, 99.0)
            .with_underlying_symbol(underlying)
            .unwrap(),
    ]);

    let mut backtest = StrategyBacktest::builder(replay)
        .sim(TqSim::new())
        .quote(alias)
        .price_tick(alias, 1.0)
        .build()
        .await
        .unwrap();

    let ctx = backtest.next().await.unwrap().unwrap();
    assert_eq!(ctx.event().underlying_symbol(), Some(underlying));
    let quote = ctx.quote(alias).unwrap();
    assert_eq!(quote.last_price, 99.0);
    assert_eq!(quote.underlying_symbol, underlying);
}

#[tokio::test]
async fn strategy_backtest_uses_default_price_tick_for_kline_quote_synthesis() {
    let replay = ReplayMarketSource::new(vec![kline_event(
        "SHFE.rb2501",
        1_000,
        101.0,
        105.0,
        97.0,
        99.0,
    )]);

    let mut backtest = StrategyBacktest::builder(replay)
        .sim(TqSim::new())
        .default_price_tick(0.5)
        .build()
        .await
        .unwrap();

    let ctx = backtest.next().await.unwrap().unwrap();
    let quote = ctx.quote("SHFE.rb2501").unwrap();
    assert_eq!(quote.last_price, 99.0);
    assert_eq!(quote.ask_price1, 99.5);
    assert_eq!(quote.bid_price1, 98.5);
}

#[tokio::test]
async fn strategy_backtest_uses_replayed_quote_metadata_for_kline_quote_synthesis() {
    let replay = ReplayMarketSource::new(vec![
        ReplayMarketEvent::quote(
            "fixture",
            "SHFE.rb2501",
            500,
            Some(500),
            Quote {
                datetime: "2026-05-15 09:29:00.000000".to_string(),
                last_price: 100.0,
                ask_price1: 100.0,
                ask_volume1: 10,
                bid_price1: 99.5,
                bid_volume1: 8,
                price_tick: 0.5,
                price_decs: 1,
                volume_multiple: 10,
                margin: 1_000.0,
                commission: 2.5,
                ..Quote::default()
            },
        )
        .unwrap(),
        kline_event("SHFE.rb2501", 1_000, 101.0, 105.0, 97.0, 99.0),
    ]);

    let mut backtest = StrategyBacktest::builder(replay)
        .sim(TqSim::new())
        .build()
        .await
        .unwrap();

    let ctx = backtest.next().await.unwrap().unwrap();
    let quote = ctx.quote("SHFE.rb2501").unwrap();
    assert_eq!(quote.price_tick, 0.5);
    assert_eq!(quote.price_decs, 1);
    assert_eq!(quote.volume_multiple, 10);
    assert_eq!(quote.margin, 1_000.0);
    assert_eq!(quote.commission, 2.5);

    let ctx = backtest.next().await.unwrap().unwrap();
    let quote = ctx.quote("SHFE.rb2501").unwrap();
    assert_eq!(quote.last_price, 99.0);
    assert_eq!(quote.ask_price1, 99.5);
    assert_eq!(quote.bid_price1, 98.5);
}

#[tokio::test]
async fn strategy_backtest_explicit_price_tick_overrides_replayed_quote_metadata() {
    let replay = ReplayMarketSource::new(vec![
        ReplayMarketEvent::quote(
            "fixture",
            "SHFE.rb2501",
            500,
            Some(500),
            Quote {
                last_price: 100.0,
                ask_price1: 100.0,
                ask_volume1: 10,
                bid_price1: 99.5,
                bid_volume1: 8,
                price_tick: 0.5,
                ..Quote::default()
            },
        )
        .unwrap(),
        kline_event("SHFE.rb2501", 1_000, 101.0, 105.0, 97.0, 99.0),
    ]);

    let mut backtest = StrategyBacktest::builder(replay)
        .sim(TqSim::new())
        .price_tick("SHFE.rb2501", 1.0)
        .build()
        .await
        .unwrap();

    let _ctx = backtest.next().await.unwrap().unwrap();
    let ctx = backtest.next().await.unwrap().unwrap();
    let quote = ctx.quote("SHFE.rb2501").unwrap();
    assert_eq!(quote.ask_price1, 100.0);
    assert_eq!(quote.bid_price1, 98.0);
}

#[tokio::test]
async fn strategy_backtest_kline_checkpoints_fill_pending_orders_without_extra_strategy_steps() {
    let replay = ReplayMarketSource::new(vec![
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
    let replay = ReplayMarketSource::new(vec![kline_event(
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
        let replay = ReplayMarketSource::new(Vec::new());
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
async fn strategy_backtest_rejects_invalid_default_price_tick_config() {
    for price_tick in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        let replay = ReplayMarketSource::new(Vec::new());
        let err = match StrategyBacktest::builder(replay)
            .default_price_tick(price_tick)
            .build()
            .await
        {
            Ok(_) => panic!("invalid default_price_tick should fail"),
            Err(error) => error,
        };
        assert!(
            matches!(err, TaskError::InvalidState(message) if message.contains("default_price_tick"))
        );
    }
}

#[tokio::test]
async fn strategy_backtest_summary_tracks_counts_and_final_snapshots() {
    let replay = ReplayMarketSource::new(vec![
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
    assert_eq!(summary.trade_log().len(), summary.trades().len());
    assert_eq!(summary.initial_account().balance, 10_000_000.0);
    assert_eq!(summary.final_account().user_id, "TQSIM");
    assert_eq!(summary.final_positions()[0].pos_long, 1);
    assert_eq!(summary.balance_change(), 0.0);
    assert_eq!(summary.balance_return_rate(), 0.0);
}

#[tokio::test]
async fn strategy_backtest_summary_tracks_closed_profit_observations() {
    let replay = ReplayMarketSource::new(vec![
        quote_event("SHFE.rb2501", 1_000, 100.0, 10, 99.0, 8),
        quote_event("SHFE.rb2501", 2_000, 111.0, 10, 110.0, 8),
        quote_event("SHFE.rb2501", 3_000, 95.0, 10, 94.0, 8),
        quote_event("SHFE.rb2501", 4_000, 91.0, 10, 90.0, 8),
    ]);

    let mut backtest = StrategyBacktest::builder(replay)
        .sim(
            TqSim::new()
                .with_contract_multiplier("SHFE.rb2501", 10.0)
                .with_commission("SHFE.rb2501", 1.0),
        )
        .build()
        .await
        .unwrap();

    let mut ctx = backtest.next().await.unwrap().unwrap();
    ctx.orders("TQSIM")
        .buy_open("SHFE.rb2501", 1)
        .limit(100.0)
        .send_once("closed-profit-open-win")
        .await
        .unwrap();
    ctx.finish_sim_step().unwrap();

    let mut ctx = backtest.next().await.unwrap().unwrap();
    ctx.orders("TQSIM")
        .sell_close("SHFE.rb2501", 1)
        .limit(110.0)
        .send_once("closed-profit-close-win")
        .await
        .unwrap();
    ctx.finish_sim_step().unwrap();

    let mut ctx = backtest.next().await.unwrap().unwrap();
    ctx.orders("TQSIM")
        .buy_open("SHFE.rb2501", 1)
        .limit(95.0)
        .send_once("closed-profit-open-loss")
        .await
        .unwrap();
    ctx.finish_sim_step().unwrap();

    let mut ctx = backtest.next().await.unwrap().unwrap();
    ctx.orders("TQSIM")
        .sell_close("SHFE.rb2501", 1)
        .limit(90.0)
        .send_once("closed-profit-close-loss")
        .await
        .unwrap();
    ctx.finish_sim_step().unwrap();

    let summary = backtest.summary();
    assert_eq!(summary.realized_profit(), 50.0);
    assert_eq!(summary.total_commission(), 4.0);
    assert_eq!(summary.net_realized_profit(), 46.0);
    assert_eq!(summary.closed_profit_points().len(), 2);
    assert_eq!(summary.closed_profit_points()[0].event_count(), 2);
    assert_eq!(
        summary.closed_profit_points()[0].event_time_ns(),
        Some(2_000)
    );
    assert_eq!(summary.closed_profit_points()[0].trade_count(), 1);
    assert_eq!(summary.closed_profit_points()[0].profit(), 100.0);
    assert_eq!(summary.closed_profit_points()[1].event_count(), 4);
    assert_eq!(
        summary.closed_profit_points()[1].event_time_ns(),
        Some(4_000)
    );
    assert_eq!(summary.closed_profit_points()[1].trade_count(), 1);
    assert_eq!(summary.closed_profit_points()[1].profit(), -50.0);
    assert_eq!(summary.closed_trade_count(), 2);
    assert_eq!(summary.closed_profit_observation_count(), 2);
    assert_eq!(summary.winning_closed_profit_observation_count(), 1);
    assert_eq!(summary.losing_closed_profit_observation_count(), 1);
    assert_eq!(summary.gross_profit(), 100.0);
    assert_eq!(summary.gross_loss(), 50.0);
    assert_eq!(summary.profit_loss_ratio(), 2.0);
    assert_eq!(summary.winning_rate(), 0.5);
}

#[tokio::test]
async fn strategy_backtest_summary_tracks_balance_points_and_drawdown() {
    let replay =
        ReplayMarketSource::new(vec![quote_event("SHFE.rb2501", 1_000, 100.0, 10, 99.0, 8)]);

    let mut backtest = StrategyBacktest::builder(replay)
        .sim(TqSim::new().with_commission("SHFE.rb2501", 12.5))
        .build()
        .await
        .unwrap();

    let mut ctx = backtest.next().await.unwrap().unwrap();
    ctx.orders("TQSIM")
        .buy_open("SHFE.rb2501", 1)
        .limit(100.0)
        .send_once("summary-drawdown-order")
        .await
        .unwrap();
    ctx.finish_sim_step().unwrap();

    let summary = backtest.summary();
    assert_eq!(summary.balance_points().len(), 2);
    assert_eq!(summary.balance_points()[0].event_count(), 0);
    assert_eq!(summary.balance_points()[0].balance(), 10_000_000.0);
    assert_eq!(summary.balance_points()[1].event_count(), 1);
    assert_eq!(summary.balance_points()[1].balance(), 9_999_987.5);
    assert_eq!(summary.peak_balance(), 10_000_000.0);
    assert_eq!(summary.max_balance_drawdown(), 12.5);
    assert_eq!(summary.balance_change(), -12.5);
    assert!((summary.balance_return_rate() + 0.00000125).abs() < 1e-12);
    assert!((summary.max_balance_drawdown_rate() - 0.00000125).abs() < 1e-12);
    assert!((summary.balance_points()[1].return_rate() + 0.00000125).abs() < 1e-12);
    assert!((summary.balance_points()[1].drawdown_rate() - 0.00000125).abs() < 1e-12);
}

#[tokio::test]
async fn strategy_backtest_summary_tracks_mark_to_market_equity_points() {
    let replay = ReplayMarketSource::new(vec![
        quote_event("SHFE.rb2501", 86_400_000_000_000, 100.0, 10, 99.0, 8),
        quote_event("SHFE.rb2501", 172_800_000_000_000, 110.0, 10, 109.0, 8),
    ]);

    let mut backtest = StrategyBacktest::builder(replay)
        .sim(TqSim::new().with_contract_multiplier("SHFE.rb2501", 10.0))
        .build()
        .await
        .unwrap();

    let mut ctx = backtest.next().await.unwrap().unwrap();
    ctx.orders("TQSIM")
        .buy_open("SHFE.rb2501", 1)
        .limit(100.0)
        .send_once("summary-equity-order")
        .await
        .unwrap();
    ctx.finish_sim_step().unwrap();
    let _ctx = backtest.next().await.unwrap().unwrap();

    let summary = backtest.summary();
    assert_eq!(summary.final_account().float_profit, 100.0);
    assert_eq!(summary.initial_equity(), 10_000_000.0);
    assert_eq!(summary.final_equity(), 10_000_100.0);
    assert_eq!(summary.equity_change(), 100.0);
    assert_eq!(summary.peak_equity(), 10_000_100.0);
    assert_eq!(summary.max_equity_drawdown(), 0.0);
    assert_eq!(summary.equity_points().len(), 2);
    assert_eq!(summary.equity_points()[0].event_count(), 0);
    assert_eq!(summary.equity_points()[0].event_time_ns(), None);
    assert_eq!(summary.equity_points()[0].equity(), 10_000_000.0);
    assert_eq!(summary.equity_points()[1].event_count(), 2);
    assert_eq!(
        summary.equity_points()[1].event_time_ns(),
        Some(172_800_000_000_000)
    );
    assert_eq!(summary.equity_points()[1].equity(), 10_000_100.0);
    assert!((summary.equity_return_rate() - 0.00001).abs() < 1e-12);
    assert!((summary.equity_points()[1].return_rate() - 0.00001).abs() < 1e-12);
}

#[tokio::test]
async fn strategy_backtest_summary_derives_daily_equity_returns_and_sharpe() {
    let replay = ReplayMarketSource::new(vec![
        quote_event("SHFE.rb2501", 86_400_000_000_000, 100.0, 10, 99.0, 8),
        quote_event("SHFE.rb2501", 172_800_000_000_000, 110.0, 10, 109.0, 8),
        quote_event("SHFE.rb2501", 259_200_000_000_000, 105.0, 10, 104.0, 8),
    ]);

    let mut backtest = StrategyBacktest::builder(replay)
        .sim(TqSim::new().with_contract_multiplier("SHFE.rb2501", 10.0))
        .build()
        .await
        .unwrap();

    let mut ctx = backtest.next().await.unwrap().unwrap();
    ctx.orders("TQSIM")
        .buy_open("SHFE.rb2501", 1)
        .limit(100.0)
        .send_once("summary-daily-equity-order")
        .await
        .unwrap();
    ctx.finish_sim_step().unwrap();
    let _ctx = backtest.next().await.unwrap().unwrap();
    let _ctx = backtest.next().await.unwrap().unwrap();

    let summary = backtest.summary();
    let daily = summary.daily_equity_returns();
    assert_eq!(daily.len(), 2);
    assert_eq!(
        daily[0].date(),
        NaiveDate::from_ymd_opt(1970, 1, 3).unwrap()
    );
    assert_eq!(daily[0].equity(), 10_000_100.0);
    assert!((daily[0].return_rate() - 0.00001).abs() < 1e-12);
    assert_eq!(
        daily[1].date(),
        NaiveDate::from_ymd_opt(1970, 1, 4).unwrap()
    );
    assert_eq!(daily[1].equity(), 10_000_050.0);
    assert!((daily[1].return_rate() + 0.0000049999500005).abs() < 1e-15);
    assert!(summary.annualized_daily_sharpe_ratio().is_finite());
}

fn quote_event(
    symbol: &str,
    datetime: i64,
    ask_price1: f64,
    ask_volume1: i64,
    bid_price1: f64,
    bid_volume1: i64,
) -> ReplayMarketEvent {
    ReplayMarketEvent::quote(
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
) -> ReplayMarketEvent {
    ReplayMarketEvent::tick(
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
) -> ReplayMarketEvent {
    ReplayMarketEvent::kline(
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
