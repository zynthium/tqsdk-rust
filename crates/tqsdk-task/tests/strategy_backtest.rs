use chrono::NaiveDate;
use tqsdk_core::{Kline, Quote, Symbol, Tick};
use tqsdk_session::{InstrumentClass, InstrumentSpec};
use tqsdk_task::TaskError;
use tqsdk_task::backtest::{StrategyBacktest, StrategyBacktestDailyReturnWindow};
use tqsdk_task::replay::{ReplayMarketEvent, ReplayMarketSource};
use tqsdk_task::sim::TqSim;
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

#[test]
fn tqsim_exposes_margin_and_commission_configuration() {
    let mut sim = TqSim::new()
        .with_margin("SHFE.rb2501", 1_000.0)
        .with_commission("SHFE.rb2501", 3.5);

    assert_eq!(sim.margin("SHFE.rb2501"), 1_000.0);
    assert_eq!(sim.commission("SHFE.rb2501"), 3.5);
    assert_eq!(sim.margin("DCE.i2501"), 0.0);
    assert_eq!(sim.commission("DCE.i2501"), 0.0);

    sim.apply_quote_metadata(
        "SHFE.rb2501",
        &Quote {
            margin: 2_000.0,
            commission: 7.0,
            volume_multiple: 20,
            ..Quote::default()
        },
    );
    assert_eq!(sim.margin("SHFE.rb2501"), 1_000.0);
    assert_eq!(sim.commission("SHFE.rb2501"), 3.5);

    sim.apply_quote_metadata(
        "DCE.i2501",
        &Quote {
            margin: 2_000.0,
            commission: 7.0,
            volume_multiple: 20,
            ..Quote::default()
        },
    );
    assert_eq!(sim.margin("DCE.i2501"), 2_000.0);
    assert_eq!(sim.commission("DCE.i2501"), 7.0);
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
async fn strategy_backtest_stamps_orders_and_trades_with_replay_event_time() {
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
    ctx.orders("TQSIM")
        .buy_open("SHFE.rb2501", 1)
        .limit(99.0)
        .send_once("timed-order")
        .await
        .unwrap();
    let report = ctx.finish_sim_step().unwrap();
    assert_eq!(report.orders()[0].insert_date_time, 1_000);
    assert!(report.trades().is_empty());

    let ctx = backtest.next().await.unwrap().unwrap();
    let order = ctx.sim().orders().into_iter().next().unwrap();
    let trade = ctx.sim().trades().into_iter().next().unwrap();
    assert_eq!(order.insert_date_time, 1_000);
    assert_eq!(trade.trade_date_time, 2_000);
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
async fn strategy_backtest_executes_main_continuous_order_on_replay_underlying_symbol() {
    let alias = "KQ.m@SHFE.rb";
    let underlying = "SHFE.rb2501";
    let replay = ReplayMarketSource::new(vec![
        quote_event(alias, 1_000, 100.0, 10, 99.0, 8)
            .with_underlying_symbol(underlying)
            .unwrap(),
    ]);

    let mut backtest = StrategyBacktest::builder(replay)
        .sim(TqSim::new().with_margin(underlying, 1_000.0))
        .quote(alias)
        .build()
        .await
        .unwrap();

    let mut ctx = backtest.next().await.unwrap().unwrap();
    ctx.orders("TQSIM")
        .buy_open(alias, 2)
        .limit(101.0)
        .send_once("main-continuous-order")
        .await
        .unwrap();
    let report = ctx.finish_sim_step().unwrap();

    assert_eq!(report.trades().len(), 1);
    assert_eq!(report.trades()[0].exchange_id, "SHFE");
    assert_eq!(report.trades()[0].instrument_id, "rb2501");
    assert_eq!(ctx.position("TQSIM", underlying).unwrap().pos_long, 2);
    assert_eq!(ctx.position("TQSIM", alias).unwrap().pos_long, 2);

    let summary = backtest.summary();
    assert_eq!(summary.trades()[0].exchange_id, "SHFE");
    assert_eq!(summary.trades()[0].instrument_id, "rb2501");
    assert!(
        summary
            .final_positions()
            .iter()
            .any(|position| position.exchange_id == "SHFE"
                && position.instrument_id == "rb2501"
                && position.pos_long == 2)
    );
    assert!(
        summary
            .final_positions()
            .iter()
            .any(|position| position.exchange_id == "KQ"
                && position.instrument_id == "m@SHFE.rb"
                && position.pos_long == 2)
    );
}

#[tokio::test]
async fn strategy_backtest_fills_pending_main_continuous_order_on_later_underlying_quote() {
    let alias = "KQ.m@SHFE.rb";
    let underlying = "SHFE.rb2501";
    let replay = ReplayMarketSource::new(vec![
        quote_event(alias, 1_000, 100.0, 10, 99.0, 8)
            .with_underlying_symbol(underlying)
            .unwrap(),
        quote_event(alias, 2_000, 98.0, 10, 97.0, 8)
            .with_underlying_symbol(underlying)
            .unwrap(),
    ]);

    let mut backtest = StrategyBacktest::builder(replay)
        .sim(TqSim::new())
        .quote(alias)
        .build()
        .await
        .unwrap();

    let mut ctx = backtest.next().await.unwrap().unwrap();
    let ticket = ctx
        .orders("TQSIM")
        .buy_open(alias, 1)
        .limit(99.0)
        .send_once("main-continuous-pending-order")
        .await
        .unwrap();
    let report = ctx.finish_sim_step().unwrap();
    assert_eq!(report.orders()[0].exchange_id, "SHFE");
    assert_eq!(report.orders()[0].instrument_id, "rb2501");
    assert_eq!(report.orders()[0].status, "ALIVE");
    assert!(report.trades().is_empty());

    let ctx = backtest.next().await.unwrap().unwrap();
    assert_eq!(ctx.position("TQSIM", underlying).unwrap().pos_long, 1);
    assert_eq!(ctx.position("TQSIM", alias).unwrap().pos_long, 1);
    assert!(matches!(
        ticket.status(ctx.task_host().api()).unwrap(),
        OrderTicketState::Filled { .. }
    ));
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
async fn strategy_backtest_uses_instrument_spec_for_kline_metadata() {
    let replay = ReplayMarketSource::new(vec![kline_event(
        "SHFE.rb2501",
        1_000,
        100.0,
        105.0,
        99.0,
        102.0,
    )]);

    let mut backtest = StrategyBacktest::builder(replay)
        .sim(TqSim::new())
        .instrument_spec(instrument_spec("SHFE.rb2501", 0.5, 10))
        .build()
        .await
        .unwrap();

    let mut ctx = backtest.next().await.unwrap().unwrap();
    let quote = ctx.quote("SHFE.rb2501").unwrap();
    assert_eq!(quote.ask_price1, 102.5);
    assert_eq!(quote.bid_price1, 101.5);
    ctx.orders("TQSIM")
        .buy_open("SHFE.rb2501", 1)
        .limit(102.5)
        .send_once("instrument-spec-open")
        .await
        .unwrap();
    ctx.finish_sim_step().unwrap();
    let position = ctx.sim().position("SHFE.rb2501");
    assert_eq!(position.open_cost_long, 1_025.0);
    assert_eq!(position.market_value_long, 1_020.0);
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
    assert_eq!(summary.buy_trade_count(), 2);
    assert_eq!(summary.sell_trade_count(), 2);
    assert_eq!(summary.open_trade_count(), 2);
    assert_eq!(summary.close_trade_count(), 2);
    assert_eq!(summary.closed_profit_observation_count(), 2);
    assert_eq!(summary.winning_closed_profit_observation_count(), 1);
    assert_eq!(summary.losing_closed_profit_observation_count(), 1);
    assert_eq!(summary.gross_profit(), 100.0);
    assert_eq!(summary.gross_loss(), 50.0);
    assert_eq!(summary.profit_loss_ratio(), 2.0);
    assert_eq!(summary.winning_rate(), 0.5);

    let metrics = summary.performance_metrics();
    assert_eq!(
        metrics.start_date_utc(),
        Some(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
    );
    assert_eq!(
        metrics.end_date_utc(),
        Some(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
    );
    assert_eq!(metrics.start_balance(), 10_000_000.0);
    assert_eq!(metrics.end_balance(), 10_000_046.0);
    assert_eq!(metrics.balance_change(), 46.0);
    assert_nan_or_close(metrics.balance_return_rate(), summary.balance_return_rate());
    assert_eq!(
        metrics.balance_trading_day_count(),
        summary.balance_trading_day_count()
    );
    assert_eq!(
        metrics.profitable_balance_day_count(),
        summary.profitable_balance_day_count()
    );
    assert_eq!(
        metrics.losing_balance_day_count(),
        summary.losing_balance_day_count()
    );
    assert_eq!(metrics.open_trade_count(), 2);
    assert_eq!(metrics.close_trade_count(), 2);
    assert_eq!(metrics.total_commission(), 4.0);
    assert_eq!(metrics.realized_profit(), 50.0);
    assert_eq!(metrics.net_realized_profit(), 46.0);
    assert_nan_or_close(metrics.average_risk_ratio(), summary.average_risk_ratio());
    assert_eq!(metrics.winning_rate(), 0.5);
    assert_eq!(metrics.profit_loss_ratio(), 2.0);
    assert_eq!(
        metrics.max_balance_drawdown(),
        summary.max_balance_drawdown()
    );
    assert_eq!(
        metrics.max_balance_drawdown_rate(),
        summary.max_balance_drawdown_rate()
    );
    assert_nan_or_close(
        metrics.annualized_balance_return_rate(),
        summary.annualized_balance_return_rate(),
    );
    assert_nan_or_close(
        metrics.annualized_daily_balance_sharpe_ratio(),
        summary.annualized_daily_balance_sharpe_ratio(),
    );
    assert_nan_or_close(
        metrics.annualized_daily_balance_sortino_ratio(),
        summary.annualized_daily_balance_sortino_ratio(),
    );
    assert_nan_or_close(
        metrics.annualized_daily_balance_calmar_ratio(),
        summary.annualized_daily_balance_calmar_ratio(),
    );
}

#[tokio::test]
async fn strategy_backtest_summary_counts_break_even_close_observations() {
    let replay = ReplayMarketSource::new(vec![
        quote_event("SHFE.rb2501", 1_000, 100.0, 10, 100.0, 8),
        quote_event("SHFE.rb2501", 2_000, 100.0, 10, 100.0, 8),
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
        .send_once("closed-profit-open-even")
        .await
        .unwrap();
    ctx.finish_sim_step().unwrap();

    let mut ctx = backtest.next().await.unwrap().unwrap();
    ctx.orders("TQSIM")
        .sell_close("SHFE.rb2501", 1)
        .limit(100.0)
        .send_once("closed-profit-close-even")
        .await
        .unwrap();
    ctx.finish_sim_step().unwrap();

    let summary = backtest.summary();
    assert_eq!(summary.realized_profit(), 0.0);
    assert_eq!(summary.closed_profit_points().len(), 1);
    assert_eq!(summary.closed_profit_points()[0].event_count(), 2);
    assert_eq!(summary.closed_profit_points()[0].trade_count(), 1);
    assert_eq!(summary.closed_profit_points()[0].profit(), 0.0);
    assert_eq!(summary.closed_trade_count(), 1);
    assert_eq!(summary.buy_trade_count(), 1);
    assert_eq!(summary.sell_trade_count(), 1);
    assert_eq!(summary.open_trade_count(), 1);
    assert_eq!(summary.close_trade_count(), 1);
    assert_eq!(summary.closed_profit_observation_count(), 1);
    assert_eq!(summary.winning_closed_profit_observation_count(), 1);
    assert_eq!(summary.losing_closed_profit_observation_count(), 0);
    assert_eq!(summary.winning_rate(), 1.0);
    assert_eq!(summary.gross_profit(), 0.0);
    assert_eq!(summary.gross_loss(), 0.0);
    assert!(summary.profit_loss_ratio().is_nan());
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
    assert_eq!(summary.balance_points().len(), 3);
    assert_eq!(summary.balance_points()[0].event_count(), 0);
    assert_eq!(summary.balance_points()[0].balance(), 10_000_000.0);
    assert_eq!(summary.balance_points()[1].event_count(), 1);
    assert_eq!(summary.balance_points()[1].event_time_ns(), Some(1_000));
    assert_eq!(summary.balance_points()[1].balance(), 10_000_000.0);
    assert_eq!(summary.balance_points()[2].event_count(), 1);
    assert_eq!(summary.balance_points()[2].event_time_ns(), Some(1_000));
    assert_eq!(summary.balance_points()[2].balance(), 9_999_987.5);
    assert_eq!(summary.peak_balance(), 10_000_000.0);
    assert_eq!(summary.max_balance_drawdown(), 12.5);
    assert_eq!(summary.balance_change(), -12.5);
    assert!((summary.balance_return_rate() + 0.00000125).abs() < 1e-12);
    assert!((summary.max_balance_drawdown_rate() - 0.00000125).abs() < 1e-12);
    assert_eq!(summary.balance_points()[1].return_rate(), 0.0);
    assert!((summary.balance_points()[2].return_rate() + 0.00000125).abs() < 1e-12);
    assert!((summary.balance_points()[2].drawdown_rate() - 0.00000125).abs() < 1e-12);

    let daily_balance = summary.daily_balance_returns();
    assert_eq!(daily_balance.len(), 1);
    assert_eq!(
        daily_balance[0].date(),
        NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
    );
    assert_eq!(daily_balance[0].balance(), 9_999_987.5);
    assert_eq!(daily_balance[0].profit(), -12.5);
    assert_eq!(daily_balance[0].drawdown(), 12.5);
    assert!((daily_balance[0].drawdown_rate() - 0.00000125).abs() < 1e-12);
    assert!((daily_balance[0].return_rate() + 0.00000125).abs() < 1e-12);
    let expected_annualized = (1.0_f64 - 0.00000125).powf(250.0) - 1.0;
    assert!((summary.annualized_balance_return_rate() - expected_annualized).abs() < 1e-12);
    assert!(summary.annualized_daily_balance_sharpe_ratio().is_nan());
    assert!(summary.annualized_daily_balance_sortino_ratio().is_nan());
    assert!(summary.annualized_daily_balance_calmar_ratio().is_finite());
}

#[tokio::test]
async fn strategy_backtest_summary_tracks_event_date_range() {
    let replay = ReplayMarketSource::new(vec![
        quote_event("SHFE.rb2501", 86_400_000_000_000, 100.0, 10, 99.0, 8),
        quote_event("SHFE.rb2501", 172_800_000_000_000, 101.0, 10, 100.0, 8),
    ]);

    let mut backtest = StrategyBacktest::builder(replay)
        .sim(TqSim::new())
        .build()
        .await
        .unwrap();

    while backtest.next().await.unwrap().is_some() {}

    let summary = backtest.summary();
    assert_eq!(summary.start_event_time_ns(), Some(86_400_000_000_000));
    assert_eq!(summary.end_event_time_ns(), Some(172_800_000_000_000));
    assert_eq!(
        summary.start_event_date_utc(),
        Some(NaiveDate::from_ymd_opt(1970, 1, 2).unwrap())
    );
    assert_eq!(
        summary.end_event_date_utc(),
        Some(NaiveDate::from_ymd_opt(1970, 1, 3).unwrap())
    );
}

#[tokio::test]
async fn strategy_backtest_summary_tracks_average_risk_ratio() {
    let replay = ReplayMarketSource::new(vec![
        quote_event("SHFE.rb2501", 86_400_000_000_000, 100.0, 10, 99.0, 8),
        quote_event("SHFE.rb2501", 172_800_000_000_000, 101.0, 10, 100.0, 8),
    ]);

    let mut backtest = StrategyBacktest::builder(replay)
        .sim(TqSim::new().with_margin("SHFE.rb2501", 1_000.0))
        .build()
        .await
        .unwrap();

    let mut ctx = backtest.next().await.unwrap().unwrap();
    ctx.orders("TQSIM")
        .buy_open("SHFE.rb2501", 1)
        .limit(100.0)
        .send_once("risk-ratio-open")
        .await
        .unwrap();
    ctx.finish_sim_step().unwrap();
    let _ctx = backtest.next().await.unwrap().unwrap();

    let summary = backtest.summary();
    assert_eq!(summary.risk_ratio_points().len(), 4);
    assert_eq!(summary.risk_ratio_points()[0].risk_ratio(), 0.0);
    assert_eq!(
        summary.risk_ratio_points()[1].event_time_ns(),
        Some(86_400_000_000_000)
    );
    assert_eq!(summary.risk_ratio_points()[1].risk_ratio(), 0.0);
    assert_eq!(
        summary.risk_ratio_points()[2].event_time_ns(),
        Some(86_400_000_000_000)
    );
    assert_eq!(summary.risk_ratio_points()[2].risk_ratio(), 0.0001);
    assert_eq!(
        summary.risk_ratio_points()[3].event_time_ns(),
        Some(172_800_000_000_000)
    );
    assert_eq!(summary.risk_ratio_points()[3].risk_ratio(), 0.0001);
    assert!((summary.average_risk_ratio() - 0.00005).abs() < 1e-12);
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
    assert_eq!(summary.equity_points().len(), 3);
    assert_eq!(summary.equity_points()[0].event_count(), 0);
    assert_eq!(summary.equity_points()[0].event_time_ns(), None);
    assert_eq!(summary.equity_points()[0].equity(), 10_000_000.0);
    assert_eq!(summary.equity_points()[1].event_count(), 1);
    assert_eq!(
        summary.equity_points()[1].event_time_ns(),
        Some(86_400_000_000_000)
    );
    assert_eq!(summary.equity_points()[1].equity(), 10_000_000.0);
    assert_eq!(summary.equity_points()[2].event_count(), 2);
    assert_eq!(
        summary.equity_points()[2].event_time_ns(),
        Some(172_800_000_000_000)
    );
    assert_eq!(summary.equity_points()[2].equity(), 10_000_100.0);
    assert!((summary.equity_return_rate() - 0.00001).abs() < 1e-12);
    assert_eq!(summary.equity_points()[1].return_rate(), 0.0);
    assert!((summary.equity_points()[2].return_rate() - 0.00001).abs() < 1e-12);
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
    assert_eq!(daily.len(), 3);
    assert_eq!(
        daily[0].date(),
        NaiveDate::from_ymd_opt(1970, 1, 2).unwrap()
    );
    assert_eq!(daily[0].equity(), 10_000_000.0);
    assert_eq!(daily[0].profit(), 0.0);
    assert_eq!(daily[0].drawdown(), 0.0);
    assert_eq!(daily[0].drawdown_rate(), 0.0);
    assert_eq!(daily[0].return_rate(), 0.0);
    assert_eq!(
        daily[1].date(),
        NaiveDate::from_ymd_opt(1970, 1, 3).unwrap()
    );
    assert_eq!(daily[1].equity(), 10_000_100.0);
    assert_eq!(daily[1].profit(), 100.0);
    assert_eq!(daily[1].drawdown(), 0.0);
    assert_eq!(daily[1].drawdown_rate(), 0.0);
    assert!((daily[1].return_rate() - 0.00001).abs() < 1e-12);
    assert_eq!(
        daily[2].date(),
        NaiveDate::from_ymd_opt(1970, 1, 4).unwrap()
    );
    assert_eq!(daily[2].equity(), 10_000_050.0);
    assert_eq!(daily[2].profit(), -50.0);
    assert_eq!(daily[2].drawdown(), 50.0);
    assert!((daily[2].drawdown_rate() - 0.0000049999500005).abs() < 1e-15);
    assert!((daily[2].return_rate() + 0.0000049999500005).abs() < 1e-15);
    let windows = [
        StrategyBacktestDailyReturnWindow::new(
            NaiveDate::from_ymd_opt(2026, 5, 15).unwrap(),
            0,
            100_000_000_000_000,
        ),
        StrategyBacktestDailyReturnWindow::new(
            NaiveDate::from_ymd_opt(2026, 5, 18).unwrap(),
            100_000_000_000_000,
            200_000_000_000_000,
        ),
        StrategyBacktestDailyReturnWindow::new(
            NaiveDate::from_ymd_opt(2026, 5, 19).unwrap(),
            200_000_000_000_000,
            300_000_000_000_000,
        ),
        StrategyBacktestDailyReturnWindow::new(
            NaiveDate::from_ymd_opt(2026, 5, 20).unwrap(),
            300_000_000_000_000,
            400_000_000_000_000,
        ),
        StrategyBacktestDailyReturnWindow::new(
            NaiveDate::from_ymd_opt(2026, 5, 21).unwrap(),
            400_000_000_000_000,
            400_000_000_000_000,
        ),
    ];
    let windowed_equity = summary.daily_equity_returns_for_windows(&windows);
    assert_eq!(windowed_equity.len(), 4);
    assert_eq!(
        windowed_equity[0].date(),
        NaiveDate::from_ymd_opt(2026, 5, 15).unwrap()
    );
    assert_eq!(windowed_equity[0].equity(), 10_000_000.0);
    assert_eq!(windowed_equity[0].profit(), 0.0);
    assert_eq!(windowed_equity[0].drawdown(), 0.0);
    assert_eq!(windowed_equity[0].drawdown_rate(), 0.0);
    assert_eq!(windowed_equity[0].return_rate(), 0.0);
    assert_eq!(
        windowed_equity[1].date(),
        NaiveDate::from_ymd_opt(2026, 5, 18).unwrap()
    );
    assert_eq!(windowed_equity[1].equity(), 10_000_100.0);
    assert_eq!(windowed_equity[1].profit(), 100.0);
    assert_eq!(windowed_equity[1].drawdown(), 0.0);
    assert_eq!(windowed_equity[1].drawdown_rate(), 0.0);
    assert!((windowed_equity[1].return_rate() - 0.00001).abs() < 1e-12);
    assert_eq!(
        windowed_equity[2].date(),
        NaiveDate::from_ymd_opt(2026, 5, 19).unwrap()
    );
    assert_eq!(windowed_equity[2].equity(), 10_000_050.0);
    assert_eq!(windowed_equity[2].profit(), -50.0);
    assert_eq!(windowed_equity[2].drawdown(), 50.0);
    assert!((windowed_equity[2].drawdown_rate() - 0.0000049999500005).abs() < 1e-15);
    assert!((windowed_equity[2].return_rate() + 0.0000049999500005).abs() < 1e-15);
    assert_eq!(
        windowed_equity[3].date(),
        NaiveDate::from_ymd_opt(2026, 5, 20).unwrap()
    );
    assert_eq!(windowed_equity[3].equity(), 10_000_050.0);
    assert_eq!(windowed_equity[3].return_rate(), 0.0);
    let windowed_balance = summary.daily_balance_returns_for_windows(&windows);
    assert_eq!(windowed_balance.len(), 4);
    assert_eq!(windowed_balance[0].balance(), 10_000_000.0);
    assert_eq!(windowed_balance[3].balance(), 10_000_000.0);
    assert!(
        windowed_balance
            .iter()
            .all(|daily| daily.return_rate() == 0.0)
    );
    assert_eq!(summary.equity_trading_day_count(), 3);
    assert_eq!(summary.profitable_equity_day_count(), 1);
    assert_eq!(summary.losing_equity_day_count(), 1);
    assert_eq!(summary.max_consecutive_profitable_equity_days(), 1);
    assert_eq!(summary.max_consecutive_losing_equity_days(), 1);
    assert_eq!(summary.balance_trading_day_count(), 3);
    assert_eq!(summary.profitable_balance_day_count(), 0);
    assert_eq!(summary.losing_balance_day_count(), 0);
    assert_eq!(summary.max_consecutive_profitable_balance_days(), 0);
    assert_eq!(summary.max_consecutive_losing_balance_days(), 0);
    assert!(summary.annualized_daily_sharpe_ratio().is_finite());
    assert_eq!(
        summary.annualized_daily_sharpe_ratio(),
        summary.annualized_daily_equity_sharpe_ratio()
    );
    let expected_annualized = (1.0_f64 + 0.000005).powf(250.0 / 3.0) - 1.0;
    assert!((summary.annualized_equity_return_rate() - expected_annualized).abs() < 1e-12);
    assert!(summary.annualized_daily_equity_sortino_ratio().is_finite());
    assert!(summary.annualized_daily_equity_calmar_ratio().is_finite());

    let risk_free_rate = 0.025;
    let sharpe_with_risk_free =
        summary.annualized_daily_equity_sharpe_ratio_with_risk_free_rate(risk_free_rate);
    assert!(sharpe_with_risk_free.is_finite());
    assert!(sharpe_with_risk_free < summary.annualized_daily_equity_sharpe_ratio());
    assert_eq!(
        summary.annualized_daily_sharpe_ratio_with_risk_free_rate(risk_free_rate),
        sharpe_with_risk_free
    );
    assert!(
        summary
            .annualized_daily_equity_sortino_ratio_with_risk_free_rate(risk_free_rate)
            .is_finite()
    );
    assert!(
        summary
            .annualized_daily_equity_calmar_ratio_with_risk_free_rate(risk_free_rate)
            .is_finite()
    );
    assert!(
        summary.annualized_daily_equity_calmar_ratio_with_risk_free_rate(risk_free_rate)
            < summary.annualized_daily_equity_calmar_ratio()
    );
}

#[tokio::test]
async fn strategy_backtest_summary_derives_rolling_daily_equity_ratios() {
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
        .send_once("summary-rolling-equity-order")
        .await
        .unwrap();
    ctx.finish_sim_step().unwrap();
    let _ctx = backtest.next().await.unwrap().unwrap();
    let _ctx = backtest.next().await.unwrap().unwrap();

    let summary = backtest.summary();
    let daily = summary.daily_equity_returns();
    let returns = [
        daily[0].return_rate(),
        daily[1].return_rate(),
        daily[2].return_rate(),
    ];
    let drawdowns = [
        daily[0].drawdown_rate(),
        daily[1].drawdown_rate(),
        daily[2].drawdown_rate(),
    ];

    let sharpe = summary.rolling_daily_equity_sharpe_ratios(2);
    assert_eq!(sharpe.len(), 3);
    assert_eq!(
        sharpe[0].date(),
        NaiveDate::from_ymd_opt(1970, 1, 2).unwrap()
    );
    assert_eq!(sharpe[0].sample_count(), 1);
    assert!(sharpe[0].ratio().is_nan());
    assert_eq!(sharpe[1].sample_count(), 2);
    assert!((sharpe[1].ratio() - expected_annualized_sharpe(&returns[0..2])).abs() < 1e-12);
    assert!((sharpe[2].ratio() - expected_annualized_sharpe(&returns[1..3])).abs() < 1e-12);

    let sortino = summary.rolling_daily_equity_sortino_ratios(2);
    assert_eq!(sortino.len(), 3);
    assert!(sortino[0].ratio().is_nan());
    assert!(sortino[1].ratio().is_nan());
    assert!((sortino[2].ratio() - expected_annualized_sortino(&returns[1..3])).abs() < 1e-12);

    let calmar = summary.rolling_daily_equity_calmar_ratios(2);
    assert_eq!(calmar.len(), 3);
    assert!(calmar[0].ratio().is_nan());
    assert!(calmar[1].ratio().is_nan());
    assert!(
        (calmar[2].ratio() - expected_annualized_calmar(&returns[1..3], &drawdowns[1..3])).abs()
            < 1e-12
    );

    assert!(summary.rolling_daily_equity_sharpe_ratios(0).is_empty());
    assert!(summary.rolling_daily_equity_sortino_ratios(0).is_empty());
    assert!(summary.rolling_daily_equity_calmar_ratios(0).is_empty());
}

#[tokio::test]
async fn strategy_backtest_summary_derives_rolling_daily_balance_ratios() {
    let replay = ReplayMarketSource::new(vec![
        quote_event("SHFE.rb2501", 86_400_000_000_000, 100.0, 10, 99.0, 8),
        quote_event("SHFE.rb2501", 172_800_000_000_000, 111.0, 10, 110.0, 8),
        quote_event("SHFE.rb2501", 259_200_000_000_000, 95.0, 10, 94.0, 8),
        quote_event("SHFE.rb2501", 345_600_000_000_000, 91.0, 10, 90.0, 8),
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
        .send_once("summary-rolling-balance-open-win")
        .await
        .unwrap();
    ctx.finish_sim_step().unwrap();

    let mut ctx = backtest.next().await.unwrap().unwrap();
    ctx.orders("TQSIM")
        .sell_close("SHFE.rb2501", 1)
        .limit(110.0)
        .send_once("summary-rolling-balance-close-win")
        .await
        .unwrap();
    ctx.finish_sim_step().unwrap();

    let mut ctx = backtest.next().await.unwrap().unwrap();
    ctx.orders("TQSIM")
        .buy_open("SHFE.rb2501", 1)
        .limit(95.0)
        .send_once("summary-rolling-balance-open-loss")
        .await
        .unwrap();
    ctx.finish_sim_step().unwrap();

    let mut ctx = backtest.next().await.unwrap().unwrap();
    ctx.orders("TQSIM")
        .sell_close("SHFE.rb2501", 1)
        .limit(90.0)
        .send_once("summary-rolling-balance-close-loss")
        .await
        .unwrap();
    ctx.finish_sim_step().unwrap();

    let summary = backtest.summary();
    let daily = summary.daily_balance_returns();
    assert_eq!(daily.len(), 4);
    let returns = daily
        .iter()
        .map(|day| day.return_rate())
        .collect::<Vec<_>>();
    let drawdowns = daily
        .iter()
        .map(|day| day.drawdown_rate())
        .collect::<Vec<_>>();

    let sharpe = summary.rolling_daily_balance_sharpe_ratios(2);
    let sortino = summary.rolling_daily_balance_sortino_ratios(2);
    let calmar = summary.rolling_daily_balance_calmar_ratios(2);
    assert_eq!(sharpe.len(), daily.len());
    assert_eq!(sortino.len(), daily.len());
    assert_eq!(calmar.len(), daily.len());
    assert!(sharpe[0].ratio().is_nan());
    assert!(sortino[0].ratio().is_nan());
    assert!(calmar[0].ratio().is_nan());
    for index in 1..daily.len() {
        let start = index + 1 - 2;
        assert_eq!(sharpe[index].date(), daily[index].date());
        assert_eq!(sharpe[index].sample_count(), 2);
        assert_nan_or_close(
            sharpe[index].ratio(),
            expected_annualized_sharpe(&returns[start..=index]),
        );
        assert_nan_or_close(
            sortino[index].ratio(),
            expected_annualized_sortino(&returns[start..=index]),
        );
        assert_nan_or_close(
            calmar[index].ratio(),
            expected_annualized_calmar(&returns[start..=index], &drawdowns[start..=index]),
        );
    }

    assert!(summary.rolling_daily_balance_sharpe_ratios(0).is_empty());
    assert!(summary.rolling_daily_balance_sortino_ratios(0).is_empty());
    assert!(summary.rolling_daily_balance_calmar_ratios(0).is_empty());

    let report = summary.performance_report(2);
    assert_eq!(report.metrics(), &summary.performance_metrics());
    assert_eq!(report.daily_balance_returns(), daily.as_slice());
    let daily_equity = summary.daily_equity_returns();
    assert_eq!(report.daily_equity_returns(), daily_equity.as_slice());
    assert_rolling_points_match(
        report.rolling_balance_sharpe_ratios(),
        summary.rolling_daily_balance_sharpe_ratios(2).as_slice(),
    );
    assert_rolling_points_match(
        report.rolling_balance_sortino_ratios(),
        summary.rolling_daily_balance_sortino_ratios(2).as_slice(),
    );
    assert_rolling_points_match(
        report.rolling_balance_calmar_ratios(),
        summary.rolling_daily_balance_calmar_ratios(2).as_slice(),
    );
    assert_rolling_points_match(
        report.rolling_equity_sharpe_ratios(),
        summary.rolling_daily_equity_sharpe_ratios(2).as_slice(),
    );
    assert_rolling_points_match(
        report.rolling_equity_sortino_ratios(),
        summary.rolling_daily_equity_sortino_ratios(2).as_slice(),
    );
    assert_rolling_points_match(
        report.rolling_equity_calmar_ratios(),
        summary.rolling_daily_equity_calmar_ratios(2).as_slice(),
    );
}

fn assert_rolling_points_match(
    actual: &[tqsdk_task::backtest::StrategyBacktestRollingRatioPoint],
    expected: &[tqsdk_task::backtest::StrategyBacktestRollingRatioPoint],
) {
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        assert_eq!(actual.date(), expected.date());
        assert_eq!(actual.sample_count(), expected.sample_count());
        assert_nan_or_close(actual.ratio(), expected.ratio());
    }
}

fn assert_nan_or_close(actual: f64, expected: f64) {
    if expected.is_nan() {
        assert!(actual.is_nan(), "expected NaN, got {actual}");
    } else {
        assert!(
            (actual - expected).abs() < 1e-12,
            "expected {expected}, got {actual}"
        );
    }
}

fn expected_annualized_sharpe(returns: &[f64]) -> f64 {
    if returns.len() < 2 {
        return f64::NAN;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance = returns
        .iter()
        .map(|return_rate| {
            let diff = return_rate - mean;
            diff * diff
        })
        .sum::<f64>()
        / (returns.len() - 1) as f64;
    let std_dev = variance.sqrt();
    if std_dev == 0.0 {
        f64::NAN
    } else {
        mean / std_dev * 250.0_f64.sqrt()
    }
}

fn expected_annualized_sortino(returns: &[f64]) -> f64 {
    if returns.len() < 2 {
        return f64::NAN;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let downside_variance = returns
        .iter()
        .filter(|return_rate| **return_rate < 0.0)
        .map(|return_rate| return_rate * return_rate)
        .sum::<f64>()
        / returns.len() as f64;
    let downside_dev = downside_variance.sqrt();
    if downside_dev == 0.0 {
        f64::NAN
    } else {
        mean / downside_dev * 250.0_f64.sqrt()
    }
}

fn expected_annualized_calmar(returns: &[f64], drawdown_rates: &[f64]) -> f64 {
    if returns.is_empty() {
        return f64::NAN;
    }
    let total_return = returns
        .iter()
        .fold(1.0, |growth, return_rate| growth * (1.0 + return_rate))
        - 1.0;
    let annualized_return = (1.0 + total_return).powf(250.0 / returns.len() as f64) - 1.0;
    let max_drawdown = drawdown_rates.iter().copied().fold(0.0_f64, f64::max);
    if max_drawdown <= 0.0 {
        f64::NAN
    } else {
        annualized_return / max_drawdown
    }
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

fn instrument_spec(symbol: &str, price_tick: f64, volume_multiple: i64) -> InstrumentSpec {
    let (exchange_id, product_id) =
        symbol
            .split_once('.')
            .map_or(("", symbol), |(exchange, instrument)| {
                let product_len = instrument
                    .chars()
                    .take_while(|ch| ch.is_ascii_alphabetic())
                    .count();
                (exchange, &instrument[..product_len])
            });
    InstrumentSpec {
        symbol: Symbol::new(symbol),
        exchange_id: exchange_id.to_string(),
        product_id: product_id.to_string(),
        class: InstrumentClass::Future,
        price_tick,
        volume_multiple,
        expire_datetime_secs: None,
        underlying_symbol: None,
    }
}
