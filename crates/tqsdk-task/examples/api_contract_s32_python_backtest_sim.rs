//! Scenario: Python-compatible 本地回测模拟账户
//!
//! User goal:
//! - 不连接真实服务，用本地历史/缓存行情推进策略
//! - 使用 Python `TqSim` 风格的本地撮合账户，而不是 fake broker
//! - 同一套策略 context 读取 quote/account/position 并提交 typed order
//!
//! API contract:
//! - `StrategyBacktest` 消费 task-owned `ReplayMarketSource`
//! - `TqSim` 是本地模拟账户，不依赖 TQKQ provider-backed sim
//! - 限价单按 Python 语义撮合：穿过对手价时一次性全部成交，成交价为委托价
//! - 本地 replay quote/tick/kline event 都能进入最小回测闭环
//! - 市价类订单没有对手盘时撤单
//! - `summary()` 只提供轻量计数与最终账户/订单/成交/持仓快照，不承诺完整报告
//! - fake broker 仍是测试工具，不承担 Python-compatible 账户语义

use tqsdk_core::{Kline, Quote, Tick};
use tqsdk_task::{
    ReplayMarketEvent, ReplayMarketSource, StrategyBacktest, StrategyBacktestSummary, TqSim,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let symbol = "SHFE.rb2501";
    let replay = ReplayMarketSource::new(vec![
        ReplayMarketEvent::quote(
            "fixture",
            symbol,
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
        )?,
        ReplayMarketEvent::tick(
            "fixture",
            symbol,
            2_000,
            Some(2_000),
            Tick {
                datetime: 2_000,
                last_price: 98.0,
                ask_price1: 98.0,
                ask_volume1: 10,
                bid_price1: 97.0,
                bid_volume1: 8,
                volume: 100,
                open_interest: 50,
                ..Tick::default()
            },
        )?,
        ReplayMarketEvent::kline(
            "fixture",
            symbol,
            3_000,
            Some(3_000),
            60_000_000_000,
            Kline {
                id: 1,
                datetime: 3_000,
                open: 98.0,
                high: 104.0,
                low: 96.0,
                close: 102.0,
                volume: 1_000,
                close_oi: 55,
                ..Kline::default()
            },
        )?,
    ]);

    let mut backtest = StrategyBacktest::builder(replay)
        .sim(TqSim::new().with_margin(symbol, 1_000.0))
        .quote(symbol)
        .price_tick(symbol, 1.0)
        .build()
        .await?;

    while let Some(mut ctx) = backtest.next().await? {
        let quote = ctx.quote(symbol)?;
        let position = ctx.position("TQSIM", symbol)?;
        if position.pos_long == 0 {
            ctx.orders("TQSIM")
                .buy_open(symbol, 1)
                .limit(quote.ask_price1 + 1.0)
                .send_once("python-backtest-entry-1")
                .await?;
            let report = ctx.finish_sim_step()?;
            println!(
                "orders={} trades={} pos_long={}",
                report.orders().len(),
                report.trades().len(),
                ctx.position("TQSIM", symbol)?.pos_long
            );
        }
    }

    print_summary(backtest.summary());
    Ok(())
}

fn print_summary(summary: StrategyBacktestSummary) {
    println!(
        "events={} quote={} tick={} kline={} orders={} trades={} closed={} win_rate={} pl_ratio={} net_profit={} available={} positions={}",
        summary.event_count(),
        summary.quote_count(),
        summary.tick_count(),
        summary.kline_count(),
        summary.orders().len(),
        summary.trades().len(),
        summary.closed_profit_observation_count(),
        summary.winning_rate(),
        summary.profit_loss_ratio(),
        summary.net_realized_profit(),
        summary.final_account().available,
        summary.final_positions().len()
    );
}
