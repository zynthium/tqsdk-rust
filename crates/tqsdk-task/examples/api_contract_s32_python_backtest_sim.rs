//! Scenario: Python-compatible 本地回测模拟账户
//!
//! User goal:
//! - 不连接真实服务，用本地历史/缓存行情推进策略
//! - 使用 Python `TqSim` 风格的本地撮合账户，而不是 fake broker
//! - 同一套策略 context 读取 quote/account/position 并提交 typed order
//!
//! API contract:
//! - `StrategyBacktest` 消费 `MarketCacheReplay`
//! - `TqSim` 是本地模拟账户，不依赖 TQKQ provider-backed sim
//! - 限价单按 Python 语义撮合：穿过对手价时一次性全部成交，成交价为委托价
//! - 市价类订单没有对手盘时撤单
//! - fake broker 仍是测试工具，不承担 Python-compatible 账户语义

use tqsdk_core::Quote;
use tqsdk_data::{MarketCacheEvent, MarketCacheReplay};
use tqsdk_task::{StrategyBacktest, TqSim};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let symbol = "SHFE.rb2501";
    let replay = MarketCacheReplay::new(vec![MarketCacheEvent::quote(
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
    )?]);

    let mut backtest = StrategyBacktest::builder(replay)
        .sim(TqSim::new().with_margin(symbol, 1_000.0))
        .quote(symbol)
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

    Ok(())
}
