//! Scenario: 历史行情回放
//!
//! User goal:
//! - 历史行情按时间顺序驱动同一套策略逻辑
//! - quote / kline replay 进入标准 strategy context
//! - 回放策略可以复用 typed 下单与 fake broker 验证成交
//!
//! API contract:
//! - history/cache replay 是 public strategy replay driver，不是用户手写 runtime for-loop
//! - replay event 输出标准 quote/kline/tick 状态读取面
//! - 策略无需区分 live market event 和 replay market event 的状态读取 API
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 用户自己把历史 K线改造成 runtime DIFF JSON
//! - `ReplayCommand` 或 provider 内部 protocol type 泄漏到策略逻辑
//! - `serde_json::Value`
//! - 多套 event schema
//!
//! Regression signal:
//! - 历史/cache 回放不能复用实时策略的 `StrategyContext`
//! - 回放推进和策略状态读取各自维护 revision
//! - 用户需要自己处理排序、runtime ingest 或后台任务
//!
//! Review questions:
//! - 当前 API 是否自然表达历史回放驱动策略？
//! - 是否存在状态一致性风险？
//! - 剩余 history adapter / replay clock gap 是否被明确排除？

use std::time::Duration;

use tqsdk_core::{Kline, Quote};
use tqsdk_data::{MarketCacheEvent, MarketCacheReplay};
use tqsdk_task::StrategyReplay;
use tqsdk_task::testing::{FakeBroker, FakeMarket};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let symbol = "SHFE.au2602";
    let duration = Duration::from_secs(60);

    let quote = Quote {
        last_price: 480.5,
        ..Quote::default()
    };
    let kline = Kline {
        id: 1,
        datetime: 1_900,
        open: 480.0,
        high: 482.0,
        low: 479.0,
        close: 481.0,
        ..Kline::default()
    };

    let replay = MarketCacheReplay::new(vec![
        MarketCacheEvent::quote("cache", symbol, 1_000, Some(900), quote)?,
        MarketCacheEvent::kline(
            "cache",
            symbol,
            2_000,
            Some(1_900),
            60_000_000_000,
            kline,
        )?,
    ]);

    let mut strategy = StrategyReplay::builder(replay)
        .market(
            FakeMarket::new()
                .account("sim", 100_000.0)
                .position("sim", symbol, 0),
        )
        .broker(FakeBroker::new().fill_all())
        .account("sim")
        .quote(symbol)
        .kline(symbol, duration, 32)
        .build()
        .await?;

    while let Some(mut ctx) = strategy.next().await? {
        let event = ctx.event();
        println!(
            "replay source={} symbol={} event_time_ns={}",
            event.source(),
            event.symbol(),
            event.event_time_ns()
        );

        let last_price = ctx.quote(symbol)?.last_price;
        let last_close = ctx.kline(symbol, duration)?.last().map(|row| row.close);
        let position = ctx.position("sim", symbol)?;

        if matches!(last_close, Some(close) if close > last_price) && position.pos_long == 0 {
            ctx.orders("sim")
                .buy_open(symbol, 1)
                .limit(last_price)
                .send_once("history-replay-entry-1")
                .await?;

            let report = ctx.finish_test_step().await?;
            println!(
                "orders={} trades={} pos_long={}",
                report.orders().len(),
                report.trades().len(),
                report.position("sim", symbol)?.pos_long
            );
        }
    }

    Ok(())
}
