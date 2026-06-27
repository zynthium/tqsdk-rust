#![cfg_attr(not(test), forbid(unsafe_code))]
//! Scenario 38: Local offline backtest through the `tqsdk` facade.
//!
//! Demonstrates the usage of `.replay_backtest()` on the TqBuilder.
//! The exact same `while tq.next()` and `quote.load()` strategy code runs
//! against an explicit local replay source without connecting to servers.

use tqsdk::advanced::task::replay::ReplayMarketSource;
use tqsdk::prelude::*;

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    // Usually you'd build this from downloaded history rows or explicit replay events,
    // but here we just use an empty replay to prove it compiles and runs.
    let replay = ReplayMarketSource::new(vec![]);

    let mut tq = Tq::new()
        // No auth_env() or connect to server needed for local backtest
        .replay_backtest(replay)
        .quote_symbol("SHFE.au2510")
        .price_tick("SHFE.au2510", 0.02)
        .connect()
        .await?;

    let quote = tq.quote("SHFE.au2510").await?;

    // This loop runs purely offline driven by the replay cache
    while tq.next().await? {
        let snapshot = quote.load()?;
        println!("{} last_price={}", snapshot.datetime, snapshot.last_price);
    }

    // You can also inspect the summary and typed metrics of the backtest afterwards
    if let Some(summary) = tq.backtest_summary() {
        println!(
            "Backtest finished! Executed {} events.",
            summary.event_count()
        );
    }
    if let Some(metrics) = tq.backtest_performance_metrics() {
        println!(
            "Backtest return={} max_drawdown={}",
            metrics.balance_return_rate(),
            metrics.max_balance_drawdown_rate()
        );
    }
    if let Some(report) = tq.backtest_performance_report(21) {
        println!(
            "Report days={} rolling_points={}",
            report.daily_balance_returns().len(),
            report.rolling_balance_sharpe_ratios().len()
        );
    }

    Ok(())
}
