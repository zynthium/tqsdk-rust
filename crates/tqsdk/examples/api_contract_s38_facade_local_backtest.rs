#![cfg_attr(not(test), forbid(unsafe_code))]
//! Scenario 38: Local offline backtest through the `tqsdk` facade.
//!
//! Demonstrates the usage of `.local_backtest()` on the TqBuilder.
//! The exact same `while tq.next()` and `quote.load()` strategy code runs
//! against a locally downloaded market data cache without connecting to servers.

use tqsdk::advanced::task::ReplayMarketSource;
use tqsdk::prelude::*;

#[allow(dead_code)]
async fn build_continuous_minute_backtest(
    data: &tqsdk::advanced::data::DataClient,
) -> tqsdk::Result<TqBuilder> {
    Tq::futures()
        .default_price_tick(1.0)
        .local_backtest_continuous_minute_history(
            data,
            "KQ.m@SHFE.rb",
            1_767_715_200_000_000_000,
            1_768_233_600_000_000_000,
        )
        .await
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    // Usually you'd build this from downloaded history rows or explicit replay events,
    // but here we just use an empty replay to prove it compiles and runs.
    let replay = ReplayMarketSource::new(vec![]);

    let mut tq = Tq::new()
        // No auth_env() or connect to server needed for local backtest
        .local_backtest(replay)
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

    // You can also inspect the summary of the backtest afterwards
    if let Some(summary) = tq.backtest_summary() {
        println!(
            "Backtest finished! Executed {} events.",
            summary.event_count()
        );
    }

    Ok(())
}
