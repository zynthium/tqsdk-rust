#![cfg_attr(not(test), forbid(unsafe_code))]
//! Scenario 39: Zero-branch strategy logic across multiple execution modes.
//!
//! Demonstrates how to write a strategy body as a single function taking `&mut Tq`,
//! and then running it identically in live, server backtest, and local backtest modes.

use tqsdk::advanced::task::ReplayMarketSource;
use tqsdk::prelude::*;

// The strategy body knows nothing about the execution mode!
async fn run_strategy(tq: &mut Tq) -> tqsdk::Result<()> {
    let quote = tq.quote("SHFE.au2510").await?;

    while tq.next().await? {
        let snapshot = quote.load()?;
        println!("{} last_price={}", snapshot.datetime, snapshot.last_price);
    }

    // Check if we have a backtest summary (only exists in local backtest)
    if let Some(summary) = tq.backtest_summary() {
        println!(
            "Backtest summary: {} events processed",
            summary.event_count()
        );
    }

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    println!("=== 1. Server Backtest ===");
    // Server backtest (mocked by early return for this example to avoid network calls)
    /*
    let mut tq_server = Tq::new()
        .auth_env()?
        .backtest(1_735_747_200_000_000_000, 1_748_707_200_000_000_000)
        .connect()
        .await?;
    run_strategy(&mut tq_server).await?;
    */

    println!("=== 2. Local Offline Backtest ===");
    // Local backtest
    let replay = ReplayMarketSource::new(vec![]);
    let mut tq_local = Tq::new()
        .local_backtest(replay)
        .quote_symbol("SHFE.au2510")
        .price_tick("SHFE.au2510", 0.02)
        .connect()
        .await?;
    run_strategy(&mut tq_local).await?;

    println!("=== 3. Live Trading ===");
    // Live (mocked by early return for this example)
    /*
    let mut tq_live = Tq::new()
        .auth_env()?
        .trade_target_tqkq()
        .connect()
        .await?;
    run_strategy(&mut tq_live).await?;
    */

    Ok(())
}
