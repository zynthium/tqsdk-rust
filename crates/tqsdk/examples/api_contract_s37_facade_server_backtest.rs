#![cfg_attr(not(test), forbid(unsafe_code))]
//! Scenario 37: Server-side backtest through the `tqsdk` facade.
//!
//! Demonstrates that switching from live to backtest requires only adding
//! `.backtest(start_ns, end_ns)` to the builder — the strategy body stays
//! exactly the same as in live mode (see `api_contract_s33_default_facade`).

use tqsdk::prelude::*;

#[allow(dead_code)]
fn build_with_custom_replay_endpoint(
    replay_url: impl Into<String>,
    start_ns: i64,
    end_ns: i64,
) -> tqsdk::Result<TqBuilder> {
    Ok(Tq::futures()
        .auth_env()?
        .replay_url(replay_url)
        .backtest(start_ns, end_ns))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    // 2025-01-02 00:00:00 CST  →  1735747200_000_000_000 ns
    let start_ns: i64 = 1_735_747_200_000_000_000;
    // 2025-06-01 00:00:00 CST  →  1748707200_000_000_000 ns
    let end_ns: i64 = 1_748_707_200_000_000_000;

    let mut tq = Tq::new()
        .auth_env()?
        .backtest(start_ns, end_ns) // ← only difference from live
        .connect()
        .await?;

    let quote = tq.quote("SHFE.au2510").await?;

    while tq.next().await? {
        let snapshot = quote.load()?;
        println!("{} last_price={}", snapshot.datetime, snapshot.last_price);
    }

    Ok(())
}
