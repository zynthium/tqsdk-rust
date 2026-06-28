#![cfg_attr(not(test), forbid(unsafe_code))]
//! Scenario 45: Warm up the persistent backtest tick cache without running a strategy.

use tqsdk::prelude::*;

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    let start_ns = 1_781_182_800_000_000_000;
    let end_ns = 1_781_182_860_000_000_000;
    let symbol = "KQ.i@SHFE.au";

    let warmup = Tq::futures()
        .auth_env()?
        .backtest(start_ns, end_ns)
        .cache_dir(".tqsdk/backtest_ticks")?
        .universe(format!("symbol:{symbol}"))?
        .remote_on_miss()
        .batch_size(10)
        .warmup()
        .await?;

    assert_eq!(warmup.symbols_total, 1);

    let cached = Tq::futures()
        .backtest(start_ns, end_ns)
        .cache_dir(".tqsdk/backtest_ticks")?
        .universe(format!("symbol:{symbol}"))?
        .remote_on_miss()
        .warmup()
        .await?;

    assert!(!cached.remote_used);
    Ok(())
}
