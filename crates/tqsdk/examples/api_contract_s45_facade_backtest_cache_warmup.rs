#![cfg_attr(not(test), forbid(unsafe_code))]
//! Scenario 45: Warm up the persistent backtest tick cache without running a strategy.

use std::time::Duration;
use tqsdk::prelude::*;

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    let start_ns = 1_781_182_800_000_000_000;
    let end_ns = 1_781_182_860_000_000_000;
    let symbol = "KQ.i@SHFE.au";
    let remote_fill = BacktestRemoteFillConfig::from_environment()
        .with_symbol_batch_size(1)
        .with_symbol_concurrency(1)
        .with_idle_timeout(Duration::from_secs(120));

    let warmup = Tq::futures()
        .auth_env()?
        .backtest(start_ns, end_ns)
        .cache_dir(".tqsdk/backtest_ticks")?
        .universe(format!("symbol:{symbol}"))?
        .remote_on_miss()
        .remote_fill_config(remote_fill)
        .remote_fill_lock_wait(Duration::from_secs(30))
        .warmup()
        .await?;

    assert_eq!(warmup.symbols_total, 1);
    assert_eq!(warmup.logical_symbols, vec![symbol.to_string()]);

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
