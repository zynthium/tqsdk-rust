#![cfg_attr(not(test), forbid(unsafe_code))]
//! Scenario 43: Cache-backed local backtest through the `tqsdk` facade.

use tqsdk::{BacktestTickCache, Tq};
use tqsdk_core::Tick;

const SYMBOL: &str = "SHFE.rb2601";

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    let cache_dir = temp_cache_dir();
    let cache = BacktestTickCache::open(&cache_dir)?;
    cache.store_ticks(
        SYMBOL,
        1_000,
        3_000,
        [tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
    )?;

    let mut tq = Tq::new()
        .backtest(1_000, 3_000)
        .cache_dir(&cache_dir)?
        .cache_only()
        .universe(format!("symbol:{SYMBOL}"))?
        .connect()
        .await?;

    let quote = tq.quote(SYMBOL).await?;
    let mut events = 0;
    while tq.next().await? {
        events += 1;
        let snapshot = quote.load()?;
        assert!(snapshot.last_price >= 100.0);
    }

    assert_eq!(events, 2);
    Ok(())
}

fn tick(id: i64, datetime: i64, last_price: f64) -> Tick {
    Tick {
        id,
        datetime,
        last_price,
        ask_price1: last_price + 0.5,
        ask_volume1: 1,
        bid_price1: last_price - 0.5,
        bid_volume1: 1,
        ..Tick::default()
    }
}

fn temp_cache_dir() -> std::path::PathBuf {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time should be after the Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "tqsdk-backtest-history-cache-contract-{}-{unique}",
        std::process::id()
    ))
}
