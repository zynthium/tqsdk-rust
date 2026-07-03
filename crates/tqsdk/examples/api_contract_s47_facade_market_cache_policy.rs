#![cfg_attr(not(test), forbid(unsafe_code))]
//! Scenario 47: Shared market-cache policy across live recording and local backtest.

use tqsdk::prelude::*;

const SYMBOL: &str = "KQ.i@SHFE.au";
const CACHE_DIR: &str = ".tqsdk/backtest_ticks";

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    let cache = MarketCachePolicy::new(CACHE_DIR).record_ticks([SYMBOL]);

    if std::env::var_os("TQ_RUN_LIVE_RECORD_TICKS").is_some() {
        let mut tq = Tq::futures()
            .auth_env()?
            .market_cache(cache.clone())
            .connect()
            .await?;
        let quote = tq.quote(SYMBOL).await?;

        while tq.next().await? {
            let _snapshot = quote.load()?;
            let _cache_health = tq.record_ticks_health();
        }

        let _gap_warmup_policy = tq.recorded_market_cache_policy();
    }

    let _backtest = Tq::futures()
        .market_cache(cache)
        .backtest(1_781_182_800_000_000_000, 1_781_182_860_000_000_000)
        .cache_only();

    Ok(())
}
