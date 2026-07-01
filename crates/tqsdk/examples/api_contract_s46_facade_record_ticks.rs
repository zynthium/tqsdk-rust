#![cfg_attr(not(test), forbid(unsafe_code))]
//! Scenario 46: Explicit live tick recording into the shared backtest cache.

use tqsdk::prelude::*;

const SYMBOL: &str = "KQ.i@SHFE.au";

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    if std::env::var_os("TQ_RUN_LIVE_RECORD_TICKS").is_none() {
        return Ok(());
    }

    let mut tq = Tq::futures().auth_env()?.connect().await?;
    tq.record_ticks(".tqsdk/backtest_ticks", [SYMBOL]).await?;
    let quote = tq.quote(SYMBOL).await?;

    while tq.next().await? {
        let _snapshot = quote.load()?;
    }

    Ok(())
}
