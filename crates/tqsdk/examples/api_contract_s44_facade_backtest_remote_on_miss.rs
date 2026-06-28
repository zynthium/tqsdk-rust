#![cfg_attr(not(test), forbid(unsafe_code))]
//! Scenario 44: Remote-on-miss backtest cache fill through the `tqsdk` facade.

use tqsdk::prelude::*;

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    let start_ns = 1_781_172_000_000_000_000;
    let end_ns = 1_781_258_401_000_000_000;
    let symbol = "SHFE.au2608";

    let mut tq = Tq::futures()
        .auth_env()?
        .backtest(start_ns, end_ns)
        .cache_dir(".tqsdk/backtest_ticks")?
        .universe(format!("symbol:{symbol}"))?
        .remote_on_miss()
        .connect()
        .await?;

    let quote = tq.quote(symbol).await?;
    while tq.next().await? {
        let _last_price = quote.load()?.last_price;
    }
    Ok(())
}
