#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk::prelude::*;

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    let mut tq = Tq::futures()
        .auth_env()?
        .trade_target_tqkq()
        .connect()
        .await?;

    let quote = tq.quote("SHFE.au2602").await?;
    let target = tq.target_pos_tqkq("SHFE.au2602").await?;

    while tq.next().await? {
        let snapshot = quote.load()?;
        if snapshot.last_price > 3600.0 {
            target.set(1)?;
        } else {
            target.close()?;
        }
    }

    Ok(())
}
