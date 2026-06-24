#![cfg_attr(not(test), forbid(unsafe_code))]
//! Scenario 41: Server-side single-day replay through the `tqsdk` facade.
//!
//! Demonstrates the Rust counterpart to Python `TqReplay(date)`: switching from
//! live to official server replay only changes the builder. The strategy body
//! keeps using the same `next()` / `quote()` loop.

use chrono::NaiveDate;
use tqsdk::prelude::*;

#[allow(dead_code)]
fn build_server_replay(replay_date: NaiveDate) -> tqsdk::Result<TqBuilder> {
    Tq::futures().auth_env()?.server_replay(replay_date)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    let replay_date = NaiveDate::from_ymd_opt(2026, 6, 25)
        .expect("example replay date should be a valid weekday");

    let mut tq = Tq::new()
        .auth_env()?
        .server_replay(replay_date)?
        .connect()
        .await?;
    tq.set_replay_speed(3.0).await?;

    let quote = tq.quote("SHFE.au2612").await?;

    while tq.next().await? {
        let snapshot = quote.load()?;
        println!("{} last_price={}", snapshot.datetime, snapshot.last_price);
    }

    Ok(())
}
