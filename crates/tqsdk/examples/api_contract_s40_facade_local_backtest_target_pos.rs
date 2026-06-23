#![cfg_attr(not(test), forbid(unsafe_code))]
//! Scenario 40: Default facade local backtest supports TargetPos same-body logic.
//!
//! The strategy body uses quote, position, target position, and the same
//! `Tq::next()` loop shape a live strategy would use. The mode difference stays
//! in the builder and account id supplied by the caller.

use tqsdk::advanced::core::Quote;
use tqsdk::advanced::task::{ReplayMarketEvent, ReplayMarketSource};
use tqsdk::prelude::*;

const SYMBOL: &str = "SHFE.rb2501";

async fn run_strategy(tq: &mut Tq, account_id: &str, symbol: &str) -> tqsdk::Result<()> {
    let quote = tq.quote(symbol).await?;
    let target = tq.target_pos(account_id, symbol)?;

    while tq.next().await? {
        let snapshot = quote.load()?;
        let position = tq.position(account_id, symbol).load()?;

        if position.pos_long == 0 && snapshot.last_price >= 100.0 {
            target.set(1)?;
        }
    }

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    let replay = ReplayMarketSource::new(vec![
        quote_event(1_000, 100.0)?,
        quote_event(2_000, 101.0)?,
        quote_event(3_000, 102.0)?,
    ]);

    let mut tq = Tq::new().local_backtest(replay).connect().await?;

    run_strategy(&mut tq, LOCAL_BACKTEST_ACCOUNT_ID, SYMBOL).await?;

    let summary = tq
        .backtest_summary()
        .ok_or(tqsdk::advanced::task::TaskError::InvalidState(
            "local backtest summary missing",
        ))?;
    let final_position =
        summary
            .final_positions()
            .first()
            .ok_or(tqsdk::advanced::task::TaskError::InvalidState(
                "local backtest final position missing",
            ))?;

    println!(
        "events={} orders={} trades={} final_pos_long={}",
        summary.event_count(),
        summary.orders().len(),
        summary.trade_log().len(),
        final_position.pos_long,
    );
    println!(
        "initial_balance={} final_balance={} balance_change={} balance_return_rate={}",
        summary.initial_account().balance,
        summary.final_account().balance,
        summary.balance_change(),
        summary.balance_return_rate(),
    );

    Ok(())
}

fn quote_event(received_at_ns: i64, last_price: f64) -> tqsdk::Result<ReplayMarketEvent> {
    Ok(ReplayMarketEvent::quote(
        "fixture",
        SYMBOL,
        received_at_ns,
        Some(received_at_ns),
        Quote {
            datetime: format!("2025-01-01 09:30:{:02}.000000", received_at_ns / 1_000),
            last_price,
            ask_price1: last_price,
            ask_volume1: 10,
            bid_price1: last_price - 1.0,
            bid_volume1: 10,
            price_tick: 1.0,
            volume_multiple: 10,
            ..Quote::default()
        },
    )?)
}
