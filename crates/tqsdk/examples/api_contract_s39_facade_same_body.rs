#![cfg_attr(not(test), forbid(unsafe_code))]
//! Scenario 39: Multi-symbol strategy logic shared by local backtest,
//! simulated trading, and live trading.
//!
//! The strategy body only accepts `&mut Tq`, an account id, and symbol config.
//! Changing execution mode is isolated to the builder code in `main()`.

use tqsdk::advanced::core::Quote;
use tqsdk::advanced::task::{ReplayMarketEvent, ReplayMarketSource};
use tqsdk::prelude::*;

const NEAR_SYMBOL: &str = "SHFE.rb2610";
const FAR_SYMBOL: &str = "SHFE.rb2701";

#[derive(Debug, Clone, Copy)]
struct SpreadLegs<'a> {
    near: &'a str,
    far: &'a str,
    open_threshold: f64,
    close_threshold: f64,
}

async fn run_spread_strategy(tq: &mut Tq, legs: SpreadLegs<'_>) -> tqsdk::Result<()> {
    let near_quote = tq.quote(legs.near).await?;
    let far_quote = tq.quote(legs.far).await?;
    let near_target = tq.target_pos_default(legs.near)?;
    let far_target = tq.target_pos_default(legs.far)?;

    while tq.next().await? {
        let Some(near) = near_quote.snapshot()? else {
            continue;
        };
        let Some(far) = far_quote.snapshot()? else {
            continue;
        };
        if !near.last_price.is_finite() || !far.last_price.is_finite() {
            continue;
        }

        let spread = near.last_price - far.last_price;
        println!(
            "{} near={} far={} spread={}",
            near.datetime, near.last_price, far.last_price, spread
        );

        if spread > legs.open_threshold {
            near_target.set(-1)?;
            far_target.set(1)?;
        } else if spread < legs.close_threshold {
            near_target.close()?;
            far_target.close()?;
        }
    }

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    let mode = std::env::var("TQ_EXAMPLE_MODE").unwrap_or_else(|_| "local-backtest".to_string());
    let legs = SpreadLegs {
        near: NEAR_SYMBOL,
        far: FAR_SYMBOL,
        open_threshold: 250.0,
        close_threshold: 200.0,
    };

    match mode.as_str() {
        "local-backtest" => {
            let mut tq = build_local_backtest().await?;
            run_spread_strategy(&mut tq, legs).await?;
            print_local_summary(&tq);
        }
        #[cfg(feature = "live")]
        "tqkq-sim" => {
            let mut tq = build_tqkq_sim().await?;
            run_spread_strategy(&mut tq, legs).await?;
        }
        #[cfg(feature = "live")]
        "live" => {
            let mut tq = build_live_account().await?;
            run_spread_strategy(&mut tq, legs).await?;
        }
        other => {
            eprintln!("unsupported TQ_EXAMPLE_MODE={other}; use local-backtest, tqkq-sim, or live");
        }
    }

    Ok(())
}

async fn build_local_backtest() -> tqsdk::Result<Tq> {
    let replay = ReplayMarketSource::new(vec![
        quote_event(1_000, NEAR_SYMBOL, 4_300.0)?,
        quote_event(2_000, FAR_SYMBOL, 4_080.0)?,
        quote_event(3_000, NEAR_SYMBOL, 4_350.0)?,
        quote_event(4_000, FAR_SYMBOL, 4_085.0)?,
        quote_event(5_000, NEAR_SYMBOL, 4_260.0)?,
        quote_event(6_000, FAR_SYMBOL, 4_080.0)?,
        quote_event(7_000, NEAR_SYMBOL, 4_255.0)?,
        quote_event(8_000, FAR_SYMBOL, 4_085.0)?,
    ]);

    let tq = Tq::new()
        .local_backtest(replay)
        .quote_symbol(NEAR_SYMBOL)
        .quote_symbol(FAR_SYMBOL)
        .price_tick(NEAR_SYMBOL, 1.0)
        .price_tick(FAR_SYMBOL, 1.0)
        .connect()
        .await?;

    Ok(tq)
}

#[cfg(feature = "live")]
async fn build_tqkq_sim() -> tqsdk::Result<Tq> {
    Tq::new().auth_env()?.tqkq_sim().connect().await
}

#[cfg(feature = "live")]
async fn build_live_account() -> tqsdk::Result<Tq> {
    Tq::new().auth_env()?.trade_account_env()?.connect().await
}

fn quote_event(
    received_at_ns: i64,
    symbol: &str,
    last_price: f64,
) -> tqsdk::Result<ReplayMarketEvent> {
    Ok(ReplayMarketEvent::quote(
        "fixture",
        symbol,
        received_at_ns,
        Some(received_at_ns),
        Quote {
            datetime: format!("2025-01-01 09:30:{:02}.000000", received_at_ns / 1_000),
            last_price,
            ask_price1: last_price + 1.0,
            ask_volume1: 10,
            bid_price1: last_price - 1.0,
            bid_volume1: 10,
            price_tick: 1.0,
            volume_multiple: 10,
            margin: 1_000.0,
            commission: 0.0,
            ..Quote::default()
        },
    )?)
}

fn print_local_summary(tq: &Tq) {
    let Some(summary) = tq.backtest_summary() else {
        return;
    };

    println!(
        "summary events={} orders={} trades={} balance_change={}",
        summary.event_count(),
        summary.orders().len(),
        summary.trades().len(),
        summary.balance_change()
    );
    for position in summary.final_positions() {
        println!(
            "position {}.{} pos={} long={} short={}",
            position.exchange_id,
            position.instrument_id,
            position.pos,
            position.pos_long,
            position.pos_short
        );
    }
}
