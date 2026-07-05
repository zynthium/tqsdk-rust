#![cfg_attr(not(test), forbid(unsafe_code))]
//! Scenario 48: Embedded monitoring dashboard through the `tqsdk` facade.
//!
//! Run without credentials to start a local replay-backed monitor:
//!
//! ```bash
//! cargo run -p tqsdk --features monitoring --example api_contract_s48_facade_monitoring_dashboard
//! ```
//!
//! Set `TQ_RUN_LIVE_MONITOR=1` to observe a live session and shared tick cache recording.

use std::time::Duration;

use tqsdk::advanced::task::replay::ReplayMarketSource;
use tqsdk::prelude::*;

const SYMBOL: &str = "KQ.i@SHFE.au";
const LOCAL_SYMBOL: &str = "SHFE.au2510";
const CACHE_DIR: &str = ".tqsdk/backtest_ticks";
const DEFAULT_MONITOR_PORT: u16 = 18_688;
const DEFAULT_HOLD_SECS: u64 = 30;

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    if std::env::var_os("TQ_RUN_LIVE_MONITOR").is_some() {
        #[cfg(feature = "live")]
        {
            return run_live_monitor().await;
        }
        #[cfg(not(feature = "live"))]
        {
            eprintln!("TQ_RUN_LIVE_MONITOR requires the tqsdk live feature; running local demo");
        }
    }

    run_local_monitor().await
}

async fn run_local_monitor() -> tqsdk::Result<()> {
    let monitoring = MonitoringConfig::localhost(monitor_port()).with_cache_inventory(CACHE_DIR);
    let replay = ReplayMarketSource::new(vec![]);

    let tq = Tq::new()
        .monitoring(monitoring)
        .replay_backtest(replay)
        .quote_symbol(LOCAL_SYMBOL)
        .price_tick(LOCAL_SYMBOL, 0.02)
        .connect()
        .await?;

    print_monitor_links(&tq);
    print_monitor_snapshot(&tq);
    hold_for_dashboard().await;
    Ok(())
}

#[cfg(feature = "live")]
async fn run_live_monitor() -> tqsdk::Result<()> {
    let cache = MarketCachePolicy::new(CACHE_DIR).record_ticks([SYMBOL]);
    let mut tq = Tq::futures()
        .auth_env()?
        .market_cache(cache)
        .monitoring(MonitoringConfig::localhost(monitor_port()))
        .connect()
        .await?;
    let quote = tq.quote(SYMBOL).await?;

    print_monitor_links(&tq);
    let until = tokio::time::Instant::now() + hold_duration();
    while tokio::time::Instant::now() < until && tq.next().await? {
        let _snapshot = quote.load()?;
        print_monitor_snapshot(&tq);
    }
    Ok(())
}

fn print_monitor_links(tq: &Tq) {
    if let Some(addr) = tq.monitor_addr() {
        println!("monitor dashboard: http://{addr}/monitor");
        println!("monitor snapshot:  http://{addr}/monitor/api/snapshot");
    }
}

fn print_monitor_snapshot(tq: &Tq) {
    let Some(snapshot) = tq.monitor_snapshot() else {
        return;
    };
    println!(
        "mode={:?} wait_steps={} tick_batches={} cache_rows={} inventory_rows={}",
        snapshot.process.mode,
        snapshot.latency.wait_steps,
        snapshot.market.tick_batches,
        snapshot.cache.rows_written,
        snapshot.history.inventory_rows
    );
}

async fn hold_for_dashboard() {
    tokio::time::sleep(hold_duration()).await;
}

fn hold_duration() -> Duration {
    Duration::from_secs(env_u64("TQ_MONITOR_SECONDS", DEFAULT_HOLD_SECS))
}

fn monitor_port() -> u16 {
    env_u16("TQ_MONITOR_PORT", DEFAULT_MONITOR_PORT)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u16(name: &str, default: u16) -> u16 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
