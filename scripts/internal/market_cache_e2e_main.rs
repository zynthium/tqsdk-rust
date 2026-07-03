#![forbid(unsafe_code)]

use std::env;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use tqsdk::prelude::*;

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    let label = env::var("TQ_E2E_LABEL").unwrap_or_else(|_| "market-cache-e2e".to_string());
    let cache_dir = PathBuf::from(required_env("TQ_E2E_CACHE_DIR"));
    let start_ns = read_i64_env("TQ_E2E_START_NS");
    let end_ns = read_i64_env("TQ_E2E_END_NS");
    let symbols = read_symbols_env("TQ_E2E_SYMBOLS");
    let batch_size = read_usize_env("TQ_E2E_BATCH_SIZE");
    let live_seconds = read_optional_u64_env("TQ_E2E_LIVE_SECONDS").unwrap_or_default();
    let live_min_updates = read_optional_usize_env("TQ_E2E_LIVE_MIN_UPDATES").unwrap_or_default();
    let skip_remote = env::var_os("TQ_E2E_SKIP_REMOTE").is_some();
    let skip_cache_only = env::var_os("TQ_E2E_SKIP_CACHE_ONLY").is_some();
    let skip_replay = env::var_os("TQ_E2E_SKIP_REPLAY").is_some();

    let policy =
        MarketCachePolicy::new(&cache_dir).record_ticks(symbols.iter().map(String::as_str));

    let total_started = Instant::now();
    let live_report = if live_seconds > 0 {
        run_live_recording(policy.clone(), &symbols, live_seconds, live_min_updates).await?
    } else {
        LiveReport::default()
    };

    let remote_report = if skip_remote {
        WarmupReport::skipped("remote")
    } else {
        run_remote_warmup(policy.clone(), start_ns, end_ns, batch_size).await?
    };

    let cache_only_report = if skip_cache_only {
        WarmupReport::skipped("cache_only")
    } else {
        run_cache_only_warmup(policy.clone(), start_ns, end_ns, batch_size).await?
    };

    let replay_report = if skip_replay {
        ReplayReport::default()
    } else {
        run_cache_only_replay(policy, start_ns, end_ns).await?
    };

    let total_elapsed_s = total_started.elapsed().as_secs_f64();
    print_result(&[
        ("label", label),
        ("symbols", symbols.join(",")),
        ("symbol_count", symbols.len().to_string()),
        ("range_start_ns", start_ns.to_string()),
        ("range_end_ns", end_ns.to_string()),
        ("batch_size", batch_size.to_string()),
        ("live_requested", (live_seconds > 0).to_string()),
        ("live_seconds", live_seconds.to_string()),
        ("live_min_updates", live_min_updates.to_string()),
        ("live_updates", live_report.updates.to_string()),
        (
            "live_total_appended_rows",
            live_report.total_appended_rows.to_string(),
        ),
        ("live_gap_detected", live_report.gap_detected.to_string()),
        ("live_flush_count", live_report.flush_count.to_string()),
        ("remote_skipped", remote_report.skipped.to_string()),
        (
            "remote_symbols_total",
            remote_report.symbols_total.to_string(),
        ),
        (
            "remote_complete_symbols",
            remote_report.complete_symbols.to_string(),
        ),
        (
            "remote_symbols_missing",
            remote_report.symbols_missing.to_string(),
        ),
        (
            "remote_symbols_filled",
            remote_report.symbols_filled.to_string(),
        ),
        (
            "remote_rows_written",
            remote_report.rows_written.to_string(),
        ),
        ("remote_used", remote_report.remote_used.to_string()),
        (
            "remote_rows_by_symbol",
            remote_report.rows_by_symbol.join(","),
        ),
        (
            "remote_actions_by_symbol",
            remote_report.actions_by_symbol.join(","),
        ),
        (
            "remote_elapsed_s",
            format!("{:.3}", remote_report.elapsed_s),
        ),
        ("cache_only_skipped", cache_only_report.skipped.to_string()),
        (
            "cache_only_symbols_total",
            cache_only_report.symbols_total.to_string(),
        ),
        (
            "cache_only_complete_symbols",
            cache_only_report.complete_symbols.to_string(),
        ),
        (
            "cache_only_symbols_missing",
            cache_only_report.symbols_missing.to_string(),
        ),
        (
            "cache_only_rows_written",
            cache_only_report.rows_written.to_string(),
        ),
        (
            "cache_only_elapsed_s",
            format!("{:.3}", cache_only_report.elapsed_s),
        ),
        ("replay_skipped", replay_report.skipped.to_string()),
        ("replay_updates", replay_report.updates.to_string()),
        ("replay_tick_count", replay_report.tick_count.to_string()),
        (
            "replay_elapsed_s",
            format!("{:.3}", replay_report.elapsed_s),
        ),
        ("total_elapsed_s", format!("{total_elapsed_s:.3}")),
        ("cache_dir", cache_dir.display().to_string()),
    ]);
    Ok(())
}

async fn run_live_recording(
    policy: MarketCachePolicy,
    symbols: &[String],
    live_seconds: u64,
    live_min_updates: usize,
) -> tqsdk::Result<LiveReport> {
    let mut tq = Tq::futures()
        .auth_env()?
        .market_cache(policy)
        .connect()
        .await?;
    let mut quotes = Vec::new();
    for symbol in symbols {
        quotes.push(tq.quote(symbol).await?);
    }

    let deadline = Instant::now() + Duration::from_secs(live_seconds);
    let mut updates = 0usize;
    while Instant::now() < deadline || updates < live_min_updates {
        let step_deadline = tokio::time::Instant::from_std(deadline);
        if !tq.wait_update(Some(step_deadline)).await? {
            break;
        }
        for quote in &quotes {
            let _ = quote.load()?;
        }
        updates = updates.saturating_add(1);
        if updates >= live_min_updates && Instant::now() >= deadline {
            break;
        }
    }

    let health = tq.record_ticks_health().cloned();
    Ok(LiveReport {
        updates,
        total_appended_rows: health
            .as_ref()
            .map(|health| health.total_appended_rows)
            .unwrap_or_default(),
        gap_detected: health
            .as_ref()
            .map(|health| health.gap_detected)
            .unwrap_or_default(),
        flush_count: health
            .as_ref()
            .map(|health| health.flush_count)
            .unwrap_or_default(),
    })
}

async fn run_remote_warmup(
    policy: MarketCachePolicy,
    start_ns: i64,
    end_ns: i64,
    batch_size: usize,
) -> tqsdk::Result<WarmupReport> {
    let started = Instant::now();
    let report = Tq::futures()
        .auth_env()?
        .market_cache(policy)
        .backtest(start_ns, end_ns)
        .remote_on_miss()
        .batch_size(batch_size)
        .warmup()
        .await?;
    Ok(WarmupReport::from_report(report, started.elapsed()))
}

async fn run_cache_only_warmup(
    policy: MarketCachePolicy,
    start_ns: i64,
    end_ns: i64,
    batch_size: usize,
) -> tqsdk::Result<WarmupReport> {
    let started = Instant::now();
    let report = Tq::futures()
        .market_cache(policy)
        .backtest(start_ns, end_ns)
        .cache_only()
        .batch_size(batch_size)
        .warmup()
        .await?;
    Ok(WarmupReport::from_report(report, started.elapsed()))
}

async fn run_cache_only_replay(
    policy: MarketCachePolicy,
    start_ns: i64,
    end_ns: i64,
) -> tqsdk::Result<ReplayReport> {
    let started = Instant::now();
    let mut tq = Tq::futures()
        .market_cache(policy)
        .backtest(start_ns, end_ns)
        .cache_only()
        .connect()
        .await?;
    let mut updates = 0usize;
    while tq.next().await? {
        updates = updates.saturating_add(1);
    }
    let tick_count = tq
        .backtest_summary()
        .map(|summary| summary.tick_count())
        .unwrap_or_default();
    Ok(ReplayReport {
        skipped: false,
        updates,
        tick_count,
        elapsed_s: started.elapsed().as_secs_f64(),
    })
}

#[derive(Default)]
struct LiveReport {
    updates: usize,
    total_appended_rows: usize,
    gap_detected: bool,
    flush_count: u64,
}

struct WarmupReport {
    skipped: bool,
    symbols_total: usize,
    complete_symbols: usize,
    symbols_missing: usize,
    symbols_filled: usize,
    rows_written: usize,
    remote_used: bool,
    rows_by_symbol: Vec<String>,
    actions_by_symbol: Vec<String>,
    elapsed_s: f64,
}

impl WarmupReport {
    fn skipped(_kind: &str) -> Self {
        Self {
            skipped: true,
            symbols_total: 0,
            complete_symbols: 0,
            symbols_missing: 0,
            symbols_filled: 0,
            rows_written: 0,
            remote_used: false,
            rows_by_symbol: Vec::new(),
            actions_by_symbol: Vec::new(),
            elapsed_s: 0.0,
        }
    }

    fn from_report(report: BacktestCacheWarmupReport, elapsed: Duration) -> Self {
        Self {
            skipped: false,
            symbols_total: report.symbols_total,
            complete_symbols: report
                .symbols
                .iter()
                .filter(|symbol| symbol.after.is_complete())
                .count(),
            symbols_missing: report.symbols_missing,
            symbols_filled: report.symbols_filled,
            rows_written: report.rows_written,
            remote_used: report.remote_used,
            rows_by_symbol: report
                .symbols
                .iter()
                .map(|symbol| format!("{}:{}", symbol.symbol, symbol.rows_written))
                .collect(),
            actions_by_symbol: report
                .symbols
                .iter()
                .map(|symbol| format!("{}:{:?}", symbol.symbol, symbol.action))
                .collect(),
            elapsed_s: elapsed.as_secs_f64(),
        }
    }
}

#[derive(Default)]
struct ReplayReport {
    skipped: bool,
    updates: usize,
    tick_count: usize,
    elapsed_s: f64,
}

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} is required"))
}

fn read_i64_env(name: &str) -> i64 {
    required_env(name)
        .parse()
        .unwrap_or_else(|_| panic!("{name} must be an i64"))
}

fn read_usize_env(name: &str) -> usize {
    required_env(name)
        .parse()
        .unwrap_or_else(|_| panic!("{name} must be a usize"))
}

fn read_optional_u64_env(name: &str) -> Option<u64> {
    env::var(name).ok().map(|value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be a u64"))
    })
}

fn read_optional_usize_env(name: &str) -> Option<usize> {
    env::var(name).ok().map(|value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be a usize"))
    })
}

fn read_symbols_env(name: &str) -> Vec<String> {
    let symbols = required_env(name)
        .split(',')
        .map(str::trim)
        .filter(|symbol| !symbol.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    assert!(
        !symbols.is_empty(),
        "{name} must contain at least one symbol"
    );
    symbols
}

fn print_result(fields: &[(&str, String)]) {
    print!("E2E_RESULT");
    for (key, value) in fields {
        print!("\t{key}={}", sanitize_field(value));
    }
    println!();
}

fn sanitize_field(value: &str) -> String {
    value.replace(['\t', '\n', '\r'], " ")
}
