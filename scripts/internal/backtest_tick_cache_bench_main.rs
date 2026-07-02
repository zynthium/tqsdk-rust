#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};
use std::time::Instant;

use tqsdk::prelude::*;

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    let label = env::var("TQ_BENCH_LABEL").unwrap_or_else(|_| "bench".to_string());
    let cache_dir = PathBuf::from(required_env("TQ_BENCH_CACHE_DIR"));
    let start_ns = read_i64_env("TQ_BENCH_START_NS");
    let end_ns = read_i64_env("TQ_BENCH_END_NS");
    let batch_size = read_usize_env("TQ_BENCH_BATCH_SIZE");
    let skip_cache_only = env::var_os("TQ_BENCH_SKIP_CACHE_ONLY").is_some();
    let skip_replay = env::var_os("TQ_BENCH_SKIP_REPLAY").is_some();
    if env::var_os("TQ_BENCH_VERIFY_EXISTING_CACHE").is_some() {
        return verify_existing_cache(label, cache_dir, start_ns, end_ns, batch_size).await;
    }
    let universe = required_env("TQ_BENCH_UNIVERSE");

    let total_started = Instant::now();
    let warmup_started = Instant::now();
    let warmup = Tq::futures()
        .auth_env()?
        .backtest(start_ns, end_ns)
        .cache_dir(&cache_dir)?
        .universe(&universe)?
        .remote_on_miss()
        .batch_size(batch_size)
        .warmup()
        .await?;
    let warmup_elapsed_s = warmup_started.elapsed().as_secs_f64();

    let symbols = warmup
        .symbols
        .iter()
        .map(|symbol| symbol.symbol.clone())
        .collect::<Vec<_>>();
    let complete_symbols = warmup
        .symbols
        .iter()
        .filter(|symbol| symbol.after.is_complete())
        .count();

    let mut cache_only_elapsed_s = 0.0;
    let mut cache_only_missing = 0usize;
    let mut cache_only_skipped = 0usize;
    if !skip_cache_only {
        let cache_only_started = Instant::now();
        let cache_only = with_symbols(
            Tq::futures()
                .backtest(start_ns, end_ns)
                .cache_dir(&cache_dir)?
                .cache_only()
                .batch_size(batch_size),
            &symbols,
        )
        .warmup()
        .await?;
        cache_only_elapsed_s = cache_only_started.elapsed().as_secs_f64();
        cache_only_missing = cache_only.symbols_missing;
        cache_only_skipped = cache_only.symbols_skipped;
    }

    let mut replay_elapsed_s = 0.0;
    let mut replay_updates = 0usize;
    let mut replay_tick_count = 0usize;
    if !skip_replay {
        let replay_started = Instant::now();
        let mut tq = with_symbols(
            Tq::futures()
                .backtest(start_ns, end_ns)
                .cache_dir(&cache_dir)?
                .cache_only(),
            &symbols,
        )
        .connect()
        .await?;
        while tq.next().await? {
            replay_updates = replay_updates.saturating_add(1);
        }
        replay_elapsed_s = replay_started.elapsed().as_secs_f64();
        replay_tick_count = tq
            .backtest_summary()
            .map(|summary| summary.tick_count())
            .unwrap_or_default();
    }

    let action_counts =
        warmup
            .symbols
            .iter()
            .fold(BTreeMap::<String, usize>::new(), |mut counts, symbol| {
                *counts.entry(format!("{:?}", symbol.action)).or_default() += 1;
                counts
            });
    let rows_by_symbol = warmup
        .symbols
        .iter()
        .map(|symbol| format!("{}:{}", symbol.symbol, symbol.rows_written))
        .collect::<Vec<_>>()
        .join(",");
    let actions_by_symbol = warmup
        .symbols
        .iter()
        .map(|symbol| format!("{}:{:?}", symbol.symbol, symbol.action))
        .collect::<Vec<_>>()
        .join(",");
    let filled_remote = action_counts
        .get("FilledRemote")
        .copied()
        .unwrap_or_default();
    let refreshed_remote = action_counts
        .get("RefreshedRemote")
        .copied()
        .unwrap_or_default();
    let total_elapsed_s = total_started.elapsed().as_secs_f64();
    let rows_per_warmup_s = if warmup_elapsed_s > 0.0 {
        warmup.rows_written as f64 / warmup_elapsed_s
    } else {
        0.0
    };

    print_result(&[
        ("label", label),
        ("batch_size", batch_size.to_string()),
        ("symbols_total", warmup.symbols_total.to_string()),
        ("complete_symbols", complete_symbols.to_string()),
        ("symbols_missing", warmup.symbols_missing.to_string()),
        ("symbols_filled", warmup.symbols_filled.to_string()),
        ("symbols_skipped", warmup.symbols_skipped.to_string()),
        ("filled_remote", filled_remote.to_string()),
        ("refreshed_remote", refreshed_remote.to_string()),
        ("rows_written", warmup.rows_written.to_string()),
        ("rows_by_symbol", rows_by_symbol),
        ("actions_by_symbol", actions_by_symbol),
        ("remote_used", warmup.remote_used.to_string()),
        ("cache_only_missing", cache_only_missing.to_string()),
        ("cache_only_skipped", cache_only_skipped.to_string()),
        ("replay_updates", replay_updates.to_string()),
        ("replay_tick_count", replay_tick_count.to_string()),
        ("warmup_elapsed_s", format!("{warmup_elapsed_s:.3}")),
        ("cache_only_elapsed_s", format!("{cache_only_elapsed_s:.3}")),
        ("replay_elapsed_s", format!("{replay_elapsed_s:.3}")),
        ("total_elapsed_s", format!("{total_elapsed_s:.3}")),
        ("rows_per_warmup_s", format!("{rows_per_warmup_s:.3}")),
        ("cache_dir", cache_dir.display().to_string()),
    ]);
    Ok(())
}

async fn verify_existing_cache(
    label: String,
    cache_dir: PathBuf,
    start_ns: i64,
    end_ns: i64,
    batch_size: usize,
) -> tqsdk::Result<()> {
    let symbols = read_cache_symbols(&cache_dir)?;
    let total_started = Instant::now();

    let cache_only_started = Instant::now();
    let cache_only = with_symbols(
        Tq::futures()
            .backtest(start_ns, end_ns)
            .cache_dir(&cache_dir)?
            .cache_only()
            .batch_size(batch_size),
        &symbols,
    )
    .warmup()
    .await?;
    let cache_only_elapsed_s = cache_only_started.elapsed().as_secs_f64();

    let replay_started = Instant::now();
    let mut tq = with_symbols(
        Tq::futures()
            .backtest(start_ns, end_ns)
            .cache_dir(&cache_dir)?
            .cache_only(),
        &symbols,
    )
    .connect()
    .await?;
    let mut replay_updates = 0usize;
    while tq.next().await? {
        replay_updates = replay_updates.saturating_add(1);
    }
    let replay_elapsed_s = replay_started.elapsed().as_secs_f64();
    let replay_tick_count = tq
        .backtest_summary()
        .map(|summary| summary.tick_count())
        .unwrap_or_default();

    let complete_symbols = cache_only
        .symbols
        .iter()
        .filter(|symbol| symbol.after.is_complete())
        .count();
    let action_counts =
        cache_only
            .symbols
            .iter()
            .fold(BTreeMap::<String, usize>::new(), |mut counts, symbol| {
                *counts.entry(format!("{:?}", symbol.action)).or_default() += 1;
                counts
            });
    let rows_by_symbol = cache_only
        .symbols
        .iter()
        .map(|symbol| format!("{}:{}", symbol.symbol, symbol.rows_written))
        .collect::<Vec<_>>()
        .join(",");
    let actions_by_symbol = cache_only
        .symbols
        .iter()
        .map(|symbol| format!("{}:{:?}", symbol.symbol, symbol.action))
        .collect::<Vec<_>>()
        .join(",");
    let total_elapsed_s = total_started.elapsed().as_secs_f64();
    let rows_per_warmup_s = if cache_only_elapsed_s > 0.0 {
        cache_only.rows_written as f64 / cache_only_elapsed_s
    } else {
        0.0
    };

    print_result(&[
        ("label", label),
        ("verify_existing_cache", "true".to_string()),
        ("batch_size", batch_size.to_string()),
        ("symbols_total", cache_only.symbols_total.to_string()),
        ("complete_symbols", complete_symbols.to_string()),
        ("symbols_missing", cache_only.symbols_missing.to_string()),
        ("symbols_filled", cache_only.symbols_filled.to_string()),
        ("symbols_skipped", cache_only.symbols_skipped.to_string()),
        (
            "filled_remote",
            action_counts
                .get("FilledRemote")
                .copied()
                .unwrap_or_default()
                .to_string(),
        ),
        (
            "refreshed_remote",
            action_counts
                .get("RefreshedRemote")
                .copied()
                .unwrap_or_default()
                .to_string(),
        ),
        ("rows_written", cache_only.rows_written.to_string()),
        ("rows_by_symbol", rows_by_symbol),
        ("actions_by_symbol", actions_by_symbol),
        ("remote_used", cache_only.remote_used.to_string()),
        ("cache_only_missing", cache_only.symbols_missing.to_string()),
        ("cache_only_skipped", cache_only.symbols_skipped.to_string()),
        ("replay_updates", replay_updates.to_string()),
        ("replay_tick_count", replay_tick_count.to_string()),
        ("warmup_elapsed_s", "0.000".to_string()),
        ("cache_only_elapsed_s", format!("{cache_only_elapsed_s:.3}")),
        ("replay_elapsed_s", format!("{replay_elapsed_s:.3}")),
        ("total_elapsed_s", format!("{total_elapsed_s:.3}")),
        ("rows_per_warmup_s", format!("{rows_per_warmup_s:.3}")),
        ("cache_dir", cache_dir.display().to_string()),
    ]);
    Ok(())
}

fn with_symbols(mut builder: BacktestBuilder, symbols: &[String]) -> BacktestBuilder {
    for symbol in symbols {
        builder = builder.symbol(symbol.as_str());
    }
    builder
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

fn read_cache_symbols(cache_dir: &Path) -> tqsdk::Result<Vec<String>> {
    let series_dir = cache_dir.join("series");
    let entries = std::fs::read_dir(&series_dir).map_err(|source| {
        tqsdk::advanced::data::DataError::Validation(format!(
            "failed to read cache series dir {}: {source}",
            series_dir.display()
        ))
    })?;
    let mut symbols = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| {
            tqsdk::advanced::data::DataError::Validation(format!(
                "failed to read cache series entry in {}: {source}",
                series_dir.display()
            ))
        })?;
        if !entry
            .file_type()
            .map_err(|source| {
                tqsdk::advanced::data::DataError::Validation(format!(
                    "failed to read cache series file type {}: {source}",
                    entry.path().display()
                ))
            })?
            .is_dir()
        {
            continue;
        }
        symbols.push(entry.file_name().to_string_lossy().into_owned());
    }
    symbols.sort();
    if symbols.is_empty() {
        return Err(tqsdk::advanced::data::DataError::Validation(format!(
            "cache series dir {} contains no symbols",
            series_dir.display()
        ))
        .into());
    }
    Ok(symbols)
}

fn print_result(fields: &[(&str, String)]) {
    print!("BENCH_RESULT");
    for (key, value) in fields {
        print!("\t{key}={}", sanitize_field(value));
    }
    println!();
}

fn sanitize_field(value: &str) -> String {
    value.replace(['\t', '\n', '\r'], " ")
}
