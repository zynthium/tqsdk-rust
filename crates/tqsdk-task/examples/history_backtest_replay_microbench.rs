#![forbid(unsafe_code)]

use std::{
    env,
    error::Error,
    fs,
    hint::black_box,
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use tqsdk_core::Tick;
use tqsdk_data::HistorySeriesCache;
use tqsdk_task::{
    HistoryBacktestReplayRequest, HistoryBacktestReplayStream, MAX_HISTORY_REPLAY_BATCH_EVENTS,
    ReplayMarketEvent,
};

const DEFAULT_SYMBOL_COUNTS: &[usize] = &[10, 100, 500, 1_000];
const DEFAULT_ROWS_PER_SYMBOL: usize = 256;
const DEFAULT_BATCH_SIZE: usize = 256;
const TICK_INTERVAL_NS: i64 = 1_000_000;
const START_NS: i64 = 1_713_660_000_000_000_000;

fn main() -> Result<(), Box<dyn Error>> {
    let symbol_counts = env_usize_list("TQSDK_BACKTEST_REPLAY_BENCH_SYMBOLS")
        .unwrap_or_else(|| DEFAULT_SYMBOL_COUNTS.to_vec());
    let rows_per_symbol = env_usize(
        "TQSDK_BACKTEST_REPLAY_BENCH_ROWS_PER_SYMBOL",
        DEFAULT_ROWS_PER_SYMBOL,
    );
    let batch_size = env_usize("TQSDK_BACKTEST_REPLAY_BENCH_BATCH_SIZE", DEFAULT_BATCH_SIZE);
    if !(1..=MAX_HISTORY_REPLAY_BATCH_EVENTS).contains(&batch_size) {
        return Err(format!(
            "TQSDK_BACKTEST_REPLAY_BENCH_BATCH_SIZE must be within 1..={MAX_HISTORY_REPLAY_BATCH_EVENTS}"
        )
        .into());
    }

    println!("tqsdk-task CacheOnly TQBN decode/merge/callback microbench");
    println!("profile: run with --release for useful numbers");
    println!(
        "{:<12} {:>8} {:>10} {:>10} {:>12} {:>13} {:>13} {:>13} {:>13} {:>13}",
        "mode",
        "symbols",
        "rows/sym",
        "batch",
        "events",
        "p50 ns/event",
        "p95 ns/event",
        "p99 ns/event",
        "p999 ns/event",
        "events/s",
    );

    for symbol_count in symbol_counts {
        let reports = run_case(symbol_count, rows_per_symbol, batch_size)?;
        for report in reports {
            print_result(&report);
        }
    }

    Ok(())
}

fn run_case(
    symbol_count: usize,
    rows_per_symbol: usize,
    batch_size: usize,
) -> Result<[BenchResult; 2], Box<dyn Error>> {
    let root = temp_root()?;
    let result = (|| {
        let cache = HistorySeriesCache::open(&root)?;
        let symbols = (0..symbol_count)
            .map(|index| format!("SHFE.replay{index:04}"))
            .collect::<Vec<_>>();
        let end_ns =
            START_NS.saturating_add(usize_to_i64(rows_per_symbol).saturating_mul(TICK_INTERVAL_NS));

        for (symbol_index, symbol) in symbols.iter().enumerate() {
            cache.write_tick_range(
                symbol,
                START_NS,
                end_ns,
                &ticks(rows_per_symbol, symbol_index),
            )?;
        }

        Ok([
            measure_replay(
                cache.clone(),
                &symbols,
                START_NS,
                end_ns,
                1,
                "single",
                rows_per_symbol,
            )?,
            measure_replay(
                cache,
                &symbols,
                START_NS,
                end_ns,
                batch_size,
                "batch",
                rows_per_symbol,
            )?,
        ])
    })();
    let cleanup = fs::remove_dir_all(&root);

    match (result, cleanup) {
        (Ok(reports), Ok(())) => Ok(reports),
        (Ok(_), Err(error)) => Err(Box::new(error)),
        (Err(error), _) => Err(error),
    }
}

fn measure_replay(
    cache: HistorySeriesCache,
    symbols: &[String],
    start_ns: i64,
    end_ns: i64,
    batch_size: usize,
    mode: &'static str,
    rows_per_symbol: usize,
) -> Result<BenchResult, Box<dyn Error>> {
    let opened = Instant::now();
    let mut stream = HistoryBacktestReplayStream::new(HistoryBacktestReplayRequest {
        cache,
        start_ns,
        end_ns,
        tick_symbols: symbols.to_vec(),
        native_klines: Vec::new(),
        synthetic_klines: Vec::new(),
    })?;
    let open_elapsed = opened.elapsed();

    let started = Instant::now();
    let mut latencies = Vec::new();
    let mut events = 0_usize;
    let mut strategy_elapsed = Duration::ZERO;
    loop {
        let batch_started = Instant::now();
        let batch = stream.next_batch_sync(batch_size)?;
        if batch.is_empty() {
            break;
        }
        let strategy_started = Instant::now();
        for event in &batch {
            strategy_callback(event);
        }
        strategy_elapsed = strategy_elapsed.saturating_add(strategy_started.elapsed());
        let elapsed_per_event = duration_per_event(batch_started.elapsed(), batch.len());
        latencies.push(elapsed_per_event);
        events = events.saturating_add(batch.len());
    }

    Ok(BenchResult {
        mode,
        symbols: symbols.len(),
        rows_per_symbol,
        batch_size,
        events,
        open_elapsed,
        elapsed: started.elapsed(),
        strategy_elapsed,
        latencies,
    })
}

fn strategy_callback(event: &ReplayMarketEvent) {
    black_box((event.symbol(), event.event_time_ns(), event.payload_kind()));
}

fn ticks(rows: usize, symbol_index: usize) -> Vec<Tick> {
    (0..rows)
        .map(|row_index| {
            let row_id = usize_to_i64(row_index);
            Tick {
                id: row_id,
                datetime: START_NS.saturating_add(row_id.saturating_mul(TICK_INTERVAL_NS)),
                last_price: 3_500.0 + symbol_index as f64 + row_index as f64 * 0.01,
                volume: row_id.saturating_mul(10),
                ..Tick::default()
            }
        })
        .collect()
}

fn duration_per_event(elapsed: Duration, events: usize) -> Duration {
    let events = u128::try_from(events).unwrap_or(u128::MAX).max(1);
    let nanos = elapsed.as_nanos() / events;
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

fn temp_root() -> Result<PathBuf, Box<dyn Error>> {
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = env::temp_dir().join(format!(
        "tqsdk-history-backtest-replay-microbench-{}-{suffix}",
        std::process::id()
    ));
    fs::create_dir(&root)?;
    Ok(root)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_usize_list(name: &str) -> Option<Vec<usize>> {
    env::var(name).ok().as_deref().and_then(parse_usize_list)
}

fn parse_usize_list(value: &str) -> Option<Vec<usize>> {
    let values = value
        .split(',')
        .filter_map(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .collect::<Vec<_>>();
    (!values.is_empty()).then_some(values)
}

fn usize_to_i64(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

struct BenchResult {
    mode: &'static str,
    symbols: usize,
    rows_per_symbol: usize,
    batch_size: usize,
    events: usize,
    open_elapsed: Duration,
    elapsed: Duration,
    strategy_elapsed: Duration,
    latencies: Vec<Duration>,
}

impl BenchResult {
    fn events_per_second(&self) -> f64 {
        let seconds = self.elapsed.as_secs_f64();
        if seconds == 0.0 {
            return 0.0;
        }
        self.events as f64 / seconds
    }

    fn percentile_ns(&self, percentile: f64) -> f64 {
        if self.latencies.is_empty() {
            return 0.0;
        }
        let mut samples = self
            .latencies
            .iter()
            .map(Duration::as_nanos)
            .collect::<Vec<_>>();
        samples.sort_unstable();
        let index = ((samples.len() - 1) as f64 * percentile).ceil() as usize;
        samples[index] as f64
    }
}

fn print_result(result: &BenchResult) {
    println!(
        "{:<12} {:>8} {:>10} {:>10} {:>12} {:>13.1} {:>13.1} {:>13.1} {:>13.1} {:>13.1}",
        result.mode,
        result.symbols,
        result.rows_per_symbol,
        result.batch_size,
        result.events,
        result.percentile_ns(0.50),
        result.percentile_ns(0.95),
        result.percentile_ns(0.99),
        result.percentile_ns(0.999),
        result.events_per_second(),
    );
    println!(
        "  mode={} stream_open_ns={} strategy_callback_ns_per_event={} events={} batch_size={}",
        result.mode,
        result.open_elapsed.as_nanos(),
        duration_per_event(result.strategy_elapsed, result.events).as_nanos(),
        result.events,
        result.batch_size,
    );
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{BenchResult, duration_per_event, parse_usize_list};

    #[test]
    fn parses_symbol_counts() {
        assert_eq!(parse_usize_list("1, 10, 0, 100"), Some(vec![1, 10, 100]));
        assert_eq!(parse_usize_list("0, nope"), None);
    }

    #[test]
    fn reports_normalized_batch_latency() {
        assert_eq!(
            duration_per_event(Duration::from_nanos(10), 4),
            Duration::from_nanos(2)
        );

        let result = BenchResult {
            mode: "batch",
            symbols: 1,
            rows_per_symbol: 5,
            batch_size: 5,
            events: 5,
            open_elapsed: Duration::from_nanos(1),
            elapsed: Duration::from_secs(2),
            strategy_elapsed: Duration::from_nanos(5),
            latencies: vec![
                Duration::from_nanos(1),
                Duration::from_nanos(2),
                Duration::from_nanos(3),
                Duration::from_nanos(4),
                Duration::from_nanos(5),
            ],
        };

        assert_eq!(result.percentile_ns(0.50), 3.0);
        assert_eq!(result.percentile_ns(0.999), 5.0);
        assert_eq!(result.events_per_second(), 2.5);
    }
}
