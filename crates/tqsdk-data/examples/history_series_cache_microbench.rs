#![forbid(unsafe_code)]

use std::env;
use std::error::Error;
use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tqsdk_core::Tick;
use tqsdk_data::{
    BacktestTickCache, HistorySeriesCache, LiveTickCacheWriter, TickDataSeriesRequest,
};

const SYMBOL: &str = "SHFE.rb2601";
const DEFAULT_ROWS: usize = 100_000;
const DEFAULT_LIVE_WRITER_ROWS: usize = 1_000;
const DEFAULT_LIVE_WRITER_BATCH_ROWS: usize = 128;
const DEFAULT_SCAN_SYMBOLS: usize = 100;
const DEFAULT_SCAN_ROWS_PER_SYMBOL: usize = 10;
const DEFAULT_COMPACT_ROWS: usize = 50_000;
const INPUT_CACHE_DIR_ENV: &str = "TQSDK_HISTORY_CACHE_BENCH_INPUT_CACHE_DIR";
const INPUT_SYMBOL_ENV: &str = "TQSDK_HISTORY_CACHE_BENCH_INPUT_SYMBOL";
const INPUT_START_NS_ENV: &str = "TQSDK_HISTORY_CACHE_BENCH_INPUT_START_NS";
const INPUT_END_NS_ENV: &str = "TQSDK_HISTORY_CACHE_BENCH_INPUT_END_NS";
type CacheTickSource = (PathBuf, String, i64, i64);

fn main() -> Result<(), Box<dyn Error>> {
    let rows = env_usize("TQSDK_HISTORY_CACHE_BENCH_ROWS", DEFAULT_ROWS);
    let live_writer_rows = env_usize(
        "TQSDK_HISTORY_CACHE_BENCH_LIVE_WRITER_ROWS",
        DEFAULT_LIVE_WRITER_ROWS,
    );
    let live_writer_batch_rows = env_usize(
        "TQSDK_HISTORY_CACHE_BENCH_LIVE_WRITER_BATCH_ROWS",
        DEFAULT_LIVE_WRITER_BATCH_ROWS,
    );
    let scan_symbols = env_usize(
        "TQSDK_HISTORY_CACHE_BENCH_SCAN_SYMBOLS",
        DEFAULT_SCAN_SYMBOLS,
    );
    let scan_rows_per_symbol = env_usize(
        "TQSDK_HISTORY_CACHE_BENCH_SCAN_ROWS_PER_SYMBOL",
        DEFAULT_SCAN_ROWS_PER_SYMBOL,
    );
    let compact_rows = env_usize(
        "TQSDK_HISTORY_CACHE_BENCH_COMPACT_ROWS",
        DEFAULT_COMPACT_ROWS,
    );
    let keep = env_bool("TQSDK_HISTORY_CACHE_BENCH_KEEP");
    let cache_source = cache_tick_source_from_env()?;
    let input_ticks = match cache_source.as_ref() {
        Some((root, symbol, start_ns, end_ns)) => {
            load_cache_ticks(root, symbol, *start_ns, *end_ns)?
        }
        None => ticks(rows, 1_713_660_000_000_000_000),
    };

    let root = temp_root()?;
    println!("tqsdk-data history series cache microbench");
    println!("profile: run with --release for useful numbers");
    println!(
        "format: {}",
        HistorySeriesCache::open(root.join("format"))?.format_id()
    );
    match cache_source.as_ref() {
        Some((cache_dir, symbol, start_ns, end_ns)) => println!(
            "write/read input: cache {} {} [{start_ns}, {end_ns})",
            cache_dir.display(),
            symbol
        ),
        None => println!("write/read input: synthetic {} rows", input_ticks.len()),
    }
    println!("root: {}", root.display());
    println!();

    println!(
        "{:<30} {:>12} {:>12} {:>14} {:>14}",
        "case", "items", "ms", "ns/item", "bytes"
    );
    let write_read = run_write_read_zstd(root.join("write-read"), input_ticks.as_slice())?;
    print_result(&write_read.write);
    print_result(&write_read.coverage);
    print_result(&write_read.read);
    print_result(&write_read.read_slice);
    for result in &write_read.compression {
        print_compression(result);
    }

    let live_writer = run_live_tick_writer(
        root.join("live-writer-single"),
        live_writer_rows,
        1,
        "live_record_ticks",
    )?;
    print_result(&live_writer);
    let live_writer_buffered = run_live_tick_writer(
        root.join("live-writer-buffered"),
        live_writer_rows,
        live_writer_batch_rows,
        "live_record_ticks_buffered",
    )?;
    print_result(&live_writer_buffered);

    let scan = run_scan(root.join("scan"), scan_symbols, scan_rows_per_symbol)?;
    print_result(&scan);

    let compact = run_compaction(root.join("compact"), compact_rows)?;
    print_result(&compact.compact);
    println!(
        "{:<30} {:>12} {:>12} {:>13.1}% {:>14}",
        "compact_size_delta",
        compact.rows,
        "-",
        compact.shrink_percent(),
        format_bytes_delta(compact.bytes_before, compact.bytes_after)
    );

    if keep {
        println!();
        println!("kept benchmark root: {}", root.display());
    } else {
        let _ = fs::remove_dir_all(&root);
    }

    Ok(())
}

fn run_write_read_zstd(root: PathBuf, ticks: &[Tick]) -> Result<WriteReadReport, Box<dyn Error>> {
    if ticks.is_empty() {
        return Err("write/read benchmark input contains no ticks".into());
    }
    let cache = HistorySeriesCache::open(&root)?;
    let start_ns = ticks.first().map_or(0, |row| row.datetime);
    let end_ns = ticks.last().map_or(start_ns.saturating_add(1), |row| {
        row.datetime.saturating_add(1)
    });

    let start = Instant::now();
    cache.write_tick_range(SYMBOL, start_ns, end_ns, ticks)?;
    let write_elapsed = start.elapsed();
    let series_path = single_tqbn_file(&root)?;
    let bytes = tqbn_size_bytes(&root)?;
    let write = BenchResult::new("write_ticks", ticks.len(), write_elapsed, bytes);

    let start = Instant::now();
    let coverage = cache.tick_coverage(SYMBOL, start_ns, end_ns)?;
    let coverage_elapsed = start.elapsed();
    black_box(&coverage);
    let coverage = BenchResult::new("inspect_tick_coverage", 1, coverage_elapsed, bytes);

    let start = Instant::now();
    let series =
        cache.read_tick_data_series(TickDataSeriesRequest::new(SYMBOL, start_ns, end_ns))?;
    let read_elapsed = start.elapsed();
    black_box(series.rows());
    let read = BenchResult::new("read_ticks", series.len(), read_elapsed, bytes);

    let slice_rows = (ticks.len() / 100).max(1);
    let slice_start = (ticks.len() - slice_rows) / 2;
    let slice = &ticks[slice_start..slice_start + slice_rows];
    let slice_start_ns = slice.first().unwrap().datetime;
    let slice_end_ns = slice.last().unwrap().datetime.saturating_add(1);
    let start = Instant::now();
    let slice_series = cache.read_tick_data_series(TickDataSeriesRequest::new(
        SYMBOL,
        slice_start_ns,
        slice_end_ns,
    ))?;
    let read_slice_elapsed = start.elapsed();
    black_box(slice_series.rows());
    let read_slice = BenchResult::new(
        "read_ticks_1pct",
        slice_series.len(),
        read_slice_elapsed,
        bytes,
    );

    let mut compression = Vec::new();
    for level in [1_u8, 3_u8] {
        if let Some(result) = run_zstd(&series_path, level)? {
            compression.push(result);
        }
    }

    Ok(WriteReadReport {
        write,
        coverage,
        read,
        read_slice,
        compression,
    })
}

fn run_live_tick_writer(
    root: PathBuf,
    rows: usize,
    batch_rows: usize,
    name: &'static str,
) -> Result<BenchResult, Box<dyn Error>> {
    let cache = BacktestTickCache::open(&root)?;
    let mut writer = LiveTickCacheWriter::new(cache.clone());
    let ticks = ticks(rows, 1_713_660_000_000_000_000);
    let start_ns = ticks.first().map_or(0, |row| row.datetime);
    let end_ns = ticks.last().map_or(start_ns.saturating_add(1), |row| {
        row.datetime.saturating_add(1)
    });

    let start = Instant::now();
    for batch in ticks.chunks(batch_rows.max(1)) {
        writer.push_ticks(SYMBOL, batch.iter().cloned())?;
    }
    writer.flush()?;
    let elapsed = start.elapsed();
    black_box(cache.coverage(SYMBOL, start_ns, end_ns)?);

    Ok(BenchResult::new(
        name,
        rows,
        elapsed,
        tqbn_size_bytes(&root)?,
    ))
}

fn run_scan(
    root: PathBuf,
    symbols: usize,
    rows_per_symbol: usize,
) -> Result<BenchResult, Box<dyn Error>> {
    let cache = HistorySeriesCache::open(&root)?;
    for index in 0..symbols {
        let symbol = format!("SHFE.scan{index:04}");
        let base_ns = 1_713_660_000_000_000_000_i64 + (index as i64) * 1_000_000_000;
        let rows = ticks(rows_per_symbol, base_ns);
        cache.write_tick_range(
            &symbol,
            base_ns,
            base_ns + rows_per_symbol as i64 * 1_000_000,
            &rows,
        )?;
    }

    let start = Instant::now();
    let scan = cache.scan()?;
    let elapsed = start.elapsed();
    black_box(&scan);
    Ok(BenchResult::new(
        "scan_symbol_files",
        scan.files.len(),
        elapsed,
        dir_size_bytes(&root)?,
    ))
}

fn run_compaction(root: PathBuf, rows: usize) -> Result<CompactionReport, Box<dyn Error>> {
    let cache = HistorySeriesCache::open(&root)?;
    let first = ticks(rows, 1_713_660_000_000_000_000);
    let second = ticks_with_price_offset(rows, 1_713_660_000_000_000_000, 5.0);
    let start_ns = first.first().map_or(0, |row| row.datetime);
    let end_ns = first.last().map_or(start_ns.saturating_add(1), |row| {
        row.datetime.saturating_add(1)
    });

    cache.write_tick_range(SYMBOL, start_ns, end_ns, &first)?;
    cache.write_tick_range(SYMBOL, start_ns, end_ns, &second)?;
    let bytes_before = tqbn_size_bytes(&root)?;

    let start = Instant::now();
    let maintenance = cache.enforce_limits(None, None)?;
    let elapsed = start.elapsed();
    black_box(maintenance);

    let bytes_after = tqbn_size_bytes(&root)?;
    Ok(CompactionReport {
        compact: BenchResult::new("compact_duplicate_ticks", rows, elapsed, bytes_after),
        rows,
        bytes_before,
        bytes_after,
    })
}

fn run_zstd(path: &Path, level: u8) -> Result<Option<CompressionResult>, Box<dyn Error>> {
    let output_path = path.with_extension(format!("tqbn.zst{level}"));
    let input_bytes = fs::metadata(path)?.len();
    let start = Instant::now();
    let status = match Command::new("zstd")
        .arg("-q")
        .arg("-f")
        .arg(format!("-{level}"))
        .arg(path)
        .arg("-o")
        .arg(&output_path)
        .status()
    {
        Ok(status) => status,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Box::new(error)),
    };
    let elapsed = start.elapsed();
    if !status.success() {
        return Ok(None);
    }
    let compressed_bytes = fs::metadata(&output_path)?.len();
    Ok(Some(CompressionResult {
        level,
        input_bytes,
        compressed_bytes,
        elapsed,
    }))
}

fn ticks(rows: usize, start_datetime_ns: i64) -> Vec<Tick> {
    ticks_with_price_offset(rows, start_datetime_ns, 0.0)
}

fn ticks_with_price_offset(rows: usize, start_datetime_ns: i64, price_offset: f64) -> Vec<Tick> {
    (0..rows)
        .map(|index| {
            let id = index as i64;
            let datetime = start_datetime_ns + id * 1_000_000;
            let last_price = 3_500.0 + price_offset + (index % 500) as f64 * 0.2;
            Tick {
                id,
                datetime,
                last_price,
                highest: last_price + 1.0,
                lowest: last_price - 1.0,
                average: last_price + 0.5,
                volume: id * 10,
                amount: last_price * 10.0,
                open_interest: 100_000 + id,
                bid_price1: last_price - 0.2,
                bid_volume1: 10 + (index % 50) as i64,
                ask_price1: last_price + 0.2,
                ask_volume1: 11 + (index % 50) as i64,
                bid_price2: last_price - 0.4,
                bid_volume2: 8 + (index % 50) as i64,
                ask_price2: last_price + 0.4,
                ask_volume2: 9 + (index % 50) as i64,
                bid_price3: last_price - 0.6,
                bid_volume3: 6 + (index % 50) as i64,
                ask_price3: last_price + 0.6,
                ask_volume3: 7 + (index % 50) as i64,
                bid_price4: last_price - 0.8,
                bid_volume4: 4 + (index % 50) as i64,
                ask_price4: last_price + 0.8,
                ask_volume4: 5 + (index % 50) as i64,
                bid_price5: last_price - 1.0,
                bid_volume5: 2 + (index % 50) as i64,
                ask_price5: last_price + 1.0,
                ask_volume5: 3 + (index % 50) as i64,
                ..Tick::default()
            }
        })
        .collect()
}

fn temp_root() -> Result<PathBuf, Box<dyn Error>> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = env::temp_dir().join(format!(
        "tqsdk-history-cache-microbench-{}-{nanos}",
        std::process::id()
    ));
    fs::create_dir_all(&root)?;
    Ok(root)
}

fn dir_size_bytes(path: &Path) -> Result<u64, Box<dyn Error>> {
    let mut total = 0_u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total += dir_size_bytes(&entry.path())?;
        } else if metadata.is_file() {
            total += metadata.len();
        }
    }
    Ok(total)
}

fn single_tqbn_file(root: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let mut files = tqbn_files(root)?;
    files.sort();
    files
        .into_iter()
        .next()
        .ok_or_else(|| "benchmark cache contains no TQBN file".into())
}

fn tqbn_size_bytes(root: &Path) -> Result<u64, Box<dyn Error>> {
    tqbn_files(root)?
        .into_iter()
        .try_fold(0_u64, |total, path| {
            Ok(total.saturating_add(fs::metadata(path)?.len()))
        })
}

fn tqbn_files(root: &Path) -> Result<Vec<PathBuf>, Box<dyn Error>> {
    let mut files = Vec::new();
    collect_tqbn_files(root, &mut files)?;
    Ok(files)
}

fn collect_tqbn_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<(), Box<dyn Error>> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let entry_path = entry.path();
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            collect_tqbn_files(&entry_path, files)?;
        } else if metadata.is_file()
            && entry_path
                .extension()
                .is_some_and(|extension| extension == "tqbn")
        {
            files.push(entry_path);
        }
    }
    Ok(())
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_bool(name: &str) -> bool {
    env::var(name)
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
}

fn cache_tick_source_from_env() -> Result<Option<CacheTickSource>, Box<dyn Error>> {
    parse_cache_tick_source(
        env::var(INPUT_CACHE_DIR_ENV).ok().as_deref(),
        env::var(INPUT_SYMBOL_ENV).ok().as_deref(),
        env::var(INPUT_START_NS_ENV).ok().as_deref(),
        env::var(INPUT_END_NS_ENV).ok().as_deref(),
    )
    .map_err(Into::into)
}

fn parse_cache_tick_source(
    cache_dir: Option<&str>,
    symbol: Option<&str>,
    start_ns: Option<&str>,
    end_ns: Option<&str>,
) -> Result<Option<CacheTickSource>, String> {
    let has_non_root_value = symbol.is_some() || start_ns.is_some() || end_ns.is_some();
    let Some(cache_dir) = cache_dir else {
        return if has_non_root_value {
            Err(format!(
                "{INPUT_CACHE_DIR_ENV} is required when configuring a cache benchmark input"
            ))
        } else {
            Ok(None)
        };
    };
    let Some(symbol) = symbol.filter(|value| !value.is_empty()) else {
        return Err(format!(
            "{INPUT_SYMBOL_ENV} is required with {INPUT_CACHE_DIR_ENV}"
        ));
    };
    let (Some(start_ns), Some(end_ns)) = (start_ns, end_ns) else {
        return Err(format!(
            "{INPUT_START_NS_ENV} and {INPUT_END_NS_ENV} are required with {INPUT_CACHE_DIR_ENV}"
        ));
    };
    let start_ns = start_ns
        .parse::<i64>()
        .map_err(|error| format!("{INPUT_START_NS_ENV} must be an i64 timestamp: {error}"))?;
    let end_ns = end_ns
        .parse::<i64>()
        .map_err(|error| format!("{INPUT_END_NS_ENV} must be an i64 timestamp: {error}"))?;
    if start_ns >= end_ns {
        return Err(format!(
            "{INPUT_START_NS_ENV} must be less than {INPUT_END_NS_ENV}"
        ));
    }
    Ok(Some((
        PathBuf::from(cache_dir),
        symbol.to_string(),
        start_ns,
        end_ns,
    )))
}

fn load_cache_ticks(
    cache_dir: &Path,
    symbol: &str,
    start_ns: i64,
    end_ns: i64,
) -> Result<Vec<Tick>, Box<dyn Error>> {
    let cache = HistorySeriesCache::open_read_only(cache_dir);
    let ticks = cache
        .read_tick_data_series(TickDataSeriesRequest::new(symbol, start_ns, end_ns))?
        .into_rows();
    if ticks.is_empty() {
        return Err("cache benchmark input contains no ticks".into());
    }
    Ok(ticks)
}

struct WriteReadReport {
    write: BenchResult,
    coverage: BenchResult,
    read: BenchResult,
    read_slice: BenchResult,
    compression: Vec<CompressionResult>,
}

struct CompactionReport {
    compact: BenchResult,
    rows: usize,
    bytes_before: u64,
    bytes_after: u64,
}

impl CompactionReport {
    fn shrink_percent(&self) -> f64 {
        if self.bytes_before == 0 {
            0.0
        } else {
            (self.bytes_before.saturating_sub(self.bytes_after)) as f64 * 100.0
                / self.bytes_before as f64
        }
    }
}

struct BenchResult {
    name: &'static str,
    items: usize,
    elapsed: Duration,
    bytes: u64,
}

impl BenchResult {
    fn new(name: &'static str, items: usize, elapsed: Duration, bytes: u64) -> Self {
        Self {
            name,
            items,
            elapsed,
            bytes,
        }
    }

    fn ns_per_item(&self) -> f64 {
        if self.items == 0 {
            0.0
        } else {
            self.elapsed.as_nanos() as f64 / self.items as f64
        }
    }
}

struct CompressionResult {
    level: u8,
    input_bytes: u64,
    compressed_bytes: u64,
    elapsed: Duration,
}

impl CompressionResult {
    fn ratio(&self) -> f64 {
        if self.input_bytes == 0 {
            0.0
        } else {
            self.compressed_bytes as f64 / self.input_bytes as f64
        }
    }

    fn throughput_mb_s(&self) -> f64 {
        let seconds = self.elapsed.as_secs_f64();
        if seconds == 0.0 {
            0.0
        } else {
            self.input_bytes as f64 / 1_048_576.0 / seconds
        }
    }
}

fn print_result(result: &BenchResult) {
    println!(
        "{:<30} {:>12} {:>12.2} {:>14.1} {:>14}",
        result.name,
        result.items,
        result.elapsed.as_secs_f64() * 1_000.0,
        result.ns_per_item(),
        result.bytes
    );
}

fn print_compression(result: &CompressionResult) {
    println!(
        "{:<30} {:>12} {:>12.2} {:>13.1}% {:>14}",
        format!("zstd_level_{}", result.level),
        result.input_bytes,
        result.elapsed.as_secs_f64() * 1_000.0,
        result.ratio() * 100.0,
        format!(
            "{} ({:.1} MiB/s)",
            result.compressed_bytes,
            result.throughput_mb_s()
        )
    );
}

fn format_bytes_delta(before: u64, after: u64) -> String {
    if after <= before {
        format!("-{}", before - after)
    } else {
        format!("+{}", after - before)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::parse_cache_tick_source;

    #[test]
    fn cache_tick_source_requires_all_config_values() {
        let error = parse_cache_tick_source(Some("/tmp/cache"), Some("SHFE.rb2601"), None, None)
            .unwrap_err();

        assert_eq!(
            error,
            "TQSDK_HISTORY_CACHE_BENCH_INPUT_START_NS and TQSDK_HISTORY_CACHE_BENCH_INPUT_END_NS are required with TQSDK_HISTORY_CACHE_BENCH_INPUT_CACHE_DIR"
        );
    }

    #[test]
    fn cache_tick_source_parses_complete_config() {
        let source = parse_cache_tick_source(
            Some("/tmp/cache"),
            Some("SHFE.rb2601"),
            Some("100"),
            Some("200"),
        )
        .unwrap();

        assert_eq!(
            source,
            Some((
                PathBuf::from("/tmp/cache"),
                "SHFE.rb2601".to_string(),
                100,
                200
            ))
        );
    }
}
