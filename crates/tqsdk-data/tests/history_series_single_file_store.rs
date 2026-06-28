use std::path::{Path, PathBuf};

use tqsdk_core::Tick;
use tqsdk_data::{
    BacktestTickCache, HistorySeriesCache, HistorySeriesCoverageRequest, HistorySeriesKind,
    TickDataSeriesRequest,
};

const SERIES_FILE_FORMAT_ID: &str = "tqsdk.series-file.v1";

#[test]
fn backtest_tick_cache_open_uses_single_file_store() {
    let dir = temp_dir("backtest-open-single-file");

    let cache = BacktestTickCache::open(&dir).unwrap();

    assert_eq!(cache.history_cache().root_dir(), dir.as_path());
    assert_eq!(cache.history_cache().format_id(), SERIES_FILE_FORMAT_ID);
}

#[test]
fn series_file_store_uses_one_final_file_per_symbol_period() {
    let dir = temp_dir("one-file-per-symbol-period");
    let cache = BacktestTickCache::open(&dir).unwrap();

    cache
        .store_ticks(
            "SHFE.rb2601",
            1_000,
            4_000,
            [tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
        )
        .unwrap();

    let tick_file = dir.join("series").join("SHFE.rb2601").join("tick.tqseries");
    assert!(tick_file.is_file(), "missing {}", tick_file.display());

    let files = regular_files(&dir);
    assert_eq!(files, vec![tick_file]);
    assert!(!dir.join(".SHFE.rb2601.0.coverage").exists());
}

#[test]
fn series_file_store_embeds_coverage_and_reopens_complete_range() {
    let dir = temp_dir("embedded-coverage-reopen");
    let cache = BacktestTickCache::open(&dir).unwrap();

    cache
        .store_ticks(
            "DCE.i2601",
            1_000,
            5_000,
            [tick(1, 1_000, 100.0), tick(2, 3_000, 101.0)],
        )
        .unwrap();

    let reopened = BacktestTickCache::open(&dir).unwrap();
    let coverage = reopened.coverage("DCE.i2601", 1_000, 5_000).unwrap();
    assert!(coverage.is_complete());
    assert_eq!(coverage.cached_ranges, vec![(1_000, 5_000)]);
    assert_eq!(coverage.missing_ranges, Vec::<(i64, i64)>::new());

    let rows = reopened
        .load_series(TickDataSeriesRequest::new("DCE.i2601", 1_000, 5_000))
        .unwrap();
    assert_eq!(rows.iter().map(|row| row.id).collect::<Vec<_>>(), vec![1, 2]);
}

#[test]
fn series_file_store_partial_rows_do_not_create_coverage() {
    let dir = temp_dir("partial-rows-no-coverage");
    let history = HistorySeriesCache::open_series_file(&dir).unwrap();

    history
        .write_tick_rows_without_coverage(
            "SHFE.au2608",
            &[tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
        )
        .unwrap();

    let coverage = history
        .coverage(HistorySeriesCoverageRequest {
            symbol: "SHFE.au2608".to_string(),
            kind: HistorySeriesKind::Tick,
            range_start_ns: 1_000,
            range_end_ns: 3_000,
        })
        .unwrap();
    assert_eq!(coverage.cached_ranges, Vec::<(i64, i64)>::new());
    assert_eq!(coverage.missing_ranges, vec![(1_000, 3_000)]);
}

fn tick(id: i64, datetime: i64, last_price: f64) -> Tick {
    Tick {
        id,
        datetime,
        last_price,
        highest: last_price,
        lowest: last_price,
        average: last_price,
        bid_price1: last_price - 1.0,
        bid_volume1: 1,
        ask_price1: last_price + 1.0,
        ask_volume1: 1,
        ..Tick::default()
    }
}

fn temp_dir(name: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "tqsdk-series-file-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn regular_files(root: &Path) -> Vec<PathBuf> {
    fn walk(dir: &Path, files: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, files);
            } else if path.is_file() {
                files.push(path);
            }
        }
    }

    let mut files = Vec::new();
    walk(root, &mut files);
    files.sort();
    files
}
