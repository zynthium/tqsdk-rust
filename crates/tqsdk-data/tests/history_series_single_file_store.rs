use std::path::{Path, PathBuf};

use tqsdk_core::Tick;
use tqsdk_data::{
    BacktestTickCache, HISTORY_SERIES_CACHE_FORMAT_ID, HistorySeriesCache,
    HistorySeriesCacheFileStatus, TickDataSeriesRequest,
};

#[test]
fn backtest_tick_cache_open_uses_history_cache_store() {
    let dir = temp_dir("backtest-open-history-cache");

    let cache = BacktestTickCache::open(&dir).unwrap();
    let status = cache.inspect("SHFE.rb2601", 1_000, 2_000).unwrap();

    assert_eq!(status.cache_dir, dir);
    assert_eq!(status.backend_format, HISTORY_SERIES_CACHE_FORMAT_ID);
}

#[test]
fn history_cache_store_uses_one_final_file_per_symbol_period() {
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

    let tick_file = daily_tick_file(&dir, "19700101", "SHFE.rb2601");
    assert!(tick_file.is_file(), "missing {}", tick_file.display());

    let files = regular_files(&dir);
    assert_eq!(files, vec![tick_file]);
    assert!(!dir.join(".SHFE.rb2601.0.coverage").exists());
}

#[test]
fn history_cache_store_embeds_coverage_and_reopens_complete_range() {
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
    assert_eq!(
        rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn backtest_tick_cache_inspect_reports_backend_path_and_missing_ranges() {
    let dir = temp_dir("inspect-backend-path-missing");
    let cache = BacktestTickCache::open(&dir).unwrap();

    let path = cache.tick_series_path("SHFE.rb2601");
    assert_eq!(path, dir.join("series").join("tick").join("SHFE.rb2601"));

    cache
        .store_ticks(
            "SHFE.rb2601",
            1_000,
            3_000,
            [tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
        )
        .unwrap();

    let status = cache.inspect("SHFE.rb2601", 1_000, 5_000).unwrap();

    assert_eq!(status.backend_format, HISTORY_SERIES_CACHE_FORMAT_ID);
    assert_eq!(status.cache_dir, dir);
    assert_eq!(status.series_path, path);
    assert!(status.series_path_exists);
    assert!(!status.is_complete());
    assert_eq!(status.cached_ranges, vec![(1_000, 3_000)]);
    assert_eq!(status.missing_ranges, vec![(3_000, 5_000)]);
}

#[test]
fn backtest_tick_cache_purge_symbol_ticks_removes_rows_and_coverage() {
    let dir = temp_dir("purge-symbol-ticks");
    let cache = BacktestTickCache::open(&dir).unwrap();

    cache
        .store_ticks(
            "SHFE.rb2601",
            1_000,
            3_000,
            [tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
        )
        .unwrap();

    let path = cache.tick_series_path("SHFE.rb2601");
    let tick_file = daily_tick_file(&dir, "19700101", "SHFE.rb2601");
    assert!(tick_file.exists());

    let report = cache.purge_symbol_ticks("SHFE.rb2601").unwrap();
    assert_eq!(report.symbol, "SHFE.rb2601");
    assert_eq!(report.series_path, path);
    assert!(report.removed);
    assert!(!tick_file.exists());

    let coverage = cache.coverage("SHFE.rb2601", 1_000, 3_000).unwrap();
    assert_eq!(coverage.cached_ranges, Vec::<(i64, i64)>::new());
    assert_eq!(coverage.missing_ranges, vec![(1_000, 3_000)]);
}

#[test]
fn history_cache_store_partial_rows_do_not_create_coverage() {
    let dir = temp_dir("partial-rows-no-coverage");
    let cache = BacktestTickCache::open(&dir).unwrap();
    let rows = [tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)];

    cache.append_partial_ticks("SHFE.au2608", rows).unwrap();

    let coverage = cache.coverage("SHFE.au2608", 1_000, 3_000).unwrap();
    assert_eq!(coverage.cached_ranges, Vec::<(i64, i64)>::new());
    assert_eq!(coverage.missing_ranges, vec![(1_000, 3_000)]);
}

#[test]
fn history_cache_store_scan_reports_tqbn_rows() {
    let dir = temp_dir("scan-reports-tqbn");
    let cache = BacktestTickCache::open(&dir).unwrap();

    cache
        .store_ticks(
            "SHFE.rb2601",
            1_000,
            3_000,
            [tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
        )
        .unwrap();

    let scan = HistorySeriesCache::open(&dir).unwrap().scan().unwrap();

    assert_eq!(scan.files.len(), 1);
    let file = &scan.files[0];
    assert_eq!(file.path, daily_tick_file(&dir, "19700101", "SHFE.rb2601"));
    assert_eq!(file.status, HistorySeriesCacheFileStatus::Readable);
    assert_eq!(file.symbol.as_deref(), Some("SHFE.rb2601"));
    assert_eq!(file.duration_ns, Some(0));
    assert_eq!(file.id_range, Some((1, 3)));
    assert_eq!(file.rows, 2);
    assert_eq!(file.schema_version, Some(2));
    assert!(file.error.is_none());
}

#[test]
fn backtest_tick_cache_inventory_groups_tick_files_by_symbol() {
    let dir = temp_dir("tick-inventory-groups-symbols");
    let cache = BacktestTickCache::open(&dir).unwrap();
    let before_boundary = 35_999_999_999_000;
    let after_boundary = 36_000_000_000_000;

    cache
        .store_ticks(
            "SHFE.rb2601",
            before_boundary,
            after_boundary + 1_000,
            [
                tick(1, before_boundary, 100.0),
                tick(2, after_boundary, 101.0),
            ],
        )
        .unwrap();
    cache
        .store_ticks("DCE.i2601", 1_000, 3_000, [tick(7, 1_000, 200.0)])
        .unwrap();

    let inventory = cache.inventory().unwrap();

    assert_eq!(inventory.cache_dir, dir);
    assert_eq!(inventory.backend_format, HISTORY_SERIES_CACHE_FORMAT_ID);
    assert_eq!(inventory.total_files, 3);
    assert_eq!(inventory.total_rows, 3);
    assert_eq!(inventory.total_days, 2);
    assert_eq!(inventory.problem_files, 0);
    assert_eq!(inventory.symbols.len(), 2);

    let dce = inventory
        .symbols
        .iter()
        .find(|symbol| symbol.symbol == "DCE.i2601")
        .unwrap();
    assert_eq!(dce.files, 1);
    assert_eq!(dce.rows, 1);
    assert_eq!(dce.days, 1);
    assert_eq!(dce.id_range, Some((7, 8)));
    assert_eq!(dce.problem_files, 0);

    let shfe = inventory
        .symbols
        .iter()
        .find(|symbol| symbol.symbol == "SHFE.rb2601")
        .unwrap();
    assert_eq!(shfe.files, 2);
    assert_eq!(shfe.rows, 2);
    assert_eq!(shfe.days, 2);
    assert_eq!(shfe.id_range, Some((1, 3)));
    assert!(shfe.bytes > 0);
}

#[test]
fn history_cache_store_partitions_tick_files_by_trading_day() {
    let dir = temp_dir("daily-partition-ticks");
    let cache = BacktestTickCache::open(&dir).unwrap();
    let before_boundary = 35_999_999_999_000;
    let after_boundary = 36_000_000_000_000;

    cache
        .store_ticks(
            "SHFE.rb2601",
            before_boundary,
            after_boundary + 1_000,
            [
                tick(1, before_boundary, 100.0),
                tick(2, after_boundary, 101.0),
            ],
        )
        .unwrap();

    let day1 = daily_tick_file(&dir, "19700101", "SHFE.rb2601");
    let day2 = daily_tick_file(&dir, "19700102", "SHFE.rb2601");
    assert!(day1.is_file(), "missing {}", day1.display());
    assert!(day2.is_file(), "missing {}", day2.display());

    let scan = HistorySeriesCache::open(&dir).unwrap().scan().unwrap();
    assert_eq!(
        scan.files
            .iter()
            .map(|file| file.file_name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "19700101/tick/SHFE.rb2601.tqbn",
            "19700102/tick/SHFE.rb2601.tqbn"
        ]
    );

    let rows = cache
        .load_series(TickDataSeriesRequest::new(
            "SHFE.rb2601",
            before_boundary,
            after_boundary + 1_000,
        ))
        .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn history_cache_store_enforce_limits_removes_series_files_over_size_limit() {
    let dir = temp_dir("enforce-size-limit");
    let cache = BacktestTickCache::open(&dir).unwrap();

    for symbol in ["DCE.i2601", "DCE.j2601"] {
        cache
            .store_ticks(
                symbol,
                1_000,
                3_000,
                [tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
            )
            .unwrap();
    }

    let report = HistorySeriesCache::open(&dir)
        .unwrap()
        .enforce_limits(Some(0), None)
        .unwrap();

    assert_eq!(report.removed_files, 2);
    assert!(report.removed_bytes > 0);
    assert!(regular_files(&dir).is_empty());
}

#[test]
fn history_cache_store_enforce_limits_compacts_duplicate_appends() {
    let dir = temp_dir("compact-duplicate-appends");
    let cache = BacktestTickCache::open(&dir).unwrap();

    cache
        .store_ticks(
            "DCE.i2601",
            1_000,
            3_000,
            [tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
        )
        .unwrap();
    cache
        .store_ticks(
            "DCE.i2601",
            1_000,
            3_000,
            [tick(1, 1_000, 110.0), tick(2, 2_000, 111.0)],
        )
        .unwrap();

    let tick_file = daily_tick_file(&dir, "19700101", "DCE.i2601");
    let size_before = std::fs::metadata(&tick_file).unwrap().len();

    let report = HistorySeriesCache::open(&dir)
        .unwrap()
        .enforce_limits(None, None)
        .unwrap();

    let size_after = std::fs::metadata(&tick_file).unwrap().len();
    assert_eq!(report.removed_files, 0);
    assert!(
        size_after < size_before,
        "expected compacted file to shrink from {size_before} bytes, got {size_after}"
    );

    let rows = cache
        .load_series(TickDataSeriesRequest::new("DCE.i2601", 1_000, 3_000))
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.iter().map(|row| row.last_price).collect::<Vec<_>>(),
        vec![110.0, 111.0]
    );
}

#[test]
fn tick_fill_accumulator_marks_continuous_rows_complete() {
    let mut fill = tqsdk_data::BacktestTickFill::new("SHFE.rb2601", 1_000, 4_000);
    fill.push(tick(1, 1_000, 100.0)).unwrap();
    fill.push(tick(2, 2_000, 101.0)).unwrap();
    fill.push(tick(3, 3_500, 102.0)).unwrap();

    let report = fill.finish(1_000_000_000).unwrap();

    assert!(report.complete);
    assert_eq!(report.unique_rows, 3);
    assert_eq!(report.id_range, Some((1, 3)));
}

#[test]
fn tick_fill_accumulator_rejects_id_gap() {
    let mut fill = tqsdk_data::BacktestTickFill::new("SHFE.rb2601", 1_000, 4_000);
    fill.push(tick(1, 1_000, 100.0)).unwrap();
    fill.push(tick(3, 3_500, 102.0)).unwrap();

    let report = fill.finish(1_000_000_000).unwrap();

    assert!(!report.complete);
    assert_eq!(
        report.gap_summary.as_deref(),
        Some("tick id range 1..=3 contains 2 unique rows")
    );
}

#[test]
fn tick_fill_accumulator_accepts_continuous_idle_tail() {
    let mut fill = tqsdk_data::BacktestTickFill::new("SHFE.rb2601", 1_000, 10_000);
    fill.push(tick(1, 1_000, 100.0)).unwrap();
    fill.push(tick(2, 2_000, 101.0)).unwrap();

    let strict = fill.finish(1_000).unwrap();
    let idle = fill.finish_after_idle(1_000).unwrap();

    assert!(!strict.complete);
    assert!(idle.complete);
    assert_eq!(idle.unique_rows, 2);
    assert_eq!(idle.id_range, Some((1, 2)));
}

#[test]
fn tick_fill_accumulator_accepts_empty_idle_slice() {
    let fill = tqsdk_data::BacktestTickFill::new("SHFE.rb2601", 1_000, 10_000);

    let strict = fill.finish(1_000).unwrap();
    let idle = fill.finish_after_idle(1_000).unwrap();

    assert!(!strict.complete);
    assert!(idle.complete);
    assert_eq!(idle.unique_rows, 0);
    assert_eq!(idle.id_range, None);
    assert_eq!(idle.first_datetime_ns, None);
    assert_eq!(idle.last_datetime_ns, None);
    assert_eq!(idle.gap_summary, None);
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
        "tqsdk-history-cache-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn daily_tick_file(root: &Path, day: &str, symbol: &str) -> PathBuf {
    root.join("series")
        .join(day)
        .join("tick")
        .join(format!("{}.tqbn", symbol.replace('/', "%2F")))
}

fn regular_files(root: &Path) -> Vec<PathBuf> {
    fn walk(dir: &Path, files: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, files);
            } else if path.is_file()
                && path.file_name().and_then(|name| name.to_str()) != Some(".tqbn.lock")
            {
                files.push(path);
            }
        }
    }

    let mut files = Vec::new();
    walk(root, &mut files);
    files.sort();
    files
}
