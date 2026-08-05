use std::path::PathBuf;

use chrono::NaiveDate;
use tqsdk_core::Tick;
use tqsdk_data::{
    BacktestTickCache, HistorySeriesCache, HistorySeriesCacheFileStatus, TickDataSeriesRequest,
    backtest_tick_trading_day_range,
};

#[test]
fn tqbn_scan_ignores_legacy_tqseries_files() {
    let dir = temp_dir("scan-ignores-legacy");
    let legacy_path = dir.join("series").join("SHFE.rb2601").join("tick.tqseries");
    std::fs::create_dir_all(legacy_path.parent().unwrap()).unwrap();
    std::fs::write(&legacy_path, b"legacy").unwrap();

    let scan = HistorySeriesCache::open(&dir).unwrap().scan().unwrap();

    assert!(scan.files.is_empty(), "{scan:?}");
}

#[test]
fn tqbn_scan_ignores_sidecar_lock_files() {
    let dir = temp_dir("scan-ignores-lock");
    let cache = HistorySeriesCache::open(&dir).unwrap();

    cache
        .write_tick_range("DCE.i2601", 1_000, 2_000, &[tick(7, 1_000, 100.0)])
        .unwrap();

    let lock_path = dir
        .join("series")
        .join("19700101")
        .join("tick")
        .join("DCE.i2601.tqbn.lock");
    assert!(lock_path.is_file(), "missing {}", lock_path.display());

    let scan = cache.scan().unwrap();
    assert_eq!(scan.files.len(), 1);
    assert_eq!(scan.files[0].file_name, "19700101/tick/DCE.i2601.tqbn");
}

#[test]
fn tqbn_enforce_limits_compacts_duplicate_rows_last_write_wins() {
    let dir = temp_dir("duplicate-tick-compaction");
    let cache = HistorySeriesCache::open(&dir).unwrap();

    cache
        .write_tick_range("DCE.i2601", 1_000, 2_000, &[tick(7, 1_000, 100.0)])
        .unwrap();
    cache
        .write_tick_range("DCE.i2601", 1_000, 2_000, &[tick(7, 1_000, 110.0)])
        .unwrap();

    let path = daily_tick_file(&dir, "19700101", "DCE.i2601");
    let size_before = std::fs::metadata(&path).unwrap().len();

    let report = cache.enforce_limits(None, None).unwrap();

    let size_after = std::fs::metadata(&path).unwrap().len();
    assert_eq!(report.removed_files, 0);
    assert!(
        size_after < size_before,
        "expected compacted file to shrink from {size_before} bytes, got {size_after}"
    );

    let rows = cache
        .read_tick_data_series(TickDataSeriesRequest::new("DCE.i2601", 1_000, 2_000))
        .unwrap();
    assert_eq!(rows.rows().len(), 1);
    assert_eq!(rows.rows()[0].last_price, 110.0);

    let scan = cache.scan().unwrap();
    assert_eq!(scan.files.len(), 1);
    assert_eq!(scan.files[0].status, HistorySeriesCacheFileStatus::Readable);
    assert_eq!(scan.files[0].rows, 1);
    assert_eq!(scan.files[0].id_range, Some((7, 8)));
}

#[test]
fn tqbn_compaction_preserves_empty_declared_coverage() {
    let dir = temp_dir("empty-coverage-compaction");
    let cache = BacktestTickCache::open(&dir).unwrap();

    cache
        .mark_complete("DCE.i2601", 1_000, 2_000, 0, None)
        .unwrap();

    HistorySeriesCache::open(&dir)
        .unwrap()
        .enforce_limits(None, None)
        .unwrap();

    let reopened = BacktestTickCache::open(&dir).unwrap();
    let coverage = reopened.coverage("DCE.i2601", 1_000, 2_000).unwrap();
    assert!(coverage.is_complete());
    assert_eq!(coverage.cached_ranges, vec![(1_000, 2_000)]);
    assert_eq!(coverage.missing_ranges, Vec::<(i64, i64)>::new());
}

#[test]
fn backtest_tick_cache_compacts_only_requested_symbol_ticks() {
    let dir = temp_dir("backtest-symbol-compaction");
    let cache = BacktestTickCache::open(&dir).unwrap();

    cache
        .append_partial_ticks("DCE.i2601", [tick(7, 1_000, 100.0)])
        .unwrap();
    cache
        .append_partial_ticks("DCE.i2601", [tick(7, 1_000, 110.0)])
        .unwrap();
    cache
        .mark_complete("DCE.i2601", 1_000, 2_000, 1, Some((7, 8)))
        .unwrap();
    cache
        .append_partial_ticks("SHFE.rb2601", [tick(9, 1_000, 200.0)])
        .unwrap();
    cache
        .append_partial_ticks("SHFE.rb2601", [tick(9, 1_000, 210.0)])
        .unwrap();
    cache
        .mark_complete("SHFE.rb2601", 1_000, 2_000, 1, Some((9, 10)))
        .unwrap();

    let target_path = daily_tick_file(&dir, "19700101", "DCE.i2601");
    let other_path = daily_tick_file(&dir, "19700101", "SHFE.rb2601");
    let target_size_before = std::fs::metadata(&target_path).unwrap().len();
    let other_size_before = std::fs::metadata(&other_path).unwrap().len();

    cache.compact_symbol_ticks("DCE.i2601").unwrap();

    let target_size_after = std::fs::metadata(&target_path).unwrap().len();
    let other_size_after = std::fs::metadata(&other_path).unwrap().len();
    assert!(
        target_size_after < target_size_before,
        "expected target file to shrink from {target_size_before} bytes, got {target_size_after}"
    );
    assert_eq!(other_size_after, other_size_before);

    let history = HistorySeriesCache::open(&dir).unwrap();
    let rows = history
        .read_tick_data_series(TickDataSeriesRequest::new("DCE.i2601", 1_000, 2_000))
        .unwrap();
    assert_eq!(rows.rows().len(), 1);
    assert_eq!(rows.rows()[0].last_price, 110.0);
}

#[test]
fn backtest_tick_cache_range_compaction_touches_only_intersecting_days() {
    let dir = temp_dir("backtest-range-compaction");
    let cache = BacktestTickCache::open(&dir).unwrap();
    let first =
        backtest_tick_trading_day_range(NaiveDate::from_ymd_opt(2026, 1, 5).unwrap()).unwrap();
    let second =
        backtest_tick_trading_day_range(NaiveDate::from_ymd_opt(2026, 1, 6).unwrap()).unwrap();
    for (range, id) in [(first, 7), (second, 8)] {
        let datetime = range.start_ns.saturating_add(1);
        cache
            .append_partial_ticks("DCE.i2601", [tick(id, datetime, 100.0)])
            .unwrap();
        cache
            .append_partial_ticks("DCE.i2601", [tick(id, datetime, 110.0)])
            .unwrap();
        cache
            .mark_complete("DCE.i2601", range.start_ns, range.end_ns, 1, Some((id, id)))
            .unwrap();
    }

    let first_path = daily_tick_file(&dir, "20260105", "DCE.i2601");
    let second_path = daily_tick_file(&dir, "20260106", "DCE.i2601");
    let first_size_before = std::fs::metadata(&first_path).unwrap().len();
    let second_size_before = std::fs::metadata(&second_path).unwrap().len();

    cache
        .compact_symbol_ticks_in_range("DCE.i2601", first.start_ns, first.end_ns)
        .unwrap();

    assert!(std::fs::metadata(first_path).unwrap().len() < first_size_before);
    assert_eq!(
        std::fs::metadata(second_path).unwrap().len(),
        second_size_before
    );
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
        "tqsdk-tqbn-compaction-{name}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn daily_tick_file(root: &std::path::Path, day: &str, symbol: &str) -> PathBuf {
    root.join("series")
        .join(day)
        .join("tick")
        .join(format!("{}.tqbn", symbol.replace('/', "%2F")))
}
