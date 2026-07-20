use chrono::{FixedOffset, NaiveDate, TimeZone};
use tqsdk_core::Tick;
use tqsdk_data::{
    BacktestTickCache, DataError, HistorySeriesCacheFileStatus,
    backtest_tick_trading_day_for_timestamp_ns, backtest_tick_trading_day_range,
};

#[test]
fn trading_day_helpers_use_the_tqbn_cst_evening_boundary() {
    let friday_before_close = cst_ns(2026, 7, 17, 17, 59, 59);
    let friday_evening = cst_ns(2026, 7, 17, 18, 0, 0);

    assert_eq!(
        backtest_tick_trading_day_for_timestamp_ns(friday_before_close).unwrap(),
        NaiveDate::from_ymd_opt(2026, 7, 17).unwrap()
    );
    assert_eq!(
        backtest_tick_trading_day_for_timestamp_ns(friday_evening).unwrap(),
        NaiveDate::from_ymd_opt(2026, 7, 20).unwrap()
    );

    let range =
        backtest_tick_trading_day_range(NaiveDate::from_ymd_opt(2026, 7, 18).unwrap()).unwrap();
    assert_eq!(
        range.trading_day,
        NaiveDate::from_ymd_opt(2026, 7, 20).unwrap()
    );
    assert_eq!(range.start_ns, friday_evening);
    assert_eq!(range.end_ns, cst_ns(2026, 7, 20, 18, 0, 0));
}

#[test]
fn fast_inventory_counts_valid_daily_tqbn_files_without_decoding_rows() {
    let dir = temp_dir("fast-inventory-valid");
    let cache = BacktestTickCache::open(&dir).unwrap();
    cache
        .store_ticks("SHFE.rb2601", 1_000, 2_000, [tick(1, 1_000)])
        .unwrap();

    let inventory = cache.fast_inventory().unwrap();

    assert_eq!(inventory.total_files, 1);
    assert_eq!(inventory.total_days, 1);
    assert_eq!(inventory.problem_files, 0);
    assert!(inventory.total_bytes > 4);
    assert_eq!(inventory.symbols.len(), 1);
    assert_eq!(inventory.symbols[0].symbol, "SHFE.rb2601");
    assert_eq!(inventory.symbols[0].files, 1);
    assert_eq!(inventory.symbols[0].problem_files, 0);
}

#[test]
fn fast_inventory_and_diagnostics_report_bad_tqbn_magic() {
    let dir = temp_dir("fast-inventory-bad-magic");
    let path = daily_tick_file(&dir, "19700101", "SHFE.rb2601");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, b"BAD!").unwrap();
    let cache = BacktestTickCache::open(&dir).unwrap();

    let fast = cache.fast_inventory().unwrap();
    assert_eq!(fast.total_files, 1);
    assert_eq!(fast.problem_files, 1);
    assert_eq!(fast.symbols[0].problem_files, 1);

    let report = cache.diagnose().unwrap();
    assert_eq!(report.files.len(), 1);
    assert_eq!(report.problem_files, 1);
    assert_eq!(report.files[0].trading_day.as_deref(), Some("1970-01-01"));
    assert_eq!(
        report.files[0].status,
        HistorySeriesCacheFileStatus::IncompleteWrite
    );
    assert!(report.files[0].is_problem());
    assert!(report.files[0].error.as_deref().unwrap().contains("magic"));
}

#[test]
fn operation_lock_allows_shared_readers_and_rejects_remote_fill() {
    let dir = temp_dir("operation-lock");
    let cache = BacktestTickCache::open(&dir).unwrap();
    let first_reader = cache.try_acquire_consistency_read_lock().unwrap();
    let second_reader = cache.try_acquire_consistency_read_lock().unwrap();

    let error = cache.try_acquire_remote_fill_lock().unwrap_err();
    assert!(matches!(
        error,
        DataError::CacheBusy {
            operation: "remote fill",
            ..
        }
    ));

    drop(first_reader);
    drop(second_reader);
    let writer = cache.try_acquire_remote_fill_lock().unwrap();
    assert_eq!(writer.cache_dir(), dir.as_path());
    assert!(writer.path().ends_with(".tqsdk-cache-operation.lock"));
}

#[test]
fn read_only_cache_does_not_create_a_missing_root_or_allow_writes() {
    let dir = std::env::temp_dir().join(format!(
        "tqsdk-backtest-tick-cache-read-only-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cache = BacktestTickCache::open_read_only(&dir);

    let coverage = cache.coverage("SHFE.rb2601", 1_000, 2_000).unwrap();
    assert!(!coverage.is_complete());
    assert!(!dir.exists());

    let error = cache
        .store_ticks("SHFE.rb2601", 1_000, 2_000, [tick(1, 1_000)])
        .unwrap_err();
    assert!(matches!(
        error,
        DataError::InvalidState("history cache was opened read-only")
    ));
    assert!(!dir.exists());
}

fn cst_ns(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> i64 {
    FixedOffset::east_opt(8 * 60 * 60)
        .unwrap()
        .with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap()
}

fn daily_tick_file(root: &std::path::Path, day: &str, symbol: &str) -> std::path::PathBuf {
    root.join("series")
        .join(day)
        .join("tick")
        .join(format!("{}.tqbn", symbol.replace('/', "%2F")))
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "tqsdk-backtest-tick-cache-ops-{name}-{}-{unique}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn tick(id: i64, datetime: i64) -> Tick {
    Tick {
        id,
        datetime,
        ..Tick::default()
    }
}
