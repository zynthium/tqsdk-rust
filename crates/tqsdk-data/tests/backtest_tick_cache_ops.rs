use chrono::{FixedOffset, NaiveDate, TimeZone};
use tqsdk_core::Tick;
use tqsdk_data::{
    BacktestTickCache, BacktestTickCacheLockRepairMode, BacktestTickCacheLockRepairStatus,
    DataError, HistorySeriesCacheFileStatus, TickDataSeriesRequest,
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
fn repair_tick_locks_dry_run_reports_missing_companion_lock() {
    let dir = temp_dir("repair-tick-locks-dry-run");
    let cache = BacktestTickCache::open(&dir).unwrap();
    cache
        .store_ticks("SHFE.rb2601", 1_000, 2_000, [tick(1, 1_000)])
        .unwrap();
    let path = daily_tick_file(&dir, "19700101", "SHFE.rb2601");
    let lock_path = path.with_extension("tqbn.lock");
    assert!(lock_path.exists());
    std::fs::remove_file(&lock_path).unwrap();

    let report = BacktestTickCache::open_read_only(&dir)
        .repair_tick_locks(BacktestTickCacheLockRepairMode::DryRun)
        .unwrap();

    assert_eq!(report.files.len(), 1);
    assert_eq!(report.missing_files, 1);
    assert_eq!(report.files[0].path, path);
    assert_eq!(report.files[0].lock_path, lock_path);
    assert_eq!(
        report.files[0].status,
        BacktestTickCacheLockRepairStatus::Missing
    );
    assert!(report.files[0].error.is_none());
    assert!(!lock_path.exists());
}

#[test]
fn repair_tick_locks_dry_run_reports_missing_legacy_partition_lock() {
    let dir = temp_dir("repair-tick-legacy-lock-dry-run");
    let cache = BacktestTickCache::open(&dir).unwrap();
    cache
        .store_ticks("DCE.i2601", 1_000, 2_000, [tick(1, 1_000)])
        .unwrap();
    let path = daily_tick_file(&dir, "19700101", "DCE.i2601");
    let lock_path = path.with_extension("tqbn.lock");
    let partition_dir = path.parent().unwrap();
    let legacy_lock_path = partition_dir.join(".tqbn.lock");
    assert!(lock_path.is_file());
    assert!(!legacy_lock_path.exists());

    let report = BacktestTickCache::open_read_only(&dir)
        .repair_tick_locks(BacktestTickCacheLockRepairMode::DryRun)
        .unwrap();

    assert_eq!(report.missing_files, 0);
    assert_eq!(report.legacy_partition_locks_missing, 1);
    assert_eq!(report.legacy_partition_locks.len(), 1);
    assert_eq!(
        report.legacy_partition_locks[0].partition_dir,
        partition_dir
    );
    assert_eq!(report.legacy_partition_locks[0].lock_path, legacy_lock_path);
    assert_eq!(
        report.legacy_partition_locks[0].status,
        BacktestTickCacheLockRepairStatus::Missing
    );
    assert!(report.legacy_partition_locks[0].error.is_none());
    assert!(!legacy_lock_path.exists());
}

#[test]
fn repair_tick_locks_apply_repairs_legacy_partition_lock_without_mutating_tqbn() {
    let dir = temp_dir("repair-tick-legacy-lock-apply");
    let cache = BacktestTickCache::open(&dir).unwrap();
    for symbol in ["DCE.i2601", "SHFE.rb2601"] {
        cache
            .store_ticks(symbol, 1_000, 2_000, [tick(1, 1_000)])
            .unwrap();
    }
    let first_path = daily_tick_file(&dir, "19700101", "DCE.i2601");
    let second_path = daily_tick_file(&dir, "19700101", "SHFE.rb2601");
    let first_lock_path = first_path.with_extension("tqbn.lock");
    let second_lock_path = second_path.with_extension("tqbn.lock");
    let partition_dir = first_path.parent().unwrap().to_path_buf();
    assert_eq!(second_path.parent().unwrap(), partition_dir);
    let legacy_lock_path = partition_dir.join(".tqbn.lock");
    assert!(first_lock_path.is_file());
    assert!(second_lock_path.is_file());
    assert!(!legacy_lock_path.exists());

    let first_tqbn_before = std::fs::read(&first_path).unwrap();
    let second_tqbn_before = std::fs::read(&second_path).unwrap();
    let first_coverage_before = cache.coverage("DCE.i2601", 1_000, 2_000).unwrap();
    let second_coverage_before = cache.coverage("SHFE.rb2601", 1_000, 2_000).unwrap();

    let repaired = cache
        .repair_tick_locks(BacktestTickCacheLockRepairMode::Apply)
        .unwrap();

    assert_eq!(repaired.created_files, 0);
    assert_eq!(repaired.legacy_partition_locks_created, 1);
    assert_eq!(repaired.legacy_partition_locks.len(), 1);
    assert_eq!(
        repaired.legacy_partition_locks[0].partition_dir,
        partition_dir
    );
    assert_eq!(
        repaired.legacy_partition_locks[0].lock_path,
        legacy_lock_path
    );
    assert_eq!(
        repaired.legacy_partition_locks[0].status,
        BacktestTickCacheLockRepairStatus::Created
    );
    assert!(legacy_lock_path.is_file());
    assert_eq!(std::fs::read(&first_path).unwrap(), first_tqbn_before);
    assert_eq!(std::fs::read(&second_path).unwrap(), second_tqbn_before);
    assert_eq!(
        cache.coverage("DCE.i2601", 1_000, 2_000).unwrap(),
        first_coverage_before
    );
    assert_eq!(
        cache.coverage("SHFE.rb2601", 1_000, 2_000).unwrap(),
        second_coverage_before
    );

    let repeated = cache
        .repair_tick_locks(BacktestTickCacheLockRepairMode::Apply)
        .unwrap();

    assert_eq!(repeated.created_files, 0);
    assert_eq!(repeated.legacy_partition_locks_created, 0);
    assert_eq!(repeated.legacy_partition_locks_already_present, 1);
    assert_eq!(
        repeated.legacy_partition_locks[0].status,
        BacktestTickCacheLockRepairStatus::AlreadyPresent
    );
    assert_eq!(std::fs::read(&first_path).unwrap(), first_tqbn_before);
    assert_eq!(std::fs::read(&second_path).unwrap(), second_tqbn_before);

    std::fs::remove_file(&first_lock_path).unwrap();
    let series = BacktestTickCache::open_read_only(&dir)
        .load_series(TickDataSeriesRequest::new("DCE.i2601", 1_000, 2_000))
        .unwrap();
    assert_eq!(series.iter().map(|row| row.id).collect::<Vec<_>>(), vec![1]);
}

#[test]
fn repair_tick_locks_apply_is_idempotent_and_preserves_tqbn_and_coverage() {
    let dir = temp_dir("repair-tick-locks-apply");
    let cache = BacktestTickCache::open(&dir).unwrap();
    cache
        .store_ticks("SHFE.rb2601", 1_000, 2_000, [tick(1, 1_000)])
        .unwrap();
    let path = daily_tick_file(&dir, "19700101", "SHFE.rb2601");
    let lock_path = path.with_extension("tqbn.lock");
    std::fs::remove_file(&lock_path).unwrap();
    let tqbn_before = std::fs::read(&path).unwrap();
    let coverage_before = cache.coverage("SHFE.rb2601", 1_000, 2_000).unwrap();

    let repaired = cache
        .repair_tick_locks(BacktestTickCacheLockRepairMode::Apply)
        .unwrap();

    assert_eq!(repaired.created_files, 1);
    assert_eq!(
        repaired.files[0].status,
        BacktestTickCacheLockRepairStatus::Created
    );
    assert!(lock_path.exists());
    assert_eq!(std::fs::read(&path).unwrap(), tqbn_before);
    assert_eq!(
        cache.coverage("SHFE.rb2601", 1_000, 2_000).unwrap(),
        coverage_before
    );

    let repeated = cache
        .repair_tick_locks(BacktestTickCacheLockRepairMode::Apply)
        .unwrap();

    assert_eq!(repeated.created_files, 0);
    assert_eq!(repeated.already_present_files, 1);
    assert_eq!(
        repeated.files[0].status,
        BacktestTickCacheLockRepairStatus::AlreadyPresent
    );
    assert_eq!(std::fs::read(&path).unwrap(), tqbn_before);
}

#[test]
fn repair_tick_locks_continues_after_a_per_file_failure() {
    let dir = temp_dir("repair-tick-locks-best-effort");
    let cache = BacktestTickCache::open(&dir).unwrap();
    for symbol in ["DCE.i2601", "SHFE.rb2601"] {
        cache
            .store_ticks(symbol, 1_000, 2_000, [tick(1, 1_000)])
            .unwrap();
    }
    let repaired_path = daily_tick_file(&dir, "19700101", "DCE.i2601");
    let repaired_lock_path = repaired_path.with_extension("tqbn.lock");
    std::fs::remove_file(&repaired_lock_path).unwrap();
    let failed_path = daily_tick_file(&dir, "19700101", "SHFE.rb2601");
    let failed_lock_path = failed_path.with_extension("tqbn.lock");
    std::fs::remove_file(&failed_lock_path).unwrap();
    std::fs::create_dir(&failed_lock_path).unwrap();

    let report = cache
        .repair_tick_locks(BacktestTickCacheLockRepairMode::Apply)
        .unwrap();

    assert_eq!(report.created_files, 1);
    assert_eq!(report.failed_files, 1);
    assert!(repaired_lock_path.is_file());
    let failed = report
        .files
        .iter()
        .find(|file| file.path == failed_path)
        .unwrap();
    assert_eq!(failed.status, BacktestTickCacheLockRepairStatus::Failed);
    assert!(failed.error.as_deref().unwrap().contains("regular file"));
}

#[test]
fn repair_tick_locks_dry_run_reports_an_invalid_companion_lock() {
    let dir = temp_dir("repair-tick-locks-invalid-dry-run");
    let cache = BacktestTickCache::open(&dir).unwrap();
    cache
        .store_ticks("SHFE.rb2601", 1_000, 2_000, [tick(1, 1_000)])
        .unwrap();
    let path = daily_tick_file(&dir, "19700101", "SHFE.rb2601");
    let lock_path = path.with_extension("tqbn.lock");
    std::fs::remove_file(&lock_path).unwrap();
    std::fs::create_dir(&lock_path).unwrap();

    let report = cache
        .repair_tick_locks(BacktestTickCacheLockRepairMode::DryRun)
        .unwrap();

    assert_eq!(report.failed_files, 1);
    assert_eq!(
        report.files[0].status,
        BacktestTickCacheLockRepairStatus::Failed
    );
    assert!(
        report.files[0]
            .error
            .as_deref()
            .unwrap()
            .contains("regular file")
    );
}

#[test]
fn operation_lock_allows_parallel_fills_and_excludes_maintenance() {
    let dir = temp_dir("operation-lock");
    let cache = BacktestTickCache::open(&dir).unwrap();
    let first_fill = cache.try_acquire_remote_fill_shared_lock().unwrap();
    let second_fill = cache.try_acquire_remote_fill_shared_lock().unwrap();

    let error = cache.try_acquire_consistency_read_lock().unwrap_err();
    assert!(matches!(
        error,
        DataError::CacheBusy {
            operation: "consistency read",
            ..
        }
    ));

    drop(first_fill);
    drop(second_fill);
    let maintenance = cache.try_acquire_consistency_read_lock().unwrap();
    assert_eq!(maintenance.cache_dir(), dir.as_path());
    assert!(maintenance.path().ends_with(".tqsdk-cache-operation.lock"));
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

#[test]
fn tick_range_purge_removes_only_intersecting_trading_day_partitions() {
    let dir = temp_dir("purge-range");
    let cache = BacktestTickCache::open(&dir).unwrap();
    let first =
        backtest_tick_trading_day_range(NaiveDate::from_ymd_opt(2026, 7, 23).unwrap()).unwrap();
    let second =
        backtest_tick_trading_day_range(NaiveDate::from_ymd_opt(2026, 7, 24).unwrap()).unwrap();
    let symbol = "SHFE.rb2601";
    cache
        .store_ticks(
            symbol,
            first.start_ns,
            second.end_ns,
            [tick(1, first.start_ns + 1), tick(2, second.start_ns + 1)],
        )
        .unwrap();
    let first_path = daily_tick_file(&dir, "20260723", symbol);
    let second_path = daily_tick_file(&dir, "20260724", symbol);
    assert!(first_path.exists());
    assert!(second_path.exists());
    let second_before = std::fs::read(&second_path).unwrap();

    let report = cache
        .purge_symbol_ticks_in_range(symbol, first.start_ns, first.end_ns)
        .unwrap();

    assert_eq!(report.removed_files, 1);
    assert!(report.removed_bytes > 0);
    assert!(!first_path.exists());
    assert!(second_path.exists());
    assert_eq!(std::fs::read(&second_path).unwrap(), second_before);
    assert_eq!(
        cache
            .load_series(TickDataSeriesRequest::new(
                symbol,
                second.start_ns,
                second.end_ns,
            ))
            .unwrap()
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![2]
    );
}

#[test]
fn provisional_tick_checkpoint_never_counts_as_final_coverage() {
    let dir = temp_dir("provisional-checkpoint");
    let cache = BacktestTickCache::open(&dir).unwrap();
    cache
        .append_partial_ticks("SHFE.rb2601", [tick(1, 1_000), tick(2, 2_000)])
        .unwrap();

    let checkpoint = cache
        .mark_provisional("SHFE.rb2601", 1_000, 3_000, 3_000, 2, Some((1, 2)))
        .unwrap();

    assert_eq!(checkpoint.range_start_ns, 1_000);
    assert_eq!(checkpoint.complete_through_ns, 3_000);
    assert_eq!(checkpoint.as_of_ns, 3_000);
    assert_eq!(checkpoint.rows, 2);
    assert_eq!(checkpoint.id_range, Some((1, 2)));
    let final_coverage = cache.coverage("SHFE.rb2601", 1_000, 4_000).unwrap();
    assert_eq!(final_coverage.cached_ranges, Vec::<(i64, i64)>::new());
    assert_eq!(final_coverage.missing_ranges, vec![(1_000, 4_000)]);
    let checkpoint = cache
        .mark_provisional("SHFE.rb2601", 1_000, 3_500, 3_500, 2, Some((1, 2)))
        .unwrap();
    assert_eq!(checkpoint.complete_through_ns, 3_500);

    cache.compact_symbol_ticks("SHFE.rb2601").unwrap();
    let reopened = BacktestTickCache::open(&dir).unwrap();
    assert_eq!(
        reopened
            .provisional_coverage("SHFE.rb2601", 1_000, 4_000)
            .unwrap(),
        Some(checkpoint)
    );

    reopened
        .mark_complete("SHFE.rb2601", 1_000, 4_000, 2, Some((1, 2)))
        .unwrap();
    assert!(
        reopened
            .provisional_coverage("SHFE.rb2601", 1_000, 4_000)
            .unwrap()
            .is_none()
    );
    reopened.compact_symbol_ticks("SHFE.rb2601").unwrap();
    assert!(
        reopened
            .provisional_coverage("SHFE.rb2601", 1_000, 4_000)
            .unwrap()
            .is_none()
    );
}

#[test]
fn provisional_tick_checkpoint_rejects_cross_partition_ranges() {
    let dir = temp_dir("provisional-cross-partition");
    let cache = BacktestTickCache::open(&dir).unwrap();
    let day = NaiveDate::from_ymd_opt(2026, 7, 24).unwrap();
    let range = backtest_tick_trading_day_range(day).unwrap();
    let complete_through_ns = range.end_ns.saturating_add(1);

    let error = cache
        .mark_provisional(
            "SHFE.rb2601",
            range.start_ns,
            complete_through_ns,
            complete_through_ns,
            0,
            None,
        )
        .unwrap_err();

    assert!(matches!(
        error,
        DataError::InvalidState(
            "provisional tick coverage must stay within one TQBN trading-day partition"
        )
    ));

    let error = cache
        .mark_provisional(
            "SHFE.rb2601",
            range.start_ns,
            range.end_ns,
            range.end_ns.saturating_add(1),
            0,
            None,
        )
        .unwrap_err();

    assert!(matches!(
        error,
        DataError::InvalidState(
            "provisional tick coverage must stay within one TQBN trading-day partition"
        )
    ));
    let _ = std::fs::remove_dir_all(dir);
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
