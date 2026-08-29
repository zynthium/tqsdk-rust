use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::process::{Command, Stdio};

use chrono::{NaiveDate, TimeZone, Utc};
use serde_json::{Value, json};
use tqsdk::advanced::core::{Kline, Tick};
use tqsdk_cache::{TradingCalendarHolidaysSnapshot, write_trading_calendar_holidays_snapshot};
use tqsdk_data::{
    BACKTEST_HISTORY_METADATA_SCHEMA_VERSION, BacktestHistoryMarketKind,
    BacktestHistoryMetadataCache, BacktestHistoryMetadataSnapshot, BacktestHistoryPhysicalSegment,
    BacktestHistoryTradingDay, BacktestTickCache, DailyKlineCache, KlineSessionTemplate,
    MinuteKlineCache, MinuteKlineCacheSnapshot, TradingCalendarHolidays,
    backtest_tick_trading_day_for_timestamp_ns, backtest_tick_trading_day_range,
};

fn v3_result<'a>(json: &'a Value, command: &str, status: &str, exit_code: i32) -> &'a Value {
    assert_eq!(json["schema_version"], 3);
    assert_eq!(json["kind"], "tqsdk-cache.result");
    assert_eq!(json["command"], command);
    assert_eq!(json["status"], status);
    assert_eq!(json["exit_code"], exit_code);
    assert!(json["generated_at"].is_string());
    assert!(json["duration_ms"].is_u64());
    assert_eq!(json["tool"]["name"], "tqsdk-cache");
    assert!(json["warnings"].is_array());
    &json["result"]
}

#[test]
fn inventory_is_read_only_for_a_missing_cache_root() {
    let cache_dir = temp_dir("inventory");
    let output = run_json(["--cache-dir", cache_dir.to_str().unwrap(), "inventory"]);

    assert!(output.status.success());
    assert!(!cache_dir.exists());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "inventory", "success", 0);
    assert_eq!(json["error"], Value::Null);
    assert_eq!(result["schema_version"], 2);
    assert_eq!(result["command"], "inventory");
    assert_eq!(result["total_files"], 0);
}

#[test]
fn minute_inventory_is_read_only_for_a_missing_cache_root() {
    let cache_dir = temp_dir("minute-inventory");
    let output = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "inventory",
    ]);

    assert!(output.status.success());
    assert!(!cache_dir.exists());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "inventory", "success", 0);
    assert_eq!(result["cache_kind"], "minute");
    assert_eq!(result["total_files"], 0);
    assert_eq!(result["total_bytes"], 0);
}

#[test]
fn daily_and_all_inventory_include_prefix_scanned_daily_files() {
    let cache_dir = temp_dir("daily-inventory");
    let range = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    let symbol = "SHFE:rb2601";
    let cache = DailyKlineCache::open(&cache_dir).unwrap();
    cache
        .store_final_range(
            symbol,
            range.start_ns,
            range.end_ns,
            &MinuteKlineCacheSnapshot::cst_v1(),
            &[],
        )
        .unwrap();

    let daily = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "daily",
        "inventory",
    ]);
    assert!(daily.status.success());
    let json: Value = serde_json::from_slice(&daily.stdout).unwrap();
    let result = v3_result(&json, "inventory", "success", 0);
    assert_eq!(result["cache_kind"], "daily");
    assert_eq!(result["total_files"], 1);
    assert_eq!(result["symbols"][0]["symbol"], symbol);

    let all = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "all",
        "inventory",
    ]);
    assert!(all.status.success());
    let json: Value = serde_json::from_slice(&all.stdout).unwrap();
    let result = v3_result(&json, "inventory", "success", 0);
    assert_eq!(result["daily"]["total_files"], 1);
    assert_eq!(result["daily"]["symbols"][0]["symbol"], symbol);

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn all_kind_is_rejected_for_targeted_cache_operations() {
    let output = run_json([
        "--kind",
        "all",
        "inspect",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
    ]);

    assert_eq!(output.status.code(), Some(2));
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let _ = v3_result(&json, "inspect", "error", 2);
    assert_eq!(json["error"]["code"], "usage");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("--kind all")
    );
}

#[test]
fn repair_locks_dry_run_reports_missing_companion_without_creating_it() {
    let cache_dir = temp_dir("repair-locks-dry-run");
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    cache
        .store_ticks(
            "SHFE.rb2601",
            1_000,
            2_000,
            [Tick {
                id: 1,
                datetime: 1_000,
                ..Tick::default()
            }],
        )
        .unwrap();
    let path = cache.diagnose().unwrap().files.remove(0).path;
    let lock_path = path.with_extension("tqbn.lock");
    fs::remove_file(&lock_path).unwrap();

    let output = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "tick",
        "repair-locks",
    ]);

    assert!(output.status.success());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "repair-locks", "success", 0);
    assert_eq!(result["cache_kind"], "tick");
    assert_eq!(result["dry_run"], true);
    assert_eq!(result["missing_files"], 1);
    assert_eq!(
        result["files"][0]["path"],
        fs::canonicalize(&path).unwrap().display().to_string()
    );
    assert_eq!(result["files"][0]["status"], "missing");
    assert!(!lock_path.exists());

    let _ = fs::remove_dir_all(cache_dir);
}

#[test]
fn repair_locks_apply_creates_missing_companion_lock() {
    let cache_dir = temp_dir("repair-locks-apply");
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    cache
        .store_ticks(
            "SHFE.rb2601",
            1_000,
            2_000,
            [Tick {
                id: 1,
                datetime: 1_000,
                ..Tick::default()
            }],
        )
        .unwrap();
    let path = cache.diagnose().unwrap().files.remove(0).path;
    let lock_path = path.with_extension("tqbn.lock");
    fs::remove_file(&lock_path).unwrap();

    let output = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "repair-locks",
        "--apply",
    ]);

    assert!(output.status.success());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "repair-locks", "success", 0);
    assert_eq!(result["dry_run"], false);
    assert_eq!(result["created_files"], 1);
    assert_eq!(result["failed_files"], 0);
    assert_eq!(result["files"][0]["status"], "created");
    assert!(lock_path.is_file());

    let _ = fs::remove_dir_all(cache_dir);
}

#[test]
fn repair_locks_apply_reports_legacy_partition_locks_separately_from_files() {
    let cache_dir = temp_dir("repair-locks-legacy-partition");
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    cache
        .store_ticks(
            "SHFE.rb2601",
            1_000,
            2_000,
            [Tick {
                id: 1,
                datetime: 1_000,
                ..Tick::default()
            }],
        )
        .unwrap();
    let path = cache.diagnose().unwrap().files.remove(0).path;
    let lock_path = path.with_extension("tqbn.lock");
    let partition_dir = path.parent().unwrap();
    let legacy_lock_path = partition_dir.join(".tqbn.lock");
    assert!(lock_path.is_file());
    assert!(!legacy_lock_path.exists());

    let output = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--output-schema",
        "v3",
        "repair-locks",
        "--apply",
    ]);

    assert!(output.status.success());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "repair-locks", "success", 0);
    assert_eq!(result["legacy_partition_locks_created"], 1);
    assert_eq!(result["created_files"], 0);
    assert_eq!(result["legacy_partition_locks"][0]["status"], "created");
    assert_eq!(
        result["legacy_partition_locks"][0]["lock_path"],
        fs::canonicalize(partition_dir)
            .unwrap()
            .join(".tqbn.lock")
            .display()
            .to_string()
    );
    assert_eq!(result["files"][0]["status"], "already_present");
    assert!(legacy_lock_path.is_file());

    let _ = fs::remove_dir_all(cache_dir);
}

#[test]
fn repair_locks_dry_run_returns_nonzero_for_an_invalid_companion_lock() {
    let cache_dir = temp_dir("repair-locks-invalid-dry-run");
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    cache
        .store_ticks(
            "SHFE.rb2601",
            1_000,
            2_000,
            [Tick {
                id: 1,
                datetime: 1_000,
                ..Tick::default()
            }],
        )
        .unwrap();
    let path = cache.diagnose().unwrap().files.remove(0).path;
    let lock_path = path.with_extension("tqbn.lock");
    fs::remove_file(&lock_path).unwrap();
    fs::create_dir(&lock_path).unwrap();

    let output = run_json(["--cache-dir", cache_dir.to_str().unwrap(), "repair-locks"]);

    assert_eq!(output.status.code(), Some(1));
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "repair-locks", "incomplete", 1);
    assert_eq!(result["dry_run"], true);
    assert_eq!(result["failed_files"], 1);
    assert_eq!(result["files"][0]["status"], "failed");
    assert!(
        result["files"][0]["error"]
            .as_str()
            .unwrap()
            .contains("regular file")
    );
    assert!(lock_path.is_dir());

    let _ = fs::remove_dir_all(cache_dir);
}

#[test]
fn minute_inspect_uses_the_logical_symbol_as_its_cache_key() {
    let cache_dir = temp_dir("minute-inspect");
    let range = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    let cache = MinuteKlineCache::open(&cache_dir).unwrap();
    cache
        .store_final_range(
            "KQ.m@SHFE.au",
            range.start_ns,
            range.end_ns,
            &MinuteKlineCacheSnapshot::cst_v1(),
            &[],
        )
        .unwrap();

    let output = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "inspect",
        "--symbol",
        "KQ.m@SHFE.au",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
    ]);

    assert!(output.status.success());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "inspect", "success", 0);
    assert_eq!(result["cache_kind"], "minute");
    assert_eq!(result["statuses"][0]["symbol"], "KQ.m@SHFE.au");
    assert_eq!(result["statuses"][0]["complete"], true);

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn minute_inspect_uses_the_persisted_metadata_snapshot() {
    let cache_dir = temp_dir("minute-inspect-metadata-snapshot");
    let symbol = "KQ.i@SHFE.au";
    store_metadata_backed_minute_coverage(&cache_dir, symbol, day(2020, 1, 2));

    let output = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "inspect",
        "--symbol",
        symbol,
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
    ]);

    assert!(output.status.success());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "inspect", "success", 0);
    assert_eq!(result["statuses"][0]["symbol"], symbol);
    assert_eq!(result["statuses"][0]["complete"], true);

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn minute_inspect_keeps_using_a_cached_immutable_snapshot_after_active_metadata_moves() {
    let cache_dir = temp_dir("minute-inspect-historical-metadata-snapshot");
    let symbol = "KQ.i@SHFE.au";
    let trading_day = day(2020, 1, 2);
    let initial = store_metadata_backed_minute_coverage(&cache_dir, symbol, trading_day);
    let range = backtest_tick_trading_day_range(trading_day).unwrap();

    let advanced = BacktestHistoryMetadataCache::open(&cache_dir)
        .unwrap()
        .store_snapshot(BacktestHistoryMetadataSnapshot {
            schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
            market_kind: BacktestHistoryMarketKind::Futures,
            logical_symbol: symbol.to_string(),
            captured_at_ns: range.end_ns.saturating_add(1),
            trading_days: vec![BacktestHistoryTradingDay {
                date: trading_day.to_string(),
                is_trading_day: true,
                start_ns: range.start_ns,
                end_ns: range.end_ns,
            }],
            session: KlineSessionTemplate::cst_trading_day(),
            physical_segments: vec![BacktestHistoryPhysicalSegment {
                physical_symbol: symbol.to_string(),
                start_ns: range.start_ns,
                end_ns: range.end_ns,
            }],
            snapshot_hash: String::new(),
        })
        .unwrap();
    assert_ne!(initial.snapshot_hash, advanced.snapshot_hash);

    let output = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "inspect",
        "--symbol",
        symbol,
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
    ]);

    assert!(output.status.success());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "inspect", "success", 0);
    assert_eq!(result["statuses"][0]["complete"], true);

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn minute_doctor_reports_the_v5_month_file_without_touching_tick_cache() {
    let cache_dir = temp_dir("minute-doctor");
    let range = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    let cache = MinuteKlineCache::open(&cache_dir).unwrap();
    cache
        .store_final_range(
            "KQ.i@SHFE.au",
            range.start_ns,
            range.end_ns,
            &MinuteKlineCacheSnapshot::cst_v1(),
            &[],
        )
        .unwrap();

    let output = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "doctor",
    ]);

    assert!(output.status.success());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "doctor", "success", 0);
    assert_eq!(result["cache_kind"], "minute");
    assert_eq!(result["problem_files"], 0);
    assert_eq!(result["files"][0]["status"], "readable");
    assert_eq!(result["files"][0]["schema_version"], 5);

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn daily_and_all_doctor_include_fully_decoded_daily_files() {
    let cache_dir = temp_dir("daily-doctor");
    let range = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    DailyKlineCache::open(&cache_dir)
        .unwrap()
        .store_final_range(
            "KQ.i@SHFE.au",
            range.start_ns,
            range.end_ns,
            &MinuteKlineCacheSnapshot::cst_v1(),
            &[],
        )
        .unwrap();

    let daily = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "daily",
        "doctor",
    ]);
    assert!(daily.status.success());
    let json: Value = serde_json::from_slice(&daily.stdout).unwrap();
    let result = v3_result(&json, "doctor", "success", 0);
    assert_eq!(result["cache_kind"], "daily");
    assert_eq!(result["problem_files"], 0);
    assert_eq!(result["files"][0]["symbol"], "KQ.i@SHFE.au");
    assert_eq!(result["files"][0]["status"], "readable");

    let all = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "all",
        "doctor",
    ]);
    assert!(all.status.success());
    let json: Value = serde_json::from_slice(&all.stdout).unwrap();
    let result = v3_result(&json, "doctor", "success", 0);
    assert_eq!(result["daily"]["problem_files"], 0);
    assert_eq!(result["daily"]["files"][0]["symbol"], "KQ.i@SHFE.au");

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn minute_migrate_rewrites_v4_with_a_rollback_backup() {
    let cache_dir = temp_dir("minute-migrate-v4");
    let range = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    let cache = MinuteKlineCache::open(&cache_dir).unwrap();
    cache
        .store_final_range(
            "KQ.i@SHFE.au",
            range.start_ns,
            range.end_ns,
            &MinuteKlineCacheSnapshot::cst_v1(),
            &[],
        )
        .unwrap();
    let path = cache.month_file_path("KQ.i@SHFE.au", "202001");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[4..6].copy_from_slice(&4_u16.to_le_bytes());
    std::fs::write(&path, bytes).unwrap();
    let backup_dir = cache_dir.with_file_name(format!(
        "{}-backup",
        cache_dir.file_name().unwrap().to_string_lossy()
    ));

    let output = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "migrate",
        "--apply",
        "--backup-dir",
        backup_dir.to_str().unwrap(),
    ]);

    assert!(output.status.success());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "migrate", "success", 0);
    assert_eq!(result["cache_kind"], "minute");
    assert_eq!(result["legacy_files"], 1);
    assert_eq!(result["completed"], true);
    assert_eq!(
        std::fs::read(backup_dir.join(path.strip_prefix(&cache_dir).unwrap())).unwrap()[4..6],
        4_u16.to_le_bytes()
    );
    assert_eq!(std::fs::read(&path).unwrap()[4..6], 5_u16.to_le_bytes());

    let _ = std::fs::remove_dir_all(cache_dir);
    let _ = std::fs::remove_dir_all(backup_dir);
}

#[test]
fn minute_doctor_rejects_a_concurrent_shared_fill_root_gate() {
    let cache_dir = temp_dir("minute-doctor-root-lock-busy");
    let root_gate = BacktestTickCache::open(&cache_dir).unwrap();
    let shared_lock = root_gate.try_acquire_remote_fill_shared_lock().unwrap();

    let output = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "doctor",
    ]);

    assert_eq!(output.status.code(), Some(75));
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let _ = v3_result(&json, "doctor", "error", 75);
    assert_eq!(json["error"]["code"], "cache_busy");

    drop(shared_lock);
    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn minute_fill_dry_run_is_cache_only_and_does_not_create_the_root() {
    let cache_dir = temp_dir("minute-fill-dry-run");
    let output = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-03",
        "--dry-run",
    ]);

    assert_eq!(output.status.code(), Some(1));
    assert!(!cache_dir.exists());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "fill", "incomplete", 1);
    assert_eq!(result["cache_kind"], "minute");
    assert_eq!(result["dry_run"], true);
    assert_eq!(result["report"]["cache_kind"], "minute");
    assert_eq!(result["report"]["remote_used"], false);
    assert_eq!(result["report"]["complete"], false);
}

#[test]
fn daily_fill_dry_run_is_cache_only_and_does_not_create_the_root() {
    let cache_dir = temp_dir("daily-fill-dry-run");
    let output = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "daily",
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-03",
        "--dry-run",
    ]);

    assert_eq!(output.status.code(), Some(1));
    assert!(!cache_dir.exists());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "fill", "incomplete", 1);
    assert_eq!(result["cache_kind"], "daily");
    assert_eq!(result["dry_run"], true);
    assert_eq!(result["report_path"], Value::Null);
    assert_eq!(result["symbols"][0]["symbol"], "SHFE.rb2601");
    assert_eq!(result["symbols"][0]["complete"], false);
}

#[test]
fn daily_fill_accepts_the_same_scheduler_flags_as_tick_and_minute() {
    let cache_dir = temp_dir("daily-fill-shared-flags");
    let output = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "daily",
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-03",
        "--symbol-batch-size",
        "1",
        "--symbol-concurrency",
        "2",
        "--idle-timeout-secs",
        "60",
        "--batch-timeout-secs",
        "0",
        "--lock-wait-secs",
        "300",
        "--dry-run",
    ]);

    assert_eq!(output.status.code(), Some(1));
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "fill", "incomplete", 1);
    assert_eq!(result["cache_kind"], "daily");
    assert!(!cache_dir.exists());
}

#[test]
fn daily_fill_rejects_scheduler_values_outside_the_shared_bounds() {
    let output = run_json([
        "--kind",
        "daily",
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-03",
        "--symbol-concurrency",
        "5",
        "--dry-run",
    ]);

    assert_eq!(output.status.code(), Some(1));
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let _ = v3_result(&json, "fill", "error", 1);
    assert_eq!(json["error"]["code"], "data_error");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("between 1 and 4")
    );
}

#[test]
fn daily_fill_jsonl_progress_and_report_use_the_shared_terminal_contract() {
    let cache_dir = temp_dir("daily-fill-progress-v3");
    let range = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    DailyKlineCache::open(&cache_dir)
        .unwrap()
        .store_final_range(
            "SHFE.rb2601",
            range.start_ns,
            range.end_ns,
            &MinuteKlineCacheSnapshot::cst_v1(),
            &[],
        )
        .unwrap();
    let report_path = cache_dir.join("daily-v3-report.json");
    let output = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "daily",
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
        "--progress",
        "jsonl",
        "--report",
        report_path.to_str().unwrap(),
    ]);

    assert!(output.status.success());
    let records = String::from_utf8_lossy(&output.stderr)
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert!(records.iter().any(|record| record["event"] == "planning"));
    assert!(records.iter().any(|record| record["event"] == "batch"));
    assert_eq!(
        records
            .iter()
            .filter(|record| record["event"] == "complete")
            .count(),
        1
    );
    assert!(records.iter().all(|record| record["cache_kind"] == "daily"));

    let report: Value = serde_json::from_slice(&std::fs::read(&report_path).unwrap()).unwrap();
    assert_eq!(report["schema_version"], 3);
    assert_eq!(report["cache_kind"], "daily");
    assert_eq!(report["status"], "complete");
    assert_eq!(report["interrupted"], false);
    assert_eq!(report["error"], Value::Null);
    assert_eq!(
        report["requested_range"],
        json!([range.start_ns, range.end_ns])
    );
    assert_eq!(report["symbols"][0]["symbol"], "SHFE.rb2601");
    assert_eq!(report["symbols"][0]["status"], "complete");
    assert_eq!(
        report["symbols"][0]["requested_ranges"],
        json!([[range.start_ns, range.end_ns]])
    );
    assert_eq!(report["symbols"][0]["rows_written"], 0);
    assert_eq!(report["symbols"][0]["interrupted"], false);
    assert_eq!(report["symbols"][0]["error"], Value::Null);

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[cfg(unix)]
#[test]
fn daily_fill_sigint_persists_one_interrupted_terminal_report() {
    let cache_dir = temp_dir("daily-fill-sigint-v3");
    let range = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    DailyKlineCache::open(&cache_dir)
        .unwrap()
        .store_final_range(
            "SHFE.rb2601",
            range.start_ns,
            range.end_ns,
            &MinuteKlineCacheSnapshot::cst_v1(),
            &[],
        )
        .unwrap();
    let root_gate = BacktestTickCache::open(&cache_dir).unwrap();
    let _exclusive = root_gate.try_acquire_remote_fill_lock().unwrap();
    let report_path = cache_dir.join("daily-interrupted-v3.json");
    let mut child = Command::new(env!("CARGO_BIN_EXE_tqsdk-cache"))
        .env_remove("TQ_AUTH_USER")
        .env_remove("TQ_AUTH_PASS")
        .args(["--output-format", "json"])
        .args([
            "--cache-dir",
            cache_dir.to_str().unwrap(),
            "--kind",
            "daily",
            "fill",
            "--symbol",
            "SHFE.rb2601",
            "--start-day",
            "2020-01-02",
            "--end-day",
            "2020-01-02",
            "--lock-wait-secs",
            "30",
            "--progress",
            "jsonl",
            "--report",
            report_path.to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stderr = BufReader::new(child.stderr.take().unwrap());
    let mut progress_lines = Vec::new();
    loop {
        let mut line = String::new();
        assert!(stderr.read_line(&mut line).unwrap() > 0);
        let record: Value = serde_json::from_str(line.trim()).unwrap();
        let reached_lock_wait = record["sequence"].as_u64().is_some_and(|value| value >= 2)
            && record["batch"]["total"] == 1;
        progress_lines.push(record);
        if reached_lock_wait {
            break;
        }
    }
    assert!(
        Command::new("kill")
            .args(["-INT", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let status = child.wait().unwrap();
    let mut remaining_stderr = String::new();
    stderr.read_to_string(&mut remaining_stderr).unwrap();
    progress_lines.extend(
        remaining_stderr
            .lines()
            .filter(|line| line.trim_start().starts_with('{'))
            .map(|line| serde_json::from_str::<Value>(line).unwrap()),
    );
    let mut stdout = String::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut stdout)
        .unwrap();

    assert_eq!(status.code(), Some(130), "stdout={stdout}");
    assert_eq!(
        progress_lines
            .iter()
            .filter(|record| record["event"] == "complete")
            .count(),
        1
    );
    let report: Value = serde_json::from_slice(&fs::read(&report_path).unwrap()).unwrap();
    assert_eq!(report["schema_version"], 3);
    assert_eq!(report["cache_kind"], "daily");
    assert_eq!(report["status"], "interrupted");
    assert_eq!(report["interrupted"], true);
    assert_eq!(report["symbols"][0]["interrupted"], true);

    let _ = fs::remove_dir_all(cache_dir);
}

#[test]
fn daily_fill_failure_persists_one_failed_terminal_report() {
    let cache_dir = temp_dir("daily-fill-failed-v3");
    let range = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    DailyKlineCache::open(&cache_dir)
        .unwrap()
        .store_final_range(
            "SHFE.rb2601",
            range.start_ns,
            range.end_ns,
            &MinuteKlineCacheSnapshot::cst_v1(),
            &[],
        )
        .unwrap();
    let root_gate = BacktestTickCache::open(&cache_dir).unwrap();
    let _exclusive = root_gate.try_acquire_remote_fill_lock().unwrap();
    let report_path = cache_dir.join("daily-failed-v3.json");
    let output = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "daily",
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
        "--lock-wait-secs",
        "1",
        "--progress",
        "jsonl",
        "--report",
        report_path.to_str().unwrap(),
    ]);

    assert!(!output.status.success());
    let records = String::from_utf8_lossy(&output.stderr)
        .lines()
        .filter(|line| line.trim_start().starts_with('{'))
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        records
            .iter()
            .filter(|record| record["event"] == "complete")
            .count(),
        1
    );
    let report: Value = serde_json::from_slice(&fs::read(&report_path).unwrap()).unwrap();
    assert_eq!(report["schema_version"], 3);
    assert_eq!(report["cache_kind"], "daily");
    assert_eq!(report["status"], "failed");
    assert_eq!(report["interrupted"], false);
    assert!(report["error"].is_string());
    assert_eq!(report["symbols"][0]["status"], "failed");

    let _ = fs::remove_dir_all(cache_dir);
}

#[test]
fn daily_fill_dry_run_has_a_human_readable_summary() {
    let cache_dir = temp_dir("daily-fill-text");
    let output = run([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "daily",
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-03",
        "--dry-run",
    ]);

    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Kind: daily"));
    assert!(stdout.contains("Mode: dry run"));
    assert!(stdout.contains("Coverage: incomplete | Remote: not used | Rows written: 0"));
    assert!(stdout.contains("Symbols (1)"));
    assert!(!cache_dir.exists());
}

#[test]
fn daily_inspect_and_verify_read_final_coverage_without_remote_access() {
    let cache_dir = temp_dir("daily-inspect-verify");
    let range = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    DailyKlineCache::open(&cache_dir)
        .unwrap()
        .store_final_range(
            "SHFE.rb2601",
            range.start_ns,
            range.end_ns,
            &MinuteKlineCacheSnapshot::cst_v1(),
            &[],
        )
        .unwrap();

    let inspected = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "daily",
        "inspect",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
    ]);
    assert!(inspected.status.success());
    let json: Value = serde_json::from_slice(&inspected.stdout).unwrap();
    let result = v3_result(&json, "inspect", "success", 0);
    assert_eq!(result["cache_kind"], "daily");
    assert_eq!(result["statuses"][0]["complete"], true);

    let verified = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "daily",
        "verify",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
        "--replay",
        "--min-rows",
        "0",
    ]);
    assert!(verified.status.success());
    let json: Value = serde_json::from_slice(&verified.stdout).unwrap();
    let result = v3_result(&json, "verify", "success", 0);
    assert_eq!(result["cache_kind"], "daily");
    assert_eq!(result["coverage_complete"], true);
    assert_eq!(result["replay_rows"], 0);

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn daily_fill_writes_a_report_that_daily_verify_can_bind() {
    let cache_dir = temp_dir("daily-fill-report");
    let range = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    DailyKlineCache::open(&cache_dir)
        .unwrap()
        .store_final_range(
            "SHFE.rb2601",
            range.start_ns,
            range.end_ns,
            &MinuteKlineCacheSnapshot::cst_v1(),
            &[],
        )
        .unwrap();
    let report_path = cache_dir.join("daily-fill-report.json");

    let filled = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "daily",
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
        "--report",
        report_path.to_str().unwrap(),
    ]);
    assert!(filled.status.success());
    let json: Value = serde_json::from_slice(&filled.stdout).unwrap();
    let result = v3_result(&json, "fill", "success", 0);
    assert_eq!(result["cache_kind"], "daily");
    assert_eq!(result["report"]["cache_kind"], "daily");
    assert_eq!(result["report"]["complete"], true);
    assert!(report_path.exists());

    let verified = run_without_auth_json([
        "--kind",
        "daily",
        "verify",
        "--report",
        report_path.to_str().unwrap(),
    ]);
    assert!(verified.status.success());
    let json: Value = serde_json::from_slice(&verified.stdout).unwrap();
    let result = v3_result(&json, "verify", "success", 0);
    assert_eq!(result["cache_kind"], "daily");
    assert_eq!(result["source_report"], "bound");
    assert_eq!(result["coverage_complete"], true);
    assert_eq!(result["symbols"], json!(["SHFE.rb2601"]));
    assert_eq!(result["requested_days"]["start_day"], "2020-01-02");
    assert_eq!(result["requested_days"]["end_day"], "2020-01-02");

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn daily_purge_is_whole_symbol_only_and_requires_confirmation() {
    let cache_dir = temp_dir("daily-purge");
    let range = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    let cache = DailyKlineCache::open(&cache_dir).unwrap();
    cache
        .store_final_range(
            "SHFE.rb2601",
            range.start_ns,
            range.end_ns,
            &MinuteKlineCacheSnapshot::cst_v1(),
            &[],
        )
        .unwrap();
    let path = cache.symbol_file_path("SHFE.rb2601");
    assert!(path.exists());

    let dry_run = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "daily",
        "purge",
        "--symbol",
        "SHFE.rb2601",
        "--dry-run",
    ]);
    assert!(dry_run.status.success());
    let json: Value = serde_json::from_slice(&dry_run.stdout).unwrap();
    let result = v3_result(&json, "purge", "success", 0);
    assert_eq!(result["cache_kind"], "daily");
    assert_eq!(result["would_remove_files"].as_array().unwrap().len(), 1);
    assert!(path.exists());

    let not_confirmed = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "daily",
        "purge",
        "--symbol",
        "SHFE.rb2601",
    ]);
    assert_eq!(not_confirmed.status.code(), Some(2));
    assert!(path.exists());

    let date_scoped = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "daily",
        "purge",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--yes",
    ]);
    assert_eq!(date_scoped.status.code(), Some(2));
    assert!(path.exists());

    let root_gate = BacktestTickCache::open(&cache_dir).unwrap();
    let shared_lock = root_gate.try_acquire_remote_fill_shared_lock().unwrap();
    let busy = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "daily",
        "purge",
        "--symbol",
        "SHFE.rb2601",
        "--yes",
    ]);
    assert_eq!(busy.status.code(), Some(75));
    assert!(path.exists());
    drop(shared_lock);

    let purged = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "daily",
        "purge",
        "--symbol",
        "SHFE.rb2601",
        "--yes",
    ]);
    assert!(purged.status.success());
    let json: Value = serde_json::from_slice(&purged.stdout).unwrap();
    let result = v3_result(&json, "purge", "success", 0);
    assert_eq!(result["cache_kind"], "daily");
    assert_eq!(result["removed_files"], 1);
    assert!(!path.exists());

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn tick_purge_requires_a_day_range_confirmation_and_the_root_lock() {
    let cache_dir = temp_dir("tick-purge-range");
    let first = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    let second = backtest_tick_trading_day_range(day(2020, 1, 3)).unwrap();
    let symbol = "SHFE.rb2601";
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    cache
        .store_ticks(
            symbol,
            first.start_ns,
            second.end_ns,
            [
                Tick {
                    id: 1,
                    datetime: first.start_ns + 1,
                    ..Tick::default()
                },
                Tick {
                    id: 2,
                    datetime: second.start_ns + 1,
                    ..Tick::default()
                },
            ],
        )
        .unwrap();
    let first_path = cache
        .diagnose()
        .unwrap()
        .files
        .into_iter()
        .find(|file| file.trading_day.as_deref() == Some("2020-01-02"))
        .unwrap()
        .path;
    let second_path = cache
        .diagnose()
        .unwrap()
        .files
        .into_iter()
        .find(|file| file.trading_day.as_deref() == Some("2020-01-03"))
        .unwrap()
        .path;
    let second_before = std::fs::read(&second_path).unwrap();

    let missing_range = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "tick",
        "purge",
        "--symbol",
        symbol,
        "--yes",
    ]);
    assert_eq!(missing_range.status.code(), Some(2));

    let dry_run = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "tick",
        "purge",
        "--symbol",
        symbol,
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
        "--dry-run",
    ]);
    assert!(dry_run.status.success());
    let json: Value = serde_json::from_slice(&dry_run.stdout).unwrap();
    let result = v3_result(&json, "purge", "success", 0);
    assert_eq!(result["would_remove_files"].as_array().unwrap().len(), 1);
    assert_eq!(result["would_remove_files"][0]["trading_day"], "2020-01-02");
    assert!(first_path.exists());

    let not_confirmed = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "tick",
        "purge",
        "--symbol",
        symbol,
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
    ]);
    assert_eq!(not_confirmed.status.code(), Some(2));

    let root_gate = BacktestTickCache::open(&cache_dir).unwrap();
    let shared_lock = root_gate.try_acquire_remote_fill_shared_lock().unwrap();
    let busy = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "tick",
        "purge",
        "--symbol",
        symbol,
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
        "--yes",
    ]);
    assert_eq!(busy.status.code(), Some(75));
    assert!(first_path.exists());
    drop(shared_lock);

    let purged = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "tick",
        "purge",
        "--symbol",
        symbol,
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
        "--yes",
    ]);
    assert!(purged.status.success());
    let json: Value = serde_json::from_slice(&purged.stdout).unwrap();
    let result = v3_result(&json, "purge", "success", 0);
    assert_eq!(result["removed_files"], 1);
    assert!(!first_path.exists());
    assert!(second_path.exists());
    assert_eq!(std::fs::read(&second_path).unwrap(), second_before);

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn minute_fill_static_universe_dry_run_does_not_warm_the_cache() {
    let cache_dir = temp_dir("minute-fill-static-universe-dry-run");
    let output = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "fill",
        "--universe",
        "symbol:KQ.m@SHFE.au",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-03",
        "--dry-run",
    ]);

    assert_eq!(output.status.code(), Some(1));
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "fill", "incomplete", 1);
    assert_eq!(result["report"]["cache_kind"], "minute");
    assert_eq!(result["report"]["remote_used"], false);
    assert_eq!(result["report"]["complete"], false);
    assert!(!cache_dir.exists());
}

#[test]
fn minute_fill_dry_run_uses_the_persisted_metadata_snapshot() {
    let cache_dir = temp_dir("minute-fill-dry-run-metadata-snapshot");
    let symbol = "KQ.i@SHFE.au";
    store_metadata_backed_minute_coverage(&cache_dir, symbol, day(2020, 1, 2));

    let output = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "fill",
        "--symbol",
        symbol,
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
        "--dry-run",
    ]);

    assert!(output.status.success());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "fill", "success", 0);
    assert_eq!(result["report"]["complete"], true);
    assert_eq!(result["report"]["remote_used"], false);

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn minute_stock_fill_rejects_futures_universe_selectors() {
    let output = run_json([
        "--kind",
        "minute",
        "--market",
        "stock",
        "fill",
        "--universe",
        "main:all",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-03",
        "--dry-run",
    ]);

    assert_eq!(output.status.code(), Some(2));
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let _ = v3_result(&json, "fill", "error", 2);
    assert_eq!(json["error"]["code"], "usage");
    assert!(json["error"]["message"].as_str().unwrap().contains("stock"));
}

#[test]
fn repair_stale_is_explicitly_limited_to_mutating_minute_fills() {
    let tick = run_json([
        "--kind",
        "tick",
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-03",
        "--dry-run",
        "--repair-stale",
    ]);
    assert_eq!(tick.status.code(), Some(2));
    let tick_json: Value = serde_json::from_slice(&tick.stdout).unwrap();
    assert_eq!(tick_json["error"]["code"], "usage");
    assert!(
        tick_json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("--kind minute")
    );

    let minute = run_json([
        "--kind",
        "minute",
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-03",
        "--dry-run",
        "--repair-stale",
    ]);
    assert_eq!(minute.status.code(), Some(2));
    let minute_json: Value = serde_json::from_slice(&minute.stdout).unwrap();
    assert_eq!(minute_json["error"]["code"], "usage");
    assert!(
        minute_json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("--dry-run")
    );
}

#[test]
fn minute_repair_stale_keeps_partitions_when_remote_fill_lock_is_busy() {
    let cache_dir = temp_dir("minute-repair-stale-lock-busy");
    let symbol = "KQ.i@SHFE.au";
    let month_path = stale_minute_partition(&cache_dir, symbol);
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    let lock = cache.try_acquire_remote_fill_lock().unwrap();

    let output = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "fill",
        "--symbol",
        symbol,
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
        "--repair-stale",
    ]);

    assert_eq!(
        output.status.code(),
        Some(75),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(month_path.exists());

    drop(lock);
    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn minute_repair_stale_requires_auth_before_removing_partitions() {
    let cache_dir = temp_dir("minute-repair-stale-auth");
    let symbol = "KQ.i@SHFE.au";
    let month_path = stale_minute_partition(&cache_dir, symbol);

    let output = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "fill",
        "--symbol",
        symbol,
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
        "--repair-stale",
    ]);

    assert_eq!(output.status.code(), Some(1));
    assert!(month_path.exists());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("remote backtest cache fill requires auth")
    );

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn tick_fill_rejects_stock_market_instead_of_silently_using_futures() {
    let output = run_json([
        "--kind",
        "tick",
        "--market",
        "stock",
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-03",
        "--dry-run",
    ]);

    assert_eq!(output.status.code(), Some(2));
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let _ = v3_result(&json, "fill", "error", 2);
    assert_eq!(json["error"]["code"], "usage");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("--kind minute")
    );
}

#[test]
fn minute_fill_reuses_complete_coverage_without_auth_and_writes_a_minute_report() {
    let cache_dir = temp_dir("minute-fill-complete");
    let start = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    let end = backtest_tick_trading_day_range(day(2020, 1, 3)).unwrap();
    let cache = MinuteKlineCache::open(&cache_dir).unwrap();
    cache
        .store_final_range(
            "SHFE.rb2601",
            start.start_ns,
            end.end_ns,
            &MinuteKlineCacheSnapshot::cst_v1(),
            &[],
        )
        .unwrap();
    let report_path = cache_dir.join("minute-fill-report.json");

    let output = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-03",
        "--report",
        report_path.to_str().unwrap(),
    ]);

    assert!(output.status.success());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "fill", "success", 0);
    assert_eq!(result["cache_kind"], "minute");
    assert_eq!(result["report"]["remote_used"], false);
    assert_eq!(result["report"]["complete"], true);
    assert_eq!(result["report"]["symbols"][0]["symbol"], "SHFE.rb2601");
    assert!(report_path.exists());
    let persisted: Value = serde_json::from_slice(&fs::read(&report_path).unwrap()).unwrap();
    assert_eq!(persisted["schema_version"], 3);
    assert_eq!(persisted["cache_kind"], "minute");

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn tick_and_minute_auth_failures_after_planning_persist_v3_reports() {
    for kind in ["tick", "minute"] {
        let cache_dir = temp_dir(&format!("{kind}-failed-report-v3"));
        BacktestTickCache::open(&cache_dir).unwrap();
        let report_path = cache_dir.join(format!("{kind}-failed-v3.json"));
        let output = run_without_auth_json([
            "--cache-dir",
            cache_dir.to_str().unwrap(),
            "--kind",
            kind,
            "fill",
            "--symbol",
            "SHFE.rb2601",
            "--start-day",
            "2020-01-02",
            "--end-day",
            "2020-01-02",
            "--report",
            report_path.to_str().unwrap(),
        ]);

        assert!(!output.status.success(), "kind={kind}");
        let report: Value = serde_json::from_slice(&fs::read(&report_path).unwrap()).unwrap();
        assert_eq!(report["schema_version"], 3, "kind={kind}");
        assert_eq!(report["cache_kind"], kind);
        assert_eq!(report["status"], "failed");
        assert_eq!(report["interrupted"], false);
        assert!(report["error"].is_string());
        assert_eq!(report["symbols"][0]["status"], "failed");

        let _ = fs::remove_dir_all(cache_dir);
    }
}

#[test]
fn minute_verify_uses_a_minute_fill_report_and_reads_final_coverage() {
    let cache_dir = temp_dir("minute-verify-report");
    let start = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    let cache = MinuteKlineCache::open(&cache_dir).unwrap();
    cache
        .store_final_range(
            "SHFE.rb2601",
            start.start_ns,
            start.end_ns,
            &MinuteKlineCacheSnapshot::cst_v1(),
            &[],
        )
        .unwrap();
    let report_path = cache_dir.join("minute-verify-report.json");
    let fill = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
        "--report",
        report_path.to_str().unwrap(),
    ]);
    assert!(fill.status.success());

    let verified = run_without_auth_json([
        "--kind",
        "minute",
        "verify",
        "--report",
        report_path.to_str().unwrap(),
    ]);
    assert!(verified.status.success());
    let json: Value = serde_json::from_slice(&verified.stdout).unwrap();
    let result = v3_result(&json, "verify", "success", 0);
    assert_eq!(result["cache_kind"], "minute");
    assert_eq!(result["source_report"], "bound");
    assert_eq!(result["coverage_complete"], true);
    assert_eq!(result["symbols"], json!(["SHFE.rb2601"]));
    assert_eq!(result["requested_days"]["start_day"], "2020-01-02");
    assert_eq!(result["requested_days"]["end_day"], "2020-01-02");

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn minute_verify_uses_the_persisted_metadata_snapshot() {
    let cache_dir = temp_dir("minute-verify-metadata-snapshot");
    let symbol = "KQ.i@SHFE.au";
    store_metadata_backed_minute_coverage(&cache_dir, symbol, day(2020, 1, 2));

    let verified = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "verify",
        "--symbol",
        symbol,
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
    ]);

    assert!(verified.status.success());
    let json: Value = serde_json::from_slice(&verified.stdout).unwrap();
    let result = v3_result(&json, "verify", "success", 0);
    assert_eq!(result["coverage_complete"], true);

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn minute_verify_rejects_a_concurrent_shared_fill_root_gate() {
    let cache_dir = temp_dir("minute-verify-root-lock-busy");
    let root_gate = BacktestTickCache::open(&cache_dir).unwrap();
    let shared_lock = root_gate.try_acquire_remote_fill_shared_lock().unwrap();

    let verified = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "verify",
        "--symbol",
        "KQ.i@SHFE.au",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
    ]);

    assert_eq!(verified.status.code(), Some(75));
    let json: Value = serde_json::from_slice(&verified.stdout).unwrap();
    let _ = v3_result(&json, "verify", "error", 75);
    assert_eq!(json["error"]["code"], "cache_busy");

    drop(shared_lock);
    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn minute_purge_requires_yes_and_dry_run_only_lists_the_month_partition() {
    let cache_dir = temp_dir("minute-purge");
    let range = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    let cache = MinuteKlineCache::open(&cache_dir).unwrap();
    cache
        .store_final_range(
            "SHFE.rb2601",
            range.start_ns,
            range.end_ns,
            &MinuteKlineCacheSnapshot::cst_v1(),
            &[],
        )
        .unwrap();
    let month_path = cache.month_file_path("SHFE.rb2601", "202001");
    assert!(month_path.exists());
    let root_gate = BacktestTickCache::open(&cache_dir).unwrap();
    let shared_lock = root_gate.try_acquire_remote_fill_shared_lock().unwrap();

    let dry_run = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "purge",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
        "--dry-run",
    ]);
    assert!(dry_run.status.success());
    assert!(month_path.exists());
    let json: Value = serde_json::from_slice(&dry_run.stdout).unwrap();
    let result = v3_result(&json, "purge", "success", 0);
    assert_eq!(result["cache_kind"], "minute");
    assert_eq!(result["dry_run"], true);
    assert_eq!(result["would_remove_files"].as_array().unwrap().len(), 1);

    let not_confirmed = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "purge",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
    ]);
    assert_eq!(not_confirmed.status.code(), Some(2));
    assert!(month_path.exists());

    let busy = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "purge",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
        "--yes",
    ]);
    assert_eq!(busy.status.code(), Some(75));
    let json: Value = serde_json::from_slice(&busy.stdout).unwrap();
    let _ = v3_result(&json, "purge", "error", 75);
    assert_eq!(json["error"]["code"], "cache_busy");
    assert!(month_path.exists());

    drop(shared_lock);
    let purged = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "purge",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
        "--yes",
    ]);
    assert!(purged.status.success());
    assert!(!month_path.exists());

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn minute_purge_dry_run_lists_a_stale_snapshot_month_partition() {
    let cache_dir = temp_dir("minute-purge-stale-snapshot");
    let symbol = "KQ.m@SHFE.au";
    let range = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    let cache = MinuteKlineCache::open(&cache_dir).unwrap();
    cache
        .store_final_range(
            symbol,
            range.start_ns,
            range.end_ns,
            &MinuteKlineCacheSnapshot::cst_v1(),
            &[],
        )
        .unwrap();
    let month_path = cache.month_file_path(symbol, "202001");

    BacktestHistoryMetadataCache::open(&cache_dir)
        .unwrap()
        .store_snapshot(BacktestHistoryMetadataSnapshot {
            schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
            market_kind: BacktestHistoryMarketKind::Futures,
            logical_symbol: symbol.to_string(),
            captured_at_ns: range.end_ns,
            trading_days: vec![BacktestHistoryTradingDay {
                date: day(2020, 1, 2).to_string(),
                is_trading_day: true,
                start_ns: range.start_ns,
                end_ns: range.end_ns,
            }],
            session: KlineSessionTemplate::new("changed-session", Vec::new()).unwrap(),
            physical_segments: vec![BacktestHistoryPhysicalSegment {
                physical_symbol: symbol.to_string(),
                start_ns: range.start_ns,
                end_ns: range.end_ns,
            }],
            snapshot_hash: String::new(),
        })
        .unwrap();

    let dry_run = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "purge",
        "--symbol",
        symbol,
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
        "--dry-run",
    ]);

    assert!(
        dry_run.status.success(),
        "dry-run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&dry_run.stdout),
        String::from_utf8_lossy(&dry_run.stderr)
    );
    assert!(month_path.exists());
    let json: Value = serde_json::from_slice(&dry_run.stdout).unwrap();
    let result = v3_result(&json, "purge", "success", 0);
    assert_eq!(result["would_remove_files"].as_array().unwrap().len(), 1);
    let canonical_month_path = fs::canonicalize(&month_path).unwrap();
    assert_eq!(
        result["would_remove_files"][0]["path"],
        canonical_month_path.to_string_lossy().as_ref()
    );

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn minute_fill_progress_jsonl_uses_v2_and_identifies_the_cache_kind() {
    let cache_dir = temp_dir("minute-progress-jsonl");
    let range = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    let cache = MinuteKlineCache::open(&cache_dir).unwrap();
    cache
        .store_final_range(
            "SHFE.rb2601",
            range.start_ns,
            range.end_ns,
            &MinuteKlineCacheSnapshot::cst_v1(),
            &[],
        )
        .unwrap();

    let output = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
        "--progress",
        "jsonl",
    ]);

    assert!(output.status.success());
    let records = String::from_utf8_lossy(&output.stderr)
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert!(!records.is_empty());
    assert!(records.iter().all(|record| {
        record["schema_version"] == 2
            && record["kind"] == "tqsdk-cache.progress"
            && record["cache_kind"] == "minute"
    }));
    assert!(
        records.iter().any(|record| record["event"] == "snapshot"),
        "minute fill should leave the planning phase after receiving its plan telemetry"
    );
    assert_eq!(records.last().unwrap()["event"], "complete");

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn minute_fill_progress_uses_the_resolved_trading_calendar() {
    let cache_dir = temp_dir("minute-progress-calendar");
    let start_day = day(2026, 7, 17);
    let end_day = day(2026, 7, 20);
    let start_range = backtest_tick_trading_day_range(start_day).unwrap();
    let end_range = backtest_tick_trading_day_range(end_day).unwrap();
    let cache = MinuteKlineCache::open(&cache_dir).unwrap();
    cache
        .store_final_range(
            "SHFE.rb2601",
            start_range.start_ns,
            end_range.end_ns,
            &MinuteKlineCacheSnapshot::cst_v1(),
            &[],
        )
        .unwrap();
    let calendar = TradingCalendarHolidaysSnapshot::from_holidays(
        TradingCalendarHolidays::new("https://example.invalid/holidays.json", [day(2026, 1, 1)])
            .unwrap(),
    )
    .unwrap();
    write_trading_calendar_holidays_snapshot(&cache_dir, &calendar).unwrap();

    let output = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2026-07-17",
        "--end-day",
        "2026-07-20",
        "--progress",
        "jsonl",
    ]);

    assert!(output.status.success());
    let records = String::from_utf8_lossy(&output.stderr)
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    let snapshot = records
        .iter()
        .find(|record| record["event"] == "snapshot")
        .expect("minute fill should emit a plan snapshot");
    assert_eq!(snapshot["calendar"]["source"], "local");
    assert_eq!(snapshot["coverage"]["covered_days"], 2);
    assert_eq!(snapshot["coverage"]["planned_days"], 2);

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn fill_dry_run_does_not_create_cache_data_or_start_remote_fill() {
    let cache_dir = temp_dir("dry-run");
    let output = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-03",
        "--dry-run",
    ]);

    assert_eq!(output.status.code(), Some(1));
    assert!(!cache_dir.exists());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "fill", "incomplete", 1);
    assert_eq!(json["error"], Value::Null);
    assert_eq!(result["command"], "fill");
    assert_eq!(result["dry_run"], true);
    assert_eq!(result["report"]["complete"], false);
}

#[test]
fn fill_reuses_complete_cache_without_auth_and_report_binds_its_root() {
    let cache_dir = temp_dir("complete-fill");
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    let start = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    let end = backtest_tick_trading_day_range(day(2020, 1, 3)).unwrap();
    cache
        .mark_complete("SHFE.rb2601", start.start_ns, end.end_ns, 0, None)
        .unwrap();
    let report_path = cache_dir.join("fill-report.json");

    let output = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-03",
        "--report",
        report_path.to_str().unwrap(),
    ]);

    assert!(output.status.success());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "fill", "success", 0);
    assert_eq!(result["schema_version"], 2);
    assert_eq!(result["report"]["complete"], true);
    assert_eq!(result["report"]["remote_used"], false);
    assert_eq!(
        result["report"]["selector"]["symbols"],
        serde_json::json!(["SHFE.rb2601"])
    );
    assert!(result["report"]["resolved_range"].is_object());
    assert_eq!(result["report"]["calendar"]["mode"], "auto");
    assert_eq!(result["report"]["calendar"]["source"], "partition_fallback");
    assert_eq!(
        result["report"]["physical_symbols"][0]["day_stats"]["planned_days"],
        2
    );
    assert_eq!(
        result["report"]["physical_symbols"][0]["day_stats"]["received_days"],
        0
    );
    assert!(report_path.exists());
    let persisted: Value = serde_json::from_slice(&fs::read(&report_path).unwrap()).unwrap();
    assert_eq!(persisted["schema_version"], 3);
    assert_eq!(persisted["cache_kind"], "tick");

    let verified = run_without_auth_json(["verify", "--report", report_path.to_str().unwrap()]);
    assert!(verified.status.success());
    let verified_json: Value = serde_json::from_slice(&verified.stdout).unwrap();
    let verified_result = v3_result(&verified_json, "verify", "success", 0);
    assert_eq!(verified_result["cache_kind"], "tick");
    assert_eq!(verified_result["source_report"], "bound");
    assert_eq!(verified_result["coverage_complete"], true);
    assert_eq!(verified_result["symbols"], json!(["SHFE.rb2601"]));
    assert_eq!(verified_result["requested_days"]["start_day"], "2020-01-02");
    assert_eq!(verified_result["requested_days"]["end_day"], "2020-01-03");

    let another_cache_dir = temp_dir("wrong-root");
    let wrong_root = run_without_auth_json([
        "--cache-dir",
        another_cache_dir.to_str().unwrap(),
        "verify",
        "--report",
        report_path.to_str().unwrap(),
    ]);
    assert_eq!(wrong_root.status.code(), Some(2));
    let wrong_root_json: Value = serde_json::from_slice(&wrong_root.stdout).unwrap();
    let _ = v3_result(&wrong_root_json, "verify", "error", 2);
    assert_eq!(wrong_root_json["error"]["code"], "usage");

    let _ = std::fs::remove_dir_all(&cache_dir);
    let _ = std::fs::remove_dir_all(&another_cache_dir);
}

#[test]
fn fill_rejects_last_trading_days_when_calendar_is_off() {
    let output = run_json([
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--last-trading-days",
        "5",
        "--calendar",
        "off",
    ]);

    assert_eq!(output.status.code(), Some(2));
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let _ = v3_result(&json, "fill", "error", 2);
    assert_eq!(json["error"]["code"], "usage");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("--last-trading-days")
    );
}

#[test]
fn fill_start_day_defaults_to_latest_closed_trading_day() {
    let output = run_json([
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
    ]);

    assert_eq!(output.status.code(), Some(0));
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let _ = v3_result(&json, "fill", "success", 0);
}

#[test]
fn fill_progress_off_keeps_stderr_quiet_and_stdout_machine_readable() {
    let cache_dir = temp_dir("progress-off");
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    let start = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    cache
        .mark_complete("SHFE.rb2601", start.start_ns, start.end_ns, 0, None)
        .unwrap();

    let output = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
        "--progress",
        "off",
    ]);

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "fill", "success", 0);
    assert_eq!(result["report"]["complete"], true);

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn fill_progress_plain_keeps_stdout_json_and_emits_structured_stderr() {
    let cache_dir = temp_dir("progress-plain");
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    let start = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    cache
        .mark_complete("SHFE.rb2601", start.start_ns, start.end_ns, 0, None)
        .unwrap();

    let output = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
        "--progress",
        "plain",
    ]);

    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("tqsdk-cache: phase=complete"));
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "fill", "success", 0);
    assert_eq!(result["report"]["complete"], true);

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn fill_progress_plain_reports_a_specific_failure_summary() {
    let cache_dir = temp_dir("progress-failure");
    let output = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-03",
        "--calendar",
        "off",
        "--progress",
        "plain",
    ]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("fill failed; strict local coverage was not committed"));
    assert!(!stderr.contains("fill ended before a final progress summary"));
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let _ = v3_result(&json, "fill", "error", 1);
    assert!(json["error"]["code"].is_string());

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn fill_auto_detects_current_open_day_without_opt_in() {
    let cache_dir = temp_dir("open-day-auto-dry-run");
    let open_day = current_open_day().format("%Y-%m-%d").to_string();
    let output = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        open_day.as_str(),
        "--end-day",
        open_day.as_str(),
        "--dry-run",
    ]);

    assert_eq!(output.status.code(), Some(1));
    assert!(!cache_dir.exists());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "fill", "incomplete", 1);
    assert_eq!(result["report"]["coverage_state"], "provisional");
    assert_eq!(result["report"]["day_complete"], false);
    assert!(result["report"]["complete_through_ns"].is_null());
}

#[test]
fn fill_require_final_rejects_current_open_day() {
    let cache_dir = temp_dir("open-day-require-final");
    let open_day = current_open_day().format("%Y-%m-%d").to_string();
    let output = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        open_day.as_str(),
        "--end-day",
        open_day.as_str(),
        "--require-final",
        "--dry-run",
    ]);

    assert_eq!(output.status.code(), Some(2));
    assert!(!cache_dir.exists());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let _ = v3_result(&json, "fill", "error", 2);
    assert_eq!(json["error"]["code"], "usage");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("requires a closed --end-day")
    );
}

#[test]
fn fill_dry_run_accepts_current_open_day_with_opt_in() {
    let cache_dir = temp_dir("open-day-dry-run");
    let open_day = current_open_day().format("%Y-%m-%d").to_string();
    let output = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        open_day.as_str(),
        "--end-day",
        open_day.as_str(),
        "--include-open-day",
        "--dry-run",
    ]);

    assert_eq!(output.status.code(), Some(1));
    assert!(!cache_dir.exists());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "fill", "incomplete", 1);
    assert_eq!(result["report"]["coverage_state"], "provisional");
    assert_eq!(result["report"]["day_complete"], false);
    assert!(result["report"]["complete_through_ns"].is_null());
}

#[test]
fn fill_dry_run_reports_existing_partial_open_day_high_water_mark() {
    let cache_dir = temp_dir("open-day-partial-checkpoint");
    let open_day = current_open_day();
    let open_range = backtest_tick_trading_day_range(open_day).unwrap();
    let now_ns = current_time_ns();
    let horizon_ns = now_ns.saturating_sub(5_000_000_000).min(open_range.end_ns);
    assert!(horizon_ns > open_range.start_ns + 1);
    let checkpoint_ns = open_range.start_ns + (horizon_ns - open_range.start_ns) / 2;
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    cache
        .mark_provisional(
            "SHFE.rb2601",
            open_range.start_ns,
            checkpoint_ns,
            checkpoint_ns,
            0,
            None,
        )
        .unwrap();
    let open_day = open_day.format("%Y-%m-%d").to_string();

    let output = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        open_day.as_str(),
        "--end-day",
        open_day.as_str(),
        "--dry-run",
    ]);

    assert_eq!(output.status.code(), Some(1));
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "fill", "incomplete", 1);
    assert_eq!(
        result["report"]["complete_through_ns"].as_i64(),
        Some(checkpoint_ns)
    );
    assert_eq!(result["report"]["day_complete"], false);

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn fill_reuses_current_open_day_checkpoint_without_auth() {
    let cache_dir = temp_dir("open-day-checkpoint");
    let open_day = current_open_day();
    let open_range = backtest_tick_trading_day_range(open_day).unwrap();
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    cache
        .mark_provisional(
            "SHFE.rb2601",
            open_range.start_ns,
            open_range.end_ns,
            open_range.end_ns,
            0,
            None,
        )
        .unwrap();
    let open_day = open_day.format("%Y-%m-%d").to_string();

    let output = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        open_day.as_str(),
        "--end-day",
        open_day.as_str(),
        "--progress",
        "off",
    ]);

    assert!(output.status.success());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "fill", "success", 0);
    assert_eq!(result["report"]["complete"], false);
    assert_eq!(result["report"]["coverage_state"], "provisional");
    assert_eq!(result["report"]["day_complete"], false);
    assert!(result["report"]["complete_through_ns"].is_i64());
    assert_eq!(result["report"]["remote_used"], false);

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn fill_treats_a_previous_day_checkpoint_as_needing_final_reconciliation() {
    let cache_dir = temp_dir("closed-day-provisional-checkpoint");
    let open_range = backtest_tick_trading_day_range(current_open_day()).unwrap();
    let previous_day =
        backtest_tick_trading_day_for_timestamp_ns(open_range.start_ns.saturating_sub(1)).unwrap();
    let previous_range = backtest_tick_trading_day_range(previous_day).unwrap();
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    cache
        .mark_provisional(
            "SHFE.rb2601",
            previous_range.start_ns,
            previous_range.end_ns,
            previous_range.end_ns,
            0,
            None,
        )
        .unwrap();
    let previous_day = previous_day.format("%Y-%m-%d").to_string();

    let output = run_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        previous_day.as_str(),
        "--end-day",
        previous_day.as_str(),
        "--include-open-day",
        "--dry-run",
    ]);

    assert_eq!(output.status.code(), Some(1));
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "fill", "incomplete", 1);
    assert_eq!(result["report"]["coverage_state"], "final");
    assert!(result["report"]["complete_through_ns"].is_null());
    assert_eq!(result["report"]["day_complete"], false);

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn fill_reports_cross_process_root_lock_contention() {
    let cache_dir = temp_dir("root-lock");
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    let _lock = cache.try_acquire_remote_fill_lock().unwrap();

    let output = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-03",
    ]);

    assert_eq!(output.status.code(), Some(75));
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let _ = v3_result(&json, "fill", "error", 75);
    assert_eq!(json["error"]["code"], "cache_busy");
    assert_eq!(json["error"]["retryable"], true);

    drop(_lock);
    let _ = std::fs::remove_dir_all(&cache_dir);
}

#[test]
fn explicit_json_clap_parse_errors_use_the_v3_error_envelope() {
    let output = run_json(["--definitely-not-an-option"]);

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stderr.is_empty());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let _ = v3_result(&json, "unknown", "error", 2);
    assert_eq!(json["error"]["code"], "usage");
    assert!(json["error"]["message"].is_string());
}

#[test]
fn default_output_is_human_readable_even_when_stdout_is_captured() {
    let cache_dir = temp_dir("output-text");
    let output = run(["--cache-dir", cache_dir.to_str().unwrap(), "inventory"]);

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("tqsdk-cache inventory: success"));
    assert!(stdout.contains("Files: 0 | Days: 0 | Size: 0 B | Problems: 0"));
    assert!(stdout.contains("JSON output: --output-format json"));
    assert!(!stdout.trim_start().starts_with('{'));

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn fill_tick_accepts_a_pinned_historical_universe_plan() {
    let cache_dir = temp_dir("historical-universe-plan");
    let scope = tqsdk_data::DynamicUniverseScope::all();
    let plan = tqsdk_data::CatalogSnapshot::new(
        "fixture-v1",
        "calendar-sha256:fixture",
        true,
        scope.clone(),
        vec![
            tqsdk_data::CatalogContract::new(
                "SHFE.au2406",
                "SHFE",
                "au",
                vec![tqsdk_data::ActiveInterval::new(10, 20).unwrap()],
            )
            .unwrap(),
        ],
    )
    .unwrap()
    .compile_timeline(0, 30, scope, [])
    .unwrap()
    .prepare(tqsdk_data::UniverseBudget::new(4, 4).unwrap())
    .unwrap();
    BacktestTickCache::open(&cache_dir)
        .unwrap()
        .mark_complete("SHFE.au2406", 10, 20, 0, None)
        .unwrap();
    let plan_path = cache_dir.join("historical-universe-plan.json");
    fs::write(&plan_path, serde_json::to_vec(&plan).unwrap()).unwrap();

    let output = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "fill",
        "--universe-timeline",
        plan_path.to_str().unwrap(),
        "--dry-run",
    ]);
    assert!(output.status.success());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "fill", "success", 0);
    assert_eq!(result["plan_sha256"], plan.plan_sha256);
    assert_eq!(result["symbols_warmed"], 1);

    let _ = fs::remove_dir_all(cache_dir);
}

#[test]
fn fill_daily_accepts_a_pinned_historical_universe_plan() {
    let cache_dir = temp_dir("historical-universe-daily-plan");
    let scope = tqsdk_data::DynamicUniverseScope::all();
    let plan = tqsdk_data::CatalogSnapshot::new(
        "fixture-v1",
        "calendar-sha256:fixture",
        true,
        scope.clone(),
        vec![
            tqsdk_data::CatalogContract::new(
                "SHFE.au2406",
                "SHFE",
                "au",
                vec![tqsdk_data::ActiveInterval::new(10, 20).unwrap()],
            )
            .unwrap(),
        ],
    )
    .unwrap()
    .compile_timeline(0, 30, scope, [])
    .unwrap()
    .prepare(tqsdk_data::UniverseBudget::new(4, 4).unwrap())
    .unwrap();
    BacktestTickCache::open(&cache_dir).unwrap();
    let plan_path = cache_dir.join("historical-universe-plan.json");
    fs::write(&plan_path, serde_json::to_vec(&plan).unwrap()).unwrap();

    let output = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "daily",
        "fill",
        "--universe-timeline",
        plan_path.to_str().unwrap(),
        "--dry-run",
    ]);
    assert_eq!(output.status.code(), Some(1));
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "fill", "incomplete", 1);
    assert_eq!(result["universe_timeline"]["plan_sha256"], plan.plan_sha256);
    assert_eq!(result["universe_timeline"]["physical_symbols"], 1);
    let _ = fs::remove_dir_all(cache_dir);
}

#[test]
fn fill_minute_accepts_a_pinned_historical_universe_plan() {
    let cache_dir = temp_dir("historical-universe-minute-plan");
    let scope = tqsdk_data::DynamicUniverseScope::all();
    let plan = tqsdk_data::CatalogSnapshot::new(
        "fixture-v1",
        "calendar-sha256:fixture",
        true,
        scope.clone(),
        vec![
            tqsdk_data::CatalogContract::new(
                "SHFE.au2406",
                "SHFE",
                "au",
                vec![tqsdk_data::ActiveInterval::new(10, 20).unwrap()],
            )
            .unwrap(),
        ],
    )
    .unwrap()
    .compile_timeline(0, 30, scope, [])
    .unwrap()
    .prepare(tqsdk_data::UniverseBudget::new(4, 4).unwrap())
    .unwrap();
    BacktestTickCache::open(&cache_dir).unwrap();
    let plan_path = cache_dir.join("historical-universe-plan.json");
    fs::write(&plan_path, serde_json::to_vec(&plan).unwrap()).unwrap();

    let output = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "--kind",
        "minute",
        "fill",
        "--universe-timeline",
        plan_path.to_str().unwrap(),
        "--dry-run",
    ]);
    assert_eq!(output.status.code(), Some(1));
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "fill", "incomplete", 1);
    assert_eq!(result["universe_timeline"]["plan_sha256"], plan.plan_sha256);
    assert_eq!(result["universe_timeline"]["physical_symbols"], 1);
    let _ = fs::remove_dir_all(cache_dir);
}

#[test]
fn default_output_uses_human_stderr_for_runtime_errors() {
    let output = run([
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--last-trading-days",
        "5",
        "--calendar",
        "off",
    ]);

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("tqsdk-cache fill: error (exit 2)"));
    assert!(stderr.contains("--last-trading-days"));
    assert!(!stderr.trim_start().starts_with('{'));
}

#[test]
fn default_text_rejects_json_rendering_options() {
    let output = run(["--pretty", "inventory"]);

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("require --output-format json"));
}

#[test]
fn output_schema_v2_preserves_legacy_top_level_result_and_stderr_errors() {
    let cache_dir = temp_dir("output-v2");
    let output = run([
        "--output-format",
        "json",
        "--output-schema",
        "v2",
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "inventory",
    ]);

    assert!(output.status.success());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["schema_version"], 2);
    assert_eq!(json["command"], "inventory");
    assert_eq!(json["total_files"], 0);
    assert!(json.get("result").is_none());
    assert!(json.get("kind").is_none());

    let usage_error = run([
        "--output-format",
        "json",
        "--output-schema",
        "v2",
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--last-trading-days",
        "5",
        "--calendar",
        "off",
    ]);
    assert_eq!(usage_error.status.code(), Some(2));
    assert!(usage_error.stdout.is_empty());
    assert!(String::from_utf8_lossy(&usage_error.stderr).contains("--last-trading-days"));

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn fill_progress_jsonl_emits_versioned_stderr_records() {
    let cache_dir = temp_dir("progress-jsonl");
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    let start = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    cache
        .mark_complete("SHFE.rb2601", start.start_ns, start.end_ns, 0, None)
        .unwrap();

    let output = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-02",
        "--progress",
        "jsonl",
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(stdout.lines().count(), 1);
    let json: Value = serde_json::from_str(stdout.trim()).unwrap();
    let result = v3_result(&json, "fill", "success", 0);
    assert_eq!(result["report"]["complete"], true);

    let stderr = String::from_utf8_lossy(&output.stderr);
    let mut previous_sequence = 0;
    let records = stderr
        .lines()
        .map(|line| {
            let record: Value = serde_json::from_str(line).unwrap();
            assert_eq!(record["schema_version"], 2);
            assert_eq!(record["kind"], "tqsdk-cache.progress");
            assert_eq!(record["cache_kind"], "tick");
            assert!(matches!(
                record["event"].as_str(),
                Some("planning" | "inspection" | "snapshot" | "complete")
            ));
            let sequence = record["sequence"].as_u64().unwrap();
            assert!(sequence > previous_sequence);
            previous_sequence = sequence;
            record
        })
        .collect::<Vec<_>>();
    assert!(!records.is_empty());
    let inspection = records
        .iter()
        .find(|record| record["event"] == "inspection")
        .expect("cache inspection should emit a JSONL progress record");
    assert_eq!(inspection["inspection"]["total_ranges"], 1);
    assert_eq!(inspection["inspection"]["checked_ranges"], 1);
    assert_eq!(inspection["inspection"]["complete_ranges"], 1);
    assert_eq!(inspection["inspection"]["incomplete_ranges"], 0);
    assert_eq!(inspection["inspection"]["physical_symbol"], "SHFE.rb2601");
    assert_eq!(records.last().unwrap()["event"], "complete");
    assert_eq!(records.last().unwrap()["status"], "complete");

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn query_jsonl_reads_a_cache_only_main_contract_and_canonicalizes_fields() {
    let cache_dir = temp_dir("query-jsonl-cache-only");
    let fixture = seed_query_fixture(&cache_dir, 8, false);
    let start = rfc3339(fixture.start_ns);
    let end = rfc3339(fixture.tick_end_ns);
    let output = run_query_without_auth(&[
        "--cache-dir".to_string(),
        cache_dir.display().to_string(),
        "--output-format".to_string(),
        "jsonl".to_string(),
        "query".to_string(),
        "--symbol".to_string(),
        fixture.logical_symbol.clone(),
        "--series".to_string(),
        "tick".to_string(),
        "--start".to_string(),
        start,
        "--end".to_string(),
        end,
        "--policy".to_string(),
        "cache-only".to_string(),
        "--timestamp".to_string(),
        "offset".to_string(),
        "--fields".to_string(),
        "last_price,time,volume".to_string(),
    ]);

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let records = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(records[0]["record"], "manifest");
    assert_eq!(records[0]["protocol"], "tqsdk-history-jsonl/1");
    let block = records
        .iter()
        .find(|record| record["record"] == "block")
        .unwrap();
    assert_eq!(block["metadata"]["status"], "verified");
    assert_eq!(block["fields"], serde_json::json!(["t", "lp", "v"]));
    let row = records
        .iter()
        .find(|record| record["record"] == "row")
        .unwrap();
    let keys = row["data"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    assert_eq!(keys, vec!["lp", "t", "v"]);
    assert_eq!(row["data"]["t"], 0);
    assert_eq!(records.last().unwrap()["status"], "success");

    let _ = fs::remove_dir_all(cache_dir);
}

#[test]
fn query_remote_on_miss_rejects_an_exclusive_cache_root_gate() {
    let cache_dir = temp_dir("query-remote-root-lock-busy");
    let fixture = seed_query_fixture(&cache_dir, 1, false);
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    let exclusive = cache.try_acquire_consistency_read_lock().unwrap();
    let output = run_query_without_auth(&[
        "--cache-dir".to_string(),
        cache_dir.display().to_string(),
        "--output-format".to_string(),
        "json".to_string(),
        "query".to_string(),
        "--symbol".to_string(),
        fixture.logical_symbol,
        "--series".to_string(),
        "tick".to_string(),
        "--start".to_string(),
        rfc3339(fixture.start_ns),
        "--end".to_string(),
        rfc3339(fixture.tick_end_ns),
        "--policy".to_string(),
        "remote-on-miss".to_string(),
    ]);

    assert_eq!(output.status.code(), Some(75));
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let _ = v3_result(&json, "query", "error", 75);
    assert_eq!(json["error"]["code"], "cache_busy");

    drop(exclusive);
    let _ = fs::remove_dir_all(cache_dir);
}

#[test]
fn query_llm_csv_is_lossless_without_a_budget_and_writes_atomically() {
    let cache_dir = temp_dir("query-llm-atomic");
    let fixture = seed_query_fixture(&cache_dir, 8, false);
    let output_path = cache_dir.join("analysis.csv");
    let output = run_query_without_auth(&[
        "--cache-dir".to_string(),
        cache_dir.display().to_string(),
        "--output-format".to_string(),
        "llm-csv".to_string(),
        "query".to_string(),
        "--symbol".to_string(),
        fixture.logical_symbol.clone(),
        "--series".to_string(),
        "tick".to_string(),
        "--start".to_string(),
        rfc3339(fixture.start_ns),
        "--end".to_string(),
        rfc3339(fixture.tick_end_ns),
        "--policy".to_string(),
        "cache-only".to_string(),
        "--fields".to_string(),
        "last_price,time,volume,open_interest".to_string(),
        "--output".to_string(),
        output_path.display().to_string(),
    ]);

    assert!(output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("wrote"));
    let content = fs::read_to_string(&output_path).unwrap();
    assert!(content.starts_with("protocol,tqllm-csv/3\n"));
    assert!(
        content.contains("meta,model,gpt-5.6,numbers,decimal,compression,lossless,partial,false")
    );
    assert!(content.contains("block,b1,symbol,KQ.m@SHFE.au,series,tick,rows,8,source,cache,final,true,underlying,SHFE.au2002"));
    assert!(content.contains("time,mode,iso,timezone,Asia/Shanghai,precision,s,ref,2020-01-01T19:00:00+08:00,end,2020-01-01T19:00:09+08:00,end_exclusive,true,row_time,event"));
    assert!(content.contains("columns,t=time,lp=last_price,v=cumulative_volume,oi=open_interest"));
    assert!(content.contains("\nt,lp,v,oi\n"));
    assert!(content.contains("\n2020-01-01T19:00:00+08:00,100,100,1000\n"));
    assert!(content.contains("block_end,rows,8"));
    assert!(content.contains("document_end,status,success"));
    assert!(!content.contains(".000000000Z"));
    assert!(!content.contains("period_ns"));
    assert!(!content.contains("snapshot_hash"));
    assert!(!content.contains("query_id"));
    assert!(!content.contains("drill_down_id"));
    assert!(content.ends_with('\n'));

    let _ = fs::remove_dir_all(cache_dir);
}

#[test]
fn query_llm_csv_supports_compact_relative_time_offsets() {
    let cache_dir = temp_dir("query-llm-time-modes");
    let fixture = seed_query_fixture(&cache_dir, 8, true);
    let arguments = [
        "--cache-dir".to_string(),
        cache_dir.display().to_string(),
        "--output-format".to_string(),
        "llm-csv".to_string(),
        "query".to_string(),
        "--symbol".to_string(),
        fixture.logical_symbol.clone(),
        "--series".to_string(),
        "kline".to_string(),
        "--period".to_string(),
        "1m".to_string(),
        "--start".to_string(),
        rfc3339(fixture.start_ns),
        "--end".to_string(),
        rfc3339(fixture.kline_end_ns),
        "--policy".to_string(),
        "cache-only".to_string(),
        "--fields".to_string(),
        "time,open,high,low,close,volume,close_oi".to_string(),
    ];

    let offset = run_query_without_auth(
        &arguments
            .iter()
            .cloned()
            .chain(["--llm-time".to_string(), "offset".to_string()])
            .collect::<Vec<_>>(),
    );

    assert!(offset.status.success());
    let offset = String::from_utf8(offset.stdout).unwrap();
    assert!(offset.contains("block,b1,symbol,KQ.m@SHFE.au,series,kline,period,1m,rows,3,source,cache,final,true,underlying,SHFE.au2002"));
    assert!(offset.contains(
        "time,mode,offset,timezone,Asia/Shanghai,unit,1m,ref,2020-01-01T19:00+08:00,end,3,end_exclusive,true,bar_time,start"
    ));
    assert!(
        offset
            .contains("\nt,o,h,l,c,v,oi\n0,100,101,99,100.5,10,101\n1,101,102,100,101.5,11,102\n")
    );

    let both = run_query_without_auth(
        &arguments
            .iter()
            .cloned()
            .chain(["--llm-time".to_string(), "both".to_string()])
            .collect::<Vec<_>>(),
    );

    assert!(both.status.success());
    let both = String::from_utf8(both.stdout).unwrap();
    assert!(both.contains("time,mode,both,timezone,Asia/Shanghai,precision,m,unit,1m,ref,2020-01-01T19:00+08:00,end,3,end_exclusive,true,bar_time,start"));
    assert!(
        both.contains("\nt,dt,o,h,l,c,v,oi\n2020-01-01T19:00+08:00,0,100,101,99,100.5,10,101\n")
    );

    let _ = fs::remove_dir_all(cache_dir);
}

#[test]
fn query_llm_csv_allows_utc_timezone_override() {
    let cache_dir = temp_dir("query-llm-utc-timezone");
    let fixture = seed_query_fixture(&cache_dir, 8, false);
    let output = run_query_without_auth(&[
        "--cache-dir".to_string(),
        cache_dir.display().to_string(),
        "--output-format".to_string(),
        "llm-csv".to_string(),
        "query".to_string(),
        "--symbol".to_string(),
        fixture.logical_symbol,
        "--series".to_string(),
        "tick".to_string(),
        "--start".to_string(),
        rfc3339(fixture.start_ns),
        "--end".to_string(),
        rfc3339(fixture.tick_end_ns),
        "--policy".to_string(),
        "cache-only".to_string(),
        "--fields".to_string(),
        "time,last_price".to_string(),
        "--llm-timezone".to_string(),
        "utc".to_string(),
    ]);

    assert!(output.status.success());
    let content = String::from_utf8(output.stdout).unwrap();
    assert!(content.starts_with("protocol,tqllm-csv/3\n"));
    assert!(content.contains("time,mode,iso,timezone,UTC,precision,s,ref,2020-01-01T11:00:00Z,end,2020-01-01T11:00:09Z,end_exclusive,true,row_time,event"));
    assert!(content.contains("\n2020-01-01T11:00:00Z,100\n"));

    let _ = fs::remove_dir_all(cache_dir);
}

#[test]
fn query_llm_csv_declares_price_tick_for_scaled_integers() {
    let cache_dir = temp_dir("query-llm-scaled-price");
    let fixture = seed_query_fixture(&cache_dir, 8, false);
    let output = run_query_without_auth(&[
        "--cache-dir".to_string(),
        cache_dir.display().to_string(),
        "--output-format".to_string(),
        "llm-csv".to_string(),
        "query".to_string(),
        "--symbol".to_string(),
        fixture.logical_symbol,
        "--series".to_string(),
        "tick".to_string(),
        "--start".to_string(),
        rfc3339(fixture.start_ns),
        "--end".to_string(),
        rfc3339(fixture.tick_end_ns),
        "--policy".to_string(),
        "cache-only".to_string(),
        "--fields".to_string(),
        "time,last_price".to_string(),
        "--number-format".to_string(),
        "scaled-int".to_string(),
        "--price-tick".to_string(),
        "0.1".to_string(),
    ]);

    assert!(output.status.success());
    let content = String::from_utf8(output.stdout).unwrap();
    assert!(
        content
            .contains("meta,model,gpt-5.6,numbers,scaled-int,compression,lossless,partial,false")
    );
    assert!(content.contains("source,cache,final,true,price_tick,0.1,underlying,SHFE.au2002"));
    assert!(content.contains("\n2020-01-01T19:00:00+08:00,1000\n"));

    let _ = fs::remove_dir_all(cache_dir);
}

#[test]
fn query_llm_csv_compresses_deterministically_within_a_token_budget() {
    let cache_dir = temp_dir("query-llm-budget");
    let fixture = seed_query_fixture(&cache_dir, 128, false);
    let output = run_query_without_auth(&[
        "--cache-dir".to_string(),
        cache_dir.display().to_string(),
        "--output-format".to_string(),
        "llm-csv".to_string(),
        "query".to_string(),
        "--symbol".to_string(),
        fixture.logical_symbol.clone(),
        "--series".to_string(),
        "tick".to_string(),
        "--start".to_string(),
        rfc3339(fixture.start_ns),
        "--end".to_string(),
        rfc3339(fixture.tick_end_ns),
        "--policy".to_string(),
        "cache-only".to_string(),
        "--data-token-budget".to_string(),
        "900".to_string(),
        "--focus".to_string(),
        "price".to_string(),
    ]);

    assert!(output.status.success());
    let content = String::from_utf8(output.stdout).unwrap();
    assert!(content.contains("compression,lossy"));
    assert!(content.contains("focus,price"));
    assert!(content.contains("rows_original,128"));
    assert!(!content.contains("rows_emitted,128"));

    let _ = fs::remove_dir_all(cache_dir);
}

#[test]
fn query_request_file_supports_mixed_tick_and_kline_blocks() {
    let cache_dir = temp_dir("query-request-file");
    let fixture = seed_query_fixture(&cache_dir, 8, true);
    let request_file = cache_dir.join("query.toml");
    fs::write(
        &request_file,
        format!(
            "version = 1\n\n[[request]]\nsymbol = \"{}\"\nseries = \"tick\"\nstart = \"{}\"\nend = \"{}\"\nfields = [\"time\", \"last_price\"]\nweight = 2\n\n[[request]]\nsymbol = \"{}\"\nseries = \"kline\"\nperiod = \"60s\"\nstart = \"{}\"\nend = \"{}\"\nfields = [\"close\", \"time\"]\n",
            fixture.logical_symbol,
            rfc3339(fixture.start_ns),
            rfc3339(fixture.tick_end_ns),
            fixture.logical_symbol,
            rfc3339(fixture.start_ns),
            rfc3339(fixture.kline_end_ns),
        ),
    )
    .unwrap();
    let output = run_query_without_auth(&[
        "--cache-dir".to_string(),
        cache_dir.display().to_string(),
        "--output-format".to_string(),
        "json".to_string(),
        "query".to_string(),
        "--request-file".to_string(),
        request_file.display().to_string(),
        "--policy".to_string(),
        "cache-only".to_string(),
    ]);

    assert!(output.status.success());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let result = v3_result(&json, "query", "success", 0);
    assert_eq!(result["blocks"].as_array().unwrap().len(), 2);
    assert_eq!(
        result["blocks"][0]["fields"],
        serde_json::json!(["t", "lp"])
    );
    assert_eq!(result["blocks"][1]["series"], "kline");
    assert_eq!(result["blocks"][1]["fields"], serde_json::json!(["t", "c"]));

    let _ = fs::remove_dir_all(cache_dir);
}

#[test]
fn query_llm_csv_requires_verified_metadata_unless_partial_is_explicit() {
    let cache_dir = temp_dir("query-llm-metadata");
    let range = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    let start_ns = range.start_ns + 60 * 1_000_000_000;
    let end_ns = start_ns + 2 * 1_000_000_000;
    BacktestTickCache::open(&cache_dir)
        .unwrap()
        .store_ticks(
            "SHFE.au2002",
            range.start_ns,
            range.end_ns,
            vec![Tick {
                id: 1,
                datetime: start_ns,
                last_price: 100.0,
                volume: 1,
                open_interest: 1,
                ..Tick::default()
            }],
        )
        .unwrap();
    let output = run_query_without_auth(&[
        "--cache-dir".to_string(),
        cache_dir.display().to_string(),
        "--output-format".to_string(),
        "llm-csv".to_string(),
        "query".to_string(),
        "--symbol".to_string(),
        "SHFE.au2002".to_string(),
        "--series".to_string(),
        "tick".to_string(),
        "--start".to_string(),
        rfc3339(start_ns),
        "--end".to_string(),
        rfc3339(end_ns),
        "--policy".to_string(),
        "cache-only".to_string(),
    ]);

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("verified metadata sidecars"));

    let _ = fs::remove_dir_all(cache_dir);
}

#[test]
fn query_llm_csv_partial_metadata_failure_emits_a_gap() {
    let cache_dir = temp_dir("query-llm-partial-metadata");
    let fixture = seed_query_fixture(&cache_dir, 8, false);
    let output = run_query_without_auth(&[
        "--cache-dir".to_string(),
        cache_dir.display().to_string(),
        "--output-format".to_string(),
        "llm-csv".to_string(),
        "query".to_string(),
        "--symbol".to_string(),
        "SHFE.au2002".to_string(),
        "--series".to_string(),
        "tick".to_string(),
        "--start".to_string(),
        rfc3339(fixture.start_ns),
        "--end".to_string(),
        rfc3339(fixture.tick_end_ns),
        "--policy".to_string(),
        "cache-only".to_string(),
        "--allow-partial".to_string(),
    ]);

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    let content = String::from_utf8(output.stdout).unwrap();
    assert!(content.starts_with("protocol,tqllm-csv/3\n"));
    assert!(content.contains("gap,1,SHFE.au2002,metadata_unavailable,"));
    assert!(content.contains("document_end,status,partial"));

    let _ = fs::remove_dir_all(cache_dir);
}

#[test]
fn query_llm_csv_budget_fails_closed_when_compression_is_off() {
    let cache_dir = temp_dir("query-llm-compression-off");
    let fixture = seed_query_fixture(&cache_dir, 128, false);
    let output = run_query_without_auth(&[
        "--cache-dir".to_string(),
        cache_dir.display().to_string(),
        "--output-format".to_string(),
        "llm-csv".to_string(),
        "query".to_string(),
        "--symbol".to_string(),
        fixture.logical_symbol,
        "--series".to_string(),
        "tick".to_string(),
        "--start".to_string(),
        rfc3339(fixture.start_ns),
        "--end".to_string(),
        rfc3339(fixture.tick_end_ns),
        "--policy".to_string(),
        "cache-only".to_string(),
        "--data-token-budget".to_string(),
        "900".to_string(),
        "--compression".to_string(),
        "off".to_string(),
    ]);

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("enable compression"));

    let _ = fs::remove_dir_all(cache_dir);
}

#[test]
fn query_rejects_cache_management_kind_and_stock_market() {
    let kind = run(["--kind", "minute", "query"]);
    assert_eq!(kind.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&kind.stderr).contains("query does not use --kind"));

    let market = run(["--market", "stock", "query"]);
    assert_eq!(market.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&market.stderr).contains("only --market futures"));
}

#[test]
fn metadata_refresh_requires_auth_before_advancing_active_sidecar() {
    let cache_dir = temp_dir("metadata-refresh-auth");
    let output = run_without_auth_json([
        "--cache-dir",
        cache_dir.to_str().unwrap(),
        "metadata-refresh",
        "--symbol",
        "SHFE.op2601",
        "--start",
        "2025-09-25T00:00:00+08:00",
        "--end",
        "2025-09-26T00:00:00+08:00",
    ]);

    assert_eq!(output.status.code(), Some(1));
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let _ = v3_result(&json, "metadata-refresh", "error", 1);
    assert_eq!(json["error"]["code"], "data_error");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("TQ_AUTH_USER is required")
    );
    assert!(
        !cache_dir
            .join("backtest-history-metadata-v1/SHFE.op2601/active.json")
            .exists()
    );
    let _ = fs::remove_dir_all(cache_dir);
}

fn run_json<const N: usize>(args: [&str; N]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_tqsdk-cache"))
        .args(["--output-format", "json"])
        .args(args)
        .output()
        .unwrap()
}

fn run<const N: usize>(args: [&str; N]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_tqsdk-cache"))
        .args(args)
        .output()
        .unwrap()
}

fn run_without_auth_json<const N: usize>(args: [&str; N]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_tqsdk-cache"))
        .env_remove("TQ_AUTH_USER")
        .env_remove("TQ_AUTH_PASS")
        .args(["--output-format", "json"])
        .args(args)
        .output()
        .unwrap()
}

fn run_query_without_auth(args: &[String]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_tqsdk-cache"))
        .env_remove("TQ_AUTH_USER")
        .env_remove("TQ_AUTH_PASS")
        .args(args)
        .output()
        .unwrap()
}

struct QueryFixture {
    logical_symbol: String,
    start_ns: i64,
    tick_end_ns: i64,
    kline_end_ns: i64,
}

fn seed_query_fixture(
    cache_dir: &std::path::Path,
    tick_rows: usize,
    include_minutes: bool,
) -> QueryFixture {
    let logical_symbol = "KQ.m@SHFE.au";
    let physical_symbol = "SHFE.au2002";
    let range = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    let start_ns = range.start_ns + 60 * 60 * 1_000_000_000;
    let tick_end_ns = start_ns + i64::try_from(tick_rows + 1).unwrap() * 1_000_000_000;
    let kline_end_ns = start_ns + 3 * 60 * 1_000_000_000;
    let snapshot = BacktestHistoryMetadataCache::open(cache_dir)
        .unwrap()
        .store_snapshot(BacktestHistoryMetadataSnapshot {
            schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
            market_kind: BacktestHistoryMarketKind::Futures,
            logical_symbol: logical_symbol.to_string(),
            captured_at_ns: start_ns,
            trading_days: vec![BacktestHistoryTradingDay {
                date: day(2020, 1, 2).to_string(),
                is_trading_day: true,
                start_ns: range.start_ns,
                end_ns: range.end_ns,
            }],
            session: KlineSessionTemplate::cst_trading_day(),
            physical_segments: vec![BacktestHistoryPhysicalSegment {
                physical_symbol: physical_symbol.to_string(),
                start_ns: range.start_ns,
                end_ns: range.end_ns,
            }],
            snapshot_hash: String::new(),
        })
        .unwrap();
    let ticks = (0..tick_rows)
        .map(|offset| Tick {
            id: i64::try_from(offset + 1).unwrap(),
            datetime: start_ns + i64::try_from(offset).unwrap() * 1_000_000_000,
            last_price: 100.0 + offset as f64 * 0.1,
            ask_price1: 100.1 + offset as f64 * 0.1,
            ask_volume1: 10 + i64::try_from(offset).unwrap(),
            bid_price1: 99.9 + offset as f64 * 0.1,
            bid_volume1: 9 + i64::try_from(offset).unwrap(),
            volume: 100 + i64::try_from(offset).unwrap(),
            amount: 10_000.0 + offset as f64,
            open_interest: 1_000 + i64::try_from(offset).unwrap(),
            ..Tick::default()
        })
        .collect::<Vec<_>>();
    BacktestTickCache::open(cache_dir)
        .unwrap()
        .store_ticks(physical_symbol, range.start_ns, range.end_ns, ticks)
        .unwrap();
    if include_minutes {
        let minute_snapshot = MinuteKlineCacheSnapshot::new(
            snapshot.schema_version,
            snapshot.snapshot_hash.clone(),
            snapshot.session.snapshot_hash(),
        )
        .unwrap();
        let rows = (0..3)
            .map(|offset| Kline {
                id: i64::from(offset + 1),
                datetime: start_ns + i64::from(offset) * 60 * 1_000_000_000,
                open: 100.0 + f64::from(offset),
                high: 101.0 + f64::from(offset),
                low: 99.0 + f64::from(offset),
                close: 100.5 + f64::from(offset),
                volume: 10 + i64::from(offset),
                open_oi: 100 + i64::from(offset),
                close_oi: 101 + i64::from(offset),
                ..Kline::default()
            })
            .collect::<Vec<_>>();
        MinuteKlineCache::open(cache_dir)
            .unwrap()
            .store_final_range(
                logical_symbol,
                range.start_ns,
                range.end_ns,
                &minute_snapshot,
                &rows,
            )
            .unwrap();
    }
    QueryFixture {
        logical_symbol: logical_symbol.to_string(),
        start_ns,
        tick_end_ns,
        kline_end_ns,
    }
}

fn rfc3339(timestamp_ns: i64) -> String {
    let seconds = timestamp_ns.div_euclid(1_000_000_000);
    let nanos = u32::try_from(timestamp_ns.rem_euclid(1_000_000_000)).unwrap();
    Utc.timestamp_opt(seconds, nanos)
        .single()
        .unwrap()
        .to_rfc3339()
}

fn day(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, day).unwrap()
}

fn current_open_day() -> NaiveDate {
    backtest_tick_trading_day_for_timestamp_ns(current_time_ns()).unwrap()
}

fn current_time_ns() -> i64 {
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    i64::try_from(now_ns).unwrap()
}

fn store_metadata_backed_minute_coverage(
    cache_dir: &std::path::Path,
    symbol: &str,
    day: NaiveDate,
) -> BacktestHistoryMetadataSnapshot {
    let range = backtest_tick_trading_day_range(day).unwrap();
    let stored = BacktestHistoryMetadataCache::open(cache_dir)
        .unwrap()
        .store_snapshot(BacktestHistoryMetadataSnapshot {
            schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
            market_kind: BacktestHistoryMarketKind::Futures,
            logical_symbol: symbol.to_string(),
            captured_at_ns: range.end_ns,
            trading_days: vec![BacktestHistoryTradingDay {
                date: day.to_string(),
                is_trading_day: true,
                start_ns: range.start_ns,
                end_ns: range.end_ns,
            }],
            session: KlineSessionTemplate::cst_trading_day(),
            physical_segments: vec![BacktestHistoryPhysicalSegment {
                physical_symbol: symbol.to_string(),
                start_ns: range.start_ns,
                end_ns: range.end_ns,
            }],
            snapshot_hash: String::new(),
        })
        .unwrap();
    let snapshot = MinuteKlineCacheSnapshot::new(
        stored.schema_version,
        stored.snapshot_hash.clone(),
        stored.session.snapshot_hash(),
    )
    .unwrap();
    MinuteKlineCache::open(cache_dir)
        .unwrap()
        .store_final_range(symbol, range.start_ns, range.end_ns, &snapshot, &[])
        .unwrap();
    stored
}

fn stale_minute_partition(cache_dir: &std::path::Path, symbol: &str) -> std::path::PathBuf {
    let trading_day = day(2020, 1, 2);
    let range = backtest_tick_trading_day_range(trading_day).unwrap();
    store_metadata_backed_minute_coverage(cache_dir, symbol, trading_day);
    let month_path = MinuteKlineCache::open(cache_dir)
        .unwrap()
        .month_file_path(symbol, "202001");

    BacktestHistoryMetadataCache::open(cache_dir)
        .unwrap()
        .store_snapshot(BacktestHistoryMetadataSnapshot {
            schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
            market_kind: BacktestHistoryMarketKind::Futures,
            logical_symbol: symbol.to_string(),
            captured_at_ns: range.end_ns.saturating_add(1),
            trading_days: vec![BacktestHistoryTradingDay {
                date: trading_day.to_string(),
                is_trading_day: true,
                start_ns: range.start_ns,
                end_ns: range.end_ns,
            }],
            session: KlineSessionTemplate::new("changed-session", Vec::new()).unwrap(),
            physical_segments: vec![BacktestHistoryPhysicalSegment {
                physical_symbol: symbol.to_string(),
                start_ns: range.start_ns,
                end_ns: range.end_ns,
            }],
            snapshot_hash: String::new(),
        })
        .unwrap();

    assert!(month_path.exists());
    month_path
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "tqsdk-cache-cli-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}
