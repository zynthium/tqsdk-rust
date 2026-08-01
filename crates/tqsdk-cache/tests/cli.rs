use std::process::Command;

use chrono::NaiveDate;
use serde_json::Value;
use tqsdk_data::{
    BACKTEST_HISTORY_METADATA_SCHEMA_VERSION, BacktestHistoryMarketKind,
    BacktestHistoryMetadataCache, BacktestHistoryMetadataSnapshot, BacktestHistoryPhysicalSegment,
    BacktestHistoryTradingDay, BacktestTickCache, KlineSessionTemplate, MinuteKlineCache,
    MinuteKlineCacheSnapshot, TradingCalendarRow, backtest_tick_trading_day_for_timestamp_ns,
    backtest_tick_trading_day_range,
};
use tqsdk_cache::{TradingCalendarSnapshot, write_trading_calendar_snapshot};

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
fn minute_doctor_reports_the_v4_month_file_without_touching_tick_cache() {
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
    assert_eq!(result["files"][0]["schema_version"], 4);

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

    let _ = std::fs::remove_dir_all(cache_dir);
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
    let calendar = TradingCalendarSnapshot::from_rows(vec![
        TradingCalendarRow {
            date: "2026-07-17".to_string(),
            trading: true,
        },
        TradingCalendarRow {
            date: "2026-07-18".to_string(),
            trading: false,
        },
        TradingCalendarRow {
            date: "2026-07-19".to_string(),
            trading: false,
        },
        TradingCalendarRow {
            date: "2026-07-20".to_string(),
            trading: true,
        },
    ])
    .unwrap();
    write_trading_calendar_snapshot(&cache_dir, &calendar).unwrap();

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

    let verified = run_without_auth_json(["verify", "--report", report_path.to_str().unwrap()]);
    assert!(verified.status.success());
    let verified_json: Value = serde_json::from_slice(&verified.stdout).unwrap();
    let verified_result = v3_result(&verified_json, "verify", "success", 0);
    assert_eq!(verified_result["source_report"], "bound");
    assert_eq!(verified_result["coverage_complete"], true);

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
fn fill_requires_an_explicit_end_day_without_last_trading_days() {
    let output = run_json([
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
    ]);

    assert_eq!(output.status.code(), Some(2));
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let _ = v3_result(&json, "fill", "error", 2);
    assert_eq!(json["error"]["code"], "usage");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("--end-day")
    );
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
fn default_output_uses_human_stderr_for_runtime_errors() {
    let output = run([
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
    ]);

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("tqsdk-cache fill: error (exit 2)"));
    assert!(stderr.contains("--end-day"));
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
        "--start-day",
        "2020-01-02",
    ]);
    assert_eq!(usage_error.status.code(), Some(2));
    assert!(usage_error.stdout.is_empty());
    assert!(String::from_utf8_lossy(&usage_error.stderr).contains("--end-day"));

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
) {
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
        stored.snapshot_hash,
        stored.session.snapshot_hash(),
    )
    .unwrap();
    MinuteKlineCache::open(cache_dir)
        .unwrap()
        .store_final_range(symbol, range.start_ns, range.end_ns, &snapshot, &[])
        .unwrap();
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
