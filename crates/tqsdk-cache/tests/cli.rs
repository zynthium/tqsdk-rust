use std::process::Command;

use chrono::NaiveDate;
use serde_json::Value;
use tqsdk_data::{BacktestTickCache, backtest_tick_trading_day_range};

#[test]
fn inventory_is_read_only_for_a_missing_cache_root() {
    let cache_dir = temp_dir("inventory");
    let output = run(["--cache-dir", cache_dir.to_str().unwrap(), "inventory"]);

    assert!(output.status.success());
    assert!(!cache_dir.exists());
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["command"], "inventory");
    assert_eq!(json["total_files"], 0);
}

#[test]
fn fill_dry_run_does_not_create_cache_data_or_start_remote_fill() {
    let cache_dir = temp_dir("dry-run");
    let output = run([
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
    assert_eq!(json["command"], "fill");
    assert_eq!(json["dry_run"], true);
    assert_eq!(json["report"]["complete"], false);
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

    let output = run_without_auth([
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
    assert_eq!(json["schema_version"], 2);
    assert_eq!(json["report"]["complete"], true);
    assert_eq!(json["report"]["remote_used"], false);
    assert_eq!(
        json["report"]["selector"]["symbols"],
        serde_json::json!(["SHFE.rb2601"])
    );
    assert!(json["report"]["resolved_range"].is_object());
    assert_eq!(json["report"]["calendar"]["mode"], "auto");
    assert_eq!(json["report"]["calendar"]["source"], "partition_fallback");
    assert_eq!(
        json["report"]["physical_symbols"][0]["day_stats"]["planned_days"],
        2
    );
    assert_eq!(
        json["report"]["physical_symbols"][0]["day_stats"]["received_days"],
        0
    );
    assert!(report_path.exists());

    let verified = run_without_auth(["verify", "--report", report_path.to_str().unwrap()]);
    assert!(verified.status.success());
    let verified_json: Value = serde_json::from_slice(&verified.stdout).unwrap();
    assert_eq!(verified_json["source_report"], "bound");
    assert_eq!(verified_json["coverage_complete"], true);

    let another_cache_dir = temp_dir("wrong-root");
    let wrong_root = run_without_auth([
        "--cache-dir",
        another_cache_dir.to_str().unwrap(),
        "verify",
        "--report",
        report_path.to_str().unwrap(),
    ]);
    assert_eq!(wrong_root.status.code(), Some(2));

    let _ = std::fs::remove_dir_all(&cache_dir);
    let _ = std::fs::remove_dir_all(&another_cache_dir);
}

#[test]
fn fill_rejects_last_trading_days_when_calendar_is_off() {
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
    assert!(String::from_utf8_lossy(&output.stderr).contains("--last-trading-days"));
}

#[test]
fn fill_requires_an_explicit_end_day_without_last_trading_days() {
    let output = run([
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
    ]);

    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("--end-day"));
}

#[test]
fn fill_progress_off_keeps_stderr_quiet_and_stdout_machine_readable() {
    let cache_dir = temp_dir("progress-off");
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    let start = backtest_tick_trading_day_range(day(2020, 1, 2)).unwrap();
    cache
        .mark_complete("SHFE.rb2601", start.start_ns, start.end_ns, 0, None)
        .unwrap();

    let output = run_without_auth([
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
    assert_eq!(json["command"], "fill");
    assert_eq!(json["report"]["complete"], true);

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

    let output = run_without_auth([
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
    assert_eq!(json["command"], "fill");
    assert_eq!(json["report"]["complete"], true);

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn fill_progress_plain_reports_a_specific_failure_summary() {
    let cache_dir = temp_dir("progress-failure");
    let output = run_without_auth([
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

    let _ = std::fs::remove_dir_all(cache_dir);
}

#[test]
fn fill_rejects_provisional_open_day_mode() {
    let output = run([
        "fill",
        "--symbol",
        "SHFE.rb2601",
        "--start-day",
        "2020-01-02",
        "--end-day",
        "2020-01-03",
        "--include-open-day",
    ]);

    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("--include-open-day"));
}

#[test]
fn fill_reports_cross_process_root_lock_contention() {
    let cache_dir = temp_dir("root-lock");
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    let _lock = cache.try_acquire_remote_fill_lock().unwrap();

    let output = run_without_auth([
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
    assert!(String::from_utf8_lossy(&output.stderr).contains("busy"));

    drop(_lock);
    let _ = std::fs::remove_dir_all(&cache_dir);
}

fn run<const N: usize>(args: [&str; N]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_tqsdk-cache"))
        .args(args)
        .output()
        .unwrap()
}

fn run_without_auth<const N: usize>(args: [&str; N]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_tqsdk-cache"))
        .env_remove("TQ_AUTH_USER")
        .env_remove("TQ_AUTH_PASS")
        .args(args)
        .output()
        .unwrap()
}

fn day(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, day).unwrap()
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
