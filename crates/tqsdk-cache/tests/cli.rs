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
    assert_eq!(json["report"]["complete"], true);
    assert_eq!(json["report"]["remote_used"], false);
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
