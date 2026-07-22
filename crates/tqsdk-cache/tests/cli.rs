use std::process::Command;

use chrono::NaiveDate;
use serde_json::Value;
use tqsdk_data::{BacktestTickCache, backtest_tick_trading_day_range};

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
fn fill_rejects_provisional_open_day_mode() {
    let output = run_json([
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
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    let _ = v3_result(&json, "fill", "error", 2);
    assert_eq!(json["error"]["code"], "usage");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("--include-open-day")
    );
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
            assert_eq!(record["schema_version"], 1);
            assert_eq!(record["kind"], "tqsdk-cache.progress");
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
