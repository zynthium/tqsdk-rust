use std::io::{self, Write};

use serde_json::Value;

pub(crate) fn write_result(
    mut output: impl Write,
    value: &Value,
    status: &str,
    exit_code: i32,
    duration_ms: u64,
) -> io::Result<()> {
    let command = string(value, "command").unwrap_or("operation");
    writeln!(output, "tqsdk-cache {command}: {status}")?;
    if exit_code != 0 {
        writeln!(output, "Exit code: {exit_code}")?;
    }

    match command {
        "inventory" => write_inventory(&mut output, value)?,
        "inspect" => write_inspect(&mut output, value)?,
        "fill" => write_fill(&mut output, value)?,
        "verify" => write_verify(&mut output, value)?,
        "doctor" => write_doctor(&mut output, value)?,
        _ => writeln!(output, "No terminal summary is available for this command.")?,
    }

    writeln!(output, "Elapsed: {}", format_duration(duration_ms))?;
    writeln!(output, "JSON output: --output-format json")?;
    Ok(())
}

pub(crate) fn write_error(
    mut output: impl Write,
    command: &str,
    message: &str,
    exit_code: i32,
    retryable: bool,
) -> io::Result<()> {
    writeln!(output, "tqsdk-cache {command}: error (exit {exit_code})")?;
    writeln!(output, "{message}")?;
    if retryable {
        writeln!(output, "Retry: yes")?;
    }
    writeln!(output, "JSON output: --output-format json")?;
    Ok(())
}

fn write_inventory(output: &mut impl Write, value: &Value) -> io::Result<()> {
    write_cache_header(output, value)?;
    writeln!(
        output,
        "Files: {} | Days: {} | Size: {} | Problems: {}",
        number(value, "total_files"),
        number(value, "total_days"),
        format_bytes(number(value, "total_bytes")),
        number(value, "problem_files"),
    )?;
    write_inventory_symbols(output, value.get("symbols"))
}

fn write_inspect(output: &mut impl Write, value: &Value) -> io::Result<()> {
    write_cache_header(output, value)?;
    write_requested_days(output, value.get("requested_days"))?;
    write_coverage_statuses(output, value.get("statuses"))
}

fn write_fill(output: &mut impl Write, value: &Value) -> io::Result<()> {
    write_cache_header(output, value)?;
    if string(value, "status") == Some("interrupted") {
        writeln!(
            output,
            "Accepted partial rows were flushed; coverage was not committed."
        )?;
        if let Some(inventory) = value.get("partial_inventory") {
            writeln!(
                output,
                "Partial cache: {} files | {} days | {}",
                number(inventory, "total_files"),
                number(inventory, "total_days"),
                format_bytes(number(inventory, "total_bytes")),
            )?;
        }
        return Ok(());
    }

    let Some(report) = value.get("report") else {
        return Ok(());
    };
    writeln!(
        output,
        "Mode: {}",
        if boolean(value, "dry_run") {
            "dry run"
        } else {
            "fill"
        }
    )?;
    write_requested_days(
        output,
        report
            .get("resolved_range")
            .or_else(|| report.get("requested_days")),
    )?;
    writeln!(
        output,
        "Coverage: {} | Remote: {} | Rows written: {}",
        if boolean(report, "complete") {
            "complete"
        } else {
            "incomplete"
        },
        if boolean(report, "remote_used") {
            "used"
        } else {
            "not used"
        },
        number(report, "rows_written"),
    )?;
    if let Some(calendar) = report.get("calendar") {
        writeln!(
            output,
            "Calendar: {} ({})",
            string(calendar, "mode").unwrap_or("-"),
            string(calendar, "source").unwrap_or("-"),
        )?;
    }
    if let Some(path) = string(value, "report_path") {
        writeln!(output, "Report: {path}")?;
    }

    let symbols = report
        .get("physical_symbols")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    if symbols.is_empty() {
        writeln!(output, "Physical symbols: none")?;
        return Ok(());
    }
    writeln!(output, "Physical symbols ({})", symbols.len())?;
    for symbol in symbols {
        let day_stats = symbol.get("day_stats");
        let day_summary = day_stats.map_or_else(
            || "day coverage unavailable".to_string(),
            |stats| {
                format!(
                    "days {}/{} | received {} | missing {}",
                    number(stats, "covered_days"),
                    number(stats, "planned_days"),
                    number(stats, "received_days"),
                    number(stats, "missing_days"),
                )
            },
        );
        writeln!(
            output,
            "  {}: {} | {} | {} rows",
            string(symbol, "symbol").unwrap_or("-"),
            string(symbol, "action").unwrap_or("-"),
            day_summary,
            number(symbol, "rows_written"),
        )?;
    }
    Ok(())
}

fn write_verify(output: &mut impl Write, value: &Value) -> io::Result<()> {
    write_cache_header(output, value)?;
    write_requested_days(output, value.get("requested_days"))?;
    writeln!(
        output,
        "Coverage: {}",
        if boolean(value, "coverage_complete") {
            "complete"
        } else {
            "incomplete"
        }
    )?;
    if let Some(source) = string(value, "source_report") {
        writeln!(output, "Source report: {source}")?;
    }
    if let Some(rows) = value.get("replay_rows").and_then(Value::as_u64) {
        let minimum = value
            .get("min_rows")
            .and_then(Value::as_u64)
            .map(|value| format!(" (minimum {value})"))
            .unwrap_or_default();
        writeln!(output, "Replay rows: {rows}{minimum}")?;
    }
    write_coverage_statuses(output, value.get("statuses"))
}

fn write_doctor(output: &mut impl Write, value: &Value) -> io::Result<()> {
    write_cache_header(output, value)?;
    let problem_files = number(value, "problem_files");
    writeln!(output, "Problem files: {problem_files}")?;
    if problem_files == 0 {
        writeln!(output, "All checked TQBN files are readable.")?;
        return Ok(());
    }
    writeln!(output, "Problems")?;
    for file in array(value, "files") {
        let status = string(file, "status").unwrap_or("unknown");
        if status == "readable" {
            continue;
        }
        writeln!(
            output,
            "  {}: {}{}",
            string(file, "path").unwrap_or("-"),
            status,
            string(file, "error")
                .map(|error| format!(" ({error})"))
                .unwrap_or_default(),
        )?;
    }
    Ok(())
}

fn write_cache_header(output: &mut impl Write, value: &Value) -> io::Result<()> {
    if let Some(cache_dir) = string(value, "cache_dir") {
        writeln!(output, "Cache: {cache_dir}")?;
    }
    if let Some(backend_format) = string(value, "backend_format") {
        writeln!(output, "Format: {backend_format}")?;
    }
    Ok(())
}

fn write_requested_days(output: &mut impl Write, value: Option<&Value>) -> io::Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    let start = string(value, "start_day").unwrap_or("-");
    let end = string(value, "end_day").unwrap_or("-");
    writeln!(output, "Trading days: {start} to {end}")
}

fn write_inventory_symbols(output: &mut impl Write, value: Option<&Value>) -> io::Result<()> {
    let symbols = value
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    if symbols.is_empty() {
        writeln!(output, "Symbols: none")?;
        return Ok(());
    }
    writeln!(output, "Symbols ({})", symbols.len())?;
    for symbol in symbols {
        writeln!(
            output,
            "  {}: {} files | {} days | {} | {} problems",
            string(symbol, "symbol").unwrap_or("-"),
            number(symbol, "files"),
            number(symbol, "days"),
            format_bytes(number(symbol, "bytes")),
            number(symbol, "problem_files"),
        )?;
    }
    Ok(())
}

fn write_coverage_statuses(output: &mut impl Write, value: Option<&Value>) -> io::Result<()> {
    let statuses = value
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    if statuses.is_empty() {
        writeln!(output, "Symbols: none")?;
        return Ok(());
    }
    writeln!(output, "Symbols ({})", statuses.len())?;
    for status in statuses {
        let cached_ranges = status
            .get("cached_ranges")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        let missing_ranges = status
            .get("missing_ranges")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        writeln!(
            output,
            "  {}: {} | cached ranges: {} | missing ranges: {}",
            string(status, "symbol").unwrap_or("-"),
            if boolean(status, "complete") {
                "complete"
            } else {
                "incomplete"
            },
            cached_ranges,
            missing_ranges,
        )?;
    }
    Ok(())
}

fn array<'a>(value: &'a Value, key: &str) -> &'a [Value] {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
}

fn string<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn number(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or_default()
}

fn boolean(value: &Value, key: &str) -> bool {
    value.get(key).and_then(Value::as_bool).unwrap_or(false)
}

fn format_duration(duration_ms: u64) -> String {
    if duration_ms < 1_000 {
        return format!("{duration_ms} ms");
    }
    format!("{:.2} s", duration_ms as f64 / 1_000.0)
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{format_bytes, write_error, write_result};

    #[test]
    fn inventory_summary_uses_human_units_and_avoids_json() {
        let value = json!({
            "command": "inventory",
            "cache_dir": "/tmp/cache",
            "backend_format": "tqsdk.tqbn.daily.v2",
            "total_files": 2,
            "total_bytes": 1_536,
            "total_days": 1,
            "problem_files": 0,
            "symbols": [],
        });
        let mut output = Vec::new();

        write_result(&mut output, &value, "success", 0, 12).unwrap();

        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("tqsdk-cache inventory: success"));
        assert!(output.contains("1.50 KiB"));
        assert!(!output.contains("\"total_files\""));
    }

    #[test]
    fn byte_formatting_uses_binary_units() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1_024), "1.00 KiB");
        assert_eq!(format_bytes(1_048_576), "1.00 MiB");
    }

    #[test]
    fn every_command_has_a_text_summary() {
        let results = [
            json!({
                "command": "inspect",
                "cache_dir": "/tmp/cache",
                "requested_days": { "start_day": "2026-07-20", "end_day": "2026-07-21" },
                "statuses": [{
                    "symbol": "SHFE.au2608",
                    "complete": true,
                    "cached_ranges": [[1, 2]],
                    "missing_ranges": [],
                }],
            }),
            json!({
                "command": "fill",
                "cache_dir": "/tmp/cache",
                "dry_run": false,
                "report_path": "/tmp/cache/reports/fill.json",
                "report": {
                    "resolved_range": { "start_day": "2026-07-20", "end_day": "2026-07-21" },
                    "complete": true,
                    "remote_used": false,
                    "rows_written": 0,
                    "calendar": { "mode": "auto", "source": "local" },
                    "physical_symbols": [{
                        "symbol": "SHFE.au2608",
                        "action": "skipped_complete",
                        "rows_written": 0,
                        "day_stats": {
                            "covered_days": 2,
                            "planned_days": 2,
                            "received_days": 0,
                            "missing_days": 0,
                        },
                    }],
                },
            }),
            json!({
                "command": "verify",
                "cache_dir": "/tmp/cache",
                "requested_days": { "start_day": "2026-07-20", "end_day": "2026-07-21" },
                "coverage_complete": true,
                "replay_rows": 12,
                "min_rows": 1,
                "statuses": [],
            }),
            json!({
                "command": "doctor",
                "cache_dir": "/tmp/cache",
                "backend_format": "tqsdk.tqbn.daily.v2",
                "problem_files": 1,
                "files": [{
                    "path": "/tmp/cache/series/20260721/tick/SHFE.au2608.tqbn",
                    "status": "incomplete_write",
                    "error": "truncated block",
                }],
            }),
        ];

        for value in results {
            let command = value["command"].as_str().unwrap();
            let mut output = Vec::new();
            write_result(&mut output, &value, "success", 0, 12).unwrap();
            let output = String::from_utf8(output).unwrap();
            assert!(output.contains(&format!("tqsdk-cache {command}: success")));
            assert!(!output.contains("\"command\""));
        }

        let mut error = Vec::new();
        write_error(&mut error, "fill", "cache is busy", 75, true).unwrap();
        let error = String::from_utf8(error).unwrap();
        assert!(error.contains("Retry: yes"));
    }
}
