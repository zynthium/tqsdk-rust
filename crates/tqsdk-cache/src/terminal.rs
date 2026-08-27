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
        "repair-locks" => write_repair_locks(&mut output, value)?,
        "migrate" => write_migrate(&mut output, value)?,
        "metadata-refresh" => write_metadata_refresh(&mut output, value)?,
        "purge" => write_purge(&mut output, value)?,
        "query" => write_query(&mut output, value)?,
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
    if string(value, "cache_kind") == Some("all") {
        for (label, inventory) in [("Tick", value.get("tick")), ("Minute", value.get("minute"))] {
            let Some(inventory) = inventory else {
                continue;
            };
            writeln!(
                output,
                "{label}: {} files | {} | {} problems",
                number(inventory, "total_files"),
                format_bytes(number(inventory, "total_bytes")),
                number(inventory, "problem_files"),
            )?;
        }
        return Ok(());
    }
    let minute = string(value, "cache_kind") == Some("minute");
    if minute {
        writeln!(
            output,
            "Files: {} | Size: {}",
            number(value, "total_files"),
            format_bytes(number(value, "total_bytes")),
        )?;
    } else {
        writeln!(
            output,
            "Files: {} | Days: {} | Size: {} | Problems: {}",
            number(value, "total_files"),
            number(value, "total_days"),
            format_bytes(number(value, "total_bytes")),
            number(value, "problem_files"),
        )?;
    }
    write_inventory_symbols(output, value.get("symbols"), minute)
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
            if string(value, "cache_kind") == Some("minute") {
                writeln!(
                    output,
                    "Partial cache: {} files | {}",
                    number(inventory, "total_files"),
                    format_bytes(number(inventory, "total_bytes")),
                )?;
            } else {
                writeln!(
                    output,
                    "Partial cache: {} files | {} days | {}",
                    number(inventory, "total_files"),
                    number(inventory, "total_days"),
                    format_bytes(number(inventory, "total_bytes")),
                )?;
            }
        }
        return Ok(());
    }

    if string(value, "cache_kind") == Some("daily") {
        writeln!(
            output,
            "Mode: {}",
            if boolean(value, "dry_run") {
                "dry run"
            } else {
                "fill"
            }
        )?;
        write_requested_days(output, value.get("requested_days"))?;
        writeln!(
            output,
            "Coverage: {} | Remote: {} | Rows written: {}",
            if boolean(value, "complete") {
                "complete"
            } else {
                "incomplete"
            },
            if boolean(value, "remote_used") {
                "used"
            } else {
                "not used"
            },
            number(value, "rows_written"),
        )?;
        if let Some(path) = string(value, "report_path") {
            writeln!(output, "Report: {path}")?;
        }
        return write_coverage_statuses(output, value.get("symbols"));
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
        let source = string(calendar, "source").unwrap_or("-");
        let source = match source {
            "local" => "local holidays",
            "remote" => "remote holidays",
            other => other,
        };
        let persistence = if calendar
            .get("persisted")
            .and_then(Value::as_bool)
            .is_some_and(|persisted| !persisted)
        {
            ", not persisted"
        } else {
            ""
        };
        let years = calendar
            .get("snapshot")
            .and_then(|snapshot| {
                Some((
                    snapshot.get("supported_year_start")?.as_i64()?,
                    snapshot.get("supported_year_end")?.as_i64()?,
                ))
            })
            .map(|(start, end)| format!(", years {start}\u{2013}{end}"))
            .unwrap_or_default();
        let candidate_hash = if persistence.is_empty() {
            String::new()
        } else {
            calendar
                .get("snapshot")
                .and_then(|snapshot| snapshot.get("content_hash"))
                .and_then(Value::as_str)
                .map(|hash| format!(", candidate {hash}"))
                .unwrap_or_default()
        };
        writeln!(
            output,
            "Calendar: {} ({}{}{}{})",
            string(calendar, "mode").unwrap_or("-"),
            source,
            persistence,
            years,
            candidate_hash,
        )?;
    }
    if let Some(path) = string(value, "report_path") {
        writeln!(output, "Report: {path}")?;
    }

    let minute = string(value, "cache_kind") == Some("minute");
    let symbols = report
        .get(if minute {
            "symbols"
        } else {
            "physical_symbols"
        })
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    if symbols.is_empty() {
        writeln!(
            output,
            "{}: none",
            if minute {
                "Minute symbols"
            } else {
                "Physical symbols"
            }
        )?;
        return Ok(());
    }
    writeln!(
        output,
        "{} ({})",
        if minute {
            "Minute symbols"
        } else {
            "Physical symbols"
        },
        symbols.len()
    )?;
    for symbol in symbols {
        let action = string(symbol, "action").unwrap_or("-");
        let (action, coverage, row_label) = if minute {
            let coverage = if symbol
                .get("after")
                .is_some_and(|after| boolean(after, "complete"))
            {
                "final coverage complete".to_string()
            } else {
                "coverage incomplete".to_string()
            };
            let action = match action {
                "skipped_complete" => "already cached",
                "filled_remote" => "downloaded",
                "refreshed_remote" => "refreshed",
                "missing_cache_only" => "cache missing",
                other => other,
            };
            (action, coverage, "rows downloaded")
        } else {
            let coverage = symbol.get("day_stats").map_or_else(
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
            (action, coverage, "rows")
        };
        writeln!(
            output,
            "  {}: {} | {} | {} {}",
            string(symbol, "symbol").unwrap_or("-"),
            action,
            coverage,
            number(symbol, "rows_written"),
            row_label,
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
    if string(value, "cache_kind") == Some("all") {
        for (label, report) in [("Tick", value.get("tick")), ("Minute", value.get("minute"))] {
            let Some(report) = report else {
                continue;
            };
            writeln!(
                output,
                "{label} problem files: {}",
                number(report, "problem_files")
            )?;
        }
        return Ok(());
    }
    let problem_files = number(value, "problem_files");
    writeln!(output, "Problem files: {problem_files}")?;
    if problem_files == 0 {
        writeln!(output, "All checked cache files are readable.")?;
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

fn write_repair_locks(output: &mut impl Write, value: &Value) -> io::Result<()> {
    write_cache_header(output, value)?;
    writeln!(
        output,
        "Files: {} scanned | Missing: {} | Created: {} | Already present: {} | Failed: {}",
        number(value, "scanned_files"),
        number(value, "missing_files"),
        number(value, "created_files"),
        number(value, "already_present_files"),
        number(value, "failed_files"),
    )?;
    writeln!(
        output,
        "Legacy partitions: {} scanned | Missing: {} | Created: {} | Already present: {} | Failed: {}",
        number(value, "legacy_partition_locks_scanned"),
        number(value, "legacy_partition_locks_missing"),
        number(value, "legacy_partition_locks_created"),
        number(value, "legacy_partition_locks_already_present"),
        number(value, "legacy_partition_locks_failed"),
    )?;
    if boolean(value, "dry_run") {
        writeln!(
            output,
            "Mode: dry run; pass --apply to create missing companion locks."
        )?;
    } else {
        writeln!(
            output,
            "Mode: applied; only missing legacy partition and per-file companion locks were created."
        )?;
    }
    for lock in array(value, "legacy_partition_locks") {
        let status = string(lock, "status").unwrap_or("unknown");
        if status == "already_present" {
            continue;
        }
        writeln!(
            output,
            "  {}: {status} -> {}{}",
            string(lock, "partition_dir").unwrap_or("-"),
            string(lock, "lock_path").unwrap_or("-"),
            string(lock, "error")
                .map(|error| format!(" ({error})"))
                .unwrap_or_default(),
        )?;
    }
    for file in array(value, "files") {
        let status = string(file, "status").unwrap_or("unknown");
        if status == "already_present" {
            continue;
        }
        writeln!(
            output,
            "  {}: {status} -> {}{}",
            string(file, "path").unwrap_or("-"),
            string(file, "lock_path").unwrap_or("-"),
            string(file, "error")
                .map(|error| format!(" ({error})"))
                .unwrap_or_default(),
        )?;
    }
    Ok(())
}

fn write_migrate(output: &mut impl Write, value: &Value) -> io::Result<()> {
    write_cache_header(output, value)?;
    writeln!(
        output,
        "Target: {} schema {}",
        string(value, "target_format").unwrap_or("-"),
        number(value, "target_schema_version"),
    )?;
    writeln!(
        output,
        "Legacy: {} files | {} symbols | {}",
        number(value, "legacy_files"),
        array(value, "legacy_symbols").len(),
        format_bytes(number(value, "source_bytes")),
    )?;
    if boolean(value, "dry_run") {
        writeln!(
            output,
            "Mode: dry run; pass --apply --backup-dir DIR to migrate."
        )?;
        return Ok(());
    }
    if boolean(value, "completed") {
        writeln!(output, "Mode: applied; deep validation passed.")?;
    } else {
        writeln!(
            output,
            "Mode: not applied; no eligible legacy files or preflight failed."
        )?;
    }
    if let Some(backup_dir) = string(value, "backup_dir") {
        writeln!(
            output,
            "Backup: {backup_dir} | {} data links | {} lock copies",
            number(value, "backup_data_files"),
            number(value, "backup_lock_files"),
        )?;
    }
    if let Some(rewritten_bytes) = value.get("rewritten_bytes").and_then(Value::as_u64) {
        writeln!(output, "Rewritten size: {}", format_bytes(rewritten_bytes))?;
    }
    Ok(())
}

fn write_metadata_refresh(output: &mut impl Write, value: &Value) -> io::Result<()> {
    write_cache_header(output, value)?;
    writeln!(output, "Symbol: {}", string(value, "symbol").unwrap_or("-"))?;
    if let Some(range) = value.get("requested_range") {
        writeln!(
            output,
            "Range: {} to {}",
            string(range, "start").unwrap_or("-"),
            string(range, "end").unwrap_or("-"),
        )?;
    }
    if let Some(snapshot) = value.get("snapshot") {
        writeln!(
            output,
            "Snapshot: {} | trading days {} | physical segments {}",
            string(snapshot, "snapshot_hash").unwrap_or("-"),
            number(snapshot, "trading_days"),
            number(snapshot, "physical_segments"),
        )?;
    }
    Ok(())
}

fn write_purge(output: &mut impl Write, value: &Value) -> io::Result<()> {
    write_cache_header(output, value)?;
    write_requested_days(output, value.get("requested_days"))?;
    let target = if string(value, "cache_kind") == Some("daily") {
        "symbol file(s)"
    } else {
        "monthly file(s)"
    };
    if boolean(value, "dry_run") {
        writeln!(
            output,
            "Dry run: {} {target} would be removed.",
            array(value, "would_remove_files").len(),
        )?;
    } else {
        writeln!(
            output,
            "Removed: {} {target}, {}.",
            number(value, "removed_files"),
            format_bytes(number(value, "removed_bytes"))
        )?;
    }
    Ok(())
}

fn write_query(output: &mut impl Write, value: &Value) -> io::Result<()> {
    if string(value, "subcommand") == Some("schema") {
        writeln!(output, "Series: {}", string(value, "series").unwrap_or("-"))?;
        let defaults = array(value, "default_fields")
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(",");
        writeln!(output, "Default fields: {defaults}")?;
        writeln!(output, "Fields ({})", array(value, "fields").len())?;
        for field in array(value, "fields") {
            let aliases = array(field, "aliases")
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join("|");
            writeln!(
                output,
                "  {}: {} ({})",
                string(field, "name").unwrap_or("-"),
                string(field, "value_kind").unwrap_or("-"),
                aliases,
            )?;
        }
        return Ok(());
    }
    if let Some(cache_dir) = string(value, "cache_dir") {
        writeln!(output, "Cache: {cache_dir}")?;
    }
    writeln!(
        output,
        "Query: {}",
        string(value, "query_id").unwrap_or("-")
    )?;
    writeln!(output, "Policy: {}", string(value, "policy").unwrap_or("-"))?;
    writeln!(
        output,
        "Blocks: {} | Partial: {}",
        array(value, "blocks").len(),
        if boolean(value, "partial") {
            "yes"
        } else {
            "no"
        },
    )?;
    for block in array(value, "blocks") {
        writeln!(
            output,
            "  {} {} {}: {} rows | {}",
            string(block, "block_id").unwrap_or("-"),
            string(block, "symbol").unwrap_or("-"),
            string(block, "series").unwrap_or("-"),
            number(block, "rows"),
            string(block, "source").unwrap_or("-"),
        )?;
    }
    for failure in array(value, "failures") {
        writeln!(
            output,
            "  gap {}: {}",
            string(failure, "symbol").unwrap_or("-"),
            string(failure, "code").unwrap_or("unknown"),
        )?;
    }
    writeln!(
        output,
        "Raw rows: --output-format jsonl | LLM context: --output-format llm-csv"
    )
}

fn write_cache_header(output: &mut impl Write, value: &Value) -> io::Result<()> {
    if let Some(cache_dir) = string(value, "cache_dir") {
        writeln!(output, "Cache: {cache_dir}")?;
    }
    if let Some(backend_format) = string(value, "backend_format") {
        writeln!(output, "Format: {backend_format}")?;
    }
    if let Some(cache_kind) = string(value, "cache_kind") {
        writeln!(output, "Kind: {cache_kind}")?;
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

fn write_inventory_symbols(
    output: &mut impl Write,
    value: Option<&Value>,
    minute: bool,
) -> io::Result<()> {
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
        if minute {
            writeln!(
                output,
                "  {}: {} files | {} months | {}",
                string(symbol, "symbol").unwrap_or("-"),
                number(symbol, "files"),
                number(symbol, "months"),
                format_bytes(number(symbol, "bytes")),
            )?;
        } else {
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
            "backend_format": "tqsdk.tqbn.daily.v3",
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
    fn minute_inventory_reports_months_without_fabricating_day_counts() {
        let value = json!({
            "command": "inventory",
            "cache_kind": "minute",
            "cache_dir": "/tmp/cache",
            "backend_format": "tqsdk.minute-kline.monthly.v4",
            "total_files": 2,
            "total_bytes": 1_536,
            "total_days": null,
            "problem_files": 0,
            "symbols": [{
                "symbol": "KQ.i@SHFE.au",
                "files": 2,
                "months": 2,
                "bytes": 1_536,
            }],
        });
        let mut output = Vec::new();

        write_result(&mut output, &value, "success", 0, 12).unwrap();

        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("Files: 2 | Size: 1.50 KiB"));
        assert!(output.contains("2 months"));
        assert!(!output.contains("0 days"));
    }

    #[test]
    fn minute_fill_summary_marks_reused_coverage_as_already_cached() {
        let value = json!({
            "command": "fill",
            "cache_kind": "minute",
            "cache_dir": "/tmp/cache",
            "dry_run": false,
            "report": {
                "requested_days": { "start_day": "2026-07-20", "end_day": "2026-07-21" },
                "complete": true,
                "remote_used": false,
                "rows_written": 0,
                "symbols": [{
                    "symbol": "KQ.i@CFFEX.IC",
                    "action": "skipped_complete",
                    "after": { "complete": true },
                    "rows_written": 0,
                }],
            },
        });
        let mut output = Vec::new();

        write_result(&mut output, &value, "success", 0, 12).unwrap();

        let output = String::from_utf8(output).unwrap();
        assert!(output.contains(
            "KQ.i@CFFEX.IC: already cached | final coverage complete | 0 rows downloaded"
        ));
        assert!(!output.contains("day coverage unavailable"));
    }

    #[test]
    fn fill_summary_describes_local_holiday_calendar_and_dry_run_candidate() {
        let value = json!({
            "command": "fill",
            "cache_dir": "/tmp/cache",
            "dry_run": true,
            "report": {
                "requested_days": { "start_day": "2026-07-20", "end_day": "2026-07-21" },
                "complete": true,
                "remote_used": false,
                "rows_written": 0,
                "calendar": {
                    "mode": "auto",
                    "source": "remote",
                    "persisted": false,
                    "snapshot": {
                        "supported_year_start": 2003,
                        "supported_year_end": 2026,
                        "content_hash": "0123456789012345678901234567890123456789"
                    }
                },
                "physical_symbols": [],
            },
        });
        let mut output = Vec::new();

        write_result(&mut output, &value, "success", 0, 12).unwrap();

        let output = String::from_utf8(output).unwrap();
        assert!(output.contains(
            "Calendar: auto (remote holidays, not persisted, years 2003–2026, candidate 0123456789012345678901234567890123456789)"
        ));
    }

    #[test]
    fn byte_formatting_uses_binary_units() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1_024), "1.00 KiB");
        assert_eq!(format_bytes(1_048_576), "1.00 MiB");
    }

    #[test]
    fn query_summary_points_to_raw_formats() {
        let value = json!({
            "command": "query",
            "cache_dir": "/tmp/cache",
            "query_id": "query-123",
            "policy": "cache-only",
            "partial": false,
            "blocks": [{
                "block_id": "b1",
                "symbol": "KQ.m@SHFE.au",
                "series": "tick",
                "rows": 8,
                "source": "cache",
            }],
            "failures": [],
        });
        let mut output = Vec::new();

        write_result(&mut output, &value, "success", 0, 12).unwrap();

        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("Query: query-123"));
        assert!(output.contains("b1 KQ.m@SHFE.au tick: 8 rows | cache"));
        assert!(
            output
                .contains("Raw rows: --output-format jsonl | LLM context: --output-format llm-csv")
        );
    }

    #[test]
    fn repair_locks_summary_explains_dry_run_without_json() {
        let value = json!({
            "command": "repair-locks",
            "cache_dir": "/tmp/cache",
            "dry_run": true,
            "scanned_files": 4,
            "missing_files": 2,
            "created_files": 0,
            "already_present_files": 2,
            "failed_files": 0,
            "legacy_partition_locks_scanned": 1,
            "legacy_partition_locks_missing": 1,
            "legacy_partition_locks_created": 0,
            "legacy_partition_locks_already_present": 0,
            "legacy_partition_locks_failed": 0,
            "legacy_partition_locks": [{
                "partition_dir": "/tmp/cache/series/20260728/tick",
                "lock_path": "/tmp/cache/series/20260728/tick/.tqbn.lock",
                "status": "missing",
                "error": null,
            }],
            "files": [{
                "path": "/tmp/cache/SHFE.au2608.tqbn",
                "lock_path": "/tmp/cache/SHFE.au2608.tqbn.lock",
                "status": "missing",
                "error": null,
            }],
        });
        let mut output = Vec::new();

        write_result(&mut output, &value, "success", 0, 12).unwrap();

        let output = String::from_utf8(output).unwrap();
        assert!(output.contains(
            "Files: 4 scanned | Missing: 2 | Created: 0 | Already present: 2 | Failed: 0"
        ));
        assert!(output.contains(
            "Legacy partitions: 1 scanned | Missing: 1 | Created: 0 | Already present: 0 | Failed: 0"
        ));
        assert!(output.contains("Mode: dry run; pass --apply to create missing companion locks."));
        assert!(output.contains(
            "/tmp/cache/series/20260728/tick: missing -> /tmp/cache/series/20260728/tick/.tqbn.lock"
        ));
        assert!(
            output.contains(
                "/tmp/cache/SHFE.au2608.tqbn: missing -> /tmp/cache/SHFE.au2608.tqbn.lock"
            )
        );
        assert!(!output.contains("No terminal summary is available"));
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
                "backend_format": "tqsdk.tqbn.daily.v3",
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

    #[test]
    fn migrate_summary_reports_plan_and_backup() {
        let value = json!({
            "command": "migrate",
            "cache_dir": "/tmp/cache",
            "target_format": "tqsdk.tqbn.daily.v3",
            "target_schema_version": 3,
            "dry_run": false,
            "completed": true,
            "legacy_files": 2,
            "legacy_symbols": ["SHFE.op2701"],
            "source_bytes": 4096,
            "rewritten_bytes": 1024,
            "backup_dir": "/tmp/backup",
            "backup_data_files": 2,
            "backup_lock_files": 3,
        });
        let mut output = Vec::new();

        write_result(&mut output, &value, "success", 0, 12).unwrap();

        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("Mode: applied"));
        assert!(output.contains("Backup: /tmp/backup"));
        assert!(!output.contains("No terminal summary is available"));
    }
}
