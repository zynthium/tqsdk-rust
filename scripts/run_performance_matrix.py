#!/usr/bin/env python3
"""Run reproducible local baselines for the four performance paths.

The runner preserves each benchmark's raw output and, where POSIX resource
accounting is available, records command CPU time, peak RSS, and filesystem
I/O block counts in the manifest. It does not invent allocation, lock-wait,
or per-event metrics that the benchmark itself did not measure.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Mapping


FORMAT_ID = "tqsdk.performance-matrix.v3"
LATENCY_TABLE_TERMS = ("p50 ns", "p95 ns", "p99 ns", "p999 ns", "events/s")
KEY_VALUE_METRIC = re.compile(
    r"(?P<key>[A-Za-z_][A-Za-z0-9_/-]*)=(?P<value>-?(?:\d+(?:\.\d*)?|\.\d+))"
)


@dataclass(frozen=True)
class BenchmarkSpec:
    name: str
    command: tuple[str, ...]
    environment: Mapping[str, str]


@dataclass(frozen=True)
class HistoryCorpusCase:
    name: str
    cache_dir: Path
    symbol: str
    start_ns: int
    end_ns: int


@dataclass(frozen=True)
class ResourceCollector:
    source: str


RESOURCE_WRAPPER = r"""
import json
import resource
import subprocess
import sys

report_path = sys.argv[1]
completed = subprocess.run(sys.argv[2:])
usage = resource.getrusage(resource.RUSAGE_CHILDREN)
max_rss_bytes = usage.ru_maxrss * (1024 if sys.platform.startswith("linux") else 1)
payload = {
    "source": "posix-rusage-children",
    "user_cpu_seconds": usage.ru_utime,
    "system_cpu_seconds": usage.ru_stime,
    "cpu_seconds": usage.ru_utime + usage.ru_stime,
    "max_rss_bytes": max_rss_bytes,
    "max_rss_scope": "maximum individual child resident set; not aggregate process-tree RSS",
    "filesystem_input_blocks": usage.ru_inblock,
    "filesystem_output_blocks": usage.ru_oublock,
}
with open(report_path, "w", encoding="utf-8") as report:
    json.dump(payload, report, sort_keys=True)
    report.write("\n")
raise SystemExit(completed.returncode)
"""


def main() -> int:
    args = parse_args()
    try:
        history_corpus = load_history_corpus(args.history_corpus)
    except (OSError, ValueError) as error:
        print(f"invalid history corpus: {error}", file=sys.stderr)
        return 2
    if args.quick and history_corpus:
        print("--history-corpus requires a comparable non-quick run", file=sys.stderr)
        return 2
    output_dir = Path(args.output_dir)
    try:
        output_dir.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        print(f"output directory already exists: {output_dir}", file=sys.stderr)
        return 2

    resource_collector = discover_resource_collector()
    manifest = {
        "format": FORMAT_ID,
        "started_at_utc": datetime.now(UTC).isoformat(),
        "quick": args.quick,
        "history_corpus": [
            {
                "name": case.name,
                "cache_dir": str(case.cache_dir),
                "symbol": case.symbol,
                "start_ns": case.start_ns,
                "end_ns": case.end_ns,
            }
            for case in history_corpus
        ],
        "environment": environment_metadata(),
        "resource_collection": {
            "available": resource_collector is not None,
            "source": resource_collector.source if resource_collector else None,
            "note": (
                    "Command CPU, peak RSS, and filesystem block counts are collected "
                    "from a fresh POSIX child-rusage wrapper."
                if resource_collector
                    else "POSIX resource accounting is unavailable; resource usage is omitted rather than inferred."
            ),
        },
        "benchmarks": [],
    }

    specs = benchmark_specs(args.quick)
    specs.extend(history_corpus_specs(history_corpus))
    try:
        measurement_commands = prebuild_benchmark_commands(specs, output_dir)
    except RuntimeError as error:
        manifest["prebuild_error"] = str(error)
        manifest["finished_at_utc"] = datetime.now(UTC).isoformat()
        manifest["status"] = "failed"
        write_manifest(output_dir, manifest)
        print(f"benchmark prebuild failed: {error}", file=sys.stderr)
        return 1

    status = 0
    for index, spec in enumerate(specs, start=1):
        result = run_benchmark(
            index,
            spec,
            measurement_commands[spec.command],
            output_dir,
            resource_collector,
        )
        manifest["benchmarks"].append(result)
        write_manifest(output_dir, manifest)
        if result["exit_code"] != 0:
            status = result["exit_code"] or 1
            break

    manifest["finished_at_utc"] = datetime.now(UTC).isoformat()
    manifest["status"] = "passed" if status == 0 else "failed"
    write_manifest(output_dir, manifest)
    return status


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run tqsdk-rust four-path local performance baselines."
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="new directory that receives stdout captures and manifest.json",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="use tiny non-comparable samples for a wiring smoke test",
    )
    parser.add_argument(
        "--history-corpus",
        metavar="PATH",
        help=(
            "JSON array or {cases:[...]} of named cache-backed 1d/1m/6m "
            "TQBN reader cases; requires a non-quick run"
        ),
    )
    return parser.parse_args()


def load_history_corpus(path: str | None) -> tuple[HistoryCorpusCase, ...]:
    """Load explicit real-data TQBN reader cases without guessing their scope."""
    if path is None:
        return ()

    source_path = Path(path)
    payload = json.loads(source_path.read_text(encoding="utf-8"))
    cases = payload.get("cases") if isinstance(payload, dict) else payload
    if not isinstance(cases, list) or not cases:
        raise ValueError("corpus must be a non-empty JSON array or {\"cases\": [...]} object")

    parsed: list[HistoryCorpusCase] = []
    names: set[str] = set()
    for index, item in enumerate(cases, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"case {index} must be an object")
        name = item.get("name")
        if not isinstance(name, str) or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", name):
            raise ValueError(f"case {index} name must match [a-z0-9][a-z0-9-]{{0,63}}")
        if name in names:
            raise ValueError(f"duplicate corpus case name: {name}")
        names.add(name)

        cache_dir_value = item.get("cache_dir")
        symbol = item.get("symbol")
        start_ns = item.get("start_ns")
        end_ns = item.get("end_ns")
        if not isinstance(cache_dir_value, str) or not cache_dir_value:
            raise ValueError(f"case {name} cache_dir must be a non-empty string")
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError(f"case {name} symbol must be a non-empty string")
        if (
            isinstance(start_ns, bool)
            or not isinstance(start_ns, int)
            or isinstance(end_ns, bool)
            or not isinstance(end_ns, int)
            or end_ns <= start_ns
        ):
            raise ValueError(f"case {name} requires integer start_ns < end_ns")

        cache_dir = Path(cache_dir_value)
        if not cache_dir.is_absolute():
            cache_dir = source_path.parent / cache_dir
        cache_dir = cache_dir.resolve()
        if not cache_dir.is_dir():
            raise ValueError(f"case {name} cache_dir is not a directory: {cache_dir}")
        parsed.append(
            HistoryCorpusCase(
                name=name,
                cache_dir=cache_dir,
                symbol=symbol.strip(),
                start_ns=start_ns,
                end_ns=end_ns,
            )
        )
    return tuple(parsed)


def history_corpus_specs(cases: tuple[HistoryCorpusCase, ...]) -> list[BenchmarkSpec]:
    command = (
        "cargo",
        "run",
        "-p",
        "tqsdk-data",
        "--release",
        "--example",
        "history_series_cache_microbench",
    )
    return [
        BenchmarkSpec(
            name=f"tqbn-streaming-reader-corpus-{case.name}",
            command=command,
            environment={
                "TQSDK_HISTORY_CACHE_BENCH_INPUT_CACHE_DIR": str(case.cache_dir),
                "TQSDK_HISTORY_CACHE_BENCH_INPUT_SYMBOL": case.symbol,
                "TQSDK_HISTORY_CACHE_BENCH_INPUT_START_NS": str(case.start_ns),
                "TQSDK_HISTORY_CACHE_BENCH_INPUT_END_NS": str(case.end_ns),
                "TQSDK_HISTORY_CACHE_BENCH_STREAM_ONLY": "1",
            },
        )
        for case in cases
    ]


def benchmark_specs(quick: bool) -> list[BenchmarkSpec]:
    if not quick:
        return full_benchmark_specs()

    if quick:
        core_env = {
            "TQSDK_DIFF_BENCH_SINGLE_ITERS": "3",
            "TQSDK_DIFF_BENCH_BATCH_ITERS": "3",
            "TQSDK_DIFF_BENCH_NOOP_ITERS": "3",
            "TQSDK_DIFF_BENCH_READ_ITERS": "3",
            "TQSDK_DIFF_BENCH_TICK_ITERS": "3",
            "TQSDK_DIFF_BENCH_BATCH_SYMBOLS": "10",
            "TQSDK_DIFF_BENCH_LARGE_BATCH_SYMBOLS": "10",
            "TQSDK_DIFF_BENCH_TICK_WINDOW": "2",
        }
        task_env = {
            "TQSDK_TASK_BENCH_ITERS": "3",
            "TQSDK_TASK_BENCH_SYMBOL_COUNTS": "1,10",
        }
        data_env = {
            "TQSDK_HISTORY_CACHE_BENCH_ROWS": "100",
            "TQSDK_HISTORY_CACHE_BENCH_LIVE_WRITER_ROWS": "10",
            "TQSDK_HISTORY_CACHE_BENCH_LIVE_WRITER_BATCH_ROWS": "4",
            "TQSDK_HISTORY_CACHE_BENCH_SCAN_SYMBOLS": "2",
            "TQSDK_HISTORY_CACHE_BENCH_SCAN_ROWS_PER_SYMBOL": "3",
            "TQSDK_HISTORY_CACHE_BENCH_COMPACT_ROWS": "20",
        }
        relay_env = {
            "TQSDK_RELAY_FANOUT_BENCH_ITERS": "3",
            "TQSDK_RELAY_FANOUT_BENCH_CLIENTS": "1,10",
        }
    else:
        core_env = {}
        task_env = {"TQSDK_TASK_BENCH_SYMBOL_COUNTS": "10,100,500,1000"}
        data_env = {}
        relay_env = {"TQSDK_RELAY_FANOUT_BENCH_CLIENTS": "10,100,500,1000"}

    return [
        BenchmarkSpec(
            "runtime-wire-decode-commit",
            ("cargo", "run", "-p", "tqsdk-core", "--release", "--example", "diff_ingest_microbench"),
            core_env,
        ),
        BenchmarkSpec(
            "commit-strategy-risk-submit",
            (
                "cargo",
                "run",
                "-p",
                "tqsdk-task",
                "--release",
                "--example",
                "commit_risk_submit_microbench",
            ),
            task_env,
        ),
        BenchmarkSpec(
            "tqbn-streaming-reader",
            (
                "cargo",
                "run",
                "-p",
                "tqsdk-data",
                "--release",
                "--example",
                "history_series_cache_microbench",
            ),
            data_env,
        ),
        BenchmarkSpec(
            "backtest-tqbn-merge-callback",
            (
                "cargo",
                "run",
                "-p",
                "tqsdk-task",
                "--release",
                "--example",
                "history_backtest_replay_microbench",
            ),
            {
                "TQSDK_BACKTEST_REPLAY_BENCH_SYMBOLS": "1,10",
                "TQSDK_BACKTEST_REPLAY_BENCH_ROWS_PER_SYMBOL": "3",
                "TQSDK_BACKTEST_REPLAY_BENCH_BATCH_SIZE": "2",
            },
        ),
        BenchmarkSpec(
            "relay-quote-fanout",
            (
                "cargo",
                "run",
                "-p",
                "tqsdk-relay",
                "--release",
                "--example",
                "quote_fanout_microbench",
            ),
            relay_env,
        ),
    ]


def full_benchmark_specs() -> list[BenchmarkSpec]:
    scales = ("10", "100", "500", "1000")
    core_command = (
        "cargo",
        "run",
        "-p",
        "tqsdk-core",
        "--release",
        "--example",
        "diff_ingest_microbench",
    )
    data_command = (
        "cargo",
        "run",
        "-p",
        "tqsdk-data",
        "--release",
        "--example",
        "history_series_cache_microbench",
    )
    specs = [
        BenchmarkSpec(
            name=f"runtime-wire-decode-commit-{scale}-symbols",
            command=core_command,
            environment={
                "TQSDK_DIFF_BENCH_BATCH_SYMBOLS": scale,
                "TQSDK_DIFF_BENCH_LARGE_BATCH_SYMBOLS": scale,
            },
        )
        for scale in scales
    ]
    specs.append(
        BenchmarkSpec(
            name="commit-strategy-risk-submit",
            command=(
                "cargo",
                "run",
                "-p",
                "tqsdk-task",
                "--release",
                "--example",
                "commit_risk_submit_microbench",
            ),
            environment={"TQSDK_TASK_BENCH_SYMBOL_COUNTS": ",".join(scales)},
        )
    )
    specs.extend(
        BenchmarkSpec(
            name=f"tqbn-streaming-reader-{scale}-symbols",
            command=data_command,
            environment={"TQSDK_HISTORY_CACHE_BENCH_SCAN_SYMBOLS": scale},
        )
        for scale in scales
    )
    specs.append(
        BenchmarkSpec(
            name="backtest-tqbn-merge-callback",
            command=(
                "cargo",
                "run",
                "-p",
                "tqsdk-task",
                "--release",
                "--example",
                "history_backtest_replay_microbench",
            ),
            environment={
                "TQSDK_BACKTEST_REPLAY_BENCH_SYMBOLS": ",".join(scales),
                "TQSDK_BACKTEST_REPLAY_BENCH_ROWS_PER_SYMBOL": "256",
                "TQSDK_BACKTEST_REPLAY_BENCH_BATCH_SIZE": "256",
            },
        )
    )
    specs.append(
        BenchmarkSpec(
            name="relay-quote-fanout",
            command=(
                "cargo",
                "run",
                "-p",
                "tqsdk-relay",
                "--release",
                "--example",
                "quote_fanout_microbench",
            ),
            environment={"TQSDK_RELAY_FANOUT_BENCH_CLIENTS": ",".join(scales)},
        )
    )
    return specs


def prebuild_benchmark_commands(
    specs: list[BenchmarkSpec], output_dir: Path
) -> dict[tuple[str, ...], tuple[str, ...]]:
    """Build examples once, so Cargo work stays out of measured resources."""
    target_dir = cargo_target_dir()
    commands: dict[tuple[str, ...], tuple[str, ...]] = {}
    built_targets: set[tuple[str, str]] = set()

    for spec in specs:
        package, example = cargo_example_target(spec.command)
        target = (package, example)
        if target not in built_targets:
            build_command = (
                "cargo",
                "build",
                "-p",
                package,
                "--release",
                "--example",
                example,
            )
            completed = subprocess.run(
                build_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
            (output_dir / f"build-{package}-{example}.txt").write_text(
                completed.stdout, encoding="utf-8"
            )
            if completed.returncode != 0:
                raise RuntimeError(
                    f"{' '.join(build_command)} exited with {completed.returncode}"
                )
            built_targets.add(target)

        suffix = ".exe" if os.name == "nt" else ""
        executable = target_dir / "release" / "examples" / f"{example}{suffix}"
        if not executable.is_file():
            raise RuntimeError(f"prebuilt benchmark executable is missing: {executable}")
        commands[spec.command] = (str(executable),)

    return commands


def cargo_example_target(command: tuple[str, ...]) -> tuple[str, str]:
    """Extract the package/example pair from this runner's Cargo command."""
    try:
        package = command[command.index("-p") + 1]
        example = command[command.index("--example") + 1]
    except (ValueError, IndexError) as error:
        raise RuntimeError(f"unsupported benchmark command: {' '.join(command)}") from error
    if command[:2] != ("cargo", "run"):
        raise RuntimeError(f"unsupported benchmark command: {' '.join(command)}")
    return package, example


def cargo_target_dir() -> Path:
    completed = subprocess.run(
        ("cargo", "metadata", "--no-deps", "--format-version=1"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"cargo metadata exited with {completed.returncode}")
    try:
        value = json.loads(completed.stdout)
        target_directory = value["target_directory"]
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise RuntimeError("cargo metadata did not return target_directory") from error
    return Path(target_directory)


def run_benchmark(
    index: int,
    spec: BenchmarkSpec,
    measurement_command: tuple[str, ...],
    output_dir: Path,
    resource_collector: ResourceCollector | None,
) -> dict[str, object]:
    environment = os.environ.copy()
    environment.update(spec.environment)
    output_file = f"{index:02d}-{spec.name}.txt"
    resource_output_file = f"{index:02d}-{spec.name}.resources.json"
    command = measurement_command
    if resource_collector:
        command = (
            sys.executable,
            "-c",
            RESOURCE_WRAPPER,
            str(output_dir / resource_output_file),
            *measurement_command,
        )
    started = time.monotonic()
    completed = subprocess.run(
        command,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    elapsed_seconds = time.monotonic() - started
    (output_dir / output_file).write_text(completed.stdout, encoding="utf-8")
    resource_usage = (
        parse_resource_usage(output_dir / resource_output_file, resource_collector.source)
        if resource_collector
        else None
    )
    reported_metrics = extract_reported_metrics(completed.stdout)
    derived_metrics = derive_resource_metrics(resource_usage, reported_metrics)

    return {
        "name": spec.name,
        "command": list(spec.command),
        "measurement_command": list(measurement_command),
        "environment": dict(sorted(spec.environment.items())),
        "output_file": output_file,
        "resource_output_file": resource_output_file if resource_collector else None,
        "resource_usage": resource_usage,
        "reported_metrics": reported_metrics,
        "derived_metrics": derived_metrics,
        "elapsed_seconds": elapsed_seconds,
        "exit_code": completed.returncode,
    }


def extract_reported_metrics(stdout: str) -> dict[str, object]:
    """Extract only benchmark-reported values; never derive missing metrics."""
    lines = stdout.splitlines()
    latency_rows: list[dict[str, object]] = []
    key_value_lines: list[dict[str, object]] = []

    for line_number, line in enumerate(lines, start=1):
        metrics = {
            match.group("key"): float(match.group("value"))
            for match in KEY_VALUE_METRIC.finditer(line)
        }
        if metrics:
            key_value_lines.append({"line": line_number, "metrics": metrics})

    for header_index, header in enumerate(lines):
        if not all(term in header for term in LATENCY_TABLE_TERMS):
            continue
        for line_number, row in enumerate(lines[header_index + 1 :], start=header_index + 2):
            cells = row.split()
            if len(cells) < 6:
                break
            try:
                p50_ns, p95_ns, p99_ns, p999_ns, events_per_second = map(
                    float, cells[-5:]
                )
            except ValueError:
                break
            label = " ".join(cells[:-5])
            if not label:
                break
            latency_rows.append(
                {
                    "line": line_number,
                    "label": label,
                    "p50_ns": p50_ns,
                    "p95_ns": p95_ns,
                    "p99_ns": p99_ns,
                    "p999_ns": p999_ns,
                    "events_per_second": events_per_second,
                }
            )

    return {
        "latency_rows": latency_rows,
        "key_value_lines": key_value_lines,
    }


def derive_resource_metrics(
    resource_usage: Mapping[str, object] | None,
    reported_metrics: Mapping[str, object],
) -> dict[str, float]:
    """Derive ratios only from a benchmark's explicit `events=` counters."""
    if resource_usage is None:
        return {}
    events = reported_event_total(reported_metrics)
    if events is None or events <= 0:
        return {}

    derived: dict[str, float] = {"reported_events": events}
    cpu_seconds = resource_usage.get("cpu_seconds")
    if isinstance(cpu_seconds, (int, float)):
        derived["cpu_ns_per_reported_event"] = float(cpu_seconds) * 1_000_000_000 / events
    for metric, derived_name in (
        ("filesystem_input_blocks", "filesystem_input_blocks_per_reported_event"),
        ("filesystem_output_blocks", "filesystem_output_blocks_per_reported_event"),
    ):
        blocks = resource_usage.get(metric)
        if isinstance(blocks, (int, float)):
            derived[derived_name] = float(blocks) / events
    return derived


def reported_event_total(reported_metrics: Mapping[str, object]) -> float | None:
    lines = reported_metrics.get("key_value_lines")
    if not isinstance(lines, list):
        return None
    values: list[float] = []
    for line in lines:
        if not isinstance(line, Mapping):
            continue
        metrics = line.get("metrics")
        if not isinstance(metrics, Mapping):
            continue
        events = metrics.get("events")
        if isinstance(events, (int, float)):
            values.append(float(events))
    return sum(values) if values else None


def discover_resource_collector() -> ResourceCollector | None:
    """Use a fresh POSIX child-rusage wrapper when the platform supports it."""

    if os.name != "posix":
        return None
    try:
        import resource  # noqa: F401
    except ImportError:
        return None
    return ResourceCollector("posix-rusage-children")


def _parse_gnu_time(path: Path, source: str) -> dict[str, object] | None:
    """Parse only documented GNU time fields and retain their explicit units."""

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    values = {
        key.strip(): value.strip()
        for line in lines
        if ":" in line
        for key, value in (line.split(":", maxsplit=1),)
    }
    user_cpu_seconds = parse_float(values.get("User time (seconds)"))
    system_cpu_seconds = parse_float(values.get("System time (seconds)"))
    max_rss_kib = parse_int(values.get("Maximum resident set size (kbytes)"))
    filesystem_input_blocks = parse_int(values.get("File system inputs"))
    filesystem_output_blocks = parse_int(values.get("File system outputs"))
    cpu_seconds = None
    if user_cpu_seconds is not None and system_cpu_seconds is not None:
        cpu_seconds = user_cpu_seconds + system_cpu_seconds
    return {
        "source": source,
        "user_cpu_seconds": user_cpu_seconds,
        "system_cpu_seconds": system_cpu_seconds,
        "cpu_seconds": cpu_seconds,
        "max_rss_bytes": max_rss_kib * 1024 if max_rss_kib is not None else None,
        "filesystem_input_blocks": filesystem_input_blocks,
        "filesystem_output_blocks": filesystem_output_blocks,
    }


def parse_float(value: str | None) -> float | None:
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None


def parse_int(value: str | None) -> int | None:
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None


def parse_resource_usage(path: Path, source: str) -> dict[str, object] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict):
        return None
    value["source"] = source
    return value


def environment_metadata() -> dict[str, str | None]:
    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "rustc": command_output(("rustc", "-V")),
        "cargo": command_output(("cargo", "-V")),
        "git_head": command_output(("git", "rev-parse", "HEAD")),
        "git_status_porcelain": command_output(("git", "status", "--porcelain")),
    }


def command_output(command: tuple[str, ...]) -> str | None:
    try:
        completed = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return completed.stdout.strip()


def write_manifest(output_dir: Path, manifest: Mapping[str, object]) -> None:
    path = output_dir / "manifest.json"
    temporary = output_dir / "manifest.json.tmp"
    temporary.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


if __name__ == "__main__":
    raise SystemExit(main())
