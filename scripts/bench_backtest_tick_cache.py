#!/usr/bin/env python3
"""Internal benchmark runner for remote backtest tick cache fills.

The runner builds a small Rust harness under target/internal-bench and executes
the same backtest cache warmup/replay flow across batch-size and optional
time-slice variants. Results are written as JSONL under target/bench-results.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


DEFAULT_UNIVERSE = (
    "main:all;index:all;"
    "!CFFEX,CZCE.ZC,CZCE.CY,CZCE.RI,CZCE.RS,CZCE.PM,CZCE.WH,CZCE.JR,"
    "DCE.rr,DCE.lg,DCE.fb,DCE.bb,SHFE.wr"
)
DEFAULT_START_NS = 1_781_182_800_000_000_000
DEFAULT_END_NS = 1_781_182_860_000_000_000

REPO_ROOT = Path(__file__).resolve().parents[1]
HARNESS_TEMPLATE = REPO_ROOT / "scripts" / "internal" / "backtest_tick_cache_bench_main.rs"
HARNESS_DIR = REPO_ROOT / "target" / "internal-bench" / "backtest-tick-cache-harness"
RESULTS_DIR = REPO_ROOT / "target" / "bench-results"


def main() -> int:
    args = parse_args()
    if not HARNESS_TEMPLATE.exists():
        raise SystemExit(f"missing harness template: {HARNESS_TEMPLATE}")

    universe = args.universe or os.environ.get("TQSDK_RELAY_FUTURES_UNIVERSE") or DEFAULT_UNIVERSE
    batch_sizes = parse_int_list(args.batch_sizes, "batch size")
    slice_values = parse_slice_list(args.slice_secs)
    started_at = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    output = args.output or RESULTS_DIR / f"backtest_tick_cache_{started_at}.jsonl"

    prepare_harness(args.profile)
    matrix = [
        (batch_size, slice_secs, repeat_index)
        for repeat_index in range(args.repeat)
        for slice_secs in slice_values
        for batch_size in batch_sizes
    ]

    if args.dry_run:
        print_plan(args, universe, output, matrix)
        return 0

    output.parent.mkdir(parents=True, exist_ok=True)
    if args.prebuild:
        run_prebuild(args.profile)

    failures = 0
    with output.open("a", encoding="utf-8") as out:
        for batch_size, slice_secs, repeat_index in matrix:
            record = run_case(args, universe, batch_size, slice_secs, repeat_index)
            out.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
            out.flush()
            print_case_summary(record)
            if not record["success"]:
                failures += 1
                if args.fail_fast:
                    break

    print(f"BENCH_RESULTS {output}")
    return 1 if failures else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark tqsdk remote backtest tick cache warmup/replay.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--universe", help="Universe expression. Defaults to env or full futures preset.")
    parser.add_argument("--start-ns", type=int, default=DEFAULT_START_NS, help="Backtest start timestamp in ns.")
    parser.add_argument("--end-ns", type=int, default=DEFAULT_END_NS, help="Backtest end timestamp in ns.")
    parser.add_argument("--batch-sizes", default="8,32,128", help="Comma-separated batch sizes.")
    parser.add_argument(
        "--slice-secs",
        default="none",
        help="Comma-separated slice seconds. Use 'none' for the default single-session path.",
    )
    parser.add_argument("--idle-timeout-secs", type=int, default=120, help="Remote idle timeout override.")
    parser.add_argument("--repeat", type=positive_int, default=1, help="Repeat each matrix case.")
    parser.add_argument(
        "--cache-root",
        type=Path,
        default=Path("/private/tmp/tqsdk-backtest-cache-bench"),
        help="Parent directory for per-run fresh caches.",
    )
    parser.add_argument("--output", type=Path, help="JSONL result file.")
    parser.add_argument("--profile", choices=["dev", "release"], default="dev", help="Cargo profile for harness.")
    parser.add_argument("--timeout-secs", type=int, help="Per-case subprocess timeout.")
    parser.add_argument(
        "--min-rows",
        type=int,
        default=0,
        help="Mark a case failed if rows_written is below this value.",
    )
    parser.add_argument(
        "--min-rows-per-symbol",
        type=int,
        default=0,
        help="Mark a case failed if any remote-filled symbol has fewer rows.",
    )
    parser.add_argument(
        "--allow-zero-rows",
        action="store_true",
        help="Allow multi-symbol remote fills to complete with zero rows.",
    )
    parser.add_argument(
        "--remote-symbol-batch-size",
        type=positive_int,
        help="Override the SDK internal remote symbol batch size.",
    )
    parser.add_argument(
        "--remote-symbol-concurrency",
        type=positive_int,
        help="Override the SDK internal remote symbol fill concurrency.",
    )
    parser.add_argument("--skip-replay", action="store_true", help="Skip cache-only replay timing.")
    parser.add_argument("--skip-cache-only", action="store_true", help="Skip cache-only warmup timing.")
    parser.add_argument(
        "--verify-existing-cache",
        action="store_true",
        help="Do not delete the case cache; run cache-only warmup and replay against existing files.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print planned matrix without running cargo.")
    parser.add_argument("--no-prebuild", dest="prebuild", action="store_false", help="Skip cargo build before matrix.")
    parser.add_argument("--fail-fast", action="store_true", help="Stop after the first failed case.")
    parser.set_defaults(prebuild=True)
    return parser.parse_args()


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def parse_int_list(value: str, label: str) -> list[int]:
    values: list[int] = []
    for token in value.split(","):
        token = token.strip()
        if not token:
            continue
        parsed = int(token)
        if parsed <= 0:
            raise argparse.ArgumentTypeError(f"{label} must be positive: {token}")
        values.append(parsed)
    if not values:
        raise argparse.ArgumentTypeError(f"at least one {label} is required")
    return values


def parse_slice_list(value: str) -> list[int | None]:
    values: list[int | None] = []
    for token in value.split(","):
        token = token.strip().lower()
        if not token:
            continue
        if token in {"none", "default", "off"}:
            values.append(None)
            continue
        parsed = int(token)
        if parsed <= 0:
            raise argparse.ArgumentTypeError(f"slice seconds must be positive or 'none': {token}")
        values.append(parsed)
    if not values:
        raise argparse.ArgumentTypeError("at least one slice setting is required")
    return values


def prepare_harness(profile: str) -> None:
    src_dir = HARNESS_DIR / "src"
    src_dir.mkdir(parents=True, exist_ok=True)
    cargo_toml = HARNESS_DIR / "Cargo.toml"
    main_rs = src_dir / "main.rs"
    cargo_toml.write_text(
        "\n".join(
            [
                "[package]",
                'name = "tqsdk-backtest-cache-bench-harness"',
                'version = "0.0.0"',
                'edition = "2024"',
                "publish = false",
                "",
                "[workspace]",
                "",
                "[dependencies]",
                f'tqsdk = {{ path = "{(REPO_ROOT / "crates" / "tqsdk").as_posix()}" }}',
                'tokio = { version = "1", features = ["macros", "rt", "time"] }',
                "",
                "[profile.release]",
                "debug = 1",
                "",
            ]
        ),
        encoding="utf-8",
    )
    main_rs.write_text(HARNESS_TEMPLATE.read_text(encoding="utf-8"), encoding="utf-8")
    if profile == "release":
        return


def print_plan(
    args: argparse.Namespace,
    universe: str,
    output: Path,
    matrix: list[tuple[int, int | None, int]],
) -> None:
    print(f"repo={REPO_ROOT}")
    print(f"harness={HARNESS_DIR}")
    print(f"output={output}")
    print(f"range_ns={args.start_ns}..{args.end_ns}")
    print(f"universe={universe}")
    print(f"remote_symbol_batch_size={args.remote_symbol_batch_size or 'sdk-default'}")
    print(f"remote_symbol_concurrency={args.remote_symbol_concurrency or 'sdk-default'}")
    for batch_size, slice_secs, repeat_index in matrix:
        print(
            "case "
            f"repeat={repeat_index + 1} "
            f"batch_size={batch_size} "
            f"slice_secs={slice_label(slice_secs)} "
            f"cache_dir={case_cache_dir(args, batch_size, slice_secs, repeat_index)}"
        )


def run_prebuild(profile: str) -> None:
    cmd = cargo_cmd("build", profile)
    print(f"BENCH_PREBUILD {' '.join(cmd)}")
    subprocess.run(cmd, cwd=HARNESS_DIR, check=True)


def run_case(
    args: argparse.Namespace,
    universe: str,
    batch_size: int,
    slice_secs: int | None,
    repeat_index: int,
) -> dict[str, Any]:
    cache_dir = case_cache_dir(args, batch_size, slice_secs, repeat_index)
    if cache_dir.exists() and not args.verify_existing_cache:
        shutil.rmtree(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env.update(
        {
            "TQ_BENCH_START_NS": str(args.start_ns),
            "TQ_BENCH_END_NS": str(args.end_ns),
            "TQ_BENCH_BATCH_SIZE": str(batch_size),
            "TQ_BENCH_CACHE_DIR": str(cache_dir),
            "TQ_BENCH_LABEL": case_label(batch_size, slice_secs, repeat_index),
            "TQSDK_REMOTE_FILL_IDLE_TIMEOUT_SECS": str(args.idle_timeout_secs),
        }
    )
    if args.verify_existing_cache:
        env["TQ_BENCH_VERIFY_EXISTING_CACHE"] = "1"
    else:
        env["TQ_BENCH_UNIVERSE"] = universe
    if args.skip_replay:
        env["TQ_BENCH_SKIP_REPLAY"] = "1"
    if args.skip_cache_only:
        env["TQ_BENCH_SKIP_CACHE_ONLY"] = "1"
    if args.allow_zero_rows:
        env["TQSDK_REMOTE_FILL_ALLOW_EMPTY_IDLE"] = "1"
    if args.remote_symbol_batch_size is not None:
        env["TQSDK_REMOTE_FILL_SYMBOL_BATCH_SIZE"] = str(args.remote_symbol_batch_size)
    if args.remote_symbol_concurrency is not None:
        env["TQSDK_REMOTE_FILL_SYMBOL_CONCURRENCY"] = str(args.remote_symbol_concurrency)
    if slice_secs is None:
        env.pop("TQSDK_REMOTE_FILL_SLICE_SECS", None)
    else:
        env["TQSDK_REMOTE_FILL_SLICE_SECS"] = str(slice_secs)

    cmd = cargo_cmd("run", args.profile)
    record: dict[str, Any] = {
        "label": env["TQ_BENCH_LABEL"],
        "batch_size": batch_size,
        "slice_secs": slice_secs,
        "idle_timeout_secs": args.idle_timeout_secs,
        "remote_symbol_batch_size": args.remote_symbol_batch_size,
        "remote_symbol_concurrency": args.remote_symbol_concurrency,
        "verify_existing_cache": args.verify_existing_cache,
        "repeat_index": repeat_index,
        "cache_dir": str(cache_dir),
        "range_start_ns": args.start_ns,
        "range_end_ns": args.end_ns,
        "profile": args.profile,
        "universe": universe,
        "success": False,
        "started_at": dt.datetime.now(dt.UTC).isoformat(),
    }
    started = time.monotonic()
    try:
        completed = subprocess.run(
            cmd,
            cwd=HARNESS_DIR,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=args.timeout_secs,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        record["process_elapsed_s"] = round(time.monotonic() - started, 3)
        record["timeout"] = True
        record["stdout_tail"] = tail_text(error.stdout or "")
        record["stderr_tail"] = tail_text(error.stderr or "")
        return record
    record["process_elapsed_s"] = round(time.monotonic() - started, 3)
    record["returncode"] = completed.returncode
    record.update(parse_harness_result(completed.stdout))
    if completed.returncode == 0 and "rows_written" in record:
        record["success"] = True
    else:
        record["stdout_tail"] = tail_text(completed.stdout)
        record["stderr_tail"] = tail_text(completed.stderr)
    annotate_record(args, record)
    return record


def annotate_record(args: argparse.Namespace, record: dict[str, Any]) -> None:
    warnings: list[str] = []
    verify_existing_cache = bool(record.get("verify_existing_cache"))
    rows_written = int(record.get("rows_written") or 0)
    replay_tick_count = int(record.get("replay_tick_count") or 0)
    effective_rows = replay_tick_count if verify_existing_cache else rows_written
    symbols_total = int(record.get("symbols_total") or 0)
    complete_symbols = int(record.get("complete_symbols") or 0)
    cache_only_missing = int(record.get("cache_only_missing") or 0)
    remote_used = bool(record.get("remote_used"))
    rows_by_symbol = parse_rows_by_symbol(str(record.get("rows_by_symbol") or ""))

    if effective_rows < args.min_rows:
        record["success"] = False
        warnings.append(f"rows_below_min:{effective_rows}<{args.min_rows}")
    if args.min_rows_per_symbol > 0:
        short_symbols = [
            f"{symbol}:{rows}"
            for symbol, rows in rows_by_symbol.items()
            if rows < args.min_rows_per_symbol
        ]
        if short_symbols:
            record["success"] = False
            warnings.append(
                f"rows_per_symbol_below_min:{','.join(short_symbols)}<{args.min_rows_per_symbol}"
            )
    elif remote_used and symbols_total > 1:
        zero_symbols = [symbol for symbol, rows in rows_by_symbol.items() if rows == 0]
        if zero_symbols:
            warnings.append(f"zero_row_remote_symbols:{','.join(zero_symbols)}")
    if not args.allow_zero_rows and remote_used and symbols_total > 1 and rows_written == 0:
        warnings.append("zero_rows_multi_symbol_remote_fill")
    if symbols_total > 0 and complete_symbols != symbols_total:
        record["success"] = False
        warnings.append(f"incomplete_symbols:{complete_symbols}/{symbols_total}")
    if not args.skip_cache_only and cache_only_missing != 0:
        record["success"] = False
        warnings.append(f"cache_only_missing:{cache_only_missing}")
    if not verify_existing_cache and not args.skip_replay and rows_written != replay_tick_count:
        warnings.append(f"rows_replay_mismatch:{rows_written}!={replay_tick_count}")

    record["warning_count"] = len(warnings)
    record["warnings"] = warnings
    record["suspicious"] = bool(warnings)


def parse_rows_by_symbol(value: str) -> dict[str, int]:
    rows: dict[str, int] = {}
    for token in value.split(","):
        token = token.strip()
        if not token or ":" not in token:
            continue
        symbol, raw_rows = token.rsplit(":", 1)
        try:
            rows[symbol] = int(raw_rows)
        except ValueError:
            continue
    return rows


def cargo_cmd(action: str, profile: str) -> list[str]:
    cmd = ["cargo", action, "--manifest-path", str(HARNESS_DIR / "Cargo.toml")]
    if profile == "release":
        cmd.append("--release")
    return cmd


def case_cache_dir(
    args: argparse.Namespace,
    batch_size: int,
    slice_secs: int | None,
    repeat_index: int,
) -> Path:
    return args.cache_root / case_label(batch_size, slice_secs, repeat_index)


def case_label(batch_size: int, slice_secs: int | None, repeat_index: int) -> str:
    return f"batch{batch_size}-slice{slice_label(slice_secs)}-r{repeat_index + 1}"


def slice_label(slice_secs: int | None) -> str:
    return "none" if slice_secs is None else str(slice_secs)


def parse_harness_result(stdout: str) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for line in stdout.splitlines():
        if not line.startswith("BENCH_RESULT\t"):
            continue
        for field in line.split("\t")[1:]:
            key, sep, value = field.partition("=")
            if not sep:
                continue
            parsed[key] = parse_scalar(value)
    return parsed


def parse_scalar(value: str) -> Any:
    if value in {"true", "false"}:
        return value == "true"
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def print_case_summary(record: dict[str, Any]) -> None:
    if record["success"]:
        status = "BENCH_WARN" if record.get("suspicious") else "BENCH_OK"
        print(
            f"{status} "
            f"label={record['label']} "
            f"rows={record.get('rows_written')} "
            f"replay_ticks={record.get('replay_tick_count')} "
            f"replay_updates={record.get('replay_updates')} "
            f"warmup_s={record.get('warmup_elapsed_s')} "
            f"process_s={record.get('process_elapsed_s')} "
            f"warnings={','.join(record.get('warnings', []))}"
        )
    else:
        print(
            "BENCH_FAIL "
            f"label={record['label']} "
            f"returncode={record.get('returncode')} "
            f"process_s={record.get('process_elapsed_s')}",
            file=sys.stderr,
        )


def tail_text(text: str | bytes, limit: int = 4000) -> str:
    if isinstance(text, bytes):
        text = text.decode("utf-8", errors="replace")
    return text[-limit:] if len(text) > limit else text


if __name__ == "__main__":
    raise SystemExit(main())
