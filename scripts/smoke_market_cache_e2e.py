#!/usr/bin/env python3
"""Run an internal end-to-end smoke for shared market cache policy.

The smoke creates a temporary Cargo harness under target/internal-bench and
checks the same cache can be used for remote warmup, cache-only warmup, and
cache-only replay. Optional live recording is available behind --live-seconds.
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


DEFAULT_SYMBOLS = "KQ.i@SHFE.au"
DEFAULT_START_NS = 1_781_182_800_000_000_000
DEFAULT_END_NS = 1_781_182_860_000_000_000

REPO_ROOT = Path(__file__).resolve().parents[1]
HARNESS_TEMPLATE = REPO_ROOT / "scripts" / "internal" / "market_cache_e2e_main.rs"
HARNESS_DIR = REPO_ROOT / "target" / "internal-bench" / "market-cache-e2e-harness"


def main() -> int:
    args = parse_args()
    if not HARNESS_TEMPLATE.exists():
        raise SystemExit(f"missing harness template: {HARNESS_TEMPLATE}")

    cache_dir = args.cache_dir
    if cache_dir.exists() and not args.keep_cache:
        shutil.rmtree(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    prepare_harness()
    if args.dry_run:
        print_plan(args)
        return 0
    if args.prebuild:
        run_prebuild(args.profile)

    record = run_harness(args)
    annotate_record(args, record)
    print_summary(record)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        print(f"E2E_RESULTS {args.output}")
    return 0 if record["success"] else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke test shared live/backtest market cache policy.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--symbols", default=DEFAULT_SYMBOLS, help="Comma-separated tick symbols.")
    parser.add_argument("--start-ns", type=int, default=DEFAULT_START_NS, help="Backtest start ns.")
    parser.add_argument("--end-ns", type=int, default=DEFAULT_END_NS, help="Backtest end ns.")
    parser.add_argument("--batch-size", type=positive_int, default=32, help="Warmup batch size.")
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("/private/tmp/tqsdk-market-cache-e2e"),
        help="Cache directory used by the smoke.",
    )
    parser.add_argument("--keep-cache", action="store_true", help="Reuse the existing cache dir.")
    parser.add_argument("--profile", choices=["dev", "release"], default="dev", help="Cargo profile.")
    parser.add_argument("--timeout-secs", type=int, default=300, help="Harness timeout.")
    parser.add_argument("--live-seconds", type=int, default=0, help="Optional live recording duration.")
    parser.add_argument(
        "--live-min-updates",
        type=int,
        default=0,
        help="Minimum live updates to wait for when live recording is enabled.",
    )
    parser.add_argument(
        "--live-min-rows",
        type=int,
        default=0,
        help="Minimum live tick rows expected when live recording is enabled.",
    )
    parser.add_argument("--skip-remote", action="store_true", help="Skip remote-on-miss warmup.")
    parser.add_argument("--skip-cache-only", action="store_true", help="Skip cache-only warmup.")
    parser.add_argument("--skip-replay", action="store_true", help="Skip cache-only replay.")
    parser.add_argument("--min-rows", type=int, default=1, help="Minimum rows expected from warmup/replay.")
    parser.add_argument("--output", type=Path, help="Optional JSON output path.")
    parser.add_argument("--dry-run", action="store_true", help="Print plan without running cargo.")
    parser.add_argument("--no-prebuild", dest="prebuild", action="store_false", help="Skip cargo build.")
    parser.set_defaults(prebuild=True)
    return parser.parse_args()


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def prepare_harness() -> None:
    src_dir = HARNESS_DIR / "src"
    src_dir.mkdir(parents=True, exist_ok=True)
    (HARNESS_DIR / "Cargo.toml").write_text(
        "\n".join(
            [
                "[package]",
                'name = "tqsdk-market-cache-e2e-harness"',
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
    (src_dir / "main.rs").write_text(HARNESS_TEMPLATE.read_text(encoding="utf-8"), encoding="utf-8")


def print_plan(args: argparse.Namespace) -> None:
    print(f"repo={REPO_ROOT}")
    print(f"harness={HARNESS_DIR}")
    print(f"cache_dir={args.cache_dir}")
    print(f"symbols={args.symbols}")
    print(f"range_ns={args.start_ns}..{args.end_ns}")
    print(f"live_seconds={args.live_seconds}")
    print(f"profile={args.profile}")


def run_prebuild(profile: str) -> None:
    cmd = cargo_cmd("build", profile)
    print(f"E2E_PREBUILD {' '.join(cmd)}")
    subprocess.run(cmd, cwd=HARNESS_DIR, check=True)


def run_harness(args: argparse.Namespace) -> dict[str, Any]:
    env = os.environ.copy()
    env.update(
        {
            "TQ_E2E_LABEL": f"market-cache-e2e-{dt.datetime.now(dt.UTC).strftime('%Y%m%dT%H%M%SZ')}",
            "TQ_E2E_CACHE_DIR": str(args.cache_dir),
            "TQ_E2E_SYMBOLS": args.symbols,
            "TQ_E2E_START_NS": str(args.start_ns),
            "TQ_E2E_END_NS": str(args.end_ns),
            "TQ_E2E_BATCH_SIZE": str(args.batch_size),
            "TQ_E2E_LIVE_SECONDS": str(args.live_seconds),
            "TQ_E2E_LIVE_MIN_UPDATES": str(args.live_min_updates),
        }
    )
    if args.skip_remote:
        env["TQ_E2E_SKIP_REMOTE"] = "1"
    if args.skip_cache_only:
        env["TQ_E2E_SKIP_CACHE_ONLY"] = "1"
    if args.skip_replay:
        env["TQ_E2E_SKIP_REPLAY"] = "1"

    record: dict[str, Any] = {
        "success": False,
        "cache_dir": str(args.cache_dir),
        "symbols": args.symbols,
        "range_start_ns": args.start_ns,
        "range_end_ns": args.end_ns,
        "profile": args.profile,
        "started_at": dt.datetime.now(dt.UTC).isoformat(),
    }
    cmd = cargo_cmd("run", args.profile)
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
    if completed.returncode != 0:
        record["stdout_tail"] = tail_text(completed.stdout)
        record["stderr_tail"] = tail_text(completed.stderr)
    return record


def annotate_record(args: argparse.Namespace, record: dict[str, Any]) -> None:
    warnings: list[str] = []
    if record.get("returncode") != 0:
        warnings.append(f"returncode:{record.get('returncode')}")

    remote_skipped = as_bool(record.get("remote_skipped"))
    cache_only_skipped = as_bool(record.get("cache_only_skipped"))
    replay_skipped = as_bool(record.get("replay_skipped"))
    remote_rows = as_int(record.get("remote_rows_written"))
    remote_missing = as_int(record.get("remote_symbols_missing"))
    remote_total = as_int(record.get("remote_symbols_total"))
    remote_complete = as_int(record.get("remote_complete_symbols"))
    cache_only_missing = as_int(record.get("cache_only_symbols_missing"))
    replay_ticks = as_int(record.get("replay_tick_count"))
    live_requested = as_bool(record.get("live_requested"))
    live_updates = as_int(record.get("live_updates"))
    live_rows = as_int(record.get("live_total_appended_rows"))

    if not remote_skipped:
        if remote_rows < args.min_rows:
            warnings.append(f"remote_rows_below_min:{remote_rows}<{args.min_rows}")
        if remote_total != remote_complete:
            warnings.append(f"remote_incomplete_symbols:{remote_complete}/{remote_total}")
        if remote_missing != 0:
            warnings.append(f"remote_missing:{remote_missing}")
    if not cache_only_skipped and cache_only_missing != 0:
        warnings.append(f"cache_only_missing:{cache_only_missing}")
    if not replay_skipped and replay_ticks < args.min_rows:
        warnings.append(f"replay_ticks_below_min:{replay_ticks}<{args.min_rows}")
    if live_requested and args.live_min_updates > 0 and live_updates < args.live_min_updates:
        warnings.append(f"live_updates_below_min:{live_updates}<{args.live_min_updates}")
    if live_requested and args.live_min_rows > 0 and live_rows < args.live_min_rows:
        warnings.append(f"live_rows_below_min:{live_rows}<{args.live_min_rows}")

    record["warnings"] = warnings
    record["warning_count"] = len(warnings)
    record["success"] = record.get("returncode") == 0 and not warnings


def parse_harness_result(stdout: str) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for line in stdout.splitlines():
        if not line.startswith("E2E_RESULT\t"):
            continue
        for field in line.split("\t")[1:]:
            key, sep, value = field.partition("=")
            if sep:
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


def print_summary(record: dict[str, Any]) -> None:
    status = "E2E_OK" if record["success"] else "E2E_FAIL"
    print(
        f"{status} "
        f"remote_rows={record.get('remote_rows_written')} "
        f"remote_missing={record.get('remote_symbols_missing')} "
        f"cache_only_missing={record.get('cache_only_symbols_missing')} "
        f"replay_ticks={record.get('replay_tick_count')} "
        f"live_updates={record.get('live_updates')} "
        f"process_s={record.get('process_elapsed_s')} "
        f"warnings={','.join(record.get('warnings', []))}"
    )
    if not record["success"]:
        if record.get("stdout_tail"):
            print("STDOUT_TAIL\n" + str(record["stdout_tail"]), file=sys.stderr)
        if record.get("stderr_tail"):
            print("STDERR_TAIL\n" + str(record["stderr_tail"]), file=sys.stderr)


def cargo_cmd(action: str, profile: str) -> list[str]:
    cmd = ["cargo", action, "--manifest-path", str(HARNESS_DIR / "Cargo.toml")]
    if profile == "release":
        cmd.append("--release")
    return cmd


def as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() == "true"


def tail_text(text: str | bytes, limit: int = 4000) -> str:
    if isinstance(text, bytes):
        text = text.decode("utf-8", errors="replace")
    return text[-limit:] if len(text) > limit else text


if __name__ == "__main__":
    raise SystemExit(main())
