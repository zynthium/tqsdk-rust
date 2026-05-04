#!/usr/bin/env python3
"""Create a minimal TQSDK Rust project from bundled templates."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

TEMPLATES = {
    "wait-quote-loop": ["tqsdk-wait"],
}


def dependency_line(crate: str, source: str, value: str) -> str:
    if source == "version":
        return f'{crate} = "{value}"'
    if source == "git":
        return f'{crate} = {{ git = "{value}" }}'
    if source == "path":
        crate_path = Path(value).expanduser().resolve() / "crates" / crate
        return f'{crate} = {{ path = "{crate_path.as_posix()}" }}'
    raise ValueError(f"unsupported sdk source: {source}")


def replace_tokens(root: Path, tokens: dict[str, str]) -> None:
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for key, value in tokens.items():
            text = text.replace(key, value)
        path.write_text(text, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", help="Directory to create")
    parser.add_argument("--template", choices=sorted(TEMPLATES), default="wait-quote-loop")
    parser.add_argument("--sdk-source", choices=["git", "path", "version"], required=True)
    parser.add_argument("--sdk-value", required=True, help="Git URL, local SDK checkout path, or crate version")
    parser.add_argument("--symbol", default="SHFE.au2602")
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    skill_dir = script_dir.parent
    template_dir = skill_dir / "assets" / "templates" / args.template
    dest = Path(args.destination).expanduser().resolve()

    if dest.exists() and any(dest.iterdir()):
        raise SystemExit(f"destination exists and is not empty: {dest}")
    if not template_dir.exists():
        raise SystemExit(f"template not found: {template_dir}")

    shutil.copytree(template_dir, dest, dirs_exist_ok=True)
    tokens = {
        "{{TQSDK_WAIT_DEPENDENCY}}": dependency_line("tqsdk-wait", args.sdk_source, args.sdk_value),
        "{{SYMBOL}}": args.symbol,
    }
    replace_tokens(dest, tokens)
    print(f"created {args.template} project at {dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
