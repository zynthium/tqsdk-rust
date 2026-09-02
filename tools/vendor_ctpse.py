#!/usr/bin/env python3
"""Vendor reviewed official tqsdk-ctpse wheels into the offline bundle layout.

This is a maintainer-only supply-chain action. It uses Python's standard
library, does not import tqsdk or tqsdk_ctpse, and must be invoked with an
explicit acknowledgement of redistribution permission.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path, PurePosixPath


PACKAGE = "tqsdk-ctpse"
PACKAGE_PREFIX = "tqsdk_ctpse/"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def targets_for_wheel(filename: str) -> tuple[str, ...]:
    name = filename.lower()
    if "manylinux" in name and "x86_64" in name:
        return ("x86_64-unknown-linux-gnu",)
    if "win_amd64" in name:
        return ("x86_64-pc-windows-msvc",)
    if "win32" in name:
        return ("i686-pc-windows-msvc",)
    if "macosx" in name and "universal2" in name:
        return ("aarch64-apple-darwin", "x86_64-apple-darwin")
    return ()


def bundled_files_and_primary(wheel: Path) -> tuple[list[str], str]:
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
    files = [
        name
        for name in names
        if native_relative_path(name) is not None
        and not name.endswith("/")
        and not name.endswith((".py", ".pyc"))
        and ".dist-info/" not in name
    ]
    if not files:
        raise ValueError("wheel has no native tqsdk_ctpse payload")

    def likely_primary(name: str) -> bool:
        lower = name.lower()
        return "collect" in lower and (
            lower.endswith((".so", ".dll", ".dylib")) or ".framework/" in lower
        )

    primary = next((name for name in files if likely_primary(name)), None)
    if primary is None:
        raise ValueError("wheel has no recognizable client-system collector library")
    for name in files:
        relative = native_relative_path(name)
        if relative is None or relative.is_absolute() or ".." in relative.parts:
            raise ValueError("wheel native payload has an unsafe path")
    return files, primary


def native_relative_path(name: str) -> PurePosixPath | None:
    path = PurePosixPath(name)
    parts = path.parts
    if parts[:1] == ("tqsdk_ctpse",):
        return PurePosixPath(*parts[1:])
    if (
        len(parts) >= 4
        and parts[0].startswith("tqsdk_ctpse-")
        and parts[0].endswith(".data")
        and parts[1:3] == ("purelib", "tqsdk_ctpse")
    ):
        return PurePosixPath(*parts[3:])
    return None


def download(url: str, destination: Path, expected_sha256: str) -> None:
    if destination.is_file() and sha256_file(destination) == expected_sha256:
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as temporary:
        temporary_path = Path(temporary.name)
        try:
            with urllib.request.urlopen(url, timeout=60) as response:
                while chunk := response.read(1024 * 1024):
                    temporary.write(chunk)
            temporary.flush()
            os.fsync(temporary.fileno())
        except BaseException:
            temporary_path.unlink(missing_ok=True)
            raise
    if sha256_file(temporary_path) != expected_sha256:
        temporary_path.unlink(missing_ok=True)
        raise ValueError("downloaded wheel SHA-256 differs from PyPI metadata")
    temporary_path.replace(destination)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", default="1.2.0")
    parser.add_argument(
        "--destination",
        type=Path,
        default=Path("third_party") / "tqsdk-ctpse",
    )
    parser.add_argument(
        "--accept-redistribution-license",
        action="store_true",
        help="required acknowledgement that redistribution of these exact wheels is authorized",
    )
    args = parser.parse_args()
    if not args.accept_redistribution_license:
        parser.error("refusing to download official binaries without redistribution acknowledgement")

    metadata_url = f"https://pypi.org/pypi/{PACKAGE}/{args.version}/json"
    with urllib.request.urlopen(metadata_url, timeout=30) as response:
        release = json.load(response)

    destination = args.destination / args.version
    artifacts: dict[str, dict[str, object]] = {}
    for item in release["urls"]:
        filename = item["filename"]
        if item.get("packagetype") != "bdist_wheel":
            continue
        targets = targets_for_wheel(filename)
        if not targets:
            continue
        sha256 = item["digests"]["sha256"]
        if not re.fullmatch(r"[0-9a-f]{64}", sha256):
            raise ValueError("PyPI returned an invalid SHA-256")
        wheel = destination / filename
        download(item["url"], wheel, sha256)
        files, primary = bundled_files_and_primary(wheel)
        artifact = {
            "wheel": filename,
            "sha256": sha256,
            "primary_library": primary,
            "files": files,
        }
        for target in targets:
            if target in artifacts:
                raise ValueError(f"multiple wheels match Cargo target {target}")
            artifacts[target] = artifact

    required_targets = {"x86_64-unknown-linux-gnu", "x86_64-pc-windows-msvc"}
    missing = required_targets - artifacts.keys()
    if missing:
        raise ValueError(f"release lacks required target wheels: {', '.join(sorted(missing))}")
    destination.mkdir(parents=True, exist_ok=True)
    manifest = {"version": args.version, "artifacts": artifacts}
    temporary = destination / ".manifest.json.tmp"
    temporary.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(destination / "manifest.json")
    print(f"wrote reviewed bundle manifest: {destination / 'manifest.json'}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, zipfile.BadZipFile) as error:
        print(f"vendor_ctpse.py: {error}", file=sys.stderr)
        raise SystemExit(1)
