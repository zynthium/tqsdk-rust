# Official `tqsdk-ctpse` bundle

This directory contains the reviewed, version-pinned official artifacts embedded by the private
`tqsdk-ctpse-helper` process. It contains no credentials or customer data.

The `1.2.0` bundle covers these Cargo targets:

| Target | Official payload |
| --- | --- |
| `x86_64-unknown-linux-gnu` | `LinuxDataCollect64.so` |
| `x86_64-apple-darwin`, `aarch64-apple-darwin` | universal `MacDataCollect.framework` |
| `x86_64-pc-windows-msvc` | `WinDataCollect64.dll` |
| `i686-pc-windows-msvc` | `WinDataCollect32.dll` |

Windows ARM64 is intentionally unsupported because the official release has no matching
artifact. `build.rs` never downloads: it verifies the wheel SHA-256 declared in
`1.2.0/manifest.json`, extracts only the listed runtime files, and embeds them into the helper
for the current Cargo target.

The official package declares `License: UNKNOWN`. The checked-in binaries were added only after
the repository maintainer explicitly approved redistribution of this exact release. Re-vendoring
is a maintainer-only supply-chain action:

```bash
python3 tools/vendor_ctpse.py --accept-redistribution-license --version 1.2.0
```

The tool fetches published wheels, verifies the PyPI SHA-256 values, excludes test collectors,
and atomically writes the manifest. `manifest.example.json` is a non-loadable schema example.
