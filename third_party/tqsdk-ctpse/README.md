# Official `tqsdk-ctpse` bundle

This directory defines the offline bundle contract used by the private
`tqsdk-ctpse-helper` process. It intentionally contains **no official wheel,
`.so`, DLL, framework, credentials, or customer data** in this revision.

The official package currently declares `License: UNKNOWN`. Do not add its
artifacts to this repository, a release archive, or Git LFS until the project
has recorded permission to redistribute the exact release. The helper remains
usable with a user-supplied library through `TQ_TRADE_CTPSE_LIBRARY`.

After that approval, a maintainer runs the reviewed, explicit vendor command:

```bash
python3 tools/vendor_ctpse.py --accept-redistribution-license --version 1.2.0
```

It fetches the published wheels, verifies each PyPI SHA-256, writes
`third_party/tqsdk-ctpse/1.2.0/manifest.json`, and leaves only wheels plus the
manifest for review. `build.rs` is offline: it never downloads. It validates
the manifest and wheel hash for the current Cargo target, then embeds only the
declared native files in `tqsdk-ctpse-helper`.

`manifest.example.json` is the checked-in schema. Its placeholder values are
not a bundle and are deliberately ignored by the build. An actual
`manifest.json` must be generated from PyPI metadata, reviewed with the wheel
files, and committed only after redistribution approval.
