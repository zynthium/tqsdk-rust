# `tqsdk-ctpse-helper`

Private, non-published executable used by `tqsdk-session` to isolate dynamic
loading of the official CTP 穿透式客户端信息 collector. It is not part of the
SDK public API and must be distributed beside the application executable when
the embedded native bundle is enabled.

The protocol is intentionally narrow:

```text
tqsdk-ctpse-helper [--library <official-ctpse-library>]
```

On success stdout is exactly one JSON object with
`client_system_info`; it never receives account credentials or trade passwords.
The parent process validates the result before adding it to a Future login.

Without an embedded bundle, pass a verified official library through an
absolute `--library` path (normally via `TQ_TRADE_CTPSE_LIBRARY`). The helper
does not download and does not attempt a pure-Rust reimplementation of the
collector. Its process boundary isolates the SDK address space and crashes;
it is not an OS sandbox for the dynamically loaded official library.
