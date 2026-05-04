# Crate Selection

Use `scenario-router.md` first. This file explains crate boundaries once the scenario is known.

## Decision Table

| User goal | Use | Why |
| --- | --- | --- |
| Python-style live market/trade loop | `tqsdk-wait` | Single owner, `wait_update()`, `is_changing()`, live refs, serial windows |
| Multi-consumer async events or fan-out | `tqsdk-stream` | Commit stream, filters, event streams, lag diagnostics, managed sinks |
| One-shot metadata/query/service calls | `tqsdk-session` | GraphQL/query, schema, symbol info, quotes metadata, calendar, settlement, ranking, EDB |
| Strategy execution helpers | `tqsdk-task` | `TaskHost`, `TargetPosTask`, scheduler, risk gate, typed order builders, fake broker tests |
| Historical/offline research | `tqsdk-data` | Data pages, data series, downloads, CSV export, history cache, option Greeks, replay cache |
| Runtime substrate or custom facade | `tqsdk-core` plus `tqsdk-session` | Commands, adapters, commit/revision/cursor, `RuntimeReader` hot path |

## Shortcut Questions

If the request is unclear, decide by asking what the user wants to consume:

| Answer | Crate |
| --- | --- |
| "A live object that changes" | `tqsdk-wait` |
| "Events for multiple consumers" | `tqsdk-stream` |
| "A query result" | `tqsdk-session` |
| "A managed order/strategy abstraction" | `tqsdk-task` |
| "Historical rows/files/cache" | `tqsdk-data` |
| "Runtime commits/cursors" | `tqsdk-core` plus `tqsdk-session` |

## Dependency Patterns

Use one of these dependency forms depending on how the SDK is distributed.

Published crates:

```toml
[dependencies]
tqsdk-wait = "<version>"
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

Git dependency:

```toml
[dependencies]
tqsdk-wait = { git = "https://github.com/OWNER/tqsdk-rust" }
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

Local checkout:

```toml
[dependencies]
tqsdk-wait = { path = "../tqsdk-rust/crates/tqsdk-wait" }
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

Replace `tqsdk-wait` with the selected crate. Some patterns need multiple crates, for example `tqsdk-task` plus `tqsdk-core` for typed trade enums.
