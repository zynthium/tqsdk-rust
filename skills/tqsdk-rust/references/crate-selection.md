# Crate Selection

Use `scenario-router.md` first. This file explains crate boundaries once the scenario is known. If a Python TqSdk API appears to span several concerns, split it by Rust crate ownership before writing code.

## Decision Table

| User goal | Use | Why |
| --- | --- | --- |
| Python-style live market/trade loop | `tqsdk-wait` | Single owner, `wait_update()`, `is_changing()`, live refs, serial windows |
| Multi-consumer async events or fan-out | `tqsdk-stream` | Commit stream, filters, event streams, lag diagnostics, managed sinks |
| One-shot metadata/query/service calls | `tqsdk-session` | GraphQL/query, schema, symbol info, quotes metadata, calendar, settlement, ranking, EDB |
| Strategy execution helpers | `tqsdk-task` | `TaskHost`, `TargetPosTask`, scheduler, risk gate, typed order builders, fake broker tests |
| Historical/offline research | `tqsdk-data` | Data pages, data series, downloads, CSV export, history cache, option Greeks, replay cache |
| Runtime substrate or custom facade | `tqsdk-core` plus `tqsdk-session` | Commands, adapters, commit/revision/cursor, `RuntimeReader` hot path |

## Boundary Rules

- `tqsdk-session` owns one-shot request/response APIs: GraphQL, schema, metadata, calendar, settlement, ranking, EDB, auth refresh, replay control, and low-level command wait helpers.
- `tqsdk-wait` owns Python-style single-owner live refs and `wait_update()` consumption. It can expose `session()` but must not copy direct-query APIs.
- `tqsdk-stream` owns multi-consumer commit/event streams, filters, lag diagnostics, and stream sinks. It can expose `session()` but must not become the metadata/query layer.
- `tqsdk-task` owns strategy execution, target position, schedulers, risk gates, ownership, multi-account order foundations, fake broker tests, replay strategy host, and the S31 trading desk profile.
- `tqsdk-data` owns research/offline data, history pages/series/downloads, CSV export, Python-compatible history cache, option Greeks, and market-cache replay materialization.
- `tqsdk-core` owns runtime substrate only. Do not re-export auth/http/TqKq implementation details or add facade convenience APIs there.

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

`SessionClientBuilder::build()` and `DataClientBuilder::build()` are synchronous constructors. `TqApiBuilder::build().await` and `TqStreamBuilder::build().await` are async because those facades wrap live session startup behavior.
