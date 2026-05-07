# tqsdk-rust

Rust crates for building Tianqin/TQSDK market data, trading, strategy, and research
workflows on a shared async runtime.

[![CI](https://github.com/zynthium/tqsdk-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/zynthium/tqsdk-rust/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## Overview

`tqsdk-rust` is a Cargo workspace for a Rust implementation of the TQSDK
runtime model. It separates the low-level protocol substrate from user-facing
facades, so the same session, state tree, commit log, and cursor semantics can
support Python-style `wait_update()` loops, Rust-native streams, execution
tools, and offline research code.

The project is designed for users who need stronger type boundaries and more
control than a monolithic SDK usually provides:

- strategy developers who want a familiar single-owner `wait_update()` API
- async Rust services that need multiple consumers over the same live session
- trading tools that need order tasks, risk gates, and deterministic test hooks
- research pipelines that need history pages, downloads, exports, and replay
- SDK contributors who need a protocol-complete runtime core

## Workspace Crates

| Crate | Use it for |
| --- | --- |
| [`tqsdk-core`](crates/tqsdk-core) | Low-level async protocol substrate, state store, commit/revision model, runtime reader, cursors, adapters, and schema types |
| [`tqsdk-session`](crates/tqsdk-session) | Shared session ownership, lazy connection, command progress, one-shot direct query, metadata, schema, and service queries |
| [`tqsdk-wait`](crates/tqsdk-wait) | Python-style single-owner `TqApi`, `wait_update()`, `is_changing()`, live object refs, serial windows, and wait-style trade commands |
| [`tqsdk-stream`](crates/tqsdk-stream) | Rust async-native multi-consumer commit streams, object streams, filters, lag diagnostics, health status, and slow-consumer isolation foundations |
| [`tqsdk-task`](crates/tqsdk-task) | Execution tooling, `TargetPosTask`, schedulers, typed order builders, risk gates, strategy host, fake market/broker test harnesses, and low-latency trading desk profile |
| [`tqsdk-data`](crates/tqsdk-data) | Research and offline data APIs, history page/series/download, CSV export, option greeks, continuous contract data, cache, and replay foundations |

## Status

This repository is under active development. The crates are versioned at
`0.1.0` and are intended to be used from this workspace or by Git dependency
until a crates.io release is cut. Public examples under `crates/*/examples` are
treated as API contracts and are kept compilable as the surface evolves.

## Requirements

- Rust 1.85 or newer
- Tokio for async examples and live session code
- Tianqin/TQSDK credentials for live market, trading, query, and history
  examples

Live examples read credentials from:

```bash
export TQ_AUTH_USER="your-account"
export TQ_AUTH_PASS="your-password"
```

## Installation

Use the crates from the workspace while developing locally:

```toml
[dependencies]
tqsdk-wait = { path = "crates/tqsdk-wait" }
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

For another project, depend on the Git repository:

```toml
[dependencies]
tqsdk-wait = { git = "https://github.com/zynthium/tqsdk-rust" }
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

Swap `tqsdk-wait` for `tqsdk-session`, `tqsdk-stream`, `tqsdk-task`, or
`tqsdk-data` depending on the API shape you need.

## Quick Start

Read live quote updates with the Python-style wait facade:

```rust
use tqsdk_wait::TqApiBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let mut api = TqApiBuilder::new(user, pass).build().await?;
    let quote = api.get_quote("SHFE.au2602").await?;

    loop {
        if !api.wait_update(None).await? {
            continue;
        }

        if api.is_changing(&quote)? {
            let snapshot = quote.load(&api)?;
            println!("{} {}", snapshot.datetime, snapshot.last_price);
        }
    }
}
```

Run the matching example:

```bash
cargo run -p tqsdk-wait --example quote_wait
```

Run it once and exit after the first printed update:

```bash
TQ_WAIT_ONCE=1 cargo run -p tqsdk-wait --example quote_wait
```

## Choosing an API

Use `tqsdk-wait` when you want a familiar single-owner loop:

```rust
let mut api = tqsdk_wait::TqApiBuilder::new(user, pass).build().await?;
let quote = api.get_quote("SHFE.au2602").await?;
api.wait_update(None).await?;
let snapshot = quote.load(&api)?;
```

Use `tqsdk-stream` when multiple async consumers need to observe the same live
session:

```rust
use futures::StreamExt;

let stream = tqsdk_stream::TqStreamBuilder::new(user, pass).build().await?;
stream.subscribe_quotes(["SHFE.au2602"]).await?;
let mut quotes = stream.quote_stream("SHFE.au2602")?;
let update = quotes.next().await.ok_or("quote stream closed")??;
```

Use `tqsdk-session` for one-shot metadata, schema, service, and low-level query
work:

```rust
let session = tqsdk_session::SessionClientBuilder::new(user, pass)
    .enable_query()
    .build()?;
let rows = session.query_symbol_info(&["SHFE.au2602"]).await?;
```

Use `tqsdk-data` for history and research workflows:

```rust
use std::time::Duration;

let session = tqsdk_session::SessionClientBuilder::new(user, pass)
    .futures_market()
    .build()?;
let client = tqsdk_data::DataClient::from_session(session);
let request = tqsdk_data::KlineDataPageRequest::new(
    "SHFE.au2602",
    Duration::from_secs(60),
    128,
);
let page = client.get_kline_data_page(request).await?;
```

## Examples

Representative examples:

| Scenario | Command |
| --- | --- |
| Quote updates with `wait_update()` | `cargo run -p tqsdk-wait --example quote_wait` |
| Quote stream consumer | `cargo run -p tqsdk-stream --example quote_stream` |
| Metadata query | `cargo run -p tqsdk-session --example query_symbol_info` |
| Command wait helper | `cargo run -p tqsdk-session --example query_command_wait` |
| Kline page query | `cargo run -p tqsdk-data --example kline_data_page` |
| Kline CSV export | `cargo run -p tqsdk-data --example kline_export_csv` |
| Target position task | `cargo run -p tqsdk-task --example target_pos` |
| Low-latency trading desk profile | `cargo run -p tqsdk-task --example api_contract_s31_low_latency_trading_desk` |

Additional API-contract examples live under each crate's `examples/` directory.

## Architecture

The workspace follows a "stable substrate, replaceable facades" design:

```text
tqsdk-core
    ^
    |
tqsdk-session
    ^
    |
tqsdk-wait / tqsdk-stream / tqsdk-data
    ^
    |
tqsdk-task
```

All externally visible state changes flow through the runtime commit model:

```text
RuntimeCommand / RuntimeInput
    -> ProtocolAdapter
    -> NormalizedMutation
    -> RuntimeHandle
    -> StateStore
    -> CommitResult
    -> RuntimeReader / UpdateCursor
```

This keeps `wait_update()` loops, async streams, task tooling, and research
pipelines on the same state tree and revision semantics. The lower crates stay
small: direct query belongs to `tqsdk-session`, live diff consumption belongs to
`tqsdk-wait` and `tqsdk-stream`, execution tooling belongs to `tqsdk-task`, and
offline/research workflows belong to `tqsdk-data`.

See [docs/architecture](docs/architecture) for the full design notes and
[docs/architecture/validation.md](docs/architecture/validation.md) for the
validation matrix.

## Development

Clone the repository and check the workspace:

```bash
git clone https://github.com/zynthium/tqsdk-rust.git
cd tqsdk-rust
cargo check --workspace --examples
```

Common verification commands:

```bash
cargo fmt --all --check
cargo check --workspace --examples
cargo test --workspace
cargo clippy --workspace --examples --all-targets -- -D warnings
```

When changing feature flags or dependency propagation, also run:

```bash
cargo check --workspace --no-default-features
cargo check --workspace --no-default-features --examples
cargo check --workspace --all-features --examples
```

## Documentation

- [Documentation index](docs/README.md)
- [Architecture overview](docs/architecture/README.md)
- [Runtime core overview](docs/architecture/runtime-core/overview.md)
- [Crate boundary audit](docs/architecture/crate-boundaries.md)
- [Validation matrix](docs/architecture/validation.md)
- [Roadmap](ROADMAP.md)

Each crate also has its own README with crate-specific design boundaries,
examples, and public surface notes.

## Contributing

Issues and pull requests are welcome. Before making a change, read the
architecture overview and the README for the crate you plan to touch. Keep
changes scoped, preserve crate ownership boundaries, and update the relevant
architecture or crate documentation when a public API, feature flag, runtime
contract, or facade responsibility changes.

For code changes, include focused tests or API-contract examples when the change
affects public behavior.

## License

This project is licensed under the [MIT License](LICENSE).
