# `tqsdk-runtime-contract`

Low-level async runtime contract for Tianqin / TQSDK server interaction.

This crate is the V1 core substrate of a Rust reimplementation of TQSDK. It is designed for performance-sensitive callers who want the thinnest possible layer over Tianqin's official services while keeping a stable abstraction boundary for higher-level facades.

> [!IMPORTANT]
> This crate is a pure async substrate. It never creates a Tokio runtime internally. Callers must provide an active Tokio runtime for auth, HTTP, websocket, reconnect backoff, and live session driving.

> [!NOTE]
> This crate is not a `TqApi`, `wait_update()`, stream, or callback SDK. Those are intended to be built on top of this contract in separate facade crates.

## What This Crate Provides

- A protocol-complete runtime contract covering market diff, trade, replay, query, schema, auth, session, and system control.
- A single command model: `RuntimeCommand -> OutboundDispatch -> RuntimeInput -> NormalizedMutation -> CommitResult`.
- A single shared state tree for all visible data.
- A reader-first consumption model built around `RuntimeReader`, `SnapshotReadGuard`, `CommitReadGuard`, and `UpdateCursor`.
- Typed schema structs for official TQ objects and related metadata/query payloads.
- Low-level transport, auth, topology bootstrap, HTTP execution, and session orchestration primitives.

## Dependency

The Cargo package name is `tqsdk-runtime-contract`.

```toml
[dependencies]
tqsdk-runtime-contract = { path = "../tqsdk-rust" }
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

Replace the `path` dependency with your published version or git reference when you package this crate separately.

## Non-Goals

- No high-level user convenience API.
- No `wait_update()` facade.
- No stream/callback facade.
- No strategy helpers, tasks, GUI/reporting, downloader, DataFrame/polars integration, or rich typed view layer.
- No hidden side channels that bypass the commit/revision model.

## Coverage

The current core surface is intended to cover all protocol domains required by the official servers:

- DIFF protocol objects.
- Trade commands and command status projection.
- Replay/feed stepping and cursor state.
- Auth/session/system lifecycle control.
- GraphQL / HTTP query interaction.
- Schema / metadata / bootstrap interaction.

Typed schema coverage includes, among others:

- Market objects: `Quote`, `Kline`, `Tick`, `Chart`, `ChartInfo`, `TradingTime`.
- Trading objects: `Account`, `Position`, `Order`, `Trade`, `PreInsertOrder`, `Notification`, `SettlementInfo`.
- Risk objects: `RiskManagementRule`, `RiskManagementData`, `SelfTrade`, `FrequentCancellation`, `TradePositionRatio`.
- Security objects: `SecurityAccount`, `SecurityPosition`, `SecurityOrder`, `SecurityTrade`.
- Query / metadata objects: `TradingStatus`, `SymbolSettlement`, `SymbolRanking`, `TradingCalendarDay`, `EdbIndexData`.

## Core Surface

| API | Role |
| --- | --- |
| `RuntimeHandle` | Write-side entry point for command submission, ingestion, command status, and session state projection |
| `RuntimeReader` | Canonical read-side entry point |
| `SnapshotReadGuard` / `StateReadView` | Revision-bound zero-copy snapshot reads |
| `CommitReadGuard` | Exact-revision commit + state read |
| `UpdateCursor` | Independent commit consumption cursor |
| `SessionRuntime` | Auth/bootstrap/connect/recover/flush/pump orchestration |
| `AdapterRegistry` | Domain adapter registration and command/input encode/decode |
| `TqAuthProvider` | Official Tianqin auth + topology resolver implementation |
| `WebSocketTransport` / `DefaultRouteConnector` | Low-level websocket route connection |
| `ReqwestHttpExecutor` | Pending HTTP route executor for query/schema-style requests |

## Contract Model

```text
RuntimeCommand
  -> ProtocolAdapter encode
  -> OutboundDispatch
  -> transport / HTTP / replay / internal route
  -> RuntimeInput
  -> ProtocolAdapter decode
  -> NormalizedMutation
  -> CommitResult + Revision
  -> RuntimeReader / UpdateCursor
```

The key architectural rule is simple: all user-visible state must enter the same runtime state tree, and all user-visible change must be explained by the same commit/revision/causality model.

## Quick Start

### 1. Build the core runtime surface

```rust
use tqsdk_runtime_contract::{
    AdapterRegistry, Runtime, RuntimeHandle,
};

fn default_adapters() -> AdapterRegistry {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();
    registry
}

let handle = RuntimeHandle::with_adapters(default_adapters());
let reader = handle.reader();
let cursor = reader.cursor();

assert_eq!(cursor.next_revision().get(), 1);
assert_eq!(reader.head_revision(), None);
```

### 2. Submit low-level commands

```rust
use tqsdk_runtime_contract::{
    Runtime, RuntimeCommand, MarketCommand, Symbol,
};

async fn submit_quotes(handle: &impl Runtime) -> tqsdk_runtime_contract::Result<()> {
    let command_id = handle
        .submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
            symbols: vec![Symbol::new("SHFE.au2602")],
        }))
        .await?;

    println!("submitted command {}", command_id.get());
    let _reader = handle.reader();
    // Command submission only creates outbound work.
    // State becomes visible after the session/runtime ingests remote input.
    Ok(())
}
```

### 3. Drive a live session

For a real connection, the calling application assembles the runtime loop explicitly:

```rust
use tqsdk_runtime_contract::{
    AdapterRegistry, DefaultRouteConnector, EndpointConfig, MarketSessionTarget,
    PasswordCredentials, ProtocolDomain, ReqwestHttpExecutor, Runtime, RuntimeCommand,
    RuntimeHandle, SchemaCommand, SchemaId, SessionBootstrap, SessionConfig, SessionRuntime,
    TqAuthProvider,
};

fn default_adapters() -> AdapterRegistry {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();
    registry
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let username = std::env::var("TQ_AUTH_USER")?;
    let password = std::env::var("TQ_AUTH_PASS")?;

    let handle = RuntimeHandle::with_adapters(default_adapters());
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let provider = TqAuthProvider::new(PasswordCredentials::new(username, password));
    let connector = DefaultRouteConnector::default();
    let bootstrap_adapters = default_adapters();
    let config = SessionConfig::new(EndpointConfig::from_env())
        .with_market_target(MarketSessionTarget::new(false, false))
        .enable_domain(ProtocolDomain::Market)
        .enable_domain(ProtocolDomain::Schema);

    let mut run = runtime
        .establish(
            &provider,
            &provider,
            &connector,
            &config,
            &bootstrap_adapters,
        )
        .await?;

    let _market_receipts = runtime.flush_outbound(&mut run).await?;

    let schema_executor = ReqwestHttpExecutor::new()?;
    let schema_command = handle
        .submit(RuntimeCommand::Schema(SchemaCommand::Refresh {
            schema_id: SchemaId::new("symbols-latest"),
            path: "/shinny_chinese_holiday.json".to_string(),
        }))
        .await?;

    let _schema_receipts = runtime.flush_outbound(&mut run).await?;
    let _outcome = runtime
        .drive_pending_route_once(
            &mut run,
            "schema",
            &schema_executor,
            vec![schema_command],
            tqsdk_runtime_contract::CommitScope::RealtimeUpdate,
        )
        .await?;

    Ok(())
}
```

See [examples/live_probe.rs](examples/live_probe.rs) and [tests/runtime_contract_live_smoke.rs](tests/runtime_contract_live_smoke.rs) for complete end-to-end usage.

## Environment Variables

`EndpointConfig::from_env()` recognizes the following endpoint overrides:

- `TQ_AUTH_URL`
- `TQ_MD_URL`
- `TQ_TD_URL`
- `TQ_QUERY_URL`
- `TQ_INS_URL`
- `TQ_REPLAY_URL`
- `TQ_SCHEMA_URL`
- `TQ_CHINESE_HOLIDAY_URL`

The live probe and live smoke tests additionally use:

- `TQ_AUTH_USER`
- `TQ_AUTH_PASS`
- `TQ_TEST_SYMBOL`

## Design Constraints

- Single shared runtime state tree across all protocol domains.
- Single revision source and single commit model.
- Adapter code can encode/decode, but cannot publish commits on its own.
- Reader-first abstraction so future `wait_update`, stream, and callback facades can share one substrate.
- Compatibility helpers like `StateSnapshot` and `CommitLog` remain available, but they do not define the primary read model.

## Validation

The repository includes contract-focused tests for:

- V1 capability coverage across all protocol domains.
- Reader surface and revision-bound state access.
- Command ledger, commit retention, and reconnect behavior.
- HTTP executor and websocket transport contracts.
- Live smoke coverage for official auth/market/schema interaction.

Recommended regression entry points:

```bash
cargo test -q --test runtime_contract_v1_capability
cargo test -q --test runtime_contract_reader_surface --test runtime_contract_surface
cargo test -q
```

## Architecture Notes

The architecture docs in [`docs/architecture`](docs/architecture) describe the intended layering:

- [`docs/architecture/README.md`](docs/architecture/README.md)
- [`docs/architecture/runtime-core/overview.md`](docs/architecture/runtime-core/overview.md)
- [`docs/architecture/validation.md`](docs/architecture/validation.md)

The short version is:

- V1 is the protocol-complete runtime contract.
- V2+ facades should consume `RuntimeReader` and `UpdateCursor` instead of redefining the runtime core.
