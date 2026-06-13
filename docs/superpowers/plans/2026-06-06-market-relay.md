# Market Relay Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an optional `tqsdk-relay` market relay service that can accept SDK market connections, fan out quote/tick/kline data from a shared upstream tick source, and leave direct-to-TQ SDK behavior unchanged when relay is not configured.

**Architecture:** Add `crates/tqsdk-relay` as an independent workspace crate with no reverse dependency from existing SDK crates. Keep relay protocol handling, interest/chart-id mapping, in-memory cache, K-line synthesis, bootstrap queue, and observability inside the relay crate. Add only thin endpoint-builder helpers to existing SDK crates so users can explicitly point the market route at relay.

**Tech Stack:** Rust 2024, Cargo workspace, Tokio, serde/serde_json, existing `tqsdk-core` public market types, optional `yawc` for WebSocket transport, no SDK default dependency on `tqsdk-relay`.

---

## Scope Lock

- Relay is optional. If users do not configure relay, existing `tqsdk-rust` direct-to-TQ behavior remains unchanged.
- Relay v1 proxies market route only. It must not proxy trade, query, auth, schema, metadata, or account state.
- Relay v1 uses a single upstream futures tick source. Do not add upstream sharding in this plan.
- Existing SDK crates must not depend on `tqsdk-relay`.
- Existing runtime contract remains unchanged: no second state tree inside `tqsdk-core`, `tqsdk-wait`, or `tqsdk-stream`.
- K-line fixed-duration synthesis uses `[start, end)` windows. A tick at exactly `end` belongs to the next bar.
- Live smoke tests require explicit environment variables and stay ignored by default.

## File Structure

Create:

- `crates/tqsdk-relay/Cargo.toml`: relay crate metadata, features, dependencies, example/test declarations.
- `crates/tqsdk-relay/README.md`: user-facing relay boundary, configuration, and run instructions.
- `crates/tqsdk-relay/src/lib.rs`: public module exports for tests and future embedding.
- `crates/tqsdk-relay/src/main.rs`: binary entrypoint that reads config and starts relay.
- `crates/tqsdk-relay/src/config.rs`: relay config types and environment parsing.
- `crates/tqsdk-relay/src/error.rs`: `RelayError`, `RelayResult`, diagnostics without credentials.
- `crates/tqsdk-relay/src/protocol.rs`: narrow downstream market protocol DTOs and compatible `rtn_data` helpers.
- `crates/tqsdk-relay/src/interest.rs`: downstream client ids, quote/chart interest registry, chart-id mapper.
- `crates/tqsdk-relay/src/cache.rs`: in-memory tick, quote, and K-line ring buffers.
- `crates/tqsdk-relay/src/kline.rs`: fixed-duration `[start, end)` K-line synthesis and best-effort tagging.
- `crates/tqsdk-relay/src/bootstrap.rs`: bootstrap/resync request coalescing and rate/concurrency limits.
- `crates/tqsdk-relay/src/observability.rs`: health, sources, and metrics snapshots.
- `crates/tqsdk-relay/src/server.rs`: downstream WebSocket server boundary.
- `crates/tqsdk-relay/src/upstream.rs`: upstream market client boundary and testable source trait.
- `crates/tqsdk-relay/tests/config.rs`: relay config defaults and validation tests.
- `crates/tqsdk-relay/tests/protocol.rs`: downstream protocol parsing/encoding tests.
- `crates/tqsdk-relay/tests/cache.rs`: tick ring and quote projection cache tests.
- `crates/tqsdk-relay/tests/interest.rs`: multi-client interest and chart-id isolation tests.
- `crates/tqsdk-relay/tests/kline.rs`: K-line synthesis boundary tests.
- `crates/tqsdk-relay/tests/bootstrap.rs`: queue coalescing and rate-limit tests.
- `crates/tqsdk-relay/tests/integration.rs`: in-process relay state flow tests with fake upstream.
- `crates/tqsdk-relay/tests/observability.rs`: health and metrics snapshot tests.
- `crates/tqsdk-relay/tests/server.rs`: JSON command server boundary tests.
- `crates/tqsdk-relay/tests/server_ws.rs`: real downstream WebSocket loopback test.
- `crates/tqsdk-relay/tests/upstream.rs`: futures universe and upstream tick chart tests.

Modify:

- `Cargo.toml`: add `crates/tqsdk-relay` to workspace members/default-members.
- `crates/tqsdk-session/src/builder.rs`: add explicit `market_url(...)` and `market_relay(...)` builder helpers, plus macro forwarders.
- `crates/tqsdk-session/tests/session_builder.rs`: builder endpoint tests proving relay opt-in is explicit.
- `crates/tqsdk-wait/src/builder.rs`: builder test for forwarded `market_relay(...)`.
- `crates/tqsdk-stream/src/builder.rs`: builder test for forwarded `market_relay(...)`.
- `crates/tqsdk/src/lib.rs`: add facade `TqBuilder::market_relay(...)` forwarder only if `TqBuilder` does not already expose generic market endpoint forwarding.
- `crates/tqsdk/tests/facade_contract.rs`: facade surface guard for `market_relay(...)`.
- `README.md`: mention relay as optional infrastructure, not default path.
- `docs/architecture/README.md`: record that relay is optional infrastructure and does not alter runtime/facade boundaries.
- `docs/architecture/crate-boundaries.md`: add `tqsdk-relay` as an optional market infrastructure crate.
- `docs/architecture/validation.md`: add relay validation commands.
- `docs/superpowers/specs/2026-06-06-market-relay-design.md`: clarify only if implementation discovers a mismatch.

## Task 1: Scaffold `tqsdk-relay` Workspace Crate

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/tqsdk-relay/Cargo.toml`
- Create: `crates/tqsdk-relay/src/lib.rs`
- Create: `crates/tqsdk-relay/src/main.rs`
- Create: `crates/tqsdk-relay/src/error.rs`
- Create: `crates/tqsdk-relay/README.md`

- [ ] **Step 1: Write the failing workspace check**

Run:

```bash
cargo check -p tqsdk-relay
```

Expected: FAIL with `package ID specification 'tqsdk-relay' did not match any packages`.

- [ ] **Step 2: Add the crate to the workspace**

Edit root `Cargo.toml`:

```toml
[workspace]
members = [
    "crates/tqsdk",
    "crates/tqsdk-core",
    "crates/tqsdk-data",
    "crates/tqsdk-relay",
    "crates/tqsdk-session",
    "crates/tqsdk-wait",
    "crates/tqsdk-stream",
    "crates/tqsdk-task",
]
exclude = ["fuzz"]
default-members = [
    "crates/tqsdk",
    "crates/tqsdk-core",
    "crates/tqsdk-data",
    "crates/tqsdk-relay",
    "crates/tqsdk-session",
    "crates/tqsdk-wait",
    "crates/tqsdk-stream",
    "crates/tqsdk-task",
]
```

- [ ] **Step 3: Create `crates/tqsdk-relay/Cargo.toml`**

```toml
[package]
name = "tqsdk-relay"
version = "0.1.0"
edition = "2024"
rust-version = "1.85"
description = "Optional market relay and cache service for tqsdk-rust"
documentation = "https://docs.rs/tqsdk-relay"
readme = "README.md"
license.workspace = true
repository = "https://github.com/zynthium/tqsdk-rust"

[features]
default = ["server"]
server = ["dep:yawc"]

[dependencies]
futures.workspace = true
serde.workspace = true
serde_json.workspace = true
tokio.workspace = true
tqsdk-core = { path = "../tqsdk-core", version = "0.1.0" }
yawc = { workspace = true, optional = true }

[dev-dependencies]
serde_json.workspace = true
```

- [ ] **Step 4: Create the initial relay library surface**

Create `crates/tqsdk-relay/src/lib.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]
//! Optional market relay and cache service for `tqsdk-rust`.
//!
//! This crate is infrastructure. Existing SDK crates do not depend on it and
//! direct-to-TQ behavior remains the default unless users explicitly point the
//! market endpoint at a relay instance.

pub mod error;

pub use error::{RelayError, RelayResult};
```

Create `crates/tqsdk-relay/src/error.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use std::fmt;

pub type RelayResult<T> = Result<T, RelayError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayError {
    InvalidConfig(String),
    InvalidProtocol(String),
    UnsupportedCommand(String),
    Capacity(String),
    Transport(String),
    Internal(String),
}

impl RelayError {
    #[must_use]
    pub fn invalid_config(message: impl Into<String>) -> Self {
        Self::InvalidConfig(message.into())
    }

    #[must_use]
    pub fn invalid_protocol(message: impl Into<String>) -> Self {
        Self::InvalidProtocol(message.into())
    }

    #[must_use]
    pub fn unsupported_command(aid: impl Into<String>) -> Self {
        Self::UnsupportedCommand(aid.into())
    }
}

impl fmt::Display for RelayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(f, "invalid relay config: {message}"),
            Self::InvalidProtocol(message) => write!(f, "invalid relay protocol: {message}"),
            Self::UnsupportedCommand(aid) => write!(f, "unsupported relay market command: {aid}"),
            Self::Capacity(message) => write!(f, "relay capacity error: {message}"),
            Self::Transport(message) => write!(f, "relay transport error: {message}"),
            Self::Internal(message) => write!(f, "relay internal error: {message}"),
        }
    }
}

impl std::error::Error for RelayError {}
```

Create `crates/tqsdk-relay/src/main.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

fn main() {
    eprintln!("tqsdk-relay server implementation is not started yet");
}
```

- [ ] **Step 5: Create the relay README**

Create `crates/tqsdk-relay/README.md`:

```markdown
# `tqsdk-relay`

`tqsdk-relay` is an optional market relay and cache service for `tqsdk-rust`.

It is not part of the default SDK path. Existing SDK crates continue to connect
directly to Tianqin unless users explicitly configure their market endpoint to a
relay instance.

V1 scope:

- market route only
- futures tick upstream first
- quote / tick / K-line fan-out
- in-memory cache first
- optional disk cache later in the relay crate
- no trade / query / auth proxy
```

- [ ] **Step 6: Run the focused crate check**

Run:

```bash
cargo check -p tqsdk-relay
```

Expected: PASS.

- [ ] **Step 7: Commit the scaffold**

```bash
git add Cargo.toml crates/tqsdk-relay
git commit -m "feat(relay): scaffold optional market relay crate"
```

## Task 2: Add Relay Configuration

**Files:**
- Create: `crates/tqsdk-relay/src/config.rs`
- Modify: `crates/tqsdk-relay/src/lib.rs`
- Test: `crates/tqsdk-relay/tests/config.rs`

- [ ] **Step 1: Write failing config tests**

Create `crates/tqsdk-relay/tests/config.rs`:

```rust
use std::time::Duration;

use tqsdk_relay::{RelayConfig, RelayError};

#[test]
fn default_config_is_memory_only_and_local() {
    let config = RelayConfig::default();

    assert_eq!(config.downstream_listen, "127.0.0.1:7788");
    assert_eq!(config.metrics_listen, "127.0.0.1:7789");
    assert_eq!(config.tick_ring_capacity, 200_000);
    assert_eq!(config.kline_ring_capacity, 10_000);
    assert_eq!(config.bootstrap.max_concurrent_remote_charts, 4);
    assert_eq!(config.bootstrap.min_remote_request_interval, Duration::from_millis(250));
    assert!(config.disk_cache_dir.is_none());
    assert!(config.best_effort_duration_tag);
}

#[test]
fn config_rejects_empty_upstream_market_url() {
    let mut config = RelayConfig::default();
    config.upstream_market_url = String::new();

    let err = config.validate().unwrap_err();

    assert!(matches!(err, RelayError::InvalidConfig(_)));
    assert_eq!(
        err.to_string(),
        "invalid relay config: upstream_market_url must not be empty"
    );
}

#[test]
fn config_rejects_zero_ring_capacity() {
    let mut config = RelayConfig::default();
    config.tick_ring_capacity = 0;

    let err = config.validate().unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid relay config: tick_ring_capacity must be greater than zero"
    );
}
```

Run:

```bash
cargo test -p tqsdk-relay --test config
```

Expected: FAIL because `RelayConfig` does not exist.

- [ ] **Step 2: Implement config types**

Create `crates/tqsdk-relay/src/config.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use std::path::PathBuf;
use std::time::Duration;

use crate::error::{RelayError, RelayResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayConfig {
    pub upstream_market_url: String,
    pub upstream_auth_user: Option<String>,
    pub upstream_auth_pass: Option<String>,
    pub downstream_listen: String,
    pub metrics_listen: String,
    pub futures_universe_refresh: Duration,
    pub tick_ring_capacity: usize,
    pub kline_ring_capacity: usize,
    pub disk_cache_dir: Option<PathBuf>,
    pub bootstrap: BootstrapConfig,
    pub best_effort_duration_tag: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapConfig {
    pub max_concurrent_remote_charts: usize,
    pub min_remote_request_interval: Duration,
    pub per_series_cooldown: Duration,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            upstream_market_url: "wss://openmd.shinnytech.com/t/md/front/mobile".to_string(),
            upstream_auth_user: None,
            upstream_auth_pass: None,
            downstream_listen: "127.0.0.1:7788".to_string(),
            metrics_listen: "127.0.0.1:7789".to_string(),
            futures_universe_refresh: Duration::from_secs(300),
            tick_ring_capacity: 200_000,
            kline_ring_capacity: 10_000,
            disk_cache_dir: None,
            bootstrap: BootstrapConfig::default(),
            best_effort_duration_tag: true,
        }
    }
}

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            max_concurrent_remote_charts: 4,
            min_remote_request_interval: Duration::from_millis(250),
            per_series_cooldown: Duration::from_secs(30),
        }
    }
}

impl RelayConfig {
    pub fn validate(&self) -> RelayResult<()> {
        if self.upstream_market_url.trim().is_empty() {
            return Err(RelayError::invalid_config(
                "upstream_market_url must not be empty",
            ));
        }
        if self.downstream_listen.trim().is_empty() {
            return Err(RelayError::invalid_config(
                "downstream_listen must not be empty",
            ));
        }
        if self.metrics_listen.trim().is_empty() {
            return Err(RelayError::invalid_config("metrics_listen must not be empty"));
        }
        if self.tick_ring_capacity == 0 {
            return Err(RelayError::invalid_config(
                "tick_ring_capacity must be greater than zero",
            ));
        }
        if self.kline_ring_capacity == 0 {
            return Err(RelayError::invalid_config(
                "kline_ring_capacity must be greater than zero",
            ));
        }
        if self.bootstrap.max_concurrent_remote_charts == 0 {
            return Err(RelayError::invalid_config(
                "bootstrap.max_concurrent_remote_charts must be greater than zero",
            ));
        }
        Ok(())
    }
}
```

Modify `crates/tqsdk-relay/src/lib.rs`:

```rust
pub mod config;
pub mod error;

pub use config::{BootstrapConfig, RelayConfig};
pub use error::{RelayError, RelayResult};
```

- [ ] **Step 3: Run config tests**

Run:

```bash
cargo test -p tqsdk-relay --test config
```

Expected: PASS.

- [ ] **Step 4: Commit config**

```bash
git add crates/tqsdk-relay/src/config.rs crates/tqsdk-relay/src/lib.rs crates/tqsdk-relay/tests/config.rs
git commit -m "feat(relay): add relay configuration"
```

## Task 3: Implement Downstream Market Protocol DTOs

**Files:**
- Create: `crates/tqsdk-relay/src/protocol.rs`
- Modify: `crates/tqsdk-relay/src/lib.rs`
- Test: `crates/tqsdk-relay/tests/protocol.rs`

- [ ] **Step 1: Write failing protocol tests**

Create `crates/tqsdk-relay/tests/protocol.rs`:

```rust
use serde_json::json;
use tqsdk_relay::{
    DownstreamCommand, RelayKlineRow, RelayMarketFrame, RelayTickRow, SetChartCommand,
};

#[test]
fn parses_subscribe_quote_command() {
    let command = DownstreamCommand::from_value(json!({
        "aid": "subscribe_quote",
        "ins_list": "SHFE.au2602,DCE.m2609"
    }))
    .unwrap();

    assert_eq!(
        command,
        DownstreamCommand::SubscribeQuote {
            symbols: vec!["SHFE.au2602".to_string(), "DCE.m2609".to_string()]
        }
    );
}

#[test]
fn parses_set_chart_command() {
    let command = DownstreamCommand::from_value(json!({
        "aid": "set_chart",
        "chart_id": "client-chart-1",
        "ins_list": "SHFE.au2602",
        "duration": 60000000000i64,
        "view_width": 64,
        "left_kline_id": 100
    }))
    .unwrap();

    assert_eq!(
        command,
        DownstreamCommand::SetChart(SetChartCommand {
            chart_id: "client-chart-1".to_string(),
            symbols: vec!["SHFE.au2602".to_string()],
            duration_ns: 60_000_000_000,
            view_width: 64,
            left_kline_id: Some(100),
            focus_datetime_ns: None,
            focus_position: None,
        })
    );
}

#[test]
fn rejects_trade_command() {
    let err = DownstreamCommand::from_value(json!({
        "aid": "insert_order"
    }))
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        "unsupported relay market command: insert_order"
    );
}

#[test]
fn encodes_compatible_rtn_data_for_ticks_and_klines() {
    let frame = RelayMarketFrame::rtn_data(vec![
        RelayMarketFrame::tick_update(
            "SHFE.au2602",
            RelayTickRow {
                id: 17,
                datetime: 1_713_660_000_000_000_000,
                last_price: 618.5,
                volume: 200,
                open_interest: 1000,
            },
        ),
        RelayMarketFrame::kline_update(
            "SHFE.au2602",
            60_000_000_000,
            RelayKlineRow {
                id: 42,
                datetime: 1_713_660_000_000_000_000,
                open: 610.0,
                high: 620.0,
                low: 609.0,
                close: 618.5,
                volume: 200,
                open_oi: 900,
                close_oi: 1000,
            },
        ),
    ]);

    assert_eq!(
        frame.into_value(),
        json!({
            "aid": "rtn_data",
            "data": [
                {
                    "ticks": {
                        "SHFE.au2602": {
                            "last_id": 17,
                            "data": {
                                "17": {
                                    "id": 17,
                                    "datetime": 1713660000000000000i64,
                                    "last_price": 618.5,
                                    "volume": 200,
                                    "open_interest": 1000
                                }
                            }
                        }
                    }
                },
                {
                    "klines": {
                        "SHFE.au2602": {
                            "60000000000": {
                                "last_id": 42,
                                "data": {
                                    "42": {
                                        "id": 42,
                                        "datetime": 1713660000000000000i64,
                                        "open": 610.0,
                                        "high": 620.0,
                                        "low": 609.0,
                                        "close": 618.5,
                                        "volume": 200,
                                        "open_oi": 900,
                                        "close_oi": 1000
                                    }
                                }
                            }
                        }
                    }
                }
            ]
        })
    );
}
```

Run:

```bash
cargo test -p tqsdk-relay --test protocol
```

Expected: FAIL because protocol types do not exist.

- [ ] **Step 2: Implement protocol DTOs**

Create `crates/tqsdk-relay/src/protocol.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use serde_json::{Value, json};

use crate::error::{RelayError, RelayResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownstreamCommand {
    SubscribeQuote { symbols: Vec<String> },
    SetChart(SetChartCommand),
    PeekMessage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetChartCommand {
    pub chart_id: String,
    pub symbols: Vec<String>,
    pub duration_ns: i64,
    pub view_width: usize,
    pub left_kline_id: Option<i64>,
    pub focus_datetime_ns: Option<i64>,
    pub focus_position: Option<usize>,
}

impl DownstreamCommand {
    pub fn from_value(value: Value) -> RelayResult<Self> {
        let aid = value
            .get("aid")
            .and_then(Value::as_str)
            .ok_or_else(|| RelayError::invalid_protocol("market command missing string aid"))?;
        match aid {
            "subscribe_quote" => Ok(Self::SubscribeQuote {
                symbols: split_symbols(value.get("ins_list").and_then(Value::as_str).unwrap_or("")),
            }),
            "set_chart" => Ok(Self::SetChart(SetChartCommand {
                chart_id: required_string(&value, "chart_id")?,
                symbols: split_symbols(value.get("ins_list").and_then(Value::as_str).unwrap_or("")),
                duration_ns: required_i64(&value, "duration")?,
                view_width: required_usize(&value, "view_width")?,
                left_kline_id: value.get("left_kline_id").and_then(Value::as_i64),
                focus_datetime_ns: value.get("focus_datetime").and_then(Value::as_i64),
                focus_position: value
                    .get("focus_position")
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok()),
            })),
            "peek_message" => Ok(Self::PeekMessage),
            other => Err(RelayError::unsupported_command(other)),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RelayMarketFrame {
    RtnData(Vec<Value>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelayTickRow {
    pub id: i64,
    pub datetime: i64,
    pub last_price: f64,
    pub volume: i64,
    pub open_interest: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelayKlineRow {
    pub id: i64,
    pub datetime: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: i64,
    pub open_oi: i64,
    pub close_oi: i64,
}

impl RelayMarketFrame {
    #[must_use]
    pub fn rtn_data(data: Vec<Self>) -> Self {
        let values = data.into_iter().map(Self::into_inner_value).collect();
        Self::RtnData(values)
    }

    #[must_use]
    pub fn tick_update(symbol: &str, row: RelayTickRow) -> Self {
        Self::RtnData(vec![json!({
            "ticks": {
                symbol: {
                    "last_id": row.id,
                    "data": {
                        row.id.to_string(): {
                            "id": row.id,
                            "datetime": row.datetime,
                            "last_price": row.last_price,
                            "volume": row.volume,
                            "open_interest": row.open_interest
                        }
                    }
                }
            }
        })])
    }

    #[must_use]
    pub fn kline_update(symbol: &str, duration_ns: i64, row: RelayKlineRow) -> Self {
        Self::RtnData(vec![json!({
            "klines": {
                symbol: {
                    duration_ns.to_string(): {
                        "last_id": row.id,
                        "data": {
                            row.id.to_string(): {
                                "id": row.id,
                                "datetime": row.datetime,
                                "open": row.open,
                                "high": row.high,
                                "low": row.low,
                                "close": row.close,
                                "volume": row.volume,
                                "open_oi": row.open_oi,
                                "close_oi": row.close_oi
                            }
                        }
                    }
                }
            }
        })])
    }

    #[must_use]
    pub fn into_value(self) -> Value {
        match self {
            Self::RtnData(data) => json!({
                "aid": "rtn_data",
                "data": data,
            }),
        }
    }

    fn into_inner_value(self) -> Value {
        match self {
            Self::RtnData(mut values) => {
                if values.len() == 1 {
                    values.remove(0)
                } else {
                    json!({ "data": values })
                }
            }
        }
    }
}

fn split_symbols(ins_list: &str) -> Vec<String> {
    ins_list
        .split(',')
        .map(str::trim)
        .filter(|symbol| !symbol.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn required_string(value: &Value, key: &'static str) -> RelayResult<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| RelayError::invalid_protocol(format!("market command missing {key}")))
}

fn required_i64(value: &Value, key: &'static str) -> RelayResult<i64> {
    value
        .get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| RelayError::invalid_protocol(format!("market command missing {key}")))
}

fn required_usize(value: &Value, key: &'static str) -> RelayResult<usize> {
    let raw = value
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| RelayError::invalid_protocol(format!("market command missing {key}")))?;
    usize::try_from(raw)
        .map_err(|_| RelayError::invalid_protocol(format!("market command {key} is too large")))
}
```

Modify `crates/tqsdk-relay/src/lib.rs`:

```rust
pub mod config;
pub mod error;
pub mod protocol;

pub use config::{BootstrapConfig, RelayConfig};
pub use error::{RelayError, RelayResult};
pub use protocol::{
    DownstreamCommand, RelayKlineRow, RelayMarketFrame, RelayTickRow, SetChartCommand,
};
```

- [ ] **Step 3: Run protocol tests**

Run:

```bash
cargo test -p tqsdk-relay --test protocol
```

Expected: PASS.

- [ ] **Step 4: Commit protocol**

```bash
git add crates/tqsdk-relay/src/protocol.rs crates/tqsdk-relay/src/lib.rs crates/tqsdk-relay/tests/protocol.rs
git commit -m "feat(relay): add market protocol dto"
```

## Task 4: Add In-Memory Tick, Quote, and K-Line Cache

**Files:**
- Create: `crates/tqsdk-relay/src/cache.rs`
- Modify: `crates/tqsdk-relay/src/lib.rs`
- Test: `crates/tqsdk-relay/tests/cache.rs`

- [ ] **Step 1: Write failing cache tests**

Create `crates/tqsdk-relay/tests/cache.rs`:

```rust
use tqsdk_relay::{MarketCache, RelayTickRow};

fn tick(id: i64, datetime: i64, price: f64) -> RelayTickRow {
    RelayTickRow {
        id,
        datetime,
        last_price: price,
        volume: id * 10,
        open_interest: 1000 + id,
    }
}

#[test]
fn tick_ring_retains_latest_rows_per_symbol() {
    let mut cache = MarketCache::new(2, 4);

    cache.push_tick("SHFE.au2602", tick(1, 1_000, 610.0));
    cache.push_tick("SHFE.au2602", tick(2, 2_000, 611.0));
    cache.push_tick("SHFE.au2602", tick(3, 3_000, 612.0));

    let rows = cache.ticks("SHFE.au2602");
    assert_eq!(rows.iter().map(|row| row.id).collect::<Vec<_>>(), vec![2, 3]);
}

#[test]
fn quote_snapshot_is_derived_from_latest_tick() {
    let mut cache = MarketCache::new(4, 4);

    cache.push_tick("SHFE.au2602", tick(1, 1_000, 610.0));
    cache.push_tick("SHFE.au2602", tick(2, 2_000, 611.5));

    let quote = cache.quote("SHFE.au2602").unwrap();
    assert_eq!(quote.instrument_id, "SHFE.au2602");
    assert_eq!(quote.last_price, 611.5);
    assert_eq!(quote.volume, 20);
    assert_eq!(quote.open_interest, 1002);
}

#[test]
fn unknown_symbol_returns_no_quote_or_ticks() {
    let cache = MarketCache::new(4, 4);

    assert!(cache.quote("SHFE.missing").is_none());
    assert!(cache.ticks("SHFE.missing").is_empty());
}
```

Run:

```bash
cargo test -p tqsdk-relay --test cache
```

Expected: FAIL because `MarketCache` does not exist.

- [ ] **Step 2: Implement `MarketCache`**

Create `crates/tqsdk-relay/src/cache.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{HashMap, VecDeque};

use tqsdk_core::Quote;

use crate::protocol::RelayTickRow;

#[derive(Debug, Clone)]
pub struct MarketCache {
    tick_capacity: usize,
    kline_capacity: usize,
    ticks: HashMap<String, VecDeque<RelayTickRow>>,
    quotes: HashMap<String, Quote>,
}

impl MarketCache {
    #[must_use]
    pub fn new(tick_capacity: usize, kline_capacity: usize) -> Self {
        assert!(tick_capacity > 0, "tick_capacity must be greater than zero");
        assert!(kline_capacity > 0, "kline_capacity must be greater than zero");
        Self {
            tick_capacity,
            kline_capacity,
            ticks: HashMap::new(),
            quotes: HashMap::new(),
        }
    }

    pub fn push_tick(&mut self, symbol: impl AsRef<str>, row: RelayTickRow) {
        let symbol = symbol.as_ref();
        let rows = self.ticks.entry(symbol.to_string()).or_default();
        rows.push_back(row.clone());
        while rows.len() > self.tick_capacity {
            let _ = rows.pop_front();
        }
        self.quotes.insert(symbol.to_string(), quote_from_tick(symbol, &row));
    }

    #[must_use]
    pub fn ticks(&self, symbol: impl AsRef<str>) -> Vec<RelayTickRow> {
        self.ticks
            .get(symbol.as_ref())
            .map(|rows| rows.iter().cloned().collect())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn quote(&self, symbol: impl AsRef<str>) -> Option<Quote> {
        self.quotes.get(symbol.as_ref()).cloned()
    }

    #[must_use]
    pub fn kline_capacity(&self) -> usize {
        self.kline_capacity
    }
}

fn quote_from_tick(symbol: &str, row: &RelayTickRow) -> Quote {
    Quote {
        instrument_id: symbol.to_string(),
        last_price: row.last_price,
        volume: row.volume,
        open_interest: row.open_interest,
        datetime: row.datetime.to_string(),
        ..Quote::default()
    }
}
```

Modify `crates/tqsdk-relay/src/lib.rs`:

```rust
pub mod cache;
pub mod config;
pub mod error;
pub mod protocol;

pub use cache::MarketCache;
pub use config::{BootstrapConfig, RelayConfig};
pub use error::{RelayError, RelayResult};
pub use protocol::{
    DownstreamCommand, RelayKlineRow, RelayMarketFrame, RelayTickRow, SetChartCommand,
};
```

- [ ] **Step 3: Run cache tests**

Run:

```bash
cargo test -p tqsdk-relay --test cache
```

Expected: PASS.

- [ ] **Step 4: Commit cache**

```bash
git add crates/tqsdk-relay/src/cache.rs crates/tqsdk-relay/src/lib.rs crates/tqsdk-relay/tests/cache.rs
git commit -m "feat(relay): add in-memory market cache"
```

## Task 5: Implement Fixed-Duration K-Line Synthesis

**Files:**
- Create: `crates/tqsdk-relay/src/kline.rs`
- Modify: `crates/tqsdk-relay/src/lib.rs`
- Test: `crates/tqsdk-relay/tests/kline.rs`

- [ ] **Step 1: Write failing K-line synthesis tests**

Create `crates/tqsdk-relay/tests/kline.rs`:

```rust
use tqsdk_relay::{KlineSynthesis, RelayTickRow};

fn tick(id: i64, datetime: i64, price: f64, volume: i64, oi: i64) -> RelayTickRow {
    RelayTickRow {
        id,
        datetime,
        last_price: price,
        volume,
        open_interest: oi,
    }
}

#[test]
fn fixed_window_is_start_inclusive_end_exclusive() {
    let mut synth = KlineSynthesis::new("SHFE.au2602", 60_000_000_000);

    synth.push_tick(tick(1, 0, 610.0, 10, 100)).unwrap();
    synth
        .push_tick(tick(2, 59_999_999_999, 612.0, 15, 110))
        .unwrap();
    let completed = synth
        .push_tick(tick(3, 60_000_000_000, 620.0, 20, 120))
        .unwrap();

    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].datetime, 0);
    assert_eq!(completed[0].open, 610.0);
    assert_eq!(completed[0].high, 612.0);
    assert_eq!(completed[0].low, 610.0);
    assert_eq!(completed[0].close, 612.0);

    let current = synth.current_bar().unwrap();
    assert_eq!(current.datetime, 60_000_000_000);
    assert_eq!(current.open, 620.0);
    assert_eq!(current.close, 620.0);
}

#[test]
fn completed_bar_volume_uses_tick_volume_delta_inside_window() {
    let mut synth = KlineSynthesis::new("SHFE.au2602", 60_000_000_000);

    synth.push_tick(tick(1, 0, 610.0, 100, 1000)).unwrap();
    synth.push_tick(tick(2, 30_000_000_000, 612.0, 140, 1005)).unwrap();
    let completed = synth
        .push_tick(tick(3, 60_000_000_000, 611.0, 155, 1010))
        .unwrap();

    assert_eq!(completed[0].volume, 40);
    assert_eq!(completed[0].open_oi, 1000);
    assert_eq!(completed[0].close_oi, 1005);
}

#[test]
fn rejects_non_positive_duration() {
    let err = KlineSynthesis::try_new("SHFE.au2602", 0).unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid relay config: kline duration_ns must be greater than zero"
    );
}
```

Run:

```bash
cargo test -p tqsdk-relay --test kline
```

Expected: FAIL because `KlineSynthesis` does not exist.

- [ ] **Step 2: Implement K-line synthesis**

Create `crates/tqsdk-relay/src/kline.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use crate::error::{RelayError, RelayResult};
use crate::protocol::{RelayKlineRow, RelayTickRow};

#[derive(Debug, Clone)]
pub struct KlineSynthesis {
    symbol: String,
    duration_ns: i64,
    current: Option<MutableKline>,
    next_id: i64,
}

#[derive(Debug, Clone)]
struct MutableKline {
    row: RelayKlineRow,
    first_volume: i64,
    last_volume: i64,
}

impl KlineSynthesis {
    #[must_use]
    pub fn new(symbol: impl Into<String>, duration_ns: i64) -> Self {
        Self::try_new(symbol, duration_ns)
            .expect("KlineSynthesis::new requires positive duration_ns")
    }

    pub fn try_new(symbol: impl Into<String>, duration_ns: i64) -> RelayResult<Self> {
        if duration_ns <= 0 {
            return Err(RelayError::invalid_config(
                "kline duration_ns must be greater than zero",
            ));
        }
        Ok(Self {
            symbol: symbol.into(),
            duration_ns,
            current: None,
            next_id: 0,
        })
    }

    pub fn push_tick(&mut self, tick: RelayTickRow) -> RelayResult<Vec<RelayKlineRow>> {
        let start = window_start(tick.datetime, self.duration_ns);
        let mut completed = Vec::new();

        match self.current.take() {
            None => {
                self.current = Some(self.new_bar(start, &tick));
            }
            Some(mut current) if current.row.datetime == start => {
                merge_tick(&mut current, &tick);
                self.current = Some(current);
            }
            Some(current) => {
                completed.push(finalize(current));
                self.current = Some(self.new_bar(start, &tick));
            }
        }

        Ok(completed)
    }

    #[must_use]
    pub fn current_bar(&self) -> Option<RelayKlineRow> {
        self.current.as_ref().map(|current| finalize(current.clone()))
    }

    fn new_bar(&mut self, start: i64, tick: &RelayTickRow) -> MutableKline {
        let row = RelayKlineRow {
            id: self.next_id,
            datetime: start,
            open: tick.last_price,
            high: tick.last_price,
            low: tick.last_price,
            close: tick.last_price,
            volume: 0,
            open_oi: tick.open_interest,
            close_oi: tick.open_interest,
        };
        self.next_id += 1;
        MutableKline {
            row,
            first_volume: tick.volume,
            last_volume: tick.volume,
        }
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub fn duration_ns(&self) -> i64 {
        self.duration_ns
    }
}

fn window_start(datetime: i64, duration_ns: i64) -> i64 {
    datetime.div_euclid(duration_ns) * duration_ns
}

fn merge_tick(current: &mut MutableKline, tick: &RelayTickRow) {
    current.row.high = current.row.high.max(tick.last_price);
    current.row.low = current.row.low.min(tick.last_price);
    current.row.close = tick.last_price;
    current.row.close_oi = tick.open_interest;
    current.last_volume = tick.volume;
}

fn finalize(mut current: MutableKline) -> RelayKlineRow {
    current.row.volume = current.last_volume.saturating_sub(current.first_volume);
    current.row
}
```

Modify `crates/tqsdk-relay/src/lib.rs`:

```rust
pub mod cache;
pub mod config;
pub mod error;
pub mod kline;
pub mod protocol;

pub use cache::MarketCache;
pub use config::{BootstrapConfig, RelayConfig};
pub use error::{RelayError, RelayResult};
pub use kline::KlineSynthesis;
pub use protocol::{
    DownstreamCommand, RelayKlineRow, RelayMarketFrame, RelayTickRow, SetChartCommand,
};
```

- [ ] **Step 3: Run K-line tests**

Run:

```bash
cargo test -p tqsdk-relay --test kline
```

Expected: PASS.

- [ ] **Step 4: Commit K-line synthesis**

```bash
git add crates/tqsdk-relay/src/kline.rs crates/tqsdk-relay/src/lib.rs crates/tqsdk-relay/tests/kline.rs
git commit -m "feat(relay): synthesize fixed-duration klines"
```

## Task 6: Add Interest Registry and Chart ID Mapping

**Files:**
- Create: `crates/tqsdk-relay/src/interest.rs`
- Modify: `crates/tqsdk-relay/src/lib.rs`
- Test: `crates/tqsdk-relay/tests/interest.rs`

- [ ] **Step 1: Write failing interest tests**

Create `crates/tqsdk-relay/tests/interest.rs`:

```rust
use tqsdk_relay::{ClientId, InterestRegistry, SetChartCommand};

fn chart(chart_id: &str) -> SetChartCommand {
    SetChartCommand {
        chart_id: chart_id.to_string(),
        symbols: vec!["SHFE.au2602".to_string()],
        duration_ns: 60_000_000_000,
        view_width: 64,
        left_kline_id: None,
        focus_datetime_ns: None,
        focus_position: None,
    }
}

#[test]
fn same_downstream_chart_id_is_isolated_by_client() {
    let mut registry = InterestRegistry::default();
    let client_a = ClientId::new(1);
    let client_b = ClientId::new(2);

    let source_a = registry.set_chart(client_a, chart("chart-1"));
    let source_b = registry.set_chart(client_b, chart("chart-1"));

    assert_eq!(source_a, source_b);
    assert_eq!(registry.downstream_chart_id(client_a, &source_a), Some("chart-1"));
    assert_eq!(registry.downstream_chart_id(client_b, &source_b), Some("chart-1"));
    assert_eq!(registry.chart_interest_count(&source_a), 2);
}

#[test]
fn removing_one_client_keeps_shared_source_for_other_client() {
    let mut registry = InterestRegistry::default();
    let source = registry.set_chart(ClientId::new(1), chart("chart-1"));
    registry.set_chart(ClientId::new(2), chart("chart-1"));

    registry.remove_client(ClientId::new(1));

    assert_eq!(registry.chart_interest_count(&source), 1);
    assert!(registry.downstream_chart_id(ClientId::new(1), &source).is_none());
    assert_eq!(registry.downstream_chart_id(ClientId::new(2), &source), Some("chart-1"));
}

#[test]
fn quote_symbols_are_tracked_per_client() {
    let mut registry = InterestRegistry::default();
    registry.set_quotes(ClientId::new(1), vec!["SHFE.au2602".to_string()]);
    registry.set_quotes(
        ClientId::new(2),
        vec!["SHFE.au2602".to_string(), "DCE.m2609".to_string()],
    );

    assert_eq!(registry.quote_interest_count("SHFE.au2602"), 2);
    assert_eq!(registry.quote_interest_count("DCE.m2609"), 1);
}
```

Run:

```bash
cargo test -p tqsdk-relay --test interest
```

Expected: FAIL because `InterestRegistry` does not exist.

- [ ] **Step 2: Implement interest registry**

Create `crates/tqsdk-relay/src/interest.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeSet, HashMap};

use crate::protocol::SetChartCommand;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ClientId(u64);

impl ClientId {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SourceKey {
    pub symbols: Vec<String>,
    pub duration_ns: i64,
    pub view_width: usize,
}

#[derive(Debug, Default)]
pub struct InterestRegistry {
    client_quotes: HashMap<ClientId, BTreeSet<String>>,
    chart_mappings: HashMap<(ClientId, String), SourceKey>,
    reverse_chart_ids: HashMap<(ClientId, SourceKey), String>,
}

impl InterestRegistry {
    pub fn set_quotes(&mut self, client_id: ClientId, symbols: Vec<String>) {
        self.client_quotes
            .insert(client_id, symbols.into_iter().collect());
    }

    pub fn set_chart(&mut self, client_id: ClientId, command: SetChartCommand) -> SourceKey {
        let mut symbols = command.symbols;
        symbols.sort();
        symbols.dedup();
        let source = SourceKey {
            symbols,
            duration_ns: command.duration_ns,
            view_width: command.view_width,
        };
        self.chart_mappings
            .insert((client_id, command.chart_id.clone()), source.clone());
        self.reverse_chart_ids
            .insert((client_id, source.clone()), command.chart_id);
        source
    }

    pub fn remove_client(&mut self, client_id: ClientId) {
        self.client_quotes.remove(&client_id);
        self.chart_mappings
            .retain(|(mapped_client, _), _| *mapped_client != client_id);
        self.reverse_chart_ids
            .retain(|(mapped_client, _), _| *mapped_client != client_id);
    }

    #[must_use]
    pub fn quote_interest_count(&self, symbol: &str) -> usize {
        self.client_quotes
            .values()
            .filter(|symbols| symbols.contains(symbol))
            .count()
    }

    #[must_use]
    pub fn chart_interest_count(&self, source: &SourceKey) -> usize {
        self.reverse_chart_ids
            .keys()
            .filter(|(_, key)| key == source)
            .count()
    }

    #[must_use]
    pub fn downstream_chart_id(&self, client_id: ClientId, source: &SourceKey) -> Option<&str> {
        self.reverse_chart_ids
            .get(&(client_id, source.clone()))
            .map(String::as_str)
    }
}
```

Modify `crates/tqsdk-relay/src/lib.rs`:

```rust
pub mod cache;
pub mod config;
pub mod error;
pub mod interest;
pub mod kline;
pub mod protocol;

pub use cache::MarketCache;
pub use config::{BootstrapConfig, RelayConfig};
pub use error::{RelayError, RelayResult};
pub use interest::{ClientId, InterestRegistry, SourceKey};
pub use kline::KlineSynthesis;
pub use protocol::{
    DownstreamCommand, RelayKlineRow, RelayMarketFrame, RelayTickRow, SetChartCommand,
};
```

- [ ] **Step 3: Run interest tests**

Run:

```bash
cargo test -p tqsdk-relay --test interest
```

Expected: PASS.

- [ ] **Step 4: Commit interest registry**

```bash
git add crates/tqsdk-relay/src/interest.rs crates/tqsdk-relay/src/lib.rs crates/tqsdk-relay/tests/interest.rs
git commit -m "feat(relay): track downstream market interests"
```

## Task 7: Add Bootstrap / Resync Queue Limits

**Files:**
- Create: `crates/tqsdk-relay/src/bootstrap.rs`
- Modify: `crates/tqsdk-relay/src/lib.rs`
- Test: `crates/tqsdk-relay/tests/bootstrap.rs`

- [ ] **Step 1: Write failing bootstrap queue tests**

Create `crates/tqsdk-relay/tests/bootstrap.rs`:

```rust
use std::time::{Duration, Instant};

use tqsdk_relay::{BootstrapQueue, BootstrapRequest, SourceKey};

fn request(symbol: &str, duration_ns: i64, start_id: i64, end_id: i64) -> BootstrapRequest {
    BootstrapRequest {
        source: SourceKey {
            symbols: vec![symbol.to_string()],
            duration_ns,
            view_width: 64,
        },
        start_id,
        end_id,
    }
}

#[test]
fn queue_coalesces_overlapping_requests() {
    let mut queue = BootstrapQueue::new(2, Duration::from_millis(100));

    queue.enqueue(request("SHFE.au2602", 60_000_000_000, 10, 20));
    queue.enqueue(request("SHFE.au2602", 60_000_000_000, 15, 30));

    assert_eq!(queue.len(), 1);
    let next = queue.poll_ready(Instant::now()).unwrap();
    assert_eq!(next.start_id, 10);
    assert_eq!(next.end_id, 30);
}

#[test]
fn queue_respects_concurrency_limit() {
    let mut queue = BootstrapQueue::new(1, Duration::from_millis(0));
    let now = Instant::now();

    queue.enqueue(request("SHFE.au2602", 60_000_000_000, 10, 20));
    queue.enqueue(request("DCE.m2609", 60_000_000_000, 10, 20));

    assert!(queue.poll_ready(now).is_some());
    assert!(queue.poll_ready(now).is_none());
    queue.complete_one();
    assert!(queue.poll_ready(now).is_some());
}

#[test]
fn queue_respects_min_request_interval() {
    let mut queue = BootstrapQueue::new(2, Duration::from_millis(100));
    let now = Instant::now();

    queue.enqueue(request("SHFE.au2602", 60_000_000_000, 10, 20));
    queue.enqueue(request("DCE.m2609", 60_000_000_000, 10, 20));

    assert!(queue.poll_ready(now).is_some());
    queue.complete_one();
    assert!(queue.poll_ready(now + Duration::from_millis(50)).is_none());
    assert!(queue.poll_ready(now + Duration::from_millis(100)).is_some());
}
```

Run:

```bash
cargo test -p tqsdk-relay --test bootstrap
```

Expected: FAIL because `BootstrapQueue` does not exist.

- [ ] **Step 2: Implement bootstrap queue**

Create `crates/tqsdk-relay/src/bootstrap.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::interest::SourceKey;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapRequest {
    pub source: SourceKey,
    pub start_id: i64,
    pub end_id: i64,
}

#[derive(Debug)]
pub struct BootstrapQueue {
    max_inflight: usize,
    min_interval: Duration,
    inflight: usize,
    last_start: Option<Instant>,
    pending: VecDeque<BootstrapRequest>,
}

impl BootstrapQueue {
    #[must_use]
    pub fn new(max_inflight: usize, min_interval: Duration) -> Self {
        assert!(max_inflight > 0, "max_inflight must be greater than zero");
        Self {
            max_inflight,
            min_interval,
            inflight: 0,
            last_start: None,
            pending: VecDeque::new(),
        }
    }

    pub fn enqueue(&mut self, request: BootstrapRequest) {
        if let Some(existing) = self
            .pending
            .iter_mut()
            .find(|existing| existing.source == request.source)
        {
            existing.start_id = existing.start_id.min(request.start_id);
            existing.end_id = existing.end_id.max(request.end_id);
            return;
        }
        self.pending.push_back(request);
    }

    pub fn poll_ready(&mut self, now: Instant) -> Option<BootstrapRequest> {
        if self.inflight >= self.max_inflight {
            return None;
        }
        if let Some(last_start) = self.last_start
            && now.duration_since(last_start) < self.min_interval
        {
            return None;
        }
        let request = self.pending.pop_front()?;
        self.inflight += 1;
        self.last_start = Some(now);
        Some(request)
    }

    pub fn complete_one(&mut self) {
        self.inflight = self.inflight.saturating_sub(1);
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    #[must_use]
    pub fn inflight(&self) -> usize {
        self.inflight
    }
}
```

Modify `crates/tqsdk-relay/src/lib.rs`:

```rust
pub mod bootstrap;
pub mod cache;
pub mod config;
pub mod error;
pub mod interest;
pub mod kline;
pub mod protocol;

pub use bootstrap::{BootstrapQueue, BootstrapRequest};
pub use cache::MarketCache;
pub use config::{BootstrapConfig, RelayConfig};
pub use error::{RelayError, RelayResult};
pub use interest::{ClientId, InterestRegistry, SourceKey};
pub use kline::KlineSynthesis;
pub use protocol::{
    DownstreamCommand, RelayKlineRow, RelayMarketFrame, RelayTickRow, SetChartCommand,
};
```

- [ ] **Step 3: Run bootstrap tests**

Run:

```bash
cargo test -p tqsdk-relay --test bootstrap
```

Expected: PASS.

- [ ] **Step 4: Commit bootstrap queue**

```bash
git add crates/tqsdk-relay/src/bootstrap.rs crates/tqsdk-relay/src/lib.rs crates/tqsdk-relay/tests/bootstrap.rs
git commit -m "feat(relay): limit remote bootstrap requests"
```

## Task 8: Add Relay Engine With Fake Upstream Source

**Files:**
- Create: `crates/tqsdk-relay/src/upstream.rs`
- Create: `crates/tqsdk-relay/src/engine.rs`
- Modify: `crates/tqsdk-relay/src/lib.rs`
- Test: `crates/tqsdk-relay/tests/integration.rs`

- [ ] **Step 1: Write failing engine integration tests**

Create `crates/tqsdk-relay/tests/integration.rs`:

```rust
use tqsdk_relay::{
    ClientId, DownstreamCommand, RelayEngine, RelayTickRow, SetChartCommand, SourceKey,
};

fn tick(id: i64, datetime: i64, price: f64) -> RelayTickRow {
    RelayTickRow {
        id,
        datetime,
        last_price: price,
        volume: id * 10,
        open_interest: 1000 + id,
    }
}

fn chart_command(chart_id: &str) -> DownstreamCommand {
    DownstreamCommand::SetChart(SetChartCommand {
        chart_id: chart_id.to_string(),
        symbols: vec!["SHFE.au2602".to_string()],
        duration_ns: 60_000_000_000,
        view_width: 64,
        left_kline_id: None,
        focus_datetime_ns: None,
        focus_position: None,
    })
}

#[test]
fn relay_engine_fans_out_quotes_from_ticks() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let client = ClientId::new(1);

    engine
        .handle_command(
            client,
            DownstreamCommand::SubscribeQuote {
                symbols: vec!["SHFE.au2602".to_string()],
            },
        )
        .unwrap();

    let frames = engine.ingest_tick("SHFE.au2602", tick(1, 1_000, 610.0)).unwrap();

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].client_id, client);
    assert_eq!(frames[0].payload["aid"], "rtn_data");
    assert_eq!(frames[0].payload["data"][0]["quotes"]["SHFE.au2602"]["last_price"], 610.0);
}

#[test]
fn relay_engine_rewrites_chart_payload_to_downstream_chart_id() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let client = ClientId::new(1);

    engine.handle_command(client, chart_command("client-chart")).unwrap();

    engine.ingest_tick("SHFE.au2602", tick(1, 0, 610.0)).unwrap();
    let frames = engine
        .ingest_tick("SHFE.au2602", tick(2, 60_000_000_000, 620.0))
        .unwrap();

    let chart_frame = frames
        .iter()
        .find(|frame| frame.payload["data"][0].get("charts").is_some())
        .expect("completed bar should emit chart metadata for downstream chart");
    assert_eq!(
        chart_frame.payload["data"][0]["charts"]["client-chart"]["right_id"],
        0
    );
}

#[test]
fn relay_engine_tracks_bootstrap_request_without_subscribing_remote_kline_immediately() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let client = ClientId::new(1);

    engine.handle_command(client, chart_command("client-chart")).unwrap();

    let source = SourceKey {
        symbols: vec!["SHFE.au2602".to_string()],
        duration_ns: 60_000_000_000,
        view_width: 64,
    };
    assert_eq!(engine.bootstrap_pending_len(), 1);
    assert_eq!(engine.interests().chart_interest_count(&source), 1);
}
```

Run:

```bash
cargo test -p tqsdk-relay --test integration
```

Expected: FAIL because `RelayEngine` does not exist.

- [ ] **Step 2: Add upstream source traits**

Create `crates/tqsdk-relay/src/upstream.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use crate::protocol::RelayTickRow;

#[derive(Debug, Clone, PartialEq)]
pub struct UpstreamTick {
    pub symbol: String,
    pub row: RelayTickRow,
}

pub trait UpstreamTickSource {
    fn next_tick(&mut self) -> impl std::future::Future<Output = Option<UpstreamTick>> + Send + '_;
}

#[derive(Debug, Default)]
pub struct FakeUpstreamTickSource {
    ticks: std::collections::VecDeque<UpstreamTick>,
}

impl FakeUpstreamTickSource {
    pub fn push(&mut self, tick: UpstreamTick) {
        self.ticks.push_back(tick);
    }
}

impl UpstreamTickSource for FakeUpstreamTickSource {
    async fn next_tick(&mut self) -> Option<UpstreamTick> {
        self.ticks.pop_front()
    }
}
```

- [ ] **Step 3: Implement relay engine**

Create `crates/tqsdk-relay/src/engine.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashMap;
use std::time::Duration;

use serde_json::{Value, json};

use crate::bootstrap::{BootstrapQueue, BootstrapRequest};
use crate::cache::MarketCache;
use crate::error::RelayResult;
use crate::interest::{ClientId, InterestRegistry, SourceKey};
use crate::kline::KlineSynthesis;
use crate::protocol::{DownstreamCommand, RelayMarketFrame, RelayTickRow};

#[derive(Debug, Clone, PartialEq)]
pub struct DownstreamFrame {
    pub client_id: ClientId,
    pub payload: Value,
}

#[derive(Debug)]
pub struct RelayEngine {
    cache: MarketCache,
    interests: InterestRegistry,
    bootstrap: BootstrapQueue,
    klines: HashMap<SourceKey, KlineSynthesis>,
}

impl RelayEngine {
    #[must_use]
    pub fn new_memory_only(tick_capacity: usize, kline_capacity: usize) -> Self {
        Self {
            cache: MarketCache::new(tick_capacity, kline_capacity),
            interests: InterestRegistry::default(),
            bootstrap: BootstrapQueue::new(4, Duration::from_millis(250)),
            klines: HashMap::new(),
        }
    }

    pub fn handle_command(
        &mut self,
        client_id: ClientId,
        command: DownstreamCommand,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        match command {
            DownstreamCommand::SubscribeQuote { symbols } => {
                self.interests.set_quotes(client_id, symbols);
                Ok(Vec::new())
            }
            DownstreamCommand::SetChart(command) => {
                let source = self.interests.set_chart(client_id, command);
                self.bootstrap.enqueue(BootstrapRequest {
                    source,
                    start_id: i64::MIN,
                    end_id: i64::MAX,
                });
                Ok(Vec::new())
            }
            DownstreamCommand::PeekMessage => Ok(Vec::new()),
        }
    }

    pub fn ingest_tick(
        &mut self,
        symbol: impl AsRef<str>,
        row: RelayTickRow,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        let symbol = symbol.as_ref();
        self.cache.push_tick(symbol, row.clone());
        let mut frames = self.quote_frames(symbol);
        frames.extend(self.kline_frames(symbol, row)?);
        Ok(frames)
    }

    #[must_use]
    pub fn interests(&self) -> &InterestRegistry {
        &self.interests
    }

    #[must_use]
    pub fn bootstrap_pending_len(&self) -> usize {
        self.bootstrap.len()
    }

    fn quote_frames(&self, symbol: &str) -> Vec<DownstreamFrame> {
        let Some(quote) = self.cache.quote(symbol) else {
            return Vec::new();
        };
        let payload = RelayMarketFrame::rtn_data(vec![RelayMarketFrame::RtnData(vec![json!({
            "quotes": {
                symbol: {
                    "instrument_id": quote.instrument_id,
                    "datetime": quote.datetime,
                    "last_price": quote.last_price,
                    "volume": quote.volume,
                    "open_interest": quote.open_interest
                }
            }
        })])])
        .into_value();

        self.interests
            .quote_clients(symbol)
            .into_iter()
            .map(|client_id| DownstreamFrame {
                client_id,
                payload: payload.clone(),
            })
            .collect()
    }

    fn kline_frames(
        &mut self,
        symbol: &str,
        row: RelayTickRow,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        let sources = self.interests.sources_for_symbol(symbol);
        let mut frames = Vec::new();
        for source in sources {
            if source.duration_ns <= 0 {
                continue;
            }
            let synthesizer = self.klines.entry(source.clone()).or_insert_with(|| {
                KlineSynthesis::new(symbol.to_string(), source.duration_ns)
            });
            for completed in synthesizer.push_tick(row.clone())? {
                let kline_payload = RelayMarketFrame::rtn_data(vec![RelayMarketFrame::kline_update(
                    symbol,
                    source.duration_ns,
                    completed.clone(),
                )])
                .into_value();
                for client_id in self.interests.chart_clients(&source) {
                    frames.push(DownstreamFrame {
                        client_id,
                        payload: kline_payload.clone(),
                    });
                    if let Some(chart_id) = self.interests.downstream_chart_id(client_id, &source) {
                        frames.push(DownstreamFrame {
                            client_id,
                            payload: chart_payload(chart_id, completed.id),
                        });
                    }
                }
            }
        }
        Ok(frames)
    }
}

fn chart_payload(chart_id: &str, right_id: i64) -> Value {
    json!({
        "aid": "rtn_data",
        "data": [
            {
                "charts": {
                    chart_id: {
                        "left_id": right_id,
                        "right_id": right_id,
                        "more_data": false,
                        "ready": true
                    }
                }
            }
        ]
    })
}
```

- [ ] **Step 4: Extend interest registry query methods**

Modify `crates/tqsdk-relay/src/interest.rs` by adding these methods to `impl InterestRegistry`:

```rust
#[must_use]
pub fn quote_clients(&self, symbol: &str) -> Vec<ClientId> {
    self.client_quotes
        .iter()
        .filter_map(|(client_id, symbols)| symbols.contains(symbol).then_some(*client_id))
        .collect()
}

#[must_use]
pub fn sources_for_symbol(&self, symbol: &str) -> Vec<SourceKey> {
    let mut sources: Vec<_> = self
        .reverse_chart_ids
        .keys()
        .filter_map(|(_, source)| {
            source
                .symbols
                .iter()
                .any(|candidate| candidate == symbol)
                .then_some(source.clone())
        })
        .collect();
    sources.sort_by(|a, b| {
        a.symbols
            .cmp(&b.symbols)
            .then_with(|| a.duration_ns.cmp(&b.duration_ns))
            .then_with(|| a.view_width.cmp(&b.view_width))
    });
    sources.dedup();
    sources
}

#[must_use]
pub fn chart_clients(&self, source: &SourceKey) -> Vec<ClientId> {
    self.reverse_chart_ids
        .keys()
        .filter_map(|(client_id, key)| (key == source).then_some(*client_id))
        .collect()
}
```

Modify `crates/tqsdk-relay/src/lib.rs`:

```rust
pub mod bootstrap;
pub mod cache;
pub mod config;
pub mod engine;
pub mod error;
pub mod interest;
pub mod kline;
pub mod protocol;
pub mod upstream;

pub use bootstrap::{BootstrapQueue, BootstrapRequest};
pub use cache::MarketCache;
pub use config::{BootstrapConfig, RelayConfig};
pub use engine::{DownstreamFrame, RelayEngine};
pub use error::{RelayError, RelayResult};
pub use interest::{ClientId, InterestRegistry, SourceKey};
pub use kline::KlineSynthesis;
pub use protocol::{
    DownstreamCommand, RelayKlineRow, RelayMarketFrame, RelayTickRow, SetChartCommand,
};
pub use upstream::{FakeUpstreamTickSource, UpstreamTick, UpstreamTickSource};
```

- [ ] **Step 5: Run engine integration tests**

Run:

```bash
cargo test -p tqsdk-relay --test integration
```

Expected: PASS.

- [ ] **Step 6: Commit relay engine**

```bash
git add crates/tqsdk-relay/src/engine.rs crates/tqsdk-relay/src/upstream.rs crates/tqsdk-relay/src/interest.rs crates/tqsdk-relay/src/lib.rs crates/tqsdk-relay/tests/integration.rs
git commit -m "feat(relay): add market relay engine"
```

## Task 9: Add SDK Opt-In Market Relay Endpoint Helpers

**Files:**
- Modify: `crates/tqsdk-session/src/builder.rs`
- Modify: `crates/tqsdk-wait/src/builder.rs` only if macro forwarding is insufficient
- Modify: `crates/tqsdk-stream/src/builder.rs` only if macro forwarding is insufficient
- Modify: `crates/tqsdk/src/lib.rs`
- Test: `crates/tqsdk-session/tests/session_builder.rs`
- Test: `crates/tqsdk-wait/src/builder.rs`
- Test: `crates/tqsdk-stream/src/builder.rs`
- Test: `crates/tqsdk/tests/facade_contract.rs`

- [ ] **Step 1: Write failing session builder test**

Append to `crates/tqsdk-session/tests/session_builder.rs`:

```rust
#[test]
fn builder_accepts_explicit_market_relay_url_without_enabling_other_routes() {
    let builder = SessionClientBuilder::new("user", "pass")
        .futures_market()
        .market_relay("ws://127.0.0.1:7788/market");

    let endpoints = builder.endpoints();
    assert_eq!(
        endpoints.market_url.as_deref(),
        Some("ws://127.0.0.1:7788/market")
    );
    assert_eq!(endpoints.trade_url, None);
    assert_eq!(endpoints.query_url, None);
}
```

Run:

```bash
cargo test -p tqsdk-session --test session_builder builder_accepts_explicit_market_relay_url_without_enabling_other_routes
```

Expected: FAIL because `market_relay` does not exist.

- [ ] **Step 2: Add session builder helpers and macro forwarders**

Modify `SessionClientBuilder` in `crates/tqsdk-session/src/builder.rs`:

```rust
#[must_use]
pub fn market_url(mut self, market_url: impl Into<String>) -> Self {
    self.endpoints = self.endpoints.with_market_url(market_url);
    self
}

#[must_use]
pub fn market_relay(self, relay_url: impl Into<String>) -> Self {
    self.market_url(relay_url)
}
```

Add the same forwarders to `__tqsdk_impl_session_builder_forwarders!()`:

```rust
#[must_use]
pub fn market_url(mut self, market_url: impl Into<String>) -> Self {
    self.inner = self.inner.market_url(market_url);
    self
}

#[must_use]
pub fn market_relay(mut self, relay_url: impl Into<String>) -> Self {
    self.inner = self.inner.market_relay(relay_url);
    self
}
```

- [ ] **Step 3: Add wait/stream builder forwarding tests**

Append to `crates/tqsdk-wait/src/builder.rs` tests:

```rust
#[test]
fn market_relay_forwards_to_inner_session_builder() {
    let builder = TqApiBuilder::new("demo-user", "demo-pass")
        .market_relay("ws://127.0.0.1:7788/market");

    assert_eq!(
        builder.inner.endpoints().market_url.as_deref(),
        Some("ws://127.0.0.1:7788/market")
    );
}
```

Append to `crates/tqsdk-stream/src/builder.rs` tests:

```rust
#[test]
fn market_relay_forwards_to_inner_session_builder() {
    let builder = TqStreamBuilder::new("demo-user", "demo-pass")
        .market_relay("ws://127.0.0.1:7788/market");

    assert_eq!(
        builder.inner.endpoints().market_url.as_deref(),
        Some("ws://127.0.0.1:7788/market")
    );
}
```

- [ ] **Step 4: Add facade `TqBuilder::market_relay(...)`**

Modify `crates/tqsdk/src/lib.rs`:

```rust
pub struct TqBuilder {
    auth: Option<Auth>,
    query_enabled: bool,
    trade_targets: Vec<TradeTarget>,
    market_mode: MarketMode,
    market_url: Option<String>,
    backtest: Option<BacktestConfig>,
    quote_symbols: Vec<String>,
    price_ticks: std::collections::HashMap<String, f64>,
}
```

Initialize in `TqBuilder::new()`:

```rust
market_url: None,
```

Add the builder method:

```rust
#[must_use]
pub fn market_relay(mut self, relay_url: impl Into<String>) -> Self {
    self.market_url = Some(relay_url.into());
    self
}
```

Apply it before building `TqApiBuilder` in `connect()`:

```rust
if let Some(market_url) = self.market_url {
    session_builder = session_builder.market_relay(market_url);
}
```

Make sure this is only on the live/server-backtest branch. The local backtest early return must continue to ignore market relay configuration.

- [ ] **Step 5: Add facade contract guard**

Append to `crates/tqsdk/tests/facade_contract.rs`:

```rust
#[test]
fn facade_exposes_market_relay_builder_method() {
    let builder = tqsdk::Tq::futures()
        .auth("demo-user", "demo-pass")
        .market_relay("ws://127.0.0.1:7788/market")
        .trade_target_tqkq();

    let debug = format!("{builder:?}");
    assert!(debug.contains("market_url"));
}
```

- [ ] **Step 6: Run endpoint helper tests**

Run:

```bash
cargo test -p tqsdk-session --test session_builder builder_accepts_explicit_market_relay_url_without_enabling_other_routes
cargo test -p tqsdk-wait builder::tests::market_relay_forwards_to_inner_session_builder
cargo test -p tqsdk-stream builder::tests::market_relay_forwards_to_inner_session_builder
cargo test -p tqsdk facade_exposes_market_relay_builder_method
```

Expected: PASS.

- [ ] **Step 7: Commit SDK opt-in helper**

```bash
git add crates/tqsdk-session/src/builder.rs crates/tqsdk-session/tests/session_builder.rs crates/tqsdk-wait/src/builder.rs crates/tqsdk-stream/src/builder.rs crates/tqsdk/src/lib.rs crates/tqsdk/tests/facade_contract.rs
git commit -m "feat(session): add optional market relay endpoint"
```

## Task 10: Add Observability Snapshots

**Files:**
- Create: `crates/tqsdk-relay/src/observability.rs`
- Modify: `crates/tqsdk-relay/src/engine.rs`
- Modify: `crates/tqsdk-relay/src/lib.rs`
- Test: `crates/tqsdk-relay/tests/observability.rs`

- [ ] **Step 1: Write failing observability tests**

Create `crates/tqsdk-relay/tests/observability.rs`:

```rust
use tqsdk_relay::{
    ClientId, DownstreamCommand, RelayEngine, RelayTickRow, RelaySourceStatus,
};

#[test]
fn health_reports_up_when_engine_is_constructed() {
    let engine = RelayEngine::new_memory_only(16, 16);

    let health = engine.health_snapshot();

    assert!(health.ready);
    assert_eq!(health.upstream_status, RelaySourceStatus::Connecting);
    assert_eq!(health.downstream_clients, 0);
}

#[test]
fn metrics_include_clients_subscriptions_and_cache_events() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    engine
        .handle_command(
            ClientId::new(1),
            DownstreamCommand::SubscribeQuote {
                symbols: vec!["SHFE.au2602".to_string()],
            },
        )
        .unwrap();
    engine
        .ingest_tick(
            "SHFE.au2602",
            RelayTickRow {
                id: 1,
                datetime: 1,
                last_price: 610.0,
                volume: 10,
                open_interest: 100,
            },
        )
        .unwrap();

    let metrics = engine.metrics_snapshot();

    assert_eq!(metrics.downstream_clients, 1);
    assert_eq!(metrics.quote_subscriptions, 1);
    assert_eq!(metrics.ticks_ingested, 1);
    assert_eq!(metrics.bootstrap_pending, 0);
}
```

Run:

```bash
cargo test -p tqsdk-relay --test observability
```

Expected: FAIL because observability types do not exist.

- [ ] **Step 2: Implement observability types**

Create `crates/tqsdk-relay/src/observability.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelaySourceStatus {
    Connecting,
    Up,
    Degraded,
    Down,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthSnapshot {
    pub ready: bool,
    pub upstream_status: RelaySourceStatus,
    pub downstream_clients: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub downstream_clients: usize,
    pub quote_subscriptions: usize,
    pub chart_subscriptions: usize,
    pub ticks_ingested: u64,
    pub bootstrap_pending: usize,
    pub bootstrap_inflight: usize,
}
```

- [ ] **Step 3: Add counters and snapshot methods to `RelayEngine`**

Modify `RelayEngine` fields:

```rust
use crate::observability::{HealthSnapshot, MetricsSnapshot, RelaySourceStatus};

pub struct RelayEngine {
    cache: MarketCache,
    interests: InterestRegistry,
    bootstrap: BootstrapQueue,
    klines: HashMap<SourceKey, KlineSynthesis>,
    upstream_status: RelaySourceStatus,
    ticks_ingested: u64,
}
```

Initialize in `new_memory_only`:

```rust
upstream_status: RelaySourceStatus::Connecting,
ticks_ingested: 0,
```

Increment in `ingest_tick`:

```rust
self.ticks_ingested += 1;
self.upstream_status = RelaySourceStatus::Up;
```

Add methods:

```rust
#[must_use]
pub fn health_snapshot(&self) -> HealthSnapshot {
    HealthSnapshot {
        ready: true,
        upstream_status: self.upstream_status,
        downstream_clients: self.interests.client_count(),
    }
}

#[must_use]
pub fn metrics_snapshot(&self) -> MetricsSnapshot {
    MetricsSnapshot {
        downstream_clients: self.interests.client_count(),
        quote_subscriptions: self.interests.total_quote_subscriptions(),
        chart_subscriptions: self.interests.total_chart_subscriptions(),
        ticks_ingested: self.ticks_ingested,
        bootstrap_pending: self.bootstrap.len(),
        bootstrap_inflight: self.bootstrap.inflight(),
    }
}
```

Add methods to `InterestRegistry`:

```rust
#[must_use]
pub fn client_count(&self) -> usize {
    let mut clients: BTreeSet<ClientId> = self.client_quotes.keys().copied().collect();
    clients.extend(self.reverse_chart_ids.keys().map(|(client_id, _)| *client_id));
    clients.len()
}

#[must_use]
pub fn total_quote_subscriptions(&self) -> usize {
    self.client_quotes.values().map(BTreeSet::len).sum()
}

#[must_use]
pub fn total_chart_subscriptions(&self) -> usize {
    self.reverse_chart_ids.len()
}
```

Modify `crates/tqsdk-relay/src/lib.rs`:

```rust
pub mod observability;

pub use observability::{HealthSnapshot, MetricsSnapshot, RelaySourceStatus};
```

- [ ] **Step 4: Run observability tests**

Run:

```bash
cargo test -p tqsdk-relay --test observability
```

Expected: PASS.

- [ ] **Step 5: Commit observability**

```bash
git add crates/tqsdk-relay/src/observability.rs crates/tqsdk-relay/src/engine.rs crates/tqsdk-relay/src/interest.rs crates/tqsdk-relay/src/lib.rs crates/tqsdk-relay/tests/observability.rs
git commit -m "feat(relay): expose relay observability snapshots"
```

## Task 11: Add Minimal Downstream WebSocket Server Boundary

**Files:**
- Create: `crates/tqsdk-relay/src/server.rs`
- Modify: `crates/tqsdk-relay/src/main.rs`
- Modify: `crates/tqsdk-relay/src/lib.rs`
- Test: `crates/tqsdk-relay/tests/server.rs`

- [ ] **Step 1: Write failing server boundary test**

Create `crates/tqsdk-relay/tests/server.rs`:

```rust
use std::sync::{Arc, Mutex};

use serde_json::json;
use tqsdk_relay::{RelayEngine, RelayServer};

#[tokio::test(flavor = "current_thread")]
async fn server_handles_json_market_command_without_starting_real_socket() {
    let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
    let server = RelayServer::new(engine);

    let frames = server
        .handle_text(1, json!({"aid": "subscribe_quote", "ins_list": "SHFE.au2602"}).to_string())
        .await
        .unwrap();

    assert!(frames.is_empty());
    assert_eq!(
        server.engine().lock().unwrap().metrics_snapshot().quote_subscriptions,
        1
    );
}

#[tokio::test(flavor = "current_thread")]
async fn server_rejects_unsupported_non_market_command() {
    let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
    let server = RelayServer::new(engine);

    let err = server
        .handle_text(1, json!({"aid": "insert_order"}).to_string())
        .await
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "unsupported relay market command: insert_order"
    );
}
```

Run:

```bash
cargo test -p tqsdk-relay --test server
```

Expected: FAIL because `RelayServer` does not exist.

- [ ] **Step 2: Implement testable server boundary**

Create `crates/tqsdk-relay/src/server.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use std::sync::{Arc, Mutex};

use serde_json::Value;

use crate::engine::{DownstreamFrame, RelayEngine};
use crate::error::{RelayError, RelayResult};
use crate::interest::ClientId;
use crate::protocol::DownstreamCommand;

#[derive(Clone)]
pub struct RelayServer {
    engine: Arc<Mutex<RelayEngine>>,
}

impl RelayServer {
    #[must_use]
    pub fn new(engine: Arc<Mutex<RelayEngine>>) -> Self {
        Self { engine }
    }

    #[must_use]
    pub fn engine(&self) -> Arc<Mutex<RelayEngine>> {
        self.engine.clone()
    }

    pub async fn handle_text(
        &self,
        raw_client_id: u64,
        text: String,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        let value: Value = serde_json::from_str(&text)
            .map_err(|err| RelayError::invalid_protocol(format!("invalid JSON frame: {err}")))?;
        let command = DownstreamCommand::from_value(value)?;
        let mut engine = self
            .engine
            .lock()
            .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?;
        engine.handle_command(ClientId::new(raw_client_id), command)
    }
}
```

Modify `crates/tqsdk-relay/src/lib.rs`:

```rust
pub mod server;

pub use server::RelayServer;
```

Modify `crates/tqsdk-relay/src/main.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk_relay::{RelayConfig, RelayEngine};

fn main() {
    let config = RelayConfig::default();
    if let Err(err) = config.validate() {
        eprintln!("{err}");
        std::process::exit(2);
    }
    let _engine = RelayEngine::new_memory_only(config.tick_ring_capacity, config.kline_ring_capacity);
    eprintln!(
        "tqsdk-relay configured: downstream={} metrics={}",
        config.downstream_listen, config.metrics_listen
    );
}
```

- [ ] **Step 3: Run server tests**

Run:

```bash
cargo test -p tqsdk-relay --test server
```

Expected: PASS.

- [ ] **Step 4: Commit server boundary**

```bash
git add crates/tqsdk-relay/src/server.rs crates/tqsdk-relay/src/main.rs crates/tqsdk-relay/src/lib.rs crates/tqsdk-relay/tests/server.rs
git commit -m "feat(relay): add downstream server boundary"
```

## Task 12: Add Real Downstream WebSocket Loopback

**Files:**
- Modify: `crates/tqsdk-relay/Cargo.toml`
- Modify: `crates/tqsdk-relay/src/server.rs`
- Test: `crates/tqsdk-relay/tests/server_ws.rs`

- [ ] **Step 1: Add WebSocket loopback test**

Create `crates/tqsdk-relay/tests/server_ws.rs`:

```rust
use std::sync::{Arc, Mutex};

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tqsdk_relay::{RelayEngine, RelayServer};

#[tokio::test(flavor = "current_thread")]
async fn relay_accepts_websocket_market_command_and_updates_engine() {
    let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
    let server = RelayServer::new(engine.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        server.serve_once(listener).await.unwrap();
    });

    let mut stream = connect_ws(addr).await;
    send_masked_text(
        &mut stream,
        json!({"aid": "subscribe_quote", "ins_list": "SHFE.au2602"}).to_string(),
    )
    .await;
    stream.shutdown().await.unwrap();
    server_task.await.unwrap();

    assert_eq!(
        engine.lock().unwrap().metrics_snapshot().quote_subscriptions,
        1
    );
}

async fn connect_ws(addr: std::net::SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            format!(
                "GET /market HTTP/1.1\r\n\
Host: {addr}\r\n\
Upgrade: websocket\r\n\
Connection: Upgrade\r\n\
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
Sec-WebSocket-Version: 13\r\n\
\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    let mut response = Vec::new();
    let mut byte = [0_u8; 1];
    while !response.windows(4).any(|window| window == b"\r\n\r\n") {
        stream.read_exact(&mut byte).await.unwrap();
        response.push(byte[0]);
    }
    let response = String::from_utf8(response).unwrap();
    assert!(response.starts_with("HTTP/1.1 101"));
    stream
}

async fn send_masked_text(stream: &mut TcpStream, text: String) {
    let bytes = text.as_bytes();
    assert!(bytes.len() <= 125, "test frame keeps the short websocket length path");
    let mask = [1_u8, 2, 3, 4];
    let mut frame = Vec::with_capacity(bytes.len() + 6);
    frame.push(0x81);
    frame.push(0x80 | bytes.len() as u8);
    frame.extend_from_slice(&mask);
    frame.extend(bytes.iter().enumerate().map(|(index, byte)| *byte ^ mask[index % 4]));
    stream.write_all(&frame).await.unwrap();
}
```

Run:

```bash
cargo test -p tqsdk-relay --test server_ws
```

Expected: FAIL because `RelayServer::serve_once` does not exist.

- [ ] **Step 2: Add handshake dependency**

Modify `crates/tqsdk-relay/Cargo.toml`:

```toml
[dependencies]
futures.workspace = true
serde.workspace = true
serde_json.workspace = true
sha1.workspace = true
tokio.workspace = true
tqsdk-core = { path = "../tqsdk-core", version = "0.1.0" }
yawc = { workspace = true, optional = true }
```

- [ ] **Step 3: Implement one-connection WebSocket serving**

Extend `crates/tqsdk-relay/src/server.rs`:

```rust
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const WS_ACCEPT_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

impl RelayServer {
    pub async fn serve_once(&self, listener: TcpListener) -> RelayResult<()> {
        let (mut stream, _) = listener
            .accept()
            .await
            .map_err(|err| RelayError::Transport(format!("websocket accept failed: {err}")))?;
        accept_handshake(&mut stream).await?;
        let text = read_client_text_frame(&mut stream).await?;
        let frames = self.handle_text(1, text).await?;
        for frame in frames {
            write_server_text_frame(&mut stream, frame.payload.to_string()).await?;
        }
        Ok(())
    }
}

async fn accept_handshake(stream: &mut TcpStream) -> RelayResult<()> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];
    while !buffer.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream
            .read(&mut chunk)
            .await
            .map_err(|err| RelayError::Transport(format!("websocket handshake read failed: {err}")))?;
        if read == 0 {
            return Err(RelayError::invalid_protocol("websocket handshake ended early"));
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
    let request = String::from_utf8(buffer)
        .map_err(|err| RelayError::invalid_protocol(format!("invalid websocket handshake: {err}")))?;
    let key = request
        .lines()
        .find_map(|line| line.split_once(':'))
        .filter(|(name, _)| name.eq_ignore_ascii_case("sec-websocket-key"))
        .map(|(_, value)| value.trim())
        .ok_or_else(|| RelayError::invalid_protocol("missing sec-websocket-key"))?;
    let accept = websocket_accept_key(key);
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
Upgrade: websocket\r\n\
Connection: Upgrade\r\n\
Sec-WebSocket-Accept: {accept}\r\n\
\r\n"
    );
    stream
        .write_all(response.as_bytes())
        .await
        .map_err(|err| RelayError::Transport(format!("websocket handshake write failed: {err}")))?;
    Ok(())
}

async fn read_client_text_frame(stream: &mut TcpStream) -> RelayResult<String> {
    let mut header = [0_u8; 2];
    stream
        .read_exact(&mut header)
        .await
        .map_err(|err| RelayError::Transport(format!("websocket frame read failed: {err}")))?;
    let opcode = header[0] & 0x0f;
    if opcode == 0x8 {
        return Ok(r#"{"aid":"peek_message"}"#.to_string());
    }
    if opcode != 0x1 {
        return Err(RelayError::invalid_protocol("relay expects websocket text frames"));
    }
    let masked = header[1] & 0x80 != 0;
    let mut len = u64::from(header[1] & 0x7f);
    if len == 126 {
        let mut extended = [0_u8; 2];
        stream.read_exact(&mut extended).await.map_err(|err| {
            RelayError::Transport(format!("websocket extended length read failed: {err}"))
        })?;
        len = u64::from(u16::from_be_bytes(extended));
    }
    let mut mask = [0_u8; 4];
    if masked {
        stream
            .read_exact(&mut mask)
            .await
            .map_err(|err| RelayError::Transport(format!("websocket mask read failed: {err}")))?;
    }
    let mut payload = vec![0_u8; usize::try_from(len).map_err(|_| {
        RelayError::invalid_protocol("websocket payload length exceeds usize")
    })?];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|err| RelayError::Transport(format!("websocket payload read failed: {err}")))?;
    if masked {
        for (index, byte) in payload.iter_mut().enumerate() {
            *byte ^= mask[index % mask.len()];
        }
    }
    String::from_utf8(payload)
        .map_err(|err| RelayError::invalid_protocol(format!("invalid websocket text payload: {err}")))
}

async fn write_server_text_frame(stream: &mut TcpStream, text: String) -> RelayResult<()> {
    let bytes = text.as_bytes();
    let mut frame = Vec::with_capacity(bytes.len() + 10);
    frame.push(0x81);
    match bytes.len() {
        len @ 0..=125 => frame.push(len as u8),
        len @ 126..=65535 => {
            frame.push(126);
            frame.extend_from_slice(&(len as u16).to_be_bytes());
        }
        len => {
            frame.push(127);
            frame.extend_from_slice(&(len as u64).to_be_bytes());
        }
    }
    frame.extend_from_slice(bytes);
    stream
        .write_all(&frame)
        .await
        .map_err(|err| RelayError::Transport(format!("websocket frame write failed: {err}")))
}

fn websocket_accept_key(client_key: &str) -> String {
    use sha1::{Digest, Sha1};
    let mut hasher = Sha1::new();
    hasher.update(client_key.as_bytes());
    hasher.update(WS_ACCEPT_GUID.as_bytes());
    base64_standard(&hasher.finalize())
}

fn base64_standard(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        encoded.push(ALPHABET[(b0 >> 2) as usize] as char);
        encoded.push(ALPHABET[(((b0 & 0b0000_0011) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            encoded.push(ALPHABET[(((b1 & 0b0000_1111) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            encoded.push('=');
        }
        if chunk.len() > 2 {
            encoded.push(ALPHABET[(b2 & 0b0011_1111) as usize] as char);
        } else {
            encoded.push('=');
        }
    }
    encoded
}
```

- [ ] **Step 4: Run WebSocket loopback test**

Run:

```bash
cargo test -p tqsdk-relay --test server_ws
```

Expected: PASS.

- [ ] **Step 5: Commit WebSocket loopback**

```bash
git add crates/tqsdk-relay/Cargo.toml crates/tqsdk-relay/src/server.rs crates/tqsdk-relay/tests/server_ws.rs
git commit -m "feat(relay): accept downstream websocket commands"
```

## Task 13: Add Futures Universe and Upstream Tick Source Scaffold

**Files:**
- Modify: `crates/tqsdk-relay/src/config.rs`
- Modify: `crates/tqsdk-relay/src/upstream.rs`
- Test: `crates/tqsdk-relay/tests/upstream.rs`

- [ ] **Step 1: Write failing upstream scaffold tests**

Create `crates/tqsdk-relay/tests/upstream.rs`:

```rust
use tqsdk_relay::{RelayConfig, UniverseExpression, UpstreamTickChart};

#[test]
fn config_accepts_explicit_futures_universe() {
    let mut config = RelayConfig::default();
    config.futures_universe_expression =
        Some(UniverseExpression::parse("symbol:SHFE.au2602,DCE.m2609").unwrap());

    config.validate().unwrap();
    assert!(config.has_upstream_futures_source());
}

#[test]
fn upstream_tick_chart_uses_duration_zero_and_sorted_symbols() {
    let chart = UpstreamTickChart::new(
        "relay-upstream-all-futures-ticks",
        ["DCE.m2609", "SHFE.au2602"],
        10_000,
    )
    .unwrap();

    assert_eq!(chart.chart_id(), "relay-upstream-all-futures-ticks");
    assert_eq!(chart.duration_ns(), 0);
    assert_eq!(chart.view_width(), 10_000);
    assert_eq!(chart.symbols(), &["DCE.m2609".to_string(), "SHFE.au2602".to_string()]);
}
```

Run:

```bash
cargo test -p tqsdk-relay --test upstream
```

Expected: FAIL because `futures_universe_expression` and `UpstreamTickChart` do not exist.

- [ ] **Step 2: Add explicit futures universe config**

Modify `RelayConfig` in `crates/tqsdk-relay/src/config.rs`:

```rust
pub futures_universe_expression: Option<UniverseExpression>,
```

Initialize in `Default`:

```rust
futures_universe_expression: None,
```

Add to `validate()`:

```rust
if let Some(expression) = self.futures_universe_expression.as_ref() {
    // Parsed expression already validates empty selector values.
}
```

- [ ] **Step 3: Add upstream tick chart scaffold**

Extend `crates/tqsdk-relay/src/upstream.rs`:

```rust
use crate::error::{RelayError, RelayResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamTickChart {
    chart_id: String,
    symbols: Vec<String>,
    view_width: usize,
}

impl UpstreamTickChart {
    pub fn new<I, S>(
        chart_id: impl Into<String>,
        symbols: I,
        view_width: usize,
    ) -> RelayResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let chart_id = chart_id.into();
        if chart_id.trim().is_empty() {
            return Err(RelayError::invalid_config("upstream tick chart_id must not be empty"));
        }
        if view_width == 0 {
            return Err(RelayError::invalid_config("upstream tick view_width must be greater than zero"));
        }
        let mut symbols: Vec<String> = symbols
            .into_iter()
            .map(|symbol| symbol.as_ref().trim().to_string())
            .filter(|symbol| !symbol.is_empty())
            .collect();
        symbols.sort();
        symbols.dedup();
        if symbols.is_empty() {
            return Err(RelayError::invalid_config("upstream tick chart requires at least one symbol"));
        }
        Ok(Self {
            chart_id,
            symbols,
            view_width,
        })
    }

    #[must_use]
    pub fn chart_id(&self) -> &str {
        &self.chart_id
    }

    #[must_use]
    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }

    #[must_use]
    pub const fn duration_ns(&self) -> i64 {
        0
    }

    #[must_use]
    pub fn view_width(&self) -> usize {
        self.view_width
    }
}
```

Modify `crates/tqsdk-relay/src/lib.rs` export:

```rust
pub use upstream::{FakeUpstreamTickSource, UpstreamTick, UpstreamTickChart, UpstreamTickSource};
```

- [ ] **Step 4: Run upstream tests**

Run:

```bash
cargo test -p tqsdk-relay --test upstream
```

Expected: PASS.

- [ ] **Step 5: Commit upstream scaffold**

```bash
git add crates/tqsdk-relay/src/config.rs crates/tqsdk-relay/src/upstream.rs crates/tqsdk-relay/src/lib.rs crates/tqsdk-relay/tests/upstream.rs
git commit -m "feat(relay): add upstream tick source scaffold"
```

## Task 14: Update Docs and Architecture Boundaries

**Files:**
- Modify: `README.md`
- Modify: `docs/architecture/README.md`
- Modify: `docs/architecture/crate-boundaries.md`
- Modify: `docs/architecture/validation.md`
- Modify: `crates/tqsdk-relay/README.md`
- Modify: `docs/superpowers/specs/2026-06-06-market-relay-design.md` only if implementation changed a design detail

- [ ] **Step 1: Update root README optional crate table**

In `README.md`, add `tqsdk-relay` to the crate table:

```markdown
| [`tqsdk-relay`](crates/tqsdk-relay) | 可选 market relay / cache service：用共享上游 tick 源服务多个 SDK 客户端的 quote / tick / K 线请求；未配置 relay 时 SDK 仍直连天勤 |
```

Add a short note after the installation section:

```markdown
`tqsdk-relay` 是可选基础设施。普通 SDK 使用不需要启动 relay；只有需要降低多进程、
全品种、多周期行情订阅压力时，才显式把 market endpoint 指向 relay。
```

- [ ] **Step 2: Update architecture docs**

In `docs/architecture/README.md`, add `tqsdk-relay` to the implementation status list with this wording:

```markdown
- `tqsdk-relay`
  - 可选 market relay / cache service
  - 不改变 SDK 默认直连路径，不代理 trade/query/auth
  - 现有 SDK crates 不依赖 relay；用户显式配置 market endpoint 时才使用
```

In `docs/architecture/crate-boundaries.md`, add a section:

```markdown
## `tqsdk-relay`

### 正确职责

- 可选独立进程 / binary
- 代理 market route 子集
- 维护共享上游 tick source、内存行情 cache、K 线合成、bootstrap/resync 队列
- 提供 relay 自身 health / metrics / sources 观测

### 不应承担的职责

- 不代理 trade / query / auth / schema / metadata
- 不进入现有 SDK crate 的默认依赖路径
- 不改变 `tqsdk-core` runtime contract
- 不作为多 provider 行情聚合框架
```

- [ ] **Step 3: Update validation matrix**

In `docs/architecture/validation.md`, add:

```markdown
| 可选 market relay | `cargo test -p tqsdk-relay --tests` | 覆盖 relay 配置、下游 market 协议、interest/chart-id 隔离、K 线 `[start,end)` 合成、bootstrap 队列限流、observability、WebSocket loopback 和 upstream tick scaffold |
| relay endpoint opt-in | `cargo test -p tqsdk-session --test session_builder builder_accepts_explicit_market_relay_url_without_enabling_other_routes` | 确认 relay 只显式改 market endpoint，不启用 trade/query/auth |
```

- [ ] **Step 4: Expand relay README**

Replace `crates/tqsdk-relay/README.md` with:

```markdown
# `tqsdk-relay`

`tqsdk-relay` is an optional market relay and cache service for `tqsdk-rust`.
It is infrastructure, not the default SDK runtime path.

Use it when one process can subscribe to all futures ticks but many SDK clients
or many K-line periods would exceed Tianqin market subscription limits.

V1 scope:

- market route only
- futures tick upstream first
- quote / tick / fixed-duration K-line fan-out
- in-memory cache first
- bootstrap / resync queue with hard concurrency limits
- health / metrics / sources snapshots

Non-goals:

- trade proxy
- query / schema / metadata proxy
- auth proxy for downstream clients
- multi-provider aggregation
- SDK default behavior changes

SDK clients opt in by pointing their market endpoint at relay:

```rust
let mut tq = tqsdk::Tq::futures()
    .auth_env()?
    .market_relay("ws://127.0.0.1:7788/market")
    .connect()
    .await?;
```

Without `.market_relay(...)`, SDK clients continue to connect directly to Tianqin.
```

- [ ] **Step 5: Run docs check**

Run:

```bash
git diff --check
```

Expected: PASS.

- [ ] **Step 6: Commit docs**

```bash
git add README.md docs/architecture/README.md docs/architecture/crate-boundaries.md docs/architecture/validation.md crates/tqsdk-relay/README.md docs/superpowers/specs/2026-06-06-market-relay-design.md
git commit -m "docs: document optional market relay"
```

## Task 15: Final Verification and Change Detection

**Files:**
- All changed files above

- [ ] **Step 1: Run formatting and relay tests**

Run:

```bash
cargo fmt --all --check
cargo test -p tqsdk-relay --tests
```

Expected: PASS.

- [ ] **Step 2: Run SDK endpoint regression tests**

Run:

```bash
cargo test -p tqsdk-session --test session_builder
cargo test -p tqsdk-wait builder::tests
cargo test -p tqsdk-stream builder::tests
cargo test -p tqsdk facade_exposes_market_relay_builder_method
```

Expected: PASS.

- [ ] **Step 3: Run workspace checks**

Run:

```bash
cargo check --workspace --examples
cargo test --workspace
```

Expected: PASS.

- [ ] **Step 4: Run feature-slice checks**

Because this plan adds a workspace crate and a public builder surface, run:

```bash
cargo check --workspace --no-default-features
cargo check --workspace --all-features --examples
```

Expected: PASS. If `tqsdk-relay` server feature introduces a feature-specific issue, fix the feature declaration instead of skipping the check.

- [ ] **Step 5: Run GitNexus change detection**

Run GitNexus MCP `detect_changes` with staged or all changes before final commit if there are uncommitted changes:

```text
mcp__gitnexus.detect_changes(repo="/Users/joeslee/Projects/GitHub/tqsdk-rust", scope="all")
```

Expected: Changed files are limited to `tqsdk-relay`, endpoint builder forwarding, relay docs, and validation docs. Investigate any unrelated affected symbols before committing.

- [ ] **Step 6: Final commit if any verification fixes remain**

If the previous tasks left only verification fixes uncommitted:

```bash
git add -u
git add crates/tqsdk-relay
git commit -m "test(relay): verify optional market relay"
```

If no changes remain, do not create an empty commit.

## Coverage Check

- Optional relay and unchanged direct-to-TQ default: Task 9, Task 14, Task 15.
- Workspace crate / binary: Task 1, Task 11, Task 12.
- Market route only and unsupported trade/query/auth: Task 3, Task 11.
- Real SDK market WebSocket opt-in boundary: Task 9, Task 12.
- Single upstream futures tick source boundary: Task 8 creates a `UpstreamTickSource` trait and fake source; Task 13 adds explicit futures universe config and upstream tick chart scaffold.
- Tick cache and quote projection: Task 4, Task 8.
- K-line `[start, end)` synthesis: Task 5.
- Multi-client chart id isolation: Task 6, Task 8.
- Bootstrap/resync coalescing and limits: Task 7, Task 8.
- Observability: Task 10, Task 14.
- SDK endpoint opt-in: Task 9.
- No SDK dependency on relay: Task 1 and Task 15 workspace checks.
- No old SDK-internal kline-source path: plan keeps source choice outside existing SDK runtime
  and uses only explicit market endpoint opt-in.
