# Low-Level Session Stream Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the next scenario-driven public API batch for lower-level users by hardening `tqsdk-session` and `tqsdk-stream` around raw market commands, typed instrument specs, diagnostics/retry hints, and explicit fan-out lag controls.

**Architecture:** Keep `tqsdk-core` as the protocol-complete runtime substrate and avoid adding high-level facade behavior to it. Put one-shot command helpers and metadata normalization in `tqsdk-session`; put multi-consumer fan-out configuration, lag diagnostics, and health/error stream ergonomics in `tqsdk-stream`. Do not introduce a second state tree, provider-private handles, background tasks owned by users, or task/data abstractions in this batch.

**Tech Stack:** Rust 2024, Tokio tests, existing `tqsdk-core` runtime contract, `tqsdk-session`, `tqsdk-stream`, scenario contract examples, docs under `docs/scenarios` and `docs/architecture`.

---

## Batch Scope

Prioritize lower-level capabilities that support later high-level facade work:

- S5 高频裸行情直通: keep the fast path thin, but remove unnecessary raw `RuntimeCommand` boilerplate from user code.
- S21 慢消费者隔离: expose fan-out buffer capacity and typed lag diagnostics as public stream configuration, without adding a full sink runtime yet.
- S22 错误诊断与重试: add stable error categories and retry hints across session/stream errors, so user code does not parse strings.
- S23 合约信息查询与标准化: add an explicit `InstrumentSpec` metadata type instead of asking users to treat live `Quote` as the contract-spec object.
- S20 生产守护进程: improve typed health/status methods, while leaving full metrics endpoint, ctrl-c shutdown, and durable sinks as gaps.

This batch should not touch:

- `tqsdk-task` execution group, strategy host, or fake broker behavior.
- `tqsdk-data` historical cache/replay.
- multi-provider aggregation.
- cross-process order intent persistence.
- provider-specific protocol modules or auth implementation details.

## File Structure

- Modify `crates/tqsdk-core/src/error.rs`
  - Add stable `ContractErrorKind` and `RetryHint`.
  - Add `ContractError::kind()` and `ContractError::retry_hint()` without changing existing variants.
- Modify `crates/tqsdk-session/src/error.rs`
  - Add `SessionErrorKind`, `SessionErrorDiagnostic`, and methods on `SessionFacadeError`.
- Modify `crates/tqsdk-session/src/client/commands.rs`
  - Add `SessionClient::subscribe_quotes()` and `SessionClient::unsubscribe_quotes()` as low-level command helpers.
- Create `crates/tqsdk-session/src/instrument.rs`
  - Add `InstrumentSpec`, `InstrumentClass`, and conversion from metadata `Quote`.
- Modify `crates/tqsdk-session/src/metadata.rs`, `direct_query.rs`, `client.rs`, and `lib.rs`
  - Add `SessionClient::query_instrument_specs()` and trait method forwarding.
- Create `crates/tqsdk-session/tests/session_market_command_helpers.rs`
  - Test low-level quote subscription helpers without live network.
- Create `crates/tqsdk-session/tests/session_instrument_spec.rs`
  - Test `InstrumentSpec` normalization and query wrapper behavior.
- Modify `crates/tqsdk-stream/src/builder.rs`
  - Add public `commit_channel_capacity(capacity)` configuration.
- Modify `crates/tqsdk-stream/src/api.rs`
  - Wire builder capacity into `TqStream::new_with_capacity`; keep constructor default unchanged.
- Modify `crates/tqsdk-stream/src/error.rs`
  - Add `StreamErrorKind`, `StreamErrorDiagnostic`, and methods on `StreamFacadeError`.
- Modify `crates/tqsdk-stream/src/health.rs`
  - Add `StreamHealthStatus` and status helpers on `StreamHealthSnapshot`.
- Create or modify `crates/tqsdk-stream/tests/stream_diagnostics.rs`
  - Test stream error diagnostics, health status, and public capacity-driven lag behavior.
- Modify examples:
  - `crates/tqsdk-session/examples/api_contract_s05_bare_market_fast_path.rs`
  - `crates/tqsdk-session/examples/api_contract_s23_contract_metadata.rs`
  - `crates/tqsdk-stream/examples/api_contract_s20_production_daemon_health.rs`
  - `crates/tqsdk-stream/examples/api_contract_s21_slow_consumer_isolation.rs`
  - `crates/tqsdk-stream/examples/api_contract_s22_error_diagnosis_retry.rs`
- Modify docs:
  - `docs/reviews/public-api-scenario-review.md`
  - `docs/scenarios/user-layer-iteration-plan.md`
  - `docs/scenarios/api_gaps/api_contract_s20_production_daemon.rs`
  - `docs/scenarios/api_gaps/api_contract_s21_slow_consumer_isolation.rs`
  - `docs/scenarios/api_gaps/api_contract_s22_error_diagnosis_retry.rs`
  - `docs/architecture/api-stream.md`
  - `docs/architecture/ai-workflow.md`
  - `docs/architecture/README.md`
  - `crates/tqsdk-session/README.md`
  - `crates/tqsdk-stream/README.md`

## Public API Shape

### Low-level market command helper

```rust
use tqsdk_core::Symbol;
use tqsdk_session::SessionClientBuilder;

# async fn run() -> tqsdk_session::Result<()> {
let session = SessionClientBuilder::new("user", "pass")
    .futures_market()
    .build()?;

let symbol = Symbol::new("SHFE.au2602");
session.subscribe_quotes([symbol.as_str()]).await?;

let reader = session.reader().clone();
let mut cursor = reader.cursor();
while let Some(commit) = reader.next(&mut cursor) {
    let market = reader.read_market_state();
    if let Some(quote) = market.quote(&symbol)? {
        println!("{} {}", commit.revision.get(), quote.last_price);
    }
}
# Ok(())
# }
```

### Typed instrument spec

```rust
use tqsdk_session::{InstrumentClass, SessionClientBuilder};

# async fn run() -> tqsdk_session::Result<()> {
let session = SessionClientBuilder::new("user", "pass")
    .enable_query()
    .build()?;

let spec = session
    .query_instrument_specs(&["SHFE.au2602"])
    .await?
    .into_iter()
    .next()
    .expect("metadata should include the requested symbol");

assert_eq!(spec.class, InstrumentClass::Future);
assert!(spec.price_tick > 0.0);
assert!(spec.volume_multiple > 0);
# Ok(())
# }
```

### Error diagnostics and retry hints

```rust
use tqsdk_session::RetryHint;

fn classify(error: &tqsdk_stream::StreamFacadeError) -> RetryHint {
    let diagnostic = error.diagnostic();
    if diagnostic.retry_hint == RetryHint::RetryAfterReconnect {
        RetryHint::RetryAfterReconnect
    } else {
        RetryHint::DoNotRetry
    }
}
```

### Public fan-out capacity

```rust
let stream = tqsdk_stream::TqStreamBuilder::new("user", "pass")
    .futures_market()
    .commit_channel_capacity(16_384)?
    .build()
    .await?;
```

`commit_channel_capacity(0)` must return a typed validation error. Capacity is a root fan-out setting, not a per-consumer reliable queue. Full sink isolation remains a later batch.

## Task 1: Contract Error Kinds and Retry Hints

**Files:**
- Modify: `crates/tqsdk-core/src/error.rs`
- Modify: `crates/tqsdk-core/src/lib.rs`
- Test: `crates/tqsdk-core/tests/runtime_contract_error_diagnostics.rs`

- [ ] **Step 1: Write failing tests for core error classification**

Create `crates/tqsdk-core/tests/runtime_contract_error_diagnostics.rs`:

```rust
use tqsdk_core::{ContractError, ContractErrorKind, RetryHint};

#[test]
fn contract_errors_expose_stable_kind_and_retry_hint() {
    assert_eq!(
        ContractError::transport("websocket recv failed").kind(),
        ContractErrorKind::Transport
    );
    assert_eq!(
        ContractError::transport("websocket recv failed").retry_hint(),
        RetryHint::RetryAfterReconnect
    );

    assert_eq!(ContractError::auth("bad password").kind(), ContractErrorKind::Auth);
    assert_eq!(ContractError::auth("bad password").retry_hint(), RetryHint::DoNotRetry);

    assert_eq!(
        ContractError::validation("invalid symbol").kind(),
        ContractErrorKind::Validation
    );
    assert_eq!(
        ContractError::validation("invalid symbol").retry_hint(),
        RetryHint::DoNotRetry
    );

    assert_eq!(
        ContractError::http("query timeout").retry_hint(),
        RetryHint::RetryWithBackoff
    );
    assert_eq!(
        ContractError::UnsupportedCommand("market").retry_hint(),
        RetryHint::DoNotRetry
    );
}
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
cargo test -p tqsdk-core --test runtime_contract_error_diagnostics -- --nocapture
```

Expected: compile failure because `ContractErrorKind` and `RetryHint` do not exist.

- [ ] **Step 3: Implement the minimal core diagnostic API**

Add to `crates/tqsdk-core/src/error.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryHint {
    DoNotRetry,
    RetryWithBackoff,
    RetryAfterReconnect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractErrorKind {
    Validation,
    Auth,
    Transport,
    Http,
    Adapter,
    UnsupportedCommand,
    UnsupportedInput,
}

impl ContractError {
    #[must_use]
    pub fn kind(&self) -> ContractErrorKind {
        match self {
            Self::Validation(_) => ContractErrorKind::Validation,
            Self::Auth(_) => ContractErrorKind::Auth,
            Self::Transport(_) => ContractErrorKind::Transport,
            Self::Http(_) => ContractErrorKind::Http,
            Self::Adapter(_) => ContractErrorKind::Adapter,
            Self::UnsupportedCommand(_) => ContractErrorKind::UnsupportedCommand,
            Self::UnsupportedInput(_) => ContractErrorKind::UnsupportedInput,
        }
    }

    #[must_use]
    pub fn retry_hint(&self) -> RetryHint {
        match self {
            Self::Transport(_) => RetryHint::RetryAfterReconnect,
            Self::Http(_) => RetryHint::RetryWithBackoff,
            Self::Validation(_)
            | Self::Auth(_)
            | Self::Adapter(_)
            | Self::UnsupportedCommand(_)
            | Self::UnsupportedInput(_) => RetryHint::DoNotRetry,
        }
    }
}
```

Re-export both types from `crates/tqsdk-core/src/lib.rs`.

- [ ] **Step 4: Run the focused test and commit**

Run:

```bash
cargo test -p tqsdk-core --test runtime_contract_error_diagnostics -- --nocapture
```

Expected: test passes.

Commit:

```bash
git add crates/tqsdk-core/src/error.rs crates/tqsdk-core/src/lib.rs crates/tqsdk-core/tests/runtime_contract_error_diagnostics.rs
git commit -m "feat: add core error retry diagnostics"
```

## Task 2: Session Error Diagnostics

**Files:**
- Modify: `crates/tqsdk-session/src/error.rs`
- Modify: `crates/tqsdk-session/src/lib.rs`
- Test: `crates/tqsdk-session/tests/session_error_diagnostics.rs`

- [ ] **Step 1: Write failing tests for session diagnostics**

Create `crates/tqsdk-session/tests/session_error_diagnostics.rs`:

```rust
use tqsdk_core::{ContractError, RetryHint};
use tqsdk_session::{SessionErrorKind, SessionFacadeError};

#[test]
fn session_errors_expose_kind_retry_hint_and_message() {
    let transport = SessionFacadeError::from(ContractError::transport("socket closed"));
    let diagnostic = transport.diagnostic();
    assert_eq!(diagnostic.kind, SessionErrorKind::Transport);
    assert_eq!(diagnostic.retry_hint, RetryHint::RetryAfterReconnect);
    assert_eq!(diagnostic.message, "transport error: socket closed");
    assert!(transport.is_retryable());

    let invalid = SessionFacadeError::InvalidState("query route disabled");
    let diagnostic = invalid.diagnostic();
    assert_eq!(diagnostic.kind, SessionErrorKind::InvalidState);
    assert_eq!(diagnostic.retry_hint, RetryHint::DoNotRetry);
    assert!(!invalid.is_retryable());
}
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
cargo test -p tqsdk-session --test session_error_diagnostics -- --nocapture
```

Expected: compile failure because diagnostic types/methods do not exist.

- [ ] **Step 3: Implement session diagnostic wrappers**

Add to `crates/tqsdk-session/src/error.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionErrorKind {
    Validation,
    Auth,
    Transport,
    Http,
    Adapter,
    UnsupportedCommand,
    UnsupportedInput,
    InvalidState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionErrorDiagnostic {
    pub kind: SessionErrorKind,
    pub retry_hint: tqsdk_core::RetryHint,
    pub message: String,
}

impl SessionFacadeError {
    #[must_use]
    pub fn diagnostic(&self) -> SessionErrorDiagnostic {
        match self {
            Self::Core(error) => SessionErrorDiagnostic {
                kind: match error.kind() {
                    tqsdk_core::ContractErrorKind::Validation => SessionErrorKind::Validation,
                    tqsdk_core::ContractErrorKind::Auth => SessionErrorKind::Auth,
                    tqsdk_core::ContractErrorKind::Transport => SessionErrorKind::Transport,
                    tqsdk_core::ContractErrorKind::Http => SessionErrorKind::Http,
                    tqsdk_core::ContractErrorKind::Adapter => SessionErrorKind::Adapter,
                    tqsdk_core::ContractErrorKind::UnsupportedCommand => {
                        SessionErrorKind::UnsupportedCommand
                    }
                    tqsdk_core::ContractErrorKind::UnsupportedInput => {
                        SessionErrorKind::UnsupportedInput
                    }
                },
                retry_hint: error.retry_hint(),
                message: error.to_string(),
            },
            Self::InvalidState(message) => SessionErrorDiagnostic {
                kind: SessionErrorKind::InvalidState,
                retry_hint: tqsdk_core::RetryHint::DoNotRetry,
                message: format!("invalid session facade state: {message}"),
            },
        }
    }

    #[must_use]
    pub fn is_retryable(&self) -> bool {
        !matches!(self.diagnostic().retry_hint, tqsdk_core::RetryHint::DoNotRetry)
    }
}
```

Re-export `RetryHint` from `tqsdk-session` only as a convenience:

```rust
pub use tqsdk_core::RetryHint;
```

- [ ] **Step 4: Run the focused test and commit**

Run:

```bash
cargo test -p tqsdk-session --test session_error_diagnostics -- --nocapture
```

Expected: test passes.

Commit:

```bash
git add crates/tqsdk-session/src/error.rs crates/tqsdk-session/src/lib.rs crates/tqsdk-session/tests/session_error_diagnostics.rs
git commit -m "feat: add session error diagnostics"
```

## Task 3: Low-Level Session Market Command Helpers

**Files:**
- Modify: `crates/tqsdk-session/src/client/commands.rs`
- Test: `crates/tqsdk-session/tests/session_market_command_helpers.rs`
- Example: `crates/tqsdk-session/examples/api_contract_s05_bare_market_fast_path.rs`

- [ ] **Step 1: Write failing tests for quote command helpers**

Create `crates/tqsdk-session/tests/session_market_command_helpers.rs`:

```rust
use serde_json::{Value, json};
use tqsdk_core::{AdapterRegistry, OutboundFrame, OutboundRequest, ProtocolDomain, RuntimeHandle};
use tqsdk_session::SessionClient;

fn runtime_handle_with_default_adapters() -> RuntimeHandle {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    RuntimeHandle::with_adapters(adapters)
}

#[tokio::test(flavor = "current_thread")]
async fn subscribe_quotes_submits_market_command_without_raw_runtime_command() {
    let client = SessionClient::new_for_test_with_handle(runtime_handle_with_default_adapters());

    let command_id = client.subscribe_quotes(["SHFE.au2602", "DCE.m2609"]).await.unwrap();

    assert!(command_id.get() > 0);
    let dispatches = client.drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    assert_eq!(dispatches[0].domain, ProtocolDomain::Market);
    let body: Value = match &dispatches[0].request {
        OutboundRequest::Transport(OutboundFrame::Text(text)) => serde_json::from_str(text).unwrap(),
        other => panic!("expected websocket market dispatch, got {other:?}"),
    };
    assert_eq!(body.get("aid"), Some(&json!("subscribe_quote")));
    assert_eq!(body.get("ins_list"), Some(&json!("SHFE.au2602,DCE.m2609")));
}

#[tokio::test(flavor = "current_thread")]
async fn unsubscribe_quotes_submits_market_command() {
    let client = SessionClient::new_for_test_with_handle(runtime_handle_with_default_adapters());

    client.unsubscribe_quotes(["SHFE.au2602"]).await.unwrap();

    let dispatches = client.drain_dispatches().unwrap();
    let body: Value = match &dispatches[0].request {
        OutboundRequest::Transport(OutboundFrame::Text(text)) => serde_json::from_str(text).unwrap(),
        other => panic!("expected websocket market dispatch, got {other:?}"),
    };
    assert_eq!(body.get("aid"), Some(&json!("unsubscribe_quote")));
    assert_eq!(body.get("ins_list"), Some(&json!("SHFE.au2602")));
}

#[tokio::test(flavor = "current_thread")]
async fn subscribe_quotes_rejects_empty_symbol_list() {
    let client = SessionClient::new_for_test_with_handle(runtime_handle_with_default_adapters());

    let err = client.subscribe_quotes(std::iter::empty::<&str>()).await.unwrap_err();

    assert_eq!(err.diagnostic().retry_hint, tqsdk_core::RetryHint::DoNotRetry);
}
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
cargo test -p tqsdk-session --test session_market_command_helpers -- --nocapture
```

Expected: compile failure because helper methods do not exist.

- [ ] **Step 3: Implement helper methods**

Add to `SessionClient` in `crates/tqsdk-session/src/client/commands.rs`:

```rust
pub async fn subscribe_quotes<I, S>(&self, symbols: I) -> crate::error::Result<CommandId>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let symbols = symbols
        .into_iter()
        .map(|symbol| tqsdk_core::Symbol::new(symbol.as_ref()))
        .collect::<Vec<_>>();
    if symbols.is_empty() {
        return Err(crate::error::SessionFacadeError::InvalidState(
            "subscribe_quotes requires at least one symbol",
        ));
    }
    self.submit(RuntimeCommand::Market(tqsdk_core::MarketCommand::SubscribeQuotes {
        symbols,
    }))
    .await
}

pub async fn unsubscribe_quotes<I, S>(&self, symbols: I) -> crate::error::Result<CommandId>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let symbols = symbols
        .into_iter()
        .map(|symbol| tqsdk_core::Symbol::new(symbol.as_ref()))
        .collect::<Vec<_>>();
    if symbols.is_empty() {
        return Err(crate::error::SessionFacadeError::InvalidState(
            "unsubscribe_quotes requires at least one symbol",
        ));
    }
    self.submit(RuntimeCommand::Market(tqsdk_core::MarketCommand::UnsubscribeQuotes {
        symbols,
    }))
    .await
}
```

- [ ] **Step 4: Update S5 example**

In `crates/tqsdk-session/examples/api_contract_s05_bare_market_fast_path.rs`, replace raw `RuntimeCommand::Market(MarketCommand::SubscribeQuotes { ... })` with:

```rust
session.subscribe_quotes([symbol.as_str()]).await?;
```

Keep `RuntimeReader::read_market_state()` and manual `progress_once()` loop; this is the low-level escape hatch and should stay visible in S5.

- [ ] **Step 5: Run focused checks and commit**

Run:

```bash
cargo test -p tqsdk-session --test session_market_command_helpers -- --nocapture
cargo check -p tqsdk-session --example api_contract_s05_bare_market_fast_path
```

Expected: both pass.

Commit:

```bash
git add crates/tqsdk-session/src/client/commands.rs crates/tqsdk-session/tests/session_market_command_helpers.rs crates/tqsdk-session/examples/api_contract_s05_bare_market_fast_path.rs
git commit -m "feat: add session market command helpers"
```

## Task 4: Typed Instrument Metadata Specs

**Files:**
- Create: `crates/tqsdk-session/src/instrument.rs`
- Modify: `crates/tqsdk-session/src/lib.rs`
- Modify: `crates/tqsdk-session/src/direct_query.rs`
- Modify: `crates/tqsdk-session/src/metadata.rs`
- Modify: `crates/tqsdk-session/src/client.rs`
- Test: `crates/tqsdk-session/tests/session_instrument_spec.rs`
- Example: `crates/tqsdk-session/examples/api_contract_s23_contract_metadata.rs`

- [ ] **Step 1: Write failing tests for `InstrumentSpec`**

Create `crates/tqsdk-session/tests/session_instrument_spec.rs`:

```rust
use tqsdk_core::Quote;
use tqsdk_session::{InstrumentClass, InstrumentSpec};

fn quote() -> Quote {
    Quote {
        instrument_id: "SHFE.au2602".to_string(),
        exchange_id: "SHFE".to_string(),
        product_id: "au".to_string(),
        ins_class: "FUTURE".to_string(),
        price_tick: 0.02,
        volume_multiple: 1000,
        expire_datetime: Some(1_770_000_000_000_000_000),
        ..Quote::default()
    }
}

#[test]
fn instrument_spec_normalizes_contract_metadata_from_quote() {
    let spec = InstrumentSpec::try_from(quote()).unwrap();

    assert_eq!(spec.symbol.as_str(), "SHFE.au2602");
    assert_eq!(spec.exchange_id, "SHFE");
    assert_eq!(spec.product_id, "au");
    assert_eq!(spec.class, InstrumentClass::Future);
    assert_eq!(spec.price_tick, 0.02);
    assert_eq!(spec.volume_multiple, 1000);
    assert_eq!(spec.expire_datetime_ns, Some(1_770_000_000_000_000_000));
    assert!(spec.is_derivative());
}

#[test]
fn instrument_spec_rejects_missing_symbol() {
    let mut quote = quote();
    quote.instrument_id.clear();

    let err = InstrumentSpec::try_from(quote).unwrap_err();

    assert_eq!(err.diagnostic().retry_hint, tqsdk_core::RetryHint::DoNotRetry);
}
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
cargo test -p tqsdk-session --test session_instrument_spec -- --nocapture
```

Expected: compile failure because `InstrumentSpec` does not exist.

- [ ] **Step 3: Implement `instrument.rs`**

Create `crates/tqsdk-session/src/instrument.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk_core::{Quote, Symbol};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstrumentClass {
    Future,
    Continuous,
    Index,
    Option,
    Stock,
    Fund,
    Bond,
    Unknown,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InstrumentSpec {
    pub symbol: Symbol,
    pub exchange_id: String,
    pub product_id: String,
    pub class: InstrumentClass,
    pub price_tick: f64,
    pub volume_multiple: i64,
    pub expire_datetime_ns: Option<i64>,
    pub underlying_symbol: Option<Symbol>,
}

impl InstrumentSpec {
    #[must_use]
    pub fn is_derivative(&self) -> bool {
        matches!(
            self.class,
            InstrumentClass::Future
                | InstrumentClass::Continuous
                | InstrumentClass::Index
                | InstrumentClass::Option
        )
    }
}

impl TryFrom<Quote> for InstrumentSpec {
    type Error = crate::SessionFacadeError;

    fn try_from(quote: Quote) -> Result<Self, Self::Error> {
        if quote.instrument_id.is_empty() {
            return Err(crate::SessionFacadeError::InvalidState(
                "instrument metadata is missing instrument_id",
            ));
        }
        if !quote.price_tick.is_finite() || quote.price_tick <= 0.0 {
            return Err(crate::SessionFacadeError::InvalidState(
                "instrument metadata price_tick must be positive",
            ));
        }
        if quote.volume_multiple <= 0 {
            return Err(crate::SessionFacadeError::InvalidState(
                "instrument metadata volume_multiple must be positive",
            ));
        }

        let class = match quote.ins_class.as_str() {
            "FUTURE" => InstrumentClass::Future,
            "CONT" => InstrumentClass::Continuous,
            "INDEX" => InstrumentClass::Index,
            "OPTION" => InstrumentClass::Option,
            "STOCK" => InstrumentClass::Stock,
            "FUND" => InstrumentClass::Fund,
            "BOND" => InstrumentClass::Bond,
            _ => InstrumentClass::Unknown,
        };

        Ok(Self {
            symbol: Symbol::new(quote.instrument_id),
            exchange_id: quote.exchange_id,
            product_id: quote.product_id,
            class,
            price_tick: quote.price_tick,
            volume_multiple: quote.volume_multiple,
            expire_datetime_ns: quote.expire_datetime,
            underlying_symbol: (!quote.underlying_symbol.is_empty())
                .then(|| Symbol::new(quote.underlying_symbol)),
        })
    }
}
```

Export `InstrumentClass` and `InstrumentSpec` from `lib.rs`.

- [ ] **Step 4: Add query wrapper**

Add to `SessionMetadataQuery` in `direct_query.rs`:

```rust
async fn query_instrument_specs(
    &self,
    symbols: &[&str],
) -> crate::error::Result<Vec<crate::InstrumentSpec>>;
```

Implement on `SessionClient` by calling `query_symbol_info(symbols)` and converting each `Quote` into `InstrumentSpec`.

Also add an inherent method on `SessionClient` with the same signature so users do not need to import the trait for common usage:

```rust
pub async fn query_instrument_specs(
    &self,
    symbols: &[&str],
) -> crate::error::Result<Vec<crate::InstrumentSpec>> {
    self.query_symbol_info(symbols)
        .await?
        .into_iter()
        .map(crate::InstrumentSpec::try_from)
        .collect()
}
```

- [ ] **Step 5: Update S23 example**

Change `crates/tqsdk-session/examples/api_contract_s23_contract_metadata.rs` to use:

```rust
let spec = session
    .query_instrument_specs(&[symbol.as_str()])
    .await?
    .into_iter()
    .next()
    .ok_or("query_instrument_specs returned no rows")?;

println!(
    "symbol={} exchange={} product={} class={:?} tick={} multiplier={} expire={:?}",
    spec.symbol.as_str(),
    spec.exchange_id,
    spec.product_id,
    spec.class,
    spec.price_tick,
    spec.volume_multiple,
    spec.expire_datetime_ns
);
```

- [ ] **Step 6: Run focused checks and commit**

Run:

```bash
cargo test -p tqsdk-session --test session_instrument_spec -- --nocapture
cargo check -p tqsdk-session --example api_contract_s23_contract_metadata
```

Expected: both pass.

Commit:

```bash
git add crates/tqsdk-session/src/instrument.rs crates/tqsdk-session/src/lib.rs crates/tqsdk-session/src/direct_query.rs crates/tqsdk-session/src/metadata.rs crates/tqsdk-session/src/client.rs crates/tqsdk-session/tests/session_instrument_spec.rs crates/tqsdk-session/examples/api_contract_s23_contract_metadata.rs
git commit -m "feat: add typed instrument specs"
```

## Task 5: Stream Diagnostics, Health Status, and Retry Hints

**Files:**
- Modify: `crates/tqsdk-stream/src/error.rs`
- Modify: `crates/tqsdk-stream/src/health.rs`
- Modify: `crates/tqsdk-stream/src/lib.rs`
- Test: `crates/tqsdk-stream/tests/stream_diagnostics.rs`
- Examples:
  - `crates/tqsdk-stream/examples/api_contract_s20_production_daemon_health.rs`
  - `crates/tqsdk-stream/examples/api_contract_s22_error_diagnosis_retry.rs`

- [ ] **Step 1: Write failing tests for stream diagnostics**

Create `crates/tqsdk-stream/tests/stream_diagnostics.rs`:

```rust
use tqsdk_core::{ContractError, RetryHint, SessionPhase};
use tqsdk_session::SessionFacadeError;
use tqsdk_stream::{
    StreamErrorKind, StreamFacadeError, StreamHealthStatus, StreamSessionPhase,
};

mod support;

#[test]
fn stream_errors_expose_stable_kind_and_retry_hint() {
    let lagged = StreamFacadeError::Lagged { skipped: 7 };
    let diagnostic = lagged.diagnostic();
    assert_eq!(diagnostic.kind, StreamErrorKind::Lagged);
    assert_eq!(diagnostic.retry_hint, RetryHint::DoNotRetry);
    assert_eq!(diagnostic.lagged_commits, Some(7));

    let session = StreamFacadeError::Session(SessionFacadeError::from(
        ContractError::transport("websocket recv failed"),
    ));
    let diagnostic = session.diagnostic();
    assert_eq!(diagnostic.kind, StreamErrorKind::Transport);
    assert_eq!(diagnostic.retry_hint, RetryHint::RetryAfterReconnect);
    assert!(session.is_retryable());
}

#[test]
fn stream_health_status_summarizes_operational_state() {
    let stream = support::core_seed::seeded_stream();
    assert_eq!(stream.health().unwrap().status(), StreamHealthStatus::Starting);

    support::core_seed::seed_session_phase_commit(&stream, SessionPhase::Running);
    assert_eq!(stream.health().unwrap().status(), StreamHealthStatus::Healthy);

    support::core_seed::seed_session_phase_commit(&stream, SessionPhase::Reconnecting);
    support::core_seed::seed_session_reconnect_commit(&stream, "transport-error");
    let health = stream.health().unwrap();
    assert_eq!(health.session_phase, Some(StreamSessionPhase::Reconnecting));
    assert_eq!(health.status(), StreamHealthStatus::Recovering);

    stream.close_driver_for_test();
    assert_eq!(stream.health().unwrap().status(), StreamHealthStatus::Closed);
}
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
cargo test -p tqsdk-stream --test stream_diagnostics -- --nocapture
```

Expected: compile failure because diagnostic/status types do not exist.

- [ ] **Step 3: Implement stream error diagnostics**

Add to `crates/tqsdk-stream/src/error.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamErrorKind {
    Validation,
    Auth,
    Transport,
    Http,
    Adapter,
    UnsupportedCommand,
    UnsupportedInput,
    InvalidState,
    MissingValue,
    Lagged,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamErrorDiagnostic {
    pub kind: StreamErrorKind,
    pub retry_hint: tqsdk_core::RetryHint,
    pub message: String,
    pub lagged_commits: Option<u64>,
}

impl StreamFacadeError {
    #[must_use]
    pub fn diagnostic(&self) -> StreamErrorDiagnostic {
        match self {
            Self::Contract(error) => StreamErrorDiagnostic::from_contract(error),
            Self::Session(error) => StreamErrorDiagnostic::from_session(error.diagnostic()),
            Self::MissingValue { path } => StreamErrorDiagnostic {
                kind: StreamErrorKind::MissingValue,
                retry_hint: tqsdk_core::RetryHint::DoNotRetry,
                message: format!("stream value missing at path {}", path.segments().join("/")),
                lagged_commits: None,
            },
            Self::Lagged { skipped } => StreamErrorDiagnostic {
                kind: StreamErrorKind::Lagged,
                retry_hint: tqsdk_core::RetryHint::DoNotRetry,
                message: format!("stream receiver lagged and skipped {skipped} commit(s)"),
                lagged_commits: Some(*skipped),
            },
            Self::Closed => StreamErrorDiagnostic {
                kind: StreamErrorKind::Closed,
                retry_hint: tqsdk_core::RetryHint::DoNotRetry,
                message: "stream driver closed".to_string(),
                lagged_commits: None,
            },
            Self::InvalidState(message) => StreamErrorDiagnostic {
                kind: StreamErrorKind::InvalidState,
                retry_hint: tqsdk_core::RetryHint::DoNotRetry,
                message: format!("invalid stream facade state: {message}"),
                lagged_commits: None,
            },
        }
    }

    #[must_use]
    pub fn is_retryable(&self) -> bool {
        !matches!(self.diagnostic().retry_hint, tqsdk_core::RetryHint::DoNotRetry)
    }
}
```

Add these private helper constructors in the same file:

```rust
impl StreamErrorDiagnostic {
    fn from_contract(error: &tqsdk_core::ContractError) -> Self {
        Self {
            kind: match error.kind() {
                tqsdk_core::ContractErrorKind::Validation => StreamErrorKind::Validation,
                tqsdk_core::ContractErrorKind::Auth => StreamErrorKind::Auth,
                tqsdk_core::ContractErrorKind::Transport => StreamErrorKind::Transport,
                tqsdk_core::ContractErrorKind::Http => StreamErrorKind::Http,
                tqsdk_core::ContractErrorKind::Adapter => StreamErrorKind::Adapter,
                tqsdk_core::ContractErrorKind::UnsupportedCommand => {
                    StreamErrorKind::UnsupportedCommand
                }
                tqsdk_core::ContractErrorKind::UnsupportedInput => {
                    StreamErrorKind::UnsupportedInput
                }
            },
            retry_hint: error.retry_hint(),
            message: error.to_string(),
            lagged_commits: None,
        }
    }

    fn from_session(diagnostic: tqsdk_session::SessionErrorDiagnostic) -> Self {
        Self {
            kind: match diagnostic.kind {
                tqsdk_session::SessionErrorKind::Validation => StreamErrorKind::Validation,
                tqsdk_session::SessionErrorKind::Auth => StreamErrorKind::Auth,
                tqsdk_session::SessionErrorKind::Transport => StreamErrorKind::Transport,
                tqsdk_session::SessionErrorKind::Http => StreamErrorKind::Http,
                tqsdk_session::SessionErrorKind::Adapter => StreamErrorKind::Adapter,
                tqsdk_session::SessionErrorKind::UnsupportedCommand => {
                    StreamErrorKind::UnsupportedCommand
                }
                tqsdk_session::SessionErrorKind::UnsupportedInput => {
                    StreamErrorKind::UnsupportedInput
                }
                tqsdk_session::SessionErrorKind::InvalidState => StreamErrorKind::InvalidState,
            },
            retry_hint: diagnostic.retry_hint,
            message: diagnostic.message,
            lagged_commits: None,
        }
    }
}
```

- [ ] **Step 4: Implement stream health status**

Add to `crates/tqsdk-stream/src/health.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamHealthStatus {
    Starting,
    Healthy,
    Recovering,
    Degraded,
    Closed,
}

impl StreamHealthSnapshot {
    #[must_use]
    pub fn status(&self) -> StreamHealthStatus {
        if self.driver_closed {
            return StreamHealthStatus::Closed;
        }
        if self.reconnect_exhausted() {
            return StreamHealthStatus::Degraded;
        }
        match self.session_phase {
            Some(StreamSessionPhase::Running) => StreamHealthStatus::Healthy,
            Some(StreamSessionPhase::Reconnecting | StreamSessionPhase::Resyncing) => {
                StreamHealthStatus::Recovering
            }
            Some(
                StreamSessionPhase::Idle
                | StreamSessionPhase::Authenticating
                | StreamSessionPhase::Connecting
                | StreamSessionPhase::Bootstrapping,
            )
            | None => StreamHealthStatus::Starting,
            Some(StreamSessionPhase::Closed) => StreamHealthStatus::Closed,
        }
    }

    #[must_use]
    pub fn should_restart(&self) -> bool {
        matches!(self.status(), StreamHealthStatus::Degraded | StreamHealthStatus::Closed)
    }
}
```

Re-export new types from `crates/tqsdk-stream/src/lib.rs`.

- [ ] **Step 5: Promote S22 example to compiled contract**

Replace `crates/tqsdk-stream/examples/api_contract_s22_error_diagnosis_retry.rs` with a compiled example using public diagnostics:

```rust
use tqsdk_core::{ContractError, RetryHint};
use tqsdk_session::SessionFacadeError;
use tqsdk_stream::{StreamErrorKind, StreamFacadeError};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let error = StreamFacadeError::Session(SessionFacadeError::from(ContractError::transport(
        "websocket recv failed",
    )));
    let diagnostic = error.diagnostic();

    match (diagnostic.kind, diagnostic.retry_hint) {
        (StreamErrorKind::Transport, RetryHint::RetryAfterReconnect) => {
            println!("retry after reconnect: {}", diagnostic.message);
        }
        (_, RetryHint::DoNotRetry) => {
            println!("do not retry: {}", diagnostic.message);
        }
        (_, RetryHint::RetryWithBackoff) => {
            println!("retry with backoff: {}", diagnostic.message);
        }
    }

    Ok(())
}
```

Keep the header template and note that business reject classification remains represented by typed order/risk surfaces, not this low-level transport diagnostic API.

- [ ] **Step 6: Update S20 health example**

In `crates/tqsdk-stream/examples/api_contract_s20_production_daemon_health.rs`, print `health.status()` and use `health.should_restart()` instead of only `reconnect_exhausted() || driver_closed`.

- [ ] **Step 7: Run focused checks and commit**

Run:

```bash
cargo test -p tqsdk-stream --test stream_diagnostics -- --nocapture
cargo check -p tqsdk-stream --example api_contract_s20_production_daemon_health
cargo check -p tqsdk-stream --example api_contract_s22_error_diagnosis_retry
```

Expected: all pass.

Commit:

```bash
git add crates/tqsdk-stream/src/error.rs crates/tqsdk-stream/src/health.rs crates/tqsdk-stream/src/lib.rs crates/tqsdk-stream/tests/stream_diagnostics.rs crates/tqsdk-stream/examples/api_contract_s20_production_daemon_health.rs crates/tqsdk-stream/examples/api_contract_s22_error_diagnosis_retry.rs
git commit -m "feat: add stream diagnostics and health status"
```

## Task 6: Public Fan-Out Capacity and Lag Contract

**Files:**
- Modify: `crates/tqsdk-stream/src/builder.rs`
- Modify: `crates/tqsdk-stream/src/api.rs`
- Test: `crates/tqsdk-stream/tests/stream_commit_flow.rs`
- Example: `crates/tqsdk-stream/examples/api_contract_s21_slow_consumer_isolation.rs`

- [ ] **Step 1: Write failing tests for public capacity config**

Add to `crates/tqsdk-stream/tests/stream_commit_flow.rs`:

```rust
#[tokio::test(flavor = "current_thread")]
async fn public_builder_capacity_controls_lag_boundary() {
    use futures::StreamExt;
    use tqsdk_stream::{StreamFacadeError, TqStreamBuilder};

    let stream = TqStreamBuilder::from_session_builder(
        tqsdk_session::SessionClientBuilder::new("demo-user", "demo-pass"),
    )
    .commit_channel_capacity(1)
    .unwrap()
    .build()
    .await
    .unwrap();

    let mut commits = stream.commit_stream().unwrap();
    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 618.0);
    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 619.0);
    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 620.0);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let update = commits.next().await.expect("lag information should be emitted");
    assert!(matches!(
        update,
        Err(StreamFacadeError::Lagged { skipped }) if skipped >= 1
    ));
}

#[test]
fn public_builder_rejects_zero_commit_channel_capacity() {
    let err = tqsdk_stream::TqStreamBuilder::new("demo-user", "demo-pass")
        .commit_channel_capacity(0)
        .unwrap_err();

    assert_eq!(err.diagnostic().kind, tqsdk_stream::StreamErrorKind::InvalidState);
}
```

Keep this test in `stream_commit_flow.rs` because it validates fan-out semantics.

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
cargo test -p tqsdk-stream --test stream_commit_flow public_builder -- --nocapture
```

Expected: compile failure because `commit_channel_capacity` does not exist.

- [ ] **Step 3: Implement builder capacity**

Modify `TqStreamBuilder`:

```rust
#[derive(Debug, Clone)]
pub struct TqStreamBuilder {
    inner: tqsdk_session::SessionClientBuilder,
    commit_channel_capacity: usize,
}

impl TqStreamBuilder {
    #[must_use]
    pub fn from_session_builder(inner: tqsdk_session::SessionClientBuilder) -> Self {
        Self {
            inner,
            commit_channel_capacity: crate::api::DEFAULT_COMMIT_CHANNEL_CAPACITY,
        }
    }

    pub fn commit_channel_capacity(
        mut self,
        capacity: usize,
    ) -> crate::error::Result<Self> {
        if capacity == 0 {
            return Err(crate::StreamFacadeError::InvalidState(
                "commit channel capacity must be greater than zero",
            ));
        }
        self.commit_channel_capacity = capacity;
        Ok(self)
    }

    pub async fn build(self) -> crate::error::Result<TqStream> {
        let session = self.inner.build()?;
        Ok(TqStream::new_with_capacity(
            session,
            self.commit_channel_capacity,
        ))
    }
}
```

Make `DEFAULT_COMMIT_CHANNEL_CAPACITY` and `TqStream::new_with_capacity` visible to the builder as `pub(crate)`.

- [ ] **Step 4: Promote S21 example to compiled contract**

Replace `crates/tqsdk-stream/examples/api_contract_s21_slow_consumer_isolation.rs` with a compiled example that:

- builds `TqStreamBuilder::new(...).commit_channel_capacity(16_384)?.build().await?`;
- creates two independent `commit_stream()` consumers;
- documents that this batch exposes bounded fan-out and lag diagnostics, not a durable sink runtime.

Keep the example simple and avoid manual `tokio::spawn`, `mpsc`, `broadcast`, or `Arc<Mutex<_>>`.

- [ ] **Step 5: Run focused checks and commit**

Run:

```bash
cargo test -p tqsdk-stream --test stream_commit_flow public_builder -- --nocapture
cargo check -p tqsdk-stream --example api_contract_s21_slow_consumer_isolation
```

Expected: both pass.

Commit:

```bash
git add crates/tqsdk-stream/src/builder.rs crates/tqsdk-stream/src/api.rs crates/tqsdk-stream/tests/stream_commit_flow.rs crates/tqsdk-stream/examples/api_contract_s21_slow_consumer_isolation.rs
git commit -m "feat: expose stream fanout capacity"
```

## Task 7: Scenario Review and Architecture Docs

**Files:**
- Modify: `docs/reviews/public-api-scenario-review.md`
- Modify: `docs/scenarios/user-layer-iteration-plan.md`
- Modify: `docs/scenarios/api_gaps/api_contract_s20_production_daemon.rs`
- Modify: `docs/scenarios/api_gaps/api_contract_s21_slow_consumer_isolation.rs`
- Modify: `docs/scenarios/api_gaps/api_contract_s22_error_diagnosis_retry.rs`
- Modify: `docs/architecture/api-stream.md`
- Modify: `docs/architecture/ai-workflow.md`
- Modify: `docs/architecture/README.md`
- Modify: `crates/tqsdk-session/README.md`
- Modify: `crates/tqsdk-stream/README.md`

- [ ] **Step 1: Update scenario statuses conservatively**

Update `docs/reviews/public-api-scenario-review.md`:

- S5 remains `自然`, but evidence should mention `SessionClient::subscribe_quotes` plus `RuntimeReader::read_market_state`.
- S21 moves from `勉强` to `自然` only for bounded fan-out and explicit lag diagnostics. The evidence must mention that durable sink isolation is still a gap.
- S22 moves from `勉强` to `自然` for low-level transport/session/stream diagnostics. The evidence must mention that business reject classification remains covered by order/risk APIs and advanced retry orchestration remains a gap.
- S23 remains `自然`, with evidence updated from `Quote metadata fields` to `InstrumentSpec`.
- S20 remains `勉强`, with evidence updated to `StreamHealthStatus` / `should_restart`; full daemon remains a gap.

- [ ] **Step 2: Narrow gap sketches**

Update the gap files:

- `api_contract_s20_production_daemon.rs`: remove health/status as a gap; keep metrics endpoint, ctrl-c graceful shutdown, and sink lifecycle.
- `api_contract_s21_slow_consumer_isolation.rs`: remove bounded fan-out/lag diagnostics as a gap; keep durable sink runtime and per-sink retry/storage.
- `api_contract_s22_error_diagnosis_retry.rs`: remove string-based transport/session retry diagnosis as a gap; keep full retry policy orchestration and business-level rejection workflow.

- [ ] **Step 3: Update architecture docs**

Update architecture docs with these boundaries:

- `tqsdk-session` owns low-level command helpers and metadata normalization because they are one-shot command/query surfaces.
- `tqsdk-stream` owns fan-out capacity, lag diagnostics, and health status because these are continuous consumption concerns.
- `tqsdk-core` only exposes coarse error kind/retry hint and must not learn stream/sink policy.

- [ ] **Step 4: Run docs grep sanity checks**

Run:

```bash
rg -n "serde_json::Value|provider 私有|Arc<Mutex|手写 Tokio" crates/tqsdk-session/examples/api_contract_s05_bare_market_fast_path.rs crates/tqsdk-session/examples/api_contract_s23_contract_metadata.rs crates/tqsdk-stream/examples/api_contract_s20_production_daemon_health.rs crates/tqsdk-stream/examples/api_contract_s21_slow_consumer_isolation.rs crates/tqsdk-stream/examples/api_contract_s22_error_diagnosis_retry.rs
```

Expected: matches only in `Forbidden` header comments, not in executable example bodies.

- [ ] **Step 5: Commit docs**

Commit:

```bash
git add docs/reviews/public-api-scenario-review.md docs/scenarios/user-layer-iteration-plan.md docs/scenarios/api_gaps/api_contract_s20_production_daemon.rs docs/scenarios/api_gaps/api_contract_s21_slow_consumer_isolation.rs docs/scenarios/api_gaps/api_contract_s22_error_diagnosis_retry.rs docs/architecture/api-stream.md docs/architecture/ai-workflow.md docs/architecture/README.md crates/tqsdk-session/README.md crates/tqsdk-stream/README.md
git commit -m "docs: update low-level scenario status"
```

## Task 8: Full Verification

**Files:**
- No new files unless earlier tasks require fixes.

- [ ] **Step 1: Run required verification**

Run:

```bash
cargo check --workspace --examples
cargo test --workspace
cargo clippy --workspace --examples --all-targets -- -D warnings
```

Expected: all pass.

- [ ] **Step 2: Feature flag check**

This plan is not expected to modify `Cargo.toml` feature flags. If any task changes feature flags, also run:

```bash
cargo check --workspace --no-default-features
cargo check --workspace --all-features --examples
```

Expected: all pass.

- [ ] **Step 3: Final status check**

Run:

```bash
git status --short
git log --oneline -8
```

Expected: only unrelated local files may remain untracked; all plan changes should be committed in focused commits.

## Self-Review

- Spec coverage: this plan targets the lower-level scenarios S5, S20, S21, S22, and S23, and intentionally avoids task/data/provider aggregation work.
- Crate boundary check: core only receives coarse diagnostics; session receives command/query helpers; stream receives continuous consumption controls.
- Public API check: examples must not expose provider-private types, raw protocol sessions, manual channels, `Arc<Mutex<_>>`, or string-based error classification.
- Remaining gaps are explicit: durable sink isolation, full daemon supervisor, metrics endpoint, graceful shutdown, retry orchestration, and multi-provider aggregation stay out of this batch.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-27-low-level-session-stream-foundation.md`.

Two execution options:

1. Subagent-Driven (recommended): dispatch a fresh worker per task and review between tasks.
2. Inline Execution: execute tasks in this session using `superpowers:executing-plans`, with verification checkpoints.
