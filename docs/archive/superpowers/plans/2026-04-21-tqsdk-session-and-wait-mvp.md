# `tqsdk-session` + `tqsdk-wait` MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a shared `tqsdk-session` crate for mode-agnostic live session/direct-query access, then build a `tqsdk-wait` crate on top of it that provides a Python-style single-owner `wait_update()` facade without changing `tqsdk-core`.

**Architecture:** The implementation is split into two subprojects within one plan: first add a shared thin session layer that owns live session bootstrap and direct query/schema access; then add a wait facade that consumes the same `RuntimeReader + UpdateCursor` substrate and exposes `TqApi`, refs, windows, `wait_update()`, and `is_changing()`. `tqsdk-stream` is explicitly deferred to a separate plan so that commit/revision semantics and direct-query boundaries can be stabilized first.

**Tech Stack:** Rust 2024 edition, Cargo workspace, Tokio, `tqsdk-core`, `serde`, `serde_json`, `reqwest`, `yawc`.

---

## File Structure

**Workspace**
- Modify: `Cargo.toml`
- Modify: `README.md`
- Modify: `docs/architecture/README.md`
- Modify: `docs/architecture/roadmap.md`

**New shared session crate**
- Create: `crates/tqsdk-session/Cargo.toml`
- Create: `crates/tqsdk-session/README.md`
- Create: `crates/tqsdk-session/src/lib.rs`
- Create: `crates/tqsdk-session/src/error.rs`
- Create: `crates/tqsdk-session/src/config.rs`
- Create: `crates/tqsdk-session/src/builder.rs`
- Create: `crates/tqsdk-session/src/client.rs`
- Create: `crates/tqsdk-session/src/direct_query.rs`
- Create: `crates/tqsdk-session/tests/session_builder.rs`
- Create: `crates/tqsdk-session/tests/session_direct_query.rs`

**New wait facade crate**
- Create: `crates/tqsdk-wait/Cargo.toml`
- Create: `crates/tqsdk-wait/README.md`
- Create: `crates/tqsdk-wait/src/lib.rs`
- Create: `crates/tqsdk-wait/src/error.rs`
- Create: `crates/tqsdk-wait/src/builder.rs`
- Create: `crates/tqsdk-wait/src/api.rs`
- Create: `crates/tqsdk-wait/src/driver.rs`
- Create: `crates/tqsdk-wait/src/change.rs`
- Create: `crates/tqsdk-wait/src/refs/mod.rs`
- Create: `crates/tqsdk-wait/src/refs/quote.rs`
- Create: `crates/tqsdk-wait/src/refs/trading_status.rs`
- Create: `crates/tqsdk-wait/src/refs/trade.rs`
- Create: `crates/tqsdk-wait/src/refs/kline.rs`
- Create: `crates/tqsdk-wait/src/refs/tick.rs`
- Create: `crates/tqsdk-wait/src/views/mod.rs`
- Create: `crates/tqsdk-wait/src/views/kline_window.rs`
- Create: `crates/tqsdk-wait/src/views/tick_window.rs`
- Create: `crates/tqsdk-wait/tests/wait_api_surface.rs`
- Create: `crates/tqsdk-wait/tests/wait_api_market.rs`
- Create: `crates/tqsdk-wait/tests/wait_api_trade.rs`
- Create: `crates/tqsdk-wait/tests/wait_api_is_changing.rs`
- Create: `crates/tqsdk-wait/tests/support/mod.rs`
- Create: `crates/tqsdk-wait/tests/support/core_seed.rs`
- Create: `crates/tqsdk-wait/examples/quote_wait.rs`

## Scope Notes

- This plan intentionally implements `tqsdk-session` and `tqsdk-wait` only.
- `tqsdk-stream` gets its own implementation plan after the shared session crate and wait facade semantics are proven.
- Direct query/schema/metadata APIs live in `tqsdk-session`; they must not be reintroduced inside `tqsdk-wait`.

### Task 1: Add Workspace Members And Crate Skeletons

**Files:**
- Modify: `Cargo.toml`
- Modify: `README.md`
- Modify: `docs/architecture/README.md`
- Modify: `docs/architecture/roadmap.md`
- Create: `crates/tqsdk-session/Cargo.toml`
- Create: `crates/tqsdk-session/README.md`
- Create: `crates/tqsdk-session/src/lib.rs`
- Create: `crates/tqsdk-wait/Cargo.toml`
- Create: `crates/tqsdk-wait/README.md`
- Create: `crates/tqsdk-wait/src/lib.rs`

- [ ] **Step 1: Verify the new packages do not exist yet**

Run:

```bash
cargo test -p tqsdk-session -q
```

Expected: FAIL with `package ID specification 'tqsdk-session' did not match any packages`

- [ ] **Step 2: Add both crates to the workspace manifest**

Update `Cargo.toml` to:

```toml
[workspace]
members = [
    "crates/tqsdk-core",
    "crates/tqsdk-session",
    "crates/tqsdk-wait",
]
default-members = [
    "crates/tqsdk-core",
    "crates/tqsdk-session",
    "crates/tqsdk-wait",
]
resolver = "3"

[workspace.dependencies]
base64 = "0.22"
futures = "0.3"
reqwest = { version = "0.12", default-features = false, features = ["brotli", "gzip", "json", "rustls-tls", "stream"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
sha1 = "0.10"
tokio = { version = "1", features = ["io-util", "net", "rt", "time"] }
url = "2"
yawc = "0.3.3"
```

- [ ] **Step 3: Add minimal manifests and `lib.rs` skeletons for both crates**

Create `crates/tqsdk-session/Cargo.toml`:

```toml
[package]
name = "tqsdk-session"
version = "0.1.0"
edition = "2024"
readme = "README.md"

[dependencies]
serde.workspace = true
serde_json.workspace = true
tokio.workspace = true
tqsdk-core = { path = "../tqsdk-core" }
```

Create `crates/tqsdk-session/src/lib.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

pub mod builder;
pub mod client;
pub mod config;
pub mod direct_query;
pub mod error;
```

Create `crates/tqsdk-wait/Cargo.toml`:

```toml
[package]
name = "tqsdk-wait"
version = "0.1.0"
edition = "2024"
readme = "README.md"

[dependencies]
serde.workspace = true
serde_json.workspace = true
tokio.workspace = true
tqsdk-core = { path = "../tqsdk-core" }
tqsdk-session = { path = "../tqsdk-session" }
```

Create `crates/tqsdk-wait/src/lib.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

pub mod api;
pub mod builder;
pub mod change;
pub mod driver;
pub mod error;
pub mod refs;
pub mod views;

pub use api::TqApi;
pub use builder::TqApiBuilder;
pub use error::{Result, WaitFacadeError};
```

- [ ] **Step 4: Update repo docs to mention the two new crates and `tqsdk-stream` deferral**

Add this table row pattern to `README.md`:

```md
| `tqsdk-session` | `crates/tqsdk-session` | 模式无关的 live session / direct-query 薄层 |
| `tqsdk-wait` | `crates/tqsdk-wait` | Python 风格的单推进点 facade |
```

Add this note to `docs/architecture/roadmap.md`:

```md
- `tqsdk-session` 先于 `tqsdk-wait` 落地，用于承载 direct query / schema / metadata 与共享 session 装配。
- `tqsdk-stream` 延后到 `tqsdk-session + tqsdk-wait` 稳定后单独规划。
```

- [ ] **Step 5: Run workspace format and empty-crate compilation**

Run:

```bash
cargo fmt --all
cargo test -p tqsdk-session -q
cargo test -p tqsdk-wait -q
```

Expected:
- `cargo fmt --all` exits `0`
- both crate test commands pass with `0` tests

- [ ] **Step 6: Commit the workspace scaffolding**

Run:

```bash
git add Cargo.toml README.md docs/architecture/README.md docs/architecture/roadmap.md crates/tqsdk-session crates/tqsdk-wait
git commit -m "feat: scaffold session and wait facade crates"
```

### Task 2: Implement `tqsdk-session` Config, Builder, And Session Owner

**Files:**
- Create: `crates/tqsdk-session/src/error.rs`
- Create: `crates/tqsdk-session/src/config.rs`
- Create: `crates/tqsdk-session/src/builder.rs`
- Create: `crates/tqsdk-session/src/client.rs`
- Modify: `crates/tqsdk-session/src/lib.rs`
- Test: `crates/tqsdk-session/tests/session_builder.rs`

- [ ] **Step 1: Write the failing builder/session owner tests**

Create `crates/tqsdk-session/tests/session_builder.rs`:

```rust
use tqsdk_session::{SessionClientBuilder, SessionFacadeConfig};

#[test]
fn builder_keeps_explicit_facade_config() {
    let config = SessionFacadeConfig::default().with_default_view_width(256);
    let builder = SessionClientBuilder::new("user", "pass").facade_config(config.clone());
    assert_eq!(builder.facade_config_ref().default_view_width, 256);
}

#[test]
fn builder_accepts_explicit_query_schema_and_replay_urls() {
    let builder = SessionClientBuilder::new("user", "pass")
        .query_url("https://query.example.com/graphql")
        .schema_url("https://schema.example.com/latest.json")
        .replay_url("wss://replay.example.com/feed");

    let endpoints = builder.endpoints();
    assert_eq!(endpoints.query_url.as_deref(), Some("https://query.example.com/graphql"));
    assert_eq!(endpoints.schema_url.as_deref(), Some("https://schema.example.com/latest.json"));
    assert_eq!(endpoints.replay_url.as_deref(), Some("wss://replay.example.com/feed"));
}
```

- [ ] **Step 2: Run the new tests and confirm they fail**

Run:

```bash
cargo test -p tqsdk-session -q --test session_builder
```

Expected: FAIL with unresolved imports for `SessionClientBuilder` and `SessionFacadeConfig`

- [ ] **Step 3: Implement facade config and error types**

Create `crates/tqsdk-session/src/config.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionFacadeConfig {
    pub default_view_width: usize,
}

impl Default for SessionFacadeConfig {
    fn default() -> Self {
        Self {
            default_view_width: 200,
        }
    }
}

impl SessionFacadeConfig {
    pub fn with_default_view_width(mut self, default_view_width: usize) -> Self {
        self.default_view_width = default_view_width.max(1);
        self
    }
}
```

Create `crates/tqsdk-session/src/error.rs`:

```rust
use std::fmt;

pub type Result<T> = std::result::Result<T, SessionFacadeError>;

#[derive(Debug)]
pub enum SessionFacadeError {
    Core(tqsdk_core::ContractError),
    InvalidState(&'static str),
}

impl From<tqsdk_core::ContractError> for SessionFacadeError {
    fn from(value: tqsdk_core::ContractError) -> Self {
        Self::Core(value)
    }
}

impl fmt::Display for SessionFacadeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Core(err) => write!(f, "{err}"),
            Self::InvalidState(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for SessionFacadeError {}
```

- [ ] **Step 4: Implement `SessionClientBuilder` and a minimal `SessionClient` owner**

Create `crates/tqsdk-session/src/builder.rs`:

```rust
use tqsdk_core::{EndpointConfig, RuntimeHandle};

use crate::config::SessionFacadeConfig;

#[derive(Debug, Clone)]
pub struct SessionClientBuilder {
    auth_user: String,
    auth_pass: String,
    endpoints: EndpointConfig,
    facade_config: SessionFacadeConfig,
}

impl SessionClientBuilder {
    pub fn new(auth_user: impl Into<String>, auth_pass: impl Into<String>) -> Self {
        Self {
            auth_user: auth_user.into(),
            auth_pass: auth_pass.into(),
            endpoints: EndpointConfig::from_env(),
            facade_config: SessionFacadeConfig::default(),
        }
    }

    pub fn facade_config(mut self, facade_config: SessionFacadeConfig) -> Self {
        self.facade_config = facade_config;
        self
    }

    pub fn facade_config_ref(&self) -> &SessionFacadeConfig {
        &self.facade_config
    }

    pub fn query_url(mut self, url: impl Into<String>) -> Self {
        self.endpoints = self.endpoints.clone().with_query_url(url.into());
        self
    }

    pub fn schema_url(mut self, url: impl Into<String>) -> Self {
        self.endpoints = self.endpoints.clone().with_schema_url(url.into());
        self
    }

    pub fn replay_url(mut self, url: impl Into<String>) -> Self {
        self.endpoints = self.endpoints.clone().with_replay_url(url.into());
        self
    }

    pub fn endpoints(&self) -> &EndpointConfig {
        &self.endpoints
    }

    pub async fn build(self) -> crate::error::Result<crate::client::SessionClient> {
        let handle = RuntimeHandle::new();
        Ok(crate::client::SessionClient::new(handle, self.facade_config))
    }
}
```

Create `crates/tqsdk-session/src/client.rs`:

```rust
use tqsdk_core::{RuntimeHandle, RuntimeReader, SessionBootstrap, SessionRuntime};

use crate::config::SessionFacadeConfig;

#[derive(Clone)]
pub struct SessionClient {
    handle: RuntimeHandle,
    reader: RuntimeReader,
    runtime: SessionRuntime,
    facade_config: SessionFacadeConfig,
}

impl SessionClient {
    pub(crate) fn new(handle: RuntimeHandle, facade_config: SessionFacadeConfig) -> Self {
        let reader = handle.reader();
        let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
        Self {
            handle,
            reader,
            runtime,
            facade_config,
        }
    }

    pub fn handle(&self) -> &RuntimeHandle {
        &self.handle
    }

    pub fn reader(&self) -> &RuntimeReader {
        &self.reader
    }

    pub fn runtime(&self) -> &SessionRuntime {
        &self.runtime
    }

    pub fn facade_config(&self) -> &SessionFacadeConfig {
        &self.facade_config
    }
}
```

- [ ] **Step 5: Export the builder/config/client surface**

Update `crates/tqsdk-session/src/lib.rs` to:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

pub mod builder;
pub mod client;
pub mod config;
pub mod direct_query;
pub mod error;

pub use builder::SessionClientBuilder;
pub use client::SessionClient;
pub use config::SessionFacadeConfig;
pub use error::{Result, SessionFacadeError};
```

- [ ] **Step 6: Re-run the builder test and full crate test suite**

Run:

```bash
cargo test -p tqsdk-session -q --test session_builder
cargo test -p tqsdk-session -q
```

Expected: both commands PASS

- [ ] **Step 7: Commit the session builder/session owner layer**

Run:

```bash
git add crates/tqsdk-session/src crates/tqsdk-session/tests/session_builder.rs
git commit -m "feat: add session facade builder and owner"
```

### Task 3: Implement `tqsdk-session` Direct Query And Core Driving Helpers

**Files:**
- Create: `crates/tqsdk-session/src/direct_query.rs`
- Modify: `crates/tqsdk-session/src/client.rs`
- Modify: `crates/tqsdk-session/src/lib.rs`
- Test: `crates/tqsdk-session/tests/session_direct_query.rs`

- [ ] **Step 1: Write the failing direct-query and helper tests**

Create `crates/tqsdk-session/tests/session_direct_query.rs`:

```rust
use serde_json::json;
use tqsdk_core::RuntimeHandle;
use tqsdk_session::{SessionClient, SessionFacadeConfig};

#[test]
fn test_only_session_client_keeps_handle_and_reader_aligned() {
    let handle = RuntimeHandle::new();
    let client = SessionClient::new_for_test_with_handle(handle, SessionFacadeConfig::default());
    assert_eq!(client.reader().head_revision(), client.handle().reader().head_revision());
}

#[tokio::test(flavor = "current_thread")]
async fn graphql_fetch_submits_query_command() {
    let handle = RuntimeHandle::new();
    let client = SessionClient::new_for_test_with_handle(handle, SessionFacadeConfig::default());
    let command_id = client.query_graphql("query Ping { ping }", Some(json!({"x": 1}))).await.unwrap();
    assert!(command_id.get() > 0);
}
```

- [ ] **Step 2: Run the new direct-query tests and confirm they fail**

Run:

```bash
cargo test -p tqsdk-session -q --test session_direct_query
```

Expected: FAIL with missing `new_for_test` and `query_graphql`
Expected: FAIL with missing `new_for_test_with_handle` and `query_graphql`

- [ ] **Step 3: Add a test-only constructor and direct query helper surface**

Create `crates/tqsdk-session/src/direct_query.rs`:

```rust
use serde_json::Value;
use tqsdk_core::CommandId;

use crate::error::Result;

pub trait SessionDirectQuery {
    async fn query_graphql(&self, query: &str, variables: Option<Value>) -> Result<CommandId>;
    async fn refresh_schema(&self, schema_id: &str, path: &str) -> Result<CommandId>;
}
```

Update `crates/tqsdk-session/src/client.rs` with:

```rust
use serde_json::Value;
use tqsdk_core::{CommandId, QueryCommand, QueryId, Runtime, RuntimeCommand, SchemaCommand, SchemaId};

impl SessionClient {
    #[doc(hidden)]
    pub fn new_for_test_with_handle(
        handle: RuntimeHandle,
        facade_config: SessionFacadeConfig,
    ) -> Self {
        Self::new(handle, facade_config)
    }

    pub async fn query_graphql(
        &self,
        query: &str,
        variables: Option<Value>,
    ) -> crate::error::Result<CommandId> {
        let query_id = QueryId::new(format!("query-{}", query.len()));
        let command = RuntimeCommand::Query(QueryCommand::Fetch {
            query_id: query_id.clone(),
            query: query.to_string(),
            variables,
        });
        Ok(self.handle.submit(command).await?)
    }

    pub async fn refresh_schema(
        &self,
        schema_id: &str,
        path: &str,
    ) -> crate::error::Result<CommandId> {
        let command = RuntimeCommand::Schema(SchemaCommand::Refresh {
            schema_id: SchemaId::new(schema_id),
            path: path.to_string(),
        });
        Ok(self.handle.submit(command).await?)
    }
}
```

- [ ] **Step 4: Add low-level driving helpers that `tqsdk-wait` will reuse**

Extend `crates/tqsdk-session/src/client.rs` with:

```rust
impl SessionClient {
    pub fn reader_clone(&self) -> RuntimeReader {
        self.reader.clone()
    }

    pub fn runtime_clone(&self) -> SessionRuntime {
        self.runtime.clone()
    }

    pub async fn submit(
        &self,
        command: tqsdk_core::RuntimeCommand,
    ) -> crate::error::Result<tqsdk_core::CommandId> {
        Ok(self.handle.submit(command).await?)
    }

    pub async fn drain_dispatches(
        &self,
    ) -> crate::error::Result<Vec<tqsdk_core::OutboundDispatch>> {
        Ok(self.handle.drain_dispatches()?)
    }
}
```

- [ ] **Step 5: Re-run direct-query tests and whole session crate**

Run:

```bash
cargo test -p tqsdk-session -q --test session_direct_query
cargo test -p tqsdk-session -q
```

Expected: PASS

- [ ] **Step 6: Commit the direct-query/shared-helper layer**

Run:

```bash
git add crates/tqsdk-session/src/direct_query.rs crates/tqsdk-session/src/client.rs crates/tqsdk-session/tests/session_direct_query.rs
git commit -m "feat: add session direct query surface"
```

### Task 4: Implement `tqsdk-wait` Driver And `TqApi` Wait Semantics

**Files:**
- Create: `crates/tqsdk-wait/src/error.rs`
- Create: `crates/tqsdk-wait/src/builder.rs`
- Create: `crates/tqsdk-wait/src/api.rs`
- Create: `crates/tqsdk-wait/src/driver.rs`
- Modify: `crates/tqsdk-wait/src/lib.rs`
- Test: `crates/tqsdk-wait/tests/wait_api_surface.rs`
- Test: `crates/tqsdk-wait/tests/support/mod.rs`
- Test: `crates/tqsdk-wait/tests/support/core_seed.rs`

- [ ] **Step 1: Write failing tests for deferred commits and single-owner waits**

Create `crates/tqsdk-wait/tests/wait_api_surface.rs`:

```rust
use std::time::Duration;

use tqsdk_wait::TqApi;

mod support;

#[tokio::test(flavor = "current_thread")]
async fn wait_update_returns_deferred_commit_before_polling() {
    let mut api = support::seeded_api();
    support::seed_quote_commit(&mut api, "SHFE.au2602", 618.5);

    assert!(api.wait_update(None).await.unwrap());
    assert!(api.last_commit().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn concurrent_wait_update_is_rejected() {
    let mut api = support::seeded_api();
    let first = api.begin_wait_for_test().unwrap();
    let err = api.begin_wait_for_test().unwrap_err();
    drop(first);
    assert!(matches!(err, tqsdk_wait::WaitFacadeError::ConcurrentWaitUpdate));
}

#[tokio::test(flavor = "current_thread")]
async fn wait_update_timeout_returns_false() {
    let mut api = support::seeded_api();
    let ok = api.wait_update(Some(tokio::time::Instant::now() + Duration::from_millis(10))).await.unwrap();
    assert!(!ok);
}
```

- [ ] **Step 2: Run the wait surface tests and confirm they fail**

Run:

```bash
cargo test -p tqsdk-wait -q --test wait_api_surface
```

Expected: FAIL with missing `TqApi` and support helpers

- [ ] **Step 3: Implement facade error, builder, driver skeleton, and `TqApi`**

Create `crates/tqsdk-wait/src/error.rs`:

```rust
use std::fmt;

pub type Result<T> = std::result::Result<T, WaitFacadeError>;

#[derive(Debug)]
pub enum WaitFacadeError {
    Session(tqsdk_session::SessionFacadeError),
    Core(tqsdk_core::ContractError),
    ConcurrentWaitUpdate,
    InvalidState(&'static str),
}

impl From<tqsdk_session::SessionFacadeError> for WaitFacadeError {
    fn from(value: tqsdk_session::SessionFacadeError) -> Self {
        Self::Session(value)
    }
}

impl From<tqsdk_core::ContractError> for WaitFacadeError {
    fn from(value: tqsdk_core::ContractError) -> Self {
        Self::Core(value)
    }
}

impl fmt::Display for WaitFacadeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session(err) => write!(f, "{err}"),
            Self::Core(err) => write!(f, "{err}"),
            Self::ConcurrentWaitUpdate => f.write_str("concurrent wait_update"),
            Self::InvalidState(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for WaitFacadeError {}
```

Create `crates/tqsdk-wait/src/driver.rs`:

```rust
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tqsdk_core::{CommitResult, RuntimeReader, SessionRuntime, UpdateCursor};
use tqsdk_session::SessionClient;

pub(crate) struct WaitDriver {
    pub(crate) session: SessionClient,
    pub(crate) reader: RuntimeReader,
    pub(crate) cursor: UpdateCursor,
    pub(crate) runtime: SessionRuntime,
    pub(crate) deferred_commits: VecDeque<CommitResult>,
    pub(crate) last_commit: Option<CommitResult>,
    pub(crate) waiting: AtomicBool,
    pub(crate) next_order_seq: AtomicU64,
}

impl WaitDriver {
    pub(crate) fn begin_wait(&self) -> Result<WaitGuard, crate::error::WaitFacadeError> {
        if self.waiting.swap(true, Ordering::SeqCst) {
            return Err(crate::error::WaitFacadeError::ConcurrentWaitUpdate);
        }
        Ok(WaitGuard(&self.waiting))
    }
}

#[doc(hidden)]
pub struct WaitGuard<'a>(&'a AtomicBool);

impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}
```

Create `crates/tqsdk-wait/src/builder.rs`:

```rust
use tqsdk_session::SessionClientBuilder;

use crate::api::TqApi;

pub struct TqApiBuilder {
    inner: SessionClientBuilder,
}

impl TqApiBuilder {
    pub fn new(auth_user: impl Into<String>, auth_pass: impl Into<String>) -> Self {
        Self {
            inner: SessionClientBuilder::new(auth_user, auth_pass),
        }
    }

    pub async fn build(self) -> crate::error::Result<TqApi> {
        let session = self.inner.build().await?;
        Ok(TqApi::new(session))
    }
}
```

Create `crates/tqsdk-wait/src/api.rs`:

```rust
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64};

use tqsdk_core::CommitResult;
use tqsdk_session::SessionClient;

pub struct TqApi {
    pub(crate) driver: crate::driver::WaitDriver,
}

impl TqApi {
    pub fn new(session: SessionClient) -> Self {
        let handle = session.handle().clone();
        Self::new_for_test(handle, session)
    }

    #[doc(hidden)]
    pub fn new_for_test(handle: tqsdk_core::RuntimeHandle, session: SessionClient) -> Self {
        let reader = handle.reader();
        let cursor = reader.cursor();
        let runtime = session.runtime_clone();
        Self {
            driver: crate::driver::WaitDriver {
                session,
                reader,
                cursor,
                runtime,
                deferred_commits: VecDeque::new(),
                last_commit: None,
                waiting: AtomicBool::new(false),
                next_order_seq: AtomicU64::new(1),
            },
        }
    }

    pub async fn wait_update(
        &mut self,
        _deadline: Option<tokio::time::Instant>,
    ) -> crate::error::Result<bool> {
        let _guard = self.driver.begin_wait()?;
        if let Some(commit) = self.driver.deferred_commits.pop_front() {
            self.driver.last_commit = Some(commit);
            return Ok(true);
        }
        Ok(false)
    }

    pub fn last_commit(&self) -> Option<&CommitResult> {
        self.driver.last_commit.as_ref()
    }

    #[doc(hidden)]
    pub fn begin_wait_for_test(&self) -> crate::error::Result<crate::driver::WaitGuard<'_>> {
        self.driver.begin_wait()
    }

    #[doc(hidden)]
    pub fn handle_for_test(&self) -> tqsdk_core::RuntimeHandle {
        self.driver.runtime.handle()
    }

    #[doc(hidden)]
    pub fn push_deferred_commit_for_test(&mut self, commit: CommitResult) {
        self.driver.deferred_commits.push_back(commit);
    }
}
```

- [ ] **Step 4: Add test support that seeds core commits without network access**

Create `crates/tqsdk-wait/tests/support/mod.rs`:

```rust
mod core_seed;

pub use core_seed::*;
```

Create `crates/tqsdk-wait/tests/support/core_seed.rs`:

```rust
use serde_json::json;
use tqsdk_core::{AdapterRegistry, CommitScope, InputPayload, IoEvent, ProtocolDomain, RuntimeHandle};
use tqsdk_session::{SessionClient, SessionFacadeConfig};
use tqsdk_wait::TqApi;

pub fn seeded_api() -> TqApi {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    let handle = RuntimeHandle::with_adapters(adapters);
    let session = SessionClient::new_for_test_with_handle(handle.clone(), SessionFacadeConfig::default());
    TqApi::new_for_test(handle, session)
}

pub fn seed_quote_commit(api: &mut TqApi, symbol: &str, last_price: f64) {
    let commit = api
        .handle_for_test()
        .ingest(
            tqsdk_core::RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "quotes": {
                        symbol: {
                            "last_price": last_price,
                            "datetime": "2026-04-21 09:30:00.000000"
                        }
                    }
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .unwrap();
    api.push_deferred_commit_for_test(commit);
}

pub fn seed_ready_kline_chart(api: &mut TqApi, symbol: &str, duration_ns: i64, view_width: usize) {
    let chart_id = format!("wait-kline-{symbol}-{duration_ns}-{view_width}");
    let commit = api
        .handle_for_test()
        .ingest(
            tqsdk_core::RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "charts": {
                        chart_id: {
                            "state": {
                                "ins_list": symbol,
                                "duration": duration_ns,
                            },
                            "left_id": 100,
                            "right_id": 101,
                            "more_data": false,
                            "ready": true,
                        }
                    },
                    "klines": {
                        symbol: {
                            duration_ns.to_string(): {
                                "data": {
                                    "100": {
                                        "datetime": 1_713_660_000_000_000_000_i64,
                                        "open": 618.0,
                                        "high": 620.0,
                                        "low": 617.0,
                                        "close": 619.0,
                                        "volume": 12,
                                        "open_oi": 100,
                                        "close_oi": 101
                                    },
                                    "101": {
                                        "datetime": 1_713_660_060_000_000_000_i64,
                                        "open": 619.0,
                                        "high": 621.0,
                                        "low": 618.0,
                                        "close": 620.0,
                                        "volume": 15,
                                        "open_oi": 101,
                                        "close_oi": 103
                                    }
                                }
                            }
                        }
                    }
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .unwrap();
    api.push_deferred_commit_for_test(commit);
}

pub fn seed_trade_snapshot(api: &mut TqApi, account_id: &str, symbol: &str) {
    let commit = api
        .handle_for_test()
        .ingest(
            tqsdk_core::RuntimeInput::Io(IoEvent {
                route: "trade".to_string(),
                domains: vec![ProtocolDomain::Trade],
                payload: InputPayload::Json(json!({
                    "trade": {
                        account_id: {
                            "accounts": {
                                "CNY": {
                                    "balance": 100000.0,
                                    "available": 80000.0
                                }
                            },
                            "positions": {
                                symbol: {
                                    "exchange_id": "SHFE",
                                    "instrument_id": "au2602",
                                    "pos_long": 1,
                                    "pos": 1
                                }
                            },
                            "orders": {
                                "wait-order-1": {
                                    "order_id": "wait-order-1",
                                    "exchange_id": "SHFE",
                                    "instrument_id": "au2602",
                                    "direction": "BUY",
                                    "offset": "OPEN",
                                    "status": "ALIVE"
                                }
                            },
                            "trades": {
                                "wait-trade-1": {
                                    "trade_id": "wait-trade-1",
                                    "order_id": "wait-order-1",
                                    "exchange_id": "SHFE",
                                    "instrument_id": "au2602",
                                    "direction": "BUY",
                                    "offset": "OPEN",
                                    "price": 618.0,
                                    "volume": 1
                                }
                            }
                        }
                    }
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .unwrap();
    api.push_deferred_commit_for_test(commit);
}
```

- [ ] **Step 5: Re-run the wait surface tests until they pass**

Run:

```bash
cargo test -p tqsdk-wait -q --test wait_api_surface
```

Expected: PASS

- [ ] **Step 6: Commit the wait driver and API skeleton**

Run:

```bash
git add crates/tqsdk-wait/src crates/tqsdk-wait/tests/wait_api_surface.rs crates/tqsdk-wait/tests/support
git commit -m "feat: add wait facade driver skeleton"
```

### Task 5: Implement `is_changing()` And Scalar State Refs

**Files:**
- Create: `crates/tqsdk-wait/src/change.rs`
- Create: `crates/tqsdk-wait/src/refs/mod.rs`
- Create: `crates/tqsdk-wait/src/refs/quote.rs`
- Create: `crates/tqsdk-wait/src/refs/trading_status.rs`
- Create: `crates/tqsdk-wait/src/refs/trade.rs`
- Modify: `crates/tqsdk-wait/src/api.rs`
- Test: `crates/tqsdk-wait/tests/wait_api_is_changing.rs`

- [ ] **Step 1: Write failing tests for object-level and field-level change matching**

Create `crates/tqsdk-wait/tests/wait_api_is_changing.rs`:

```rust
mod support;

#[tokio::test(flavor = "current_thread")]
async fn quote_change_is_visible_after_wait_update() {
    let mut api = support::seeded_api();
    let quote = api.quote_ref("SHFE.au2602");
    support::seed_quote_commit(&mut api, "SHFE.au2602", 619.0);

    assert!(api.wait_update(None).await.unwrap());
    assert!(api.is_changing(&quote).unwrap());
    assert!(api.is_changing_fields(&quote, &["last_price"]).unwrap());
    assert!(!api.is_changing_fields(&quote, &["ask_price1"]).unwrap());
}
```

- [ ] **Step 2: Run the change-matching test and confirm it fails**

Run:

```bash
cargo test -p tqsdk-wait -q --test wait_api_is_changing
```

Expected: FAIL with missing `quote_ref`, `is_changing`, and `is_changing_fields`

- [ ] **Step 3: Implement ref identity and change matching**

Create `crates/tqsdk-wait/src/change.rs`:

```rust
use tqsdk_core::{ChangeSet, ObjectKey, StatePath};

pub trait ChangeTrackedRef {
    fn object_key(&self) -> Option<ObjectKey>;
    fn state_path(&self) -> StatePath;
}

pub fn matches_any(changes: &ChangeSet, target: &impl ChangeTrackedRef) -> bool {
    if let Some(key) = target.object_key()
        && changes.object_hits.contains(&key)
    {
        return true;
    }
    changes.path_hits.iter().any(|path| path == &target.state_path())
}

pub fn matches_fields(
    changes: &ChangeSet,
    target: &impl ChangeTrackedRef,
    fields: &[&str],
) -> bool {
    let Some(key) = target.object_key() else {
        return false;
    };
    changes.field_hits.iter().any(|hit| {
        hit.object == key && fields.iter().any(|field| *field == hit.field)
    })
}
```

Create `crates/tqsdk-wait/src/refs/mod.rs`:

```rust
mod quote;
mod trade;
mod trading_status;

pub use quote::QuoteRef;
pub use trade::{AccountRef, OrderRef, PositionRef, TradeRef};
pub use trading_status::TradingStatusRef;
```

Create `crates/tqsdk-wait/src/refs/quote.rs`:

```rust
use tqsdk_core::{ObjectKey, Quote, StatePath, Symbol};

use crate::api::TqApi;
use crate::change::ChangeTrackedRef;

#[derive(Debug, Clone)]
pub struct QuoteRef {
    symbol: Symbol,
}

impl QuoteRef {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: Symbol::new(symbol.into()),
        }
    }

    pub fn is_ready(&self, api: &TqApi) -> crate::error::Result<bool> {
        Ok(self.snapshot(api)?.is_some())
    }

    pub fn snapshot(&self, api: &TqApi) -> crate::error::Result<Option<Quote>> {
        let guard = api.driver.reader.read();
        guard
            .decode_path::<Quote>(&["quotes", self.symbol.as_str()])
            .map_err(Into::into)
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<Quote> {
        self.snapshot(api)?
            .ok_or(crate::error::WaitFacadeError::InvalidState("quote not ready"))
    }
}

impl ChangeTrackedRef for QuoteRef {
    fn object_key(&self) -> Option<ObjectKey> {
        Some(ObjectKey::Quote {
            symbol: self.symbol.clone(),
        })
    }

    fn state_path(&self) -> StatePath {
        StatePath::new(["quotes", self.symbol.as_str()])
    }
}
```

Create `crates/tqsdk-wait/src/refs/trade.rs`:

```rust
use tqsdk_core::{Account, AccountId, ObjectKey, Order, OrderId, Position, StatePath, Symbol, Trade, TradeId};

use crate::{api::TqApi, change::ChangeTrackedRef};

#[derive(Debug, Clone)]
pub struct AccountRef {
    account_id: AccountId,
}

impl AccountRef {
    pub fn new(account_id: impl Into<String>) -> Self {
        Self {
            account_id: AccountId::new(account_id.into()),
        }
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<Account> {
        api.driver
            .reader
            .read()
            .decode_path::<Account>(&["trade", self.account_id.as_str(), "accounts", "CNY"])?
            .ok_or(crate::error::WaitFacadeError::InvalidState("account not ready"))
    }
}

#[derive(Debug, Clone)]
pub struct PositionRef {
    account_id: AccountId,
    symbol: Symbol,
}

impl PositionRef {
    pub fn new(account_id: impl Into<String>, symbol: impl Into<String>) -> Self {
        Self {
            account_id: AccountId::new(account_id.into()),
            symbol: Symbol::new(symbol.into()),
        }
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<Position> {
        api.driver
            .reader
            .read()
            .decode_path::<Position>(&["trade", self.account_id.as_str(), "positions", self.symbol.as_str()])?
            .ok_or(crate::error::WaitFacadeError::InvalidState("position not ready"))
    }
}

#[derive(Debug, Clone)]
pub struct OrderRef {
    account_id: AccountId,
    order_id: OrderId,
}

impl OrderRef {
    pub fn new(account_id: impl Into<String>, order_id: impl Into<String>) -> Self {
        Self {
            account_id: AccountId::new(account_id.into()),
            order_id: OrderId::new(order_id.into()),
        }
    }

    pub fn is_ready(&self, api: &TqApi) -> crate::error::Result<bool> {
        Ok(self.snapshot(api)?.is_some())
    }

    pub fn snapshot(&self, api: &TqApi) -> crate::error::Result<Option<Order>> {
        api.driver
            .reader
            .read()
            .decode_path::<Order>(&["trade", self.account_id.as_str(), "orders", self.order_id.as_str()])
            .map_err(Into::into)
    }
}

impl ChangeTrackedRef for OrderRef {
    fn object_key(&self) -> Option<ObjectKey> {
        Some(ObjectKey::Order {
            account_id: self.account_id.clone(),
            order_id: self.order_id.clone(),
        })
    }

    fn state_path(&self) -> StatePath {
        StatePath::new(["trade", self.account_id.as_str(), "orders", self.order_id.as_str()])
    }
}

#[derive(Debug, Clone)]
pub struct TradeRef {
    account_id: AccountId,
    trade_id: TradeId,
}

impl TradeRef {
    pub fn new(account_id: impl Into<String>, trade_id: impl Into<String>) -> Self {
        Self {
            account_id: AccountId::new(account_id.into()),
            trade_id: TradeId::new(trade_id.into()),
        }
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<Trade> {
        api.driver
            .reader
            .read()
            .decode_path::<Trade>(&["trade", self.account_id.as_str(), "trades", self.trade_id.as_str()])?
            .ok_or(crate::error::WaitFacadeError::InvalidState("trade not ready"))
    }
}
```

Create `crates/tqsdk-wait/src/refs/trading_status.rs`:

```rust
use tqsdk_core::{ObjectKey, StatePath, Symbol, TradingStatus};

use crate::{api::TqApi, change::ChangeTrackedRef};

#[derive(Debug, Clone)]
pub struct TradingStatusRef {
    symbol: Symbol,
}

impl TradingStatusRef {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: Symbol::new(symbol.into()),
        }
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<TradingStatus> {
        api.driver
            .reader
            .read()
            .decode_path::<TradingStatus>(&["trading_status", self.symbol.as_str()])?
            .ok_or(crate::error::WaitFacadeError::InvalidState("trading status not ready"))
    }
}

impl ChangeTrackedRef for TradingStatusRef {
    fn object_key(&self) -> Option<ObjectKey> {
        Some(ObjectKey::TradingStatus {
            symbol: self.symbol.clone(),
        })
    }

    fn state_path(&self) -> StatePath {
        StatePath::new(["trading_status", self.symbol.as_str()])
    }
}
```

- [ ] **Step 4: Wire `TqApi` object/field change methods**

Add to `crates/tqsdk-wait/src/api.rs`:

```rust
use crate::change::{matches_any, matches_fields};
use crate::refs::QuoteRef;

impl TqApi {
    pub fn quote_ref(&self, symbol: &str) -> QuoteRef {
        QuoteRef::new(symbol)
    }

    pub fn is_changing(
        &self,
        target: &impl crate::change::ChangeTrackedRef,
    ) -> crate::error::Result<bool> {
        Ok(self
            .driver
            .last_commit
            .as_ref()
            .is_some_and(|commit| matches_any(&commit.changes, target)))
    }

    pub fn is_changing_fields(
        &self,
        target: &impl crate::change::ChangeTrackedRef,
        fields: &[&str],
    ) -> crate::error::Result<bool> {
        Ok(self
            .driver
            .last_commit
            .as_ref()
            .is_some_and(|commit| matches_fields(&commit.changes, target, fields)))
    }
}
```

- [ ] **Step 5: Re-run change tests and full wait crate**

Run:

```bash
cargo test -p tqsdk-wait -q --test wait_api_is_changing
cargo test -p tqsdk-wait -q
```

Expected: PASS

- [ ] **Step 6: Commit change matching and scalar refs**

Run:

```bash
git add crates/tqsdk-wait/src/change.rs crates/tqsdk-wait/src/refs crates/tqsdk-wait/tests/wait_api_is_changing.rs
git commit -m "feat: add wait facade change matching"
```

### Task 6: Implement Serial Windows And Market Wait APIs

**Files:**
- Create: `crates/tqsdk-wait/src/views/mod.rs`
- Create: `crates/tqsdk-wait/src/views/kline_window.rs`
- Create: `crates/tqsdk-wait/src/views/tick_window.rs`
- Create: `crates/tqsdk-wait/src/refs/kline.rs`
- Create: `crates/tqsdk-wait/src/refs/tick.rs`
- Modify: `crates/tqsdk-wait/src/api.rs`
- Test: `crates/tqsdk-wait/tests/wait_api_market.rs`

- [ ] **Step 1: Write failing tests for `get_quote`, `get_kline_serial`, and `get_tick_serial`**

Create `crates/tqsdk-wait/tests/wait_api_market.rs`:

```rust
mod support;

#[tokio::test(flavor = "current_thread")]
async fn get_quote_returns_ref_without_waiting_for_first_tick() {
    let mut api = support::seeded_api();
    let quote = api.get_quote("SHFE.au2602").await.unwrap();
    assert!(!quote.is_ready(&api).unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn get_trading_status_returns_ref_without_blocking() {
    let mut api = support::seeded_api();
    let status = api.get_trading_status("SHFE.au2602").await.unwrap();
    assert!(status.load(&api).is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn get_kline_serial_waits_for_initial_ready_and_preserves_commit_for_user() {
    let mut api = support::seeded_api();
    support::seed_ready_kline_chart(&mut api, "SHFE.au2602", 60_000_000_000, 64);

    let serial = api.get_kline_serial("SHFE.au2602", std::time::Duration::from_secs(60), 64).await.unwrap();
    assert!(serial.is_ready(&api).unwrap());
    assert!(api.wait_update(None).await.unwrap());
}
```

- [ ] **Step 2: Run the market facade tests and confirm they fail**

Run:

```bash
cargo test -p tqsdk-wait -q --test wait_api_market
```

Expected: FAIL with missing `get_quote`, `get_trading_status`, `get_kline_serial`, `KlineSerialRef`, and window types

- [ ] **Step 3: Implement window and serial ref types**

Create `crates/tqsdk-wait/src/views/kline_window.rs`:

```rust
use tqsdk_core::Kline;

#[derive(Debug, Clone, Default)]
pub struct KlineWindow {
    rows: Vec<Kline>,
}

impl KlineWindow {
    pub fn new(rows: Vec<Kline>) -> Self {
        Self { rows }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn last(&self) -> Option<&Kline> {
        self.rows.last()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Kline> {
        self.rows.iter()
    }
}
```

Create `crates/tqsdk-wait/src/views/tick_window.rs`:

```rust
use tqsdk_core::Tick;

#[derive(Debug, Clone, Default)]
pub struct TickWindow {
    rows: Vec<Tick>,
}

impl TickWindow {
    pub fn new(rows: Vec<Tick>) -> Self {
        Self { rows }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn last(&self) -> Option<&Tick> {
        self.rows.last()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Tick> {
        self.rows.iter()
    }
}
```

Create `crates/tqsdk-wait/src/views/mod.rs`:

```rust
mod kline_window;
mod tick_window;

pub use kline_window::KlineWindow;
pub use tick_window::TickWindow;
```

Create `crates/tqsdk-wait/src/refs/kline.rs`:

```rust
use tqsdk_core::Kline;

use crate::{api::TqApi, views::KlineWindow};

#[derive(Debug, Clone)]
pub struct KlineSerialRef {
    pub(crate) symbol: String,
    pub(crate) duration_ns: i64,
    pub(crate) view_width: usize,
    pub(crate) chart_id: String,
}

impl KlineSerialRef {
    pub fn is_ready(&self, api: &TqApi) -> crate::error::Result<bool> {
        let guard = api.driver.reader.read();
        let ready = guard
            .get_path(&["charts", self.chart_id.as_str(), "ready"])
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        let more_data = guard
            .get_path(&["charts", self.chart_id.as_str(), "more_data"])
            .and_then(|value| value.as_bool())
            .unwrap_or(true);
        Ok(ready && !more_data && !self.load(api)?.is_empty())
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<KlineWindow> {
        let guard = api.driver.reader.read();
        let mut rows = Vec::new();
        let duration_key = self.duration_ns.to_string();
        let data_path = ["klines", self.symbol.as_str(), duration_key.as_str(), "data"];
        if let Some(data) = guard.get_path(&data_path).and_then(|value| value.as_object()) {
            let mut ids = data.keys().filter_map(|key| key.parse::<i64>().ok()).collect::<Vec<_>>();
            ids.sort_unstable();
            for id in ids.into_iter().rev().take(self.view_width).rev() {
                let id_key = id.to_string();
                if let Some(row) = guard.decode_path::<Kline>(&[
                    "klines",
                    self.symbol.as_str(),
                    duration_key.as_str(),
                    "data",
                    id_key.as_str(),
                ])? {
                    rows.push(row);
                }
            }
        }
        Ok(KlineWindow::new(rows))
    }
}
```

Create `crates/tqsdk-wait/src/refs/tick.rs`:

```rust
use tqsdk_core::Tick;

use crate::{api::TqApi, views::TickWindow};

#[derive(Debug, Clone)]
pub struct TickSerialRef {
    pub(crate) symbol: String,
    pub(crate) view_width: usize,
}

impl TickSerialRef {
    pub fn is_ready(&self, api: &TqApi) -> crate::error::Result<bool> {
        Ok(!self.load(api)?.is_empty())
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<TickWindow> {
        let guard = api.driver.reader.read();
        let mut rows = Vec::new();
        if let Some(data) = guard.get_path(&["ticks", self.symbol.as_str(), "data"]).and_then(|value| value.as_object()) {
            let mut ids = data.keys().filter_map(|key| key.parse::<i64>().ok()).collect::<Vec<_>>();
            ids.sort_unstable();
            for id in ids.into_iter().rev().take(self.view_width).rev() {
                let id_key = id.to_string();
                if let Some(row) = guard.decode_path::<Tick>(&["ticks", self.symbol.as_str(), "data", id_key.as_str()])? {
                    rows.push(row);
                }
            }
        }
        Ok(TickWindow::new(rows))
    }
}
```

- [ ] **Step 4: Implement async market methods and ready-wait bookkeeping**

Add to `crates/tqsdk-wait/src/api.rs`:

```rust
use tqsdk_core::{MarketChartCommand, MarketCommand, RuntimeCommand, Symbol};

impl TqApi {
    pub async fn get_quote(&mut self, symbol: &str) -> crate::error::Result<crate::refs::QuoteRef> {
        self.driver
            .session
            .submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
                symbols: vec![Symbol::new(symbol)],
            }))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;
        Ok(self.quote_ref(symbol))
    }

    pub async fn get_trading_status(
        &mut self,
        symbol: &str,
    ) -> crate::error::Result<crate::refs::TradingStatusRef> {
        self.driver
            .session
            .submit(RuntimeCommand::Market(MarketCommand::SubscribeTradingStatus {
                symbols: vec![Symbol::new(symbol)],
            }))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;
        Ok(crate::refs::TradingStatusRef::new(symbol))
    }

    pub async fn get_kline_serial(
        &mut self,
        symbol: &str,
        duration: std::time::Duration,
        data_length: usize,
    ) -> crate::error::Result<crate::refs::KlineSerialRef> {
        let duration_ns =
            (duration.as_secs() as i64) * 1_000_000_000 + i64::from(duration.subsec_nanos());
        let chart_id = format!("wait-kline-{symbol}-{duration_ns}-{data_length}");
        self.driver
            .session
            .submit(RuntimeCommand::Market(MarketCommand::SetChart(MarketChartCommand {
                chart_id: chart_id.clone(),
                symbols: vec![Symbol::new(symbol)],
                duration_ns,
                view_width: data_length,
                left_kline_id: None,
                focus_datetime_ns: None,
                focus_position: None,
            })))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;
        let serial = crate::refs::KlineSerialRef {
            symbol: symbol.to_string(),
            duration_ns,
            view_width: data_length,
            chart_id,
        };
        self.wait_until_ready_for_test(|api| serial.is_ready(api)).await?;
        Ok(serial)
    }

    pub async fn get_tick_serial(
        &mut self,
        symbol: &str,
        data_length: usize,
    ) -> crate::error::Result<crate::refs::TickSerialRef> {
        self.driver
            .session
            .submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
                symbols: vec![Symbol::new(symbol)],
            }))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;
        let serial = crate::refs::TickSerialRef {
            symbol: symbol.to_string(),
            view_width: data_length,
        };
        self.wait_until_ready_for_test(|api| serial.is_ready(api)).await?;
        Ok(serial)
    }

    async fn wait_until_ready_for_test<F>(&mut self, mut ready: F) -> crate::error::Result<()>
    where
        F: FnMut(&Self) -> crate::error::Result<bool>,
    {
        while !ready(self)? {
            if !self.wait_update(None).await? {
                return Err(crate::error::WaitFacadeError::InvalidState("object not ready"));
            }
        }
        Ok(())
    }
}
```

- [ ] **Step 5: Re-run market tests and then the whole wait crate**

Run:

```bash
cargo test -p tqsdk-wait -q --test wait_api_market
cargo test -p tqsdk-wait -q
```

Expected: PASS

- [ ] **Step 6: Commit serial windows and market APIs**

Run:

```bash
git add crates/tqsdk-wait/src/views crates/tqsdk-wait/src/refs/kline.rs crates/tqsdk-wait/src/refs/tick.rs crates/tqsdk-wait/src/api.rs crates/tqsdk-wait/tests/wait_api_market.rs
git commit -m "feat: add wait facade market APIs"
```

### Task 7: Implement Trade Wait APIs, Docs, And Live Smoke

**Files:**
- Modify: `crates/tqsdk-wait/src/refs/trade.rs`
- Modify: `crates/tqsdk-wait/src/api.rs`
- Create: `crates/tqsdk-wait/tests/wait_api_trade.rs`
- Create: `crates/tqsdk-wait/examples/quote_wait.rs`
- Create: `crates/tqsdk-wait/README.md`
- Modify: `README.md`
- Modify: `docs/architecture/README.md`

- [ ] **Step 1: Write failing tests for trade refs and command wrappers**

Create `crates/tqsdk-wait/tests/wait_api_trade.rs`:

```rust
mod support;

#[tokio::test(flavor = "current_thread")]
async fn insert_order_returns_order_ref_without_local_overlay() {
    let mut api = support::seeded_api();
    let order = api
        .insert_order(
            "sim",
            "SHFE.au2602",
            tqsdk_core::TradeDirection::Buy,
            Some(tqsdk_core::TradeOffset::Open),
            1,
            Some(serde_json::json!(618.0)),
        )
        .await
        .unwrap();

    assert!(!order.is_ready(&api).unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn account_position_order_and_trade_refs_decode_from_state_tree() {
    let mut api = support::seeded_api();
    support::seed_trade_snapshot(&mut api, "sim", "SHFE.au2602");
    assert!(api.get_account("sim").load(&api).unwrap().balance >= 0.0);
    assert_eq!(api.get_position("sim", "SHFE.au2602").load(&api).unwrap().instrument_id, "au2602");
}
```

- [ ] **Step 2: Run the trade facade tests and confirm they fail**

Run:

```bash
cargo test -p tqsdk-wait -q --test wait_api_trade
```

Expected: FAIL with missing trade ref APIs and wrapper methods

- [ ] **Step 3: Implement trade refs and wrappers without local state overlay**

Add to `crates/tqsdk-wait/src/api.rs`:

```rust
use std::sync::atomic::Ordering;

impl TqApi {
    pub fn get_account(&self, account_id: &str) -> crate::refs::AccountRef {
        crate::refs::AccountRef::new(account_id)
    }

    pub fn get_position(&self, account_id: &str, symbol: &str) -> crate::refs::PositionRef {
        crate::refs::PositionRef::new(account_id, symbol)
    }

    pub fn get_order(&self, account_id: &str, order_id: &str) -> crate::refs::OrderRef {
        crate::refs::OrderRef::new(account_id, order_id)
    }

    pub fn get_trade(&self, account_id: &str, trade_id: &str) -> crate::refs::TradeRef {
        crate::refs::TradeRef::new(account_id, trade_id)
    }

    pub async fn insert_order(
        &mut self,
        account_id: &str,
        symbol: &str,
        direction: tqsdk_core::TradeDirection,
        offset: Option<tqsdk_core::TradeOffset>,
        volume: i64,
        limit_price: Option<serde_json::Value>,
    ) -> crate::error::Result<crate::refs::OrderRef> {
        let order_seq = self.driver.next_order_seq.fetch_add(1, Ordering::Relaxed);
        let order_id = tqsdk_core::OrderId::new(format!("wait-order-{order_seq}"));
        self.driver
            .session
            .submit(tqsdk_core::RuntimeCommand::Trade(
                tqsdk_core::TradeCommand::InsertOrder(tqsdk_core::TradeInsertOrderCommand {
                    account_id: tqsdk_core::AccountId::new(account_id),
                    order_id: order_id.clone(),
                    symbol: tqsdk_core::Symbol::new(symbol),
                    direction,
                    offset,
                    volume,
                    price_type: tqsdk_core::TradePriceType::Limit,
                    limit_price,
                    time_condition: tqsdk_core::TradeTimeCondition::Gfd,
                    volume_condition: tqsdk_core::TradeVolumeCondition::Any,
                }),
            ))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;
        Ok(crate::refs::OrderRef::new(account_id, order_id.as_str()))
    }

    pub async fn cancel_order(
        &mut self,
        account_id: &str,
        order_id: &str,
    ) -> crate::error::Result<()> {
        self.driver
            .session
            .submit(tqsdk_core::RuntimeCommand::Trade(tqsdk_core::TradeCommand::CancelOrder {
                account_id: tqsdk_core::AccountId::new(account_id),
                order_id: tqsdk_core::OrderId::new(order_id),
            }))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;
        Ok(())
    }

    pub async fn confirm_settlement(&mut self, account_id: &str) -> crate::error::Result<()> {
        self.driver
            .session
            .submit(tqsdk_core::RuntimeCommand::Trade(
                tqsdk_core::TradeCommand::ConfirmSettlement {
                    account_id: tqsdk_core::AccountId::new(account_id),
                },
            ))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;
        Ok(())
    }
}
```

- [ ] **Step 4: Add README/example that proves the wait facade shape**

Create `crates/tqsdk-wait/examples/quote_wait.rs`:

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

- [ ] **Step 5: Run the trade tests, example compilation, and full workspace checks**

Run:

```bash
cargo test -p tqsdk-wait -q --test wait_api_trade
cargo test -p tqsdk-wait -q
cargo test -p tqsdk-wait -q --example quote_wait --no-run
cargo test -q
cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::perf
```

Expected: all commands PASS

- [ ] **Step 6: Commit the trade facade and documentation**

Run:

```bash
git add crates/tqsdk-wait README.md docs/architecture/README.md
git commit -m "feat: add wait facade trade MVP"
```

## Self-Review

### Spec coverage
- `tqsdk-session` shared thin layer: covered by Tasks 1-3.
- direct query/schema/metadata live in `tqsdk-session`: covered by Task 3.
- `tqsdk-wait` as single-owner commit consumer: covered by Tasks 4-7.
- deferred commits and `is_changing()` semantics: covered by Tasks 4-5.
- serial windows and Rust-native snapshot shape: covered by Task 6.
- trade commands without local overlay: covered by Task 7.
- `tqsdk-stream` deferral: intentionally excluded from this plan and called out in Scope Notes.

### Placeholder scan
- No `TODO` / `TBD`.
- No “implement later” language in task steps.
- Each code-changing step includes concrete code snippets.
- Every command step includes the exact shell command to run.

### Type consistency
- Shared thin layer type names are consistently `SessionClient` / `SessionClientBuilder` / `SessionFacadeConfig`.
- Wait facade type names are consistently `TqApi`, `WaitDriver`, `QuoteRef`, `KlineSerialRef`, `TickSerialRef`.
- All tasks assume `tqsdk-core` remains the sole source of state and commit truth; no task introduces a second state tree or local order overlay.
