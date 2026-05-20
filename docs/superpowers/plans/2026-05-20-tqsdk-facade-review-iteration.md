# Tqsdk Facade Review Iteration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stabilize the new top-level `tqsdk` facade prototype after multi-role public API review, fixing unsafe default semantics before the crate becomes an accepted public contract.

**Architecture:** Keep `tqsdk` as a thin default facade over the existing `wait` / `task` / `data` / `session` crates. The iteration narrows root exports, removes premature stock/default-login shortcuts, resolves TQKQ account ids through the established session, and makes `TargetPos` intent semantics explicit without introducing a second runtime, state tree, direct-query layer, task engine, or data store.

**Tech Stack:** Rust 2024, Cargo workspace, `tqsdk` facade crate, `tqsdk-session` TQKQ auth helpers, `tqsdk-wait` live refs, `tqsdk-task` target-position tasks, architecture docs under `docs/architecture`.

---

## Review Decisions

- Accept P1: `Tq::stock()` is premature in this facade prototype because the current trade helpers route through futures-only defaults. Remove it from `tqsdk` for this iteration; stock remains available through the lower-level crates.
- Accept P1: README examples must not use literal `"TQKQ"` as an account id. Add facade helpers that resolve the authenticated TQKQ account id and construct target-position tasks from that resolved id.
- Accept P1: `TargetPos::wait_target_reached()` can hang without a concurrently driven host. Remove it from the top-level wrapper in this iteration; expose task status/report helpers and keep all progress driven by `Tq::next()` / `Tq::wait_update()`.
- Accept P2: Root exports and `prelude` are too broad. Keep the crate root focused on `Tq`, `TqBuilder`, `TargetPos`, `Error`, and `Result`; keep lower-level access under curated `advanced::*`.
- Accept P2: `tqsdk::Result` should cover session errors if `Tq::session()` remains public. Add `Error::Session` and `From<SessionFacadeError>`.
- Accept P3: `auth_env()` should trim and reject empty credentials, and facade docs/tests should cover real default-flow code instead of only type visibility.
- Defer full stock facade design. Adding explicit `stock`/`futures` trade-account APIs is a separate design pass because it changes default facade scope and public contract shape.

## File Map

Modify:

- `crates/tqsdk/src/lib.rs`: narrow exports, remove stock/default-login shortcuts, add session error conversion, add TQKQ account helpers, tighten `TargetPos`, reject empty env values.
- `crates/tqsdk/Cargo.toml`: add the default facade example and its required feature gate.
- `crates/tqsdk/tests/facade_contract.rs`: replace shallow type-only checks with source guards and compile-surface checks for the stabilized facade.
- `crates/tqsdk/README.md`: update default example, remove stock promise, document facade features and advanced boundary.
- `README.md`: update user-entry guidance, default facade example, and dependency diagram wording.
- `docs/architecture/ai-workflow.md`: mark the diagram as conceptual layering or replace it with a dependency matrix.
- `docs/architecture/README.md`: update the top-level facade public-surface description after narrowing `advanced::*`.
- `docs/architecture/crate-boundaries.md`: add missing `tqsdk-data` summary entries and align the `tqsdk` facade boundary.
- `docs/architecture/validation.md`: add facade feature matrix, package order, and default facade example coverage.
- `docs/scenarios/user-layer-iteration-plan.md`: include `tqsdk` in public API admission rules while preserving internal ownership boundaries.

Create:

- `crates/tqsdk/examples/api_contract_s33_default_facade.rs`: scenario-driven compile contract for the documented default facade path.

## Task 1: Narrow the Facade Surface

**Files:**
- Modify: `crates/tqsdk/tests/facade_contract.rs`
- Modify: `crates/tqsdk/src/lib.rs`

- [ ] **Step 1: Write failing surface guards**

Replace `crates/tqsdk/tests/facade_contract.rs` with:

```rust
use tqsdk::prelude::*;

#[test]
fn prelude_exposes_default_strategy_surface() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<TqBuilder>();

    let _builder = Tq::futures().auth("demo-user", "demo-pass");
    let _: Option<Tq> = None;
    let _: Option<QuoteRef> = None;
    let _: Option<QuoteSet> = None;
    let _: Option<TargetPos> = None;
}

#[test]
fn advanced_namespaces_keep_curated_underlying_access() {
    let _session = tqsdk::advanced::session::SessionClientBuilder::new("demo-user", "demo-pass")
        .futures_market();
    let _stream =
        tqsdk::advanced::stream::TqStreamBuilder::new("demo-user", "demo-pass").futures_market();
    let _data = tqsdk::advanced::data::DataClient::new();
    let _split = tqsdk::advanced::task::VolumeSplitPolicy::new(1, 2).unwrap();

    let _ = std::any::type_name::<tqsdk::advanced::runtime::RuntimeReader>();
}

#[test]
fn facade_root_exports_are_curated() {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"))
        .expect("read facade source");

    for broad_root_export in [
        "pub use tqsdk_data::DataClient;",
        "pub use tqsdk_session::{SessionClient, SessionClientBuilder};",
        "pub use tqsdk_stream::{TqStream, TqStreamBuilder};",
        "pub use tqsdk_task::{TargetPosTask, TaskHost};",
        "pub use tqsdk_wait::{TqApi, TqApiBuilder};",
    ] {
        assert!(
            !source.contains(broad_root_export),
            "facade root exports lower-level crate directly: {broad_root_export}"
        );
    }

    for wildcard_export in [
        "pub use tqsdk_core::*",
        "pub use tqsdk_data::*",
        "pub use tqsdk_session::*",
        "pub use tqsdk_stream::*",
        "pub use tqsdk_task::*",
        "pub use tqsdk_wait::*",
    ] {
        assert!(
            !source.contains(wildcard_export),
            "advanced namespace must be curated, found wildcard: {wildcard_export}"
        );
    }
}
```

- [ ] **Step 2: Run the new guard and verify it fails**

Run:

```bash
cargo test -p tqsdk facade_root_exports_are_curated
```

Expected: FAIL because `crates/tqsdk/src/lib.rs` still has broad root exports and `advanced::*` wildcard exports.

- [ ] **Step 3: Narrow root exports and curate `advanced::*`**

In `crates/tqsdk/src/lib.rs`, delete these root exports:

```rust
pub use tqsdk_data::DataClient;
pub use tqsdk_session::{SessionClient, SessionClientBuilder};
pub use tqsdk_stream::{TqStream, TqStreamBuilder};
pub use tqsdk_task::{TargetPosTask, TaskHost};
pub use tqsdk_wait::{TqApi, TqApiBuilder};
```

Replace the `prelude` and `advanced` modules with:

```rust
/// Common imports for strategy-oriented users.
pub mod prelude {
    pub use crate::{Error, Result, TargetPos, Tq, TqBuilder};
    pub use tqsdk_wait::{AccountRef, PositionRef, QuoteRef, QuoteSet, WaitStep};
}

/// Explicit access to the underlying crates for advanced users.
pub mod advanced {
    pub mod core {
        pub use tqsdk_core::{TradeAccountType, TradeDirection, TradeOffset};
    }

    pub mod data {
        pub use tqsdk_data::{DataClient, DataError};
    }

    pub mod runtime {
        pub use tqsdk_core::{CommitResult, RuntimeHandle, RuntimeReader, UpdateCursor};
    }

    pub mod session {
        pub use tqsdk_session::{SessionClient, SessionClientBuilder, SessionFacadeError};
    }

    pub mod stream {
        pub use tqsdk_stream::{TqStream, TqStreamBuilder};
    }

    pub mod task {
        pub use tqsdk_task::{
            OffsetPriority, PriceMode, TargetPosConfig, TargetPosTask,
            TargetPosTaskExecutionReport, TaskError, TaskHost, VolumeSplitPolicy,
        };
    }

    pub mod wait {
        pub use tqsdk_wait::{
            AccountRef, KlineHandle, KlineWindow, OrderPrice, OrderRef, OrderTicket,
            OrderTicketState, PositionRef, QuoteRef, QuoteSet, TickHandle, TickWindow, TqApi,
            TqApiBuilder, TradingStatusRef, WaitFacadeError, WaitStep,
        };
    }
}
```

- [ ] **Step 4: Run focused facade tests**

Run:

```bash
cargo test -p tqsdk --tests
```

Expected: PASS for the narrowed surface tests.

- [ ] **Step 5: Commit the surface narrowing**

```bash
git add crates/tqsdk/src/lib.rs crates/tqsdk/tests/facade_contract.rs
git commit -m "fix(tqsdk): narrow facade public surface"
```

## Task 2: Remove Premature Stock and Futures-Only Login Shortcuts

**Files:**
- Modify: `crates/tqsdk/tests/facade_contract.rs`
- Modify: `crates/tqsdk/src/lib.rs`
- Modify: `crates/tqsdk/README.md`

- [ ] **Step 1: Add source guard for removed shortcuts**

Append this test to `crates/tqsdk/tests/facade_contract.rs`:

```rust
#[test]
fn facade_does_not_expose_premature_stock_or_hardcoded_trade_login() {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"))
        .expect("read facade source");

    for removed_surface in [
        "pub fn stock(",
        "pub async fn login_trade_account(",
        "MarketKind::Stock",
        "TradeAccountType::Future,",
    ] {
        assert!(
            !source.contains(removed_surface),
            "premature or futures-only facade surface remains: {removed_surface}"
        );
    }
}
```

- [ ] **Step 2: Run the guard and verify it fails**

Run:

```bash
cargo test -p tqsdk facade_does_not_expose_premature_stock_or_hardcoded_trade_login
```

Expected: FAIL because `Tq::stock()`, `TqBuilder::stock()`, `MarketKind::Stock`, and `login_trade_account()` still exist.

- [ ] **Step 3: Remove stock and hardcoded trade login from `Tq`**

In `crates/tqsdk/src/lib.rs`, delete `Tq::stock()` and `Tq::login_trade_account()`.

Replace `TqBuilder` with a futures-only builder:

```rust
#[derive(Debug, Clone)]
pub struct TqBuilder {
    auth: Option<Auth>,
    query_enabled: bool,
    trade_targets: Vec<TradeTarget>,
}

impl TqBuilder {
    #[must_use]
    pub fn futures() -> Self {
        Self {
            auth: None,
            query_enabled: false,
            trade_targets: Vec::new(),
        }
    }

    pub async fn connect(self) -> Result<Tq> {
        let auth = self.auth.ok_or(Error::MissingAuth)?;
        let mut builder =
            tqsdk_session::SessionClientBuilder::new(auth.user, auth.pass).futures_market();
        if self.query_enabled {
            builder = builder.enable_query();
        }
        for target in self.trade_targets {
            builder = target.apply(builder);
        }
        let api = tqsdk_wait::TqApiBuilder::from_session_builder(builder)
            .build()
            .await?;
        Ok(Tq::from_api(api))
    }
}
```

Delete the `MarketKind` enum entirely.

- [ ] **Step 4: Remove stock from the facade README**

In `crates/tqsdk/README.md`, change the opening bullet list from:

```markdown
- `Tq::futures()` / `Tq::stock()`
```

to:

```markdown
- `Tq::futures()`
```

- [ ] **Step 5: Run focused tests**

Run:

```bash
cargo test -p tqsdk --tests
```

Expected: PASS, and no test should reference `Tq::stock()`.

- [ ] **Step 6: Commit shortcut removal**

```bash
git add crates/tqsdk/src/lib.rs crates/tqsdk/tests/facade_contract.rs crates/tqsdk/README.md
git commit -m "fix(tqsdk): remove premature stock facade shortcuts"
```

## Task 3: Resolve TQKQ Account IDs Through Session

**Files:**
- Modify: `crates/tqsdk/tests/facade_contract.rs`
- Modify: `crates/tqsdk/src/lib.rs`

- [ ] **Step 1: Add session error conversion and TQKQ surface tests**

Append these tests to `crates/tqsdk/tests/facade_contract.rs`:

```rust
#[test]
fn facade_result_accepts_session_errors() {
    let error = tqsdk_session::SessionFacadeError::InvalidState("facade contract");
    let _: tqsdk::Error = error.into();
}

#[test]
fn facade_exposes_tqkq_target_helpers_instead_of_literal_account_ids() {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"))
        .expect("read facade source");

    for required_surface in [
        "pub async fn tqkq_account_id(&self) -> Result<String>",
        "pub async fn target_pos_tqkq(&mut self, symbol: &str) -> Result<TargetPos>",
        "pub async fn target_pos_tqkq_numbered(",
        "tqkq_login_command()",
        "tqkq_login_command_numbered(number)",
    ] {
        assert!(
            source.contains(required_surface),
            "missing resolved TQKQ facade helper: {required_surface}"
        );
    }
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cargo test -p tqsdk facade_
```

Expected: FAIL because `Error::Session` and the TQKQ helpers do not exist yet.

- [ ] **Step 3: Add `Session` error variant**

In `crates/tqsdk/src/lib.rs`, extend `Error`:

```rust
pub enum Error {
    MissingAuth,
    MissingAuthEnv {
        name: &'static str,
        source: env::VarError,
    },
    EmptyAuthEnv {
        name: &'static str,
    },
    Session(Box<tqsdk_session::SessionFacadeError>),
    Wait(Box<tqsdk_wait::WaitFacadeError>),
    Task(Box<tqsdk_task::TaskError>),
    Data(Box<tqsdk_data::DataError>),
}
```

Update `Display` and `source()` with these arms:

```rust
Self::EmptyAuthEnv { name } => {
    write!(f, "environment variable {name} must not be empty")
}
Self::Session(error) => write!(f, "{error}"),
```

```rust
Self::EmptyAuthEnv { .. } => None,
Self::Session(error) => Some(&**error),
```

Add:

```rust
impl From<tqsdk_session::SessionFacadeError> for Error {
    fn from(error: tqsdk_session::SessionFacadeError) -> Self {
        Self::Session(Box::new(error))
    }
}
```

- [ ] **Step 4: Add resolved TQKQ helpers**

Add these methods to `impl Tq`:

```rust
#[cfg(feature = "live")]
pub async fn tqkq_account_id(&self) -> Result<String> {
    let login = self.session().tqkq_login_command().await?;
    Ok(login.account_id.as_str().to_owned())
}

#[cfg(feature = "live")]
pub async fn tqkq_account_id_numbered(&self, number: u8) -> Result<String> {
    let login = self.session().tqkq_login_command_numbered(number).await?;
    Ok(login.account_id.as_str().to_owned())
}

#[cfg(feature = "live")]
pub async fn target_pos_tqkq(&mut self, symbol: &str) -> Result<TargetPos> {
    let account_id = self.tqkq_account_id().await?;
    self.target_pos(&account_id, symbol)
}

#[cfg(feature = "live")]
pub async fn target_pos_tqkq_numbered(
    &mut self,
    number: u8,
    symbol: &str,
) -> Result<TargetPos> {
    let account_id = self.tqkq_account_id_numbered(number).await?;
    self.target_pos(&account_id, symbol)
}
```

- [ ] **Step 5: Run focused tests and feature checks**

Run:

```bash
cargo test -p tqsdk --tests
cargo check -p tqsdk --no-default-features --tests
```

Expected: both PASS. The no-default build passes because the TQKQ helpers are gated behind `live`.

- [ ] **Step 6: Commit TQKQ helpers**

```bash
git add crates/tqsdk/src/lib.rs crates/tqsdk/tests/facade_contract.rs
git commit -m "fix(tqsdk): resolve tqkq account ids in facade"
```

## Task 4: Make `TargetPos` Intent Semantics Explicit

**Files:**
- Modify: `crates/tqsdk/tests/facade_contract.rs`
- Modify: `crates/tqsdk/src/lib.rs`

- [ ] **Step 1: Add source guard for synchronous intent API**

Append this test to `crates/tqsdk/tests/facade_contract.rs`:

```rust
#[test]
fn target_pos_wrapper_uses_sync_intent_api_and_no_direct_wait() {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"))
        .expect("read facade source");

    for expected_surface in [
        "pub fn set(&self, volume: i64) -> Result<()>",
        "pub fn close(&self) -> Result<()>",
        "pub fn is_finished(&self) -> bool",
        "pub fn last_error(&self) -> Option<tqsdk_task::TaskError>",
        "pub fn execution_report(&self) -> tqsdk_task::TargetPosTaskExecutionReport",
    ] {
        assert!(
            source.contains(expected_surface),
            "missing explicit target-position wrapper surface: {expected_surface}"
        );
    }

    for removed_surface in [
        "pub async fn set(&mut self",
        "pub async fn close(&mut self",
        "pub async fn wait_target_reached",
    ] {
        assert!(
            !source.contains(removed_surface),
            "misleading async target-position surface remains: {removed_surface}"
        );
    }
}
```

- [ ] **Step 2: Run guard and verify it fails**

Run:

```bash
cargo test -p tqsdk target_pos_wrapper_uses_sync_intent_api_and_no_direct_wait
```

Expected: FAIL because `TargetPos::set`, `close`, and `wait_target_reached` are still async and status/report helpers are missing.

- [ ] **Step 3: Replace `TargetPos` methods**

In `crates/tqsdk/src/lib.rs`, replace the `set`, `close`, and `wait_target_reached` methods with:

```rust
#[must_use]
pub fn is_finished(&self) -> bool {
    self.inner.is_finished()
}

#[must_use]
pub fn current_target_volume(&self) -> Option<i64> {
    self.inner.current_target_volume()
}

#[must_use]
pub fn last_error(&self) -> Option<tqsdk_task::TaskError> {
    self.inner.last_error()
}

#[must_use]
pub fn execution_report(&self) -> tqsdk_task::TargetPosTaskExecutionReport {
    self.inner.execution_report()
}

pub fn set(&self, volume: i64) -> Result<()> {
    self.inner.set_target_volume(volume).map_err(Error::from)
}

pub fn close(&self) -> Result<()> {
    self.set(0)
}
```

Do not add a facade-level `wait_target_reached` replacement in this iteration. The documented default control flow is `target.set(volume)?` inside a `while tq.next().await?` loop, and progress remains owned by `Tq`.

- [ ] **Step 4: Run focused tests**

Run:

```bash
cargo test -p tqsdk --tests
```

Expected: PASS.

- [ ] **Step 5: Commit target-position semantics**

```bash
git add crates/tqsdk/src/lib.rs crates/tqsdk/tests/facade_contract.rs
git commit -m "fix(tqsdk): make target position intent synchronous"
```

## Task 5: Validate `auth_env()` Credentials Early

**Files:**
- Modify: `crates/tqsdk/src/lib.rs`

- [ ] **Step 1: Add unit tests for env value parsing**

Add this module to the bottom of `crates/tqsdk/src/lib.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::{parse_env_value, Error};

    #[test]
    fn parse_env_value_trims_non_empty_credentials() {
        assert_eq!(
            parse_env_value("TQ_AUTH_USER", "  demo-user  ".to_string()).unwrap(),
            "demo-user"
        );
    }

    #[test]
    fn parse_env_value_rejects_empty_credentials() {
        assert!(matches!(
            parse_env_value("TQ_AUTH_PASS", "   ".to_string()),
            Err(Error::EmptyAuthEnv {
                name: "TQ_AUTH_PASS"
            })
        ));
    }
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cargo test -p tqsdk parse_env_value_
```

Expected: FAIL because `parse_env_value` and `Error::EmptyAuthEnv` are not fully implemented yet.

- [ ] **Step 3: Implement trimmed env parsing**

Replace `read_env` with:

```rust
fn read_env(name: &'static str) -> Result<String> {
    let value = env::var(name).map_err(|source| Error::MissingAuthEnv { name, source })?;
    parse_env_value(name, value)
}

fn parse_env_value(name: &'static str, value: String) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(Error::EmptyAuthEnv { name });
    }
    Ok(trimmed.to_owned())
}
```

Keep the `EmptyAuthEnv` `Display` and `source()` arms added in Task 3.

- [ ] **Step 4: Run focused tests**

Run:

```bash
cargo test -p tqsdk parse_env_value_
cargo test -p tqsdk --tests
```

Expected: both PASS.

- [ ] **Step 5: Commit env validation**

```bash
git add crates/tqsdk/src/lib.rs
git commit -m "fix(tqsdk): reject empty auth environment values"
```

## Task 6: Add a Real Default Facade Contract Example

**Files:**
- Modify: `crates/tqsdk/Cargo.toml`
- Create: `crates/tqsdk/examples/api_contract_s33_default_facade.rs`
- Modify: `crates/tqsdk/tests/facade_contract.rs`

- [ ] **Step 1: Add example discovery guard**

Append this test to `crates/tqsdk/tests/facade_contract.rs`:

```rust
#[test]
fn default_facade_contract_example_exists() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/api_contract_s33_default_facade.rs"
    );
    let source = std::fs::read_to_string(path).expect("read default facade example");

    for required in [
        "use tqsdk::prelude::*;",
        "Tq::futures()",
        ".auth_env()?",
        ".trade_target_tqkq()",
        "target_pos_tqkq(\"SHFE.au2602\").await?",
        "while tq.next().await?",
        "target.set(1)?",
    ] {
        assert!(
            source.contains(required),
            "default facade example missing required flow fragment: {required}"
        );
    }
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cargo test -p tqsdk default_facade_contract_example_exists
```

Expected: FAIL because the example file does not exist.

- [ ] **Step 3: Register the example feature gate**

Add this to `crates/tqsdk/Cargo.toml`:

```toml
[[example]]
name = "api_contract_s33_default_facade"
required-features = ["live"]
```

- [ ] **Step 4: Create the example**

Create `crates/tqsdk/examples/api_contract_s33_default_facade.rs`:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk::prelude::*;

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    let mut tq = Tq::futures()
        .auth_env()?
        .trade_target_tqkq()
        .connect()
        .await?;

    let quote = tq.quote("SHFE.au2602").await?;
    let target = tq.target_pos_tqkq("SHFE.au2602").await?;

    while tq.next().await? {
        let snapshot = quote.load()?;
        if snapshot.last_price > 3600.0 {
            target.set(1)?;
        } else {
            target.close()?;
        }
    }

    Ok(())
}
```

- [ ] **Step 5: Run example checks**

Run:

```bash
cargo check -p tqsdk --example api_contract_s33_default_facade
cargo check -p tqsdk --no-default-features --examples
cargo test -p tqsdk default_facade_contract_example_exists
```

Expected: all PASS. The no-default example check skips the live-gated example.

- [ ] **Step 6: Commit the contract example**

```bash
git add crates/tqsdk/Cargo.toml crates/tqsdk/examples/api_contract_s33_default_facade.rs crates/tqsdk/tests/facade_contract.rs
git commit -m "test(tqsdk): add default facade contract example"
```

## Task 7: Align Public Documentation and Architecture Notes

**Files:**
- Modify: `README.md`
- Modify: `crates/tqsdk/README.md`
- Modify: `docs/architecture/ai-workflow.md`
- Modify: `docs/architecture/README.md`
- Modify: `docs/architecture/crate-boundaries.md`
- Modify: `docs/architecture/validation.md`
- Modify: `docs/scenarios/user-layer-iteration-plan.md`

- [ ] **Step 1: Update README user-entry guidance**

In `README.md`, replace the current guidance lines:

```markdown
- 想快速写策略、查询、目标持仓或历史数据：从 `tqsdk` 开始。
- 想快速写策略或迁移 Python TQSDK 心智：从 `tqsdk-wait` 开始。
```

with:

```markdown
- 想快速写策略、查询、目标持仓或历史数据：从 `tqsdk` 开始。
- 已经明确需要单 owner `TqApi` / `wait_update()` 底层控制，或正在迁移现有 wait facade 代码：直接使用 `tqsdk-wait`。
```

- [ ] **Step 2: Update README default facade example**

In `README.md`, replace the default facade code block with:

```rust
use tqsdk::prelude::*;

let mut tq = Tq::futures()
    .auth_env()?
    .trade_target_tqkq()
    .connect()
    .await?;

let quote = tq.quote("SHFE.au2602").await?;
let target = tq.target_pos_tqkq("SHFE.au2602").await?;

while tq.next().await? {
    if quote.load()?.last_price > 3600.0 {
        target.set(1)?;
    }
}
```

- [ ] **Step 3: Replace misleading dependency diagram wording**

In `README.md` and `docs/architecture/ai-workflow.md`, keep the visual stack only if the preceding sentence says it is conceptual user/capability layering:

```markdown
仓库采用“稳定底座 + 可替换 facade”的分层。下图表示用户能力层级，不是 Cargo 依赖图：
```

Add this dependency note after the diagram:

```markdown
实际 Cargo 依赖中，`tqsdk` 作为默认入口会直接依赖 `tqsdk-core`、`tqsdk-session`、
`tqsdk-wait`、`tqsdk-stream`、`tqsdk-task` 和 `tqsdk-data`；内部能力归属仍由这些
crate 自己维护。
```

- [ ] **Step 4: Update `crates/tqsdk/README.md`**

Replace its example with the same resolved-TQKQ flow from Step 2, and add this feature section:

```markdown
## Features

- `default = ["live", "services"]`：默认用户入口，包含 live 连接与服务查询能力。
- `live`：向内部 `session` / `wait` / `stream` / `task` / `data` crate 传播 live feature，并启用 TQ auth 派生的 TQKQ helper。
- `services`：向内部 crate 传播服务查询相关 HTTP 能力。
- `default-features = false`：保留 facade 类型和不依赖 live auth 的组合入口；live-only helper 不参与编译。
```

- [ ] **Step 5: Update architecture docs**

Make these exact documentation changes:

- `docs/architecture/README.md`: describe `advanced::*` as curated escape hatches, not full wildcard mirrors.
- `docs/architecture/crate-boundaries.md`: add `tqsdk-data` to the one-line summary near the top and to the final judgment list.
- `docs/scenarios/user-layer-iteration-plan.md`: change the admission bullet to include `tqsdk` as the default facade while stating implementation ownership remains in internal crates.

- [ ] **Step 6: Update validation docs**

In `docs/architecture/validation.md`, add the new example to the current implementation validation table:

```markdown
| 默认 facade crate | `crates/tqsdk/tests/facade_contract.rs`、`crates/tqsdk/examples/api_contract_s33_default_facade.rs` | 覆盖 `tqsdk::prelude::*`、`Tq` / `TqBuilder`、resolved TQKQ target-position helper、`TargetPos` intent API 和 curated `advanced::*` 下钻命名空间 |
```

Update the package order to:

```markdown
`tqsdk-core` -> `tqsdk-session` -> `tqsdk-wait` / `tqsdk-stream` ->
`tqsdk-data` -> `tqsdk-task` -> `tqsdk`。
```

Add these commands to the feature matrix:

```markdown
9. `cargo build -p tqsdk --no-default-features`
10. `cargo build -p tqsdk --no-default-features --features live`
11. `cargo build -p tqsdk --no-default-features --features services`
12. `cargo build -p tqsdk --all-features`
13. `cargo test -p tqsdk`
```

Renumber the remaining commands in that list.

- [ ] **Step 7: Run documentation checks**

Run:

```bash
git diff --check
cargo check -p tqsdk --examples
```

Expected: both PASS.

- [ ] **Step 8: Commit documentation alignment**

```bash
git add README.md crates/tqsdk/README.md docs/architecture/ai-workflow.md docs/architecture/README.md docs/architecture/crate-boundaries.md docs/architecture/validation.md docs/scenarios/user-layer-iteration-plan.md
git commit -m "docs: align facade review iteration"
```

## Task 8: Final Verification and Scope Review

**Files:**
- No source edits unless verification exposes a regression in files changed by Tasks 1-7.

- [ ] **Step 1: Run GitNexus change detection**

Run the code-review graph `detect_changes_tool` against `HEAD` with `detail_level = "minimal"` after the final implementation commit. Expected: changed scope is limited to `crates/tqsdk`, root/workspace manifests if touched, and the listed docs.

- [ ] **Step 2: Run required facade checks**

Run:

```bash
cargo fmt --all --check
cargo check -p tqsdk --examples
cargo test -p tqsdk --tests
cargo clippy -p tqsdk --all-targets -- -D warnings
cargo check -p tqsdk --no-default-features --tests
cargo check -p tqsdk --no-default-features --examples
cargo check -p tqsdk --no-default-features --features live
cargo check -p tqsdk --no-default-features --features services
cargo check -p tqsdk --all-features --examples
git diff --check
```

Expected: all PASS.

- [ ] **Step 3: Run workspace checks that previously passed**

Run:

```bash
cargo check --workspace --examples
cargo check --workspace --no-default-features
cargo check --workspace --no-default-features --examples
cargo check --workspace --all-features --examples
cargo clippy --workspace --examples --all-targets -- -D warnings
```

Expected: all PASS.

- [ ] **Step 4: Re-run known problematic workspace tests and record status**

Run:

```bash
cargo test -p tqsdk-task --test scheduler scheduler_advances_steps_via_host_wait_updates
cargo test --workspace
cargo test -p tqsdk-session --no-default-features
```

Expected for this iteration:

- If `tqsdk-task --test scheduler` still fails with the same scheduler failure and no files under `crates/tqsdk-task` changed, record it as pre-existing.
- If `cargo test --workspace` fails only because of that scheduler failure, record it as pre-existing.
- If `cargo test -p tqsdk-session --no-default-features` still fails compiling `live_smoke` service-gated methods and no files under `crates/tqsdk-session` changed, record it as pre-existing.
- Any new failure in `crates/tqsdk`, manifests, or docs changed by this plan must be fixed before completion.

- [ ] **Step 5: Final review statement**

Prepare the completion note with:

- The commit list created by Tasks 1-7.
- The accepted review findings and where each was addressed.
- The verification commands and exact pass/fail status.
- A statement that architecture docs were updated because this iteration changes facade public API and validation surface.
