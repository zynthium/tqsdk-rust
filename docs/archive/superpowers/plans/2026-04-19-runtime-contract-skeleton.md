# Runtime Contract Skeleton Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scaffold a compileable Rust crate that exposes the V1 protocol-complete runtime contract and protocol-adapter contract surfaces from the approved design spec.

**Architecture:** Build a small library crate named `tqsdk-runtime-contract` with focused modules for identifiers, commands, inputs/mutations, state/commit types, adapter traits, and runtime traits. Keep V1 strictly at the contract layer: no transport implementation, no user facade, no `wait_update()` adapter, no stream/callback facade.

**Tech Stack:** Rust 2021, `cargo test`, `serde_json`

---

## File Structure

- `Cargo.toml`
  Declares the `tqsdk-runtime-contract` library crate and the single `serde_json` dependency used by mutation payloads.
- `src/lib.rs`
  Re-exports the stable public contract surface and keeps module boundaries explicit.
- `src/error.rs`
  Defines `ContractError` and `Result<T>`.
- `src/ids.rs`
  Defines `Revision`, `CommandId`, `CursorId`, `ProtocolDomain`, and stable identifier newtypes such as `Symbol`, `AccountId`, `OrderId`, `TradeId`, `QueryId`, `SchemaId`, `ReplaySessionId`, and `AuthId`.
- `src/commands.rs`
  Defines `RuntimeCommand`, domain command enums, command envelope/status, and outbound request shells.
- `src/events.rs`
  Defines `RuntimeInput`, input event shells, `NormalizedMutation`, `FieldMutation`, and `MutationSource`.
- `src/state.rs`
  Defines `StatePath`, `SeriesKey`, `ObjectKey`, `ChangeHit`, `ChangeSet`, `StateSnapshot`, `CommitScope`, `CommitResult`, and `UpdateCursor`.
- `src/adapter.rs`
  Defines `ProtocolAdapter` and `AdapterRegistry`.
- `src/runtime.rs`
  Defines `Runtime`, `RuntimeHandle`, and `CommitLog` skeletons.
- `tests/runtime_contract_bootstrap.rs`
  Verifies the crate boots and exports compile.
- `tests/runtime_contract_surface.rs`
  Verifies the public contract shape, variant routing, snapshot/cursor constructors, and mutation/change-set data carriers.

### Task 1: Bootstrap The Crate

**Files:**
- Create: `Cargo.toml`
- Create: `src/lib.rs`
- Create: `tests/runtime_contract_bootstrap.rs`

- [ ] **Step 1: Write the failing bootstrap test**

```rust
// tests/runtime_contract_bootstrap.rs
use tqsdk_runtime_contract as _;

#[test]
fn crate_bootstraps() {
    assert!(true);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test runtime_contract_bootstrap -v`
Expected: FAIL with an error equivalent to `could not find Cargo.toml`

- [ ] **Step 3: Write the minimal crate bootstrap**

```toml
# Cargo.toml
[package]
name = "tqsdk-runtime-contract"
version = "0.1.0"
edition = "2021"

[dependencies]
serde_json = "1"
```

```rust
// src/lib.rs
pub mod adapter;
pub mod commands;
pub mod error;
pub mod events;
pub mod ids;
pub mod runtime;
pub mod state;

pub use adapter::{AdapterRegistry, ProtocolAdapter};
pub use commands::{
    CausationMeta, CommandEnvelope, CommandStatus, HttpRequest, InternalRequest, MarketCommand, OutboundFrame,
    OutboundRequest, QueryCommand, ReplayCommand, ReplayRequest, RuntimeCommand, SchemaCommand, SystemCommand,
    TradeCommand,
};
pub use error::{ContractError, Result};
pub use events::{
    AuthEvent, FieldMutation, InternalEvent, IoEvent, MutationSource, NormalizedMutation, ReplayEvent, RuntimeInput,
    TimerEvent,
};
pub use ids::{
    AccountId, AuthId, CommandId, CursorId, OrderId, ProtocolDomain, QueryId, ReplaySessionId, Revision, SchemaId,
    Symbol, TradeId,
};
pub use runtime::{CommitLog, Runtime, RuntimeHandle};
pub use state::{
    ChangeHit, ChangeSet, CommitResult, CommitScope, ObjectKey, PathSegment, SeriesKey, StatePath, StateSnapshot,
    UpdateCursor,
};
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test runtime_contract_bootstrap -v`
Expected: PASS with `test crate_bootstraps ... ok`

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml src/lib.rs tests/runtime_contract_bootstrap.rs
git commit -m "feat: bootstrap runtime contract crate"
```

### Task 2: Add Stable Identifiers And Error Surface

**Files:**
- Create: `src/error.rs`
- Create: `src/ids.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write the failing identifier/error test**

```rust
// tests/runtime_contract_surface.rs
use tqsdk_runtime_contract::{ContractError, ProtocolDomain, Revision, Symbol};

#[test]
fn ids_and_domain_surface_are_stable() {
    let revision = Revision::new(7);
    let symbol = Symbol::new("SHFE.au2602");

    assert_eq!(revision.get(), 7);
    assert_eq!(symbol.as_str(), "SHFE.au2602");
    assert_eq!(ProtocolDomain::Trade.as_str(), "trade");
    assert_eq!(ContractError::validation("bad command").to_string(), "validation error: bad command");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test runtime_contract_surface ids_and_domain_surface_are_stable -v`
Expected: FAIL with unresolved imports or missing methods such as `Revision::new`

- [ ] **Step 3: Write the identifier and error modules**

```rust
// src/error.rs
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractError {
    Validation(String),
    Adapter(String),
    UnsupportedCommand(&'static str),
    UnsupportedInput(&'static str),
}

impl ContractError {
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }
}

impl Display for ContractError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(message) => write!(f, "validation error: {message}"),
            Self::Adapter(message) => write!(f, "adapter error: {message}"),
            Self::UnsupportedCommand(kind) => write!(f, "unsupported command: {kind}"),
            Self::UnsupportedInput(kind) => write!(f, "unsupported input: {kind}"),
        }
    }
}

impl std::error::Error for ContractError {}

pub type Result<T> = std::result::Result<T, ContractError>;
```

```rust
// src/ids.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Revision(u64);

impl Revision {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommandId(u64);

impl CommandId {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CursorId(u64);

impl CursorId {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

string_id!(Symbol);
string_id!(AccountId);
string_id!(OrderId);
string_id!(TradeId);
string_id!(QueryId);
string_id!(SchemaId);
string_id!(ReplaySessionId);
string_id!(AuthId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProtocolDomain {
    System,
    Market,
    Trade,
    Replay,
    Query,
    Schema,
}

impl ProtocolDomain {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Market => "market",
            Self::Trade => "trade",
            Self::Replay => "replay",
            Self::Query => "query",
            Self::Schema => "schema",
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test runtime_contract_surface ids_and_domain_surface_are_stable -v`
Expected: PASS with `test ids_and_domain_surface_are_stable ... ok`

- [ ] **Step 5: Commit**

```bash
git add src/error.rs src/ids.rs src/lib.rs tests/runtime_contract_surface.rs
git commit -m "feat: add runtime contract ids and errors"
```

### Task 3: Add Commands, Command Ledger, And Outbound Request Shells

**Files:**
- Create: `src/commands.rs`
- Modify: `src/lib.rs`
- Test: `tests/runtime_contract_surface.rs`

- [ ] **Step 1: Extend the failing contract test for commands**

```rust
// tests/runtime_contract_surface.rs
use tqsdk_runtime_contract::{
    AccountId, CausationMeta, CommandEnvelope, CommandId, CommandStatus, MarketCommand, OutboundRequest,
    ProtocolDomain, QueryCommand, QueryId, ReplayCommand, RuntimeCommand, SchemaCommand, SchemaId, Symbol,
    SystemCommand, TradeCommand,
};

#[test]
fn runtime_commands_route_to_expected_domains() {
    let market = RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
        symbols: vec![Symbol::new("SHFE.au2602")],
    });
    let trade = RuntimeCommand::Trade(TradeCommand::InsertOrder {
        account_id: AccountId::new("sim"),
        symbol: Symbol::new("SHFE.au2602"),
        volume: 2,
    });
    let query = RuntimeCommand::Query(QueryCommand::Fetch {
        query_id: QueryId::new("quotes-page-1"),
        path: "/graphql/quotes".to_string(),
    });
    let schema = RuntimeCommand::Schema(SchemaCommand::Refresh {
        schema_id: SchemaId::new("instrument-schema"),
    });
    let replay = RuntimeCommand::Replay(ReplayCommand::Step);
    let system = RuntimeCommand::System(SystemCommand::Shutdown);

    assert_eq!(market.domain(), ProtocolDomain::Market);
    assert_eq!(trade.domain(), ProtocolDomain::Trade);
    assert_eq!(query.domain(), ProtocolDomain::Query);
    assert_eq!(schema.domain(), ProtocolDomain::Schema);
    assert_eq!(replay.domain(), ProtocolDomain::Replay);
    assert_eq!(system.domain(), ProtocolDomain::System);

    let envelope = CommandEnvelope {
        id: CommandId::new(9),
        command: market,
        causation: CausationMeta::default(),
    };

    assert_eq!(envelope.id.get(), 9);
    assert_eq!(CommandStatus::Queued.as_str(), "queued");
    assert!(matches!(
        OutboundRequest::internal_label("flush-peek"),
        OutboundRequest::Internal(_)
    ));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test runtime_contract_surface runtime_commands_route_to_expected_domains -v`
Expected: FAIL with unresolved items such as `RuntimeCommand` or `CommandStatus`

- [ ] **Step 3: Write the command and outbound-request module**

```rust
// src/commands.rs
use crate::ids::{AccountId, CommandId, OrderId, ProtocolDomain, QueryId, SchemaId, Symbol};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeCommand {
    System(SystemCommand),
    Market(MarketCommand),
    Trade(TradeCommand),
    Replay(ReplayCommand),
    Query(QueryCommand),
    Schema(SchemaCommand),
}

impl RuntimeCommand {
    pub fn domain(&self) -> ProtocolDomain {
        match self {
            Self::System(_) => ProtocolDomain::System,
            Self::Market(_) => ProtocolDomain::Market,
            Self::Trade(_) => ProtocolDomain::Trade,
            Self::Replay(_) => ProtocolDomain::Replay,
            Self::Query(_) => ProtocolDomain::Query,
            Self::Schema(_) => ProtocolDomain::Schema,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SystemCommand {
    Shutdown,
    RefreshAuth,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketCommand {
    SubscribeQuotes { symbols: Vec<Symbol> },
    UnsubscribeQuotes { symbols: Vec<Symbol> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TradeCommand {
    InsertOrder {
        account_id: AccountId,
        symbol: Symbol,
        volume: i64,
    },
    CancelOrder {
        account_id: AccountId,
        order_id: OrderId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayCommand {
    Step,
    Reset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryCommand {
    Fetch { query_id: QueryId, path: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaCommand {
    Refresh { schema_id: SchemaId },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CausationMeta {
    pub parent: Option<CommandId>,
    pub note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandEnvelope {
    pub id: CommandId,
    pub command: RuntimeCommand,
    pub causation: CausationMeta,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandStatus {
    Queued,
    Sent,
    Acked,
    PartiallyApplied,
    Completed,
    Rejected,
    Failed,
    Cancelled,
}

impl CommandStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Sent => "sent",
            Self::Acked => "acked",
            Self::PartiallyApplied => "partially_applied",
            Self::Completed => "completed",
            Self::Rejected => "rejected",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundFrame {
    Text(String),
    Binary(Vec<u8>),
    Ping,
    Close,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayRequest {
    pub action: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalRequest {
    pub label: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundRequest {
    Transport(OutboundFrame),
    Http(HttpRequest),
    Replay(ReplayRequest),
    Internal(InternalRequest),
}

impl OutboundRequest {
    pub fn internal_label(label: &'static str) -> Self {
        Self::Internal(InternalRequest { label })
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test runtime_contract_surface runtime_commands_route_to_expected_domains -v`
Expected: PASS with `test runtime_commands_route_to_expected_domains ... ok`

- [ ] **Step 5: Commit**

```bash
git add src/commands.rs src/lib.rs tests/runtime_contract_surface.rs
git commit -m "feat: add runtime commands and outbound request shells"
```

### Task 4: Add Inputs, Mutations, State Paths, Object Keys, And Commit Types

**Files:**
- Create: `src/events.rs`
- Create: `src/state.rs`
- Modify: `src/lib.rs`
- Test: `tests/runtime_contract_surface.rs`

- [ ] **Step 1: Extend the failing contract test for state and commit carriers**

```rust
// tests/runtime_contract_surface.rs
use serde_json::json;
use tqsdk_runtime_contract::{
    ChangeHit, ChangeSet, CommitResult, CommitScope, CursorId, FieldMutation, MutationSource, NormalizedMutation,
    ObjectKey, Revision, SeriesKey, StatePath, StateSnapshot, Symbol, UpdateCursor,
};

#[test]
fn snapshot_cursor_and_mutation_types_are_revision_bound() {
    let path = StatePath::new(["market", "quotes", "SHFE.au2602"]);
    let quote_key = ObjectKey::Quote {
        symbol: Symbol::new("SHFE.au2602"),
    };
    let mutation = NormalizedMutation {
        path: path.clone(),
        object: Some(quote_key.clone()),
        fields: vec![FieldMutation {
            field: "last_price".to_string(),
            value: json!(618.5),
        }],
        source: MutationSource::MarketDiff,
    };

    let snapshot = StateSnapshot::new(Revision::new(3));
    let cursor = UpdateCursor::new(CursorId::new(1), Revision::new(4));
    let changes = ChangeSet {
        path_hits: vec![path.clone()],
        object_hits: vec![quote_key.clone()],
        field_hits: vec![ChangeHit::field(path.clone(), quote_key.clone(), "last_price")],
    };
    let commit = CommitResult::new(Revision::new(4), changes.clone(), vec![], CommitScope::RealtimeUpdate);

    assert_eq!(snapshot.revision().get(), 3);
    assert_eq!(cursor.next_revision().get(), 4);
    assert_eq!(mutation.fields.len(), 1);
    assert_eq!(changes.object_hits.len(), 1);
    assert_eq!(commit.revision.get(), 4);

    let series = SeriesKey {
        primary: Symbol::new("SHFE.au2602"),
        secondary: vec![Symbol::new("SHFE.au2604")],
        duration_ns: 60_000_000_000,
        view_width: 128,
        right_id: Some(42),
    };

    assert_eq!(series.view_width, 128);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test runtime_contract_surface snapshot_cursor_and_mutation_types_are_revision_bound -v`
Expected: FAIL with unresolved items such as `StatePath` or `CommitResult`

- [ ] **Step 3: Write the input/mutation and state/commit modules**

```rust
// src/events.rs
use serde_json::Value;

use crate::state::{ObjectKey, StatePath};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeInput {
    Io(IoEvent),
    Timer(TimerEvent),
    Auth(AuthEvent),
    Replay(ReplayEvent),
    Internal(InternalEvent),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IoEvent {
    pub label: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerEvent {
    pub label: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthEvent {
    pub label: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayEvent {
    pub label: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalEvent {
    pub label: &'static str,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldMutation {
    pub field: String,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedMutation {
    pub path: StatePath,
    pub object: Option<ObjectKey>,
    pub fields: Vec<FieldMutation>,
    pub source: MutationSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationSource {
    MarketDiff,
    TradeReply,
    QueryResult,
    SchemaBootstrap,
    ReplayStep,
    SessionControl,
}
```

```rust
// src/state.rs
use crate::ids::{AccountId, CommandId, CursorId, OrderId, QueryId, ReplaySessionId, Revision, SchemaId, Symbol, TradeId};

pub type PathSegment = String;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StatePath(Vec<PathSegment>);

impl StatePath {
    pub fn new<I, S>(segments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self(segments.into_iter().map(Into::into).collect())
    }

    pub fn segments(&self) -> &[PathSegment] {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SeriesKey {
    pub primary: Symbol,
    pub secondary: Vec<Symbol>,
    pub duration_ns: i64,
    pub view_width: usize,
    pub right_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ObjectKey {
    Quote { symbol: Symbol },
    Kline { series: SeriesKey, bar_id: i64 },
    Tick { symbol: Symbol, tick_id: i64 },
    Account { account_id: AccountId },
    Position { account_id: AccountId, symbol: Symbol },
    Order { account_id: AccountId, order_id: OrderId },
    Trade { account_id: AccountId, trade_id: TradeId },
    QueryResult { query_id: QueryId },
    SchemaNode { schema_id: SchemaId },
    ReplayCursor { session_id: ReplaySessionId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeHit {
    pub path: StatePath,
    pub object: ObjectKey,
    pub field: String,
}

impl ChangeHit {
    pub fn field(path: StatePath, object: ObjectKey, field: impl Into<String>) -> Self {
        Self {
            path,
            object,
            field: field.into(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChangeSet {
    pub path_hits: Vec<StatePath>,
    pub object_hits: Vec<ObjectKey>,
    pub field_hits: Vec<ChangeHit>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateSnapshot {
    revision: Revision,
}

impl StateSnapshot {
    pub fn new(revision: Revision) -> Self {
        Self { revision }
    }

    pub fn revision(&self) -> Revision {
        self.revision
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitScope {
    InitialReady,
    RealtimeUpdate,
    ResyncRecovery,
    ReplayStep,
    QueryRefresh,
    SessionTransition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitResult {
    pub revision: Revision,
    pub changes: ChangeSet,
    pub caused_by: Vec<CommandId>,
    pub scope: CommitScope,
}

impl CommitResult {
    pub fn new(revision: Revision, changes: ChangeSet, caused_by: Vec<CommandId>, scope: CommitScope) -> Self {
        Self {
            revision,
            changes,
            caused_by,
            scope,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateCursor {
    id: CursorId,
    next_revision: Revision,
}

impl UpdateCursor {
    pub fn new(id: CursorId, next_revision: Revision) -> Self {
        Self { id, next_revision }
    }

    pub fn id(&self) -> CursorId {
        self.id
    }

    pub fn next_revision(&self) -> Revision {
        self.next_revision
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test runtime_contract_surface snapshot_cursor_and_mutation_types_are_revision_bound -v`
Expected: PASS with `test snapshot_cursor_and_mutation_types_are_revision_bound ... ok`

- [ ] **Step 5: Commit**

```bash
git add src/events.rs src/state.rs src/lib.rs tests/runtime_contract_surface.rs
git commit -m "feat: add runtime inputs mutations and commit state types"
```

### Task 5: Add Adapter And Runtime Traits

**Files:**
- Create: `src/adapter.rs`
- Create: `src/runtime.rs`
- Modify: `src/lib.rs`
- Test: `tests/runtime_contract_surface.rs`

- [ ] **Step 1: Extend the failing contract test for adapters and runtime**

```rust
// tests/runtime_contract_surface.rs
use tqsdk_runtime_contract::{
    AdapterRegistry, CommitLog, ContractError, ProtocolAdapter, ProtocolDomain, Result, Runtime, RuntimeHandle,
    RuntimeCommand, RuntimeInput, StateSnapshot, UpdateCursor,
};

struct TestAdapter;

impl ProtocolAdapter for TestAdapter {
    fn domain(&self) -> ProtocolDomain {
        ProtocolDomain::System
    }

    fn accepts_command(&self, cmd: &RuntimeCommand) -> bool {
        matches!(cmd, RuntimeCommand::System(_))
    }

    fn encode(&mut self, _cmd: &RuntimeCommand) -> Result<Vec<tqsdk_runtime_contract::OutboundRequest>> {
        Err(ContractError::UnsupportedCommand("system skeleton"))
    }

    fn accepts_input(&self, input: &RuntimeInput) -> bool {
        matches!(input, RuntimeInput::Internal(_))
    }

    fn decode(&mut self, _input: &RuntimeInput) -> Result<Vec<tqsdk_runtime_contract::NormalizedMutation>> {
        Ok(vec![])
    }
}

#[test]
fn adapter_registry_and_runtime_handle_surface_compile() {
    let mut registry = AdapterRegistry::new();
    registry.register_domain(ProtocolDomain::System);

    let handle = RuntimeHandle::new();
    let snapshot: StateSnapshot = handle.latest_snapshot();
    let cursor: UpdateCursor = handle.cursor();
    let log = CommitLog::new();

    assert_eq!(registry.domains(), &[ProtocolDomain::System]);
    assert_eq!(snapshot.revision().get(), 0);
    assert_eq!(cursor.next_revision().get(), 1);
    assert_eq!(log.head_revision(), None);

    fn assert_runtime<T: Runtime>(_value: &T) {}
    assert_runtime(&handle);

    let adapter = TestAdapter;
    assert_eq!(adapter.domain(), ProtocolDomain::System);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test runtime_contract_surface adapter_registry_and_runtime_handle_surface_compile -v`
Expected: FAIL with unresolved items such as `ProtocolAdapter` or `RuntimeHandle`

- [ ] **Step 3: Write the adapter and runtime modules**

```rust
// src/adapter.rs
use crate::{
    commands::{OutboundRequest, RuntimeCommand},
    error::Result,
    events::{NormalizedMutation, RuntimeInput},
    ids::ProtocolDomain,
};

pub trait ProtocolAdapter {
    fn domain(&self) -> ProtocolDomain;
    fn accepts_command(&self, cmd: &RuntimeCommand) -> bool;
    fn encode(&mut self, cmd: &RuntimeCommand) -> Result<Vec<OutboundRequest>>;
    fn accepts_input(&self, input: &RuntimeInput) -> bool;
    fn decode(&mut self, input: &RuntimeInput) -> Result<Vec<NormalizedMutation>>;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AdapterRegistry {
    domains: Vec<ProtocolDomain>,
}

impl AdapterRegistry {
    pub fn new() -> Self {
        Self { domains: Vec::new() }
    }

    pub fn register_domain(&mut self, domain: ProtocolDomain) {
        if !self.domains.contains(&domain) {
            self.domains.push(domain);
        }
    }

    pub fn domains(&self) -> &[ProtocolDomain] {
        &self.domains
    }
}
```

```rust
// src/runtime.rs
use crate::{
    commands::RuntimeCommand,
    error::Result,
    ids::{CommandId, CursorId, Revision},
    state::{StateSnapshot, UpdateCursor},
};

pub trait Runtime {
    async fn submit(&self, cmd: RuntimeCommand) -> Result<CommandId>;
    fn latest_snapshot(&self) -> StateSnapshot;
    fn cursor(&self) -> UpdateCursor;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitLog {
    head: Option<Revision>,
}

impl CommitLog {
    pub fn new() -> Self {
        Self { head: None }
    }

    pub fn head_revision(&self) -> Option<Revision> {
        self.head
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeHandle;

impl RuntimeHandle {
    pub fn new() -> Self {
        Self
    }
}

impl Runtime for RuntimeHandle {
    async fn submit(&self, _cmd: RuntimeCommand) -> Result<CommandId> {
        Ok(CommandId::new(1))
    }

    fn latest_snapshot(&self) -> StateSnapshot {
        StateSnapshot::new(Revision::new(0))
    }

    fn cursor(&self) -> UpdateCursor {
        UpdateCursor::new(CursorId::new(1), Revision::new(1))
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test runtime_contract_surface adapter_registry_and_runtime_handle_surface_compile -v`
Expected: PASS with `test adapter_registry_and_runtime_handle_surface_compile ... ok`

- [ ] **Step 5: Commit**

```bash
git add src/adapter.rs src/runtime.rs src/lib.rs tests/runtime_contract_surface.rs
git commit -m "feat: add adapter and runtime contract traits"
```

### Task 6: Run Full Surface Verification

**Files:**
- Modify: `tests/runtime_contract_surface.rs`
- Modify: `tests/runtime_contract_bootstrap.rs`

- [ ] **Step 1: Add a full-surface regression test**

```rust
// tests/runtime_contract_surface.rs
#[test]
fn public_surface_exports_are_usable_together() {
    let _revision = tqsdk_runtime_contract::Revision::new(11);
    let _command = tqsdk_runtime_contract::RuntimeCommand::System(
        tqsdk_runtime_contract::SystemCommand::RefreshAuth,
    );
    let _input = tqsdk_runtime_contract::RuntimeInput::Internal(
        tqsdk_runtime_contract::InternalEvent { label: "checkpoint" },
    );
    let _scope = tqsdk_runtime_contract::CommitScope::SessionTransition;
    let _domain = tqsdk_runtime_contract::ProtocolDomain::Schema;

    assert!(true);
}
```

- [ ] **Step 2: Run the full test suite**

Run: `cargo test -v`
Expected: PASS with both integration test files green

- [ ] **Step 3: Tighten crate root exports if anything is missing**

```rust
// src/lib.rs
pub mod adapter;
pub mod commands;
pub mod error;
pub mod events;
pub mod ids;
pub mod runtime;
pub mod state;

pub use adapter::{AdapterRegistry, ProtocolAdapter};
pub use commands::{
    CausationMeta, CommandEnvelope, CommandStatus, HttpRequest, InternalRequest, MarketCommand, OutboundFrame,
    OutboundRequest, QueryCommand, ReplayCommand, ReplayRequest, RuntimeCommand, SchemaCommand, SystemCommand,
    TradeCommand,
};
pub use error::{ContractError, Result};
pub use events::{
    AuthEvent, FieldMutation, InternalEvent, IoEvent, MutationSource, NormalizedMutation, ReplayEvent, RuntimeInput,
    TimerEvent,
};
pub use ids::{
    AccountId, AuthId, CommandId, CursorId, OrderId, ProtocolDomain, QueryId, ReplaySessionId, Revision, SchemaId,
    Symbol, TradeId,
};
pub use runtime::{CommitLog, Runtime, RuntimeHandle};
pub use state::{
    ChangeHit, ChangeSet, CommitResult, CommitScope, ObjectKey, PathSegment, SeriesKey, StatePath, StateSnapshot,
    UpdateCursor,
};
```

- [ ] **Step 4: Re-run the full test suite**

Run: `cargo test -v`
Expected: PASS with output ending in `test result: ok`

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs tests/runtime_contract_surface.rs tests/runtime_contract_bootstrap.rs
git commit -m "test: verify runtime contract surface exports"
```

## Self-Review

### Spec coverage

- Public runtime contract surface: covered by Tasks 2-5.
- Protocol adapter contract: covered by Task 5.
- Unified command, input, mutation, state, commit, cursor model: covered by Tasks 3-5.
- Contract-only V1 boundary: preserved by the chosen file set and by excluding any facade files.

### Placeholder scan

- No `TBD`, `TODO`, or “implement later” language remains in tasks.
- Every code-writing step includes concrete Rust code.
- Every verification step includes an exact command and expected result.

### Type consistency

- `RuntimeCommand`, `RuntimeInput`, `NormalizedMutation`, `StateSnapshot`, `CommitResult`, `UpdateCursor`, `ProtocolAdapter`, and `RuntimeHandle` use one consistent naming set across all tasks.
- The crate name is consistently `tqsdk_runtime_contract` in tests and `tqsdk-runtime-contract` in `Cargo.toml`.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-19-runtime-contract-skeleton.md`. Two execution options:

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**
