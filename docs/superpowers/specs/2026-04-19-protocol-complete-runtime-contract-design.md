# Protocol-Complete Runtime Contract Design

**Date:** 2026-04-19
**Status:** Approved design draft
**Scope:** V1 foundation layer for a Rust-native TqSdk runtime

## 1. Summary

V1 is not a user-facing `wait_update()` SDK and not a `stream/callback` SDK.
V1 is a protocol-complete runtime contract that unifies all remote interactions under one commit model.

This runtime contract must be sufficient to support, without core redesign:

- a Python-style `wait_update` facade
- a Rust-style `stream/callback` facade

The V1 runtime must cover:

- all DIFF-protocol-backed objects
- trade commands and trade state
- replay/feed commands and replay state
- auth/session/system control
- GraphQL / HTTP query flows
- schema / metadata / bootstrap interactions

V1 must not provide any high-level user facade.

## 2. Goals

- Define one stable runtime contract for all remote protocols and objects.
- Ensure all visible state flows through one `Revision` / `CommitResult` / `ChangeSet` model.
- Make `wait_update` and `stream/callback` future adapters over the same commit log and cursor model.
- Keep protocol-specific complexity inside adapters rather than inside user-facing facades.
- Preserve enough semantic fidelity to later build Python-compatible behavior without rewriting the kernel.

## 3. Non-Goals

V1 explicitly does not provide:

- `TqApi`
- `wait_update()` facade
- stream facade
- callback facade
- high-level quote / kline / tick / order / account views
- `TargetPosTask`
- strategy/task orchestration
- DataFrame / polars / downloader / web helper / GUI / report layers
- Python surface compatibility at the API naming level
- end-user ergonomics for strategy authors

V1 is judged on contract completeness, not end-user convenience.

## 4. Public Boundary

V1 exposes exactly two stable public surfaces.

### 4.1 Runtime Contract

The runtime contract is the only canonical public entry point for V1.

It includes:

- `RuntimeHandle`
- `RuntimeCommand`
- `RuntimeInput`
- `Revision`
- `CommitResult`
- `ChangeSet`
- `StateSnapshot`
- `UpdateCursor`
- command/result identity types such as `CommandId` and `CursorId`

### 4.2 Protocol Adapter Contract

The protocol adapter contract is the only stable extension surface in V1.

It includes:

- `ProtocolAdapter`
- `ProtocolDomain`
- `NormalizedMutation`
- `OutboundRequest`
- adapter registration / composition interfaces

### 4.3 Not Public in V1

The following do not belong in the V1 public surface:

- user strategy facades
- wait adapters
- stream adapters
- callback adapters
- typed convenience views
- task helpers

## 5. Unified Commit Model

All protocols must flow through one shared commit pipeline:

```text
RuntimeCommand
  -> SessionRuntime
  -> ProtocolAdapter.encode()
  -> Transport / HTTP / ReplayFeed
  -> ProtocolAdapter.decode()
  -> RuntimeInput / NormalizedMutation
  -> StateStore.apply()
  -> ProjectionEngine.project()
  -> CommitAssembler
  -> CommitResult { revision, changes, snapshot_ref }
  -> CommitLog
  -> UpdateCursor
```

### 5.1 Rules

- No protocol may bypass `CommitAssembler`.
- `Revision` advances only when a visible commit is formed.
- `StateSnapshot` must always represent a committed revision, never a mid-merge state.
- Future `wait_update` and `stream/callback` adapters may differ only in commit consumption, never in commit generation.

### 5.2 Core Runtime Responsibilities

The runtime owns:

- session lifecycle
- command submission
- transport and request dispatch coordination
- adapter routing
- mutation aggregation
- state application
- projection
- commit assembly
- commit log publication
- cursor creation

## 6. Public Contract Shape

The V1 contract should use stable shells with domain-specific payloads.

```rust
pub struct Revision(u64);
pub struct CommandId(u64);
pub struct CursorId(u64);

pub struct RuntimeHandle;
pub struct StateSnapshot;
pub struct CommitResult;
pub struct ChangeSet;
pub struct UpdateCursor;

pub enum RuntimeCommand {
    System(SystemCommand),
    Market(MarketCommand),
    Trade(TradeCommand),
    Replay(ReplayCommand),
    Query(QueryCommand),
    Schema(SchemaCommand),
}

pub enum RuntimeInput {
    Io(IoEvent),
    Timer(TimerEvent),
    Auth(AuthEvent),
    Replay(ReplayEvent),
    Internal(InternalEvent),
}

pub trait ProtocolAdapter {
    fn domain(&self) -> ProtocolDomain;
    fn accepts_command(&self, cmd: &RuntimeCommand) -> bool;
    fn encode(&mut self, cmd: &RuntimeCommand) -> Result<Vec<OutboundRequest>>;
    fn accepts_input(&self, input: &RuntimeInput) -> bool;
    fn decode(&mut self, input: &RuntimeInput) -> Result<Vec<NormalizedMutation>>;
}

pub trait Runtime {
    async fn submit(&self, cmd: RuntimeCommand) -> Result<CommandId>;
    fn latest_snapshot(&self) -> StateSnapshot;
    fn cursor(&self) -> UpdateCursor;
}
```

### 6.1 Contract Intent

- `RuntimeHandle` is the only V1 entry point.
- `submit()` returns command identity, not command completion.
- `StateSnapshot` is revision-bound and read-only.
- `UpdateCursor` is the shared base for all future consumer styles.
- `ProtocolAdapter` may encode/decode protocol traffic, but may not publish commits or mutate cursors directly.

## 7. Unified State Model

`StateSnapshot` must support both protocol-native structure and logical object identity.

### 7.1 Two Views Over the Same Snapshot

1. `StatePath`
   Preserves wire/protocol-oriented layout for DIFF, GraphQL, HTTP, replay, and schema state.

2. `ObjectKey`
   Provides stable logical identity for future facades and change queries.

### 7.2 Namespaces

The unified state tree should reserve at least these namespaces:

- `system/*`
- `schema/*`
- `market/*`
- `trade/*`
- `replay/*`
- `query/*`
- `runtime/*`

### 7.3 Identity Types

```rust
pub struct StatePath(Vec<PathSegment>);

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
```

### 7.4 State Rules

- Every mutation must write to at least one `StatePath`.
- When a logical object can be identified, it should also map to an `ObjectKey`.
- Query and schema results must enter the same committed snapshot as market/trade/replay state.
- `ChangeSet` must support path hits, object hits, and field hits.

## 8. Command Causality and Error Model

V1 must treat command lifecycle as part of the runtime contract.

### 8.1 Command Envelope

```rust
pub struct CommandEnvelope {
    pub id: CommandId,
    pub issued_at: Instant,
    pub command: RuntimeCommand,
    pub causation: CausationMeta,
}

pub struct CommitResult {
    pub revision: Revision,
    pub changes: ChangeSet,
    pub caused_by: SmallVec<[CommandId; 4]>,
    pub scope: CommitScope,
}
```

### 8.2 Command Status

Command ledger state belongs in the runtime snapshot.

```rust
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
```

### 8.3 Error Layers

1. Submit-time error
   Local validation failure. Command never enters runtime.

2. Command-scoped error
   Runtime accepted the command, but remote execution failed or was rejected.

3. Session-scoped error
   Connection, auth, replay source, transport, or runtime-level failure.

### 8.4 Error Rules

- Visible command results must not live in out-of-band futures.
- Command-scoped and session-scoped errors must enter committed state.
- Future facades must observe command outcomes through commits, not through private completion channels.

## 9. Adapter Composition

V1 uses multiple protocol adapters coordinated by one runtime, not one giant adapter.

### 9.1 Adapter Types

- `SystemAdapter`
- `MarketDiffAdapter`
- `TradeAdapter`
- `QueryAdapter`
- `ReplayAdapter`

### 9.2 Runtime Responsibilities

The runtime:

- routes commands to the owning adapter
- routes inputs to interested adapters
- gathers normalized mutations
- applies state changes
- runs projection
- produces commits

### 9.3 Adapter Rules

- A business command has one owning business adapter.
- Runtime may derive auxiliary system work, but ownership stays singular.
- An input may be observed by multiple adapters.
- Only normalized mutations may affect committed visible state.
- Adapters may keep private short-lived protocol state.
- Any state visible to consumers must be written into `StateStore`.
- Adapters may not publish notifications, advance revisions, or move cursors.

## 10. Validation Boundary

V1 is validated at the contract level.

### 10.1 Required Acceptance Criteria

- Every remote interaction goes through the unified command-to-commit pipeline.
- All visible results enter the same `StateSnapshot`.
- All visible changes are expressed through one `Revision` / `ChangeSet` model.
- All consumers read through `UpdateCursor` / `CommitLog`.
- No adapter bypasses the commit model.
- Trade, replay, query, schema, and system errors share the same commit semantics.
- A future `wait_update` facade can be implemented without changing the runtime core.
- A future `stream/callback` facade can be implemented without changing the runtime core.

### 10.2 Acceptable Deferrals

The following may be deferred past V1:

- final `wait_update()` behavior
- final `is_changing()` API
- final stream facade shape
- final callback facade shape
- final user-facing module naming

## 11. Architecture Doc Rewrite Plan

The existing architecture docs should be rewritten around this design.

### 11.1 Required Directional Changes

- `README.md`
  Reframe V1 as a unified runtime contract, not a wait-first API layer.
- `roadmap.md`
  Replace quote-wait-first staging with protocol-complete contract-first staging.
- `api-layers.md`
  Move `wait` / `stream` / `callback` to V2+ adapters.
- `runtime-core/overview.md`
  Center on `RuntimeHandle`, `CommitLog`, `UpdateCursor`, `ProtocolAdapter`.
- `runtime-core/modules.md`
  Center on session runtime, adapter registry, state/commit core.
- `runtime-core/data-contracts.md`
  Lock `RuntimeCommand`, `RuntimeInput`, `NormalizedMutation`, `OutboundRequest`, `CommitResult`.
- `runtime-core/type-system.md`
  Lock `StatePath`, `ObjectKey`, `CommandId`, `Revision`, `CursorId`.
- `validation.md`
  Shift V1 validation from `wait_update` semantics to contract completeness.

## 12. Open Risks

- If `NormalizedMutation` is too raw, future facades will need kernel changes.
- If it is too high-level, V1 will prematurely freeze object semantics.
- Query/schema state can easily drift into side caches unless explicitly forced into `StateSnapshot`.
- Trade and replay may pressure the model toward protocol-specific shortcuts; these must be rejected if they bypass commit semantics.

## 13. Decision

V1 will be implemented as a protocol-complete runtime contract.

High-level consumption styles, including Python-style `wait_update` and Rust-style `stream/callback`, are explicitly deferred to adapter layers built on top of:

- one runtime handle
- one unified state snapshot
- one commit model
- one shared cursor/log foundation
