# V1 Core Crate Engine Refactor Design

**Date:** 2026-04-20
**Status:** Proposed design draft
**Scope:** Refactor the V1 runtime-contract crate into a publishable standalone core crate for high-performance consumers and future V2 facade crates

## 1. Summary

The current V1 crate is protocol-complete and semantically correct, but its implementation strategy is still biased toward convenience rather than hot-path efficiency.

The biggest mismatches with the intended audience are:

- `latest_snapshot() -> StateSnapshot` clones the full snapshot on every read
- core runtime state is guarded by coarse `Arc<Mutex<...>>` shells
- public reads are shaped around owned JSON snapshots instead of revision-bound zero-copy views
- runtime internals mix command routing, state storage, commit publication, and reader concerns in a small number of large modules

This refactor keeps V1 protocol semantics intact while changing the internal engine and part of the public core surface so the crate can be published as:

- a stable low-level runtime crate
- a dependency target for future V2 wait/stream/callback facade crates
- a performance-oriented base for advanced users rather than a research-oriented convenience SDK

## 2. Goals

- Preserve the existing protocol-complete semantic contract:
  - DIFF objects
  - trade commands and state
  - replay/feed progression
  - auth/session/system control
  - GraphQL / HTTP query
  - schema / metadata / bootstrap
- Replace clone-heavy public reads with revision-bound zero-copy read access.
- Separate internal runtime responsibilities into smaller modules with explicit ownership.
- Keep the crate maintainable enough to publish independently and evolve without V2 coupling.
- Preserve a narrow and defensible public surface for downstream facade crates.
- Improve hot-path cost without attempting a full typed-state rewrite in the same phase.

## 3. Non-Goals

This refactor does not:

- add any V2 facade
- add `wait_update`, stream, callback, or `TqApi`
- introduce typed quote/order/account views
- replace all `serde_json::Value` state with a typed arena store
- redesign protocol semantics, revision semantics, or command semantics
- change transport topology, auth semantics, or query/schema wire behavior

The phase is an engine refactor, not a product-surface expansion.

## 4. Chosen Approach

The chosen approach is a boundary-preserving engine refactor with limited public API breakage.

### 4.1 Why This Approach

It keeps the hardest-won part of V1 intact, which is the unified protocol contract, while fixing the parts that would make this crate a poor long-term dependency for higher-performance consumers.

It deliberately avoids a full typed-state rewrite because that would combine:

- semantic risk
- storage redesign
- API redesign
- migration burden

into one oversized phase.

### 4.2 What Changes

- The read surface becomes reader/guard based instead of clone-a-snapshot based.
- Runtime internals are split into smaller modules with clearer responsibilities.
- `StateSnapshot` stops being the primary stable public read artifact.
- `CommitLog` stops being the primary user-facing read entry point.
- Downstream crates read through a runtime reader abstraction bound to revision/cursor semantics.

### 4.3 What Does Not Change

- `RuntimeCommand`
- `RuntimeInput`
- `ProtocolAdapter`
- `NormalizedMutation`
- `Revision`
- `CommitResult`
- `ChangeSet`
- `UpdateCursor`
- command-to-commit semantics
- session/reconnect/bootstrap semantics

## 5. Target Public Surface

The refactored core crate should expose a smaller, more stable public surface.

### 5.1 Stable Public Runtime Surface

```rust
pub trait Runtime {
    fn submit(&self, cmd: RuntimeCommand) -> impl Future<Output = Result<CommandId>> + Send;
    fn reader(&self) -> RuntimeReader;
}

pub struct RuntimeHandle;
pub struct RuntimeReader;
pub struct SnapshotReadGuard<'a>;

impl RuntimeReader {
    pub fn cursor(&self) -> UpdateCursor;
    pub fn head_revision(&self) -> Option<Revision>;
    pub fn read(&self) -> SnapshotReadGuard<'_>;
    pub fn next(&self, cursor: &mut UpdateCursor) -> Option<CommitResult>;
}

impl SnapshotReadGuard<'_> {
    pub fn revision(&self) -> Revision;
    pub fn get<I, S>(&self, path: I) -> Option<&serde_json::Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>;
}
```

### 5.2 Public Contract Rules

- `RuntimeHandle` remains the write-side entry point.
- `RuntimeReader` becomes the only stable read-side entry point.
- `SnapshotReadGuard` is revision-bound and zero-copy.
- `UpdateCursor` remains the shared commit-consumption primitive.
- `CommitResult` remains owned for now; snapshot reads become borrowed.

### 5.3 Public API Breakage Allowed in This Phase

- `latest_snapshot() -> StateSnapshot` may be removed.
- direct public reliance on `StateSnapshot` as an owned cloneable snapshot may be removed or demoted from stable-surface status
- direct `CommitLog` usage may be replaced by `RuntimeReader`

This breakage is acceptable because the crate has not yet been published as a stable ecosystem dependency and this phase exists specifically to lock in the right base API before V2 crates depend on it.

## 6. Internal Module Layout

The crate should be reorganized around explicit ownership boundaries.

### 6.1 Keep as Pure Contract Modules

- `commands`
- `events`
- `ids`
- `error`
- `auth`

These modules remain mostly shape/type oriented.

### 6.2 Split Runtime Internals

The current `runtime` module should be decomposed into:

- `runtime/mod.rs`
  - public runtime facade exports only
- `runtime/handle.rs`
  - `RuntimeHandle`
  - command submission
  - reader construction
- `runtime/reader.rs`
  - `RuntimeReader`
  - `SnapshotReadGuard`
  - cursor-based commit consumption
- `runtime/command_ledger.rs`
  - command id allocation
  - command domain registry
  - command detail seed storage
  - command status mutation helpers
- `runtime/commit_log.rs`
  - revision-indexed commit storage
  - cursor advancement logic
- `runtime/commit_engine.rs`
  - mutation application
  - visible-change detection
  - commit assembly

### 6.3 Split State Internals

The current `state` module should be decomposed into:

- `state/mod.rs`
  - public state-facing exports only
- `state/store.rs`
  - mutable canonical state
- `state/read.rs`
  - borrowed read-only access helpers
- `state/changes.rs`
  - `ChangeHit`
  - `ChangeSet`
  - `CommitResult`
  - `CommitScope`
  - `UpdateCursor`
- `state/path.rs`
  - `StatePath`
  - `ObjectKey`
  - `SeriesKey`

### 6.4 Leave Protocol Adapters Separate

`adapter` remains separate because protocol encode/decode belongs at the boundary, not inside runtime state management.

Its responsibilities remain:

- protocol command encoding
- protocol input decoding
- protocol-local short-lived adapter state

It must not gain:

- commit ownership
- reader behavior
- snapshot APIs

### 6.5 Keep Session Runtime as Orchestration

`session_runtime` remains a separate orchestration layer and should be treated as:

- transport/bootstrap/recovery coordination
- route pumping
- route executor wiring
- command status derivation from commits

It should not own:

- canonical state store
- commit storage
- reader API

## 7. Engine Design Constraints

### 7.1 Read Path

The read path must stop cloning the full snapshot.

Required properties:

- zero-copy path lookup from a borrowed guard
- revision-bound reads
- no mutable access from read APIs
- no implicit snapshot duplication for downstream facade polling

### 7.2 Commit Path

The commit path must remain unified.

Required properties:

- command submission does not advance revision
- only visible mutations create commits
- all protocol domains share one revision stream
- command causality remains attached to commits

### 7.3 State Storage

For this phase, canonical state may remain JSON-backed internally, but it must be encapsulated behind store/read abstractions.

That means:

- `serde_json::Value` may stay inside the store
- direct owned snapshot cloning is no longer the primary API
- downstream crates must not depend on the storage representation

This preserves freedom for a subsequent typed/interned store migration phase.

## 8. Migration Rules for Existing Code

The refactor must preserve behavioral coverage while updating call sites.

### 8.1 Old Style

```rust
let snapshot = handle.latest_snapshot();
let value = snapshot.get(["quotes", "SHFE.au2602", "last_price"]);
```

### 8.2 New Style

```rust
let reader = handle.reader();
let snapshot = reader.read();
let value = snapshot.get(["quotes", "SHFE.au2602", "last_price"]);
```

### 8.3 Consumer Rule

Future V2 crates must consume the core crate through:

- `RuntimeHandle`
- `RuntimeReader`
- `UpdateCursor`
- `CommitResult`
- contract types

They must not depend on internal state storage structures.

## 9. Validation Requirements

This refactor is complete only if all of the following remain true:

- current V1 capability tests still pass after API migration
- live smoke still proves:
  - auth + market websocket contract
  - schema/http contract
- downstream-style read access works without full snapshot clone
- cursor-based consumption still supports future:
  - wait facade
  - stream facade
  - callback facade

Additional required validation for this phase:

- compile-level proof that readers can read snapshots without owning them
- no public V2 convenience surface leaks into the crate
- docs reflect the new reader-centric public contract

## 10. Release Intent

After this refactor, the crate should be publishable as a standalone core crate whose contract promise is:

- protocol-complete
- facade-free
- revision/cursor/commit stable
- performance-oriented on hot reads
- maintainable as a long-lived dependency for higher-layer crates

This phase does not need to finish the ultimate storage model.
It does need to finish the right dependency shape.
