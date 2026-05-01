# Core Safe Surface Narrowing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the `tqsdk-core` root public exports that the disposition matrix marks safe to internalize, without changing the documented runtime contract.

**Architecture:** Keep `RuntimeHandle`, `RuntimeReader`, `RuntimeInput`, `NormalizedMutation`, `OutboundRequest`, `OutboundDispatch`, and reader/cursor primitives public. Move the aggregation helper out of the normal public build because it is test-only today, and hide `OutboundEnvelope` by replacing external tests with `RuntimeHandle::drain_dispatches()`. Do not change command state transitions, runtime commits, auth/session behavior, or trade command shape in this plan.

**Tech Stack:** Rust, Cargo integration tests, architecture docs under `docs/architecture`.

---

## Files

- Modify: `crates/tqsdk-core/src/lib.rs`
- Modify: `crates/tqsdk-core/src/aggregation.rs`
- Modify: `crates/tqsdk-core/src/runtime/mod.rs`
- Modify: `crates/tqsdk-core/src/runtime/handle.rs`
- Modify: `crates/tqsdk-core/tests/runtime_contract_runtime_core.rs`
- Modify: `crates/tqsdk-core/tests/runtime_contract_command_ledger.rs`
- Delete: `crates/tqsdk-core/tests/runtime_contract_aggregation.rs`
- Modify: `docs/architecture/api-layers.md`
- Do not modify in this focused plan: `crates/tqsdk-core/src/auth.rs`
- Do not modify in this focused plan: `crates/tqsdk-core/src/commands.rs`

## Deferred Out Of This Child Plan

- Do not touch `AuthContext`; it was handled by `docs/superpowers/plans/2026-04-29-core-auth-context-privacy.md`.
- Do not restructure `TradePreInsertOrderCommand`; the disposition matrix marks it `split-plan`.
- Do not internalize `RuntimeInput`, `NormalizedMutation`, `SnapshotReadGuard`, `CommitLog`, `OutboundRequest`, `OutboundDispatch`, or any type explicitly protected by `docs/architecture/api-layers.md`.

## Task 1: Characterize Current Safe Surface Usage

- [x] **Step 1: Confirm usage is limited to root exports and tests**

Run:

```bash
rg -n "AggregatedCommit|AggregatedCursor|AggregatedRuntimeReader|AggregatedSnapshotReadGuard|StateSourceId|OutboundEnvelope|drain_outbound" crates docs README.md
```

Expected:

```text
Aggregation symbols are only referenced by `src/aggregation.rs`, root re-exports, and `runtime_contract_aggregation.rs`.
`OutboundEnvelope` and `drain_outbound` are only referenced by runtime internals, root re-exports, and core contract tests.
```

- [x] **Step 2: Run focused baseline tests**

Run:

```bash
cargo test -p tqsdk-core --test runtime_contract_aggregation
cargo test -p tqsdk-core --test runtime_contract_runtime_core
cargo test -p tqsdk-core --test runtime_contract_command_ledger
```

Expected:

```text
All focused baseline tests pass before moving or narrowing the public surface.
```

## Task 2: Move Aggregation Coverage To A Private Unit Test

- [x] **Step 1: Add unit-test coverage to `crates/tqsdk-core/src/aggregation.rs`**

Append this `#[cfg(test)]` module to the end of the file:

```rust
#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        AdapterRegistry, CommitScope, InputPayload, IoEvent, ProtocolDomain, RuntimeHandle,
        RuntimeInput,
    };

    use super::{AggregatedRuntimeReader, StateSourceId};

    #[test]
    fn aggregated_reader_keeps_two_source_snapshots_and_commits_separate() {
        let primary = runtime_with_default_adapters();
        let backup = runtime_with_default_adapters();

        ingest_quote(&primary, 601.0);
        ingest_quote(&backup, 701.0);

        let mut aggregate = AggregatedRuntimeReader::new();
        let primary_id = StateSourceId::new("primary");
        let backup_id = StateSourceId::new("backup");
        aggregate.insert_source(primary_id.clone(), primary.reader());
        aggregate.insert_source(backup_id.clone(), backup.reader());

        let read = aggregate.read();
        assert_eq!(read.revision(&primary_id).unwrap().get(), 1);
        assert_eq!(read.revision(&backup_id).unwrap().get(), 1);
        assert_eq!(
            read.get(&primary_id, ["quotes", "SHFE.au2602", "last_price"]),
            Some(&json!(601.0))
        );
        assert_eq!(
            read.get(&backup_id, ["quotes", "SHFE.au2602", "last_price"]),
            Some(&json!(701.0))
        );
        drop(read);

        let mut cursor = aggregate.cursor();
        ingest_quote(&primary, 602.0);
        ingest_quote(&backup, 702.0);

        let first = aggregate
            .next(&mut cursor)
            .expect("primary update should be visible through aggregate cursor");
        let second = aggregate
            .next(&mut cursor)
            .expect("backup update should be visible through aggregate cursor");
        assert_eq!(first.source_id.as_str(), "primary");
        assert_eq!(first.commit.revision.get(), 2);
        assert_eq!(second.source_id.as_str(), "backup");
        assert_eq!(second.commit.revision.get(), 2);
        assert!(
            aggregate.next(&mut cursor).is_none(),
            "aggregate cursor should advance each source independently"
        );
    }

    fn runtime_with_default_adapters() -> RuntimeHandle {
        let mut registry = AdapterRegistry::new();
        registry.register_default_adapters();
        RuntimeHandle::with_adapters(registry)
    }

    fn ingest_quote(handle: &RuntimeHandle, last_price: f64) {
        handle
            .ingest(
                RuntimeInput::Io(IoEvent {
                    route: "market.shared".to_string(),
                    domains: vec![ProtocolDomain::Market],
                    payload: InputPayload::Json(json!({
                        "aid": "rtn_data",
                        "data": [{
                            "quotes": {
                                "SHFE.au2602": {
                                    "last_price": last_price
                                }
                            }
                        }]
                    })),
                }),
                Vec::new(),
                CommitScope::RealtimeUpdate,
            )
            .unwrap()
            .expect("quote update should publish a commit");
    }
}
```

- [x] **Step 2: Remove the public integration contract file**

Delete:

```text
crates/tqsdk-core/tests/runtime_contract_aggregation.rs
```

- [x] **Step 3: Remove aggregation root re-exports from `crates/tqsdk-core/src/lib.rs`**

Change:

```rust
mod aggregation;
```

To:

```rust
#[cfg(test)]
mod aggregation;
```

Remove this root export block:

```rust
pub use aggregation::{
    AggregatedCommit, AggregatedCursor, AggregatedRuntimeReader, AggregatedSnapshotReadGuard,
    StateSourceId,
};
```

- [x] **Step 4: Run the moved aggregation test**

Run:

```bash
cargo test -p tqsdk-core aggregated_reader_keeps_two_source_snapshots_and_commits_separate
```

Expected:

```text
The aggregation coverage still passes as a private unit test, and no public integration test depends on root aggregation exports.
```

## Task 3: Hide OutboundEnvelope And Use OutboundDispatch In Tests

- [x] **Step 1: Make `OutboundEnvelope` crate-private in `crates/tqsdk-core/src/runtime/handle.rs`**

Change:

```rust
pub struct OutboundEnvelope {
    pub command_id: CommandId,
    pub request: OutboundRequest,
}
```

To:

```rust
pub(crate) struct OutboundEnvelope {
    pub(crate) command_id: CommandId,
    pub(crate) request: OutboundRequest,
}
```

- [x] **Step 2: Remove `RuntimeHandle::drain_outbound()`**

Delete this method from `crates/tqsdk-core/src/runtime/handle.rs`:

```rust
pub fn drain_outbound(&self) -> Vec<OutboundEnvelope> {
    let mut inner = mutex_lock(&self.inner);
    inner.outbound.drain(..).collect()
}
```

Do not change `RuntimeHandle::drain_dispatches()`.

- [x] **Step 3: Remove `OutboundEnvelope` from public re-exports**

In `crates/tqsdk-core/src/runtime/mod.rs`, change:

```rust
pub use handle::{OutboundEnvelope, Runtime, RuntimeHandle};
```

To:

```rust
pub use handle::{Runtime, RuntimeHandle};
```

In `crates/tqsdk-core/src/lib.rs`, remove `OutboundEnvelope` from the `pub use runtime::{ ... }` list.

- [x] **Step 4: Update `runtime_contract_runtime_core.rs` to assert dispatches**

Replace `OutboundEnvelope` imports with `OutboundDispatch`, and replace `handle.drain_outbound()` assertions with `handle.drain_dispatches().unwrap()` assertions.

The market subscribe expectation should use this shape:

```rust
OutboundDispatch {
    command_id,
    domain: ProtocolDomain::Market,
    account_id: None,
    request: OutboundRequest::Transport(OutboundFrame::Text(
        json!({"aid": "subscribe_quote", "ins_list": "SHFE.au2602"}).to_string(),
    )),
}
```

The refresh-auth expectation should use this shape:

```rust
OutboundDispatch {
    command_id: submit_id,
    domain: ProtocolDomain::System,
    account_id: None,
    request: OutboundRequest::internal_label("refresh-auth"),
}
```

- [x] **Step 5: Update `runtime_contract_command_ledger.rs` to assert dispatches**

Replace `OutboundEnvelope` imports with `OutboundDispatch`, and replace the insert-order `drain_outbound()` assertion with `handle.drain_dispatches().unwrap()`.

The insert-order expectation should include:

```rust
OutboundDispatch {
    command_id,
    domain: ProtocolDomain::Trade,
    account_id: Some(AccountId::new("simnow")),
    request: OutboundRequest::Transport(OutboundFrame::Text(
        json!({
            "aid": "insert_order",
            "user_id": "simnow",
            "order_id": "order-1",
            "exchange_id": "SHFE",
            "instrument_id": "au2602",
            "direction": "BUY",
            "offset": "OPEN",
            "volume": 2,
            "price_type": "LIMIT",
            "limit_price": 618.5,
            "time_condition": "GFD",
            "volume_condition": "ANY",
        })
        .to_string(),
    )),
}
```

- [x] **Step 6: Run focused outbound tests**

Run:

```bash
cargo test -p tqsdk-core --test runtime_contract_runtime_core
cargo test -p tqsdk-core --test runtime_contract_command_ledger
```

Expected:

```text
Runtime outbound coverage still passes through `OutboundDispatch`; `OutboundEnvelope` is no longer imported from `tqsdk_core`.
```

## Task 4: Sync Architecture Documentation

- [x] **Step 1: Update `docs/architecture/api-layers.md`**

Under the V1 runtime contract list, keep `OutboundRequest` and add this sentence after the protocol adapter contract bullets:

```text
Raw runtime outbox envelopes and multi-source aggregation helpers are not part of the V1 public contract; low-level route consumers should use `OutboundDispatch` and reader/cursor primitives.
```

- [x] **Step 2: Confirm protected symbols remain documented**

Run:

```bash
rg -n "RuntimeInput|NormalizedMutation|SnapshotReadGuard|CommitLog|OutboundRequest|OutboundDispatch" docs/architecture/api-layers.md crates/tqsdk-core/src/lib.rs
```

Expected:

```text
Protected symbols still appear in public exports and architecture docs.
```

## Task 5: Verify Core Surface Narrowing

- [x] **Step 1: Run focused symbol scan**

Run:

```bash
rg -n "AggregatedCommit|AggregatedCursor|AggregatedRuntimeReader|AggregatedSnapshotReadGuard|StateSourceId|OutboundEnvelope|drain_outbound" crates docs README.md
```

Expected:

```text
Aggregation names only appear in `src/aggregation.rs`; `OutboundEnvelope` and `drain_outbound` only appear in private runtime internals.
```

- [x] **Step 2: Run parent Task 6 core contract commands**

Run:

```bash
cargo test -p tqsdk-core -q --test runtime_contract_v1_capability
cargo test -p tqsdk-core -q --test runtime_contract_reader_surface --test runtime_contract_surface
cargo test -p tqsdk-core
cargo check --workspace --examples
```

Expected:

```text
Core contract tests and workspace examples still pass after removing the root public exports.
```

## Task 6: Commit

- [x] **Step 1: Stage the focused core surface narrowing batch**

Run:

```bash
git add crates/tqsdk-core/src/lib.rs crates/tqsdk-core/src/aggregation.rs crates/tqsdk-core/src/runtime/mod.rs crates/tqsdk-core/src/runtime/handle.rs crates/tqsdk-core/tests/runtime_contract_runtime_core.rs crates/tqsdk-core/tests/runtime_contract_command_ledger.rs crates/tqsdk-core/tests/runtime_contract_aggregation.rs docs/architecture/api-layers.md
```

- [x] **Step 2: Commit**

Run:

```bash
git commit -m "refactor(core): narrow runtime internal surface"
```

Expected:

```text
A focused commit exists for aggregation root export removal and `OutboundEnvelope` internalization only. No `AuthContext` or `TradePreInsertOrderCommand` changes are included.
```

Output:

- Verification before commit:
  - `cargo test -p tqsdk-core -q --test runtime_contract_v1_capability`
  - `cargo test -p tqsdk-core -q --test runtime_contract_reader_surface --test runtime_contract_surface`
  - `cargo test -p tqsdk-core`
  - `cargo check --workspace --examples`
- Committed in worktree `.worktrees/audit-guardrails` as `1556e93 refactor(core): narrow runtime internal surface`.
- Parent Task 6 documentation sync was completed separately as `33e1df5 docs(core): clarify runtime public surface` because the initial focused code commit had already been created and was not amended.
