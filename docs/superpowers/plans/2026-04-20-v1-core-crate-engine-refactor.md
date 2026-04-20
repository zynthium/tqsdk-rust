# V1 Core Crate Engine Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor the V1 runtime-contract crate into a publishable standalone core crate with a reader-based zero-copy read surface and cleaner internal module boundaries, while preserving protocol-complete runtime semantics.

**Architecture:** The refactor keeps command, revision, commit, cursor, adapter, and session semantics intact while replacing clone-heavy snapshot reads with a `RuntimeReader` plus revision-bound `SnapshotReadGuard`. Runtime internals are split into handle, reader, commit log, command ledger, and commit engine modules, and state internals are split into store, read, path, and change modules. Existing tests are migrated in stages so the protocol contract remains continuously verified.

**Tech Stack:** Rust 2024, std synchronization primitives, `serde_json`, `tokio`, `reqwest`, existing runtime/session/adapter test suite.

---

## File Structure

**Create:**
- `src/runtime/mod.rs`
- `src/runtime/handle.rs`
- `src/runtime/reader.rs`
- `src/runtime/commit_log.rs`
- `src/runtime/command_ledger.rs`
- `src/runtime/commit_engine.rs`
- `src/state/mod.rs`
- `src/state/store.rs`
- `src/state/read.rs`
- `src/state/changes.rs`
- `src/state/path.rs`
- `tests/runtime_contract_reader_surface.rs`

**Modify:**
- `src/lib.rs`
- `src/session_runtime.rs`
- `tests/runtime_contract_surface.rs`
- `tests/runtime_contract_runtime_core.rs`
- `tests/runtime_contract_v1_capability.rs`
- `tests/runtime_contract_live_smoke.rs`
- `tests/runtime_contract_session_*.rs`
- `tests/runtime_contract_*`
- `docs/architecture/README.md`
- `docs/architecture/validation.md`
- `docs/architecture/runtime-core/overview.md`

**Delete or replace during migration:**
- `src/runtime.rs`
- `src/state.rs`

**Responsibility map:**
- `src/runtime/handle.rs`: write-side runtime API, command submission, reader creation
- `src/runtime/reader.rs`: read-side API, borrowed snapshot access, cursor-based log reads
- `src/runtime/commit_log.rs`: revision-indexed commit storage and cursor advancement
- `src/runtime/command_ledger.rs`: command metadata, domain mapping, status detail seeds
- `src/runtime/commit_engine.rs`: mutation application, visible-change filtering, commit assembly
- `src/state/store.rs`: mutable canonical state storage
- `src/state/read.rs`: borrowed read guard over store
- `src/state/changes.rs`: `ChangeHit`, `ChangeSet`, `CommitResult`, `CommitScope`, `UpdateCursor`
- `src/state/path.rs`: `StatePath`, `ObjectKey`, `SeriesKey`

### Task 1: Introduce Reader-Centric Public Surface

**Files:**
- Create: `tests/runtime_contract_reader_surface.rs`
- Modify: `src/lib.rs`
- Modify: `src/runtime.rs`
- Modify: `src/state.rs`
- Test: `tests/runtime_contract_reader_surface.rs`
- Test: `tests/runtime_contract_surface.rs`

- [ ] **Step 1: Write the failing reader-surface tests**

```rust
use tqsdk_runtime_contract::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, ProtocolDomain, Revision, Runtime,
    RuntimeHandle, RuntimeInput, RuntimeReader,
};

#[test]
fn runtime_reader_exposes_zero_copy_snapshot_reads_and_cursor_access() {
    let handle = runtime_with_default_adapters();
    let reader: RuntimeReader = handle.reader();

    {
        let snapshot = reader.read();
        assert_eq!(snapshot.revision(), Revision::new(0));
        assert_eq!(snapshot.get(["quotes", "SHFE.au2602"]), None);
    }

    handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(serde_json::json!({
                    "data": [{
                        "quotes": {
                            "SHFE.au2602": { "last_price": 512.0 }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap();

    {
        let snapshot = reader.read();
        assert_eq!(snapshot.revision(), Revision::new(1));
        assert_eq!(
            snapshot.get(["quotes", "SHFE.au2602", "last_price"]),
            Some(&serde_json::json!(512.0))
        );
    }

    let mut cursor = reader.cursor();
    let commit = reader.next(&mut cursor).unwrap();
    assert_eq!(commit.revision, Revision::new(1));
}

#[test]
fn runtime_handle_runtime_trait_surface_uses_reader_not_owned_snapshot() {
    let handle = RuntimeHandle::new();
    let reader = handle.reader();
    assert_eq!(reader.head_revision(), None);
    assert_eq!(reader.cursor().next_revision().get(), 1);
}
```

- [ ] **Step 2: Run the new tests to verify they fail for the expected reason**

Run: `cargo test -q --test runtime_contract_reader_surface`
Expected: FAIL with missing `RuntimeReader`, missing `handle.reader()`, or missing borrowed read API.

- [ ] **Step 3: Implement the minimal reader API in the existing modules first**

```rust
pub trait Runtime {
    fn submit(&self, cmd: RuntimeCommand) -> impl Future<Output = Result<CommandId>> + Send;
    fn reader(&self) -> RuntimeReader;
}

#[derive(Clone)]
pub struct RuntimeReader {
    inner: Arc<Mutex<RuntimeCore>>,
    commit_log: CommitLog,
}

pub struct SnapshotReadGuard<'a> {
    guard: std::sync::MutexGuard<'a, RuntimeCore>,
}

impl RuntimeReader {
    pub fn read(&self) -> SnapshotReadGuard<'_> {
        SnapshotReadGuard {
            guard: self.inner.lock().expect("runtime mutex poisoned"),
        }
    }

    pub fn cursor(&self) -> UpdateCursor {
        let next_revision = Revision::new(
            self.commit_log
                .head_revision()
                .map_or(1, |revision| revision.get() + 1),
        );
        let mut inner = self.inner.lock().expect("runtime mutex poisoned");
        let cursor_id = CursorId::new(inner.next_cursor_id);
        inner.next_cursor_id += 1;
        UpdateCursor::new(cursor_id, next_revision)
    }

    pub fn next(&self, cursor: &mut UpdateCursor) -> Option<CommitResult> {
        self.commit_log.next(cursor)
    }
}

impl SnapshotReadGuard<'_> {
    pub fn revision(&self) -> Revision {
        self.guard.snapshot.revision()
    }

    pub fn get<I, S>(&self, path: I) -> Option<&serde_json::Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.guard.snapshot.get(path)
    }
}
```

- [ ] **Step 4: Run the targeted reader-surface tests to verify they pass**

Run: `cargo test -q --test runtime_contract_reader_surface --test runtime_contract_surface`
Expected: PASS

- [ ] **Step 5: Commit the reader-surface introduction**

```bash
git add tests/runtime_contract_reader_surface.rs src/lib.rs src/runtime.rs src/state.rs tests/runtime_contract_surface.rs
git commit -m "refactor: introduce reader-based runtime surface"
```

### Task 2: Split Runtime And State Modules Without Changing Semantics

**Files:**
- Create: `src/runtime/mod.rs`
- Create: `src/runtime/handle.rs`
- Create: `src/runtime/reader.rs`
- Create: `src/runtime/commit_log.rs`
- Create: `src/runtime/command_ledger.rs`
- Create: `src/runtime/commit_engine.rs`
- Create: `src/state/mod.rs`
- Create: `src/state/store.rs`
- Create: `src/state/read.rs`
- Create: `src/state/changes.rs`
- Create: `src/state/path.rs`
- Modify: `src/lib.rs`
- Delete: `src/runtime.rs`
- Delete: `src/state.rs`
- Test: `tests/runtime_contract_runtime_core.rs`
- Test: `tests/runtime_contract_commit_semantics.rs`
- Test: `tests/runtime_contract_batch_commit.rs`

- [ ] **Step 1: Write a failing compile-level migration by switching `lib.rs` to module directories**

```rust
pub mod runtime;
pub mod state;

pub use runtime::{Runtime, RuntimeHandle, RuntimeReader, SnapshotReadGuard};
pub use state::{
    ChangeHit, ChangeSet, CommitResult, CommitScope, ObjectKey, PathSegment, SeriesKey, StatePath,
    UpdateCursor,
};
```

- [ ] **Step 2: Run the targeted runtime/state tests to verify the module split fails before implementation**

Run: `cargo test -q --test runtime_contract_runtime_core --test runtime_contract_commit_semantics --test runtime_contract_batch_commit`
Expected: FAIL with unresolved modules or missing moved items.

- [ ] **Step 3: Move runtime and state responsibilities into smaller modules while preserving exact logic**

```rust
// src/runtime/mod.rs
mod command_ledger;
mod commit_engine;
mod commit_log;
mod handle;
mod reader;

pub use handle::{Runtime, RuntimeHandle};
pub use reader::{RuntimeReader, SnapshotReadGuard};
pub use commit_log::CommitLog;

// src/state/mod.rs
mod changes;
mod path;
mod read;
mod store;

pub use changes::{ChangeHit, ChangeSet, CommitResult, CommitScope, UpdateCursor};
pub use path::{ObjectKey, PathSegment, SeriesKey, StatePath};
pub use read::StateReadView;
pub(crate) use store::StateStore;
```

- [ ] **Step 4: Run the targeted runtime/state tests to verify behavior stays green**

Run: `cargo test -q --test runtime_contract_runtime_core --test runtime_contract_commit_semantics --test runtime_contract_batch_commit`
Expected: PASS

- [ ] **Step 5: Commit the module split**

```bash
git add src/lib.rs src/runtime src/state tests/runtime_contract_runtime_core.rs tests/runtime_contract_commit_semantics.rs tests/runtime_contract_batch_commit.rs
git commit -m "refactor: split runtime and state internals into modules"
```

### Task 3: Migrate Session Runtime And Contract Tests To Reader APIs

**Files:**
- Modify: `src/session_runtime.rs`
- Modify: `tests/runtime_contract_session_state.rs`
- Modify: `tests/runtime_contract_session_cycle.rs`
- Modify: `tests/runtime_contract_session_runtime.rs`
- Modify: `tests/runtime_contract_session_reconnect.rs`
- Modify: `tests/runtime_contract_v1_capability.rs`
- Modify: `tests/runtime_contract_live_smoke.rs`
- Test: `tests/runtime_contract_session_runtime.rs`
- Test: `tests/runtime_contract_v1_capability.rs`
- Test: `tests/runtime_contract_live_smoke.rs`

- [ ] **Step 1: Write the failing migration in one high-value test by replacing `latest_snapshot()` with `reader().read()`**

```rust
let reader = handle.reader();
let snapshot = reader.read();
assert_eq!(
    snapshot.get(["runtime", "commands", command_segment.as_str(), "status"]),
    Some(&serde_json::json!("completed"))
);
```

- [ ] **Step 2: Run the focused tests to verify failures are due to old snapshot access patterns**

Run: `cargo test -q --test runtime_contract_v1_capability --test runtime_contract_session_runtime`
Expected: FAIL with stale `latest_snapshot` usage or incompatible snapshot references.

- [ ] **Step 3: Update session-runtime internals and tests to use the reader API consistently**

```rust
let reader = self.handle.reader();
let snapshot = reader.read();
let status = snapshot.get(["query", query_id, "has_more"]).cloned();
```

```rust
let reader = handle.reader();
let snapshot = reader.read();
assert!(snapshot.get(["system", "session", "topology", "routes"]).is_some());
```

- [ ] **Step 4: Run the session and capability suite to verify migrated behavior**

Run: `cargo test -q --test runtime_contract_session_runtime --test runtime_contract_session_cycle --test runtime_contract_session_reconnect --test runtime_contract_v1_capability`
Expected: PASS

- [ ] **Step 5: Run the live smoke test after the read-path migration**

Run: `cargo test --test runtime_contract_live_smoke -- --ignored --nocapture`
Expected: PASS with both ignored live smoke tests executing successfully.

- [ ] **Step 6: Commit the migration off `latest_snapshot()`**

```bash
git add src/session_runtime.rs tests/runtime_contract_session_state.rs tests/runtime_contract_session_cycle.rs tests/runtime_contract_session_runtime.rs tests/runtime_contract_session_reconnect.rs tests/runtime_contract_v1_capability.rs tests/runtime_contract_live_smoke.rs
git commit -m "refactor: migrate runtime consumers to reader-based reads"
```

### Task 4: Remove Legacy Snapshot Surface And Update Documentation

**Files:**
- Modify: `src/lib.rs`
- Modify: `tests/runtime_contract_surface.rs`
- Modify: `tests/runtime_contract_bootstrap.rs`
- Modify: `tests/runtime_contract_runtime_core.rs`
- Modify: `docs/architecture/README.md`
- Modify: `docs/architecture/validation.md`
- Modify: `docs/architecture/runtime-core/overview.md`
- Modify: `docs/architecture/runtime-core/modules.md`
- Test: `tests/runtime_contract_surface.rs`
- Test: `tests/runtime_contract_bootstrap.rs`

- [ ] **Step 1: Write the failing public-surface assertions for the final exported API**

```rust
use tqsdk_runtime_contract::{Runtime, RuntimeHandle, RuntimeReader, SnapshotReadGuard};

#[test]
fn public_surface_exports_reader_api() {
    let handle = RuntimeHandle::new();
    let reader: RuntimeReader = handle.reader();
    let snapshot = reader.read();
    assert_eq!(snapshot.revision().get(), 0);
}
```

- [ ] **Step 2: Run the public-surface tests to verify final surface cleanup is still needed**

Run: `cargo test -q --test runtime_contract_surface --test runtime_contract_bootstrap`
Expected: FAIL if legacy `latest_snapshot` / `StateSnapshot` exports still shape the tests or docs.

- [ ] **Step 3: Remove or demote the legacy snapshot-oriented API and update docs to reader terminology**

```rust
pub use runtime::{CommitLog, Runtime, RuntimeHandle, RuntimeReader, SnapshotReadGuard};
pub use state::{
    ChangeHit, ChangeSet, CommitResult, CommitScope, ObjectKey, PathSegment, SeriesKey, StatePath,
    UpdateCursor,
};
```

```md
- V1 reads state through `RuntimeReader::read()`
- V2 adapters consume commits through `RuntimeReader::next()` and `UpdateCursor`
- the core crate no longer promises owned full-snapshot reads on every poll
```

- [ ] **Step 4: Run the full verification suite**

Run: `cargo test`
Expected: PASS

Run: `cargo clippy --all-targets --all-features`
Expected: PASS

Run: `cargo test --test runtime_contract_live_smoke -- --ignored --nocapture`
Expected: PASS

- [ ] **Step 5: Commit the final public-surface cleanup and docs**

```bash
git add src/lib.rs tests/runtime_contract_surface.rs tests/runtime_contract_bootstrap.rs tests/runtime_contract_runtime_core.rs docs/architecture/README.md docs/architecture/validation.md docs/architecture/runtime-core/overview.md docs/architecture/runtime-core/modules.md
git commit -m "refactor: finalize reader-centric core crate surface"
```

## Self-Review

### Spec coverage

- Reader-based zero-copy public reads: covered by Task 1 and Task 3
- Runtime/state module decomposition: covered by Task 2
- Publishable standalone core crate shape: covered by Task 4
- Preservation of command/commit/cursor semantics: covered by Task 2 through Task 4 regression suites
- No V2 facade leakage: covered by Task 4 public-surface cleanup

No gaps found relative to the design spec.

### Placeholder scan

- Removed vague “cleanup later” language
- Every task has explicit files, commands, and expected outcomes
- No `TODO`/`TBD` placeholders remain

### Type consistency

- `RuntimeReader` is the stable read-side type in all tasks
- `SnapshotReadGuard` is the borrowed read handle in all tasks
- `UpdateCursor` remains the commit-consumption primitive throughout
- `StateSnapshot` is treated as a migrated-away legacy surface consistently
