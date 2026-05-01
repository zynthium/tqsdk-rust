# Shared CommitResult Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the deep `CommitResult` clone on runtime commit publication by making runtime commit consumption share `Arc<CommitResult>`.

**Architecture:** This is a runtime contract source-breaking change. `CommitResult` remains the public metadata struct with public fields, while `SharedCommitResult = Arc<CommitResult>` becomes the return/storage type for `RuntimeHandle`, `CommitLog`, `RuntimeReader`, aggregation, wait, and stream commit delivery. Facades must keep consuming the same commit log/state tree and must not introduce local commit copies or alternate state channels.

**Tech Stack:** Rust, `Arc`, Cargo workspace tests, docs under `docs/architecture/`, `docs/reviews/`, and this superpowers plan.

---

## Scope

In scope:

- Add `tqsdk_core::SharedCommitResult`.
- Change runtime commit-producing and commit-consuming APIs to use `SharedCommitResult`.
- Update wait/stream/task/data/session workspace callers.
- Keep `CommitResult` struct fields and `CommitResult::new(...)` intact for manual construction and WAL recovery.
- Update docs/review records to state this source-breaking runtime contract migration is complete.

Out of scope:

- Changing state tree storage away from `serde_json::Value`.
- Changing `CommitResult` fields, `ChangeSet`, or causality semantics.
- Adding a second commit bus or facade-owned commit cache.

## Task 1: Add Runtime Contract Test for Shared Commit Identity

**Files:**
- Modify: `crates/tqsdk-core/tests/runtime_contract_runtime_core.rs`

- [x] **Step 1: Write the failing test**

Add this test near the existing commit log tests:

```rust
#[test]
fn runtime_publishes_and_returns_the_same_shared_commit() {
    use std::sync::Arc;

    let handle = runtime_with_default_adapters();
    let returned = handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market.shared".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "quotes": {
                            "SHFE.au2602": {
                                "last_price": 618.5
                            }
                        }
                    }]
                })),
            }),
            Vec::new(),
            CommitScope::RealtimeUpdate,
        )
        .expect("ingest should succeed")
        .expect("quote update should publish a commit");

    let mut cursor = handle.cursor_from(returned.revision);
    let logged = handle
        .commit_log()
        .next(&mut cursor)
        .expect("published commit should be retained in the log");

    assert!(Arc::ptr_eq(&returned, &logged));
}
```

- [x] **Step 2: Run the test and verify RED**

Run:

```bash
cargo test -p tqsdk-core --test runtime_contract_runtime_core runtime_publishes_and_returns_the_same_shared_commit
```

Expected before implementation: compile failure because `ingest(...)` and `CommitLog::next(...)` return owned `CommitResult`, not `Arc<CommitResult>`.

Observed RED: `Arc::ptr_eq` failed to compile because `ingest(...)` and
`CommitLog::next(...)` still returned owned `CommitResult`.

## Task 2: Migrate Core Runtime to Shared Commits

**Files:**
- Modify: `crates/tqsdk-core/src/state/changes.rs`
- Modify: `crates/tqsdk-core/src/state/mod.rs`
- Modify: `crates/tqsdk-core/src/lib.rs`
- Modify: `crates/tqsdk-core/src/runtime/commit_engine.rs`
- Modify: `crates/tqsdk-core/src/runtime/commit_log.rs`
- Modify: `crates/tqsdk-core/src/runtime/reader.rs`
- Modify: `crates/tqsdk-core/src/runtime/handle.rs`
- Modify: `crates/tqsdk-core/src/aggregation.rs`

- [x] **Step 1: Add the shared type alias**

In `crates/tqsdk-core/src/state/changes.rs`, after `CommitResult`:

```rust
pub type SharedCommitResult = Arc<CommitResult>;
```

Export it from `crates/tqsdk-core/src/state/mod.rs`:

```rust
pub use changes::{ChangeHit, ChangeSet, CommitResult, CommitScope, SharedCommitResult, UpdateCursor};
```

Export it from `crates/tqsdk-core/src/lib.rs` root re-exports next to `CommitResult`.

- [x] **Step 2: Change commit assembly to return shared commits**

Change `CommitEngine::apply(...)` to return `Option<SharedCommitResult>` and to call `on_commit(Arc::clone(&commit))`:

```rust
pub(crate) fn apply(
    snapshot: &StateStore,
    mutations: Vec<NormalizedMutation>,
    domains: Vec<ProtocolDomain>,
    caused_by: Vec<CommandId>,
    scope: CommitScope,
    on_commit: impl FnOnce(SharedCommitResult),
) -> Option<SharedCommitResult> {
    if mutations.is_empty() {
        return None;
    }

    let next_revision = Revision::new(snapshot.revision().get() + 1);
    snapshot.apply_with(next_revision, &mutations, |applied| {
        let changes = ChangeSet::from_mutations(&applied);
        let commit = Arc::new(CommitResult::new(next_revision, domains, changes, caused_by, scope));
        on_commit(Arc::clone(&commit));
        commit
    })
}
```

- [x] **Step 3: Change commit log storage and cursor output**

In `crates/tqsdk-core/src/runtime/commit_log.rs`:

```rust
pub fn next(&self, cursor: &mut UpdateCursor) -> Option<SharedCommitResult> {
    let state = recover_poisoned_lock(self.inner.read());
    let commit = state.commit_at(cursor.next_revision())?.clone();
    drop(state);

    cursor.set_next_revision(Revision::new(commit.revision.get() + 1));
    Some(commit)
}

pub(crate) fn commit_at(&self, revision: Revision) -> Option<SharedCommitResult> {
    recover_poisoned_lock(self.inner.read())
        .commit_at(revision)
        .cloned()
}

pub(crate) fn publish(&self, commit: SharedCommitResult) {
    let mut state = recover_poisoned_lock(self.inner.write());
    state.head = Some(commit.revision);
    if state.entries.is_empty() {
        state.first_retained_revision = Some(commit.revision);
    }
    state.entries.push_back(commit);
    state.trim();
    drop(state);
    self.notified.notify_waiters();
}
```

Change `CommitLogInner.entries` to `VecDeque<SharedCommitResult>`.

- [x] **Step 4: Change reader/handle/aggregation public surfaces**

Update signatures:

```rust
pub fn RuntimeReader::next(&self, cursor: &mut UpdateCursor) -> Option<SharedCommitResult>
pub fn RuntimeHandle::ingest(...) -> Result<Option<SharedCommitResult>>
pub fn RuntimeHandle::ingest_batch(...) -> Result<Option<SharedCommitResult>>
pub fn RuntimeHandle::record_command_status(...) -> Result<Option<SharedCommitResult>>
```

Change `CommitReadGuard` to store `SharedCommitResult` while keeping:

```rust
pub fn commit(&self) -> &CommitResult {
    &self.commit
}
```

Change `AggregatedCommit.commit` to `SharedCommitResult`.

- [x] **Step 5: Run core tests and verify GREEN**

Run:

```bash
cargo test -p tqsdk-core --test runtime_contract_runtime_core runtime_publishes_and_returns_the_same_shared_commit
cargo test -p tqsdk-core --tests
```

Expected: the new identity test passes, and existing core tests compile with `SharedCommitResult`.

Observed GREEN:

- `cargo test -p tqsdk-core --test runtime_contract_runtime_core runtime_publishes_and_returns_the_same_shared_commit`
- `cargo test -p tqsdk-core --tests`

## Task 3: Migrate Workspace Consumers

**Files:**
- Modify: `crates/tqsdk-wait/src/driver.rs`
- Modify: `crates/tqsdk-wait/src/api.rs`
- Modify: `crates/tqsdk-stream/src/api.rs`
- Modify: `crates/tqsdk-stream/src/driver.rs`
- Modify: `crates/tqsdk-stream/src/filter.rs`
- Modify: `crates/tqsdk-stream/src/typed.rs`
- Modify: `crates/tqsdk-stream/src/event.rs`
- Modify: `crates/tqsdk-stream/src/market_event.rs`
- Modify: `crates/tqsdk-stream/src/quote_subscription.rs`
- Modify: `crates/tqsdk-stream/src/sink.rs`
- Modify: affected examples/tests under `crates/tqsdk-*`

- [x] **Step 1: Update wait facade storage**

Change wait driver state:

```rust
pub(crate) deferred_commits: VecDeque<tqsdk_core::SharedCommitResult>,
pub(crate) last_commit: Option<tqsdk_core::SharedCommitResult>,
```

Keep `TqApi::last_commit()` source-compatible by returning a borrowed `CommitResult`:

```rust
pub fn last_commit(&self) -> Option<&tqsdk_core::CommitResult> {
    self.driver.last_commit.as_deref()
}
```

In test fixture push helpers, convert owned `CommitResult` into shared with `Arc::new(commit)`.

- [x] **Step 2: Update stream commit delivery**

Change stream public item type:

```rust
impl Stream for CommitStream {
    type Item = crate::error::Result<tqsdk_core::SharedCommitResult>;
}
```

Change `DriverEvent::Commit` to carry `SharedCommitResult`.

Change filtered streams and typed value updates to carry `SharedCommitResult`; helper functions should continue accepting `&CommitResult` and rely on deref coercion from `&SharedCommitResult`.

- [x] **Step 3: Update managed sink API to avoid retry deep clones**

Change sink trait and function wrapper:

```rust
pub trait CommitSink: Send + 'static {
    fn handle_commit(&mut self, commit: tqsdk_core::SharedCommitResult) -> StreamSinkFuture;
}

impl<F> CommitSink for F
where
    F: FnMut(tqsdk_core::SharedCommitResult) -> StreamSinkFuture + Send + 'static,
{
    fn handle_commit(&mut self, commit: tqsdk_core::SharedCommitResult) -> StreamSinkFuture {
        self(commit)
    }
}
```

Change `deliver_commit(...)` to take `SharedCommitResult`; retry attempts should call `sink.handle_commit(Arc::clone(&commit)).await`.

- [x] **Step 4: Update tests/examples that construct or accept commits**

Where tests manually construct `CommitResult`, keep construction as owned and wrap only where a stream/runtime API now expects shared:

```rust
let commit = Arc::new(CommitResult::new(...));
```

Where tests only inspect fields, no change is needed because `Arc<CommitResult>` supports field autoderef.

- [x] **Step 5: Run workspace verification**

Run:

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace --tests
```

Expected: all workspace crates compile and tests pass.

Observed intermediate workspace verification:

- `cargo check --workspace`
- `cargo test --workspace --tests`

## Task 4: Update Docs and Review State

**Files:**
- Modify: `docs/architecture/runtime-core/data-contracts.md`
- Modify: `docs/architecture/runtime-core/modules.md`
- Modify: `docs/architecture/validation.md`
- Modify: `docs/reviews/comprehensive-review-2026-04-30.md`
- Modify: `docs/superpowers/plans/2026-04-29-review-remediation-plan.md`
- Modify: `docs/superpowers/plans/2026-05-01-shared-commit-result.md`

- [x] **Step 1: Document shared commit ownership**

Update runtime-core docs to state:

```text
CommitResult is the immutable commit metadata payload; runtime publication and cursor consumption use SharedCommitResult = Arc<CommitResult> so the commit log, immediate producer return value, stream fan-out, and sink retries do not deep-clone ChangeSet/causality vectors.
```

- [x] **Step 2: Mark review item complete**

Update the comprehensive review and umbrella remediation plan:

```text
`apply_and_publish_locked` no longer deep-clones `CommitResult`; runtime commit publication now shares `Arc<CommitResult>` through `SharedCommitResult`.
```

- [x] **Step 3: Final verification and commit**

Run:

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace --tests
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Then commit:

```bash
git add crates/tqsdk-core crates/tqsdk-wait crates/tqsdk-stream crates/tqsdk-session crates/tqsdk-data crates/tqsdk-task docs/architecture/runtime-core/data-contracts.md docs/architecture/runtime-core/modules.md docs/architecture/validation.md docs/reviews/comprehensive-review-2026-04-30.md docs/superpowers/plans/2026-04-29-review-remediation-plan.md docs/superpowers/plans/2026-05-01-shared-commit-result.md
git commit -m "refactor: share runtime commit results"
```

Observed final verification:

- `cargo fmt --all --check`
- `cargo check --workspace`
- `cargo check --workspace --examples`
- `cargo test --workspace --tests`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `git diff --check`

## Self-Review

- The plan addresses the exact reviewed clone site by changing ownership, not by moving the clone elsewhere.
- `CommitResult` remains the public metadata struct, so manual construction and field access are preserved.
- The source-breaking part is explicit: APIs that return or stream commits now return `SharedCommitResult`.
- No facade-local commit bus or second state tree is introduced.
