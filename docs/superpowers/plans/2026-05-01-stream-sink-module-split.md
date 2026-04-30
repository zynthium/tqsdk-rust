# Stream Sink Module Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split `crates/tqsdk-stream/src/sink.rs` into focused internal modules without changing the public `tqsdk-stream` sink/WAL/journal API.

**Architecture:** This is a source-compatible internal refactor. `crate::sink::*` remains the public module surface consumed by `crates/tqsdk-stream/src/lib.rs`, `api.rs`, `shutdown.rs`, examples, and tests. The split keeps managed sink runtime, shared sink state, WAL, commit journal, and JSONL writer responsibilities separate while preserving the same commit stream, retry, WAL, journal, and graceful shutdown semantics.

**Tech Stack:** Rust modules, Tokio tasks, `serde_json` JSONL IO, existing `stream_commit_flow` characterization tests, `cargo check/test/clippy`.

---

## Scope

In scope:

- Keep `crates/tqsdk-stream/src/sink.rs` as the module root.
- Create child modules under `crates/tqsdk-stream/src/sink/`.
- Move existing definitions without changing type names, method names, return types, or public re-exports.
- Add a source-level guardrail test to keep the split from regressing to one large file.
- Update review/plan documents after verification.

Out of scope:

- Changing sink retry semantics.
- Changing WAL or commit journal JSONL format.
- Internalizing any `StreamSink*` or `StreamCommitJournal*` public type.
- Replacing `std::fs` sync JSONL maintenance APIs with async IO.
- Changing `CommitSink` or `StreamSinkFuture` signatures.

## File Structure

- Modify: `crates/tqsdk-stream/src/sink.rs`
  - Root module only.
  - Declares child modules and re-exports the same public types currently exported from `crate::sink`.
- Create: `crates/tqsdk-stream/src/sink/options.rs`
  - `StreamSinkOptions`
  - `StreamSinkProfile`
  - `StreamSinkRetryPolicy`
- Create: `crates/tqsdk-stream/src/sink/state.rs`
  - `StreamSinkStatus`
  - `StreamSinkStats`
  - `StreamSinkShutdownReport`
  - private `StreamSinkState`
  - `pub(super) SharedStreamSinkState`
  - state mutation helpers currently named `current_status`, `set_status`, `increment_processed`, `add_lagged`, `increment_retry_attempts`, `increment_wal_records`, `increment_journal_records`, `record_error`, `clear_error`, and `report`
- Create: `crates/tqsdk-stream/src/sink/writer.rs`
  - private `JsonlRecordWriter`
  - `pub(super) StreamSinkWalWriter`
  - `pub(super) StreamCommitJournalWriter`
- Create: `crates/tqsdk-stream/src/sink/wal.rs`
  - `StreamSinkWalFsyncPolicy`
  - `StreamSinkWalRecordKind`
  - `StreamSinkWalRecord`
  - `StreamSinkWalCompaction`
  - `StreamSinkWalCompactionReport`
  - `StreamSinkWalRecovery`
  - `StreamSinkWalRecoveryReport`
  - private WAL compaction/recovery helpers
- Create: `crates/tqsdk-stream/src/sink/journal.rs`
  - `StreamCommitJournal`
  - `StreamCommitJournalRecord`
  - `StreamCommitJournalScope`
  - `StreamCommitJournalDomain`
  - `StreamCommitJournalReplayReport`
  - private journal read/replay helpers
- Create: `crates/tqsdk-stream/src/sink/runtime.rs`
  - `StreamSinkFuture`
  - `CommitSink`
  - `StreamSinkHandle`
  - private `StreamSinkRuntime`
  - `run_sink`, `deliver_commit`, and `flush_sink`
- Modify: `crates/tqsdk-stream/tests/stream_events.rs`
  - Add source-level module split guardrail.
- Modify: `docs/reviews/comprehensive-review-2026-04-30.md`
  - Mark the full `sink.rs` module-directory split complete after verification.
- Modify: `docs/superpowers/plans/2026-04-29-review-remediation-plan.md`
  - Remove `sink.rs` from the remaining module split list after verification.
- Modify: `docs/superpowers/plans/2026-05-01-stream-sink-module-split.md`
  - Check off executed steps and record verification.

## Task 1: Add Sink Split Guardrail Test

**Files:**
- Modify: `crates/tqsdk-stream/tests/stream_events.rs`

- [x] **Step 1: Write the failing structure test**

Add this test near the existing sink source-scanning tests:

```rust
#[test]
fn stream_sink_is_split_into_focused_modules() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let sink_root = root.join("src/sink.rs");
    let sink_dir = root.join("src/sink");

    for module in [
        "options.rs",
        "state.rs",
        "writer.rs",
        "wal.rs",
        "journal.rs",
        "runtime.rs",
    ] {
        assert!(
            sink_dir.join(module).exists(),
            "sink module {module} should exist under src/sink/"
        );
    }

    let source = std::fs::read_to_string(&sink_root).expect("sink root should be readable");
    for module_decl in [
        "mod options;",
        "mod state;",
        "mod writer;",
        "mod wal;",
        "mod journal;",
        "mod runtime;",
    ] {
        assert!(
            source.contains(module_decl),
            "sink root should declare {module_decl}"
        );
    }

    assert!(
        !source.contains("async fn run_sink"),
        "sink runtime loop should live in src/sink/runtime.rs"
    );
    assert!(
        !source.contains("fn compact_jsonl_wal"),
        "WAL compaction should live in src/sink/wal.rs"
    );
    assert!(
        !source.contains("fn read_jsonl_commit_journal"),
        "commit journal IO should live in src/sink/journal.rs"
    );
}
```

- [x] **Step 2: Run the structure test and verify RED**

Run:

```bash
cargo test -p tqsdk-stream --test stream_events stream_sink_is_split_into_focused_modules
```

Expected before implementation:

```text
FAILED stream_sink_is_split_into_focused_modules
```

The failure should report at least one missing module under `src/sink/`.

Observed RED: failed because `src/sink/options.rs` did not exist.

## Task 2: Create Sink Module Root and Public Re-exports

**Files:**
- Modify: `crates/tqsdk-stream/src/sink.rs`
- Create: `crates/tqsdk-stream/src/sink/options.rs`
- Create: `crates/tqsdk-stream/src/sink/state.rs`
- Create: `crates/tqsdk-stream/src/sink/writer.rs`
- Create: `crates/tqsdk-stream/src/sink/wal.rs`
- Create: `crates/tqsdk-stream/src/sink/journal.rs`
- Create: `crates/tqsdk-stream/src/sink/runtime.rs`

- [x] **Step 1: Replace root file with module declarations and re-exports**

After moving the definitions in Tasks 3-6, `crates/tqsdk-stream/src/sink.rs` should contain only:

```rust
mod journal;
mod options;
mod runtime;
mod state;
mod wal;
mod writer;

pub use journal::{
    StreamCommitJournal, StreamCommitJournalDomain, StreamCommitJournalRecord,
    StreamCommitJournalReplayReport, StreamCommitJournalScope,
};
pub use options::{StreamSinkOptions, StreamSinkProfile, StreamSinkRetryPolicy};
pub use runtime::{CommitSink, StreamSinkFuture, StreamSinkHandle};
pub use state::{StreamSinkShutdownReport, StreamSinkStats, StreamSinkStatus};
pub use wal::{
    StreamSinkWalCompaction, StreamSinkWalCompactionReport, StreamSinkWalFsyncPolicy,
    StreamSinkWalRecord, StreamSinkWalRecordKind, StreamSinkWalRecovery,
    StreamSinkWalRecoveryReport,
};
```

- [x] **Step 2: Keep crate-level public exports unchanged**

Do not edit the `pub use sink::{ ... }` block in `crates/tqsdk-stream/src/lib.rs` except if rustfmt reflows it.

Run:

```bash
cargo check -p tqsdk-stream
```

Expected after Tasks 3-6 complete:

```text
Finished `dev` profile ...
```

## Task 3: Move Sink Options and State

**Files:**
- Create: `crates/tqsdk-stream/src/sink/options.rs`
- Create: `crates/tqsdk-stream/src/sink/state.rs`
- Modify: `crates/tqsdk-stream/src/sink.rs`

- [x] **Step 1: Move options definitions unchanged**

Move these existing definitions from `sink.rs` into `sink/options.rs`:

- `StreamSinkOptions`
- `StreamSinkProfile`
- `StreamSinkRetryPolicy`
- `impl Default for StreamSinkOptions`
- `impl StreamSinkOptions`
- `impl StreamSinkProfile`
- `impl StreamSinkRetryPolicy`

Required imports in `options.rs`:

```rust
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::Result;

use super::wal::StreamSinkWalFsyncPolicy;
```

- [x] **Step 2: Move state definitions unchanged**

Move these existing definitions from `sink.rs` into `sink/state.rs`:

- `StreamSinkStatus`
- `StreamSinkStats`
- `StreamSinkShutdownReport`
- private `StreamSinkState`
- `pub(super) SharedStreamSinkState`
- `impl SharedStreamSinkState`
- `impl StreamSinkStats`
- `impl StreamSinkShutdownReport`
- helpers `current_status`, `set_status`, `increment_processed`, `add_lagged`, `increment_retry_attempts`, `increment_wal_records`, `increment_journal_records`, `record_error`, `clear_error`, and `report`

Required imports in `state.rs`:

```rust
use std::sync::{Arc, Mutex};

use crate::StreamFacadeError;
```

Mark `SharedStreamSinkState` and helper functions `pub(super)` so `runtime.rs` can use them while keeping them internal to `crate::sink`.

## Task 4: Move WAL and Commit Journal IO

**Files:**
- Create: `crates/tqsdk-stream/src/sink/writer.rs`
- Create: `crates/tqsdk-stream/src/sink/wal.rs`
- Create: `crates/tqsdk-stream/src/sink/journal.rs`
- Modify: `crates/tqsdk-stream/src/sink.rs`

- [x] **Step 1: Move shared JSONL writer unchanged**

Move these existing definitions into `sink/writer.rs`:

- `JsonlRecordWriter`
- `StreamSinkWalWriter`
- `StreamCommitJournalWriter`
- `impl JsonlRecordWriter`
- `impl StreamSinkWalWriter`
- `impl StreamCommitJournalWriter`

Required imports in `writer.rs`:

```rust
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;

use serde::Serialize;
use tqsdk_core::CommitResult;

use crate::Result;

use super::journal::StreamCommitJournalRecord;
use super::wal::{StreamSinkWalFsyncPolicy, StreamSinkWalRecord};
```

Make `StreamSinkWalWriter` and `StreamCommitJournalWriter` `pub(super)`.

- [x] **Step 2: Move WAL types and helpers unchanged**

Move these existing definitions into `sink/wal.rs`:

- `StreamSinkWalFsyncPolicy`
- `StreamSinkWalRecordKind`
- `StreamSinkWalRecord`
- `StreamSinkWalCompaction`
- `StreamSinkWalCompactionReport`
- `StreamSinkWalRecovery`
- `StreamSinkWalRecoveryReport`
- `impl Default for StreamSinkWalCompaction`
- `impl StreamSinkWalCompaction`
- `impl StreamSinkWalCompactionReport`
- `impl StreamSinkWalRecovery`
- `impl StreamSinkWalRecoveryReport`
- `impl StreamSinkWalRecord`
- `compact_jsonl_wal`
- `scan_jsonl_wal`
- `compaction_temp_path`
- `commit_scope`

Required imports in `wal.rs`:

```rust
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tqsdk_core::{CommitResult, Revision, SharedCommitResult};

use crate::{Result, StreamFacadeError};
```

- [x] **Step 3: Move commit journal types and helpers unchanged**

Move these existing definitions into `sink/journal.rs`:

- `StreamCommitJournal`
- `StreamCommitJournalRecord`
- `StreamCommitJournalScope`
- `StreamCommitJournalDomain`
- `StreamCommitJournalReplayReport`
- `impl StreamCommitJournal`
- `impl StreamCommitJournalRecord`
- `impl StreamCommitJournalScope`
- `impl StreamCommitJournalDomain`
- `impl StreamCommitJournalReplayReport`
- `read_jsonl_commit_journal`
- `replay_jsonl_commit_journal`
- `should_replay_journal_revision`

Required imports in `journal.rs`:

```rust
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde::{Deserialize, Serialize};
use tqsdk_core::{ChangeSet, CommandId, CommitResult, CommitScope, ProtocolDomain, Revision, StatePath};

use crate::Result;

use super::runtime::CommitSink;
```

Use `record.to_commit().into()` when replaying to preserve the `SharedCommitResult` sink contract.

## Task 5: Move Managed Sink Runtime

**Files:**
- Create: `crates/tqsdk-stream/src/sink/runtime.rs`
- Modify: `crates/tqsdk-stream/src/sink.rs`

- [x] **Step 1: Move sink runtime definitions unchanged**

Move these existing definitions into `sink/runtime.rs`:

- `StreamSinkFuture`
- `CommitSink`
- `StreamSinkHandle`
- private `StreamSinkRuntime<S>`
- blanket `impl<F> CommitSink for F`
- `impl StreamSinkHandle`
- `run_sink`
- `deliver_commit`
- `flush_sink`
- `write_wal_record`
- `write_commit_journal_record`

Required imports in `runtime.rs`:

```rust
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::StreamExt;
use tqsdk_core::SharedCommitResult;

use crate::{CommitStream, Result, StreamFacadeError};

use super::journal::StreamCommitJournalWriter;
use super::options::{StreamSinkOptions, StreamSinkRetryPolicy};
use super::state::{
    SharedStreamSinkState, StreamSinkShutdownReport, StreamSinkStatus, add_lagged, clear_error,
    current_status, increment_journal_records, increment_processed, increment_retry_attempts,
    increment_wal_records, record_error, report, set_status,
};
use super::wal::{StreamSinkWalRecord, StreamSinkWalRecordKind};
use super::writer::StreamSinkWalWriter;
```

- [x] **Step 2: Verify stream crate compile**

Run:

```bash
cargo check -p tqsdk-stream
```

Expected:

```text
Finished `dev` profile ...
```

If imports fail, fix visibility by using `pub(super)` only inside `crate::sink`. Do not make helper functions `pub` at crate root.

Observed compile check:

- `cargo check -p tqsdk-stream`

## Task 6: Run Characterization and Workspace Verification

**Files:**
- Modify: `docs/reviews/comprehensive-review-2026-04-30.md`
- Modify: `docs/superpowers/plans/2026-04-29-review-remediation-plan.md`
- Modify: `docs/superpowers/plans/2026-05-01-stream-sink-module-split.md`

- [x] **Step 1: Run focused sink tests**

Run:

```bash
cargo test -p tqsdk-stream --test stream_events stream_sink_is_split_into_focused_modules
cargo test -p tqsdk-stream --test stream_commit_flow
```

Expected:

```text
test result: ok
```

Observed:

- `cargo test -p tqsdk-stream --test stream_events stream_sink_is_split_into_focused_modules`
- `cargo test -p tqsdk-stream --test stream_commit_flow`

- [x] **Step 2: Run final verification**

Run:

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace --tests
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Expected: all commands exit 0.

Observed final verification:

- `cargo fmt --all --check`
- `cargo check --workspace`
- `cargo check --workspace --examples`
- `cargo test --workspace --tests`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `git diff --check`

- [x] **Step 3: Update review state**

In `docs/reviews/comprehensive-review-2026-04-30.md`:

- Add a bullet under `2026-05-01 后续批次已落地`:
  - `` `tqsdk-stream/src/sink.rs` 已拆为 `sink/` 模块目录，public `CommitSink` / WAL / commit journal surface 保持不变。``
- Remove `sink.rs` from the remaining independent plan item:
  - Change `` `transport.rs`、`account_group.rs`、`sink.rs` 模块级拆分`` to `` `transport.rs`、`account_group.rs` 模块级拆分``.
- Mark the maintainability table row for full `sink.rs` split as completed in the finding text.

In `docs/superpowers/plans/2026-04-29-review-remediation-plan.md`:

- Add a completed item mentioning this plan.
- Change the remaining item to only mention `transport.rs` and `account_group.rs`.

- [x] **Step 4: Commit**

Run:

```bash
git add crates/tqsdk-stream/src/sink.rs crates/tqsdk-stream/src/sink crates/tqsdk-stream/tests/stream_events.rs docs/reviews/comprehensive-review-2026-04-30.md docs/superpowers/plans/2026-04-29-review-remediation-plan.md docs/superpowers/plans/2026-05-01-stream-sink-module-split.md
git commit -m "refactor: split stream sink modules"
```

## Self-Review

- Public crate exports remain unchanged because `crates/tqsdk-stream/src/lib.rs` still re-exports the same `crate::sink::*` names.
- Behavior remains covered by `stream_commit_flow`, which exercises commit fan-out, managed sink runtime, retry, WAL, journal replay, compaction, recovery, and graceful shutdown.
- The new source-level structure test prevents the single-file sink implementation from silently returning.
- This plan does not resolve the separate `transport.rs` or `account_group.rs` module split items.
