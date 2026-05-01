# Data Stream Task API Narrowing Triage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decide whether Task 7 can safely narrow `tqsdk-data`, `tqsdk-stream`, and `tqsdk-task` public re-exports in this batch.

**Architecture:** This is a public API redesign triage, not a mechanical re-export cleanup. Symbols marked `needs-arch-change` or `split-plan` must not be removed while scenario examples, README files, or architecture docs still present them as user-facing contracts. If preconditions are not met, record the deferral and do not change code.

**Tech Stack:** Rust workspace examples, ripgrep evidence scans, existing scenario and architecture docs.

---

## Files

- Read: `docs/reviews/public-api-disposition-matrix.md`
- Read: `docs/reviews/public-api-scenario-review.md`
- Read: `docs/architecture/api-data.md`
- Read: `docs/architecture/api-stream.md`
- Read: `docs/architecture/api-task.md`
- Read: `crates/tqsdk-data/README.md`
- Read: `crates/tqsdk-stream/README.md`
- Read: `crates/tqsdk-task/README.md`
- Read: affected `crates/*/examples/api_contract_sXX_*.rs`
- Modify only if a safe narrowing actually proceeds: crate `src/lib.rs`, docs, README, and examples for that crate.

## Task 1: Collect Current Example And Documentation Evidence

- [x] **Step 1: Scan `tqsdk-data` S18 cache public surface references**

Run:

```bash
rg -n "MarketCache(ReaderCheckpoint|ReaderLag|ReaderManifest|Recovery|WriterElection|Queue|Lock|Index|Compaction|ServiceOpen|DaemonShutdown|SupervisorShutdown|AtomicCompaction|CompactionOwnership)" crates/tqsdk-data docs/architecture/api-data.md docs/reviews/public-api-scenario-review.md
```

Expected:

```text
If S18 examples, README, or architecture docs still import or document these names directly, do not narrow `tqsdk-data` in this batch.
```

- [x] **Step 2: Scan `tqsdk-stream` S21 WAL and journal references**

Run:

```bash
rg -n "StreamSinkWal|StreamCommitJournal" crates/tqsdk-stream docs/architecture/api-stream.md docs/reviews/public-api-scenario-review.md
```

Expected:

```text
If S21 examples, README, or architecture docs still import or document these names directly, do not narrow `tqsdk-stream` in this batch.
```

- [x] **Step 3: Scan `tqsdk-task` report/status references**

Run:

```bash
rg -n "StrategySupervisorHealthStatus|StrategyRunStopReason|StrategyShutdownSignal|StrategyTelemetryEventKind|MultiAccountOrderStatus|ExecutionGroupStatus|ExecutionLegState" crates/tqsdk-task docs/architecture/api-task.md docs/reviews/public-api-scenario-review.md docs/scenarios
```

Expected:

```text
If task examples, tests, architecture docs, or scenario gap docs still import or document these names directly, do not narrow `tqsdk-task` in this batch.
```

## Task 2: Decide `tqsdk-data` Scope

- [x] **Step 1: Compare scan evidence with the disposition matrix**

Decision rule:

```text
Proceed only if the candidate is not `needs-arch-change`, and no scenario example or README still imports it directly.
```

- [x] **Step 2: Record the data decision**

Expected decision for the current repository state:

```text
Do not narrow `tqsdk-data` in Task 7. S18 examples and `api-data.md` still directly present reader manifest, recovery, writer election, queue/lock/index, compaction, service, daemon, and supervisor types as public scenario contracts.
```

## Task 3: Decide `tqsdk-stream` Scope

- [x] **Step 1: Compare scan evidence with the disposition matrix**

Decision rule:

```text
Proceed only after S21 docs/examples stop directly depending on WAL record, WAL maintenance, and commit journal types.
```

- [x] **Step 2: Record the stream decision**

Expected decision for the current repository state:

```text
Do not narrow `tqsdk-stream` in Task 7. `api_contract_s21_slow_consumer_isolation.rs`, `api-stream.md`, `crates/tqsdk-stream/README.md`, and stream tests still directly use `StreamSinkWal*` and `StreamCommitJournal*`.
```

## Task 4: Decide `tqsdk-task` Scope

- [x] **Step 1: Compare scan evidence with the disposition matrix**

Decision rule:

```text
Proceed only after S12/S13/S15/S20 examples and architecture docs no longer require independent status, signal, reason, kind, or state imports.
```

- [x] **Step 2: Record the task decision**

Expected decision for the current repository state:

```text
Do not narrow `tqsdk-task` in Task 7. S15/S20 docs/examples still use `StrategyShutdownSignal`, S20 uses telemetry and supervisor health concepts, and S12/S13 architecture docs expose `ExecutionGroupStatus` and `MultiAccountOrderStatus` as status return types.
```

## Task 5: Verify No Accidental Code Changes

- [x] **Step 1: Check worktree status**

Run:

```bash
git status --short
```

Expected:

```text
No crate code changes for Task 7 triage.
```

- [x] **Step 2: Verify examples still compile**

Run:

```bash
cargo check --workspace --examples
```

Expected:

```text
Workspace examples compile with the current public API surface.
```

Output:

- `git status --short` in `.worktrees/audit-guardrails` showed no Task 7 code changes.
- Verification commands completed:
  - `cargo check --workspace --examples`
  - `cargo test --workspace`
  - `cargo clippy --workspace --examples --all-targets -- -D warnings`

## Task 6: Record Parent Task 7 Outcome

- [x] **Step 1: Update `docs/superpowers/plans/2026-04-29-review-remediation-plan.md`**

Record:

```text
Task 7 was triaged and not implemented as code in this batch because all non-keep data/stream/task candidates still require scenario/doc redesign or a split compatibility plan.
```

- [x] **Step 2: Do not create a code commit for Task 7**

Expected:

```text
No code commit is created for Task 7 because no public API narrowing was safe under the current scenario contracts.
```

Output:

- Task 7 outcome is triage-only: no crate code, docs, examples, or re-export changes were made.
- Reason: every non-keep candidate in `tqsdk-data`, `tqsdk-stream`, and `tqsdk-task` still has active scenario/doc/example dependencies or is marked `split-plan`.
