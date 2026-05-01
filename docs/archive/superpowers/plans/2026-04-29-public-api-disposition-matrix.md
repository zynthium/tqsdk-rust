# Public API Disposition Matrix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a reviewed disposition matrix for every public API symbol disputed by the 2026-04-29 audit inputs, so later remediation can separate safe internal cleanup from architecture-sensitive breaking changes.

**Architecture:** This task is documentation and classification only. It must not edit Rust source, crate README files, or architecture docs except to cite them as evidence. The matrix treats `docs/architecture/*`, crate README files, and `crates/*/examples/api_contract_sXX_*.rs` as public-contract evidence; `docs/archive/reviews/2026-04-29/public-api-overdesign-audit.md` and `docs/archive/reviews/2026-04-29/review-2026-04-29-pending.md` are review inputs, not authority.

**Tech Stack:** Markdown, `rg`, Cargo examples check, existing architecture docs, crate `lib.rs` public re-exports, scenario examples.

---

## File Structure

- Create: `docs/reviews/public-api-disposition-matrix.md`
  - Final matrix and decision evidence.
- Modify: `docs/superpowers/plans/2026-04-29-review-remediation-plan.md`
  - Link the completed matrix and mark Task 1 as delegated to this child plan.
- Read-only evidence:
  - `docs/archive/reviews/2026-04-29/public-api-overdesign-audit.md`
  - `docs/archive/reviews/2026-04-29/review-2026-04-29-pending.md`
  - `docs/architecture/README.md`
  - `docs/architecture/api-layers.md`
  - `docs/architecture/api-data.md`
  - `docs/architecture/api-stream.md`
  - `docs/architecture/api-task.md`
  - `docs/architecture/validation.md`
  - `docs/reviews/public-api-scenario-review.md`
  - `crates/tqsdk-core/src/lib.rs`
  - `crates/tqsdk-data/src/lib.rs`
  - `crates/tqsdk-stream/src/lib.rs`
  - `crates/tqsdk-task/src/lib.rs`
  - `crates/*/README.md`
  - `crates/*/examples/api_contract_sXX_*.rs`

---

## Disposition Definitions

Use exactly these values in the matrix:

- `keep`: Keep public. It is part of a current architecture contract, documented user surface, or required public extension point.
- `deprecate`: Keep public for now, but mark as future removal or migration target. Requires follow-up deprecation design before code changes.
- `internalize`: Safe candidate for removal from public re-exports. Evidence must show it is not promised by architecture docs, crate README files, or scenario examples.
- `needs-arch-change`: Do not change code yet. The symbol is currently documented or example-backed, so later remediation must update architecture docs, README files, and examples first.
- `split-plan`: The audit concern is valid but too broad for a single symbol decision; create a separate plan before code changes.

---

## Task 1: Extract the Disputed Symbol Inventory

**Files:**
- Create: `docs/reviews/public-api-disposition-matrix.md`
- Read: `docs/archive/reviews/2026-04-29/public-api-overdesign-audit.md`
- Read: `docs/archive/reviews/2026-04-29/review-2026-04-29-pending.md`
- Read: `crates/tqsdk-core/src/lib.rs`
- Read: `crates/tqsdk-data/src/lib.rs`
- Read: `crates/tqsdk-stream/src/lib.rs`
- Read: `crates/tqsdk-task/src/lib.rs`

- [x] **Step 1: Create the matrix document header**

Create `docs/reviews/public-api-disposition-matrix.md` with this header:

```markdown
# Public API Disposition Matrix

> Source audits: `docs/archive/reviews/2026-04-29/public-api-overdesign-audit.md`, `docs/archive/reviews/2026-04-29/review-2026-04-29-pending.md`
> Date: 2026-04-29

This document classifies disputed public API symbols before any remediation code changes.
It is the gate for public API narrowing work.

## Disposition Values

| Value | Meaning |
| --- | --- |
| `keep` | Keep public; current architecture/docs/examples depend on it. |
| `deprecate` | Keep public now; design a migration/deprecation path first. |
| `internalize` | Candidate to remove from public re-exports. |
| `needs-arch-change` | Requires architecture/docs/examples update before code changes. |
| `split-plan` | Too broad for a symbol-level decision; create a narrower plan. |

## Summary

| Crate | `keep` | `deprecate` | `internalize` | `needs-arch-change` | `split-plan` |
| --- | ---: | ---: | ---: | ---: | ---: |
| `tqsdk-core` | 0 | 0 | 0 | 0 | 0 |
| `tqsdk-data` | 0 | 0 | 0 | 0 | 0 |
| `tqsdk-stream` | 0 | 0 | 0 | 0 | 0 |
| `tqsdk-task` | 0 | 0 | 0 | 0 | 0 |

## Matrix

| Crate | Symbol or Group | Current Export | Evidence | Disposition | Required Follow-up |
| --- | --- | --- | --- | --- | --- |
```

- [x] **Step 2: Extract disputed `tqsdk-core` symbols**

Add one matrix row for each of these symbols or symbol groups:

```text
AggregatedCommit
AggregatedCursor
AggregatedRuntimeReader
AggregatedSnapshotReadGuard
StateSourceId
CommitLog
CommitReadGuard
CursorLagged
OutboundEnvelope
SnapshotReadGuard
FieldMutation
NormalizedMutation
MutationSource
InputPayload
RuntimeInput
CausationMeta
CommandEnvelope
OutboundDispatch
OutboundFrame
OutboundRequest
HttpMethod
HttpRequest
ReplayRequest
InternalRequest
QueryRequest
AuthContext fields
TradePreInsertOrderCommand structure
```

Evidence command:

```bash
rg -n "AggregatedCommit|AggregatedCursor|AggregatedRuntimeReader|AggregatedSnapshotReadGuard|StateSourceId|CommitLog|CommitReadGuard|CursorLagged|OutboundEnvelope|SnapshotReadGuard|FieldMutation|NormalizedMutation|MutationSource|InputPayload|RuntimeInput|CausationMeta|CommandEnvelope|OutboundDispatch|OutboundFrame|OutboundRequest|HttpMethod|HttpRequest|ReplayRequest|InternalRequest|QueryRequest|AuthContext|TradePreInsertOrderCommand" docs crates/tqsdk-core crates/tqsdk-data crates/tqsdk-stream crates/tqsdk-task README.md
```

Default classification rule:

- `RuntimeInput`, `NormalizedMutation`, `SnapshotReadGuard`, `CommitLog`, `OutboundRequest`: `keep` unless architecture docs are deliberately changed later.
- `AuthContext fields`: `deprecate` or `needs-arch-change`, because privatizing fields is source-breaking even if accessors exist.
- `TradePreInsertOrderCommand structure`: `split-plan`, because ergonomics and adapter simplification must be evaluated separately.

- [x] **Step 3: Extract disputed `tqsdk-data` symbols**

Add rows for all public `MarketCache*` exports from `crates/tqsdk-data/src/lib.rs`, grouped only when the group has the same evidence and same disposition.

Evidence command:

```bash
rg -n "MarketCache[A-Za-z0-9_]*" crates/tqsdk-data/src/lib.rs crates/tqsdk-data/README.md crates/tqsdk-data/examples docs/architecture/api-data.md docs/reviews/public-api-scenario-review.md docs/scenarios
```

Default classification rule:

- Any `MarketCache*` type used in `crates/tqsdk-data/examples/api_contract_s18_*.rs`, `crates/tqsdk-data/README.md`, or `docs/architecture/api-data.md`: `needs-arch-change`.
- Any `MarketCache*` type only used inside `crates/tqsdk-data/src` and tests: `internalize`.
- If all cache maintenance types remain example-backed, mark the group `needs-arch-change` and require a dedicated `tqsdk-data` API narrowing plan.

- [x] **Step 4: Extract disputed `tqsdk-stream` symbols**

Add rows for:

```text
StreamSinkWalCompaction
StreamSinkWalCompactionReport
StreamSinkWalFsyncPolicy
StreamSinkWalRecord
StreamSinkWalRecordKind
StreamSinkWalRecovery
StreamSinkWalRecoveryReport
StreamCommitJournal
StreamCommitJournalDomain
StreamCommitJournalRecord
StreamCommitJournalReplayReport
StreamCommitJournalScope
```

Evidence command:

```bash
rg -n "StreamSinkWal|StreamCommitJournal" crates/tqsdk-stream/src/lib.rs crates/tqsdk-stream/README.md crates/tqsdk-stream/examples docs/architecture/api-stream.md docs/reviews/public-api-scenario-review.md docs/scenarios
```

Default classification rule:

- Any symbol used in `api_contract_s21_slow_consumer_isolation.rs`, `crates/tqsdk-stream/README.md`, or `docs/reviews/public-api-scenario-review.md`: `needs-arch-change`.
- Symbols used only internally and in tests: `internalize`.

- [x] **Step 5: Extract disputed `tqsdk-task` symbols**

Add rows for at least these groups:

```text
StrategySupervisorHealth + StrategySupervisorHealthStatus
StrategyRunReport + StrategyRunStopReason
StrategyShutdownReport + StrategyShutdownSignal
StrategyTelemetryEvent + StrategyTelemetryEventKind
MultiAccountOrderState + MultiAccountOrderStatus
ExecutionGroupStatus / ExecutionLegState / report groups
```

Evidence command:

```bash
rg -n "StrategySupervisorHealth|StrategySupervisorHealthStatus|StrategyRunReport|StrategyRunStopReason|StrategyShutdownReport|StrategyShutdownSignal|StrategyTelemetryEvent|StrategyTelemetryEventKind|MultiAccountOrderState|MultiAccountOrderStatus|ExecutionGroupStatus|ExecutionLegState" crates/tqsdk-task/src/lib.rs crates/tqsdk-task/README.md crates/tqsdk-task/examples docs/architecture/api-task.md docs/reviews/public-api-scenario-review.md docs/scenarios
```

Default classification rule:

- If a type appears in task examples, task README, or `docs/architecture/api-task.md`: `needs-arch-change`.
- If the concern is “too many reports/statuses” rather than one symbol being accidental, use `split-plan`.

---

## Task 2: Validate Evidence Against Architecture and Examples

**Files:**
- Modify: `docs/reviews/public-api-disposition-matrix.md`
- Read: `docs/architecture/*`
- Read: `crates/*/examples/api_contract_sXX_*.rs`

- [x] **Step 1: Check architecture-protected `core` symbols**

Run:

```bash
rg -n "RuntimeInput|NormalizedMutation|SnapshotReadGuard|CommitLog|OutboundRequest" docs/architecture crates/tqsdk-core/README.md
```

Update the `Evidence` column for each protected symbol with the exact doc path, such as:

```text
`docs/architecture/api-layers.md`, `crates/tqsdk-core/README.md`
```

- [x] **Step 2: Check example-backed facade symbols**

Run:

```bash
rg -n "MarketCacheWriterElection|MarketCacheRecoveryScan|MarketCacheCompactionOwnership|StreamSinkWalCompaction|StreamSinkWalRecovery|StreamCommitJournal|StrategySupervisorHealth|StrategyTelemetryEvent" crates/*/examples docs/reviews/public-api-scenario-review.md crates/*/README.md
```

Update any matching row to `needs-arch-change` unless the example is first rewritten in a later plan.

- [x] **Step 3: Identify truly accidental exports**

Run:

```bash
rg -n "AggregatedCommit|AggregatedCursor|AggregatedRuntimeReader|AggregatedSnapshotReadGuard|StateSourceId|OutboundEnvelope|CommitReadGuard|CursorLagged|FieldMutation|MutationSource|InputPayload|CausationMeta|CommandEnvelope|OutboundDispatch|OutboundFrame|HttpMethod|HttpRequest|ReplayRequest|InternalRequest|QueryRequest" docs crates README.md
```

For each symbol, distinguish:

- documented architecture/public usage
- tests-only usage
- sibling-crate usage
- no usage outside defining crate

Only the last two categories can become `internalize` candidates, and sibling-crate usage must be handled with `tqsdk_core::internal` or crate-local imports in a later code plan.

- [x] **Step 4: Update summary counts**

Update the `## Summary` table counts so they match the rows in `## Matrix`.

---

## Task 3: Run Compatibility Check and Close Task 1

**Files:**
- Modify: `docs/reviews/public-api-disposition-matrix.md`
- Modify: `docs/superpowers/plans/2026-04-29-review-remediation-plan.md`

- [x] **Step 1: Run workspace example check**

Run:

```bash
cargo check --workspace --examples
```

Expected:

```text
Finished `dev` profile ... or a successful workspace example check with no compile errors.
```

If the command fails for an unrelated pre-existing reason, record the exact failing crate/example and error summary in the matrix under `## Verification`.

- [x] **Step 2: Add verification section**

Append this section to `docs/reviews/public-api-disposition-matrix.md`:

````markdown
## Verification

Command:

```bash
cargo check --workspace --examples
```

Result:

- Status: pass/fail
- Notes: <short exact summary>
```
````

- [x] **Step 3: Update the umbrella roadmap**

In `docs/superpowers/plans/2026-04-29-review-remediation-plan.md`:

- Keep Task 1 pointing to this child plan.
- Add a link to `docs/reviews/public-api-disposition-matrix.md`.
- Do not mark Task 2 as ready unless the matrix exists and the verification section is filled.

- [ ] **Step 4: Commit**

Use `git add -f` for ignored plan files under `docs/superpowers/plans/` if this repository wants to track the plan:

```bash
git add docs/reviews/public-api-disposition-matrix.md
git add -f docs/superpowers/plans/2026-04-29-review-remediation-plan.md docs/superpowers/plans/2026-04-29-public-api-disposition-matrix.md
git commit -m "docs: plan public api disposition matrix"
```

---

## Self-Review

- This child plan is documentation-only and cannot accidentally change code.
- It gives exact evidence commands and exact disputed symbol sets for all four affected crates.
- It makes example-backed public API decisions explicit before later breaking-change work starts.
