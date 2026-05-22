# Public API Disposition Matrix

> Source audits: [`../archive/reviews/2026-04-29/public-api-overdesign-audit.md`](../archive/reviews/2026-04-29/public-api-overdesign-audit.md), [`../archive/reviews/2026-04-29/review-2026-04-29-pending.md`](../archive/reviews/2026-04-29/review-2026-04-29-pending.md)
> Date: 2026-04-29

This document classifies disputed public API symbols before any remediation code changes.
It is the gate for public API narrowing work.

## Disposition Values

| Value | Meaning |
| --- | --- |
| `keep` | Keep public; current architecture/docs/examples depend on it. |
| `deprecate` | Keep public now; design a migration/deprecation path first. |
| `keep-advanced` | Keep public but document outside the ordinary `tqsdk`/prelude path. |
| `internalize` | Candidate to remove from public re-exports. |
| `needs-arch-change` | Requires architecture/docs/examples update before code changes. |
| `defer` | Valid concept that should remain a plan, archived sketch, or future crate/tooling scope until evidence justifies a stable contract. |
| `split-plan` | Too broad for a symbol-level decision; create a narrower plan. |
| `remove-from-active-docs` | Remove or rewrite active docs that present this as a current SDK promise. |
| `removed` | Removed from public API after architecture/docs/examples were rewritten. |

## 2026-04-29 Summary

| Crate | `keep` | `deprecate` | `internalize` | `needs-arch-change` | `split-plan` | `removed` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `tqsdk-core` | 8 | 1 | 2 | 1 | 1 | 0 |
| `tqsdk-data` | 1 | 0 | 0 | 6 | 0 | 0 |
| `tqsdk-stream` | 0 | 0 | 0 | 0 | 0 | 1 |
| `tqsdk-task` | 0 | 0 | 0 | 5 | 1 | 0 |

## Matrix

| Crate | Symbol or Group | Current Export | Evidence | Disposition | Required Follow-up |
| --- | --- | --- | --- | --- | --- |
| `tqsdk-core` | `AggregatedCommit`, `AggregatedCursor`, `AggregatedRuntimeReader`, `AggregatedSnapshotReadGuard`, `StateSourceId` | Private test-only module (`crates/tqsdk-core/src/aggregation.rs`) | Only found in private aggregation unit tests; not found in architecture docs, crate README, or examples. | `internalize` | Completed by `docs/archive/superpowers/plans/2026-04-29-core-safe-surface-narrowing.md`: root re-exports removed and coverage moved to private unit tests. |
| `tqsdk-core` | `CommitLog` | `crates/tqsdk-core/src/lib.rs` | Documented in `docs/architecture/api-layers.md`, `docs/architecture/runtime-core/overview.md`, `docs/architecture/validation.md`, and `crates/tqsdk-core/README.md` as a compatibility or low-level primitive. | `keep` | Do not internalize unless architecture docs first remove `CommitLog` from the compatibility contract. |
| `tqsdk-core` | `SnapshotReadGuard`, `CommitReadGuard`, `CursorLagged` | `crates/tqsdk-core/src/lib.rs` | Documented in `docs/architecture/api-layers.md`, `docs/architecture/runtime-core/overview.md`, `docs/architecture/validation.md`, and `crates/tqsdk-core/README.md` as reader-first contract types. | `keep` | Keep public for current runtime/read contract. |
| `tqsdk-core` | `OutboundEnvelope` | Crate-private runtime internals | Found in runtime implementation and tests; not found in architecture docs, crate README, or examples. | `internalize` | Completed by `docs/archive/superpowers/plans/2026-04-29-core-safe-surface-narrowing.md`: `OutboundEnvelope` is crate-private and tests use public `OutboundDispatch`. |
| `tqsdk-core` | `FieldMutation`, `NormalizedMutation`, `MutationSource` | `crates/tqsdk-core/src/lib.rs` | Documented in `docs/architecture/runtime-core/data-contracts.md`, `docs/architecture/diff-core.md`, `docs/architecture/runtime-core/modules.md`, and `docs/architecture/api-layers.md`. | `keep` | Keep public as runtime/protocol adapter contract. |
| `tqsdk-core` | `InputPayload` | `crates/tqsdk-core/src/lib.rs` | Used by sibling crates such as `tqsdk-data`, `tqsdk-session`, and `tqsdk-task`, plus test support; not named as a standalone stable type in architecture docs. | `needs-arch-change` | Decide whether it is a formal runtime input payload contract or sibling-crate internal bridge before changing visibility. |
| `tqsdk-core` | `RuntimeInput` | `crates/tqsdk-core/src/lib.rs` | Documented in `docs/architecture/api-layers.md`, `docs/architecture/runtime-core/data-contracts.md`, `docs/architecture/runtime-core/overview.md`, and `docs/architecture/validation.md`. | `keep` | Keep public as runtime input contract. |
| `tqsdk-core` | `CausationMeta`, `CommandEnvelope` | `crates/tqsdk-core/src/lib.rs` | Documented in `docs/architecture/runtime-core/data-contracts.md` and `docs/architecture/runtime-core/modules.md`. | `keep` | Keep public unless command envelope contract is redesigned. |
| `tqsdk-core` | `OutboundDispatch` | `crates/tqsdk-core/src/lib.rs` | Documented in `crates/tqsdk-core/README.md` as part of the command chain; used by `tqsdk-session` transport/http executor paths. | `keep` | Treat as low-level dispatch contract for now. Any narrowing needs session route refactor first. |
| `tqsdk-core` | `OutboundFrame`, `OutboundRequest` | `crates/tqsdk-core/src/lib.rs` | Documented in `docs/architecture/runtime-core/data-contracts.md`, `docs/architecture/runtime-core/modules.md`, and `docs/architecture/api-layers.md`. | `keep` | Keep public as protocol adapter output contract. |
| `tqsdk-core` | `HttpMethod`, `HttpRequest`, `ReplayRequest`, `InternalRequest`, `QueryRequest` | `crates/tqsdk-core/src/lib.rs` | `HttpMethod` and `HttpRequest` are documented in `docs/architecture/runtime-core/data-contracts.md`; the request variants are part of documented `OutboundRequest`. | `keep` | Keep public unless `OutboundRequest` is redesigned. |
| `tqsdk-core` | `AuthContext` public fields | Constructor and accessor APIs in `crates/tqsdk-core/src/auth.rs` | `AuthContext` is documented in `docs/architecture/runtime-core/session-auth.md`; direct field access is no longer part of the public contract. | `internalize` | Direct fields were removed from the public contract by `docs/archive/superpowers/plans/2026-04-29-core-auth-context-privacy.md`; constructor and accessor APIs remain public. |
| `tqsdk-core` | `TradePreInsertOrderCommand` structure | `crates/tqsdk-core/src/commands.rs` | Public command struct; audit suggests composition with `TradeInsertOrderCommand`, but ergonomics and adapter impact need separate review. | `split-plan` | Create a dedicated compatibility plan before restructuring this public command type. |
| `tqsdk-data` | Cache record entry surface: `MarketCacheEvent`, `MarketCachePayload`, `MarketCachePayloadKind`, `MarketCacheWriter`, `MarketCacheReader`, `MarketCacheReplay` | `crates/tqsdk-data/src/lib.rs` | Documented in `docs/architecture/api-data.md`, `crates/tqsdk-data/README.md`, `docs/reviews/public-api-scenario-review.md`, and S18 examples. | `keep` | Keep as the minimal offline cache/replay public surface. |
| `tqsdk-data` | History series mmap cache surface: `DataClientBuilder`, `HistorySeriesCache`, `HistorySeriesCacheBackend`, `HistorySeriesCacheReport`, `HistorySeriesCacheMiss`, `HistorySeriesCacheScanReport`, `HistorySeriesCacheMaintenanceReport` | `crates/tqsdk-data/src/lib.rs` | Documented in `docs/architecture/api-data.md`, `crates/tqsdk-data/README.md`, `docs/reviews/public-api-scenario-review.md`, `crates/tqsdk-data/examples/api_contract_s30_history_series_cache.rs`, and archived S30 sketch. | `keep` | Keep as the explicit opt-in Python-compatible history series cache foundation; do not move it into core/session/wait/stream. Python/Rust same-directory simultaneous writes remain non-goal. |
| `tqsdk-data` | S18 cache orchestration surfaces and live stream pipe: reader manifest, recovery scan/action, writer election/lease, local queue/lock/index, compaction ownership, service/daemon/supervisor, `MarketCacheStreamWriter` | Removed from `crates/tqsdk-data/src/lib.rs` | S18 is narrowed to offline `MarketCacheEvent` / writer / reader / replay. Former live-pipe/orchestration examples and cross-process sketch are archived or removed from active examples. | `rollback` | Keep out of current public API. Reintroduce only via a new user-tooling or independent service design. |
| `tqsdk-stream` | Managed sink / WAL / commit journal surface: `CommitSink`, `StreamSink*`, `StreamCommitJournal*`, `spawn_commit_sink*` | Removed from `crates/tqsdk-stream/src/lib.rs` and `TqStream` | S20/S21/S31 docs and examples were rewritten around health/shutdown, bounded fan-out lag diagnostics, and user-owned sidecars. | `removed` | Keep durable sinks, WAL, journal, compaction, recovery, and cross-process queues out of `tqsdk-stream` unless a new architecture plan explicitly reintroduces them. |
| `tqsdk-task` | `StrategySupervisorHealth` + `StrategySupervisorHealthStatus` | `crates/tqsdk-task/src/lib.rs` | `StrategySupervisorHealth` is documented in `docs/reviews/public-api-scenario-review.md` and scenario gap docs; supervisor health is part of S20 public contract. | `needs-arch-change` | Any merge of status into health must update S20 docs/examples first. |
| `tqsdk-task` | `StrategyRunReport` + `StrategyRunStopReason` | `crates/tqsdk-task/src/lib.rs` | Public report/status pair; audit concern is type-shape design rather than accidental export. | `split-plan` | Create a task API report-shape plan before changing public report types. |
| `tqsdk-task` | `StrategyShutdownReport` + `StrategyShutdownSignal` | `crates/tqsdk-task/src/lib.rs` | `StrategyShutdownSignal` is used in `api_contract_s15_live_sim_replay_switch.rs`, `api_contract_s20_strategy_supervisor.rs`, `docs/architecture/api-task.md`, and `docs/reviews/public-api-scenario-review.md`. | `needs-arch-change` | Keep until S15/S20 shutdown contract is redesigned. |
| `tqsdk-task` | `StrategyTelemetryEvent` + `StrategyTelemetryEventKind` | `crates/tqsdk-task/src/lib.rs` | `StrategyTelemetryEvent` is used in `api_contract_s20_strategy_supervisor.rs`, `docs/reviews/public-api-scenario-review.md`, and S20 gap docs. | `needs-arch-change` | Rewrite telemetry examples and docs before hiding event kind/type details. |
| `tqsdk-task` | `MultiAccountOrderState` + `MultiAccountOrderStatus` | `crates/tqsdk-task/src/lib.rs` | `MultiAccountOrderStatus` appears in `docs/architecture/api-task.md` as the status return type for multi-account tickets. | `needs-arch-change` | Keep until S13 status/report contract is redesigned. |
| `tqsdk-task` | `ExecutionGroupStatus`, `ExecutionLegState`, execution report/status groups | `crates/tqsdk-task/src/lib.rs` | `ExecutionGroupStatus` appears in `docs/architecture/api-task.md` as the status return type for execution group tickets. | `needs-arch-change` | Keep until S12 execution group report/status contract is redesigned. |

## Immediate Conclusions

- The audit suggestion to shrink `tqsdk-core` cannot be applied mechanically. Several disputed low-level types are explicitly documented runtime contracts.
- Remaining `tqsdk-data` offline cache types are documented and scenario-backed. The former `tqsdk-stream` sink/WAL/journal surface has been removed after the S20/S21/S31 contract rewrite.
- The clear `tqsdk-core` immediate internalization candidates from this pass, the aggregation surface and `OutboundEnvelope`, have been closed.
- `AuthContext` field privatization was handled as a focused source-breaking change separate from broad public API surface reduction.

## 2026-04-29 Verification

Command:

```bash
cargo check --workspace --examples
```

Result:

- Status: pass
- Notes: Workspace examples compiled successfully on 2026-04-29.

## 2026-05-22 SDK Overdesign Audit Pass

| Crate | Symbol or Group | Current Export | Evidence | Disposition | Required Follow-up |
| --- | --- | --- | --- | --- | --- |
| `tqsdk` | `Tq`, `TqBuilder`, `TargetPos`, `prelude` | Root facade | Ordinary users need one obvious entrypoint. | `keep` | Keep thin and scenario-backed by default facade examples. |
| `tqsdk` | `advanced::*` curated aliases | Root module | Useful escape hatch, but not a full replacement for sibling crate dependencies. | `keep-advanced` | Document that full-power users should depend on sibling crates directly. |
| `tqsdk-session` | Direct query, metadata, schema, service query helpers | Crate root / traits | One-shot request/response belongs here, not in wait or stream. | `keep` | Keep out of wait/stream docs except through `session()` escape hatch. |
| `tqsdk-wait` | `TqApi`, `wait_update`, `quote(s)`, live refs, order helpers | Crate root | Canonical single-owner strategy and Python-style stable snapshot path. | `keep` | Add wait-side changed quote iteration plan for full-universe single-consumer workloads. |
| `tqsdk-stream` | `quote_batches`, commit stream, filters, lag diagnostics, health/shutdown | Crate root | Genuinely different async multi-consumer consumption model. | `keep-advanced` | Present as advanced integration, not default quote-performance path. |
| `tqsdk-stream` | Broad object/event stream families | Crate root | Potentially useful but broad for 0.1 stabilization. | `keep-advanced` | Keep outside first-read docs and review before semver stabilization. |
| `tqsdk-task` | `TargetPosTask`, ownership guard, basic risk, typed task order builder, test harness, TqSim | Crate root | Mature trading workflow support and Python benchmark parity. | `keep-advanced` | Expose ordinary thin wrappers through `tqsdk` only for common strategy tasks. |
| `tqsdk-task` | Supervisor/deployment/desk/telemetry/platform-like surfaces | Crate root | Useful foundations but risk reading as a production platform. | `keep-advanced` | Remove wording that implies OMS, daemon, HTTP, GUI, durable audit, or managed sink ownership. |
| `tqsdk-data` | History page/series/download, CSV export, Greeks, cache/replay | Crate root | Research/offline workflow support and Python benchmark parity. | `keep-advanced` | Keep opt-in; do not make live facades depend on history cache in hot paths. |
| Active docs | `MarketCacheStreamWriter`, live cache pipe, managed sink/WAL/journal claims | Removed or stale | These contradict current stream/data/task boundaries. | `remove-from-active-docs` | Rewrite active docs; archived historical sketches may keep them as historical context. |
| Architecture docs | `tqsdk-api-wait`, `tqsdk-api-stream`, historical V1 facade wording | Active docs | Names drifted after actual crate names and facade work landed. | `remove-from-active-docs` | Rename to `tqsdk-wait` / `tqsdk-stream` and clarify historical context. |
