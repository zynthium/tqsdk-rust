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
| `internalize` | Candidate to remove from public re-exports. |
| `needs-arch-change` | Requires architecture/docs/examples update before code changes. |
| `split-plan` | Too broad for a symbol-level decision; create a narrower plan. |

## Summary

| Crate | `keep` | `deprecate` | `internalize` | `needs-arch-change` | `split-plan` |
| --- | ---: | ---: | ---: | ---: | ---: |
| `tqsdk-core` | 8 | 1 | 2 | 1 | 1 |
| `tqsdk-data` | 1 | 0 | 0 | 6 | 0 |
| `tqsdk-stream` | 0 | 0 | 0 | 3 | 0 |
| `tqsdk-task` | 0 | 0 | 0 | 5 | 1 |

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
| `tqsdk-data` | Reader manifest surface: `MarketCacheReaderCheckpoint`, `MarketCacheReaderLag`, `MarketCacheReaderManifest` | `crates/tqsdk-data/src/lib.rs` | Documented in `docs/architecture/api-data.md`, `crates/tqsdk-data/README.md`, `docs/reviews/public-api-scenario-review.md`, and `api_contract_s18_cache_reader_manifest.rs`. | `needs-arch-change` | Rewrite S18 docs/examples first if these are to be hidden behind a higher-level service API. |
| `tqsdk-data` | Recovery surface: `MarketCacheRecoveryFileKind`, `MarketCacheRecoveryFileReport`, `MarketCacheRecoveryReport`, `MarketCacheRecoveryScan`, `MarketCacheRecoveryAction`, `MarketCacheRecoveryActionReport` | `crates/tqsdk-data/src/lib.rs` | Documented in `docs/architecture/api-data.md`, `crates/tqsdk-data/README.md`, `docs/reviews/public-api-scenario-review.md`, and S18 recovery examples. | `needs-arch-change` | Requires a `tqsdk-data` cache maintenance API narrowing plan before code changes. |
| `tqsdk-data` | Writer election surface: `MarketCacheWriterElection`, `MarketCacheWriterElectionStatus`, `MarketCacheWriterElectionReport`, `MarketCacheWriterElectionOutcome`, `MarketCacheWriterLease` | `crates/tqsdk-data/src/lib.rs` | Documented in `docs/architecture/api-data.md`, `crates/tqsdk-data/README.md`, `docs/reviews/public-api-scenario-review.md`, and S18 writer/compaction examples. | `needs-arch-change` | Rewrite examples to use a service-level API before removing these from public re-exports. |
| `tqsdk-data` | Local maintenance primitives: `MarketCacheQueue`, `MarketCacheQueueDrainError`, `MarketCacheQueueDrainReport`, `MarketCacheLock`, `MarketCacheLockOptions`, `MarketCacheIndex`, `MarketCacheIndexKey`, `MarketCacheIndexEntry` | `crates/tqsdk-data/src/lib.rs` | Documented in `docs/architecture/api-data.md`, `crates/tqsdk-data/README.md`, `docs/reviews/public-api-scenario-review.md`, and S18 maintenance examples. | `needs-arch-change` | Decide whether maintenance primitives remain public foundations or move behind `MarketCacheService`. |
| `tqsdk-data` | Compaction surface: `MarketCacheCompaction`, `MarketCacheCompactionReport`, `MarketCacheAtomicCompactionReport`, `MarketCacheCompactionOwnership`, `MarketCacheCompactionOwnershipReport` | `crates/tqsdk-data/src/lib.rs` | Documented in `docs/architecture/api-data.md`, `crates/tqsdk-data/README.md`, `docs/reviews/public-api-scenario-review.md`, and S18 compaction examples. | `needs-arch-change` | Requires example and architecture rewrite before narrowing. |
| `tqsdk-data` | Service/daemon/supervisor surface: `MarketCacheServiceConfig`, `MarketCacheService`, `MarketCacheServiceOpenReport`, `MarketCacheServiceOpen`, `MarketCacheServiceShutdownReport`, `MarketCacheDaemonConfig`, `MarketCacheDaemon`, `MarketCacheDaemonShutdownReport`, `MarketCacheSupervisorConfig`, `MarketCacheSupervisor`, `MarketCacheSupervisorShutdownReport` | `crates/tqsdk-data/src/lib.rs` | Documented in `docs/architecture/api-data.md`, `crates/tqsdk-data/README.md`, `docs/reviews/public-api-scenario-review.md`, and S18 service/daemon/supervisor examples. | `needs-arch-change` | Keep public until the S18 boundary is rewritten or service shape is intentionally reduced. |
| `tqsdk-stream` | WAL record and fsync surface: `StreamSinkWalFsyncPolicy`, `StreamSinkWalRecord`, `StreamSinkWalRecordKind` | `crates/tqsdk-stream/src/lib.rs` | Documented in `docs/architecture/api-stream.md`, `crates/tqsdk-stream/README.md`, `docs/reviews/public-api-scenario-review.md`, and `api_contract_s21_slow_consumer_isolation.rs`. | `needs-arch-change` | Rewrite S21 docs/examples before hiding these details. |
| `tqsdk-stream` | WAL maintenance surface: `StreamSinkWalCompaction`, `StreamSinkWalCompactionReport`, `StreamSinkWalRecovery`, `StreamSinkWalRecoveryReport` | `crates/tqsdk-stream/src/lib.rs` | Documented in `docs/architecture/api-stream.md`, `crates/tqsdk-stream/README.md`, `docs/reviews/public-api-scenario-review.md`, and S21 example. | `needs-arch-change` | Requires a stream sink durability API narrowing plan. |
| `tqsdk-stream` | Commit journal surface: `StreamCommitJournal`, `StreamCommitJournalDomain`, `StreamCommitJournalRecord`, `StreamCommitJournalReplayReport`, `StreamCommitJournalScope` | `crates/tqsdk-stream/src/lib.rs` | Documented in `docs/architecture/api-stream.md`, `crates/tqsdk-stream/README.md`, `docs/reviews/public-api-scenario-review.md`, and S21 example. | `needs-arch-change` | Requires S21 example rewrite or a replacement high-level replay API first. |
| `tqsdk-task` | `StrategySupervisorHealth` + `StrategySupervisorHealthStatus` | `crates/tqsdk-task/src/lib.rs` | `StrategySupervisorHealth` is documented in `docs/reviews/public-api-scenario-review.md` and scenario gap docs; supervisor health is part of S20 public contract. | `needs-arch-change` | Any merge of status into health must update S20 docs/examples first. |
| `tqsdk-task` | `StrategyRunReport` + `StrategyRunStopReason` | `crates/tqsdk-task/src/lib.rs` | Public report/status pair; audit concern is type-shape design rather than accidental export. | `split-plan` | Create a task API report-shape plan before changing public report types. |
| `tqsdk-task` | `StrategyShutdownReport` + `StrategyShutdownSignal` | `crates/tqsdk-task/src/lib.rs` | `StrategyShutdownSignal` is used in `api_contract_s15_live_sim_replay_switch.rs`, `api_contract_s20_strategy_supervisor.rs`, `docs/architecture/api-task.md`, and `docs/reviews/public-api-scenario-review.md`. | `needs-arch-change` | Keep until S15/S20 shutdown contract is redesigned. |
| `tqsdk-task` | `StrategyTelemetryEvent` + `StrategyTelemetryEventKind` | `crates/tqsdk-task/src/lib.rs` | `StrategyTelemetryEvent` is used in `api_contract_s20_strategy_supervisor.rs`, `docs/reviews/public-api-scenario-review.md`, and S20 gap docs. | `needs-arch-change` | Rewrite telemetry examples and docs before hiding event kind/type details. |
| `tqsdk-task` | `MultiAccountOrderState` + `MultiAccountOrderStatus` | `crates/tqsdk-task/src/lib.rs` | `MultiAccountOrderStatus` appears in `docs/architecture/api-task.md` as the status return type for multi-account tickets. | `needs-arch-change` | Keep until S13 status/report contract is redesigned. |
| `tqsdk-task` | `ExecutionGroupStatus`, `ExecutionLegState`, execution report/status groups | `crates/tqsdk-task/src/lib.rs` | `ExecutionGroupStatus` appears in `docs/architecture/api-task.md` as the status return type for execution group tickets. | `needs-arch-change` | Keep until S12 execution group report/status contract is redesigned. |

## Immediate Conclusions

- The audit suggestion to shrink `tqsdk-core` cannot be applied mechanically. Several disputed low-level types are explicitly documented runtime contracts.
- `tqsdk-data` and `tqsdk-stream` cache/WAL/journal types are not accidental exports today; they are documented and scenario-backed. Narrowing them is a public API redesign.
- The clear `tqsdk-core` immediate internalization candidates from this pass, the aggregation surface and `OutboundEnvelope`, have been closed.
- `AuthContext` field privatization was handled as a focused source-breaking change separate from broad public API surface reduction.

## Verification

Command:

```bash
cargo check --workspace --examples
```

Result:

- Status: pass
- Notes: Workspace examples compiled successfully on 2026-04-29.
