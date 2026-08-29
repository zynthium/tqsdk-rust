# Deep Plan: historical dynamic universe backtest

## 1. Plan metadata

- **Objective:** make a cache-backed local backtest replay the complete, pinned historical membership of a futures universe: physical contracts join and leave at catalog-effective instants; continuous and index views are derived independently; every membership transition and its first data are visible in one runtime revision.
- **Category:** architecture / public API / runtime contract / persisted planning artifact.
- **Planning mode:** Deep, source-weighted. CodeGraph was used for navigation. GitNexus CLI did not return version, context, query, impact, or PDG output in this environment, so this plan makes no load-bearing graph claim. Before implementation, rebuild a fresh index with PDG and run impact analysis on every edited Rust symbol.
- **Evidence baseline:** commit `675be2871d9d6861279e41a440ab8cb27ee35e40`; one pre-existing unstaged change in `crates/tqsdk-cache/src/main.rs` is included in the evidence digest. It is the separate `--start-day` default work and must be preserved, not reverted or folded into this feature.

## 2. Confirmed product contract

1. A dynamic universe is a historical timeline, not a time-varying interpretation of the existing `UniverseExpression`.
2. A V1 `CatalogSnapshot` is supplied by the user, versioned, hash-pinned and validated. It must prove catalogue completeness for the requested scope and window; missing history is an error. V1 does not infer history from today's `expired=false` contracts and has no remote catalog importer.
3. Physical contracts are selected by lifecycle intervals only. Session/tradability is separate state: an unknown trading status rejects new orders, while removal from membership does not erase state, positions, or orders. Expiry/delisting with remaining positions or orders fails strictly; V1 never force-closes.
4. `Continuous` and `Index` are typed derived views, not physical aliases. Derived views exist for products with an active physical member. `Continuous` has recorded historical physical provider segments and emits `SourceRebound`; `Index` keeps stable identity and may expose constituents, never a fake sole physical provider.
5. Exact catalog timestamps control transitions; date-only catalog values are normalized through the pinned trading calendar to the first session instant. All stored/event timestamps are UTC epoch nanoseconds.
6. At the same instant, membership delta, source/binding state, instrument bootstrap, and market data become one `ReplayStep` commit. `RuntimeReader + UpdateCursor` is the only strategy-visible notification path.
7. A dynamic source binding is inherited only when explicitly requested. Default data requirement is `Required`; an active symbol may be `WarmingUp`, and a strategy may explicitly require `Ready`. Required means permanent unavailable/invalid source coverage is a prepare failure, not that pre-listing data is invented.
8. `prepare()` is an offline, deterministic preflight that writes a reusable versioned `HistoricalUniversePlan`, checks `UniverseBudget`, cache coverage, source provenance, catalog/calendar identity and warmup feasibility. Failed preparation may retain validated cache and an incomplete report, but cannot run. V1 explicitly rejects checkpoint/resume.
9. Static symbols and an existing `--universe` retain their current static semantics; dynamic membership is an additive `--universe-timeline` / plan input, not an overload of `active:all`, `main:all`, `cont:all`, or `index:all`.

## 3. Current-state evidence and gap

- [`UniverseExpression` resolution](/opt/tqsdk-rust/crates/tqsdk-data/src/universe.rs:401) returns one current `Vec<String>` via a resolver whose physical contract model uses current `expired`, so it cannot represent historical intervals. Existing selector tests explicitly cover current `active`, `main`, `index`, and `cont` semantics in [`universe_selector.rs`](/opt/tqsdk-rust/crates/tqsdk-data/tests/universe_selector.rs:18).
- [`BacktestBuilder::prepare`](/opt/tqsdk-rust/crates/tqsdk/src/lib.rs:2885) resolves its optional universe once and constructs fixed planned inputs; [`history_backtest_replay.rs`](/opt/tqsdk-rust/crates/tqsdk-task/src/history_backtest_replay.rs:1) likewise names one fixed replay/cache symbol per history source.
- The history planner already represents source slices and physical continuous segments in [`planner.rs`](/opt/tqsdk-rust/crates/tqsdk-data/src/backtest_history/planner.rs:1), but needs a timeline-aware caller and pinned catalog provenance.
- Runtime batching and replay commits already establish the desired single revision precedent in [`runtime_contract_batch_commit.rs`](/opt/tqsdk-rust/crates/tqsdk-core/tests/runtime_contract_batch_commit.rs:1) and [`runtime_contract_replay_commit.rs`](/opt/tqsdk-rust/crates/tqsdk-core/tests/runtime_contract_replay_commit.rs:1). Mutation root admission is centralized in [`handle.rs`](/opt/tqsdk-rust/crates/tqsdk-core/src/runtime/handle.rs:885), so the new neutral replay-universe state belongs there rather than in a facade-private tree.
- Replay currently orders events by `(event_time_ns, received_at_ns)` and checkpoint/resume advances only event index/time in [`replay.rs`](/opt/tqsdk-rust/crates/tqsdk-task/src/replay.rs:1); tests show resume skipping processed events in [`strategy_replay.rs`](/opt/tqsdk-rust/crates/tqsdk-task/tests/strategy_replay.rs:99). That cannot restore universe, runtime, orders, or positions safely, hence V1 rejects it.
- Snapshot `catalog.complete` currently proves completeness only for a publisher-declared service universe, not all-market historical catalog completeness ([`history-snapshot-manifest.md`](/opt/tqsdk-rust/docs/architecture/history-snapshot-manifest.md:94)). The new catalog proof must therefore be a distinct, scope/window-specific artifact.

## 4. Scope and non-goals

**In scope:** historical catalogue artifact and timeline planner; physical lifecycle/dynamic derived views; preflight plan/report/budget; atomic runtime representation; cache fill and facade inputs; strategy membership/readiness/order gates; focused contracts and documentation.

**Out of scope for V1:** remote catalogue download, reconstructing history from current metadata, calendar-free date coercion, automatic position close, dynamic stream auto-binding, any side-channel callback, changing ordinary static selector meaning, checkpoint/resume, real-account/live smoke, and a general multi-provider framework.

## 5. Implementation sequence

### Step 0 — restore graph evidence and freeze compatibility surface

Before editing, run `node .gitnexus/run.cjs analyze --index-only --pdg`, verify its version/index freshness, then run upstream impact for each target symbol: `UniverseExpression::resolve`/`resolve_futures_universe_symbols`, `BacktestBuilder::prepare`, `ReplayMarketSource::new`, `StrategyBacktest::next`, and `mutation_source_allows_root`. Stop and report before edits if any result is HIGH/CRITICAL/UNKNOWN; source-search unresolved callers. Record actual caller/process/PDG findings in the implementation PR, not this plan.

Add characterization tests ensuring `active:all`, `main:all`, `cont:all`, `index:all`, static symbols, cache sharing, and the current `tqsdk-cache fill --universe` behavior remain static/current as in [`universe_selector.rs`](/opt/tqsdk-rust/crates/tqsdk-data/tests/universe_selector.rs:18) and [`main.rs`](/opt/tqsdk-rust/crates/tqsdk-cache/src/main.rs:272). Preserve the existing start-only closed-day change in that dirty file.

### Step 1 — model and validate a pinned historical catalogue in `tqsdk-data`

Add a dedicated public module below `crates/tqsdk-data/src/` and re-export only its stable data-facing API:

- `CatalogSnapshot { format_version, catalog_id, content_sha256, calendar_identity, scope, generated_at_ns, contracts }` and `CatalogContract { physical_symbol, exchange, product, lifecycle: Vec<ActiveInterval>, tradability: Vec<...>, metadata/provenance }`.
- Validate canonical ordering, full hash, non-overlapping lifecycle intervals, valid UTC-ns bounds, selector scope, product/exchange exclusions, and calendar identity. Make absence/incomplete coverage fail closed for every requested product/window.
- Define `DynamicUniverseScope` for physical selector filters and a separate `DerivedView { continuous, index }`; retain `UniverseExpression` as the current/static selector parser. Define typed `UniverseInstrumentId::{Physical, Continuous, Index}` plus provenance, not free-form strings.
- Compile the snapshot into sorted `UniverseTimelineBatch` records: adds/removes, derived availability, provider rebinding, and per-instrument readiness prerequisites. Coalesce all changes at identical timestamps. Keep removed physical state addressable, but mark new opens ineligible.

Extend [`crates/tqsdk-data/tests/universe_selector.rs`](/opt/tqsdk-rust/crates/tqsdk-data/tests/universe_selector.rs:1) or add a dedicated `historical_universe.rs` integration test for: delisted-before-today inclusion, a newly listed contract, multiple lifecycle intervals, exclusions, catalog gap rejection, hash/calendar mismatch, same-timestamp ordering, and `main:all`/`cont:all` non-equivalence. Do not route historical selection through [`resolve_futures_universe_symbols`](/opt/tqsdk-rust/crates/tqsdk-data/src/universe.rs:401).

### Step 2 — turn the timeline into a deterministic reusable history plan

Extend the history planning API rooted at [`crates/tqsdk-data/src/backtest_history/planner.rs`](/opt/tqsdk-rust/crates/tqsdk-data/src/backtest_history/planner.rs:1):

- Introduce versioned `HistoricalUniversePlan` and `HistoricalUniversePlanReport`, carrying plan identity, catalog/calendar hashes, requested horizon, timeline batches, logical instrument identities, physical source slices, derived provider segments, binding policy/readiness window, cache coverage/finality, and budget accounting.
- Require typed `UniverseBudget` for library `prepare`; reject request count, symbols, source slices, estimated bytes, events, or warmup work beyond a declared limit with an actionable report.
- Plan physical source ranges only within lifecycle intervals. For continuous views, persist official/provider mapping segments and reject gaps; for index views retain logical identity and optional constituent provenance without a single-provider fiction. Merge static requested symbols with the dynamic timeline as a union.
- Apply explicit bindings to additions only when configured. `Required` validates durable availability/coverage; `Optional` reports a gap; `WarmingUp` is legal until the declared view width has accumulated and switches atomically to `Ready`. The default is `Required + AllowWarmingUp`; builders can demand `Ready` before a strategy acts on an instrument.
- Persist/load the plan with schema and content hash; refuse a run whose cache/catalog/calendar/options identities differ. On preflight failure, write a clearly incomplete report and never expose an executable plan.

Add data contracts next to [`backtest_history_api.rs`](/opt/tqsdk-rust/crates/tqsdk-data/tests/backtest_history_api.rs:1): round-trip/hash stability; physical clipping; continuous rebound provenance; index stability; static-plus-dynamic union; optional vs required gap; readiness progression; budget rejection; reproducible complete and incomplete reports.

### Step 3 — add a neutral atomic replay-universe mutation in `tqsdk-core`

Define a core-neutral `ReplayUniverseBatch` input/mutation (no catalogue downloading, planner, cache, or task policy) in the runtime event/normalized-mutation surface at [`crates/tqsdk-core/src/events.rs`](/opt/tqsdk-rust/crates/tqsdk-core/src/events.rs:63). It carries the replay session ID, effective timestamp, typed member upsert/remove/readiness/provenance changes, and optional bootstrap market-state mutations.

Teach default replay normalization and [`mutation_source_allows_root`](/opt/tqsdk-rust/crates/tqsdk-core/src/runtime/handle.rs:885) to admit a bounded replay-owned root, e.g. `replay/<session>/universe`, only through `MutationSource::ReplayStep`. Build one `ingest_batch(..., CommitScope::ReplayStep)` containing universe state, bootstrap, and market mutations. Update `ChangeSet`/object hits so one `RuntimeReader` cursor sees the entire transition, and prohibit private revisions/notifications.

Add core contract tests beside [`runtime_contract_replay_commit.rs`](/opt/tqsdk-rust/crates/tqsdk-core/tests/runtime_contract_replay_commit.rs:1): one revision contains membership delta + bootstrap + quote; cursor gets exactly one commit; removed state remains readable; malformed/root-escaping mutation is rejected; identical batches are deterministic. Keep the existing initial-ready batch contract in [`runtime_contract_batch_commit.rs`](/opt/tqsdk-rust/crates/tqsdk-core/tests/runtime_contract_batch_commit.rs:1) unchanged.

### Step 4 — make `tqsdk-task` schedule timeline barriers before market events

Refactor the replay assembly around [`crates/tqsdk-task/src/replay.rs`](/opt/tqsdk-rust/crates/tqsdk-task/src/replay.rs:1), [`backtest.rs`](/opt/tqsdk-rust/crates/tqsdk-task/src/backtest.rs:1), and [`history_backtest_replay.rs`](/opt/tqsdk-rust/crates/tqsdk-task/src/history_backtest_replay.rs:1):

- Accept a prepared plan, create explicit same-time batches, and order within a timestamp as universe delta/binding/bootstrap then market data, emitting one core batch rather than relying on `received_at_ns` tie order.
- Maintain eligibility, `WarmingUp`/`Ready`, and typed derived provenance from the runtime state. Add no second state tree; strategy queries read the replay root through the existing reader/cursor mechanism.
- Enforce post-removal behaviour in the sim/task boundary: historical state remains; new opens for inactive symbols and new opens with `Unknown` tradability fail deterministically; existing lifecycle semantics remain. An order created at a member's first observed market event is evaluated only from the next event.
- Reject all `resume_from`, persistent checkpoint store, and equivalent recovery inputs when a dynamic plan is present, with a precise V1 error. Keep ordinary static replay checkpoint behaviour unchanged.

Expand [`strategy_backtest.rs`](/opt/tqsdk-rust/crates/tqsdk-task/tests/strategy_backtest.rs:1), [`strategy_replay.rs`](/opt/tqsdk-rust/crates/tqsdk-task/tests/strategy_replay.rs:83), and history replay tests: initial atomic membership; addition, removal, expiry failure with open position/order; source rebound; index no-fake-provider; warming-to-ready; ready-required filter; tie ordering; first-event order delay; static compatibility; dynamic checkpoint rejection.

### Step 5 — expose only prepared dynamic plans through facade and cache CLI

In [`crates/tqsdk/src/lib.rs`](/opt/tqsdk-rust/crates/tqsdk/src/lib.rs:2873), add additive builder inputs such as `historical_universe_plan(...)` and/or `historical_universe_catalog(...).dynamic_scope(...).derived_views(...).universe_budget(...)`. `BacktestBuilder::prepare` must produce or validate `HistoricalUniversePlan`, preflight all cache/source/warmup work, and `connect/run` must consume only the prepared, identity-checked plan. Do not change `.universe(...)` resolution at [`lib.rs`](/opt/tqsdk-rust/crates/tqsdk/src/lib.rs:2885).

Update the facade contract example (add an `sXX` scenario instead of changing the static warmup example [`api_contract_s45_facade_backtest_cache_warmup.rs`](/opt/tqsdk-rust/crates/tqsdk/examples/api_contract_s45_facade_backtest_cache_warmup.rs:1)) to demonstrate catalog input, budget, preflight report, `CacheOnly` rerun, runtime cursor membership delta, and `Ready` requirement.

In [`crates/tqsdk-cache/src/main.rs`](/opt/tqsdk-rust/crates/tqsdk-cache/src/main.rs:272), retain `--universe` as static. Add an explicit `--universe-timeline <plan-or-catalog>` flow plus `--universe-budget` controls and a dry-run/JSON report that identifies plan/catalog identities, physical source slices, derived provenance, gaps, warmup/readiness, and budget. Reuse data planning/fill paths; do not build a second resolver. Add CLI tests in [`crates/tqsdk-cache/tests/cli.rs`](/opt/tqsdk-rust/crates/tqsdk-cache/tests/cli.rs:1) for static compatibility, plan acceptance, required catalog/budget, rejection of incomplete plan, and JSON determinism.

### Step 6 — make the architecture contract explicit

Update the authoritative docs in the same change:

- [`docs/architecture/crate-boundaries.md`](/opt/tqsdk-rust/docs/architecture/crate-boundaries.md:25): ownership split—data owns catalog/plan/query primitives, task owns scheduling/backtest, core owns neutral commit mutation, cache owns operator CLI/fill, facade owns wiring only.
- [`docs/architecture/runtime-core/overview.md`](/opt/tqsdk-rust/docs/architecture/runtime-core/overview.md:1): replay-universe root, same-revision causality, `RuntimeReader + UpdateCursor` visibility, and no parallel state tree.
- [`docs/architecture/history-snapshot-manifest.md`](/opt/tqsdk-rust/docs/architecture/history-snapshot-manifest.md:94): distinguish service snapshot `catalog.complete` from an all-required-scope historical `CatalogSnapshot`; document fail-closed proof and pinning.
- [`docs/architecture/backtest-tick-cache-operations.md`](/opt/tqsdk-rust/docs/architecture/backtest-tick-cache-operations.md:1), [`docs/architecture/backtest-tick-cache-cli.md`](/opt/tqsdk-rust/docs/architecture/backtest-tick-cache-cli.md:1), root README, `crates/tqsdk/README.md`, `crates/tqsdk-cache/README.md`, and [`docs/architecture/validation.md`](/opt/tqsdk-rust/docs/architecture/validation.md:1): V1 inputs, static-selector compatibility, no remote inference/resume, cache fill/report workflow, and acceptance gates.

## 6. Cross-crate flow

```text
CatalogSnapshot + Calendar + DynamicUniverseScope + Budget
  -> tqsdk-data: validate + compile HistoricalUniversePlan
  -> tqsdk-data: plan physical cache slices / derived provenance / warmup
  -> tqsdk-cache: fill or report the exact plan (optional operator path)
  -> tqsdk facade: identity-check prepared plan
  -> tqsdk-task: schedule timestamp barriers and market events
  -> tqsdk-core: one ReplayStep commit under replay/<session>/universe
  -> RuntimeReader + UpdateCursor: strategy observes delta and snapshot
```

## 7. Acceptance scenarios

| Scenario | Required proof |
| --- | --- |
| Historical survivor bias | A contract delisted before plan creation is present during its validated lifecycle, not inferred from current metadata. |
| Join/leave atomics | At one timestamp, membership, bootstrap, and first quote produce exactly one revision/cursor commit. |
| Derived views | Continuous records source rebound segments; index retains stable identity; neither is silently converted to a physical symbol. |
| Warmup | New member starts `WarmingUp`, becomes `Ready` only after width; `RequireReady` strategy filter behaves deterministically. |
| Ordering | Member's first market event cannot fill a same-step new order; next market event may. |
| Removal/expiry | No new open after inactive/unknown status; retained state stays readable; unresolved positions/orders at expiry stop strictly. |
| Preflight | Missing/incomplete catalog, calendar mismatch, source gap, overbudget, stale plan identity all fail before replay; complete plan is reusable. |
| Compatibility | Current static selectors and `--universe` behave byte-for-byte/semantically as before; start-day-only fill remains intact. |
| Safety | Dynamic plan rejects resume/checkpoint; no live credentials or real-account tests are needed. |

## 8. Validation ladder (execute after implementation)

1. Format and targeted tests while each crate changes:

```bash
cargo fmt --all --check
cargo test -p tqsdk-data --test universe_selector
cargo test -p tqsdk-data --test backtest_history_api
cargo test -p tqsdk-core --test runtime_contract_replay_commit
cargo test -p tqsdk-core --test runtime_contract_batch_commit
cargo test -p tqsdk-task --test strategy_replay
cargo test -p tqsdk-task --test strategy_backtest
cargo test -p tqsdk-cache --test cli
```

2. Compile the new facade API contract and exercise its no-network `CacheOnly` fixture path; add an offline integration fixture with a historical catalog covering add/remove/rebind/warmup/expiry.

```bash
cargo check --examples
cargo test -p tqsdk
cargo test -p tqsdk-data -p tqsdk-core -p tqsdk-task -p tqsdk-cache
```

3. Full workspace/public surface gates from [`AGENTS.md`](/opt/tqsdk-rust/AGENTS.md:1):

```bash
cargo test
cargo clippy --examples --all-targets -- -D warnings
cargo check --no-default-features
cargo check --no-default-features --examples
cargo test -p tqsdk-session --no-default-features
cargo check --all-features --examples
cargo test --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
git diff --check
```

4. Before commit, run fresh GitNexus `detect-changes --scope all`; treat partial/truncated as failure, examine every affected flow, and run the impact/PDG checks from Step 0 again for edited symbols. No live cache fill is part of this plan; any remote fill requires separate explicit credentials/authorization.

## 9. Risks and controls

| Risk | Control |
| --- | --- |
| Survivorship bias / historical absence | Fail closed unless catalog proves scope/window completeness; no current-active inference. |
| Split-brain state or double callbacks | Core-owned replay mutation and one commit; reader/cursor only. |
| Incorrect same-time causality | Explicit timeline barrier precedes data inside one batch; test cursor/revision count. |
| Fake derived identity | Typed physical/continuous/index IDs and provenance; separate continuous rebound/index constituent rules. |
| Impossible pre-list warmup | Lifecycle-clipped history plus `WarmingUp` state, never fabricated prelisting rows. |
| Cache/replay divergence | Plan identity includes catalog/calendar/options/cache coverage; facade and CLI consume same plan. |
| Resume corruption | Explicit V1 rejection for dynamic plans. |
| Cost explosion | Mandatory library budget, visible CLI budget/report, deterministic incomplete report. |

## 10. Delivery slices and stop conditions

1. `tqsdk-data` model + validation + deterministic plan/tests must land before any task/runtime wiring.
2. Core atomic mutation/tests must land before scheduler integration.
3. Task scheduling/order/readiness tests must land before facade/CLI exposure.
4. Facade/CLI/docs contract must land only after offline end-to-end scenario passes.
5. Stop and redesign if a catalogue cannot prove the requested window, a core batch cannot express all state in one revision, or readiness requires prelisting history. Do not weaken semantics to make the test pass.

## 11. Deliberate compatibility and migration policy

No existing selector grammar changes. Existing `active:all`, `main:all`, `cont:all`, `index:all`, `symbol:`, `file:`, `BacktestBuilder::universe`, and cache `--universe` remain current/static. New types and flags are additive. Persisted plans carry a schema version and fail closed on unknown required fields. V1 has no migration path for checkpoints because dynamic checkpoint/resume is unsupported.

## 12. Evidence and planning limitations

The GitNexus wrapper produced no usable output here, so no impact depth, execution-flow grouping, or PDG slice could be verified. This is intentionally recorded as an unresolved planning limitation, not treated as low risk. Source evidence was verified directly at the cited paths; CodeGraph was used for initial symbol/call-path navigation. The implementation must refresh graph evidence before symbol edits and report any HIGH/CRITICAL/UNKNOWN result to the user before proceeding.

## 13. Machine-readable context pack

```yaml
schema_version: 1
task: historical-dynamic-universe-backtest
planning_mode: deep
baseline:
  head_commit: 675be2871d9d6861279e41a440ab8cb27ee35e40
  global_dirty_digest: sha256:526145c2eefac78d04f7c53fa5870aa51ac9f037a3c8d4b9f878109e41a9819d
  preexisting_dirty_paths:
    - crates/tqsdk-cache/src/main.rs
graph:
  codegraph_used: true
  gitnexus_cli: unavailable_or_unproven
  required_before_edits:
    - analyze_index_with_pdg
    - impact_each_target_symbol
    - inspect_unknown_or_high_risk
contracts:
  catalog: pinned_complete_user_supplied_only_v1
  timestamps: utc_epoch_nanoseconds
  commit_visibility: one_replaystep_revision_reader_cursor_only
  checkpoint_resume: rejected_for_dynamic_plan_v1
  cache_policy: deterministic_preflight_then_offline_run
  static_selector_compatibility: preserved
verification:
  offline_required: true
  live_credentials_required: false
  final_graph_gate: detect_changes_scope_all
```

## 14. Evidence provenance

```json
{"schema_version":2,"head_commit":"675be2871d9d6861279e41a440ab8cb27ee35e40","generated_plan_path":"docs/plans/2026-08-29-gitnexus-plan-dynamic-universe-backtest.md","global_dirty_digest":{"algorithm":"sha256","canonicalization":"gitnexus-evidence-provenance-v2 NUL-framed UTF-8 records","value":"526145c2eefac78d04f7c53fa5870aa51ac9f037a3c8d4b9f878109e41a9819d"},"cited_paths":28}
```

The complete canonical 28-path manifest was generated by `evidence-provenance.mjs` before publication; its fields are the source of record for the cited files. The compact record above deliberately avoids duplicating every per-path digest in the plan body.
