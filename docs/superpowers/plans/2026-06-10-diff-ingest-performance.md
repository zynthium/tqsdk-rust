# Diff Ingest Performance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Raise the practical throughput ceiling of the diff protocol handling layer while preserving the runtime contract: every visible state change still flows through `RuntimeHandle -> StateStore -> CommitResult -> RuntimeReader/UpdateCursor`.

**Architecture:** Keep optimization inside the existing crate ownership boundaries. `tqsdk-core` owns protocol decode, mutation normalization, state application, commit/change metadata, and cursor semantics. `tqsdk-wait` may add no-scan quote change helpers over existing commit touch metadata. `tqsdk-stream` may optimize fan-out only after benchmark evidence, without adding a second state tree or bypassing commits.

**Tech Stack:** Rust edition 2024, Cargo workspace, `serde_json`, existing runtime/session/wait/stream crates, ignored benchmark tests or release examples for repeatable local measurement.

---

## Current Baseline

Existing benchmark command:

```bash
cargo run -p tqsdk-core --example diff_ingest_microbench --release
```

Recent local baseline:

```text
parse_json_single_quote               20000          1          0         3955.9         3955.9
ingest_single_quote                   20000          1      20000        23796.7        23796.7
ingest_noop_single_quote              20000          1          0        12756.2        12756.2
parse_json_quote_batch                 1000        100          0       275653.0         2756.5
ingest_quote_batch                     1000        100       1000      1809209.0        18092.1
ingest_large_quote_batch                250       1000        250     22446813.7        22446.8
read_market_quote_typed               50000          1          0         1141.0         1141.0
```

Initial conclusion:

- JSON parsing is not the only bottleneck; ingestion adds roughly 6x over batch JSON parsing.
- Typed hot read is already low-cost relative to diff ingestion.
- The next gains should target mutation decode/allocation and state application, not reader-side micro-optimizations.

---

## Batch 0: Measurement Guardrails

### Task 0.1: Stabilize Core Benchmark Output

- [ ] Keep `crates/tqsdk-core/examples/diff_ingest_microbench.rs` as the core ingestion baseline.
- [ ] Add a short baseline section to `docs/performance-audit-handoff.md` after the first optimization batch, not before, to avoid documenting transient numbers.
- [ ] Preserve the existing scenarios:
  - single quote JSON parse
  - single quote ingest
  - noop quote ingest
  - 100 quote batch JSON parse
  - 100 quote batch ingest
  - 1000 quote batch ingest
  - typed quote read
- [ ] Add one benchmark scenario only if it directly guides an optimization decision:
  - repeated partial quote update with 3 fields
  - mixed quote + trade roots in one diff

Validation:

```bash
cargo fmt --all --check
cargo check -p tqsdk-core --example diff_ingest_microbench
cargo run -p tqsdk-core --example diff_ingest_microbench --release
git diff --check
```

### Task 0.2: Add Stream Fan-Out Benchmark Before Stream Changes

- [ ] Create `crates/tqsdk-stream/tests/stream_fanout_microbench.rs` as an ignored benchmark-style integration test.
- [ ] Reuse `crates/tqsdk-stream/tests/support/core_seed.rs` through `mod support;`.
- [ ] Measure at least these cases:
  - `quote_batches` with 1, 10, 100, and 500 consumers
  - path-specific quote streams with 100 and 500 symbol subscriptions
  - a slow consumer lag case that intentionally does not drain every commit
- [ ] Print elapsed time and delivery counts; do not assert machine-specific latency.
- [ ] Assert only semantic invariants:
  - every live consumer receives expected commit counts when drained
  - no consumer receives unmatched symbol paths
  - slow consumer behavior is explicit and documented by the test name

Validation:

```bash
cargo test -p tqsdk-stream --test stream_fanout_microbench -- --ignored --nocapture
cargo test -p tqsdk-stream --test stream_typed
```

Commit:

```bash
git add crates/tqsdk-stream/tests/stream_fanout_microbench.rs
gitnexus detect-changes
git commit -m "test(stream): add fanout performance benchmark"
```

---

## Batch 1: Core Low-Risk Allocation Reductions

### Task 1.1: Remove Recursive Path Clones From Generic Object Flattening

Target files:

- `crates/tqsdk-core/src/adapter/common.rs`
- `crates/tqsdk-core/tests/runtime_contract_adapters.rs`
- `crates/tqsdk-core/tests/performance_surface.rs`
- `crates/tqsdk-core/examples/diff_ingest_microbench.rs`

Required pre-edit analysis:

```bash
gitnexus impact flatten_object --direction upstream
gitnexus impact decode_json_value --direction upstream
```

Implementation:

- [ ] Change the recursive flattener from clone-per-branch path construction to push/pop path mutation.
- [ ] Keep the emitted `NormalizedMutation` ordering identical for existing fixtures.
- [ ] Keep null handling and nested object handling identical.
- [ ] Add a source-level performance guard in `crates/tqsdk-core/tests/performance_surface.rs` only for the specific regression pattern being removed.
- [ ] Run the core benchmark and compare `ingest_quote_batch` and `ingest_large_quote_batch`.

Expected win:

- Small to medium improvement on large nested market diffs.
- No public API or architecture changes.

Validation:

```bash
cargo fmt --all --check
cargo test -p tqsdk-core --test runtime_contract_adapters
cargo test -p tqsdk-core --test performance_surface
cargo run -p tqsdk-core --example diff_ingest_microbench --release
git diff --check
```

Commit:

```bash
git add crates/tqsdk-core/src/adapter/common.rs crates/tqsdk-core/tests/runtime_contract_adapters.rs crates/tqsdk-core/tests/performance_surface.rs crates/tqsdk-core/examples/diff_ingest_microbench.rs
gitnexus detect-changes
git commit -m "perf(core): reduce diff path allocation"
```

### Task 1.2: Avoid Value Clones When Building Commit Change Metadata

Target files:

- `crates/tqsdk-core/src/state/store.rs`
- `crates/tqsdk-core/src/state/changes.rs`
- `crates/tqsdk-core/tests/runtime_contract_commit_semantics.rs`
- `crates/tqsdk-core/tests/performance_surface.rs`
- `crates/tqsdk-core/examples/diff_ingest_microbench.rs`

Required pre-edit analysis:

```bash
gitnexus impact StateStore::apply_with --direction upstream
gitnexus impact ChangeSet::from_mutations --direction upstream
```

Implementation:

- [ ] Keep `NormalizedMutation` unchanged as the adapter output contract.
- [ ] Add an internal applied-change representation that records:
  - affected `StateRoot`
  - normalized object path
  - changed field names
  - operation kind when needed
- [ ] Build `ChangeSet` from applied-change metadata after the state tree has been updated.
- [ ] Remove unnecessary `serde_json::Value` clones from the state application path.
- [ ] Preserve deterministic change ordering.
- [ ] Preserve command lifecycle validation and mutation source guards.

Tests:

- [ ] Existing commit semantics tests still pass.
- [ ] Add or extend a test proving noop field updates do not appear as changed fields.
- [ ] Add or extend a test proving multi-field quote updates report all touched fields once.

Expected win:

- Medium improvement on large quote batches.
- Larger improvement on high-frequency partial updates.

Validation:

```bash
cargo fmt --all --check
cargo test -p tqsdk-core --test runtime_contract_commit_semantics
cargo test -p tqsdk-core --test runtime_contract_adapters
cargo test -p tqsdk-core --test performance_surface
cargo run -p tqsdk-core --example diff_ingest_microbench --release
git diff --check
```

Commit:

```bash
git add crates/tqsdk-core/src/state/store.rs crates/tqsdk-core/src/state/changes.rs crates/tqsdk-core/tests/runtime_contract_commit_semantics.rs crates/tqsdk-core/tests/performance_surface.rs crates/tqsdk-core/examples/diff_ingest_microbench.rs
gitnexus detect-changes
git commit -m "perf(core): avoid cloned values in change metadata"
```

### Task 1.3: Re-Measure Before Adding Specialized Decode Paths

- [ ] Re-run the core benchmark in release mode.
- [ ] Compare against the baseline in this plan.
- [ ] Record whether the remaining cost is still dominated by:
  - adapter decode
  - state apply
  - change set construction
  - commit broadcast
- [ ] Do not start Batch 2 unless Batch 1 leaves a clear ingestion hotspot.

Validation:

```bash
cargo run -p tqsdk-core --example diff_ingest_microbench --release
cargo check --workspace --examples
```

---

## Batch 2: Core Market Diff Fast Paths

### Task 2.1: Add Quote Diff Fast Path Behind the Existing Adapter Contract

Target files:

- `crates/tqsdk-core/src/adapter/common.rs`
- optionally `crates/tqsdk-core/src/adapter/market_diff.rs`
- `crates/tqsdk-core/tests/runtime_contract_adapters.rs`
- `crates/tqsdk-core/examples/diff_ingest_microbench.rs`

Required pre-edit analysis:

```bash
gitnexus impact MarketAdapter::decode --direction upstream
gitnexus impact decode_tq_diff --direction upstream
```

Implementation:

- [ ] Detect `rtn_data.quotes` objects and decode them directly into quote object mutations.
- [ ] Preserve the current normalized root/path semantics:
  - root: market state
  - object path: quote symbol path
  - fields: original quote fields
- [ ] Preserve deletion/null semantics exactly.
- [ ] Fall back to generic flattening for all unrecognized roots and shapes.
- [ ] Add fixture tests comparing generic decode output and fast path output for representative quote payloads.
- [ ] Keep the fast path private to core adapters.

Expected win:

- Medium to high improvement on `ingest_quote_batch`.
- No public API change.

Validation:

```bash
cargo fmt --all --check
cargo test -p tqsdk-core --test runtime_contract_adapters
cargo run -p tqsdk-core --example diff_ingest_microbench --release
git diff --check
```

Commit:

```bash
git add crates/tqsdk-core/src/adapter/common.rs crates/tqsdk-core/src/adapter/market_diff.rs crates/tqsdk-core/tests/runtime_contract_adapters.rs crates/tqsdk-core/examples/diff_ingest_microbench.rs
gitnexus detect-changes
git commit -m "perf(core): add quote diff fast path"
```

### Task 2.2: Add Tick and Kline Row Fast Paths Only After Quote Fast Path Proves Useful

Target files:

- `crates/tqsdk-core/src/adapter/common.rs`
- optionally `crates/tqsdk-core/src/adapter/market_diff.rs`
- `crates/tqsdk-core/tests/runtime_contract_adapters.rs`
- `crates/tqsdk-core/examples/diff_ingest_microbench.rs`

Required pre-edit analysis:

```bash
gitnexus impact decode_live_serial_data --direction upstream
gitnexus impact decode_tq_diff --direction upstream
```

Implementation:

- [ ] Add fast paths for live serial rows only if benchmark fixtures show row decode is significant.
- [ ] Preserve row id/path injection behavior.
- [ ] Preserve tick/kline object shape consumed by typed readers.
- [ ] Add fixture tests for:
  - one tick row
  - one kline row
  - mixed update with quote and serial data
- [ ] Do not optimize historical/direct-query parsing here; keep scope to diff ingestion.

Validation:

```bash
cargo fmt --all --check
cargo test -p tqsdk-core --test runtime_contract_adapters
cargo run -p tqsdk-core --example diff_ingest_microbench --release
cargo check --workspace --examples
git diff --check
```

Commit:

```bash
git add crates/tqsdk-core/src/adapter/common.rs crates/tqsdk-core/src/adapter/market_diff.rs crates/tqsdk-core/tests/runtime_contract_adapters.rs crates/tqsdk-core/examples/diff_ingest_microbench.rs
gitnexus detect-changes
git commit -m "perf(core): add live serial diff fast paths"
```

---

## Batch 3: Wait Facade No-Scan Changed Quote API

### Task 3.1: Add Private Changed Quote Symbol Extraction

Target files:

- `crates/tqsdk-wait/src/change.rs`
- optionally `crates/tqsdk-wait/src/quote_changes.rs`
- `crates/tqsdk-wait/tests/wait_api_market.rs`

Required pre-edit analysis:

```bash
gitnexus impact WaitStep --direction upstream
gitnexus impact QuoteSet --direction upstream
```

Implementation:

- [ ] Add a private helper that reads quote changes from the commit touch/change metadata.
- [ ] Avoid scanning all subscribed symbols.
- [ ] Return deterministic symbol ordering.
- [ ] Keep helper independent from direct-query/session APIs.
- [ ] Add tests seeded through `api.session().handle().ingest(...)`.

Tests:

- [ ] One changed quote returns one symbol.
- [ ] Two changed quotes return both symbols in deterministic order.
- [ ] Trade-only or account-only diff returns no quote symbols.
- [ ] Noop quote update returns no changed quote symbol if the core change set marks it noop.

Validation:

```bash
cargo fmt --all --check
cargo test -p tqsdk-wait --test wait_api_market
git diff --check
```

Commit:

```bash
git add crates/tqsdk-wait/src/change.rs crates/tqsdk-wait/src/quote_changes.rs crates/tqsdk-wait/tests/wait_api_market.rs
gitnexus detect-changes
git commit -m "feat(wait): extract changed quote symbols from commits"
```

### Task 3.2: Expose `WaitStep::changed_quote_symbols()`

Target files:

- `crates/tqsdk-wait/src/step.rs`
- `crates/tqsdk-wait/tests/wait_api_market.rs`
- `crates/tqsdk-wait/README.md`
- `docs/architecture/api-wait.md` if the public API contract changes

Required pre-edit analysis:

```bash
gitnexus impact WaitStep::changed_paths --direction upstream
gitnexus impact WaitStep --direction upstream
```

Implementation:

- [ ] Add `WaitStep::changed_quote_symbols() -> impl Iterator<Item = &str>` or the closest existing ergonomic style used by `WaitStep`.
- [ ] Keep lifetimes tied to `WaitStep`; avoid allocating unless the existing `WaitStep` API style requires owned values.
- [ ] Do not expose raw runtime path internals.
- [ ] Document that the method reports symbols touched in the last wait step, not all subscribed symbols.

Tests:

- [ ] Verify the method can be called after `wait_update()`.
- [ ] Verify empty result on timeout/no commit.
- [ ] Verify repeated call on the same step is stable.

Validation:

```bash
cargo fmt --all --check
cargo test -p tqsdk-wait --test wait_api_market
cargo check --workspace --examples
git diff --check
```

Commit:

```bash
git add crates/tqsdk-wait/src/step.rs crates/tqsdk-wait/tests/wait_api_market.rs crates/tqsdk-wait/README.md docs/architecture/api-wait.md
gitnexus detect-changes
git commit -m "feat(wait): expose changed quote symbols"
```

### Task 3.3: Expose `QuoteSet::changed()` and `QuoteSet::changed_snapshots()`

Target files:

- `crates/tqsdk-wait/src/refs/quote.rs`
- `crates/tqsdk-wait/tests/wait_api_market.rs`
- `crates/tqsdk-wait/examples/api_contract_s34_batch_quote_subscription.rs`
- `crates/tqsdk-wait/README.md`
- `docs/architecture/api-wait.md` if public API contract changes

Required pre-edit analysis:

```bash
gitnexus impact QuoteSet::snapshots --direction upstream
gitnexus impact QuoteSet --direction upstream
```

Implementation:

- [ ] Add `QuoteSet::changed(&WaitStep)` returning changed quote handles/references for symbols that belong to the set.
- [ ] Add `QuoteSet::changed_snapshots(&WaitStep)` returning typed snapshots for those changed symbols.
- [ ] Use the changed symbol set from `WaitStep`; do not scan the entire subscription set as the primary path.
- [ ] For membership checks, use the existing `QuoteSet` internal representation if it already supports efficient lookup; otherwise add a small internal set/index without changing public semantics.
- [ ] Keep all direct-query APIs out of `tqsdk-wait`.

Tests:

- [ ] A `QuoteSet` with 100 subscribed symbols and 1 changed quote returns 1 changed quote.
- [ ] Changed quote outside the set is ignored.
- [ ] Multiple changed symbols preserve deterministic order.
- [ ] `changed_snapshots` returns the latest typed values.
- [ ] Existing batch quote subscription example still compiles.

Validation:

```bash
cargo fmt --all --check
cargo test -p tqsdk-wait --test wait_api_market
cargo check -p tqsdk-wait --example api_contract_s34_batch_quote_subscription
cargo check --workspace --examples
git diff --check
```

Commit:

```bash
git add crates/tqsdk-wait/src/refs/quote.rs crates/tqsdk-wait/tests/wait_api_market.rs crates/tqsdk-wait/examples/api_contract_s34_batch_quote_subscription.rs crates/tqsdk-wait/README.md docs/architecture/api-wait.md
gitnexus detect-changes
git commit -m "feat(wait): add no-scan changed quote iteration"
```

---

## Batch 4: Stream Optimizations Only If Benchmarks Justify Them

### Task 4.1: Analyze Stream Fan-Out Benchmark Results

- [ ] Run the ignored stream fan-out benchmark from Batch 0.
- [ ] Compare:
  - total dispatch time by consumer count
  - delivery count correctness
  - lag behavior
- [ ] Stop here if dispatch overhead is not a top bottleneck.
- [ ] Document the result in the final performance note.

Validation:

```bash
cargo test -p tqsdk-stream --test stream_fanout_microbench -- --ignored --nocapture
```

### Task 4.2: Add Indexed Path Dispatch If Fan-Out Scan Is the Bottleneck

Target files:

- `crates/tqsdk-stream/src/path_dispatcher.rs`
- `crates/tqsdk-stream/src/filter.rs`
- `crates/tqsdk-stream/tests/stream_typed.rs`
- `crates/tqsdk-stream/tests/stream_fanout_microbench.rs`
- `crates/tqsdk-stream/tests/performance_surface.rs`

Required pre-edit analysis:

```bash
gitnexus impact PathDispatcher --direction upstream
gitnexus impact PathMatcher --direction upstream
```

Implementation:

- [ ] Keep raw commit stream behavior unchanged.
- [ ] Index subscribers by root segment first.
- [ ] For market quote paths, optionally index by symbol segment if benchmark data shows root-only indexing is insufficient.
- [ ] Keep a fallback generic matcher list for non-indexable paths.
- [ ] Ensure unsubscribe/drop cleanup removes index entries.
- [ ] Preserve delivery order per consumer.

Tests:

- [ ] Existing stream typed tests still pass.
- [ ] Path-specific stream receives matching paths only.
- [ ] Generic path matcher still works.
- [ ] Dropped consumers are removed from the dispatcher index.
- [ ] Fan-out benchmark shows improvement at 100 and 500 consumers.

Validation:

```bash
cargo fmt --all --check
cargo test -p tqsdk-stream --test stream_typed
cargo test -p tqsdk-stream --test performance_surface
cargo test -p tqsdk-stream --test stream_fanout_microbench -- --ignored --nocapture
cargo check --workspace --examples
git diff --check
```

Commit:

```bash
git add crates/tqsdk-stream/src/path_dispatcher.rs crates/tqsdk-stream/src/filter.rs crates/tqsdk-stream/tests/stream_typed.rs crates/tqsdk-stream/tests/stream_fanout_microbench.rs crates/tqsdk-stream/tests/performance_surface.rs
gitnexus detect-changes
git commit -m "perf(stream): index path dispatch subscriptions"
```

### Task 4.3: Treat Lossy Slow-Consumer Mode as a Separate Design Change

- [ ] Do not add lossy semantics as part of hidden performance tuning.
- [ ] If slow consumers are the measured bottleneck, first write a short design update covering:
  - whether the stream is exact or latest-only
  - how lag is surfaced
  - whether commit revision continuity is preserved
  - how this affects `RuntimeReader` retention
- [ ] Update architecture docs before implementation if semantics change.
- [ ] Keep the default stream exact unless the user explicitly opts into lossy/latest-only behavior.

Validation before implementation:

```bash
cargo fmt --all --check
git diff --check
```

---

## Batch 5: Full Validation and Performance Report

### Task 5.1: Workspace Validation

Run after each batch that changes Rust code:

```bash
cargo fmt --all --check
cargo check --workspace --examples
cargo test --workspace
cargo clippy --workspace --examples --all-targets -- -D warnings
git diff --check
```

Run additionally if public API, feature flags, or crate boundaries changed:

```bash
cargo check --workspace --no-default-features
cargo check --workspace --no-default-features --examples
cargo test -p tqsdk-session --no-default-features
cargo check --workspace --all-features --examples
```

### Task 5.2: Before/After Performance Report

Target files:

- `docs/performance-audit-handoff.md`
- optionally `docs/reviews/diff-ingest-performance-2026-06.md`

Report contents:

- [ ] Baseline numbers from this plan.
- [ ] Final numbers after each implemented batch.
- [ ] Percentage change for:
  - `ingest_single_quote`
  - `ingest_noop_single_quote`
  - `ingest_quote_batch`
  - `ingest_large_quote_batch`
  - `read_market_quote_typed`
- [ ] Stream fan-out numbers if stream work was performed.
- [ ] Explicit statement of whether the layer is now CPU-bound on JSON parse, state apply, or fan-out.
- [ ] Residual risks and future work.

Validation:

```bash
git diff --check
```

Commit:

```bash
git add docs/performance-audit-handoff.md docs/reviews/diff-ingest-performance-2026-06.md
gitnexus detect-changes
git commit -m "docs: record diff ingest performance results"
```

---

## Stop Criteria

Stop the optimization pass when any of these is true:

- Core ingestion cost is within 2x of JSON parse cost for quote batches.
- Further improvement requires public API or runtime contract changes.
- Benchmark wins are below 5% and add meaningful complexity.
- Stream slow-consumer semantics require a product decision.

---

## Risk Notes

- `StateStore` and `ChangeSet` changes are the highest-risk core edits because they affect all facades.
- Specialized market fast paths must be differential-tested against generic decode behavior.
- Wait no-scan APIs are public API additions and require README/API contract updates.
- Stream indexing must not change exact delivery semantics or cursor retention behavior.
- Lossy/latest-only stream behavior is not a performance tweak; it is a separate semantic feature.
