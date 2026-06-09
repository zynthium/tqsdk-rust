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

Semantic gate:

- [ ] Write and pass golden behavior tests before changing the implementation.
- [ ] Prove repeated identical quote diffs still return no commit and do not advance revision.
- [ ] Prove `ChangeSet` is built only from actually applied field changes, never from decoded-but-noop input mutations.
- [ ] Prove `path_hits`, `object_hits`, and `field_hits` ordering stays identical for representative quote, kline/tick live-serial, and trade scalar-leaf payloads.
- [ ] Prove the write-side returned `SharedCommitResult` and the commit-log/cursor-visible commit still describe the same revision and same change metadata.
- [ ] Do not change the public shape of `CommitResult`, `ChangeSet`, `ChangeHit`, `NormalizedMutation`, or `FieldMutation` in this task.
- [ ] Do not change revision advancement semantics: a revision advances only when at least one field value actually changes in the state tree.

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
- [ ] Add or extend a test proving field-hit order remains deterministic after replacing cloned applied mutations with applied-change metadata.

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
gitnexus impact decode_io_payload --direction upstream
gitnexus impact decode_json_envelope --direction upstream
```

Fast path equivalence gate:

- [ ] Write and pass golden adapter tests against the current generic flattening output before adding the quote fast path.
- [ ] Prove quote fast path output preserves exact `NormalizedMutation` path, object key, source, field names, field values, and field ordering for representative payloads.
- [ ] Prove mixed `rtn_data` array ordering is unchanged when quote payloads appear before, after, or between non-quote payloads.
- [ ] Prove unrecognized quote-like shapes fall back to generic flattening instead of being silently dropped or normalized differently.
- [ ] Prove null values remain field updates with `Value::Null` where the current generic path treats them that way.
- [ ] Do not widen the public adapter API or export the fast path from `tqsdk-core`.
- [ ] Continue only if the release benchmark shows a meaningful win for `ingest_quote_batch`; otherwise keep the golden tests and skip the fast path implementation.

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
gitnexus impact MarketAdapter::decode --direction upstream
gitnexus impact decode_json_value --direction upstream
gitnexus impact flatten_object --direction upstream
```

Fast path equivalence gate:

- [ ] Do not start tick/kline fast paths until the quote fast path is implemented, benchmarked, and shown to justify the extra specialized decoder surface.
- [ ] Write and pass golden adapter tests against the current generic flattening output before adding any live-serial fast path.
- [ ] Prove kline fast path preserves parent series mutations such as `last_id` as well as row mutations under `data/{row_id}`.
- [ ] Prove kline fast path preserves multi-contract binding scalar leaves under `binding/{secondary}/{primary_id}` exactly as generic flattening does.
- [ ] Prove tick fast path preserves parent series mutations such as `last_id` as well as row mutations under `data/{row_id}`.
- [ ] Prove row id injection remains identical to `inject_market_data_row_id`: add `id` only when absent and keep sorted field order after injection.
- [ ] Prove object key inference remains identical for both legacy row paths and live-serial `data/{row_id}` paths.
- [ ] Prove mixed quote + kline + kline binding + tick payloads preserve current mutation ordering.
- [ ] Prove malformed/non-numeric row ids keep the current generic behavior, including when object key inference returns `None`.
- [ ] Do not optimize historical/direct-query parsing, chart lifecycle, or typed reader projection in this task.

Implementation:

- [ ] Add fast paths for live serial rows only if benchmark fixtures show row decode is significant.
- [ ] Preserve row id/path injection behavior.
- [ ] Preserve tick/kline object shape consumed by typed readers.
- [ ] Add fixture tests for:
  - one tick row
  - one kline row
  - one multi-contract kline payload containing primary rows, `binding/{secondary}/{primary_id}`, and secondary rows
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

Public API semantic gate:

- [ ] Treat this batch as a public API contract change, not as an internal performance-only change.
- [ ] Update `docs/architecture/api-wait.md`; this is required, not conditional.
- [ ] Update `crates/tqsdk-wait/README.md`; this is required, not conditional.
- [ ] Update `crates/tqsdk-wait/examples/api_contract_s34_batch_quote_subscription.rs` so the public contract demonstrates the no-scan changed quote path.
- [ ] The API must explain only the current `WaitStep` commit; it must not maintain facade-private quote revisions, per-symbol epochs, or a second changed-symbol cache.
- [ ] `WaitStep::changed_quote_symbols()` returns deduplicated changed quote symbols in current `ChangeSet` first-hit order.
- [ ] `QuoteSet::changed(&WaitStep)` and `QuoteSet::changed_snapshots(&WaitStep)` return only members of that `QuoteSet`, in the same deterministic symbol order exposed by `QuoteSet::symbols()`.
- [ ] `QuoteSet::changed*` may iterate the current step's changed symbols and perform membership lookups, but must not scan all subscribed symbols as the primary path.
- [ ] Empty/no-commit steps, unrelated roots, trade-only commits, and core-level noop commits must return empty results.
- [ ] The implementation must not add direct-query, schema, metadata, or session request/response helpers to `tqsdk-wait`.

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
- `docs/architecture/api-wait.md`

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
- [ ] Verify ordering follows current `ChangeSet` first-hit order and deduplicates repeated quote hits.

Validation:

```bash
cargo fmt --all --check
cargo test -p tqsdk-wait --test wait_api_market
cargo test -p tqsdk-wait
cargo check -p tqsdk-wait --examples
cargo check --workspace --examples
cargo test --workspace
cargo check --workspace --no-default-features
cargo check --workspace --no-default-features --examples
cargo check --workspace --all-features --examples
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
- `docs/architecture/api-wait.md`

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
- [ ] Multiple changed symbols preserve `QuoteSet::symbols()` deterministic symbol order.
- [ ] `changed_snapshots` returns the latest typed values.
- [ ] Existing batch quote subscription example still compiles.
- [ ] Source-level performance guard proves `QuoteSet::changed*` does not iterate `self.quotes.values()` or `self.quotes.iter()` as its primary changed-symbol path.

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

Stream dispatcher semantic gate:

- [ ] Keep raw `commit_stream()` behavior unchanged; this task may only change internal path-backed dispatch.
- [ ] Indexed dispatch may narrow matching `DriverEvent::Commit` delivery, but `DriverEvent::Error`, `DriverEvent::Lagged`, and `DriverEvent::Closed` must continue to be delivered to every live path subscriber.
- [ ] Preserve per-consumer delivery order for commits and driver events.
- [ ] Preserve subscribe-after-start behavior: new path subscribers must attach to the existing dispatcher task without restarting the driver.
- [ ] Preserve abort/closed behavior: aborting the dispatcher must notify all live path subscribers and mark the dispatcher closed.
- [ ] Preserve slow-consumer lag behavior: lag remains visible as `Lagged`, not as silent skipped commits or stream termination.
- [ ] Cleanup must remove dropped subscribers from every index without relying on implicit async drop side effects.
- [ ] Benchmarks may demonstrate improvement, but semantic tests are required and cannot be replaced by benchmark output.

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
- [ ] Error events are delivered to all live path subscribers regardless of path filters.
- [ ] Lagged events are delivered to all live path subscribers regardless of path filters.
- [ ] Closed events are delivered to all live path subscribers regardless of path filters.
- [ ] Subscribe-after-start reuses the existing dispatcher task and preserves delivery.
- [ ] Abort notifies all live path subscribers and prevents new subscriptions.
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

---

## Engineering Review Addendum

This addendum records the `/plan-eng-review` hard gates added to keep the full plan intact while preventing performance work from changing business semantics.

### What Already Exists

- Core benchmark baseline: `crates/tqsdk-core/examples/diff_ingest_microbench.rs` already measures parse, ingest, noop ingest, batch ingest, large batch ingest, and typed quote reads.
- Generic DIFF decoding: `crates/tqsdk-core/src/adapter/common.rs` already owns JSON envelope flattening, field sorting, object-key inference, scalar-leaf handling, and kline/tick row id injection.
- Applied-change semantics: `StateStore::apply_with` already filters noop updates before `ChangeSet::from_mutations` sees them.
- Runtime commit contract tests: `crates/tqsdk-core/tests/runtime_contract_commit_semantics.rs` already proves repeated identical quote diffs do not produce commits.
- Wait changed-object model: `WaitStep::is_changing`, `is_changing_fields`, `QuoteRef::changed_snapshot`, and `ChangeTrackedRef` already consume `ChangeSet` without private facade revisions.
- Wait batch quote storage: `QuoteSet` already stores refs in a symbol-indexed `BTreeMap`, giving deterministic symbol order.
- Wait multi-contract K-line model: `TqApi::kline_multi` uses one shared chart, and `MultiKlineHandle` reads `klines/{primary}/{duration}/binding/{secondary}/{primary_id}` to align secondary rows.
- Stream path dispatch: `PathDispatcher` and `PathMatcher` already share one root receiver for path-backed streams and precompile path filters by root.
- Stream performance guards: `crates/tqsdk-stream/tests/performance_surface.rs` already guards against rebuilding raw path filters in the commit hot path.

### NOT In Scope

- Changing `RuntimeHandle`, `RuntimeReader`, `UpdateCursor`, `CommitResult`, `ChangeSet`, `ChangeHit`, `NormalizedMutation`, or `FieldMutation` public shapes.
- Changing revision advancement semantics or allowing noop diffs to produce commits.
- Adding a second state tree, facade-private quote revision, per-symbol epoch, or local order overlay.
- Moving direct query, schema, metadata, calendar, or request/response helpers into `tqsdk-wait` or `tqsdk-stream`.
- Changing chart lifecycle, typed reader projection, historical query parsing, or `tqsdk-data` cache behavior as part of market fast paths.
- Adding lossy/latest-only stream behavior to the default stream semantics.
- Making `tqsdk-stream` the default answer for single-owner quote throughput; wait remains the single-owner path.
- Requiring full crate-wide stream example validation for Batch 4 in this plan; the accepted scope keeps Batch 4 validation focused on stream typed/performance/benchmark tests plus workspace examples.

### Data Flow Diagram

```text
DIFF JSON / RuntimeInput
        |
        v
ProtocolAdapter decode
        |
        |  NormalizedMutation[]
        v
StateStore::apply_with
        |
        |  applied field changes only
        v
ChangeSet
        |
        v
CommitResult / SharedCommitResult
        |
        +--> RuntimeReader / UpdateCursor
        |
        +--> tqsdk-wait WaitStep
        |       |
        |       +--> is_changing / changed_quote_symbols / QuoteSet::changed*
        |
        +--> tqsdk-stream CommitStream / PathDispatcher
                |
                +--> matching commits by path index
                +--> Error / Lagged / Closed broadcast to all live path subscribers
```

### Test Coverage Diagram

```text
CODE PATHS                                           STATUS
Batch 1.1 flatten_object push/pop
  ├── field sorting unchanged                        [planned]
  ├── scalar leaf unchanged                          [planned]
  └── mutation order unchanged                       [planned]

Batch 1.2 StateStore applied metadata
  ├── noop returns no commit                         [gate added]
  ├── revision advances only on real field change    [gate added]
  ├── ChangeSet from applied changes only            [gate added]
  └── returned/logged commit metadata equivalent     [gate added]

Batch 2 market fast path
  ├── quote fast path equals generic output          [gate added]
  ├── mixed rtn_data order                           [gate added]
  ├── kline/tick parent + row mutations              [gate added]
  ├── multi-contract kline binding mutations         [gate added]
  └── malformed row id fallback                      [gate added]

Batch 3 wait public API
  ├── WaitStep changed symbols ordering              [gate added]
  ├── QuoteSet changed order                         [gate added]
  ├── no-scan source guard                           [planned]
  └── public API validation matrix                   [strengthened]

Batch 4 stream dispatcher
  ├── matching commit dispatch                       [planned]
  ├── Error/Lagged/Closed to all subscribers          [gate added]
  ├── subscribe-after-start                          [gate added]
  └── crate-wide stream examples                     [not required in accepted scope]
```

### Failure Modes

| Codepath | Realistic failure mode | Test coverage | Error handling / user visibility |
| --- | --- | --- | --- |
| `StateStore::apply_with` applied metadata | Decoded-but-noop input appears in `ChangeSet`, causing false `is_changing` and spurious revisions | Batch 1.2 semantic gate | Would be user-visible as false quote/order changes; gate blocks it |
| `ChangeSet` ordering | Field/path/object order changes, making changed quote iteration unstable | Batch 1.2 ordering tests | User-visible ordering drift; deterministic tests block it |
| Quote fast path | Fast path emits different `NormalizedMutation` than generic flattening | Batch 2.1 equivalence gate | Silent state drift; golden tests block it |
| Tick/kline fast path | Row id injection or parent `last_id` mutation is lost | Batch 2.2 equivalence gate | Typed row readers can miss or mislabel rows; golden tests block it |
| Multi-contract kline fast path | `binding/{secondary}/{primary_id}` mutations are dropped or reordered | Batch 2.2 binding equivalence gate | `MultiKlineHandle` can return empty, incomplete, or misaligned rows; golden tests block it |
| Wait changed quote API | API scans all subscribed quotes or returns unstable ordering | Batch 3 semantic gate and source guard | User sees performance cliff or non-deterministic iteration; contract tests block it |
| Stream indexed dispatcher | `Error`, `Lagged`, or `Closed` only reaches indexed matching paths | Batch 4 semantic gate | Silent stream hang or missed lag diagnostics; dispatcher tests block it |
| Slow consumer pressure | Optimization hides lag instead of surfacing `Lagged` | Batch 4 semantic gate plus separate lossy-design rule | User loses commit continuity silently; lossy mode requires separate design |

### Worktree Parallelization Strategy

| Step | Modules touched | Depends on |
| --- | --- | --- |
| Batch 0 core benchmark stabilization | `crates/tqsdk-core/examples`, docs | - |
| Batch 0 stream fan-out benchmark | `crates/tqsdk-stream/tests` | - |
| Batch 1 core allocation reductions | `crates/tqsdk-core/src`, `crates/tqsdk-core/tests` | Batch 0 core baseline |
| Batch 2 market fast paths | `crates/tqsdk-core/src`, `crates/tqsdk-core/tests` | Batch 1.3 re-measure |
| Batch 3 wait changed quote API | `crates/tqsdk-wait`, `docs/architecture/api-wait.md` | Batch 1.2 applied-change semantics stable |
| Batch 4 stream indexing | `crates/tqsdk-stream/src`, `crates/tqsdk-stream/tests` | Batch 0 stream fan-out benchmark |
| Batch 5 performance report | docs | All implemented batches |

Parallel lanes:

- Lane A: Batch 0 core benchmark -> Batch 1 -> Batch 2. Sequential because core adapter/state tests and benchmark interpretation share modules.
- Lane B: Batch 0 stream fan-out -> Batch 4. Independent from core implementation, but must wait for benchmark evidence before indexing.
- Lane C: Batch 3 wait API. Can proceed after Batch 1.2 semantics are locked; it should not run in parallel with Batch 1.2 if `ChangeSet` behavior is still moving.
- Lane D: Batch 5 report. Runs after the implemented lanes merge.

Execution order:

1. Launch Lane A Batch 0 and Lane B Batch 0 in parallel worktrees.
2. Merge benchmark-only work first.
3. Run Batch 1 in Lane A.
4. After Batch 1.2 lands, Batch 2 and Batch 3 can run in separate worktrees if they avoid overlapping core files.
5. Run Batch 4 only if stream fan-out benchmark justifies it.
6. Run Batch 5 after all selected implementation batches merge.

Conflict flags:

- Batch 1 and Batch 2 both touch `crates/tqsdk-core/src/adapter` or adjacent core tests; run sequentially.
- Batch 3 depends on stable `ChangeSet` behavior; do not implement it against an unmerged Batch 1.2 branch.
- Batch 4 touches stream internals only and can stay independent from core/wait work.

### Implementation Tasks

Synthesized from this review's findings. Each task derives from a specific finding above. Run with Codex or another agentic worker; checkbox as you ship.

- [ ] **T1 (P1, human: ~2h / CC: ~20min)** - core state - Lock applied-change semantics before removing cloned values
  - Surfaced by: Architecture Issue 1 - Batch 1.2 could turn decoded-but-noop mutations into changed metadata
  - Files: `crates/tqsdk-core/src/state/store.rs`, `crates/tqsdk-core/src/state/changes.rs`, `crates/tqsdk-core/tests/runtime_contract_commit_semantics.rs`
  - Verify: `cargo test -p tqsdk-core --test runtime_contract_commit_semantics`
- [ ] **T2 (P1, human: ~2h / CC: ~25min)** - core adapter - Add differential fast-path tests before specialized market decode
  - Surfaced by: Architecture Issue 2 - Fast paths can diverge from generic flattening behavior
  - Files: `crates/tqsdk-core/src/adapter/common.rs`, `crates/tqsdk-core/tests/runtime_contract_adapters.rs`
  - Verify: `cargo test -p tqsdk-core --test runtime_contract_adapters`
- [ ] **T3 (P1, human: ~2h / CC: ~20min)** - wait API - Treat changed quote iteration as a public API contract
  - Surfaced by: Architecture Issue 3 - Wait changed quote APIs need fixed ordering and required docs
  - Files: `crates/tqsdk-wait/src/step.rs`, `crates/tqsdk-wait/src/refs/quote.rs`, `crates/tqsdk-wait/README.md`, `docs/architecture/api-wait.md`
  - Verify: `cargo test -p tqsdk-wait && cargo check -p tqsdk-wait --examples`
- [ ] **T4 (P1, human: ~2h / CC: ~20min)** - stream dispatcher - Preserve non-commit driver event broadcast under path indexing
  - Surfaced by: Architecture Issue 4 - Indexed dispatch could drop Error/Lagged/Closed for non-matching subscribers
  - Files: `crates/tqsdk-stream/src/path_dispatcher.rs`, `crates/tqsdk-stream/src/filter.rs`, `crates/tqsdk-stream/tests/stream_typed.rs`
  - Verify: `cargo test -p tqsdk-stream --test stream_typed`
- [ ] **T5 (P2, human: ~30min / CC: ~5min)** - validation - Use public API validation matrix for Batch 3
  - Surfaced by: Test Issue 1 - Wait public API changes need stronger validation than a single example check
  - Files: `docs/superpowers/plans/2026-06-10-diff-ingest-performance.md`
  - Verify: `cargo test -p tqsdk-wait && cargo check --workspace --examples`

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| CEO Review | `/plan-ceo-review` | Scope & strategy | 0 | - | - |
| Codex Review | `/codex review` | Independent 2nd opinion | 0 | - | - |
| Eng Review | `/plan-eng-review` | Architecture & tests (required) | 1 | CLEAR | 6 issues found, 0 critical gaps; gates added for core semantics, fast-path equivalence, wait public API, stream dispatcher semantics, and Batch 3 validation |
| Design Review | `/plan-design-review` | UI/UX gaps | 0 | - | - |
| DX Review | `/plan-devex-review` | Developer experience gaps | 0 | - | - |

- **UNRESOLVED:** 0
- **VERDICT:** ENG CLEARED - ready to implement the plan with the gates above.
