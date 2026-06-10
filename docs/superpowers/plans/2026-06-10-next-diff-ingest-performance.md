# Next Diff Ingest Tail Latency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cleanly measure and then reduce the remaining DIFF ingest allocation cost and p95/p99 latency risk after the 2026-06 diff ingest pass.

**Architecture:** Keep all visible state changes on the existing `RuntimeHandle -> StateStore -> CommitResult -> RuntimeReader/UpdateCursor` path. Batches 0-3 must not change public `ChangeSet`, `CommitResult`, `NormalizedMutation`, or runtime sequencing semantics. Any grouped `ChangeSet` public shape or runtime actor/sequencer change is a separate architecture-gated batch with documentation updates.

**Tech Stack:** Rust edition 2024, existing `tqsdk-core` runtime/adapter/state modules, `serde_json`, ignored benchmark-style tests or release examples, GitNexus impact checks before symbol edits.

---

## Inputs

This plan combines:

- `性能审查.md` external review.
- `docs/performance-audit-handoff.md` final results from the previous optimization pass.
- Current code in `crates/tqsdk-core/examples/diff_ingest_microbench.rs`, `crates/tqsdk-core/src/state/{store.rs,changes.rs}`, `crates/tqsdk-core/src/runtime/{handle.rs,commit_engine.rs}`.
- Current architecture boundaries in `docs/architecture/ai-workflow.md` and `docs/architecture/README.md`.

Accepted review findings:

- The current benchmark constructs quote payloads inside timed ingest loops, so the numbers mix test-data construction with runtime ingest.
- The remaining cost is likely allocation-heavy metadata/state application, not `aid` detection or typed reads.
- `ChangeSet` currently duplicates path/object/field metadata in `field_hits`.
- `StateStore` still clones path segments and field values on every changed field.
- `RuntimeHandle::ingest()` holds the `RuntimeCore` mutex from adapter decode through apply and publish, which can create command/trade tail latency during large market batches.
- Repeated mutation scans and per-symbol field sorting are secondary cleanup targets.

Rejected or deferred for this plan:

- Do not rewrite the runtime into an actor in the first implementation batch.
- Do not add a second state tree or facade-private quote cache.
- Do not make stream delivery lossy or latest-only as a hidden performance change.
- Do not change public `ChangeSet` fields until an architecture-gated batch updates docs and examples.

---

## Current Baseline

Previous final benchmark command:

```bash
cargo run -p tqsdk-core --example diff_ingest_microbench --release
```

Previous final results:

| case | final ns/iter |
| --- | ---: |
| `parse_json_single_quote` | 4088.9 |
| `ingest_single_quote` | 16909.8 |
| `ingest_noop_single_quote` | 5609.8 |
| `parse_json_quote_batch` | 219941.5 |
| `ingest_quote_batch` | 1110584.5 |
| `ingest_large_quote_batch` | 10223138.1 |
| `read_market_quote_typed` | 1023.3 |

Known measurement issue:

`run_ingest_case` currently calls `market_input(quote_rtn_data(symbols, sequence))` inside the measured loop. The next version must publish clean numbers before claiming any new performance gain.

---

## File Structure

Planned files:

- Modify `crates/tqsdk-core/examples/diff_ingest_microbench.rs`
  - Own clean release microbench cases.
  - Pre-generate `RuntimeInput` values outside timed loops.
  - Add sparse update and wire-text cases.
  - Add decode-only cases with public `AdapterRegistry::decode_input`.
- Modify `crates/tqsdk-core/tests/performance_surface.rs`
  - Add source guards preventing benchmark payload construction from re-entering timed loops.
  - Add guards for `ChangeSet` construction patterns removed in this plan.
- Modify `crates/tqsdk-core/src/state/changes.rs`
  - Add internal applied-change builder changes that reduce intermediate clones while preserving public `ChangeSet`.
  - Add tests for deterministic ordering and compatibility.
- Modify `crates/tqsdk-core/src/state/store.rs`
  - Add single-root apply path and borrowed existing-child lookup.
  - Reduce changed-field metadata cloning.
- Modify `crates/tqsdk-core/src/runtime/handle.rs`
  - Add domain/source classification only after measurement proves value.
  - Add a lock-contention benchmark or ignored test before any lock split.
- Modify `crates/tqsdk-core/src/adapter/common.rs`
  - Change quote field sorting to lower-risk improvements after measurement.
  - Split quote fast-path shape validation from mutation allocation if benchmark or source guard justifies it.
- Modify `docs/performance-audit-handoff.md`
  - Record clean benchmark results, accepted/deferred external review items, and next residual bottleneck.
- Modify architecture docs only if a later batch changes public `ChangeSet` shape or runtime sequencing:
  - `docs/architecture/README.md`
  - `docs/architecture/runtime-core/data-contracts.md`
  - `docs/architecture/runtime-core/type-system.md`
  - `docs/architecture/validation.md`

---

## Hard Gates

- Before editing any symbol, run GitNexus impact with repo name:

```bash
gitnexus impact <symbol> --direction upstream --repo tqsdk-rust
```

- Before each commit, run:

```bash
gitnexus detect-changes --scope staged --repo tqsdk-rust
git diff --cached --check
```

- Batches 0-3 must preserve:
  - revision advances only for actual field changes;
  - noop diffs produce no commit;
  - `ChangeSet.path_hits`, `object_hits`, and `field_hits` public fields and ordering;
  - `WaitStep::changed_quote_symbols()` ordering;
  - stream path/object/field filter behavior;
  - command lifecycle validation.
- If a task proposes changing public `ChangeSet` fields or `ChangeHit` field types, stop and run the architecture update batch first.

---

## Batch 0: Clean Measurement Before More Optimization

### Task 0.1: Remove Payload Construction From Timed Ingest Benchmarks

**Files:**

- Modify `crates/tqsdk-core/examples/diff_ingest_microbench.rs`
- Modify `crates/tqsdk-core/tests/performance_surface.rs`

Required pre-edit analysis:

```bash
gitnexus impact run_ingest_case --direction upstream --repo tqsdk-rust
gitnexus impact quote_rtn_data --direction upstream --repo tqsdk-rust
```

- [ ] **Step 1: Add a source guard for benchmark purity**

Add a test to `crates/tqsdk-core/tests/performance_surface.rs`:

```rust
#[test]
fn diff_ingest_bench_does_not_construct_quote_payloads_inside_ingest_timer() {
    let source = include_str!("../examples/diff_ingest_microbench.rs");
    let start = source
        .find("fn run_ingest_case")
        .expect("run_ingest_case must exist");
    let end = source[start..]
        .find("fn run_noop_case")
        .map(|offset| start + offset)
        .expect("run_noop_case must follow run_ingest_case");
    let body = &source[start..end];

    assert!(
        !body.contains("quote_rtn_data(symbols, sequence)"),
        "timed ingest benchmark must consume prebuilt inputs, not build quote payloads"
    );
    assert!(
        !body.contains("market_input(quote_rtn_data"),
        "timed ingest benchmark must not build RuntimeInput payloads inside the timer"
    );
}
```

- [ ] **Step 2: Run the guard and verify it fails**

```bash
cargo test -p tqsdk-core --test performance_surface diff_ingest_bench_does_not_construct_quote_payloads_inside_ingest_timer
```

Expected: FAIL because `run_ingest_case` still constructs `quote_rtn_data` inside the timed loop.

- [ ] **Step 3: Pre-generate input vectors outside timed loops**

In `diff_ingest_microbench.rs`, add helpers shaped like:

```rust
fn quote_inputs(iterations: u64, symbols: &[String]) -> Vec<RuntimeInput> {
    (0..iterations)
        .map(|sequence| market_input(quote_rtn_data(symbols, sequence)))
        .collect()
}

fn sparse_quote_inputs(
    iterations: u64,
    universe: &[String],
    changed_per_iter: usize,
) -> Vec<RuntimeInput> {
    (0..iterations)
        .map(|sequence| {
            let start = sequence as usize % universe.len().max(1);
            market_input(sparse_quote_rtn_data(
                universe,
                start,
                changed_per_iter,
                sequence,
            ))
        })
        .collect()
}
```

Then change `run_ingest_case` to consume the prebuilt vector:

```rust
fn run_ingest_case(
    name: &'static str,
    iterations: u64,
    symbols: &[String],
) -> tqsdk_core::Result<BenchResult> {
    let inputs = quote_inputs(iterations, symbols);
    let handle = runtime_handle();
    let start = Instant::now();
    let mut commits = 0_u64;
    for input in inputs {
        if let Some(commit) = handle.ingest(input, Vec::new(), CommitScope::RealtimeUpdate)? {
            commits += 1;
            black_box(commit.revision);
        }
    }
    Ok(BenchResult {
        name,
        iterations,
        items_per_iter: symbols.len(),
        commits,
        elapsed: start.elapsed(),
    })
}
```

- [ ] **Step 4: Add sparse quote payload helper**

Add a sparse helper that updates only `datetime`, `last_price`, and `volume`:

```rust
fn sparse_quote_rtn_data(
    universe: &[String],
    start: usize,
    changed_count: usize,
    sequence: u64,
) -> Value {
    let count = changed_count.min(universe.len());
    let mut quotes = Map::with_capacity(count);
    for offset in 0..count {
        let index = (start + offset) % universe.len();
        let symbol = &universe[index];
        let mut fields = Map::new();
        fields.insert(
            "datetime".to_string(),
            Value::String(format!("202606101001{sequence:08}")),
        );
        fields.insert(
            "last_price".to_string(),
            number(600.0 + index as f64 * 0.01 + sequence as f64 * 0.001),
        );
        fields.insert(
            "volume".to_string(),
            Value::from(sequence as i64 + index as i64),
        );
        quotes.insert(symbol.clone(), Value::Object(fields));
    }

    let mut root = Map::new();
    root.insert("quotes".to_string(), Value::Object(quotes));
    let mut envelope = Map::new();
    envelope.insert("aid".to_string(), Value::String("rtn_data".to_string()));
    envelope.insert("data".to_string(), Value::Array(vec![Value::Object(root)]));
    Value::Object(envelope)
}
```

- [ ] **Step 5: Add clean benchmark cases**

Print these cases in addition to the existing output:

```text
ingest_prebuilt_quote_batch
ingest_prebuilt_large_quote_batch
ingest_sparse_quote_batch_1000x10x3
decode_prebuilt_quote_batch
decode_text_quote_batch
ingest_text_quote_batch
```

`decode_prebuilt_quote_batch` must use `AdapterRegistry::decode_input` and `black_box(mutations.len())`. `decode_text_quote_batch` and `ingest_text_quote_batch` must use `InputPayload::Text` values generated outside the timed loop.

- [ ] **Step 6: Run validation**

```bash
cargo fmt --all --check
cargo test -p tqsdk-core --test performance_surface diff_ingest_bench_does_not_construct_quote_payloads_inside_ingest_timer
cargo check -p tqsdk-core --example diff_ingest_microbench
cargo run -p tqsdk-core --example diff_ingest_microbench --release
git diff --check
```

- [ ] **Step 7: Commit**

```bash
git add crates/tqsdk-core/examples/diff_ingest_microbench.rs crates/tqsdk-core/tests/performance_surface.rs
gitnexus detect-changes --scope staged --repo tqsdk-rust
git commit -m "bench(core): clean diff ingest measurements"
```

### Task 0.2: Add a Runtime Lock Tail-Latency Probe

**Files:**

- Create `crates/tqsdk-core/tests/runtime_ingest_tail_latency.rs`

Required pre-edit analysis:

```bash
gitnexus impact RuntimeHandle::ingest --direction upstream --repo tqsdk-rust
gitnexus impact RuntimeHandle::submit --direction upstream --repo tqsdk-rust
```

- [ ] **Step 1: Add an ignored benchmark-style test**

Create a test that drives large market ingests and records how long a command submission waits for the runtime mutex:

```rust
#![forbid(unsafe_code)]

use std::time::{Duration, Instant};

use futures::executor::block_on;
use serde_json::{Map, Value};
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, MarketCommand, ProtocolDomain,
    RuntimeCommand, RuntimeHandle, RuntimeInput, Symbol,
};

#[test]
#[ignore = "benchmark-style tail latency probe; run explicitly with --ignored --nocapture"]
fn command_submit_latency_under_large_market_ingest_is_reported() {
    let handle = runtime_handle();
    let symbols = bench_symbols(1_000);
    let market_inputs = (0..64)
        .map(|sequence| market_input(quote_rtn_data(&symbols, sequence)))
        .collect::<Vec<_>>();

    let start = Instant::now();
    let mut command_latencies = Vec::new();
    for input in market_inputs {
        let ingest_start = Instant::now();
        let commit = handle
            .ingest(input, Vec::new(), CommitScope::RealtimeUpdate)
            .expect("market ingest succeeds");
        assert!(commit.is_some());

        let command_start = Instant::now();
        let command_id = block_on(handle.submit(RuntimeCommand::Market(
            MarketCommand::SubscribeQuotes {
                symbols: vec![Symbol::new("SHFE.tail_probe")],
            },
        )))
        .expect("command submission succeeds");
        assert!(command_id.get() > 0);
        command_latencies.push(command_start.elapsed());

        eprintln!(
            "large ingest elapsed={:?} command_submit_after_ingest={:?}",
            ingest_start.elapsed(),
            command_latencies.last().copied().unwrap()
        );
    }

    command_latencies.sort();
    let p95 = command_latencies[command_latencies.len() * 95 / 100];
    eprintln!(
        "tail probe total={:?} command_submit_p95={:?}",
        start.elapsed(),
        p95
    );
    assert!(p95 >= Duration::ZERO);
}

fn runtime_handle() -> RuntimeHandle {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    RuntimeHandle::with_adapters(adapters)
}

fn market_input(payload: Value) -> RuntimeInput {
    RuntimeInput::Io(IoEvent {
        route: "market".to_string(),
        domains: vec![ProtocolDomain::Market],
        payload: InputPayload::Json(payload),
    })
}

fn bench_symbols(count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("SHFE.tail{index:04}"))
        .collect()
}
```

Use the same `quote_rtn_data` helper shape as the example benchmark. Keep this ignored and print-only; do not assert machine-specific latency.

- [ ] **Step 2: Run validation**

```bash
cargo test -p tqsdk-core --test runtime_ingest_tail_latency -- --ignored --nocapture
cargo test -p tqsdk-core --test runtime_contract_route_dispatch
git diff --check
```

- [ ] **Step 3: Commit**

```bash
git add crates/tqsdk-core/tests/runtime_ingest_tail_latency.rs
gitnexus detect-changes --scope staged --repo tqsdk-rust
git commit -m "bench(core): add runtime ingest tail latency probe"
```

---

## Batch 1: Reduce `ChangeSet` Construction Cost Without Public Shape Changes

### Task 1.1: Replace Owned `AppliedChange.fields` With Field Index Metadata

**Files:**

- Modify `crates/tqsdk-core/src/state/changes.rs`
- Modify `crates/tqsdk-core/src/state/store.rs`
- Modify `crates/tqsdk-core/tests/runtime_contract_commit_semantics.rs`
- Modify `crates/tqsdk-core/tests/performance_surface.rs`

Required pre-edit analysis:

```bash
gitnexus impact AppliedChange --direction upstream --repo tqsdk-rust
gitnexus impact ChangeSet::from_applied_changes --direction upstream --repo tqsdk-rust
gitnexus impact StateStore::apply_with --direction upstream --repo tqsdk-rust
```

- [ ] **Step 1: Add behavior tests before implementation**

Extend `runtime_contract_commit_semantics.rs` with tests that assert:

```rust
assert_eq!(
    changed.changes.path_hits,
    vec![StatePath::new(["quotes", "SHFE.au2606"])]
);
assert_eq!(
    changed.changes.object_hits,
    vec![ObjectKey::Quote {
        symbol: Symbol::new("SHFE.au2606")
    }]
);
assert_eq!(
    changed.changes.field_hits,
    vec![
        ChangeHit::field(
            StatePath::new(["quotes", "SHFE.au2606"]),
            ObjectKey::Quote {
                symbol: Symbol::new("SHFE.au2606")
            },
            "ask_price1"
        ),
        ChangeHit::field(
            StatePath::new(["quotes", "SHFE.au2606"]),
            ObjectKey::Quote {
                symbol: Symbol::new("SHFE.au2606")
            },
            "last_price"
        ),
    ]
);
```

Also assert a repeated identical quote diff produces no commit.

- [ ] **Step 2: Add a source guard for removed intermediate field clones**

Add to `performance_surface.rs`:

```rust
#[test]
fn applied_change_metadata_does_not_store_owned_field_names() {
    let source = include_str!("../src/state/changes.rs");
    assert!(
        !source.contains("pub(crate) fields: Vec<String>"),
        "AppliedChange should track changed field indexes or borrowed metadata, not clone field names before ChangeSet construction"
    );
}
```

- [ ] **Step 3: Change `AppliedChange` to store changed field indexes**

Use this internal representation:

```rust
pub(crate) struct AppliedChange {
    pub(crate) root: &'static str,
    pub(crate) path: StatePath,
    pub(crate) object: Option<ObjectKey>,
    pub(crate) field_indexes: Vec<usize>,
}
```

`apply_fields` should push the field index when a field changed. `ChangeSet::from_applied_changes` should receive both `changes: &[AppliedChange]` and `mutations: &[NormalizedMutation]` so it can read field names from the original mutation.

- [ ] **Step 4: Preserve public `ChangeSet` compatibility**

Update `CommitEngine::apply` to call:

```rust
let changes = ChangeSet::from_applied_changes(&applied, &mutations);
```

Keep `ChangeSet { path_hits, object_hits, field_hits }` unchanged.

- [ ] **Step 5: Run validation and benchmark**

```bash
cargo fmt --all --check
cargo test -p tqsdk-core --test runtime_contract_commit_semantics
cargo test -p tqsdk-core --test performance_surface
cargo run -p tqsdk-core --example diff_ingest_microbench --release
git diff --check
```

- [ ] **Step 6: Commit**

```bash
git add crates/tqsdk-core/src/state/changes.rs crates/tqsdk-core/src/state/store.rs crates/tqsdk-core/src/runtime/commit_engine.rs crates/tqsdk-core/tests/runtime_contract_commit_semantics.rs crates/tqsdk-core/tests/performance_surface.rs
gitnexus detect-changes --scope staged --repo tqsdk-rust
git commit -m "perf(core): reduce applied change metadata cloning"
```

### Task 1.2: Decide Whether Public Grouped `ChangeSet` Is Worth an Architecture Change

**Files:**

- Modify `docs/performance-audit-handoff.md`
- Modify architecture docs only if proceeding with public shape change.

- [ ] **Step 1: Compare Batch 1.1 numbers**

Record before/after for:

```text
ingest_prebuilt_quote_batch
ingest_prebuilt_large_quote_batch
ingest_sparse_quote_batch_1000x10x3
decode_prebuilt_quote_batch
```

- [ ] **Step 2: Stop or escalate by threshold**

Stop this public-shape investigation if Batch 1.1 reduces `ingest_prebuilt_large_quote_batch` by at least 15% and `ingest_sparse_quote_batch_1000x10x3` by at least 10%.

Escalate to an architecture plan only if:

- `field_hits.len()` remains the dominant allocation path in source and benchmark evidence;
- wait/stream/task consumers can be updated in one pass;
- architecture docs are updated before implementation.

- [ ] **Step 3: Document the decision**

Append a short section to `docs/performance-audit-handoff.md`:

```markdown
### Grouped ChangeSet Decision

- Batch 1.1 result:
- Decision:
- Reason:
- Public API impact:
```

- [ ] **Step 4: Commit**

```bash
git add docs/performance-audit-handoff.md
gitnexus detect-changes --scope staged --repo tqsdk-rust
git commit -m "docs: record grouped changeset decision"
```

---

## Batch 2: Reduce State Apply Copies and Lookup Work

### Task 2.1: Add Single-Root Apply Fast Path

**Files:**

- Modify `crates/tqsdk-core/src/state/store.rs`
- Modify `crates/tqsdk-core/tests/runtime_contract_commit_semantics.rs`
- Modify `crates/tqsdk-core/tests/performance_surface.rs`

Required pre-edit analysis:

```bash
gitnexus impact StateStore::apply_with --direction upstream --repo tqsdk-rust
gitnexus impact partition_path --direction upstream --repo tqsdk-rust
```

- [ ] **Step 1: Add a source guard against `BTreeSet` on single-root path**

Add to `performance_surface.rs`:

```rust
#[test]
fn state_store_apply_has_single_root_fast_path_before_btreeset_classification() {
    let source = include_str!("../src/state/store.rs");
    assert!(
        source.contains("apply_single_root"),
        "StateStore::apply_with should have a single-root fast path for common quote batches"
    );
}
```

- [ ] **Step 2: Implement root classification**

In `apply_with`, inspect the first mutation root and only allocate the `BTreeSet` path if a later mutation has a different root:

```rust
let Some(first) = mutations.first() else {
    return None;
};
let first_root = partition_path(first).0;
if mutations
    .iter()
    .all(|mutation| partition_path(mutation).0 == first_root)
{
    return self.apply_single_root(revision, first_root, mutations, on_applied);
}
```

- [ ] **Step 3: Add `apply_single_root`**

The helper locks one partition and applies mutations in original order:

```rust
fn apply_single_root<T, F>(
    &self,
    revision: Revision,
    root: StateRoot,
    mutations: &[NormalizedMutation],
    on_applied: F,
) -> Option<T>
where
    F: FnOnce(Vec<AppliedChange>) -> T,
{
    let mut partition = rwlock_write(root.partition(self));
    let mut applied = Vec::new();
    for mutation in mutations {
        let (_, path) = partition_path(mutation);
        if let Some(changed) = apply_mutation_at_partition(&mut partition, path, mutation) {
            applied.push(changed);
        }
    }
    if applied.is_empty() {
        None
    } else {
        self.revision.store(revision.get(), Ordering::SeqCst);
        Some(on_applied(applied))
    }
}
```

- [ ] **Step 4: Run validation**

```bash
cargo fmt --all --check
cargo test -p tqsdk-core --test runtime_contract_commit_semantics
cargo test -p tqsdk-core --test performance_surface
cargo run -p tqsdk-core --example diff_ingest_microbench --release
git diff --check
```

- [ ] **Step 5: Commit**

```bash
git add crates/tqsdk-core/src/state/store.rs crates/tqsdk-core/tests/runtime_contract_commit_semantics.rs crates/tqsdk-core/tests/performance_surface.rs
gitnexus detect-changes --scope staged --repo tqsdk-rust
git commit -m "perf(core): fast path single-root state apply"
```

### Task 2.2: Avoid Cloning Existing Path Segments in `ensure_child_object`

**Files:**

- Modify `crates/tqsdk-core/src/state/store.rs`
- Modify `crates/tqsdk-core/tests/performance_surface.rs`

Required pre-edit analysis:

```bash
gitnexus impact ensure_child_object --direction upstream --repo tqsdk-rust
gitnexus impact apply_mutation_at_path --direction upstream --repo tqsdk-rust
```

- [ ] **Step 1: Add a source guard**

```rust
#[test]
fn ensure_child_object_looks_up_existing_child_before_cloning_segment() {
    let source = include_str!("../src/state/store.rs");
    assert!(
        !source.contains(".entry(segment.clone())"),
        "existing state path children should be looked up by borrowed segment before allocating a key"
    );
}
```

- [ ] **Step 2: Implement borrowed lookup before insert**

Replace `map.entry(segment.clone())` with a borrowed lookup path:

```rust
if !map.contains_key(segment) {
    map.insert(segment.clone(), Value::Object(Map::new()));
}
let child = map
    .get_mut(segment)
    .expect("child was inserted or already present");
```

Keep the existing behavior that non-object children are replaced with empty objects.

- [ ] **Step 3: Run validation**

```bash
cargo fmt --all --check
cargo test -p tqsdk-core --test runtime_contract_commit_semantics
cargo test -p tqsdk-core --test performance_surface
cargo run -p tqsdk-core --example diff_ingest_microbench --release
git diff --check
```

- [ ] **Step 4: Commit**

```bash
git add crates/tqsdk-core/src/state/store.rs crates/tqsdk-core/tests/performance_surface.rs
gitnexus detect-changes --scope staged --repo tqsdk-rust
git commit -m "perf(core): avoid existing path segment allocation"
```

---

## Batch 3: Remove Repeated Scans and Low-Risk Sort Cost

### Task 3.1: Classify Mutation Roots and Sources Once

**Files:**

- Modify `crates/tqsdk-core/src/runtime/handle.rs`
- Modify `crates/tqsdk-core/src/state/store.rs`
- Modify `crates/tqsdk-core/tests/performance_surface.rs`

Required pre-edit analysis:

```bash
gitnexus impact normalize_order_lifecycle_mutations --direction upstream --repo tqsdk-rust
gitnexus impact validate_mutation_domains --direction upstream --repo tqsdk-rust
gitnexus impact RuntimeHandle::apply_and_publish_locked --direction upstream --repo tqsdk-rust
```

- [ ] **Step 1: Add tests proving pure market input skips order lifecycle normalization**

Add a source guard:

```rust
#[test]
fn pure_market_mutations_skip_trade_order_lifecycle_scan_by_domain() {
    let source = include_str!("../src/runtime/handle.rs");
    assert!(
        source.contains("domains_are_pure_market"),
        "pure market batches should skip trade order lifecycle scanning before iterating every mutation"
    );
}
```

- [ ] **Step 2: Add domain helper**

```rust
fn domains_are_pure_market(domains: &[ProtocolDomain]) -> bool {
    domains.len() == 1 && domains[0] == ProtocolDomain::Market
}
```

Use it before `normalize_order_lifecycle_mutations`:

```rust
let mutations = if domains_are_pure_market(&domains) {
    mutations
} else {
    normalize_order_lifecycle_mutations(&self.state, mutations)?
};
```

- [ ] **Step 3: Keep mutation source guard**

Do not skip `validate_mutation_domains` unless Batch 0 benchmark proves it is visible. If skipping is later added, only skip when the adapter source and input domains are already known and add tests proving market diffs cannot write `trade`, `runtime`, or `query` roots.

- [ ] **Step 4: Run validation**

```bash
cargo fmt --all --check
cargo test -p tqsdk-core --test runtime_contract_commit_semantics
cargo test -p tqsdk-core --test performance_surface
cargo run -p tqsdk-core --example diff_ingest_microbench --release
git diff --check
```

- [ ] **Step 5: Commit**

```bash
git add crates/tqsdk-core/src/runtime/handle.rs crates/tqsdk-core/tests/performance_surface.rs
gitnexus detect-changes --scope staged --repo tqsdk-rust
git commit -m "perf(core): skip trade lifecycle scan for pure market batches"
```

### Task 3.2: Reduce Quote Fast-Path Sorting and Fallback Allocation

**Files:**

- Modify `crates/tqsdk-core/src/adapter/common.rs`
- Modify `crates/tqsdk-core/tests/runtime_contract_adapters.rs`
- Modify `crates/tqsdk-core/tests/performance_surface.rs`

Required pre-edit analysis:

```bash
gitnexus impact decode_quote_object_fast_path --direction upstream --repo tqsdk-rust
gitnexus impact decode_tq_diff --direction upstream --repo tqsdk-rust
```

- [ ] **Step 1: Add differential tests**

Add adapter tests proving quote fast path output stays identical for:

- one quote with fields out of order;
- two quotes in one payload;
- one malformed quote with a nested object falling back to generic decode;
- null field values.

- [ ] **Step 2: Use `sort_unstable_by` for quote field sorting**

Change only the quote fast path:

```rust
fields.sort_unstable_by(|left, right| left.field.cmp(&right.field));
```

Field names in a JSON object are unique, so unstable sorting preserves the final sorted set and does not alter observable field order for distinct field names.

- [ ] **Step 3: Validate quote shapes before allocating mutations**

Split the shape check from output allocation:

```rust
if quotes.values().any(|value| {
    value
        .as_object()
        .is_none_or(|fields| fields.values().any(Value::is_object))
}) {
    return None;
}
```

Only allocate `mutations` after validation passes.

- [ ] **Step 4: Run validation**

```bash
cargo fmt --all --check
cargo test -p tqsdk-core --test runtime_contract_adapters
cargo test -p tqsdk-core --test performance_surface
cargo run -p tqsdk-core --example diff_ingest_microbench --release
git diff --check
```

- [ ] **Step 5: Commit**

```bash
git add crates/tqsdk-core/src/adapter/common.rs crates/tqsdk-core/tests/runtime_contract_adapters.rs crates/tqsdk-core/tests/performance_surface.rs
gitnexus detect-changes --scope staged --repo tqsdk-rust
git commit -m "perf(core): trim quote fast path sorting overhead"
```

---

## Batch 4: Runtime Lock Strategy Decision

This batch is decision-gated. Do not implement an actor rewrite or sequencer split without clear latency evidence.

### Task 4.1: Record Lock Contention Findings

**Files:**

- Modify `docs/performance-audit-handoff.md`

- [ ] **Step 1: Run the ignored tail probe before and after Batches 1-3**

```bash
cargo test -p tqsdk-core --test runtime_ingest_tail_latency -- --ignored --nocapture
```

Record p95 command submission latency printed by the test.

- [ ] **Step 2: Decide using concrete threshold**

No lock split if command submission p95 stays below 2x the no-load command submission p95.

Write a new architecture plan if p95 exceeds 2x under large market batches after Batches 1-3.

- [ ] **Step 3: Document the decision**

Append:

```markdown
### Runtime Lock Tail-Latency Decision

- No-load command submit p95:
- Under large market ingest p95:
- Decision:
- Reason:
```

- [ ] **Step 4: Commit**

```bash
git add docs/performance-audit-handoff.md
gitnexus detect-changes --scope staged --repo tqsdk-rust
git commit -m "docs: record runtime lock latency decision"
```

### Task 4.2: Architecture-Gated Sequencer Plan Only If Needed

If Task 4.1 says the p95 risk remains material, create a separate plan. That plan must not be implemented in this batch.

Required plan contents:

- Keep one global revision sequence.
- Keep one `CommitLog`.
- Keep command ledger transitions validated.
- Move expensive market decode/apply work out of the command submission critical section only when sequencing can still be proven.
- Include `docs/architecture/README.md`, `docs/architecture/runtime-core/*.md`, and `docs/architecture/validation.md` updates before code.

---

## Batch 5: Final Validation and Report

### Task 5.1: Full Workspace Validation

Run after the final code batch:

```bash
cargo fmt --all --check
cargo check --workspace --examples
cargo test --workspace
cargo clippy --workspace --examples --all-targets -- -D warnings
git diff --check
```

If public API or architecture docs changed, also run:

```bash
cargo check --workspace --no-default-features
cargo check --workspace --no-default-features --examples
cargo test -p tqsdk-session --no-default-features
cargo check --workspace --all-features --examples
```

### Task 5.2: Performance Report

**Files:**

- Modify `docs/performance-audit-handoff.md`

Report:

- Old polluted benchmark numbers.
- New clean benchmark numbers.
- Sparse quote benchmark numbers.
- Decode-only versus ingest numbers.
- ChangeSet clone reduction result.
- State apply result.
- Lock tail-latency result.
- Residual bottleneck and next stop criterion.

Commit:

```bash
git add docs/performance-audit-handoff.md
gitnexus detect-changes --scope staged --repo tqsdk-rust
git commit -m "docs: record next diff ingest performance results"
```

---

## Stop Criteria

Stop the next optimization pass when any of these is true:

- Clean `ingest_prebuilt_quote_batch / decode_prebuilt_quote_batch` is below 3x.
- Sparse `1000x10x3` quote updates are below 2x decode-only cost.
- Additional wins are below 5% while increasing runtime contract complexity.
- Further progress requires changing public `ChangeSet` shape.
- Runtime lock p95 requires actor/sequencer design; create a separate architecture plan instead of continuing this plan.

---

## Self-Review

Spec coverage:

- Benchmark pollution from `性能审查.md`: Batch 0.
- `ChangeSet` metadata duplication: Batch 1.
- State apply path/value copies: Batch 2.
- Runtime mutex p99 risk: Batch 0.2 and Batch 4.
- Repeated scans and sorting: Batch 3.
- Final report: Batch 5.

Placeholder scan:

- No task uses unresolved placeholder wording.
- Each implementation task names exact files and validation commands.
- Public API changing work is decision-gated and separated from low-risk batches.

Type consistency:

- `RuntimeInput`, `InputPayload::Json`, `InputPayload::Text`, `AdapterRegistry::decode_input`, `RuntimeHandle::ingest`, `MarketCommand::SubscribeQuotes`, and `ChangeSet` names match current code.
