# TQSDK Rust SDK Overdesign Audit Design

## Context

This branch is evaluating whether the current `tqsdk-rust` SDK public API is on
the right maturity path or has drifted into overdesign.

The user asked for an overall judgment, not only a crate-by-crate code audit.
The audit must balance:

- Rust SDK quality norms.
- Trading SDK domain requirements.
- Evidence from the current code, architecture docs, scenario contracts, and
  examples.
- External calibration from mature or adjacent libraries, including the user
  supplied public API reference:
  <https://github.com/pseudocodes/tqsdk-rs/tree/main/tqsdk-rs>.
- Official Python SDK calibration from
  <https://github.com/shinnytech/tqsdk-python>.

This is a design for the audit and iteration plan. It does not authorize broad
code changes yet.

## Goal

Produce a decision-grade review that answers four questions:

1. Is the current SDK genuinely overdesigned, or is some complexity justified by
   the trading SDK domain?
2. Which public APIs and docs should be kept, hidden as advanced, deferred, or
   removed from active documentation?
3. What should the default user path of an excellent Rust TQSDK look like?
4. What iteration plan should this branch follow before attempting more public
   API changes?

## User Decisions Already Captured

- Output should start with an overall SDK judgment.
- The review lens should balance Rust SDK best practices with trading SDK
  domain fit.
- The result should identify complexity to cut/defer and define how to mature
  the SDK into an excellent product.
- The preferred approach is: excellent SDK standards plus a subtraction
  roadmap.
- The public API reference list should include
  `pseudocodes/tqsdk-rs/tree/main/tqsdk-rs`.
- The official Python SDK should be treated as a benchmark for functional
  semantics and mature trading workflows. Because it is a different language,
  the Rust audit should focus on what behavior to match, where the Rust API can
  be more idiomatic, and where Rust should deliver lower overhead.

## Evidence Model

The audit will use four evidence classes.

### 1. Current Repository Contracts

Primary evidence:

- `AGENTS.md`
- `README.md`
- `docs/architecture/*`
- `crates/*/README.md`
- `crates/*/src/lib.rs`
- `crates/*/examples/api_contract_s*.rs`
- `docs/reviews/public-api-scenario-review.md`
- `docs/reviews/public-api-disposition-matrix.md`
- `docs/scenarios/user-layer-iteration-plan.md`
- Existing verification results from this branch.

Architecture docs and current code are authoritative. Review docs, scenario
docs, and superpowers plans are evidence, not authority.

### 2. Rust SDK Quality Criteria

An excellent Rust SDK should have:

- A small, obvious root API for first-time users.
- Clear crate ownership and no duplicate runtime/state paths.
- Feature flags that match user choices without surprising compile failures.
- Error types that are useful without leaking implementation details.
- Examples that double as stable API contracts.
- Advanced escape hatches, but not in the ordinary prelude.
- Async behavior that is explicit about runtime expectations and cancellation.
- Semver discipline around public exports.

### 3. Trading SDK Domain Criteria

Trading SDKs need more complexity than ordinary data clients. The audit should
not cut complexity that protects these invariants:

- A stable state snapshot after each update.
- Reconnect/resync barriers before exposing incomplete state.
- Explicit order intent, idempotency, and command lifecycle.
- Separate one-shot metadata queries from continuous live consumption.
- Clear live, historical, replay, and testing boundaries.
- Account, position, and order ownership that prevents accidental cross-account
  or same-symbol task conflicts.
- Safe defaults for real-money operations.

### 4. External Public API Calibration

Two external references will be used differently.

### 4a. Official Python SDK Functional Benchmark

`shinnytech/tqsdk-python` is the official maturity benchmark. It should be used
to evaluate feature completeness and user workflow semantics, not copied as a
Rust shape.

Observed benchmark semantics:

- `TqApi` presents one default user object that owns network connections,
  receives market/trade data, and maintains a complete in-memory business data
  snapshot.
- `wait_update()` is the central progression point: it sends pending requests,
  advances background tasks, receives and merges diffs, and returns only after
  new business data or timeout.
- Initialization waits for an available snapshot before ordinary strategy code
  runs.
- Reconnect handling records requests, resends subscriptions/login commands,
  suppresses incomplete upstream data, and releases data only after a complete
  market or trade snapshot is received.
- `get_quote`, `get_quote_list`, `get_kline_serial`, `insert_order`,
  `cancel_order`, account/position/order/trade refs, risk rules, target-position
  tasks, backtest/replay, and `DataDownloader` define the mature trading SDK
  workflow set.
- `TargetPosTask` enforces one task per account/symbol and documents that order
  actions are driven by subsequent `wait_update()` calls.
- Local risk hooks run before insert/cancel operations and update internal risk
  counters as data and orders flow through the API.

Rust audit implications:

- Match the official SDK's functional coverage where it represents mature user
  semantics: stable snapshots, reconnect recovery, live refs, order lifecycle,
  target position, multi-account explicitness, risk hooks, backtest/replay, and
  history download.
- Do not copy the single large `TqApi` class, implicit mutable pandas
  DataFrames, stringly typed order parameters, hidden event loop ownership, or
  broad root namespace as Rust API shape.
- Prefer typed builders, enums, newtypes, `Result` errors, explicit feature
  gates, explicit async runtime ownership, immutable commit snapshots, and
  zero-copy or low-copy hot paths where Rust can be materially better.
- Use Python SDK behavior to judge whether a Rust feature is missing or
  immature; use Rust norms to judge whether the exposed API is elegant and
  performant.

### 4b. Unofficial Rust Public API Contrast

The user-supplied reference `pseudocodes/tqsdk-rs` is useful as a public API
contrast, not as a direct template.

Observed strengths:

- A single `Client` / `ClientBuilder` entrypoint is easy to explain.
- README examples show market, history, trade, builder, callback, channel, and
  stream usage quickly.
- The crate demonstrates how a compact first screen can lower user friction.

Observed pitfalls:

- The root re-exports `DataManager`, auth, WebSocket, logger, subscription
  internals, and broad domain types, which makes implementation details part of
  the public contract.
- It offers callback, channel, and stream shapes in the ordinary path, which can
  blur the recommended default.
- Its single-crate shape is ergonomic, but does not by itself solve runtime
  recovery, state consistency, order idempotency, or long-term semver hygiene.

For `tqsdk-rust`, the lesson is not "collapse the workspace into one crate".
The lesson is: the default facade must feel almost as direct as a single-client
SDK, while keeping advanced runtime and implementation surfaces behind explicit
advanced paths.

## Working Judgment To Validate

The current project does not look like random crate splitting. The core split
has a coherent reason:

- `tqsdk-core` owns runtime substrate, commit/revision/cursor, state store, and
  schema-level protocol types.
- `tqsdk-session` owns shared session and one-shot direct query.
- `tqsdk-wait` owns Python-style single-owner `wait_update()` consumption.
- `tqsdk-stream` owns async-native multi-consumer stream consumption.
- `tqsdk-task` owns execution tooling.
- `tqsdk-data` owns research/offline history, cache, and replay data concerns.
- `tqsdk` should be a thin default facade.

The likely problem is not the existence of layers. The likely problem is the
public presentation and active documentation: too many advanced/foundation
surfaces are described as if they are current SDK product promises.

Read-only reviewer feedback supports this judgment:

- The crate split should be kept because it protects one runtime state tree, one
  commit/revision model, and distinct ownership for direct query, wait-style
  consumption, stream fan-out, execution tooling, and offline data.
- Most non-default crates should be hidden from first-read docs and presented as
  advanced paths.
- The root README currently makes users learn the crate taxonomy before the
  default `tqsdk` flow is convincing.
- `tqsdk-task` and `tqsdk-stream` have broad root surfaces that may be coherent
  inside their crates, but are risky to stabilize wholesale as a 0.1 public API.
- `tqsdk::advanced::*` is under-specified: it is presented as the escape hatch,
  but currently exposes curated subsets rather than complete sibling-crate
  surfaces.
- The default facade contract is promising but thin; it needs more root-crate
  contract examples before it can carry the whole SDK usability story.

## Review Classification

Every public surface or doc claim will be assigned one of four outcomes:

| Outcome | Meaning |
| --- | --- |
| Keep | Essential SDK contract; keep public and documented. |
| Keep as advanced | Useful for advanced users; keep out of the default facade/prelude. |
| Defer | Valid idea, but not part of current public SDK contract. |
| Remove from active docs/API | Stale, contradictory, or harmful as a current promise. |

This matrix is the main mechanism for resisting overdesign without throwing
away necessary trading SDK complexity.

## Audit Focus Areas

### Default Facade

Check whether `tqsdk` gives ordinary users one clear path:

- connect
- subscribe/read quotes
- wait for updates
- inspect account/position
- place or target orders safely
- access history as an opt-in path

The default facade should not force ordinary users to understand runtime
handles, raw commits, low-level session internals, test harnesses, or durable
sidecars.

The first-read docs should start from install and the default facade flow, then
move crate taxonomy and advanced escape hatches below the beginner path.

The default facade should be compared against Python `TqApi` for workflow
coverage, while intentionally using Rust shapes:

- typed `TqBuilder` instead of one constructor with many optional modes;
- typed refs and order tickets instead of string/dict-style mutation;
- explicit `Result` and feature-gated capabilities;
- clear ownership for `wait_update()` versus stream fan-out.

### Public Export Hygiene

Check whether crate roots and `tqsdk::advanced` expose only intentional
contracts. Pay special attention to:

- root prelude shape
- `advanced::*` aliases and re-exports
- task report/status types
- stream event/health/retry/shutdown types
- data cache/replay types
- direct-query service types

The audit must decide whether `advanced::*` should:

- become a fuller curated facade for sibling crates, or
- explicitly say that full-power users should depend on sibling crates directly.

It should not remain ambiguous.

### Performance And API Deduplication

Rust-side performance is a first-class SDK requirement. The audit must not
recommend simplification that forces users onto slower paths, extra allocation,
unnecessary cloning, full-universe scans per commit, or avoidable background task
fan-out.

At the same time, performance cannot justify multiple public ways to perform the
same user task. A strong SDK should expose one canonical high-performance path
per workflow. Additional entrypoints are acceptable only when they are:

- thin ergonomic aliases over the same implementation;
- clearly advanced escape hatches;
- or genuinely different consumption models, such as single-owner `wait_update`
  versus multi-consumer `Stream`.

The audit should flag redundant API combinations where users can build the same
workflow through several public surfaces with no semantic difference. Examples
to examine include:

- quote subscription through default facade, `tqsdk-wait`, `tqsdk-stream`, and
  raw session handles;
- direct query access through root facade versus `session()` escape hatches;
- order submission through root facade, wait helpers, task host helpers, and
  lower-level session commands;
- history access through `Tq::history()`, `tqsdk-data`, and session direct-query
  helpers.

For each duplicate-looking path, the final plan must decide whether to:

- keep one as the canonical public path;
- keep another only as an advanced escape hatch;
- collapse it into a thin alias;
- or remove/archive it from first-read docs.

The preferred shape is not "few APIs at any cost". It is: minimal public choices
with no hidden performance penalty.

### Wait / Stream Boundary

The audit must explicitly test whether `tqsdk-stream` is still necessary if
`tqsdk-wait` can implement high-performance full-universe quote subscription.

The expected boundary is:

- `tqsdk-wait` should own the default and single-owner strategy path, including
  high-performance full-universe market data if the workload has one strategy
  loop or one coordinating consumer.
- `tqsdk-stream` should not rely on "faster quote subscription" as its core
  reason to exist.
- `tqsdk-stream` remains justified only for async-native system-integration
  workloads: independent consumers, commit fan-out, explicit bounded backpressure
  and lag diagnostics, path/domain/object/field filters, event pipelines, and
  service-style composition with `futures::Stream`.
- Both facades must consume the same `RuntimeReader` / `UpdateCursor` /
  `SessionClient` substrate and must not create a second state tree.

The review should distinguish subscription throughput from consumption model.
Full-universe subscription is not enough to justify a separate crate if the
consumer still wants a single stable snapshot after each step. Conversely,
single-owner `wait_update()` performance is not enough to replace a stream
facade when multiple downstream tasks need independent progress, isolation from
slow consumers, and typed lag/error events.

Concrete audit questions:

- Can `tqsdk-wait` expose changed quote batches from `WaitStep` so full-universe
  users avoid scanning all symbols on each commit?
- Should `QuoteSet` add step-bound helpers such as changed symbol iteration or
  changed snapshots, matching the performance shape currently proven by
  `tqsdk-stream::QuoteBatchSubscription`?
- Which `tqsdk-stream` APIs are true stream-only value, and which are merely
  alternate syntax for wait-style single-consumer state reads?
- Should `tqsdk-stream` be documented as an advanced integration crate rather
  than a primary market-data-performance path?
- Should broad object/event stream root exports be kept, hidden behind modules
  or features, or deferred until their system-integration use cases are proven?

### Documentation Drift

Find contradictions where active docs still mention removed or deferred
surfaces. Known candidates include:

- `tqsdk-task` docs mentioning stream managed sink/WAL/journal sidecars.
- scenario docs mentioning live cache pipe or `MarketCacheStreamWriter`.
- active plans or review docs that describe platform/daemon behavior as current
  SDK scope.
- architecture docs that still use older `tqsdk-api-wait` /
  `tqsdk-api-stream` naming or stale "V1 is not a facade SDK" wording after the
  landed `tqsdk` facade.
- data docs that describe the crate as extremely narrow while also listing
  cache, replay, CSV export, Greeks, mmap history cache, and market cache
  capabilities.

### Validation Credibility

An excellent SDK cannot rely only on architecture prose. The audit must record
whether current validation gates pass. Known branch risks to re-check before
planning implementation:

- workspace tests previously had scheduler failures in `tqsdk-task`.
- no-default feature validation previously failed in `tqsdk-session` live smoke
  tests because service-gated methods were referenced without the right feature
  surface.

Any implementation plan that changes public API or docs must re-run at minimum:

- `git diff --check`
- relevant contract examples for changed crates
- `cargo check --workspace --examples`
- feature-matrix checks when feature gates or default dependency paths change

### API Contract Examples

The 58 current examples are useful, but they may also signal scope creep. The
audit will separate:

- examples that define core user contracts,
- examples that should move to advanced docs,
- examples that should be archived as sketches or historical inputs.

Special scrutiny should go to examples that make strategy deployment,
supervision, replay checkpointing, source merging, retry policies, or
low-latency desk profiles feel like default SDK product scope.

## Subagent Review Inputs

Four read-only review roles contributed findings:

- API contract reviewer: public API risks, export hygiene, acceptance criteria.
- Architecture reviewer: necessary boundaries versus over-fragmentation.
- Simplicity reviewer: YAGNI, overdesign smells, subtraction roadmap.
- External calibration reviewer: Rust SDK and trading SDK best-practice
  criteria, including the `pseudocodes/tqsdk-rs` reference.

Their findings are advisory. The final audit and plan must reconcile conflicts
against the project architecture docs and current code. The external-reference
section uses the main-thread source read of `pseudocodes/tqsdk-rs` because one
research subagent returned only a partial calibration.

## Deliverables

The next written output should contain:

1. Overall verdict on the SDK's current design.
2. Necessary-complexity map.
3. Overdesign-risk map.
4. Keep / advanced / defer / remove matrix.
5. Public API maturity checklist.
6. Iteration plan split into short, verifiable batches.
7. Validation commands and known blockers.
8. Official Python SDK benchmark matrix: matched semantics, Rust improvements,
   and intentionally rejected Python shapes.
9. Wait / stream boundary decision: whether `tqsdk-stream` remains, which
   workloads justify it, and which high-performance quote APIs should move into
   or remain in `tqsdk-wait`.
10. Duplicate-path audit: for each important user task, identify the canonical
    high-performance API, advanced escape hatches, and public paths to remove or
    de-emphasize.

The recommended roadmap shape is:

1. Documentation subtraction first: rewrite first-read docs around the default
   facade, move taxonomy down, remove stale sink/WAL/journal and live-pipe
   claims, and classify scenario examples.
2. Public API quarantine before stabilization: keep `tqsdk::prelude` small,
   clarify `advanced::*`, and mark or gate broad task/stream/data foundation
   surfaces that are not committed default product contracts.
3. Stabilize only core contracts: default facade, wait quote/order, stream
   quote batches/lag/health, session metadata, data history/cache, target-pos,
   basic risk, and test harness.

## Non-Goals

This audit will not:

- rewrite the SDK now;
- collapse the workspace into a single crate by default;
- copy Python SDK API names mechanically;
- copy Python SDK implementation shape mechanically;
- copy `pseudocodes/tqsdk-rs` mechanically;
- promote daemon, GUI, HTTP operations, durable queue, or cross-process cache
  service behavior into core SDK scope;
- judge quality by line count or number of crates alone.

## Acceptance Criteria

The audit and iteration plan are good enough when:

- An ordinary Rust user can identify the default crate and first three API calls
  without reading internal architecture docs.
- Advanced users can still reach runtime/session/stream/data/task primitives
  through explicit paths.
- Removed/deferred surfaces are not described as current product promises in
  active docs.
- Every proposed cut explains whether it removes code, hides API, archives docs,
  or merely changes README emphasis.
- Every kept complexity maps to a trading SDK invariant or Rust SDK best
  practice.
- Performance-critical workflows have an explicitly named canonical API and no
  required fallback to slower or more allocation-heavy paths.
- Duplicate public paths are justified by different semantics or consumption
  models, not by historical layering accidents.
- Every Python benchmark item is classified as: match behavior, improve with a
  Rust-native API, or intentionally reject as language-specific shape.
- The plan does not justify `tqsdk-stream` merely by full-universe quote
  subscription performance if `tqsdk-wait` can provide the same single-consumer
  throughput.
- The plan contains verification commands, expected risks, and a clear stopping
  point for each batch.
