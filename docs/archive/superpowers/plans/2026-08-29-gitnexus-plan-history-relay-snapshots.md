# GitNexus Engineering Plan

> Status: Completed on 2026-08-29 as a low-concurrency functional release.
>
> Closure note: all functional, bounded-resource, migration, rollback, observability, feature-matrix,
> formatting, clippy, documentation, and graph checks passed. The 8-client end-to-end p99 target did
> not pass on the production host and is retained as a non-blocking capacity characterization, not a
> satisfied performance gate or production p99 guarantee. A segmented/factorial benchmark and a
> relay-private worker process remain deferred follow-ups if higher concurrency or a market p99 SLO
> becomes required. Current architecture documents supersede the original gate wording below.

> Task: add a zero-read-interruption, local CacheOnly history adapter to `tqsdk-relay` with structure-sharing snapshot publication.
>
> Evidence verified at commit `c5d9280f44e843ae3ebd448f742d0ec9a26d2387`. The GitNexus index is at that commit, but its analyzer dependency-runtime provenance differs from the currently available runner, so graph findings are navigation evidence and current source is authoritative.
>
> Evidence provenance schema 2. The generated plan path is excluded from the digest.

## 1. Objective

Extend the existing `tqsdk-relay` daemon with a read-only history HTTP sibling while preserving the market relay's runtime, engine lock, CPU workers, ordering, and default direct-SDK path.

The history service must:

- answer only from verified, published, immutable CacheOnly snapshots;
- keep `tqsdk-data::BacktestHistoryClient` as the sole owner of planning, aggregation, coverage, finality, metadata, and main-contract mapping;
- let `tqsdk-cache` exclusively own prewarm, snapshot clone, verify, publish, recover, scrub, and garbage collection;
- let relay own only HTTP parsing, admission, projection, encoding, auditing, and operations;
- hot-swap generations without interrupting requests pinned to the old generation;
- enforce daemon-global resource bounds and retain a production-equivalent market-interference
  benchmark as capacity evidence.

This is an architecture update. Freeze the ADR, HTTP contract, and snapshot manifest contract before changing production symbols.

## 2. Current Behaviour

- [verified] `BacktestHistoryClient::query()` delegates through `query_batch()` and private `start_run()`. CacheOnly does not acquire the mutable-root gate; RemoteOnMiss does.
- [verified] `BacktestHistoryRun` streams provisional chunks and a terminal report. Dropping it sets cancellation, but detached coordinator and blocking scan work may continue until they observe cancellation.
- [verified] Terminal success already carries coverage, segments, metadata snapshot hash, and finality; failure is currently string-oriented.
- [verified] The planner handles concrete, index, and main-continuous symbols. A cache miss alone is not authoritative proof that a symbol is unknown.
- [verified] The current `tqsdk-cache query` CLI owns a duplicate field schema/default projection with 9 Kline and 29 Tick fields.
- [verified] Daily `.tqdk` and minute `.tqmk` writers replace a pathname atomically. Tick `.tqbn` files support append/truncate/recovery mutation and cannot be hardlinked safely.
- [verified] Relay startup uses a current-thread Tokio runtime around one `Arc<Mutex<RelayEngine>>`; upstream, downstream, and metrics work share that runtime.
- [verified] The existing metrics HTTP path locks `RelayEngine` for health/metrics and performs synchronous gzip. It is not a suitable history request path.
- [verified] Commit `c5d9280` introduced minute cache format v5 with zstd and an explicit backup-backed v4 migration. Compatibility is fail-closed rather than an implicit writable-root upgrade.

## 3. Relevant Architecture

- [verified] `tqsdk-data` owns history planning, metadata, coverage, cache readers, and typed cache/operator primitives.
- [verified] `tqsdk-cache` is the operator-facing cache CLI and may depend on `tqsdk-data`; relay must not depend on the CLI crate.
- [verified] `tqsdk-relay` is optional, is not in Cargo default-members, and must not become a general TQ proxy or acquire live history credentials.
- [inferred] The reusable snapshot/query/inspect seam belongs in `tqsdk-data`, the only layer both publisher and relay can consume without duplicating semantics.
- [verified] Relay currently disables `tqsdk-data` default features. The new history feature needs only local reader support including `tqbn-zstd`, never `live`, `services`, reqwest, or credentials.
- [verified] Existing cache readers already rely on opened-file/path-replacement behavior that can pin old content while a new path becomes current.
- [verified] The repository's architecture hard boundaries require the relay history path to remain separate from `RelayEngine`, `RelayServer`, market state, and market runtime locks.

## 4. GitNexus Findings

- `impact(BacktestHistoryClient::start_run, upstream, depth=3)`: HIGH; 3 direct callers, 26 total upstream symbols, one affected execution flow. Re-run immediately before editing this coordinator seam.
- `impact(BacktestHistoryRequestFailure, upstream, depth=3)`: LOW; 3 direct callers and 7 total. Preserve display compatibility while adding typed reasons.
- `impact(RelayConfig, upstream, depth=3)`: HIGH; 19 direct test consumers. Use a sibling `HistoryConfig` rather than expanding the market config surface.
- `impact(main::run, upstream, depth=3)`: LOW; the direct caller is a binary smoke-test path.
- `context(BacktestHistoryRun)`: confirms cancellation and detached coordinator lifecycle are relevant to snapshot lease ownership.
- GitNexus returned an unrelated HIGH chain for the private cache CLI `Command` due to a symbol-name/index collision. Targeted source search confines the enum to `crates/tqsdk-cache/src/main.rs`.
- GitNexus reports UNKNOWN/no callers for private `query::Field`. Text search finds its uses inside `crates/tqsdk-cache/src/query.rs`; UNKNOWN is not treated as safe, and existing query tests remain mandatory.

## 5. Statement-Level PDG Findings

No load-bearing PDG result is available. The available GitNexus CLI does not expose `pdg_query`, and the current analyzer dependency-runtime digest differs from the runner recorded in the index. Under strict freshness rules, this plan does not refresh or build a load-bearing graph layer.

Targeted current-source control-flow verification establishes these constraints:

- [verified] An HTTP-handler-owned lease is insufficient because dropping `BacktestHistoryRun` cancels but does not join detached scan work.
- [inferred] A snapshot query must clone the shared generation lease into the coordinator lifecycle so GC cannot acquire the exclusive lease until all blocking scans finish.
- [verified] `.tqbn` append/truncate behavior forbids hardlink cloning; `.tqmk` and `.tqdk` can share immutable inodes only while all subsequent writes replace paths.
- [verified] Existing concurrency and buffering controls are primarily per run or per symbol, not a daemon-global history memory cap.

## 6. Proposed Changes

### 6.1 Freeze three architecture contracts

Create:

- `docs/architecture/history-relay.md`: accepted ADR for ownership, same-process failure domain, runtime/listener/CPU separation, feature boundaries, rollout, rollback, and non-goals.
- `docs/architecture/history-relay-http.md`: HTTP v1 contract for `GET /v1/history/query`, `/coverage`, and `/schema`, including strict parameters, row representation, status mapping, ETags, gzip, audit, and CORS behavior.
- `docs/architecture/history-snapshot-manifest.md`: snapshot root, manifest v1, canonical identity, authoritative catalog, file roles, compatibility, lease protocol, durability, recovery, scrub, retention, and GC.

Update architecture indexes and crate-boundary documents in the same implementation change.

### 6.2 Deepen `tqsdk-data` behind one interface

- Add `backtest_history/schema.rs` for public typed series/field/value-kind definitions, aliases, canonical ordering, defaults, and row-to-cell extraction.
- Move the current CLI schema without changing Kline/Tick field membership, aliases, ordering, or defaults. Both cache CLI and relay JSON codec consume it.
- Add a typed `BacktestHistoryFailureReason` with exact missing ranges while retaining the current display error for compatibility.
- Add strict inspection that reuses the same validation, planner, source inspection, metadata snapshot, coverage, and finality rules as query without scanning rows.
- Add `backtest_history/snapshot.rs` for manifest types, canonical hash and snapshot ID, authoritative complete symbol catalog, format compatibility, path/file-role validation, CURRENT resolution, shared leases, and read-only CacheOnly handles.
- Expose a small snapshot interface: schema, strict inspect, and query. Raw roots, manifest parsing, and file readers remain hidden.
- Carry the snapshot lease through the detached coordinator lifecycle, including disconnect and timeout cancellation.
- Add a reusable daemon-global history resource budget spanning runs and generations. Relay must account raw chunks, JSON buffers, and compression buffers against it.
- Preserve existing `BacktestHistoryClient` query/materialization behavior and signatures where possible.

### 6.3 Add crash-safe `tqsdk-cache snapshot` operations

- Add explicit nested commands for inspect, dry-run, clone/import, prewarm, verify, publish, recover, rollback, scrub, and GC.
- Require explicit `--history-root`; do not change existing `--cache-dir` semantics or silently interpret writable roots through CURRENT.
- Reuse only `tqsdk-data` manifest/open/inspect/query primitives.
- Apply a fail-closed file-role allowlist:
  - `.tqbn`: reflink or ordinary copy only, never hardlink.
  - `.tqmk`, `.tqdk`, and content-addressed immutable metadata: reflink, then hardlink, then copy.
  - `active.json` and other pointers: independent copy or rebuild.
  - locks, lease files, operation locks, and temporary sidecars: exclude and recreate.
  - symlinks, devices, escaping/absolute/duplicate paths, and unknown roles: reject.
- Prewarm only staging. Verify CacheOnly inspection plus an actual query before publication.
- Publish in this durability order: sync staged data and manifest; sync cache and snapshot directories; rename completed snapshot; sync `snapshots/`; write, sync, and rename CURRENT; sync history root.
- Make recovery idempotent at every crash point.
- Keep current plus two previous compatible generations, but never remove a leased generation. Only publisher GC may acquire the exclusive generation lease.

### 6.4 Add a private isolated relay history module

- Add private `src/history/` with a narrow external seam accepting only `HistoryConfig`, shutdown, and an operational-status sink.
- Never pass `RelayEngine`, `RelayServer`, or their mutex into history code.
- Keep `HistoryConfig` separate from HIGH-impact `RelayConfig`. Missing configuration disables history; invalid configured bind/values fail startup; missing or invalid CURRENT makes history return 503 without making market unready.
- Use a dedicated HTTP/1 listener and Tokio runtime/thread pool. Do not extend the metrics HTTP request server.
- Require trusted gateway identity headers; default to no CORS.
- Poll CURRENT every five seconds. Acquire the new generation lease before validation, recheck the pointer, then atomically swap an `Arc<Snapshot>`. Invalid generations leave the last valid generation active. Requests pin their snapshot Arc.
- Enforce max 8 active requests, 100 ms admission queue, 10 s total timeout, 10,000 Kline rows, 50,000 Tick rows, 32 MiB uncompressed representation, and 512 MiB daemon-global history buffers.
- Consume `BacktestHistoryRun::next()` incrementally. Buffer privately but send no body before terminal success; discard the buffer on any terminal coverage, finality, metadata, corruption, cancellation, or internal failure.
- Share projection definitions with the data schema. Encode integers as decimal strings, finite floats as JSON numbers, and missing/non-finite values as null. Follow the frozen timestamp precision and timezone rules. Emit main-contract provenance only once at top level.
- Map typed failures to stable 400/404/409/413/429/500/503/504 responses. Only an authoritative complete catalog may produce 404.
- Treat first corruption of the active generation as 500, mark that generation unhealthy, then return 503 for later requests while leaving market unaffected.
- Implement strong representation ETags and `If-None-Match` including list and `*` semantics. Identity and gzip have distinct ETags and `Vary: Accept-Encoding`.
- Use gzip level 1 only above 64 KiB on two dedicated workers and only when configured market/history CPU sets are nonempty, disjoint, supported, and actually applied. If the compression queue is full, return identity within the same total timeout.
- Publish bounded, low-cardinality history readiness and counters to operations without symbol labels and without taking the market engine mutex.

### 6.5 Feature, migration, and documentation integration

- Add relay `history` feature enabling only `tqsdk-data/tqbn-zstd`; include it in relay defaults while preserving both `--no-default-features` and `--no-default-features --features history`.
- Manifest v1 declares cache format IDs/schema versions and minimum compatible reader.
- Roll out reader expansion before publisher output. Rollback switches CURRENT only to a retained verified compatible generation; never rewrite a published snapshot.
- Import an existing writable cache root non-destructively under a stable-view gate. Old and new writers must not share a staging root.
- Preserve the explicit backup-backed v4-to-v5 minute migration from commit `c5d9280`; v3 remains fail-closed.
- Update root/data/cache/relay READMEs, architecture overview/boundaries/API/format/validation docs, and the relay hard constraints in `AGENTS.md` and `CLAUDE.md`.
- Re-read and integrate the user's existing dirty edits before touching `AGENTS.md`, `CLAUDE.md`, or `docs/architecture/ai-workflow.md`.
- Add a non-live production-equivalent benchmark harness for 8 concurrent history requests, 512 MiB budget, 2 gzip workers, no market loss/reorder/disconnect, and market p99 increase no more than `max(1 ms, 10%)`.

## 7. Implementation Sequence

1. Write and cross-link the ADR, HTTP contract, and snapshot manifest contract. Resolve contradictions in current authority docs before production edits.
2. TDD the shared data schema and typed failure reason. Migrate cache query to consume the schema while keeping all existing query goldens green.
3. TDD strict inspect and snapshot validation: concrete/index/main, final empty ranges, identity, catalog completeness, format compatibility, path roles, and CURRENT resolution.
4. Re-run impact analysis for `BacktestHistoryClient::start_run`, then TDD coordinator-scoped lease retention and old/new generation pinning.
5. TDD publisher role classification, clone modes, non-destructive import, verify, publication crash points, recovery, rollback, scrub, lease-aware retention, and GC. Add CLI adapters only after the module tests pass.
6. TDD the smallest relay tracer: disabled/configured startup and `/schema` on a dedicated runtime while the market engine mutex is held.
7. Add `/coverage`, a small concrete Kline `/query`, then Tick/index/main and all projection/codec cases.
8. Add typed status mapping, all-or-nothing buffering, global limits, timeouts, disconnect cancellation, hot reload, generation health, audit, and operations one contract test at a time.
9. Add compression/affinity, feature-matrix checks, migration docs, benchmark gate, and final graph-aware review.

Each slice follows red-green-refactor. Tests exercise the public data seam, snapshot CLI, or relay HTTP/config seam and do not mock internal planners, clone helpers, or codecs.

## 8. Test Strategy

### `tqsdk-data`

- Shared schema preserves all 9 Kline and 29 Tick fields, aliases, canonical order, and defaults.
- Typed failures distinguish validation, authoritative not-found, incomplete coverage with exact ranges, provisional data, metadata incomplete, corrupt/incompatible snapshot, cancellation, and internal failure.
- Strict inspect and query agree on plan, segments, snapshot hash, coverage, and finality for concrete/index/main symbols, Tick, all legal Kline periods, and final empty ranges.
- Manifest rejects identity mismatch, escaping/absolute/duplicate paths, symlinks/devices, unknown roles, incompatible formats, incomplete catalog claims, and pointer/directory mismatch.
- Dropped runs retain shared leases until coordinator and blocking scans finish; exclusive GC cannot acquire early.

### `tqsdk-cache`

- Dry-run is read-only and reports filesystem capability, sharing, copied bytes, and retained disk.
- Import is non-destructive and retry-idempotent.
- `.tqbn` never shares an inode; safe immutable files may; locks/temp files never do.
- CacheOnly inspect plus real-query smoke precedes publish; missing/provisional/metadata/corruption blocks the whole snapshot.
- Crash injection covers clone, prewarm, verify, manifest sync, snapshot rename, CURRENT temp sync, CURRENT rename, and directory sync.
- GC keeps current plus two previous compatible snapshots and skips generations with shared leases.

### `tqsdk-relay`

- Disabled and fail-fast config cases; history-only feature build; runtime/thread identity; response while market engine mutex is held.
- Exact `/schema`, `/coverage`, and `/query` contracts for concrete/index/main, Tick, all legal Kline periods, arbitrary RFC3339 offsets, half-open ranges, projection, and ordering.
- Stable 400/404/409/413/429/500/503/504 mapping, unknown or duplicate parameters, oversized headers/body, non-GET/OPTIONS behavior, trusted identity, and no CORS.
- Identity/gzip ETags, 304 behavior, threshold, level, compression queue fallback, and total timeout.
- Row, byte, active, queue, global-buffer, and shutdown limits; disconnect cancellation.
- Five-second reload, invalid generation fallback, old-request pinning, lease-aware GC, first-corruption 500 then generation 503.
- No `RelayEngine` lock acquisition, no market runtime worker use, no symbol metric label.

### Verification commands

```bash
cargo test -p tqsdk-data --test backtest_history_api
cargo test -p tqsdk-data --test backtest_history_query
cargo test -p tqsdk-data --test backtest_history_snapshot
cargo test -p tqsdk-data --test minute_kline_cache
cargo test -p tqsdk-cache --test cli query_
cargo test -p tqsdk-cache --test snapshot_cli
cargo test -p tqsdk-relay --test history_http
cargo test -p tqsdk-relay --test history_runtime
cargo test -p tqsdk-relay --test config
cargo test -p tqsdk-relay --test binary_smoke
cargo check -p tqsdk-relay --no-default-features
cargo check -p tqsdk-relay --no-default-features --features history
cargo test -p tqsdk-relay --tests
cargo clippy -p tqsdk-data -p tqsdk-cache -p tqsdk-relay --all-targets -- -D warnings
cargo fmt --all --check
git diff --check
RUSTDOCFLAGS="-D warnings" cargo doc -p tqsdk-data -p tqsdk-relay --no-deps --all-features
node .gitnexus/run.cjs detect-changes --scope all --repo .
```

The plan authorizes no live account, trading, remote history, or credential-bearing smoke.

## 9. Risk and Impact Analysis

- HIGH — query lifecycle: `start_run` reaches query and materialization paths. Re-run impact and preserve current callers/signatures.
- HIGH — config blast radius: `RelayConfig` has 19 direct test consumers. Use sibling `HistoryConfig`.
- HIGH — premature lease release: detached scans outlive handler/run drop. Make lease coordinator-scoped and prove it with blocking-reader plus GC tests.
- HIGH — inode aliasing: hardlinked `.tqbn` can mutate a published generation. Enforce role-based modes and reject unknown roles.
- HIGH — resource multiplication: per-run limits do not cap the daemon. Use one global budget across generations and compression.
- HIGH — 404 correctness: cache absence is not symbol nonexistence. Require an authoritative complete catalog.
- HIGH — format/feature compatibility: enable local v5 compressed readers without pulling network features; expand readers before publishers.
- MEDIUM — same-process failure domain: OOM or abort can still affect market. Admission and budgets reduce risk but do not claim process isolation.
- MEDIUM — shared hardware: CPU affinity does not isolate LLC or memory bandwidth. The measured p99 gate is required.
- MEDIUM — filesystem semantics: atomic rename, fsync, advisory lock, and reflink probing require a supported local filesystem; NFS/object stores are out of scope.
- MEDIUM — dirty worktree: agent workflow files have user edits. Re-anchor before edit and never overwrite them wholesale.
- MEDIUM — graph provenance: source is authoritative until GitNexus is re-indexed with matching analyzer provenance.

## 10. Files Expected to Change

- `docs/architecture/history-relay.md`
- `docs/architecture/history-relay-http.md`
- `docs/architecture/history-snapshot-manifest.md`
- `docs/architecture/{README,crate-boundaries,api-data,history-cache-format,validation,ai-workflow}.md`
- `README.md`, `crates/tqsdk-data/README.md`, `crates/tqsdk-cache/README.md`, `crates/tqsdk-relay/README.md`
- `AGENTS.md`, `CLAUDE.md`
- `crates/tqsdk-data/src/backtest_history/{schema,snapshot}.rs`
- `crates/tqsdk-data/src/backtest_history/{mod,report,planner,executor,store_worker}.rs`
- `crates/tqsdk-data/src/lib.rs`
- `crates/tqsdk-data/tests/backtest_history_snapshot.rs`
- `crates/tqsdk-cache/src/snapshot.rs`
- `crates/tqsdk-cache/src/{main,query}.rs`
- `crates/tqsdk-cache/tests/{cli,snapshot_cli}.rs`
- `crates/tqsdk-relay/src/history/{mod,http,codec,snapshot}.rs`
- `crates/tqsdk-relay/src/{lib,main}.rs`
- `crates/tqsdk-relay/Cargo.toml`
- `crates/tqsdk-relay/tests/{history_http,history_runtime,config,binary_smoke}.rs`
- `scripts/bench_relay_history_isolation.py`

## 11. Reusable Implementation Context

```yaml
implementation_context:
  task_summary: "Add a read-only CacheOnly history sibling to tqsdk-relay with structure-sharing zero-interruption snapshots."
  acceptance_criteria:
    - "BacktestHistoryClient remains the only planner/query/coverage/finality/metadata owner."
    - "tqsdk-cache publishes verified immutable snapshots; relay only opens and queries them."
    - "Old requests remain pinned while new requests hot-swap; no valid publish interrupts reads."
    - "History cannot acquire RelayEngine/RelayServer locks or use market runtime/CPU workers."
    - "Responses are bounded, terminal-success-only CacheOnly JSON with stable typed errors and ETags."
    - "Feature, migration, rollback, and focused functional gates pass; the production-equivalent interference benchmark is non-blocking capacity evidence."
  evidence_provenance: {"schema_version":2,"head_commit":"c5d9280f44e843ae3ebd448f742d0ec9a26d2387","generated_plan_path":"docs/plans/2026-08-29-gitnexus-plan-history-relay-snapshots.md","global_dirty_digest":{"algorithm":"sha256","canonicalization":"gitnexus-evidence-provenance-v2 NUL-framed UTF-8 records","value":"3136c5ad1cf781356acf79ecfbdc7f6dc49671d0ffedd1450ec62787d010833a"},"cited_path_manifest":[{"path":"AGENTS.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"unstaged","rename_from":null,"rename_to":null,"head_digest":"sha256:0f651b990f8bfb7b521d16a5005cb6660d41721728a4ba6f0a61df40584e3a33","index_digest":"sha256:0f651b990f8bfb7b521d16a5005cb6660d41721728a4ba6f0a61df40584e3a33","worktree_digest":"sha256:c5fed05c6f3a01160734913ad2531d9129d0cc54ebe6fc72f69aa87afd80ee7b","untracked_digest":"absent"},{"path":"CLAUDE.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"unstaged","rename_from":null,"rename_to":null,"head_digest":"sha256:f9fe75d0ab7a19c152b796a35df8bdf629bf5866723dda2db360d9b8f001d833","index_digest":"sha256:f9fe75d0ab7a19c152b796a35df8bdf629bf5866723dda2db360d9b8f001d833","worktree_digest":"sha256:ce29fc8ba12de3fefe94d3b3d0a2c3f0ef882e818448bdb2c1a2e8785138a18b","untracked_digest":"absent"},{"path":"Cargo.toml","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:703c3a735fcaeff39e0c21771c8351b92bc6b14d9d275f5984e079059020aa5b","index_digest":"sha256:703c3a735fcaeff39e0c21771c8351b92bc6b14d9d275f5984e079059020aa5b","worktree_digest":"sha256:703c3a735fcaeff39e0c21771c8351b92bc6b14d9d275f5984e079059020aa5b","untracked_digest":"absent"},{"path":"README.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:8c20709e79e5fe643789263ce3b935d24084159f92920c3c612909d755b81807","index_digest":"sha256:8c20709e79e5fe643789263ce3b935d24084159f92920c3c612909d755b81807","worktree_digest":"sha256:8c20709e79e5fe643789263ce3b935d24084159f92920c3c612909d755b81807","untracked_digest":"absent"},{"path":"crates/tqsdk-cache/README.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:c521a0fac57d23a9c8d497c28879d45068fa867f85cf07595be25f95db3fae60","index_digest":"sha256:c521a0fac57d23a9c8d497c28879d45068fa867f85cf07595be25f95db3fae60","worktree_digest":"sha256:c521a0fac57d23a9c8d497c28879d45068fa867f85cf07595be25f95db3fae60","untracked_digest":"absent"},{"path":"crates/tqsdk-cache/src/main.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:921e61c12401434801adde961bacf17cac48abbee38ed6c14b4d28fd5c5b2aa7","index_digest":"sha256:921e61c12401434801adde961bacf17cac48abbee38ed6c14b4d28fd5c5b2aa7","worktree_digest":"sha256:921e61c12401434801adde961bacf17cac48abbee38ed6c14b4d28fd5c5b2aa7","untracked_digest":"absent"},{"path":"crates/tqsdk-cache/src/query.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:47a2a55c2a3afe34b8332d7faa3235d7ce79778494d20f4164a392bd8924e783","index_digest":"sha256:47a2a55c2a3afe34b8332d7faa3235d7ce79778494d20f4164a392bd8924e783","worktree_digest":"sha256:47a2a55c2a3afe34b8332d7faa3235d7ce79778494d20f4164a392bd8924e783","untracked_digest":"absent"},{"path":"crates/tqsdk-cache/src/snapshot.rs","object_kind":{"head":"absent","index":"absent","worktree":"absent","untracked":"absent"},"state":"absent","rename_from":null,"rename_to":null,"head_digest":"absent","index_digest":"absent","worktree_digest":"absent","untracked_digest":"absent"},{"path":"crates/tqsdk-cache/tests/cli.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:6f53090b3a0125a12a68b7208e4fbb3d5aaced8bc7bd62725c1440619ee31d7f","index_digest":"sha256:6f53090b3a0125a12a68b7208e4fbb3d5aaced8bc7bd62725c1440619ee31d7f","worktree_digest":"sha256:6f53090b3a0125a12a68b7208e4fbb3d5aaced8bc7bd62725c1440619ee31d7f","untracked_digest":"absent"},{"path":"crates/tqsdk-cache/tests/snapshot_cli.rs","object_kind":{"head":"absent","index":"absent","worktree":"absent","untracked":"absent"},"state":"absent","rename_from":null,"rename_to":null,"head_digest":"absent","index_digest":"absent","worktree_digest":"absent","untracked_digest":"absent"},{"path":"crates/tqsdk-data/Cargo.toml","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:9044b35cff98376292583f26cd41837d79443d99756735a5bd7d18c14fcddeec","index_digest":"sha256:9044b35cff98376292583f26cd41837d79443d99756735a5bd7d18c14fcddeec","worktree_digest":"sha256:9044b35cff98376292583f26cd41837d79443d99756735a5bd7d18c14fcddeec","untracked_digest":"absent"},{"path":"crates/tqsdk-data/README.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:ec95c7de57db0b0676dd51d32c83d61979d008dff956bcc58b2749c890edea8c","index_digest":"sha256:ec95c7de57db0b0676dd51d32c83d61979d008dff956bcc58b2749c890edea8c","worktree_digest":"sha256:ec95c7de57db0b0676dd51d32c83d61979d008dff956bcc58b2749c890edea8c","untracked_digest":"absent"},{"path":"crates/tqsdk-data/src/backtest_history/executor.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:2ea2d47da1f1ffb21301dcdc2b00e25e3f7e7e98722326a8de04f2cd9ed0c705","index_digest":"sha256:2ea2d47da1f1ffb21301dcdc2b00e25e3f7e7e98722326a8de04f2cd9ed0c705","worktree_digest":"sha256:2ea2d47da1f1ffb21301dcdc2b00e25e3f7e7e98722326a8de04f2cd9ed0c705","untracked_digest":"absent"},{"path":"crates/tqsdk-data/src/backtest_history/metadata.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:1018e013e8ff9140738471683b01d57c441becf4c62948b1210f2d00a0ce446a","index_digest":"sha256:1018e013e8ff9140738471683b01d57c441becf4c62948b1210f2d00a0ce446a","worktree_digest":"sha256:1018e013e8ff9140738471683b01d57c441becf4c62948b1210f2d00a0ce446a","untracked_digest":"absent"},{"path":"crates/tqsdk-data/src/backtest_history/mod.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:34e07839cde4125c9a399196e70596b06942c87d02baf0bbc1318b8d9d0ce975","index_digest":"sha256:34e07839cde4125c9a399196e70596b06942c87d02baf0bbc1318b8d9d0ce975","worktree_digest":"sha256:34e07839cde4125c9a399196e70596b06942c87d02baf0bbc1318b8d9d0ce975","untracked_digest":"absent"},{"path":"crates/tqsdk-data/src/backtest_history/planner.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:b327f3c71f302486df437849a64fd5b5cd779a0b24ad88b558fc2e087cf23e67","index_digest":"sha256:b327f3c71f302486df437849a64fd5b5cd779a0b24ad88b558fc2e087cf23e67","worktree_digest":"sha256:b327f3c71f302486df437849a64fd5b5cd779a0b24ad88b558fc2e087cf23e67","untracked_digest":"absent"},{"path":"crates/tqsdk-data/src/backtest_history/report.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:dcd88617b5581a3c91607825471b589b87da3c736105c35e7ebc5c4689126042","index_digest":"sha256:dcd88617b5581a3c91607825471b589b87da3c736105c35e7ebc5c4689126042","worktree_digest":"sha256:dcd88617b5581a3c91607825471b589b87da3c736105c35e7ebc5c4689126042","untracked_digest":"absent"},{"path":"crates/tqsdk-data/src/backtest_history/schema.rs","object_kind":{"head":"absent","index":"absent","worktree":"absent","untracked":"absent"},"state":"absent","rename_from":null,"rename_to":null,"head_digest":"absent","index_digest":"absent","worktree_digest":"absent","untracked_digest":"absent"},{"path":"crates/tqsdk-data/src/backtest_history/snapshot.rs","object_kind":{"head":"absent","index":"absent","worktree":"absent","untracked":"absent"},"state":"absent","rename_from":null,"rename_to":null,"head_digest":"absent","index_digest":"absent","worktree_digest":"absent","untracked_digest":"absent"},{"path":"crates/tqsdk-data/src/backtest_history/store_worker.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:6a1d32176ac640d72a5374bbf57f72260394bdb14e16cb2e6389c0d6be0b366e","index_digest":"sha256:6a1d32176ac640d72a5374bbf57f72260394bdb14e16cb2e6389c0d6be0b366e","worktree_digest":"sha256:6a1d32176ac640d72a5374bbf57f72260394bdb14e16cb2e6389c0d6be0b366e","untracked_digest":"absent"},{"path":"crates/tqsdk-data/src/backtest_tick_cache.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:3dc680c282e728ee48570ed3aa910ae9be8d50776424a08c85b66f7fd8de3562","index_digest":"sha256:3dc680c282e728ee48570ed3aa910ae9be8d50776424a08c85b66f7fd8de3562","worktree_digest":"sha256:3dc680c282e728ee48570ed3aa910ae9be8d50776424a08c85b66f7fd8de3562","untracked_digest":"absent"},{"path":"crates/tqsdk-data/src/daily_kline_cache.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:f5047af4d58b8bb1eb15f46e4795ea87e4de214829ca55a6024d567e24e780bc","index_digest":"sha256:f5047af4d58b8bb1eb15f46e4795ea87e4de214829ca55a6024d567e24e780bc","worktree_digest":"sha256:f5047af4d58b8bb1eb15f46e4795ea87e4de214829ca55a6024d567e24e780bc","untracked_digest":"absent"},{"path":"crates/tqsdk-data/src/history_series_cache/tqbn/codec.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:cb7c35b93b16d5537a9799189a06050523fcdf703b42552d94fa55f42264053f","index_digest":"sha256:cb7c35b93b16d5537a9799189a06050523fcdf703b42552d94fa55f42264053f","worktree_digest":"sha256:cb7c35b93b16d5537a9799189a06050523fcdf703b42552d94fa55f42264053f","untracked_digest":"absent"},{"path":"crates/tqsdk-data/src/history_series_cache/tqbn/mod.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:57ba75eeeaacceb406b28c29f96b795e0a0071879458379205716c15be06b3ac","index_digest":"sha256:57ba75eeeaacceb406b28c29f96b795e0a0071879458379205716c15be06b3ac","worktree_digest":"sha256:57ba75eeeaacceb406b28c29f96b795e0a0071879458379205716c15be06b3ac","untracked_digest":"absent"},{"path":"crates/tqsdk-data/src/lib.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:47656c676c5ba514fde96acebe17c1da1aaf6fc23cb8e7069fe6a4b8dc5206dc","index_digest":"sha256:47656c676c5ba514fde96acebe17c1da1aaf6fc23cb8e7069fe6a4b8dc5206dc","worktree_digest":"sha256:47656c676c5ba514fde96acebe17c1da1aaf6fc23cb8e7069fe6a4b8dc5206dc","untracked_digest":"absent"},{"path":"crates/tqsdk-data/src/minute_kline_cache.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:a0d68b8df1b3d7a6da8ecb78033df51708a025c341f4809b3df40cc43fb869f4","index_digest":"sha256:a0d68b8df1b3d7a6da8ecb78033df51708a025c341f4809b3df40cc43fb869f4","worktree_digest":"sha256:a0d68b8df1b3d7a6da8ecb78033df51708a025c341f4809b3df40cc43fb869f4","untracked_digest":"absent"},{"path":"crates/tqsdk-data/tests/backtest_history_api.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:4348ddd6b63a593b9562fba4b37252c038616194447f07353767575719f161a7","index_digest":"sha256:4348ddd6b63a593b9562fba4b37252c038616194447f07353767575719f161a7","worktree_digest":"sha256:4348ddd6b63a593b9562fba4b37252c038616194447f07353767575719f161a7","untracked_digest":"absent"},{"path":"crates/tqsdk-data/tests/backtest_history_query.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:45cf4c8bd832f07eafa26e9990ba9a134c432c8dabe72bd41630fd12d9b088f8","index_digest":"sha256:45cf4c8bd832f07eafa26e9990ba9a134c432c8dabe72bd41630fd12d9b088f8","worktree_digest":"sha256:45cf4c8bd832f07eafa26e9990ba9a134c432c8dabe72bd41630fd12d9b088f8","untracked_digest":"absent"},{"path":"crates/tqsdk-data/tests/backtest_history_snapshot.rs","object_kind":{"head":"absent","index":"absent","worktree":"absent","untracked":"absent"},"state":"absent","rename_from":null,"rename_to":null,"head_digest":"absent","index_digest":"absent","worktree_digest":"absent","untracked_digest":"absent"},{"path":"crates/tqsdk-data/tests/minute_kline_cache.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:e24c903682f047850e2f725bfdc65a3745e555eb833ddb91183ae436edc4de6c","index_digest":"sha256:e24c903682f047850e2f725bfdc65a3745e555eb833ddb91183ae436edc4de6c","worktree_digest":"sha256:e24c903682f047850e2f725bfdc65a3745e555eb833ddb91183ae436edc4de6c","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/Cargo.toml","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:2d9059931ef18f9da62c1ab98d49eb9ed051e5504e471eaf6576e9deec616932","index_digest":"sha256:2d9059931ef18f9da62c1ab98d49eb9ed051e5504e471eaf6576e9deec616932","worktree_digest":"sha256:2d9059931ef18f9da62c1ab98d49eb9ed051e5504e471eaf6576e9deec616932","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/README.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:a5f89dc8e25f663b2494c3daecbf861848f6cfe8245417e2b06c3373b0d68b4d","index_digest":"sha256:a5f89dc8e25f663b2494c3daecbf861848f6cfe8245417e2b06c3373b0d68b4d","worktree_digest":"sha256:a5f89dc8e25f663b2494c3daecbf861848f6cfe8245417e2b06c3373b0d68b4d","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/src/config.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:cddc3ccf915d88e9f5d314cf482a30361fb9dc8c4132a88cd6ca80bca8e7e3c8","index_digest":"sha256:cddc3ccf915d88e9f5d314cf482a30361fb9dc8c4132a88cd6ca80bca8e7e3c8","worktree_digest":"sha256:cddc3ccf915d88e9f5d314cf482a30361fb9dc8c4132a88cd6ca80bca8e7e3c8","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/src/engine.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:022b9dc8691371e619215f5ef2b61ca4924d2c730afefd7c6ea2661092970cb5","index_digest":"sha256:022b9dc8691371e619215f5ef2b61ca4924d2c730afefd7c6ea2661092970cb5","worktree_digest":"sha256:022b9dc8691371e619215f5ef2b61ca4924d2c730afefd7c6ea2661092970cb5","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/src/history/codec.rs","object_kind":{"head":"absent","index":"absent","worktree":"absent","untracked":"absent"},"state":"absent","rename_from":null,"rename_to":null,"head_digest":"absent","index_digest":"absent","worktree_digest":"absent","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/src/history/http.rs","object_kind":{"head":"absent","index":"absent","worktree":"absent","untracked":"absent"},"state":"absent","rename_from":null,"rename_to":null,"head_digest":"absent","index_digest":"absent","worktree_digest":"absent","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/src/history/mod.rs","object_kind":{"head":"absent","index":"absent","worktree":"absent","untracked":"absent"},"state":"absent","rename_from":null,"rename_to":null,"head_digest":"absent","index_digest":"absent","worktree_digest":"absent","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/src/history/snapshot.rs","object_kind":{"head":"absent","index":"absent","worktree":"absent","untracked":"absent"},"state":"absent","rename_from":null,"rename_to":null,"head_digest":"absent","index_digest":"absent","worktree_digest":"absent","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/src/lib.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:e0592ff3b2cd3012e6354ee1a40d5fff0ccbafa4fa71f645f1c3b24450b839e1","index_digest":"sha256:e0592ff3b2cd3012e6354ee1a40d5fff0ccbafa4fa71f645f1c3b24450b839e1","worktree_digest":"sha256:e0592ff3b2cd3012e6354ee1a40d5fff0ccbafa4fa71f645f1c3b24450b839e1","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/src/main.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:902b3be5de5cf028fb099c25f466aa507690b6b7d2a87c06a2948e7558d71629","index_digest":"sha256:902b3be5de5cf028fb099c25f466aa507690b6b7d2a87c06a2948e7558d71629","worktree_digest":"sha256:902b3be5de5cf028fb099c25f466aa507690b6b7d2a87c06a2948e7558d71629","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/src/metrics_http.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:1741722d8ea5a2e4adfbe29da27479c9c407cbccd055024387b8d686bcfafb42","index_digest":"sha256:1741722d8ea5a2e4adfbe29da27479c9c407cbccd055024387b8d686bcfafb42","worktree_digest":"sha256:1741722d8ea5a2e4adfbe29da27479c9c407cbccd055024387b8d686bcfafb42","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/src/server.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:06c47e16c8fad03a175264ae71ee15059677c58ed6a454d1bbcca49188fd2862","index_digest":"sha256:06c47e16c8fad03a175264ae71ee15059677c58ed6a454d1bbcca49188fd2862","worktree_digest":"sha256:06c47e16c8fad03a175264ae71ee15059677c58ed6a454d1bbcca49188fd2862","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/tests/binary_smoke.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:25afa08717f61a4745cde08ed81f5cd33db51b922dce9ef871279e6ea813fc5c","index_digest":"sha256:25afa08717f61a4745cde08ed81f5cd33db51b922dce9ef871279e6ea813fc5c","worktree_digest":"sha256:25afa08717f61a4745cde08ed81f5cd33db51b922dce9ef871279e6ea813fc5c","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/tests/config.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:e6353240f99062cb30d2df6361522c9604426720b262f72e41742b9f04e99915","index_digest":"sha256:e6353240f99062cb30d2df6361522c9604426720b262f72e41742b9f04e99915","worktree_digest":"sha256:e6353240f99062cb30d2df6361522c9604426720b262f72e41742b9f04e99915","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/tests/history_http.rs","object_kind":{"head":"absent","index":"absent","worktree":"absent","untracked":"absent"},"state":"absent","rename_from":null,"rename_to":null,"head_digest":"absent","index_digest":"absent","worktree_digest":"absent","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/tests/history_runtime.rs","object_kind":{"head":"absent","index":"absent","worktree":"absent","untracked":"absent"},"state":"absent","rename_from":null,"rename_to":null,"head_digest":"absent","index_digest":"absent","worktree_digest":"absent","untracked_digest":"absent"},{"path":"docs/architecture/README.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:b5f9750f80912504099c557bc6946bd66c5159006f1338ac4dcef1f6290ef483","index_digest":"sha256:b5f9750f80912504099c557bc6946bd66c5159006f1338ac4dcef1f6290ef483","worktree_digest":"sha256:b5f9750f80912504099c557bc6946bd66c5159006f1338ac4dcef1f6290ef483","untracked_digest":"absent"},{"path":"docs/architecture/ai-workflow.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"unstaged","rename_from":null,"rename_to":null,"head_digest":"sha256:f8d9b8f5899de1cc78c3e04653f3fa53ffcff6801388ce19ac9c873a705ceabc","index_digest":"sha256:f8d9b8f5899de1cc78c3e04653f3fa53ffcff6801388ce19ac9c873a705ceabc","worktree_digest":"sha256:c0d7540d18e7571b4566f26fe9fc4799a3b6232db91c3ff497407f4424407f97","untracked_digest":"absent"},{"path":"docs/architecture/api-data.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:68ae814c1159d48290e0312f3aa749bba1e6bbb3faa7427dbc8db64a2d862140","index_digest":"sha256:68ae814c1159d48290e0312f3aa749bba1e6bbb3faa7427dbc8db64a2d862140","worktree_digest":"sha256:68ae814c1159d48290e0312f3aa749bba1e6bbb3faa7427dbc8db64a2d862140","untracked_digest":"absent"},{"path":"docs/architecture/crate-boundaries.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:1b54304b1a758996a386f68187ad436a41aebb567bab4e35bf9598cd19239c3d","index_digest":"sha256:1b54304b1a758996a386f68187ad436a41aebb567bab4e35bf9598cd19239c3d","worktree_digest":"sha256:1b54304b1a758996a386f68187ad436a41aebb567bab4e35bf9598cd19239c3d","untracked_digest":"absent"},{"path":"docs/architecture/history-cache-format.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:61e293920bc40a442de0af459ee1fce0798a8b5073927251e4a9899cefce4649","index_digest":"sha256:61e293920bc40a442de0af459ee1fce0798a8b5073927251e4a9899cefce4649","worktree_digest":"sha256:61e293920bc40a442de0af459ee1fce0798a8b5073927251e4a9899cefce4649","untracked_digest":"absent"},{"path":"docs/architecture/history-relay-http.md","object_kind":{"head":"absent","index":"absent","worktree":"absent","untracked":"absent"},"state":"absent","rename_from":null,"rename_to":null,"head_digest":"absent","index_digest":"absent","worktree_digest":"absent","untracked_digest":"absent"},{"path":"docs/architecture/history-relay.md","object_kind":{"head":"absent","index":"absent","worktree":"absent","untracked":"absent"},"state":"absent","rename_from":null,"rename_to":null,"head_digest":"absent","index_digest":"absent","worktree_digest":"absent","untracked_digest":"absent"},{"path":"docs/architecture/history-snapshot-manifest.md","object_kind":{"head":"absent","index":"absent","worktree":"absent","untracked":"absent"},"state":"absent","rename_from":null,"rename_to":null,"head_digest":"absent","index_digest":"absent","worktree_digest":"absent","untracked_digest":"absent"},{"path":"docs/architecture/validation.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:4b2d35a04dd8a2b8f84051030a2fc74a087cd1b943d63cf7d6561448a50ddd94","index_digest":"sha256:4b2d35a04dd8a2b8f84051030a2fc74a087cd1b943d63cf7d6561448a50ddd94","worktree_digest":"sha256:4b2d35a04dd8a2b8f84051030a2fc74a087cd1b943d63cf7d6561448a50ddd94","untracked_digest":"absent"},{"path":"scripts/bench_relay_history_isolation.py","object_kind":{"head":"absent","index":"absent","worktree":"absent","untracked":"absent"},"state":"absent","rename_from":null,"rename_to":null,"head_digest":"absent","index_digest":"absent","worktree_digest":"absent","untracked_digest":"absent"}]}
  primary_symbols:
    - symbol: "BacktestHistoryClient::start_run"
      file: "crates/tqsdk-data/src/backtest_history/mod.rs"
      role: "Coordinator and root-gate policy; HIGH impact."
    - symbol: "BacktestHistoryRequestFailure"
      file: "crates/tqsdk-data/src/backtest_history/report.rs"
      role: "Compatibility surface for typed failures."
    - symbol: "RelayConfig"
      file: "crates/tqsdk-relay/src/config.rs"
      role: "HIGH-impact market config to leave unchanged."
    - symbol: "main::run"
      file: "crates/tqsdk-relay/src/main.rs"
      role: "Sibling daemon integration point."
  related_symbols:
    - symbol: "BacktestHistoryRun"
      relationship: "Must retain a lease through detached coordinator lifecycle."
    - symbol: "query::Field"
      relationship: "Private schema to move into the data seam; graph risk UNKNOWN."
    - symbol: "write_month_atomically / write_file_atomically"
      relationship: "Establish safe immutable pathname replacement versus in-place TQBN mutation."
    - symbol: "serve_metrics_until"
      relationship: "Operations consumer only, never history request server."
  execution_path:
    - "HTTP arrival -> strict parse and trusted identity -> bounded admission -> active Arc<Snapshot>."
    - "Snapshot strict inspect/query -> shared planner/source inspection -> BacktestHistoryRun."
    - "Chunks -> shared byte permits -> bounded JSON; terminal report validates coverage/finality/hash."
    - "Optional dedicated gzip -> representation ETag/304 -> one complete response."
    - "CURRENT poll -> lease before validate -> pointer recheck -> Arc swap; old Arc stays pinned."
  pdg_constraints: []
  architectural_patterns:
    - pattern: "Deep module with two adapters"
      location: "crates/tqsdk-data/src/backtest_history/mod.rs"
      guidance: "Keep planning, schema, inspect, manifest validation, lease, and query behind one data interface."
    - pattern: "Opened-file and generation pinning"
      location: "crates/tqsdk-data/src/minute_kline_cache.rs"
      guidance: "Readers keep old inode/generation while publishers replace pointers atomically."
    - pattern: "Explicit compatibility migration"
      location: "commit c5d9280"
      guidance: "Expand readers, publish compatible snapshots, verify, and rollback by pointer."
  files_to_modify:
    - "docs/architecture/history-relay.md"
    - "docs/architecture/history-relay-http.md"
    - "docs/architecture/history-snapshot-manifest.md"
    - "crates/tqsdk-data/src/backtest_history/schema.rs"
    - "crates/tqsdk-data/src/backtest_history/snapshot.rs"
    - "crates/tqsdk-cache/src/snapshot.rs"
    - "crates/tqsdk-relay/src/history/mod.rs"
    - "scripts/bench_relay_history_isolation.py"
  tests:
    - file: "crates/tqsdk-data/tests/backtest_history_snapshot.rs"
      scenarios: ["typed schema/failure/inspection", "manifest validation", "lease and hot-swap pinning"]
    - file: "crates/tqsdk-cache/tests/snapshot_cli.rs"
      scenarios: ["role-aware clone/import", "publish crash recovery", "lease-aware GC"]
    - file: "crates/tqsdk-relay/tests/history_http.rs"
      scenarios: ["HTTP/error/ETag/gzip contract", "limits, cancellation, no-CORS"]
    - file: "crates/tqsdk-relay/tests/history_runtime.rs"
      scenarios: ["runtime and lock isolation", "reload, old pin, generation health"]
  verification_commands:
    - "cargo test -p tqsdk-data --test backtest_history_snapshot"
    - "cargo test -p tqsdk-cache --test snapshot_cli"
    - "cargo test -p tqsdk-relay --test history_http"
    - "cargo test -p tqsdk-relay --test history_runtime"
    - "cargo test -p tqsdk-relay --tests"
    - "cargo check -p tqsdk-relay --no-default-features"
    - "cargo check -p tqsdk-relay --no-default-features --features history"
    - "cargo clippy -p tqsdk-data -p tqsdk-cache -p tqsdk-relay --all-targets -- -D warnings"
    - "cargo fmt --all --check"
    - "git diff --check"
    - "node .gitnexus/run.cjs detect-changes --scope all --repo ."
  assumptions:
    - "Deployment uses a supported local filesystem; NFS and object stores are out of scope."
    - "A controlled gateway supplies trusted identity and client quota."
    - "No live credentials or remote history access are needed for tests."
    - "Same-process OOM/abort coupling is accepted for the low-concurrency functional release subject to hard resource bounds."
  open_questions:
    - "No product decision blocks implementation; hardware affinity and benchmark baseline are deployment acceptance inputs."
  avoid:
    - "No tqsdk-historyd, RemoteOnMiss, live/services credentials, relay writes, pagination, streaming body, multi-symbol request, CORS, or generic TQ proxy."
    - "Do not put history in RelayEngine, RelayServer, or metrics_http; do not acquire market mutex."
    - "Do not hardlink TQBN or unknown roles, mutate published snapshots, or let relay run GC."
    - "Do not change existing --cache-dir semantics or silently migrate formats."
    - "Do not overwrite existing user changes in agent workflow files."
```

## 12. Assumptions and Open Questions

- [assumed] Production uses a local filesystem with reliable same-filesystem rename, fsync, advisory locks, and optional reflink probing. The contract rejects unsupported storage semantics.
- [assumed] The controlled gateway is the authentication and quota boundary. Relay trusts identity only from configured controlled peers; direct public exposure is out of scope.
- [assumed] CPU affinity is available only on supported production platforms. If missing or invalid, gzip remains disabled; documentation must not claim absolute memory-bandwidth isolation.
- [assumed] Publisher prewarm can create an authoritative symbol catalog and explicitly declare catalog completeness. Without that declaration, absence maps to metadata conflict rather than 404.
- [verified] The handoff references no existing feature ADR, issue, or active implementation plan. Historic superpowers records are non-authoritative inputs.
- [verified] Existing dirty files belong to the user. Implementation must re-anchor and preserve them.
- Deferred follow-up: segmented/factorial performance diagnosis and a relay-managed internal worker
  process if higher concurrency, an explicit market p99 SLO, or stronger OOM/abort isolation becomes
  required.

No product decision is blocking.

## 13. Definition of Done

- ADR, HTTP contract, and manifest contract are authoritative and consistent with crate boundaries.
- `tqsdk-data` exposes one typed snapshot/query/inspect/schema seam; CLI and relay contain no duplicate planner, manifest parser, cache reader, field schema, or status-string matching.
- Published generations are immutable, role-validated, content-verified, lease-protected, recoverable, rollback-capable, and hot-swapped without interrupting reads.
- Relay history has an independent listener/runtime/thread/CPU/resource path and serves while the market engine lock is held.
- Every HTTP response is bounded, all-or-nothing, stable, auditable, and derived from one validated generation.
- Feature isolation, migration, rollback, full focused test suite, formatting, clippy, docs, and graph
  change detection pass. The production-equivalent interference benchmark is recorded truthfully as
  a failed p99 capacity target and is non-blocking for this low-concurrency functional release.
