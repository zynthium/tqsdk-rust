# Daily Cache Unification Implementation Plan

> Status: completed on `feat/daily-cache-unification`; archived after Task 7 verification.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make native daily history a first-class, fail-closed backtest cache family with the same fill progress, configuration, reporting, inspection, verification, and purge contracts as tick and canonical minute caches.

**Architecture:** `tqsdk-data::BacktestHistoryClient` becomes the sole owner of fill scheduling for tick, canonical minute, and native daily requests. The `tqsdk` facade and `tqsdk-cache` CLI adapt that public orchestration API; ordinary local backtests read native 1d rows directly and aggregate integer-day periods in memory without falling back to minute data. Daily storage remains one `daily-kline-v1` file per logical symbol.

**Tech Stack:** Rust 2024, Tokio, Clap, Serde JSON, existing TQBN / `.tqmk` / `.tqdk` cache implementations, GitNexus impact checks.

---

## File map

- `crates/tqsdk-data/src/backtest_history/orchestration.rs`: public validated orchestration config, normalized progress events, cancellation, batch timeout, failure isolation, and terminal report.
- `crates/tqsdk-data/src/backtest_history/{mod.rs,fill.rs,executor.rs,request.rs,report.rs}`: expose and drive orchestration through the existing coordinator without introducing a second cache/session owner.
- `crates/tqsdk-data/src/lib.rs`: public exports for the orchestration contract.
- `crates/tqsdk/src/{lib.rs,backtest_remote.rs}`: advanced re-export, facade delegation, native daily planning, cache status/purge/warmup, and replay construction.
- `crates/tqsdk-task/src/history_backtest_replay.rs`: native daily replay input and integer-day aggregation from final 1d rows.
- `crates/tqsdk-cache/src/{main.rs,lib.rs}`: common fill runner/progress, schema-v3 reports, inventory/doctor/purge/verify support.
- `crates/tqsdk-data/tests/*`, `crates/tqsdk/tests/*`, `crates/tqsdk-task/tests/*`, `crates/tqsdk-cache/tests/cli.rs`: focused RED/GREEN contract tests.
- `README.md`, `docs/README.md`, `docs/architecture/{README.md,crate-boundaries.md,api-data.md,backtest-tick-cache-cli.md,backtest-tick-cache-operations.md,history-cache-format.md,validation.md`, `crates/{tqsdk,tqsdk-data,tqsdk-cache}/README.md`: authoritative user and validation documentation.

### Task 1: Public data-layer orchestration contract

**Files:**
- Create: `crates/tqsdk-data/src/backtest_history/orchestration.rs`
- Modify: `crates/tqsdk-data/src/backtest_history/mod.rs`
- Modify: `crates/tqsdk-data/src/lib.rs`
- Test: `crates/tqsdk-data/tests/backtest_history_api.rs`

- [ ] **Step 1: Write failing public-contract tests**

Test that a config equivalent to the following accepts defaults and rejects zero or values above four instead of clamping:

```rust
let config = BacktestHistoryFillConfig::default()
    .with_symbol_batch_size(1)?
    .with_symbol_concurrency(2)?
    .with_idle_timeout(Duration::from_secs(60))?
    .without_batch_timeout();
assert_eq!(config.symbol_batch_size(), 1);
assert!(BacktestHistoryFillConfig::default().with_symbol_concurrency(5).is_err());
```

Also assert that tick, minute, and daily progress events share one enum and that a terminal report records completed, failed, and interrupted symbols.

- [ ] **Step 2: Run the focused test and confirm RED**

Run: `cargo test -p tqsdk-data --test backtest_history_api orchestration_ -- --nocapture`

Expected: compile failure because the public orchestration types do not exist.

- [ ] **Step 3: Implement validated configuration and result types**

Define `BacktestHistoryFillConfig`, `BacktestHistoryFillProgress`, `BacktestHistoryFillSymbolResult`, `BacktestHistoryFillTerminalReport`, and a cloneable cancellation handle. Defaults are batch size 1, concurrency 2, maximum 4, idle timeout 60 seconds, no batch timeout. Every invalid setter returns `DataError::Validation`.

- [ ] **Step 4: Run focused tests and API examples**

Run: `cargo test -p tqsdk-data --test backtest_history_api orchestration_`

Run: `cargo check -p tqsdk-data --examples`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/tqsdk-data/src/backtest_history/orchestration.rs crates/tqsdk-data/src/backtest_history/mod.rs crates/tqsdk-data/src/lib.rs crates/tqsdk-data/tests/backtest_history_api.rs
git commit -m "feat(data): expose history fill orchestration"
```

### Task 2: Execute all cache families through the orchestration API

**Files:**
- Modify: `crates/tqsdk-data/src/backtest_history/orchestration.rs`
- Modify: `crates/tqsdk-data/src/backtest_history/{mod.rs,fill.rs,executor.rs}`
- Test: `crates/tqsdk-data/tests/backtest_history_api.rs`
- Test: `crates/tqsdk-data/tests/daily_kline_cache.rs`

- [ ] **Step 1: Write failing behavior tests**

Use deterministic in-process fixtures to assert: one symbol failure does not cancel siblings; global cancellation marks unfinished symbols interrupted; idle timeout and optional batch timeout are distinct; telemetry is observable while materialization runs; daily requests preserve one exact missing range.

- [ ] **Step 2: Confirm RED**

Run: `cargo test -p tqsdk-data --test backtest_history_api orchestration_run_ -- --nocapture`

- [ ] **Step 3: Implement one bounded scheduler over existing runs**

Add a public `BacktestHistoryClient::orchestrate_fill(...)` that creates existing `materialize_cache_run` instances, drains event and telemetry streams concurrently, owns cancellation/timeout decisions, and returns a terminal report. Reuse existing root-gate and per-symbol locks. Authentication failure, root-lock failure, format conflict, and cancellation are global errors; request failures remain per-symbol results.

- [ ] **Step 4: Keep old APIs as compatibility wrappers**

`materialize_cache()` and facade helpers delegate to the new engine and preserve their old return/error surface. Do not duplicate request planning or create a second session.

- [ ] **Step 5: Verify data-layer behavior**

Run: `cargo test -p tqsdk-data --test backtest_history_api`

Run: `cargo test -p tqsdk-data --test daily_kline_cache`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/tqsdk-data/src/backtest_history crates/tqsdk-data/tests/backtest_history_api.rs crates/tqsdk-data/tests/daily_kline_cache.rs
git commit -m "feat(data): unify history cache fill scheduling"
```

### Task 3: Delegate facade tick and minute warmup

**Files:**
- Modify: `crates/tqsdk/src/backtest_remote.rs`
- Modify: `crates/tqsdk/src/lib.rs`
- Test: `crates/tqsdk/tests/facade_contract.rs`

- [ ] **Step 1: Add behavior-equivalence tests**

Cover current tick/minute batching, bounded concurrency, idle timeout, batch timeout, telemetry callback, partial failure, and cache-hit-without-auth behavior through the facade.

- [ ] **Step 2: Confirm RED against the new delegation seam**

Run: `cargo test -p tqsdk --test facade_contract facade_backtest_warmup_`

- [ ] **Step 3: Replace facade-owned scheduling with an adapter**

Map `RemoteFillRuntime` to `BacktestHistoryFillConfig`, translate normalized progress back to the existing callback, and keep the current `BacktestCacheWarmupReport` compatibility surface.

- [ ] **Step 4: Verify and commit**

Run: `cargo test -p tqsdk --test facade_contract facade_backtest_warmup_`

Run: `cargo test -p tqsdk backtest_kline`

```bash
git add crates/tqsdk/src/backtest_remote.rs crates/tqsdk/src/lib.rs crates/tqsdk/tests/facade_contract.rs
git commit -m "refactor(tqsdk): delegate history fill scheduling"
```

### Task 4: Native daily ordinary backtest path

**Files:**
- Modify: `crates/tqsdk/src/lib.rs`
- Modify: `crates/tqsdk-task/src/history_backtest_replay.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`
- Test: `crates/tqsdk/src/lib.rs`
- Test: `crates/tqsdk-task/tests/history_backtest_replay.rs`
- Test: `crates/tqsdk/tests/facade_contract.rs`

- [ ] **Step 1: Write failing source-selection and replay tests**

Assert `<60s` uses tick, `60s` uses canonical minute, `>60s && <1d` uses minute aggregation, `1d` uses native daily, and integral `2d..=28d` aggregates final native daily rows. Assert a daily cache miss in CacheOnly is a validation/cache error and never opens minute fallback.

- [ ] **Step 2: Confirm RED**

Run: `cargo test -p tqsdk backtest_kline`

Run: `cargo test -p tqsdk-task --test history_backtest_replay daily_`

- [ ] **Step 3: Add native daily planned inputs and replay sources**

Extend the source enum and prepared-input structures with daily requests. Open `DailyKlineCache` in `history_backtest_stream`, pass final rows to the task replay stream, and aggregate integer-day durations using the first native timestamp as the stable phase. Reject non-integral durations at or above one day.

- [ ] **Step 4: Verify no fallback and public advanced export**

Expose orchestration types under `tqsdk::advanced::data`, not the default prelude. Test missing/corrupt daily cache fail-closed semantics.

- [ ] **Step 5: Verify and commit**

Run: `cargo test -p tqsdk-task --test history_backtest_replay`

Run: `cargo test -p tqsdk --test facade_contract`

```bash
git add crates/tqsdk/src/lib.rs crates/tqsdk-task/src/history_backtest_replay.rs crates/tqsdk-task/src/lib.rs crates/tqsdk/src/lib.rs crates/tqsdk-task/tests/history_backtest_replay.rs crates/tqsdk/tests/facade_contract.rs
git commit -m "feat(backtest): consume native daily cache"
```

### Task 5: CLI fill parity and schema-v3 reports

**Files:**
- Modify: `crates/tqsdk-cache/src/main.rs`
- Modify: `crates/tqsdk-cache/src/lib.rs`
- Test: `crates/tqsdk-cache/tests/cli.rs`

- [ ] **Step 1: Write failing CLI tests**

Assert daily accepts `--symbol-batch-size`, `--symbol-concurrency`, `--idle-timeout-secs`, `--batch-timeout-secs`, and `--lock-wait-secs`; invalid values fail validation; JSONL progress contains planning, symbol/batch progress, and one terminal event; SIGINT persists an interrupted report.

- [ ] **Step 2: Add report compatibility fixtures**

Read tick v1/v2, minute v1, and daily v1 fixtures, then assert all new reports serialize schema v3 with `cache_kind`, normalized terminal state, symbols, requested ranges, rows, error, and interruption fields. Default tick reports move to `reports/tick/`; minute and daily stay in their family directories.

- [ ] **Step 3: Confirm RED**

Run: `cargo test -p tqsdk-cache --test cli daily_fill_ report_`

- [ ] **Step 4: Route all fill kinds through one progress runner**

Make CLI parsing family-neutral, map settings to the public data-layer config, bind Ctrl-C to the cancellation handle, and atomically save complete/failed/interrupted reports after planning. Per-symbol failures continue siblings and exit non-zero after report persistence.

- [ ] **Step 5: Verify and commit**

Run: `cargo test -p tqsdk-cache --test cli fill_`

Run: `cargo test -p tqsdk-cache`

```bash
git add crates/tqsdk-cache/src/main.rs crates/tqsdk-cache/src/lib.rs crates/tqsdk-cache/tests/cli.rs
git commit -m "feat(cache-cli): unify fill progress and reports"
```

### Task 6: Operations parity for daily and tick range purge

**Files:**
- Modify: `crates/tqsdk-data/src/backtest_tick_cache.rs`
- Modify: `crates/tqsdk-data/src/daily_kline_cache.rs`
- Modify: `crates/tqsdk-cache/src/{main.rs,lib.rs}`
- Test: `crates/tqsdk-data/tests/backtest_tick_cache_ops.rs`
- Test: `crates/tqsdk-data/tests/daily_kline_cache.rs`
- Test: `crates/tqsdk-cache/tests/cli.rs`

- [ ] **Step 1: Write failing operation tests**

Assert daily inventory reads only file prefixes, doctor fully decodes and checksums, `--kind all inventory/doctor` includes daily, daily purge remains whole-symbol only, and tick purge removes only requested trading-day partitions. Every real purge requires `--yes` and the root consistency lock.

- [ ] **Step 2: Confirm RED**

Run: `cargo test -p tqsdk-cache --test cli inventory_ doctor_ purge_`

- [ ] **Step 3: Implement adapters over cache-owned APIs**

Use embedded `.tqdk` logical symbols as inventory authority and `DailyKlineCache::diagnose()` for doctor. Add a trading-day-range purge method to `BacktestTickCache`; do not rewrite surviving tick rows or change daily layout.

- [ ] **Step 4: Unify verify/readback**

Make cache-only report-bound verification use the same symbol/range selectors for all families and never require credentials on a complete cache hit.

- [ ] **Step 5: Verify and commit**

Run: `cargo test -p tqsdk-data --test backtest_tick_cache_ops`

Run: `cargo test -p tqsdk-data --test daily_kline_cache`

Run: `cargo test -p tqsdk-cache --test cli`

```bash
git add crates/tqsdk-data/src/backtest_tick_cache.rs crates/tqsdk-data/src/daily_kline_cache.rs crates/tqsdk-data/tests crates/tqsdk-cache/src crates/tqsdk-cache/tests/cli.rs
git commit -m "feat(cache-cli): align daily cache operations"
```

### Task 7: Documentation and release-grade verification

**Files:**
- Modify: `README.md`
- Modify: `docs/README.md`
- Modify: `docs/architecture/README.md`
- Modify: `docs/architecture/crate-boundaries.md`
- Modify: `docs/architecture/api-data.md`
- Modify: `docs/architecture/backtest-tick-cache-cli.md`
- Modify: `docs/architecture/backtest-tick-cache-operations.md`
- Modify: `docs/architecture/history-cache-format.md`
- Modify: `docs/architecture/validation.md`
- Modify: `crates/tqsdk/README.md`
- Modify: `crates/tqsdk-data/README.md`
- Modify: `crates/tqsdk-cache/README.md`

- [ ] **Step 1: Synchronize authoritative contracts**

Document the three-tier source policy, no minute fallback for daily, orchestration ownership, all CLI flags/defaults/limits, report v3 compatibility, operation parity, single-file daily layout, and the explicit exclusion of settlement/limit prices.

- [ ] **Step 2: Run formatting and focused validation**

Run: `cargo fmt --all --check`

Run: `cargo test -p tqsdk-data`

Run: `cargo test -p tqsdk-task --test history_backtest_replay`

Run: `cargo test -p tqsdk-cache`

Run: `cargo test -p tqsdk --test facade_contract`

Run: `cargo clippy -p tqsdk-data -p tqsdk-task -p tqsdk-cache -p tqsdk --all-targets -- -D warnings`

- [ ] **Step 3: Run workspace validation and change detection**

Run: `cargo test`

Run: `cargo check --examples`

Run: `git diff --check`

Run: `gitnexus detect-changes --scope staged`

- [ ] **Step 4: Perform credential-gated realistic smoke when credentials exist**

Fill only the requested `KQ.i@SHFE.au` interval, remove auth variables, then reread the identical interval CacheOnly. Report measured rows, coverage, report path, and exit codes. If credentials are absent, record the smoke as not run rather than claiming success.

- [ ] **Step 5: Commit documentation**

```bash
git add README.md docs crates/tqsdk/README.md crates/tqsdk-data/README.md crates/tqsdk-cache/README.md
git commit -m "docs: define unified daily cache contract"
```

## Self-review

- Every frozen requirement maps to Tasks 1-7.
- Storage migration, settlement price, upper/lower limit price, live-session daily cache, automatic eviction, and minute fallback are intentionally excluded.
- Public API ownership stays in `tqsdk-data`; facade and CLI remain adapters.
- Destructive operations stay explicit, root-locked, and `--yes` gated.
- Existing public APIs remain compatibility wrappers while new orchestration types live only in `tqsdk::advanced::data`.
