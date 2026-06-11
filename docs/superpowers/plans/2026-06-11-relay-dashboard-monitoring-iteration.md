# Relay Dashboard Monitoring Iteration Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans`
> or `superpowers:subagent-driven-development` when implementing this plan.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 relay dashboard 从“行情活动展示”迭代为可信的断流、覆盖和 tick 完整性监控器。优先修正会让页面在异常时显示健康的监控语义问题，再处理 UI 体验和 CI 护栏。

**Source inputs:**

- `dashboard优化.md`
- 当前代码：
  - `crates/tqsdk-relay/src/symbol_metrics.rs`
  - `crates/tqsdk-relay/src/engine.rs`
  - `crates/tqsdk-relay/src/metrics_http.rs`
  - `crates/tqsdk-relay/dashboard-ui/src/**`
  - `crates/tqsdk-relay/tests/symbol_metrics.rs`
  - `crates/tqsdk-relay/dashboard-ui/src/lib/*.test.ts`
- 当前架构文档：
  - `crates/tqsdk-relay/README.md`
  - `docs/architecture/validation.md`
  - `docs/architecture/market-diff-quote-tick.md`

**Non-goals:**

- 不把 dashboard 改成下游 market websocket 客户端。
- 不让 dashboard 创建订阅或发送行情命令。
- 不引入独立 dashboard 服务、SvelteKit、SSR 或大型图表框架。
- 不把历史事件持久化到磁盘；本计划只做进程内固定容量账本。

**Global guardrails:**

- 修改任何 Rust / TS 函数前，先对目标符号跑 GitNexus upstream impact analysis。
- `HIGH` / `CRITICAL` 风险先停下汇报，不继续编辑。
- 每个迭代用独立提交收口；只 stage 本迭代相关文件。
- 修改 dashboard UI 后必须刷新并提交 `crates/tqsdk-relay/src/dashboard-dist/**`。
- 每次 contract 字段变化同步更新 `crates/tqsdk-relay/README.md` 和 `docs/architecture/validation.md`。

## Iteration 0: Baseline And Contract Fixtures

**Goal:** 固定当前错误语义的可复现测试，防止后续重构误判通过。

**Files:**

- Modify `crates/tqsdk-relay/tests/symbol_metrics.rs`
- Modify `crates/tqsdk-relay/dashboard-ui/src/test/fixtures.ts`
- Modify `crates/tqsdk-relay/dashboard-ui/src/lib/integrity-model.test.ts`
- Modify `crates/tqsdk-relay/dashboard-ui/src/lib/api.test.ts`

- [x] Add failing backend tests for:
  - summary remains global when query filters / limits symbols.
  - subscribed uncovered symbol remains coverage problem during closed session.
  - historical telemetry outside current universe and current subscriptions does not dominate current snapshot.
- [x] Add failing frontend tests for:
  - `deriveIntegrity()` does not compute global health from filtered rows.
  - filtered search cannot turn global `critical` into `healthy`.
  - invalid row count is not double-counted as many separate incidents.
- [x] Record expected existing failures from baseline evidence.
  - Original pre-edit test run was missed; baseline evidence was reconstructed from `HEAD`.
  - Backend baseline: `symbol_metrics.rs` included historical telemetry keys in the current symbol set, classified no-sample symbols through session fallback, had no `filtered_total`, and had no continuity counters. The new Iteration 0/2/4/5 tests would fail on those contracts.
  - Frontend baseline: `api.ts` fetched `/metrics` and `/symbol-metrics` with `Promise.all`, and `deriveIntegrity()` computed global health from filtered `snapshot.symbols`, used a 30s stale threshold for global frame idle, and counted active invalid rows into `issueCount`. The new Iteration 0/1/3 tests would fail on those contracts.

**Validation:**

```bash
cargo test -p tqsdk-relay --test symbol_metrics
cd crates/tqsdk-relay/dashboard-ui
pnpm run test
```

Expected before implementation: new tests fail for the documented reasons.

## Iteration 1: Atomic Dashboard Snapshot Contract

**Goal:** Split global monitoring truth from filtered list rows and avoid `/metrics` plus `/symbol-metrics` non-atomic combinations.

**Architecture:**

- Add one atomic endpoint:

```text
GET /dashboard-snapshot
```

- Keep `/metrics` and `/symbol-metrics` for compatibility.
- New snapshot contains:
  - `metrics`: current `MetricsSnapshot`.
  - `global`: unfiltered global health summary.
  - `page`: filtered / sorted / limited symbol rows for the current table.
  - `received_at_unix_millis`: server-side snapshot time.
- UI uses `global` for KPI, score, top health, timeline, and incident inputs.
- UI uses `page.symbols` only for visible list/table rendering.

**Files:**

- Modify `crates/tqsdk-relay/src/symbol_metrics.rs`
- Modify `crates/tqsdk-relay/src/engine.rs`
- Modify `crates/tqsdk-relay/src/metrics_http.rs`
- Modify `crates/tqsdk-relay/dashboard-ui/src/lib/types.ts`
- Modify `crates/tqsdk-relay/dashboard-ui/src/lib/api.ts`
- Modify `crates/tqsdk-relay/dashboard-ui/src/lib/integrity-model.ts`
- Modify `crates/tqsdk-relay/dashboard-ui/src/App.svelte`
- Modify `crates/tqsdk-relay/README.md`

- [x] Add `DashboardSnapshot` / `DashboardGlobalSummary` structs.
- [x] Ensure summary counts are computed before filtering and exposed without relying on `symbols`.
- [x] Change UI fetch from `Promise.all(['/metrics', '/symbol-metrics'])` to one `/dashboard-snapshot?...`.
- [x] Replace `setInterval()` polling with `await load(); setTimeout(nextLoad, 2000)` so requests do not overlap.
- [x] Preserve `/symbol-metrics` query behavior for existing tests and external debugging.

**Exit criteria:**

- Filtering status/search/limit never changes top-level overall severity, coverage, score, global timeline, or incident inputs.
- A single refresh cannot combine metrics from one engine state with symbols from another.

**Validation:**

```bash
cargo test -p tqsdk-relay --test symbol_metrics
cargo test -p tqsdk-relay --test binary_smoke relay_binary_serves_embedded_dashboard_assets
cd crates/tqsdk-relay/dashboard-ui
pnpm run test
pnpm run check
pnpm run build
```

## Iteration 2: Tick Continuity Telemetry

**Goal:** Detect confirmed tick gaps, duplicates, and out-of-order rows instead of only detecting receive silence.

**Architecture:**

- Add per-symbol continuity state:

```rust
struct TickContinuityTelemetry {
    source_epoch: u64,
    last_tick_id: Option<i64>,
    gap_event_count: u64,
    estimated_missing_rows: u64,
    duplicate_rows: u64,
    out_of_order_rows: u64,
    last_gap_unix_millis: Option<u64>,
}
```

- Only compare row IDs inside the same `source_epoch`.
- Increment `source_epoch` when upstream source is rebuilt or chart source changes.
- Expose continuity fields in symbol snapshot and global summary.

**Files:**

- Modify `crates/tqsdk-relay/src/symbol_metrics.rs`
- Modify `crates/tqsdk-relay/src/engine.rs`
- Modify upstream reconnect / chart rebuild call sites that own source epoch changes.
- Modify `crates/tqsdk-relay/dashboard-ui/src/lib/types.ts`
- Modify `crates/tqsdk-relay/dashboard-ui/src/lib/integrity-model.ts`
- Modify dashboard components that show integrity counters.

- [x] Add tests for sequential, gap, duplicate, out-of-order, and epoch reset cases.
- [x] Add global counters: `gap_event_count`, `estimated_missing_rows`, `duplicate_rows`, `out_of_order_rows`.
- [x] Make dashboard severity distinguish suspected silence from confirmed tick integrity failure.
- [x] Add incident ledger input for `SymbolGapDetected`.

**Exit criteria:**

- Missing tick IDs produce a visible confirmed integrity problem.
- Reconnect epoch reset does not falsely mark the first post-reconnect row as a gap.

**Validation:**

```bash
cargo test -p tqsdk-relay --test symbol_metrics
cargo test -p tqsdk-relay --tests
cd crates/tqsdk-relay/dashboard-ui
pnpm run test
pnpm run check
pnpm run build
```

## Iteration 3: Fast Flow Idle And Recoverable Decode Health

**Goal:** Split global flow idle thresholds from per-symbol stale thresholds, and make decode errors recoverable.

**Architecture:**

- Keep per-symbol stale threshold around the existing `data_stale_after_secs` contract.
- Add global flow fields with millisecond or monotonic precision:
  - `last_upstream_frame_monotonic_ns` or server-computed idle ms.
  - `last_decoded_event_monotonic_ns` or server-computed idle ms.
  - warning / critical thresholds for frame idle and decoded event idle.
- Split invalid row telemetry:
  - `lifetime_invalid_rows`
  - `recent_invalid_rows_1m`
  - `current_decode_health`
  - `last_invalid_row_at`

**Files:**

- Modify `crates/tqsdk-relay/src/engine.rs`
- Modify `crates/tqsdk-relay/src/symbol_metrics.rs`
- Modify `crates/tqsdk-relay/dashboard-ui/src/lib/types.ts`
- Modify `crates/tqsdk-relay/dashboard-ui/src/lib/integrity-model.ts`
- Modify `crates/tqsdk-relay/dashboard-ui/src/components/MetricCard.svelte`
- Modify `crates/tqsdk-relay/dashboard-ui/src/components/IntegrityHero.svelte`

- [x] Add backend tests for frame idle warning above 2s and critical above 5s.
- [x] Add backend tests for event idle warning above 3s and critical above 8s.
- [x] Add frontend tests proving 30s symbol stale threshold no longer controls global frame-flow alarm.
- [x] Add decode-health tests: old bad row remains lifetime count but current health recovers after quiet window.

**Exit criteria:**

- Live upstream frame silence alerts within seconds, not after 30 seconds.
- One historical decode error does not permanently force dashboard critical/warning.

**Validation:**

```bash
cargo test -p tqsdk-relay --test symbol_metrics
cargo test -p tqsdk-relay --tests
cd crates/tqsdk-relay/dashboard-ui
pnpm run test
pnpm run check
pnpm run build
```

## Iteration 4: Orthogonal Symbol State And Session Correctness

**Goal:** Stop compressing coverage, session, flow, and integrity into one `status` enum.

**Architecture:**

- Add orthogonal fields:

```text
coverage: covered | uncovered
session: open | closed | unknown
flow: flowing | silent | disconnected | no_sample
integrity: intact | suspected | confirmed_gap
```

- Keep legacy `status` temporarily as a derived compatibility field.
- Force Asia/Shanghai interpretation for exchange session calculations.
- Treat unavailable sample as `unknown` / `no_sample`, not `closed`.
- Defer full exchange trading calendar to separate data-source task if no authoritative calendar exists yet, but keep API shaped for it.

**Files:**

- Modify `crates/tqsdk-relay/src/symbol_metrics.rs`
- Modify `crates/tqsdk-relay/dashboard-ui/src/lib/types.ts`
- Modify `crates/tqsdk-relay/dashboard-ui/src/lib/timeline.ts`
- Modify `crates/tqsdk-relay/dashboard-ui/src/components/ContinuityTimeline.svelte`
- Modify `docs/architecture/market-diff-quote-tick.md`
- Modify `crates/tqsdk-relay/README.md`

- [x] Add backend test: subscribed uncovered symbol during closed session still reports `coverage=uncovered`.
- [x] Add backend test: server local timezone cannot shift China futures session by host timezone.
- [x] Add frontend test: no bucket sample renders `unknown`, not `closed`.
- [x] Add timeline legend and colors for `unknown` / `no_sample`.

**Exit criteria:**

- Coverage problems remain visible even while market session is closed.
- Timeline visually distinguishes no data, no sample, closed, warning, and bad.

**Validation:**

```bash
cargo test -p tqsdk-relay --test symbol_metrics
cd crates/tqsdk-relay/dashboard-ui
pnpm run test
pnpm run check
pnpm run build
pnpm run test:e2e
```

## Iteration 5: Observation Read Model And Current Universe Scope

**Goal:** Reduce dashboard HTTP work inside the `RelayEngine` mutex and stop historical telemetry from polluting current health.

**Architecture:**

- Current health symbol set is:

```text
current universe union current downstream subscriptions
```

- Old telemetry without current universe membership and without current subscription moves to event/history ledger, not current health.
- Snapshot pipeline:
  - lock: copy lightweight raw telemetry and subscription snapshot.
  - unlock: classify, summarize, filter, sort, truncate, serialize.

**Files:**

- Modify `crates/tqsdk-relay/src/symbol_metrics.rs`
- Modify `crates/tqsdk-relay/src/engine.rs`
- Modify `crates/tqsdk-relay/src/metrics_http.rs`
- Modify tests under `crates/tqsdk-relay/tests/observability.rs` if endpoint behavior changes.

- [x] Add tests proving removed universe symbols disappear from current health when unsubscribed.
- [x] Add tests proving subscribed old symbols still appear as coverage problems.
- [x] Refactor lock boundary so expensive query work happens outside the engine mutex.
- [x] Add fixed-capacity backend event ledger skeleton for universe changes and flow incidents.

**Exit criteria:**

- Long-running relay does not let stale historical contracts dominate default dashboard rows.
- `/dashboard-snapshot` has one short engine lock section.

**Validation:**

```bash
cargo test -p tqsdk-relay --test symbol_metrics
cargo test -p tqsdk-relay --tests
cd crates/tqsdk-relay/dashboard-ui
pnpm run test
pnpm run check
pnpm run build
```

## Iteration 6: UI Truthfulness And CI Guardrails

**Goal:** Remove misleading UI affordances and make built dashboard assets impossible to forget.

**Files:**

- Modify `crates/tqsdk-relay/dashboard-ui/src/components/MetricCard.svelte`
- Modify `crates/tqsdk-relay/dashboard-ui/src/components/MonitorHeader.svelte`
- Modify `crates/tqsdk-relay/dashboard-ui/src/components/SymbolHealthTable.svelte`
- Modify `crates/tqsdk-relay/dashboard-ui/src/App.svelte`
- Modify `.github/workflows/**` if workflow files exist for CI.
- Modify `crates/tqsdk-relay/dashboard-ui/package.json`
- Modify `crates/tqsdk-relay/dashboard-ui/pnpm-lock.yaml`
- Modify `docs/architecture/validation.md`

- [x] Replace fake static KPI sparkline with real history or remove it.
- [x] Implement real fullscreen via `requestFullscreen()` / `exitFullscreen()` with graceful unsupported state.
- [x] Add full symbol drilldown/table path for all filtered rows, not only first 24/30/8 items.
- [x] Debounce search input before network reload.
- [x] Pin dashboard dev dependency major versions instead of `"latest"`.
- [x] Add CI steps:
  - `pnpm install --frozen-lockfile`
  - `pnpm run check`
  - `pnpm run test`
  - `pnpm run build`
  - `git diff --exit-code ../src/dashboard-dist`
  - `pnpm run test:e2e`

**Exit criteria:**

- Dashboard shows no decorative trend that can be mistaken for telemetry.
- CI fails if Svelte source and embedded `dashboard-dist` drift.
- Operator can inspect every affected symbol from the UI.

**Validation:**

```bash
cd crates/tqsdk-relay/dashboard-ui
pnpm install --frozen-lockfile
pnpm run check
pnpm run test
pnpm run build
git diff --exit-code ../src/dashboard-dist
pnpm run test:e2e
cd ../../..
cargo test -p tqsdk-relay --test binary_smoke relay_binary_serves_embedded_dashboard_assets
```

## Final Release Gate

- [x] Run GitNexus change detection:

```text
gitnexus_detect_changes(scope="all", repo="tqsdk-rust")
```

- [x] Run full relay + UI verification:

```bash
cargo test -p tqsdk-relay --tests
cargo clippy -p tqsdk-relay --all-targets -- -D warnings
cargo check -p tqsdk-relay --no-default-features
cd crates/tqsdk-relay/dashboard-ui
pnpm run check
pnpm run test
pnpm run build
pnpm run test:e2e
```

- [x] Run repo-level formatting smoke:

```bash
git diff --check
```

- [x] Confirm docs updated:
  - `crates/tqsdk-relay/README.md`
  - `docs/architecture/validation.md`
  - `docs/architecture/market-diff-quote-tick.md` if state semantics changed.

## Recommended Commit Boundaries

1. `test(relay): capture dashboard monitoring regressions`
2. `feat(relay): add atomic dashboard snapshot contract`
3. `feat(relay): track tick continuity gaps`
4. `fix(relay): split flow idle and decode health`
5. `feat(relay): expose orthogonal symbol health`
6. `perf(relay): move dashboard snapshot work off hot lock`
7. `fix(relay): make dashboard ui truthful and checked in ci`
