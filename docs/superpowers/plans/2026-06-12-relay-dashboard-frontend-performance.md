# Relay Dashboard Frontend Performance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 降低 `tqsdk-relay` 内置监控面板的首屏加载、轮询传输、前端派生计算、DOM/paint 成本，同时保持现有 relay 行情语义和只读 dashboard 边界。

**Architecture:** 保持当前 Svelte SPA + Rust embedded assets + relay metrics HTTP server 形态，不引入 SvelteKit、独立 dashboard 服务或图表框架。把全量 `global_symbols` 前端计算改为后端聚合快照，前端只保存 5 分钟聚合 timeline ring 和当前 page rows。JSON 观测接口继续 `no-store`，静态 dashboard assets 单独使用可缓存 header。

**Tech Stack:** Rust 2024, Tokio, serde, include_dir, Svelte 5, Vite 8, Tailwind v4, Vitest, Playwright.

---

## Scope And Guardrails

- 本计划只优化 relay dashboard 前端与其 HTTP contract。
- 不修改 SDK runtime contract、下游 market websocket 协议、上游订阅语义、trade/query/auth 边界。
- 修改 Rust/TS 函数前，先对目标符号跑 GitNexus upstream impact analysis；`HIGH` 或 `CRITICAL` 先停下汇报。
- 每个任务独立提交；只 stage 本任务相关文件。
- 修改 dashboard UI 后必须刷新并提交 `crates/tqsdk-relay/src/dashboard-dist/**`。
- HTTP dashboard contract 变化必须同步更新 `crates/tqsdk-relay/README.md` 与 `docs/architecture/validation.md`。

## File Structure

- Modify `crates/tqsdk-relay/src/engine.rs`
  - Add aggregate dashboard timeline DTO.
  - Stop returning full `global_symbols` in `/dashboard-snapshot`.
  - Keep filtered `page` payload for list/detail UI.
- Modify `crates/tqsdk-relay/src/metrics_http.rs`
  - Keep JSON responses `Cache-Control: no-store`.
  - Serve embedded static dashboard assets with cacheable headers.
- Modify `crates/tqsdk-relay/tests/observability.rs`
  - Add contract tests for aggregate timeline and missing `global_symbols`.
- Modify `crates/tqsdk-relay/tests/binary_smoke.rs`
  - Add HTTP header tests for static assets vs JSON endpoints.
- Modify `crates/tqsdk-relay/dashboard-ui/src/lib/types.ts`
  - Add `DashboardTimelineSample` and related frontend types.
  - Remove `global_symbols` from `DashboardSnapshot` / `RelaySnapshot`.
- Modify `crates/tqsdk-relay/dashboard-ui/src/lib/api.ts`
  - Keep single `/dashboard-snapshot` fetch path.
  - Decode new aggregate timeline fields.
- Modify `crates/tqsdk-relay/dashboard-ui/src/lib/integrity-model.ts`
  - Stop depending on unfiltered row arrays for global health.
- Modify `crates/tqsdk-relay/dashboard-ui/src/lib/timeline.ts`
  - Store aggregate timeline samples only by default.
  - Keep page-visible symbol history bounded to current visible rows.
- Modify `crates/tqsdk-relay/dashboard-ui/src/components/ContinuityTimeline.svelte`
  - Render exchange/global/subscribed aggregate rows from new timeline samples.
  - Key list rendering and reduce per-cell event handlers.
  - Reduce heavy paint styles in dense block mode.
- Modify `crates/tqsdk-relay/dashboard-ui/src/components/IncidentTable.svelte`
  - Prefer backend `events` for global incidents instead of local ledger over all symbols.
- Modify `crates/tqsdk-relay/dashboard-ui/src/App.svelte`
  - Remove `next.global_symbols` path.
  - Feed timeline/incident state from aggregate snapshot.
- Modify `crates/tqsdk-relay/dashboard-ui/vite.config.ts`
  - Enable production minification with Vite 8 supported minifier.
- Modify `crates/tqsdk-relay/dashboard-ui/package.json`
  - Add a local size-check script.
- Modify `crates/tqsdk-relay/README.md`
  - Document `/dashboard-snapshot` aggregate fields and static asset caching behavior.
- Modify `docs/architecture/validation.md`
  - Add relay dashboard frontend validation commands.

---

### Task 1: Baseline Tests For Snapshot Size And Static Headers

**Files:**
- Modify: `crates/tqsdk-relay/tests/observability.rs`
- Modify: `crates/tqsdk-relay/tests/binary_smoke.rs`

- [ ] **Step 1: Run impact analysis**

Run GitNexus upstream impact before edits:

```text
gitnexus_impact({ target: "DashboardSnapshotInputs::into_dashboard_snapshot", direction: "upstream" })
gitnexus_impact({ target: "write_bytes_response", direction: "upstream" })
```

Expected: affected surface is relay dashboard HTTP tests and embedded asset serving. If risk is `HIGH` or `CRITICAL`, stop and report.

- [ ] **Step 2: Add failing snapshot contract test**

Add to `crates/tqsdk-relay/tests/observability.rs`:

```rust
#[test]
fn engine_dashboard_snapshot_exposes_aggregate_timeline_without_global_symbol_rows() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let now = local_millis_at(9, 30, 0);
    engine.record_universe_refresh_success_for_symbols(
        ["SHFE.au2602", "DCE.m2609"],
        21,
        Some(32_000),
        None,
        now / 1_000 - 2,
    );
    engine
        .ingest_tick_at_for_test("SHFE.au2602", tick(1), now - 1_000)
        .unwrap();

    let query = tqsdk_relay::SymbolMetricsQuery {
        limit: Some(1),
        ..Default::default()
    };
    let dashboard = engine.dashboard_snapshot_at(now, &query);

    assert_eq!(dashboard.global.total, 2);
    assert_eq!(dashboard.page.symbols.len(), 1);
    assert_eq!(dashboard.timeline.global.total, 2);
    assert!(dashboard.timeline.exchanges.contains_key("SHFE"));
    assert!(dashboard.timeline.exchanges.contains_key("DCE"));
}
```

Expected before implementation: compile fails because `dashboard.timeline` does not exist.

- [ ] **Step 3: Add failing JSON shape test for removed `global_symbols`**

In existing `relay_binary_serves_health_and_metrics_json` in `crates/tqsdk-relay/tests/binary_smoke.rs`, replace the old `global_symbols` assertion with:

```rust
assert!(dashboard["timeline"]["global"]["total"].is_number());
assert!(dashboard.get("global_symbols").is_none());
```

Expected before implementation: test fails because `global_symbols` still exists and `timeline` is missing.

- [ ] **Step 4: Add failing static asset cache header test**

Extend `relay_binary_serves_embedded_dashboard_assets` in `crates/tqsdk-relay/tests/binary_smoke.rs`:

```rust
let js = wait_for_http_response(metrics_addr, "/dashboard/assets/app.js", &mut child);
assert!(js.starts_with("HTTP/1.1 200"));
assert!(js.contains("Content-Type: application/javascript; charset=utf-8"));
assert!(js.contains("Cache-Control: public, max-age=60"));
assert!(!js.contains("Cache-Control: no-store"));

let dashboard = wait_for_http_response(metrics_addr, "/dashboard-snapshot", &mut child);
assert!(dashboard.starts_with("HTTP/1.1 200"));
assert!(dashboard.contains("Cache-Control: no-store"));
```

Expected before implementation: JS asset response still contains `Cache-Control: no-store`.

- [ ] **Step 5: Run focused failing tests**

Run:

```bash
cargo test -p tqsdk-relay --test observability engine_dashboard_snapshot_exposes_aggregate_timeline_without_global_symbol_rows
cargo test -p tqsdk-relay --test binary_smoke relay_binary_serves_embedded_dashboard_assets
```

Expected: fail for missing timeline contract and current asset cache header.

- [ ] **Step 6: Commit baseline tests**

```bash
git add crates/tqsdk-relay/tests/observability.rs crates/tqsdk-relay/tests/binary_smoke.rs
git commit -m "test(relay): capture dashboard performance contracts"
```

---

### Task 2: Backend Aggregate Timeline Contract

**Files:**
- Modify: `crates/tqsdk-relay/src/engine.rs`
- Modify: `crates/tqsdk-relay/tests/observability.rs`
- Modify: `crates/tqsdk-relay/tests/binary_smoke.rs`

- [ ] **Step 1: Run impact analysis**

```text
gitnexus_impact({ target: "DashboardSnapshot", direction: "upstream" })
gitnexus_impact({ target: "DashboardSnapshotInputs::into_dashboard_snapshot", direction: "upstream" })
```

Expected: dashboard HTTP JSON and frontend tests affected. Stop on `HIGH` / `CRITICAL`.

- [ ] **Step 2: Add aggregate DTOs**

In `crates/tqsdk-relay/src/engine.rs`, near `DashboardSnapshot`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DashboardTimelineSeverity {
    Live,
    Closed,
    Warn,
    Bad,
    Unknown,
    NoSample,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DashboardTimelineScope {
    pub severity: DashboardTimelineSeverity,
    pub total: usize,
    pub problem: usize,
    pub receive_gap_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DashboardTimelineSample {
    pub global: DashboardTimelineScope,
    pub subscribed: DashboardTimelineScope,
    pub exchanges: BTreeMap<String, DashboardTimelineScope>,
}
```

- [ ] **Step 3: Change `DashboardSnapshot` shape**

Replace:

```rust
pub global_symbols: Vec<SymbolTelemetrySnapshot>,
```

with:

```rust
pub timeline: DashboardTimelineSample,
```

- [ ] **Step 4: Implement aggregate helpers**

Add helpers in `crates/tqsdk-relay/src/engine.rs`:

```rust
fn exchange_of(symbol: &str) -> String {
    symbol
        .split_once('.')
        .map(|(exchange, _)| exchange.to_ascii_uppercase())
        .unwrap_or_else(|| "UNKNOWN".to_string())
}

fn dashboard_scope_for<'a>(
    rows: impl IntoIterator<Item = &'a SymbolTelemetrySnapshot>,
) -> DashboardTimelineScope {
    let mut total = 0;
    let mut problem = 0;
    let mut bad = false;
    let mut warn = false;
    let mut all_closed = true;
    let mut all_no_sample = true;
    let mut max_receive_gap_ms = None::<u64>;

    for row in rows {
        total += 1;
        if row.problem {
            problem += 1;
        }
        bad |= matches!(row.problem_severity, crate::symbol_metrics::SymbolProblemSeverity::Bad);
        warn |= matches!(row.problem_severity, crate::symbol_metrics::SymbolProblemSeverity::Warn);
        all_closed &= matches!(row.session, crate::symbol_metrics::SymbolSession::Closed);
        all_no_sample &= matches!(row.flow, crate::symbol_metrics::SymbolFlow::NoSample);
        if let Some(gap) = row.receive_gap_ms {
            max_receive_gap_ms = Some(max_receive_gap_ms.map_or(gap, |current| current.max(gap)));
        }
    }

    let severity = if total == 0 {
        DashboardTimelineSeverity::Unknown
    } else if all_closed {
        DashboardTimelineSeverity::Closed
    } else if bad {
        DashboardTimelineSeverity::Bad
    } else if warn {
        DashboardTimelineSeverity::Warn
    } else if all_no_sample {
        DashboardTimelineSeverity::NoSample
    } else {
        DashboardTimelineSeverity::Live
    };

    DashboardTimelineScope {
        severity,
        total,
        problem,
        receive_gap_ms: max_receive_gap_ms,
    }
}

fn dashboard_timeline(rows: &[SymbolTelemetrySnapshot]) -> DashboardTimelineSample {
    let mut exchanges = BTreeMap::new();
    for row in rows {
        exchanges
            .entry(exchange_of(&row.symbol))
            .or_insert_with(Vec::new)
            .push(row);
    }

    DashboardTimelineSample {
        global: dashboard_scope_for(rows.iter()),
        subscribed: dashboard_scope_for(rows.iter().filter(|row| row.subscribed)),
        exchanges: exchanges
            .into_iter()
            .map(|(exchange, rows)| (exchange, dashboard_scope_for(rows.into_iter())))
            .collect(),
    }
}
```

- [ ] **Step 5: Wire snapshot generation**

In `DashboardSnapshotInputs::into_dashboard_snapshot`:

```rust
let global_page = self.symbol_metrics_snapshot(&SymbolMetricsQuery::default());
let timeline = dashboard_timeline(&global_page.symbols);
let page = self.symbol_metrics_snapshot(query);
DashboardSnapshot {
    received_at_unix_millis: self.received_at_unix_millis,
    metrics: self.metrics,
    global: global_page.summary,
    timeline,
    page,
    events: self.events,
}
```

- [ ] **Step 6: Run backend tests**

```bash
cargo test -p tqsdk-relay --test observability
cargo test -p tqsdk-relay --test binary_smoke relay_binary_serves_health_and_metrics_json
```

Expected: aggregate timeline tests pass; frontend still fails until Task 3.

- [ ] **Step 7: Commit backend contract**

```bash
git add crates/tqsdk-relay/src/engine.rs crates/tqsdk-relay/tests/observability.rs crates/tqsdk-relay/tests/binary_smoke.rs
git commit -m "feat(relay): aggregate dashboard timeline snapshot"
```

---

### Task 3: Frontend Consume Aggregate Timeline

**Files:**
- Modify: `crates/tqsdk-relay/dashboard-ui/src/lib/types.ts`
- Modify: `crates/tqsdk-relay/dashboard-ui/src/lib/timeline.ts`
- Modify: `crates/tqsdk-relay/dashboard-ui/src/lib/integrity-model.ts`
- Modify: `crates/tqsdk-relay/dashboard-ui/src/App.svelte`
- Modify: `crates/tqsdk-relay/dashboard-ui/src/components/ContinuityTimeline.svelte`
- Modify: `crates/tqsdk-relay/dashboard-ui/src/components/IncidentTable.svelte`
- Modify: `crates/tqsdk-relay/dashboard-ui/src/test/fixtures.ts`
- Modify: `crates/tqsdk-relay/dashboard-ui/src/lib/timeline.test.ts`
- Modify: `crates/tqsdk-relay/dashboard-ui/src/App.test.ts`

- [ ] **Step 1: Run impact analysis**

```text
gitnexus_impact({ target: "pushTimelineSample", direction: "upstream" })
gitnexus_impact({ target: "deriveIntegrity", direction: "upstream" })
gitnexus_impact({ target: "ContinuityTimeline", direction: "upstream" })
```

Expected: dashboard UI tests affected. Stop on `HIGH` / `CRITICAL`.

- [ ] **Step 2: Update frontend types**

In `types.ts`, add:

```ts
export type DashboardTimelineSeverity = 'live' | 'closed' | 'warn' | 'bad' | 'unknown' | 'no_sample';

export type DashboardTimelineScope = {
  severity: DashboardTimelineSeverity;
  total: number;
  problem: number;
  receive_gap_ms: number | null;
};

export type DashboardTimelineSample = {
  global: DashboardTimelineScope;
  subscribed: DashboardTimelineScope;
  exchanges: Record<string, DashboardTimelineScope>;
};
```

Change `DashboardSnapshot` and `RelaySnapshot`:

```ts
timeline: DashboardTimelineSample;
```

Remove:

```ts
global_symbols: SymbolRow[];
```

- [ ] **Step 3: Update fixtures**

In `src/test/fixtures.ts`, make `dashboardSnapshot(rows)` include:

```ts
timeline: {
  global: { severity: 'live', total: rows.length, problem: rows.filter((item) => item.problem).length, receive_gap_ms: 0 },
  subscribed: { severity: 'live', total: rows.filter((item) => item.subscribed).length, problem: rows.filter((item) => item.subscribed && item.problem).length, receive_gap_ms: 0 },
  exchanges: {
    SHFE: { severity: 'live', total: rows.filter((item) => item.symbol.startsWith('SHFE.')).length, problem: 0, receive_gap_ms: 0 },
    DCE: { severity: rows.some((item) => item.symbol.startsWith('DCE.') && item.problem) ? 'warn' : 'live', total: rows.filter((item) => item.symbol.startsWith('DCE.')).length, problem: rows.filter((item) => item.symbol.startsWith('DCE.') && item.problem).length, receive_gap_ms: 0 },
  },
}
```

- [ ] **Step 4: Replace timeline history shape**

In `timeline.ts`, change `TimelineSample` creation to accept backend aggregate:

```ts
export function pushTimelineSample(
  history: TimelineHistory,
  sampledAt: number,
  sample: DashboardTimelineSample,
): TimelineHistory {
  history.samples.push({ sampledAt, sample });
  history.samples = history.samples.filter((item) => item.sampledAt >= sampledAt - 300_000);
  return history;
}
```

Adjust `TimelineSample` type in `types.ts`:

```ts
export type TimelineSample = {
  sampledAt: number;
  sample: DashboardTimelineSample;
};
```

- [ ] **Step 5: Update `App.svelte` data flow**

Replace:

```ts
const nextModel = deriveIntegrity(next.metrics, next.page, next.receivedAt, model, next.global, next.global_symbols);
pushTimelineSample(timeline, nextModel);
updateIncidentLedger(incidents, nextModel);
```

with:

```ts
const nextModel = deriveIntegrity(next.metrics, next.page, next.receivedAt, model, next.global);
pushTimelineSample(timeline, next.receivedAt, next.timeline);
incidents.incidents = next.events.map((event) => ({
  id: String(event.sequence),
  at: event.at_unix_secs * 1_000,
  scope: event.kind,
  scope_symbol: event.kind,
  type: event.kind,
  detail: event.detail,
  impact: '全局',
  severity: event.kind === 'decode_incident' || event.kind === 'flow_incident' ? 'warn' : 'unknown',
}));
```

- [ ] **Step 6: Update `ContinuityTimeline.svelte` reads**

Use aggregate scopes:

```ts
function severityOf(definition: TimelineDefinition, sample: TimelineSample): TimelineSeverity {
  if (definition.kind === 'summary' && definition.key === 'global') return sample.sample.global.severity;
  if (definition.kind === 'summary' && definition.key === 'subscribed') return sample.sample.subscribed.severity;
  if (definition.kind === 'exchange') return sample.sample.exchanges[definition.exchange]?.severity ?? 'unknown';
  return 'unknown';
}

function latencyOf(definition: TimelineDefinition, sample: TimelineSample): number {
  if (definition.kind === 'summary' && definition.key === 'global') return sample.sample.global.receive_gap_ms ?? 0;
  if (definition.kind === 'summary' && definition.key === 'subscribed') return sample.sample.subscribed.receive_gap_ms ?? 0;
  if (definition.kind === 'exchange') return sample.sample.exchanges[definition.exchange]?.receive_gap_ms ?? 0;
  return 0;
}
```

- [ ] **Step 7: Update frontend tests**

In `timeline.test.ts`, assert aggregate history no longer stores per-symbol maps:

```ts
pushTimelineSample(history, NOW, {
  global: { severity: 'warn', total: 2, problem: 1, receive_gap_ms: 1_500 },
  subscribed: { severity: 'warn', total: 1, problem: 1, receive_gap_ms: 1_500 },
  exchanges: {
    SHFE: { severity: 'closed', total: 1, problem: 0, receive_gap_ms: null },
    DCE: { severity: 'warn', total: 1, problem: 1, receive_gap_ms: 1_500 },
  },
});
const buckets = timelineBuckets(history, NOW + 1, 60);
expect(buckets.at(-1)?.sample.exchanges.DCE.severity).toBe('warn');
expect('symbolSeverity' in (buckets.at(-1) as object)).toBe(false);
```

- [ ] **Step 8: Run frontend tests**

```bash
cd crates/tqsdk-relay/dashboard-ui
pnpm run test
pnpm run check
```

Expected: all dashboard UI tests pass with no `global_symbols` type usage.

- [ ] **Step 9: Commit frontend aggregate consumption**

```bash
git add crates/tqsdk-relay/dashboard-ui/src
git commit -m "refactor(relay-ui): consume aggregate dashboard timeline"
```

---

### Task 4: Static Asset Cache Headers

**Files:**
- Modify: `crates/tqsdk-relay/src/metrics_http.rs`
- Modify: `crates/tqsdk-relay/tests/binary_smoke.rs`

- [ ] **Step 1: Run impact analysis**

```text
gitnexus_impact({ target: "write_response", direction: "upstream" })
gitnexus_impact({ target: "write_bytes_response", direction: "upstream" })
```

Expected: metrics HTTP tests affected. Stop on `HIGH` / `CRITICAL`.

- [ ] **Step 2: Keep JSON no-store**

Leave `write_response` unchanged:

```rust
Cache-Control: no-store\r\n\
```

- [ ] **Step 3: Cache static embedded assets**

Change `write_bytes_response` header:

```rust
let header = format!(
    "HTTP/1.1 {status} {}\r\n\
Content-Type: {content_type}\r\n\
Content-Length: {}\r\n\
Cache-Control: public, max-age=60\r\n\
X-Content-Type-Options: nosniff\r\n\
Connection: close\r\n\
\r\n",
    status_reason(status),
    body.len(),
);
```

Rationale: filenames are fixed (`assets/app.js`, `assets/app.css`), so do not use long immutable caching until filenames become content-hashed.

- [ ] **Step 4: Run HTTP tests**

```bash
cargo test -p tqsdk-relay --test binary_smoke relay_binary_metrics_responses_are_not_cacheable relay_binary_serves_embedded_dashboard_assets
```

Expected: JSON endpoint remains `no-store`; static JS/CSS use `public, max-age=60`.

- [ ] **Step 5: Commit cache header fix**

```bash
git add crates/tqsdk-relay/src/metrics_http.rs crates/tqsdk-relay/tests/binary_smoke.rs
git commit -m "perf(relay): cache embedded dashboard assets"
```

---

### Task 5: Production Minification And Build Budget

**Files:**
- Modify: `crates/tqsdk-relay/dashboard-ui/vite.config.ts`
- Modify: `crates/tqsdk-relay/dashboard-ui/package.json`
- Modify: `crates/tqsdk-relay/dashboard-ui/src/components/ContinuityTimeline.svelte`
- Modify generated: `crates/tqsdk-relay/src/dashboard-dist/**`

- [ ] **Step 1: Run impact analysis**

```text
gitnexus_impact({ target: "trimGeneratedTrailingWhitespace", direction: "upstream" })
```

Expected: only dashboard build output affected. Stop on `HIGH` / `CRITICAL`.

- [ ] **Step 2: Enable Vite 8 minify**

Change `vite.config.ts`:

```ts
build: {
  outDir: '../src/dashboard-dist',
  emptyOutDir: true,
  cssCodeSplit: false,
  minify: 'oxc',
  rollupOptions: {
    output: {
      entryFileNames: 'assets/app.js',
      assetFileNames: (assetInfo) => {
        if (assetInfo.name?.endsWith('.css')) return 'assets/app.css';
        return 'assets/[name][extname]';
      },
      chunkFileNames: 'assets/chunk-[name].js',
    },
  },
},
```

- [ ] **Step 3: Remove unused CSS selector warning**

In `ContinuityTimeline.svelte`, replace:

```css
.cell.closed_unmarked, .legend .closed_unmarked {
  background: transparent;
  box-shadow: none;
}
```

with:

```css
.cell.closed_unmarked {
  background: transparent;
  box-shadow: none;
}
```

- [ ] **Step 4: Add size check script**

In `package.json` scripts:

```json
"size": "node -e \"const fs=require('fs'); const js=fs.statSync('../src/dashboard-dist/assets/app.js').size; const css=fs.statSync('../src/dashboard-dist/assets/app.css').size; console.log(JSON.stringify({js,css})); if(js>180000) process.exit(1); if(css>60000) process.exit(1);\""
```

- [ ] **Step 5: Build and verify budget**

```bash
cd crates/tqsdk-relay/dashboard-ui
pnpm run build
pnpm run size
```

Expected:

```text
vite build succeeds
size script exits 0
app.js <= 180000 bytes
app.css <= 60000 bytes
```

- [ ] **Step 6: Commit build minification**

```bash
git add crates/tqsdk-relay/dashboard-ui/vite.config.ts crates/tqsdk-relay/dashboard-ui/package.json crates/tqsdk-relay/dashboard-ui/src/components/ContinuityTimeline.svelte crates/tqsdk-relay/src/dashboard-dist
git commit -m "perf(relay-ui): enable dashboard production minification"
```

---

### Task 6: Dense Timeline DOM And Paint Cost

**Files:**
- Modify: `crates/tqsdk-relay/dashboard-ui/src/components/ContinuityTimeline.svelte`
- Modify: `crates/tqsdk-relay/dashboard-ui/src/components/ContinuityTimeline.test.ts`
- Modify generated: `crates/tqsdk-relay/src/dashboard-dist/**`

- [ ] **Step 1: Run impact analysis**

```text
gitnexus_impact({ target: "ContinuityTimeline", direction: "upstream" })
```

Expected: dashboard UI tests affected. Stop on `HIGH` / `CRITICAL`.

- [ ] **Step 2: Key timeline rows and buckets**

Change outer row loop opening tag from:

```svelte
{#each definitions as definition}
```

to:

```svelte
{#each definitions as definition (definition.key)}
```

Change bucket loop opening tag from:

```svelte
{#each buckets as bucket}
```

to:

```svelte
{#each buckets as bucket, bucketIndex (bucketIndex)}
{/each}
```

- [ ] **Step 3: Remove no-op mouse handlers from non-symbol cells**

Replace dense cell rendering with:

```svelte
{#if definition.kind === 'symbol'}
  <span
    class={`cell ${cellClass(definition, bucket)}`}
    onmousemove={(e) => handleHover(definition, e)}
    onmouseleave={clearHover}
  ></span>
{:else}
  <span class={`cell ${cellClass(definition, bucket)}`}></span>
{/if}
```

- [ ] **Step 4: Reduce paint-heavy effects in dense mode**

Replace:

```css
.cell {
  height: 9px;
  border-radius: 1px;
  background: #1b3343;
  transition: filter 0.15s ease;
}

.cell:hover {
  filter: brightness(1.5) saturate(1.5) contrast(1.2);
}
```

with:

```css
.cell {
  height: 9px;
  border-radius: 1px;
  background: #1b3343;
}

.cell:hover {
  outline: 1px solid color-mix(in srgb, currentColor 70%, transparent);
  outline-offset: 1px;
}
```

- [ ] **Step 5: Add interaction regression test**

In `ContinuityTimeline.test.ts`, keep existing hover behavior for expanded symbol rows and add:

```ts
it('renders stable keyed heatmap cells after switching view modes', async () => {
  const { container } = render(ContinuityTimeline, {
    props: {
      buckets: Array.from({ length: 60 }, () => null),
      rows: [row({ symbol: 'SHFE.au2602', instrument_name: '沪金2602' })],
    },
  });

  expect(container.querySelectorAll('.cell').length).toBeGreaterThan(0);
  await fireEvent.click(screen.getByText('Sparkline'));
  expect(container.querySelector('svg')).toBeTruthy();
  await fireEvent.click(screen.getByText('Blocks'));
  expect(container.querySelectorAll('.cell').length).toBeGreaterThan(0);
});
```

- [ ] **Step 6: Run UI tests and rebuild**

```bash
cd crates/tqsdk-relay/dashboard-ui
pnpm run test
pnpm run check
pnpm run build
```

Expected: tests pass; generated dist updated.

- [ ] **Step 7: Commit DOM/paint optimization**

```bash
git add crates/tqsdk-relay/dashboard-ui/src/components/ContinuityTimeline.svelte crates/tqsdk-relay/dashboard-ui/src/components/ContinuityTimeline.test.ts crates/tqsdk-relay/src/dashboard-dist
git commit -m "perf(relay-ui): reduce timeline DOM paint cost"
```

---

### Task 7: Documentation And Full Validation

**Files:**
- Modify: `crates/tqsdk-relay/README.md`
- Modify: `docs/architecture/validation.md`

- [ ] **Step 1: Document dashboard snapshot contract**

In `crates/tqsdk-relay/README.md`, update HTTP observability section with:

```markdown
`/dashboard-snapshot` 返回一次原子 dashboard 观测快照：

- `metrics`：relay 全局 metrics。
- `global`：未筛选全局 symbol summary。
- `timeline`：后端聚合后的 global / subscribed / exchange 连续性样本。
- `page`：当前筛选、排序和 limit 后的 symbol rows。
- `events`：relay 事件账本。

为降低 2 秒轮询传输量，dashboard 不再返回全量 `global_symbols`；全局时间线由 `timeline` 聚合字段驱动。
```

- [ ] **Step 2: Document validation commands**

In `docs/architecture/validation.md`, add relay dashboard frontend validation:

````markdown
Relay dashboard contract / frontend performance:

```bash
cargo test -p tqsdk-relay --test observability
cargo test -p tqsdk-relay --test binary_smoke relay_binary_serves_health_and_metrics_json relay_binary_serves_embedded_dashboard_assets relay_binary_metrics_responses_are_not_cacheable
cd crates/tqsdk-relay/dashboard-ui
pnpm run test
pnpm run check
pnpm run build
pnpm run size
```
````

- [ ] **Step 3: Run full relay/dashboard validation**

```bash
cargo fmt --all --check
cargo test -p tqsdk-relay --tests
cargo check --workspace --examples
cd crates/tqsdk-relay/dashboard-ui
pnpm run test
pnpm run check
pnpm run build
pnpm run size
git diff --check
```

Expected: all commands pass.

- [ ] **Step 4: Run GitNexus change detection**

```text
gitnexus_detect_changes()
```

Expected: affected scope limited to relay dashboard HTTP contract, dashboard UI, docs, and generated dashboard dist.

- [ ] **Step 5: Commit docs**

```bash
git add crates/tqsdk-relay/README.md docs/architecture/validation.md
git commit -m "docs(relay): document dashboard performance contract"
```

---

## Execution Order

1. Task 1 baseline tests.
2. Task 2 backend aggregate contract.
3. Task 3 frontend aggregate consumption.
4. Task 4 static asset cache headers.
5. Task 5 production minification.
6. Task 6 DOM/paint reduction.
7. Task 7 docs and full validation.

## Expected End State

- `/dashboard-snapshot` no longer sends full unfiltered `global_symbols` every 2 seconds.
- Frontend timeline stores compact aggregate samples for 5 minutes instead of per-symbol maps for the whole universe.
- Static JS/CSS are not forced through `no-store`.
- Production dashboard JS is minified with Vite 8 supported `oxc` minifier.
- Dense heatmap mode has fewer per-cell event handlers and lighter paint styles.
- README and validation docs describe new dashboard contract and required checks.
