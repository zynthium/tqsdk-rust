# Relay Dashboard Svelte Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 `tqsdk-relay` 内置 dashboard 从手写单文件 HTML/CSS/JS 重构为 Svelte 5 + Vite + Tailwind CSS 4 的可维护前端工程，同时保持 relay 仍是自包含 Rust 单二进制。

**Architecture:** `/metrics` 和 `/symbol-metrics` 的后端 contract 不变；前端新增 `dashboard-ui/`，用 TypeScript 维护 API 类型、派生完整性模型、浏览器短期历史和 UI 状态。生产构建输出到 `crates/tqsdk-relay/src/dashboard-dist/`，Rust 侧改为嵌入并服务整个构建目录，而不是硬编码 `index.html` 和 `app.js` 两个常量。

**Tech Stack:** Rust 2024, Tokio, serde/serde\_json, include\_dir, Svelte 5 runes, TypeScript, Vite, Tailwind CSS 4 Vite plugin, Vitest, Playwright.

***

## Source Inputs

- `dashboard优化.md`
  - 采用 Svelte 5 + Vite + Tailwind CSS 4。
  - 不使用 SvelteKit，不引入 Router、Redux 类全局状态库、大型图表框架、SSR、hydration 或独立前端服务器。
  - Tailwind 负责布局、间距、响应式和基础状态样式；CSS variables 与 scoped CSS 负责大屏视觉、时间带、雷达、渐变、动画和 SVG。
  - 服务端仍输出单二进制；Node/Vite 只存在于构建阶段。
- 当前代码事实
  - `crates/tqsdk-relay/src/dashboard.rs` 只用 `include_str!` 暴露 `DASHBOARD_HTML` / `DASHBOARD_JS`。
  - `crates/tqsdk-relay/src/metrics_http.rs` 只服务 `/dashboard` 和 `/dashboard/app.js`。
  - `crates/tqsdk-relay/src/dashboard/index.html` 是 69 行内联 CSS/HTML。
  - `crates/tqsdk-relay/src/dashboard/app.js` 是 518 行手写 DOM 更新、轮询、派生模型、事件账本和图表绘制。
  - `crates/tqsdk-relay/tests/dashboard_logic.mjs` 通过 Node VM 直接执行旧 `app.js`。
  - `crates/tqsdk-relay/tests/binary_smoke.rs` 已覆盖 dashboard 资产服务。
- 关键架构约束
  - relay 是可选 market relay，不改变 SDK 默认直连路径。
  - dashboard 不连接 relay market websocket，不创建下游订阅，不触发额外行情命令。
  - quote freshness 与 tick serial 状态必须拆开表达，不能用 `ticks_ingested == 0` 单独判断断流。
  - tick / quote ingest 热路径只更新当前合约 telemetry；排序、过滤、JSON 生成只发生在 HTTP snapshot 请求侧。

## File Structure

- Create `crates/tqsdk-relay/dashboard-ui/package.json`
  - Node/Vite/Svelte/Tailwind 脚本与开发依赖，只作用于 relay dashboard。
- Create `crates/tqsdk-relay/dashboard-ui/pnpm-lock.yaml`
  - 锁定 dashboard UI 构建依赖。
- Create `crates/tqsdk-relay/dashboard-ui/vite.config.ts`
  - Svelte + Tailwind Vite plugin；`base: "/dashboard/"`；输出到 `../src/dashboard-dist`。
- Create `crates/tqsdk-relay/dashboard-ui/tsconfig.json`
  - Svelte + TypeScript 配置。
- Create `crates/tqsdk-relay/dashboard-ui/vitest.config.ts`
  - Vitest + Svelte 编译测试入口。
- Create `crates/tqsdk-relay/dashboard-ui/playwright.config.ts`
  - 本地静态预览 visual smoke。
- Create `crates/tqsdk-relay/dashboard-ui/index.html`
  - Vite HTML entry。
- Create `crates/tqsdk-relay/dashboard-ui/src/main.ts`
  - 挂载 `App.svelte`。
- Create `crates/tqsdk-relay/dashboard-ui/src/App.svelte`
  - 顶层 Svelte state、derived model、polling effect、layout。
- Create `crates/tqsdk-relay/dashboard-ui/src/lib/types.ts`
  - `/metrics`、`/symbol-metrics`、derived model、history、view state 类型。
- Create `crates/tqsdk-relay/dashboard-ui/src/lib/api.ts`
  - fetch、query string、dashboard API 错误处理。
- Create `crates/tqsdk-relay/dashboard-ui/src/lib/integrity-model.ts`
  - 从 `RelayMetrics` + `SymbolMetricsSnapshot` + history 派生完整性状态。
- Create `crates/tqsdk-relay/dashboard-ui/src/lib/history.ts`
  - browser-only 采样历史、sparkline、趋势线数据。
- Create `crates/tqsdk-relay/dashboard-ui/src/lib/timeline.ts`
  - 最近 5 分钟连续性 bucket。
- Create `crates/tqsdk-relay/dashboard-ui/src/lib/incident-ledger.ts`
  - 本页状态变化事件账本。
- Create `crates/tqsdk-relay/dashboard-ui/src/lib/format.ts`
  - 时间、持续时间、rate、数字格式化。
- Create `crates/tqsdk-relay/dashboard-ui/src/components/*.svelte`
  - `MonitorHeader`, `IntegrityHero`, `MetricCard`, `RelayPipeline`, `ContinuityTimeline`, `AttentionList`, `IncidentTable`, `SymbolHealthTable`, `IntegrityTrend`, `ScoreGauge`, `DashboardControls`。
- Create `crates/tqsdk-relay/dashboard-ui/src/styles/theme.css`
  - CSS variables、深色监控主题、status palette。
- Create `crates/tqsdk-relay/dashboard-ui/src/styles/app.css`
  - Tailwind import、全局布局、稳定尺寸约束。
- Create `crates/tqsdk-relay/dashboard-ui/src/test/fixtures.ts`
  - Vitest/Playwright 共用 fixture。
- Create `crates/tqsdk-relay/dashboard-ui/src/lib/*.test.ts`
  - pure model/history/timeline/incident/api tests。
- Create `crates/tqsdk-relay/dashboard-ui/tests/dashboard.spec.ts`
  - Playwright 资产和关键 UI smoke。
- Create `crates/tqsdk-relay/src/dashboard-dist/index.html`
  - Vite build artifact，提交进仓库，保证 Rust crate 无 Node runtime 也能编译。
- Create `crates/tqsdk-relay/src/dashboard-dist/assets/app.js`
  - Vite build artifact。
- Create `crates/tqsdk-relay/src/dashboard-dist/assets/app.css`
  - Vite build artifact。
- Modify `Cargo.toml`
  - 添加 workspace dependency `include_dir = "0.7"`。
- Modify `crates/tqsdk-relay/Cargo.toml`
  - 添加 `include_dir.workspace = true`。
- Modify `crates/tqsdk-relay/src/dashboard.rs`
  - 删除 `DASHBOARD_HTML` / `DASHBOARD_JS`，改为按 path 返回 embedded asset。
- Modify `crates/tqsdk-relay/src/metrics_http.rs`
  - `/dashboard`、`/dashboard/`、`/dashboard/assets/*` 走统一静态资源服务。
- Modify `crates/tqsdk-relay/tests/binary_smoke.rs`
  - 更新 dashboard asset assertions。
- Delete `crates/tqsdk-relay/src/dashboard/index.html`
  - 旧手写页面由 Svelte source + built dist 取代。
- Delete `crates/tqsdk-relay/src/dashboard/app.js`
  - 旧手写 DOM JS 由 Svelte source + built dist 取代。
- Delete `crates/tqsdk-relay/tests/dashboard_logic.mjs`
  - 纯逻辑测试迁移到 Vitest。
- Modify `crates/tqsdk-relay/README.md`
  - 记录 dashboard UI 构建、资产嵌入、验证命令。
- Modify `docs/architecture/validation.md`
  - 增加 relay dashboard UI 验证命令。

## Architecture Rules

- 不改 `SymbolMetricsSnapshot` JSON contract，除非另开计划同步 Rust tests、README、architecture docs。
- 不把前端状态写回 relay，不新增 dashboard mutation endpoint。
- 不引入 SvelteKit；`dashboard-ui` 是 Vite SPA。
- 不在 Rust 运行时解析 Vite manifest；本计划使用固定产物路径 + embedded directory。
- 不把 Node 依赖加到 workspace 根；依赖文件只在 `crates/tqsdk-relay/dashboard-ui/`。
- 不把 generated dist 当唯一 source of truth；手改只允许改 `dashboard-ui/src/**`，再运行 build 刷新 `src/dashboard-dist/**`。
- 任何改动 `serve_metrics_stream`、`dashboard_asset`、`deriveIntegrity`、`createIncidentLedger` 前，执行 GitNexus impact analysis；HIGH/CRITICAL 时先停下汇报风险。

***

### Task 1: Scaffold Dashboard UI Toolchain

**Files:**

- Create: `crates/tqsdk-relay/dashboard-ui/package.json`
- Create: `crates/tqsdk-relay/dashboard-ui/vite.config.ts`
- Create: `crates/tqsdk-relay/dashboard-ui/tsconfig.json`
- Create: `crates/tqsdk-relay/dashboard-ui/vitest.config.ts`
- Create: `crates/tqsdk-relay/dashboard-ui/index.html`
- Create: `crates/tqsdk-relay/dashboard-ui/src/main.ts`
- Create: `crates/tqsdk-relay/dashboard-ui/src/App.svelte`
- Create: `crates/tqsdk-relay/dashboard-ui/src/styles/theme.css`
- Create: `crates/tqsdk-relay/dashboard-ui/src/styles/app.css`
- [ ] **Step 1: Create failing empty build smoke**

Run:

```bash
cd crates/tqsdk-relay/dashboard-ui
pnpm run build
```

Expected: FAIL with `No such file or directory` or `ERR_PNPM_NO_IMPORTER_MANIFEST_FOUND`, because `package.json` does not exist yet.

- [ ] **Step 2: Add package manifest**

Create `crates/tqsdk-relay/dashboard-ui/package.json`:

```json
{
  "name": "@tqsdk-relay/dashboard-ui",
  "private": true,
  "type": "module",
  "scripts": {
    "dev": "vite --host 127.0.0.1",
    "build": "vite build",
    "preview": "vite preview --host 127.0.0.1",
    "test": "vitest run",
    "test:watch": "vitest",
    "test:e2e": "playwright test",
    "check": "svelte-check --tsconfig ./tsconfig.json"
  },
  "devDependencies": {
    "@playwright/test": "latest",
    "@sveltejs/vite-plugin-svelte": "latest",
    "@tailwindcss/vite": "latest",
    "@testing-library/svelte": "latest",
    "@tsconfig/svelte": "latest",
    "jsdom": "latest",
    "svelte": "latest",
    "svelte-check": "latest",
    "tailwindcss": "latest",
    "typescript": "latest",
    "vite": "latest",
    "vitest": "latest"
  }
}
```

- [ ] **Step 3: Add Vite config with fixed embedded output**

Create `crates/tqsdk-relay/dashboard-ui/vite.config.ts`:

```ts
/// <reference types="vitest" />

import { svelte } from '@sveltejs/vite-plugin-svelte';
import tailwindcss from '@tailwindcss/vite';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  base: '/dashboard/',
  plugins: [svelte(), tailwindcss()],
  server: {
    host: '127.0.0.1',
    port: 5173,
    proxy: {
      '/metrics': 'http://127.0.0.1:7789',
      '/symbol-metrics': 'http://127.0.0.1:7789',
    },
  },
  build: {
    outDir: '../src/dashboard-dist',
    emptyOutDir: true,
    cssCodeSplit: false,
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
  test: {
    environment: 'jsdom',
    globals: true,
    include: ['src/**/*.test.ts'],
  },
});
```

- [ ] **Step 4: Add TypeScript and Vitest config**

Create `crates/tqsdk-relay/dashboard-ui/tsconfig.json`:

```json
{
  "extends": "@tsconfig/svelte/tsconfig.json",
  "compilerOptions": {
    "target": "ES2022",
    "useDefineForClassFields": true,
    "module": "ESNext",
    "resolveJsonModule": true,
    "allowJs": false,
    "checkJs": false,
    "isolatedModules": true,
    "moduleDetection": "force",
    "strict": true,
    "types": ["vitest/globals", "svelte"]
  },
  "include": ["src/**/*.ts", "src/**/*.svelte", "tests/**/*.ts", "vite.config.ts", "vitest.config.ts", "playwright.config.ts"],
  "references": []
}
```

Create `crates/tqsdk-relay/dashboard-ui/vitest.config.ts`:

```ts
export { default } from './vite.config';
```

- [ ] **Step 5: Add minimal Vite entry**

Create `crates/tqsdk-relay/dashboard-ui/index.html`:

```html
<!doctype html>
<html lang="zh-CN">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <meta name="color-scheme" content="dark" />
    <title>tqsdk-relay 行情完整性监控中心</title>
  </head>
  <body>
    <div id="app"></div>
    <script type="module" src="/src/main.ts"></script>
  </body>
</html>
```

Create `crates/tqsdk-relay/dashboard-ui/src/main.ts`:

```ts
import './styles/app.css';
import App from './App.svelte';
import { mount } from 'svelte';

const target = document.getElementById('app');

if (!target) {
  throw new Error('dashboard root #app not found');
}

mount(App, { target });
```

Create `crates/tqsdk-relay/dashboard-ui/src/App.svelte`:

```svelte
<script lang="ts">
  import './styles/theme.css';
</script>

<main class="min-h-screen bg-[var(--relay-bg)] text-[var(--relay-text)]">
  <section class="mx-auto grid min-h-screen max-w-[1720px] grid-rows-[auto_1fr] gap-3 px-3 py-3">
    <h1 class="text-xl font-semibold tracking-normal">tqsdk-relay 行情完整性监控中心</h1>
    <p class="text-sm text-[var(--relay-muted)]">正在初始化监控面板</p>
  </section>
</main>
```

Create `crates/tqsdk-relay/dashboard-ui/src/styles/app.css`:

```css
@import "tailwindcss";

:root {
  color-scheme: dark;
  font-family: Inter, "PingFang SC", "Microsoft YaHei", system-ui, sans-serif;
  font-variant-numeric: tabular-nums;
}

html,
body,
#app {
  min-width: 1120px;
  min-height: 720px;
  margin: 0;
}

body {
  background: var(--relay-bg);
}
```

Create `crates/tqsdk-relay/dashboard-ui/src/styles/theme.css`:

```css
:root {
  --relay-bg: #061016;
  --relay-panel: #0d1d26;
  --relay-panel-strong: #102c36;
  --relay-line: #2b7180;
  --relay-text: #eef8fb;
  --relay-muted: #8bb0ba;
  --relay-live: #47d18c;
  --relay-closed: #6f8490;
  --relay-warn: #f3bc4e;
  --relay-bad: #f06373;
  --relay-info: #54b6d8;
}
```

- [ ] **Step 6: Install dependencies**

Run:

```bash
cd crates/tqsdk-relay/dashboard-ui
pnpm install
```

Expected: PASS and create `pnpm-lock.yaml`.

- [ ] **Step 7: Verify scaffold builds**

Run:

```bash
cd crates/tqsdk-relay/dashboard-ui
pnpm run build
```

Expected: PASS and create:

```text
../src/dashboard-dist/index.html
../src/dashboard-dist/assets/app.js
../src/dashboard-dist/assets/app.css
```

- [ ] **Step 8: Commit scaffold**

```bash
git add crates/tqsdk-relay/dashboard-ui crates/tqsdk-relay/src/dashboard-dist
git commit -m "build(relay): scaffold dashboard ui build"
```

***

### Task 2: Port API Types And Pure Integrity Model

**Files:**

- Create: `crates/tqsdk-relay/dashboard-ui/src/lib/types.ts`
- Create: `crates/tqsdk-relay/dashboard-ui/src/lib/format.ts`
- Create: `crates/tqsdk-relay/dashboard-ui/src/lib/api.ts`
- Create: `crates/tqsdk-relay/dashboard-ui/src/lib/integrity-model.ts`
- Create: `crates/tqsdk-relay/dashboard-ui/src/test/fixtures.ts`
- Create: `crates/tqsdk-relay/dashboard-ui/src/lib/integrity-model.test.ts`
- Create: `crates/tqsdk-relay/dashboard-ui/src/lib/api.test.ts`
- [ ] **Step 1: Write model tests before implementation**

Create `crates/tqsdk-relay/dashboard-ui/src/test/fixtures.ts`:

```ts
import type { RelayMetrics, SymbolMetricsSnapshot, SymbolRow } from '../lib/types';

export const NOW = 1_700_000_100_000;

export function metrics(overrides: Partial<RelayMetrics> = {}): RelayMetrics {
  return {
    upstream_stage: 'live',
    upstream_stage_started_unix_secs: 1_700_000_000,
    upstream_transport_connected: true,
    upstream_subscription_sent: true,
    last_upstream_frame_unix_secs: 1_700_000_099,
    upstream_frames_received: 10,
    upstream_events_decoded: 20,
    upstream_invalid_tick_rows: 0,
    upstream_symbols: 2,
    downstream_clients: 1,
    ticks_ingested: 20,
    quote_subscriptions: 1,
    chart_subscriptions: 0,
    data_stale_after_secs: 30,
    ...overrides,
  };
}

export function row(overrides: Partial<SymbolRow> = {}): SymbolRow {
  return {
    symbol: 'SHFE.au2602',
    instrument_name: '沪金2602',
    status: 'live',
    problem: false,
    problem_severity: 'live',
    in_universe: true,
    subscribed: false,
    quote_subscriber_count: 0,
    chart_subscriber_count: 0,
    ticks_ingested: 5,
    receive_gap_ms: 900,
    market_time_lag_ms: 1200,
    last_receive_unix_millis: NOW - 900,
    last_tick_datetime_ns: (NOW - 1200) * 1_000_000,
    last_price: 610.2,
    last_volume: 100,
    last_open_interest: 200,
    invalid_rows: 0,
    last_invalid_row_error: null,
    ...overrides,
  };
}

export function symbolSnapshot(rows: SymbolRow[]): SymbolMetricsSnapshot {
  return {
    now_unix_millis: NOW,
    data_stale_after_millis: 30_000,
    summary: {
      total: rows.length,
      live: rows.filter((item) => item.status === 'live').length,
      closed: rows.filter((item) => item.status === 'closed').length,
      stale: rows.filter((item) => item.status === 'stale').length,
      missing: rows.filter((item) => item.status === 'missing').length,
      inactive: rows.filter((item) => item.status === 'inactive').length,
      subscribed: rows.filter((item) => item.subscribed).length,
      p95_receive_gap_ms: rows.reduce<number | null>((max, item) => {
        if (item.receive_gap_ms == null) return max;
        return max == null ? item.receive_gap_ms : Math.max(max, item.receive_gap_ms);
      }, null),
    },
    symbols: rows,
  };
}
```

Create `crates/tqsdk-relay/dashboard-ui/src/lib/integrity-model.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { deriveIntegrity, severityForRow, statusLabel } from './integrity-model';
import { metrics, NOW, row, symbolSnapshot } from '../test/fixtures';

describe('deriveIntegrity', () => {
  it('keeps closed rows out of active problem count', () => {
    const closed = row({
      status: 'closed',
      problem: false,
      problem_severity: 'closed',
      invalid_rows: 7,
      receive_gap_ms: 90_000,
      market_time_lag_ms: 90_000,
    });
    const stale = row({
      symbol: 'DCE.m2609',
      instrument_name: '豆粕2609',
      status: 'stale',
      problem: true,
      problem_severity: 'warn',
      receive_gap_ms: 90_000,
    });

    const model = deriveIntegrity(metrics(), symbolSnapshot([closed, stale]), NOW);

    expect(severityForRow(closed)).toBe('closed');
    expect(model.problems.map((item) => item.symbol)).toEqual(['DCE.m2609']);
    expect(model.issueCount).toBe(1);
    expect(model.subscribedProblems).toHaveLength(0);
  });

  it('treats subscribed inactive rows as critical operational problems', () => {
    const inactive = row({
      symbol: 'CZCE.AP610',
      instrument_name: '苹果610',
      status: 'inactive',
      problem: true,
      problem_severity: 'bad',
      in_universe: false,
      subscribed: true,
      quote_subscriber_count: 1,
      receive_gap_ms: null,
      market_time_lag_ms: null,
      last_receive_unix_millis: null,
      last_tick_datetime_ns: null,
    });

    const model = deriveIntegrity(metrics(), symbolSnapshot([inactive]), NOW);

    expect(model.overall).toBe('critical');
    expect(model.subscribedProblems.map((item) => item.symbol)).toEqual(['CZCE.AP610']);
    expect(model.coverageRatio).toBe(0);
  });

  it('exposes startup warming state without false critical alarm', () => {
    const model = deriveIntegrity(
      metrics({
        upstream_stage: 'backfilling',
        last_upstream_frame_unix_secs: null,
        upstream_frames_received: 0,
        upstream_events_decoded: 0,
      }),
      symbolSnapshot([]),
      NOW,
    );

    expect(model.overall).toBe('warming');
    expect(model.upstreamIdleMs).toBeNull();
    expect(model.issueCount).toBe(0);
  });
});

describe('statusLabel', () => {
  it('maps backend status to Chinese labels', () => {
    expect(statusLabel('live')).toBe('正常');
    expect(statusLabel('closed')).toBe('休盘');
    expect(statusLabel('stale')).toBe('静默');
    expect(statusLabel('missing')).toBe('未收到');
    expect(statusLabel('inactive')).toBe('未纳入');
  });
});
```

Create `crates/tqsdk-relay/dashboard-ui/src/lib/api.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { symbolQueryString } from './api';

describe('symbolQueryString', () => {
  it('encodes dashboard filters using existing relay query contract', () => {
    expect(
      symbolQueryString({
        statuses: ['live', 'stale'],
        subscribedOnly: true,
        q: '沪金 2602',
        sort: 'receive_gap_ms_desc',
        limit: 200,
      }),
    ).toBe('status=live%2Cstale&subscribed=1&q=%E6%B2%AA%E9%87%91+2602&sort=receive_gap_ms_desc&limit=200');
  });

  it('omits empty optional filters', () => {
    expect(
      symbolQueryString({
        statuses: [],
        subscribedOnly: false,
        q: '',
        sort: 'symbol_asc',
        limit: 200,
      }),
    ).toBe('sort=symbol_asc&limit=200');
  });
});
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cd crates/tqsdk-relay/dashboard-ui
pnpm run test
```

Expected: FAIL because `types.ts`, `api.ts`, and `integrity-model.ts` are missing.

- [ ] **Step 3: Add shared types**

Create `crates/tqsdk-relay/dashboard-ui/src/lib/types.ts`:

```ts
export type UpstreamStage = 'connecting' | 'subscribing' | 'backfilling' | 'live' | 'degraded' | 'down';
export type SymbolStatus = 'live' | 'closed' | 'stale' | 'missing' | 'inactive';
export type ProblemSeverity = 'live' | 'closed' | 'warn' | 'bad';
export type OverallSeverity = 'healthy' | 'warning' | 'critical' | 'warming';

export type SymbolSort =
  | 'symbol_asc'
  | 'status_asc'
  | 'receive_gap_ms_desc'
  | 'market_time_lag_ms_desc'
  | 'ticks_ingested_desc';

export type RelayMetrics = {
  upstream_stage: UpstreamStage;
  upstream_stage_started_unix_secs: number | null;
  upstream_transport_connected: boolean;
  upstream_subscription_sent: boolean;
  last_upstream_frame_unix_secs: number | null;
  upstream_frames_received: number;
  upstream_events_decoded: number;
  upstream_invalid_tick_rows: number;
  upstream_symbols: number;
  downstream_clients: number;
  ticks_ingested: number;
  quote_subscriptions: number;
  chart_subscriptions: number;
  data_stale_after_secs: number;
};

export type SymbolMetricsSummary = {
  total: number;
  live: number;
  closed: number;
  stale: number;
  missing: number;
  inactive: number;
  subscribed: number;
  p95_receive_gap_ms: number | null;
};

export type SymbolRow = {
  symbol: string;
  instrument_name: string | null;
  status: SymbolStatus;
  problem: boolean;
  problem_severity: ProblemSeverity;
  in_universe: boolean;
  subscribed: boolean;
  quote_subscriber_count: number;
  chart_subscriber_count: number;
  ticks_ingested: number;
  receive_gap_ms: number | null;
  market_time_lag_ms: number | null;
  last_receive_unix_millis: number | null;
  last_tick_datetime_ns: number | null;
  last_price: number | null;
  last_volume: number | null;
  last_open_interest: number | null;
  invalid_rows: number;
  last_invalid_row_error: string | null;
};

export type SymbolMetricsSnapshot = {
  now_unix_millis: number;
  data_stale_after_millis: number;
  summary: SymbolMetricsSummary;
  symbols: SymbolRow[];
};

export type RelaySnapshot = {
  metrics: RelayMetrics;
  symbols: SymbolMetricsSnapshot;
  receivedAt: number;
};

export type DashboardFilters = {
  statuses: SymbolStatus[];
  subscribedOnly: boolean;
  q: string;
  sort: SymbolSort;
  limit: number;
};

export type DashboardViewState = {
  paused: boolean;
  fullscreen: boolean;
  selectedExchange: string | null;
  selectedSymbol: string | null;
  filters: DashboardFilters;
};

export type IntegrityModel = {
  overall: OverallSeverity;
  sampledAt: number;
  metrics: RelayMetrics;
  snapshot: SymbolMetricsSnapshot;
  rows: SymbolRow[];
  problems: SymbolRow[];
  subscribedProblems: SymbolRow[];
  issueCount: number;
  invalidRowCount: number;
  activeInvalidRowCount: number;
  upstreamIdleMs: number | null;
  coverageRatio: number;
  observedUniverse: number;
  totalUniverse: number;
  frameRate: number | null;
  eventRate: number | null;
  continuityScore: number;
};
```

- [ ] **Step 4: Add API helpers**

Create `crates/tqsdk-relay/dashboard-ui/src/lib/api.ts`:

```ts
import type { DashboardFilters, RelayMetrics, SymbolMetricsSnapshot } from './types';

export class DashboardApiError extends Error {
  constructor(
    public readonly path: string,
    public readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

export function symbolQueryString(filters: DashboardFilters): string {
  const params = new URLSearchParams();
  if (filters.statuses.length > 0) params.set('status', filters.statuses.join(','));
  if (filters.subscribedOnly) params.set('subscribed', '1');
  if (filters.q.trim()) params.set('q', filters.q.trim());
  params.set('sort', filters.sort);
  params.set('limit', String(filters.limit));
  return params.toString();
}

async function fetchJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(path, { cache: 'no-store', signal });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message = typeof body.error === 'string' ? body.error : `HTTP ${response.status}`;
    throw new DashboardApiError(path, response.status, message);
  }
  return body as T;
}

export async function fetchRelaySnapshot(filters: DashboardFilters, signal?: AbortSignal) {
  const query = symbolQueryString(filters);
  const [metrics, symbols] = await Promise.all([
    fetchJson<RelayMetrics>('/metrics', signal),
    fetchJson<SymbolMetricsSnapshot>(`/symbol-metrics?${query}`, signal),
  ]);
  return { metrics, symbols, receivedAt: Date.now() };
}
```

- [ ] **Step 5: Add format helpers**

Create `crates/tqsdk-relay/dashboard-ui/src/lib/format.ts`:

```ts
const numberFormatter = new Intl.NumberFormat('zh-CN');
const rateFormatter = new Intl.NumberFormat('zh-CN', { maximumFractionDigits: 1 });
const timeFormatter = new Intl.DateTimeFormat('zh-CN', {
  hour12: false,
  timeZone: 'Asia/Shanghai',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
});

export function formatNumber(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? '--' : numberFormatter.format(value);
}

export function formatRate(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? '--' : rateFormatter.format(value);
}

export function formatDuration(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '--';
  const ms = Math.max(0, value);
  if (ms < 1_000) return `${Math.round(ms)}ms`;
  if (ms < 60_000) return `${(ms / 1_000).toFixed(1)}s`;
  return `${Math.floor(ms / 60_000)}m${Math.floor((ms % 60_000) / 1_000)}s`;
}

export function formatTime(unixMillis: number | null | undefined): string {
  if (unixMillis == null || !Number.isFinite(unixMillis)) return '--';
  return timeFormatter.format(new Date(unixMillis));
}
```

- [ ] **Step 6: Add integrity model**

Create `crates/tqsdk-relay/dashboard-ui/src/lib/integrity-model.ts`:

```ts
import type { IntegrityModel, ProblemSeverity, RelayMetrics, SymbolMetricsSnapshot, SymbolRow, SymbolStatus } from './types';

const WARMING_STAGES = new Set(['connecting', 'subscribing', 'backfilling']);

export function statusLabel(status: SymbolStatus): string {
  return {
    live: '正常',
    closed: '休盘',
    stale: '静默',
    missing: '未收到',
    inactive: '未纳入',
  }[status];
}

export function severityForRow(row: SymbolRow): ProblemSeverity {
  return row.problem_severity;
}

export function frameIdleMs(metrics: RelayMetrics, nowMillis: number): number | null {
  if (metrics.last_upstream_frame_unix_secs == null) return null;
  return Math.max(0, nowMillis - metrics.last_upstream_frame_unix_secs * 1_000);
}

export function deriveIntegrity(
  metrics: RelayMetrics,
  snapshot: SymbolMetricsSnapshot,
  sampledAt: number,
  previous?: IntegrityModel | null,
): IntegrityModel {
  const rows = Array.isArray(snapshot.symbols) ? snapshot.symbols : [];
  const universeRows = rows.filter((row) => row.in_universe);
  const observedUniverse = universeRows.filter((row) => row.last_receive_unix_millis != null).length;
  const totalUniverse = universeRows.length || Number(metrics.upstream_symbols || snapshot.summary.total || 0);
  const coverageRatio = totalUniverse > 0 ? observedUniverse / totalUniverse : 0;
  const problems = rows.filter((row) => row.problem);
  const subscribedProblems = problems.filter((row) => row.subscribed);
  const invalidRowCount = Number(metrics.upstream_invalid_tick_rows || 0);
  const activeInvalidRowCount = rows.reduce((sum, row) => sum + (row.problem ? Number(row.invalid_rows || 0) : 0), 0);
  const upstreamIdleMs = frameIdleMs(metrics, sampledAt);
  const staleAfterMs = Number(snapshot.data_stale_after_millis || metrics.data_stale_after_secs * 1_000 || 30_000);
  const sourceCritical = metrics.upstream_stage === 'down' || metrics.upstream_stage === 'degraded';
  const idleCritical = upstreamIdleMs != null && upstreamIdleMs > staleAfterMs;
  const warming = WARMING_STAGES.has(metrics.upstream_stage);
  const elapsedSeconds = previous ? Math.max(0.001, (sampledAt - previous.sampledAt) / 1_000) : null;
  const frameRate = elapsedSeconds ? Math.max(0, (metrics.upstream_frames_received - previous!.metrics.upstream_frames_received) / elapsedSeconds) : null;
  const eventRate = elapsedSeconds ? Math.max(0, (metrics.upstream_events_decoded - previous!.metrics.upstream_events_decoded) / elapsedSeconds) : null;
  const issueCount = problems.length + activeInvalidRowCount;
  const continuityScore = Math.max(
    0,
    100 -
      Math.min(55, issueCount * 9) -
      Math.min(25, (1 - coverageRatio) * 25) -
      (sourceCritical || idleCritical ? 20 : 0),
  );
  const overall = sourceCritical || idleCritical || subscribedProblems.length > 0
    ? 'critical'
    : issueCount > 0 || coverageRatio < 0.98
      ? 'warning'
      : warming
        ? 'warming'
        : 'healthy';

  return {
    overall,
    sampledAt,
    metrics,
    snapshot,
    rows,
    problems,
    subscribedProblems,
    issueCount,
    invalidRowCount,
    activeInvalidRowCount,
    upstreamIdleMs,
    coverageRatio,
    observedUniverse,
    totalUniverse,
    frameRate,
    eventRate,
    continuityScore,
  };
}
```

- [ ] **Step 7: Run pure tests**

Run:

```bash
cd crates/tqsdk-relay/dashboard-ui
pnpm run test
```

Expected: PASS.

- [ ] **Step 8: Commit model layer**

```bash
git add crates/tqsdk-relay/dashboard-ui/src/lib crates/tqsdk-relay/dashboard-ui/src/test
git commit -m "test(relay): port dashboard integrity model"
```

***

### Task 3: Add Browser History, Timeline, And Incident Ledger

**Files:**

- Create: `crates/tqsdk-relay/dashboard-ui/src/lib/history.ts`
- Create: `crates/tqsdk-relay/dashboard-ui/src/lib/timeline.ts`
- Create: `crates/tqsdk-relay/dashboard-ui/src/lib/incident-ledger.ts`
- Create: `crates/tqsdk-relay/dashboard-ui/src/lib/history.test.ts`
- Create: `crates/tqsdk-relay/dashboard-ui/src/lib/timeline.test.ts`
- Create: `crates/tqsdk-relay/dashboard-ui/src/lib/incident-ledger.test.ts`
- Modify: `crates/tqsdk-relay/dashboard-ui/src/lib/types.ts`
- [ ] **Step 1: Add tests for runtime-only history**

Create `crates/tqsdk-relay/dashboard-ui/src/lib/history.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { createHistory, pushHistorySample, sparkPoints } from './history';
import { deriveIntegrity } from './integrity-model';
import { metrics, NOW, row, symbolSnapshot } from '../test/fixtures';

describe('history', () => {
  it('keeps bounded samples and produces stable sparkline points', () => {
    const history = createHistory(3);
    for (let index = 0; index < 5; index += 1) {
      const model = deriveIntegrity(metrics({ upstream_frames_received: index }), symbolSnapshot([row()]), NOW + index * 1000);
      pushHistorySample(history, model);
    }

    expect(history.samples).toHaveLength(3);
    expect(history.samples[0].sampledAt).toBe(NOW + 2_000);
    expect(sparkPoints([0, 5, 10], 100, 20)).toBe('0,20 50,10 100,0');
  });
});
```

Create `crates/tqsdk-relay/dashboard-ui/src/lib/timeline.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { createTimelineHistory, pushTimelineSample, timelineBuckets } from './timeline';
import { deriveIntegrity } from './integrity-model';
import { metrics, NOW, row, symbolSnapshot } from '../test/fixtures';

describe('timeline', () => {
  it('records exchange and subscribed continuity without treating closed as bad', () => {
    const history = createTimelineHistory();
    const model = deriveIntegrity(
      metrics(),
      symbolSnapshot([
        row({ symbol: 'SHFE.au2602', status: 'closed', problem: false, problem_severity: 'closed' }),
        row({ symbol: 'DCE.m2609', status: 'stale', problem: true, problem_severity: 'warn', subscribed: true }),
      ]),
      NOW,
    );

    pushTimelineSample(history, model);
    const buckets = timelineBuckets(history, NOW, 60);

    expect(buckets.at(-1)?.exchangeSeverity.SHFE).toBe('closed');
    expect(buckets.at(-1)?.exchangeSeverity.DCE).toBe('warn');
    expect(buckets.at(-1)?.subscribedSeverity).toBe('warn');
  });
});
```

Create `crates/tqsdk-relay/dashboard-ui/src/lib/incident-ledger.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { createIncidentLedger, updateIncidentLedger } from './incident-ledger';
import { deriveIntegrity } from './integrity-model';
import { metrics, NOW, row, symbolSnapshot } from '../test/fixtures';

describe('incident-ledger', () => {
  it('records status transitions once per symbol transition', () => {
    const ledger = createIncidentLedger(10);
    const live = deriveIntegrity(metrics(), symbolSnapshot([row({ symbol: 'DCE.m2609', status: 'live' })]), NOW);
    const stale = deriveIntegrity(
      metrics(),
      symbolSnapshot([row({ symbol: 'DCE.m2609', status: 'stale', problem: true, problem_severity: 'warn' })]),
      NOW + 2_000,
    );

    updateIncidentLedger(ledger, live);
    updateIncidentLedger(ledger, stale);
    updateIncidentLedger(ledger, stale);

    expect(ledger.incidents).toHaveLength(1);
    expect(ledger.incidents[0]).toMatchObject({
      scope: 'DCE.m2609',
      type: '静默',
      impact: '未订阅',
      severity: 'warn',
    });
  });
});
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cd crates/tqsdk-relay/dashboard-ui
pnpm run test
```

Expected: FAIL because runtime history modules are missing.

- [ ] **Step 3: Extend types**

Add to `crates/tqsdk-relay/dashboard-ui/src/lib/types.ts`:

```ts
export type HistorySample = {
  sampledAt: number;
  frameRate: number | null;
  eventRate: number | null;
  coverageRatio: number;
  issueCount: number;
  upstreamIdleMs: number | null;
  continuityScore: number;
};

export type RuntimeHistory = {
  limit: number;
  samples: HistorySample[];
};

export type TimelineSeverity = 'live' | 'closed' | 'warn' | 'bad';

export type TimelineSample = {
  sampledAt: number;
  exchangeSeverity: Record<string, TimelineSeverity>;
  subscribedSeverity: TimelineSeverity;
  globalSeverity: TimelineSeverity;
};

export type TimelineHistory = {
  samples: TimelineSample[];
};

export type LocalIncident = {
  id: string;
  at: number;
  scope: string;
  type: string;
  detail: string;
  impact: string;
  severity: TimelineSeverity;
};

export type IncidentLedger = {
  limit: number;
  knownStatuses: Map<string, SymbolStatus>;
  incidents: LocalIncident[];
};
```

- [ ] **Step 4: Add history implementation**

Create `crates/tqsdk-relay/dashboard-ui/src/lib/history.ts`:

```ts
import type { IntegrityModel, RuntimeHistory } from './types';

export function createHistory(limit = 150): RuntimeHistory {
  return { limit, samples: [] };
}

export function pushHistorySample(history: RuntimeHistory, model: IntegrityModel): RuntimeHistory {
  history.samples.push({
    sampledAt: model.sampledAt,
    frameRate: model.frameRate,
    eventRate: model.eventRate,
    coverageRatio: model.coverageRatio,
    issueCount: model.issueCount,
    upstreamIdleMs: model.upstreamIdleMs,
    continuityScore: model.continuityScore,
  });
  if (history.samples.length > history.limit) {
    history.samples.splice(0, history.samples.length - history.limit);
  }
  return history;
}

export function sparkPoints(values: Array<number | null>, width = 160, height = 20): string {
  const valid = values.filter((value): value is number => value != null && Number.isFinite(value));
  if (valid.length === 0) return '';
  const min = Math.min(...valid);
  const max = Math.max(...valid);
  const range = Math.max(0.0001, max - min);
  return values
    .map((value, index) => {
      const safe = value == null || !Number.isFinite(value) ? min : value;
      const x = values.length === 1 ? 0 : (index / (values.length - 1)) * width;
      const y = height - ((safe - min) / range) * height;
      return `${Math.round(x)},${Math.round(y)}`;
    })
    .join(' ');
}
```

- [ ] **Step 5: Add timeline implementation**

Create `crates/tqsdk-relay/dashboard-ui/src/lib/timeline.ts`:

```ts
import type { IntegrityModel, SymbolRow, TimelineHistory, TimelineSample, TimelineSeverity } from './types';

const EXCHANGES = ['SHFE', 'DCE', 'CZCE', 'INE', 'GFEX', 'CFFEX'];

export function createTimelineHistory(): TimelineHistory {
  return { samples: [] };
}

export function exchangeOf(symbol: string): string {
  return symbol.split('.')[0]?.toUpperCase() || 'UNKNOWN';
}

export function timelineSeverityForRows(rows: SymbolRow[]): TimelineSeverity {
  if (rows.some((row) => row.problem_severity === 'bad')) return 'bad';
  if (rows.some((row) => row.problem_severity === 'warn')) return 'warn';
  if (rows.length > 0 && rows.every((row) => row.status === 'closed')) return 'closed';
  return 'live';
}

export function pushTimelineSample(history: TimelineHistory, model: IntegrityModel): TimelineHistory {
  const exchangeSeverity: Record<string, TimelineSeverity> = {};
  for (const exchange of EXCHANGES) {
    exchangeSeverity[exchange] = timelineSeverityForRows(model.rows.filter((row) => exchangeOf(row.symbol) === exchange));
  }
  const subscribedRows = model.rows.filter((row) => row.subscribed);
  const sample: TimelineSample = {
    sampledAt: model.sampledAt,
    exchangeSeverity,
    subscribedSeverity: timelineSeverityForRows(subscribedRows),
    globalSeverity: model.overall === 'critical' ? 'bad' : model.overall === 'warning' ? 'warn' : model.overall === 'warming' ? 'closed' : 'live',
  };
  history.samples.push(sample);
  history.samples = history.samples.filter((item) => item.sampledAt >= model.sampledAt - 300_000);
  return history;
}

export function timelineBuckets(history: TimelineHistory, now: number, bucketCount = 60): Array<TimelineSample | null> {
  const bucketMs = 300_000 / bucketCount;
  return Array.from({ length: bucketCount }, (_, index) => {
    const start = now - 300_000 + index * bucketMs;
    const end = start + bucketMs;
    return history.samples.findLast((sample) => sample.sampledAt >= start && sample.sampledAt < end) ?? null;
  });
}
```

- [ ] **Step 6: Add incident ledger implementation**

Create `crates/tqsdk-relay/dashboard-ui/src/lib/incident-ledger.ts`:

```ts
import { statusLabel } from './integrity-model';
import type { IncidentLedger, IntegrityModel, LocalIncident, SymbolRow, TimelineSeverity } from './types';

export function createIncidentLedger(limit = 80): IncidentLedger {
  return { limit, knownStatuses: new Map(), incidents: [] };
}

function severityForIncident(row: SymbolRow): TimelineSeverity {
  if (row.problem_severity === 'bad') return 'bad';
  if (row.problem_severity === 'warn') return 'warn';
  if (row.problem_severity === 'closed') return 'closed';
  return 'live';
}

export function updateIncidentLedger(ledger: IncidentLedger, model: IntegrityModel): IncidentLedger {
  for (const row of model.rows) {
    const before = ledger.knownStatuses.get(row.symbol);
    if (before && before !== row.status) {
      const incident: LocalIncident = {
        id: `${model.sampledAt}:${row.symbol}:${before}:${row.status}`,
        at: model.sampledAt,
        scope: row.symbol,
        type: statusLabel(row.status),
        detail: `${statusLabel(before)} -> ${statusLabel(row.status)}`,
        impact: row.subscribed ? '影响订阅' : '未订阅',
        severity: severityForIncident(row),
      };
      if (!ledger.incidents.some((item) => item.id === incident.id)) {
        ledger.incidents.unshift(incident);
      }
    }
    ledger.knownStatuses.set(row.symbol, row.status);
  }
  if (ledger.incidents.length > ledger.limit) {
    ledger.incidents.splice(ledger.limit);
  }
  return ledger;
}
```

- [ ] **Step 7: Run tests**

Run:

```bash
cd crates/tqsdk-relay/dashboard-ui
pnpm run test
```

Expected: PASS.

- [ ] **Step 8: Commit runtime model layer**

```bash
git add crates/tqsdk-relay/dashboard-ui/src/lib crates/tqsdk-relay/dashboard-ui/src/test
git commit -m "test(relay): model dashboard runtime history"
```

***

### Task 4: Build Svelte Component Layout And Controls

**Files:**

- Modify: `crates/tqsdk-relay/dashboard-ui/src/App.svelte`
- Create: `crates/tqsdk-relay/dashboard-ui/src/components/MonitorHeader.svelte`
- Create: `crates/tqsdk-relay/dashboard-ui/src/components/IntegrityHero.svelte`
- Create: `crates/tqsdk-relay/dashboard-ui/src/components/MetricCard.svelte`
- Create: `crates/tqsdk-relay/dashboard-ui/src/components/RelayPipeline.svelte`
- Create: `crates/tqsdk-relay/dashboard-ui/src/components/ContinuityTimeline.svelte`
- Create: `crates/tqsdk-relay/dashboard-ui/src/components/AttentionList.svelte`
- Create: `crates/tqsdk-relay/dashboard-ui/src/components/IncidentTable.svelte`
- Create: `crates/tqsdk-relay/dashboard-ui/src/components/SymbolHealthTable.svelte`
- Create: `crates/tqsdk-relay/dashboard-ui/src/components/IntegrityTrend.svelte`
- Create: `crates/tqsdk-relay/dashboard-ui/src/components/ScoreGauge.svelte`
- Create: `crates/tqsdk-relay/dashboard-ui/src/components/DashboardControls.svelte`
- Modify: `crates/tqsdk-relay/dashboard-ui/src/styles/theme.css`
- Modify: `crates/tqsdk-relay/dashboard-ui/src/styles/app.css`
- [ ] **Step 1: Add Svelte component smoke test**

Create `crates/tqsdk-relay/dashboard-ui/src/App.test.ts`:

```ts
import { render, screen } from '@testing-library/svelte';
import { describe, expect, it, vi } from 'vitest';
import App from './App.svelte';
import { metrics, row, symbolSnapshot } from './test/fixtures';

describe('App', () => {
  it('renders snapshot-driven relay dashboard without direct DOM mutation helpers', async () => {
    vi.stubGlobal('fetch', vi.fn(async (path: string) => {
      if (path === '/metrics') {
        return Response.json(metrics());
      }
      if (path.startsWith('/symbol-metrics')) {
        return Response.json(symbolSnapshot([
          row({ symbol: 'SHFE.au2602', instrument_name: '沪金2602' }),
          row({ symbol: 'DCE.m2609', instrument_name: '豆粕2609', status: 'stale', problem: true, problem_severity: 'warn' }),
        ]));
      }
      return Response.json({ error: 'not found' }, { status: 404 });
    }));

    render(App);

    expect(await screen.findByText('tqsdk-relay 行情完整性监控中心')).toBeTruthy();
    expect(await screen.findByText('沪金2602')).toBeTruthy();
    expect(await screen.findByText('豆粕2609')).toBeTruthy();
    expect(await screen.findByText('完整性趋势')).toBeTruthy();
  });
});
```

Run:

```bash
cd crates/tqsdk-relay/dashboard-ui
pnpm run test
```

Expected: FAIL because components and real polling are not wired.

- [ ] **Step 2: Replace** **`App.svelte`** **with state layers**

Update `crates/tqsdk-relay/dashboard-ui/src/App.svelte`:

```svelte
<script lang="ts">
  import AttentionList from './components/AttentionList.svelte';
  import ContinuityTimeline from './components/ContinuityTimeline.svelte';
  import DashboardControls from './components/DashboardControls.svelte';
  import IncidentTable from './components/IncidentTable.svelte';
  import IntegrityHero from './components/IntegrityHero.svelte';
  import IntegrityTrend from './components/IntegrityTrend.svelte';
  import MetricCard from './components/MetricCard.svelte';
  import MonitorHeader from './components/MonitorHeader.svelte';
  import RelayPipeline from './components/RelayPipeline.svelte';
  import SymbolHealthTable from './components/SymbolHealthTable.svelte';
  import { fetchRelaySnapshot } from './lib/api';
  import { createHistory, pushHistorySample } from './lib/history';
  import { createIncidentLedger, updateIncidentLedger } from './lib/incident-ledger';
  import { deriveIntegrity } from './lib/integrity-model';
  import { createTimelineHistory, pushTimelineSample, timelineBuckets } from './lib/timeline';
  import type { DashboardViewState, IntegrityModel, RelaySnapshot } from './lib/types';
  import './styles/theme.css';

  const POLL_INTERVAL_MS = 2_000;

  let snapshot = $state<RelaySnapshot | null>(null);
  let model = $state<IntegrityModel | null>(null);
  let history = $state(createHistory());
  let timeline = $state(createTimelineHistory());
  let incidents = $state(createIncidentLedger());
  let error = $state<string | null>(null);
  let view = $state<DashboardViewState>({
    paused: false,
    fullscreen: false,
    selectedExchange: null,
    selectedSymbol: null,
    filters: {
      statuses: [],
      subscribedOnly: false,
      q: '',
      sort: 'receive_gap_ms_desc',
      limit: 200,
    },
  });

  let buckets = $derived(timelineBuckets(timeline, snapshot?.receivedAt ?? Date.now(), 60));

  async function load(signal?: AbortSignal) {
    const next = await fetchRelaySnapshot(view.filters, signal);
    snapshot = next;
    const nextModel = deriveIntegrity(next.metrics, next.symbols, next.receivedAt, model);
    pushHistorySample(history, nextModel);
    pushTimelineSample(timeline, nextModel);
    updateIncidentLedger(incidents, nextModel);
    model = nextModel;
    error = null;
  }

  $effect(() => {
    if (view.paused) return;
    const controller = new AbortController();
    void load(controller.signal).catch((reason) => {
      if (!controller.signal.aborted) error = reason instanceof Error ? reason.message : String(reason);
    });
    const timer = window.setInterval(() => {
      void load(controller.signal).catch((reason) => {
        if (!controller.signal.aborted) error = reason instanceof Error ? reason.message : String(reason);
      });
    }, POLL_INTERVAL_MS);
    return () => {
      controller.abort();
      window.clearInterval(timer);
    };
  });
</script>

<main class="dashboard-shell" data-fullscreen={view.fullscreen}>
  <MonitorHeader {model} {error} bind:paused={view.paused} bind:fullscreen={view.fullscreen} />
  <DashboardControls bind:filters={view.filters} disabled={view.paused} onrefresh={() => load()} />

  {#if model}
    <IntegrityHero {model} />
    <section class="kpi-grid">
      <MetricCard label="上游帧流" value={model.frameRate} unit="/s" tone="info" />
      <MetricCard label="有效事件" value={model.eventRate} unit="/s" tone="accent" />
      <MetricCard label="合约覆盖" value={model.coverageRatio * 100} unit="%" tone="live" />
      <MetricCard label="完整性异常" value={model.issueCount} tone={model.issueCount > 0 ? 'warn' : 'live'} />
      <MetricCard label="上游静默" value={model.upstreamIdleMs} format="duration" tone="info" />
      <MetricCard label="解码坏行" value={model.invalidRowCount} tone={model.invalidRowCount > 0 ? 'bad' : 'live'} />
    </section>
    <RelayPipeline {model} />
    <section class="dashboard-main">
      <AttentionList rows={model.problems} />
      <ContinuityTimeline {buckets} rows={model.rows} />
      <IncidentTable incidents={incidents.incidents} />
    </section>
    <section class="dashboard-bottom">
      <SymbolHealthTable rows={model.rows} bind:selectedSymbol={view.selectedSymbol} />
      <IntegrityTrend {history} {model} />
    </section>
  {:else}
    <section class="panel min-h-[280px] place-content-center text-center text-[var(--relay-muted)]">
      正在读取 relay 观测数据
    </section>
  {/if}
</main>
```

- [ ] **Step 3: Add component prop contracts**

Each component must use these props and DOM signals:

```text
MonitorHeader.svelte
  props: model: IntegrityModel | null, error: string | null, paused: boolean, fullscreen: boolean
  emits/binds: paused, fullscreen
  visible text: tqsdk-relay 行情完整性监控中心, Asia/Shanghai, 实时监控中/已暂停

DashboardControls.svelte
  props: filters: DashboardFilters, disabled: boolean, onrefresh: () => void | Promise<void>
  controls: status multiselect checkboxes, subscribed toggle, q input, sort select, limit select, refresh button
  query options must match SymbolMetricsQuery::from_query_string

IntegrityHero.svelte
  props: model: IntegrityModel
  states: healthy -> 行情完整; warning -> 需要关注; critical -> 订阅受影响; warming -> 启动观测中

MetricCard.svelte
  props: label: string, value: number | null, unit?: string, tone: "live" | "warn" | "bad" | "info" | "accent", format?: "number" | "rate" | "duration"

RelayPipeline.svelte
  props: model: IntegrityModel
  nodes: 上游连接, 合约集合, 数据解码, 行情缓存, 下游服务

ContinuityTimeline.svelte
  props: buckets: Array<TimelineSample | null>, rows: SymbolRow[]
  rows: 全局, 订阅, SHFE, DCE, CZCE, INE/GFEX/CFFEX if present

AttentionList.svelte
  props: rows: SymbolRow[]
  sort: subscribed first, bad before warn, higher receive_gap_ms first

IncidentTable.svelte
  props: incidents: LocalIncident[]

SymbolHealthTable.svelte
  props: rows: SymbolRow[], selectedSymbol: string | null
  columns: 状态, 合约, 中文名称, 距上次更新, 行情时间延迟, Tick 累计, 订阅, 风险

IntegrityTrend.svelte
  props: history: RuntimeHistory, model: IntegrityModel
  contains ScoreGauge.svelte
```

- [ ] **Step 4: Add stable dashboard CSS**

Update `crates/tqsdk-relay/dashboard-ui/src/styles/app.css`:

```css
@import "tailwindcss";

:root {
  color-scheme: dark;
  font-family: Inter, "PingFang SC", "Microsoft YaHei", system-ui, sans-serif;
  font-variant-numeric: tabular-nums;
}

html,
body,
#app {
  margin: 0;
  min-width: 1120px;
  min-height: 720px;
}

body {
  background:
    radial-gradient(circle at 50% -12%, rgb(24 89 156 / 32%), transparent 35%),
    linear-gradient(180deg, #051019, var(--relay-bg) 72%);
}

.dashboard-shell {
  min-height: 100vh;
  display: grid;
  grid-template-rows: auto auto auto auto auto 1fr auto;
  gap: 8px;
  padding: 10px 12px;
  color: var(--relay-text);
}

.panel {
  position: relative;
  overflow: hidden;
  border: 1px solid var(--relay-line);
  border-radius: 8px;
  background: linear-gradient(180deg, var(--relay-panel), #07121a);
  box-shadow: 0 16px 34px rgb(0 0 0 / 34%);
}

.kpi-grid {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  gap: 8px;
}

.dashboard-main {
  display: grid;
  grid-template-columns: 260px minmax(520px, 1fr) 420px;
  gap: 8px;
  min-height: 280px;
}

.dashboard-bottom {
  display: grid;
  grid-template-columns: 1.1fr 0.9fr;
  gap: 8px;
  min-height: 220px;
}

@media (max-width: 1320px) {
  .kpi-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .dashboard-main,
  .dashboard-bottom {
    grid-template-columns: 1fr;
  }
}
```

- [ ] **Step 5: Implement components against prop contracts**

Use Tailwind classes for grid/flex/spacing/text/borders and `theme.css` variables for colors. Keep these class hooks stable for tests and visual QA:

```text
data-testid="monitor-header"
data-testid="dashboard-controls"
data-testid="integrity-hero"
data-testid="relay-pipeline"
data-testid="continuity-timeline"
data-testid="attention-list"
data-testid="incident-table"
data-testid="symbol-health-table"
data-testid="integrity-trend"
data-testid="score-gauge"
```

No component may call `document.getElementById`, set `innerHTML`, or mutate global DOM.

- [ ] **Step 6: Run component tests**

Run:

```bash
cd crates/tqsdk-relay/dashboard-ui
pnpm run test
pnpm run check
pnpm run build
```

Expected: PASS; `src/dashboard-dist/**` refreshed.

- [ ] **Step 7: Commit component layer**

```bash
git add crates/tqsdk-relay/dashboard-ui crates/tqsdk-relay/src/dashboard-dist
git commit -m "feat(relay): rebuild dashboard with svelte"
```

***

### Task 5: Serve Embedded Dashboard Dist From Rust

**Files:**

- Modify: `Cargo.toml`
- Modify: `crates/tqsdk-relay/Cargo.toml`
- Modify: `crates/tqsdk-relay/src/dashboard.rs`
- Modify: `crates/tqsdk-relay/src/metrics_http.rs`
- Modify: `crates/tqsdk-relay/tests/binary_smoke.rs`
- [ ] **Step 1: Run GitNexus impact before editing Rust symbols**

Run:

```bash
gitnexus impact dashboard_asset --repo tqsdk-rust --direction upstream
gitnexus impact serve_metrics_stream --repo tqsdk-rust --direction upstream
```

Expected: output lists direct callers and affected flows. If either result is HIGH or CRITICAL, stop and report risk before editing.

- [ ] **Step 2: Add failing binary smoke assertions**

Update `relay_binary_serves_embedded_dashboard_assets` in `crates/tqsdk-relay/tests/binary_smoke.rs` so it checks `/dashboard/` and CSS:

```rust
let html = wait_for_http_response(metrics_addr, "/dashboard/", &mut child);
assert!(html.starts_with("HTTP/1.1 200"));
assert!(html.contains("tqsdk-relay 行情完整性监控中心"));
assert!(html.contains("/dashboard/assets/app.js"));
assert!(html.contains("/dashboard/assets/app.css"));

let js = wait_for_http_response(metrics_addr, "/dashboard/assets/app.js", &mut child);
assert!(js.starts_with("HTTP/1.1 200"));
assert!(js.contains("/symbol-metrics"));
assert!(js.contains("/metrics"));

let css = wait_for_http_response(metrics_addr, "/dashboard/assets/app.css", &mut child);
assert!(css.starts_with("HTTP/1.1 200"));
assert!(css.contains("--relay-bg"));

let missing = wait_for_http_response(metrics_addr, "/dashboard/assets/missing.js", &mut child);
assert!(missing.starts_with("HTTP/1.1 404"));
```

Run:

```bash
cargo test -p tqsdk-relay --test binary_smoke relay_binary_serves_embedded_dashboard_assets
```

Expected: FAIL because Rust still serves only `/dashboard` and `/dashboard/app.js`.

- [ ] **Step 3: Add embed dependency**

Update root `Cargo.toml`:

```toml
[workspace.dependencies]
include_dir = "0.7"
```

Keep existing dependencies sorted by local convention. Update `crates/tqsdk-relay/Cargo.toml`:

```toml
[dependencies]
include_dir.workspace = true
```

- [ ] **Step 4: Replace dashboard asset constants**

Replace `crates/tqsdk-relay/src/dashboard.rs` with:

```rust
#![cfg_attr(not(test), forbid(unsafe_code))]

use include_dir::{Dir, include_dir};

static DASHBOARD_DIST: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/src/dashboard-dist");

#[derive(Debug, Clone, Copy)]
pub struct DashboardAsset<'a> {
    pub content_type: &'static str,
    pub bytes: &'a [u8],
}

pub fn dashboard_asset(path: &str) -> Option<DashboardAsset<'static>> {
    let path = normalize_dashboard_path(path)?;
    let file = DASHBOARD_DIST.get_file(path)?;
    Some(DashboardAsset {
        content_type: content_type(path),
        bytes: file.contents(),
    })
}

fn normalize_dashboard_path(path: &str) -> Option<&str> {
    let clean = path.strip_prefix('/').unwrap_or(path);
    let clean = clean.strip_prefix("dashboard").unwrap_or(clean);
    let clean = clean.strip_prefix('/').unwrap_or(clean);
    if clean.is_empty() {
        return Some("index.html");
    }
    if clean.contains("..") || clean.starts_with('/') {
        return None;
    }
    Some(clean)
}

fn content_type(path: &str) -> &'static str {
    match path.rsplit_once('.').map(|(_, extension)| extension) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "application/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("webp") => "image/webp",
        _ => "application/octet-stream",
    }
}
```

- [ ] **Step 5: Route** **`/dashboard/*`** **through generic asset service**

Modify `crates/tqsdk-relay/src/metrics_http.rs`:

```rust
use crate::dashboard::dashboard_asset;
```

Replace the old `/dashboard` and `/dashboard/app.js` match arms with:

```rust
path if path == "/dashboard" || path == "/dashboard/" || path.starts_with("/dashboard/") => {
    let Some(asset) = dashboard_asset(path) else {
        write_response(stream, 404, json!({"error": "not found"})).await?;
        return Ok(());
    };
    write_bytes_response(stream, 200, asset.content_type, asset.bytes).await?;
    return Ok(());
}
```

Add:

```rust
async fn write_bytes_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> RelayResult<()> {
    let header = format!(
        "HTTP/1.1 {status} {}\r\n\
Content-Type: {content_type}\r\n\
Content-Length: {}\r\n\
Cache-Control: no-store\r\n\
X-Content-Type-Options: nosniff\r\n\
Connection: close\r\n\
\r\n",
        status_reason(status),
        body.len(),
    );
    stream
        .write_all(header.as_bytes())
        .await
        .map_err(|err| RelayError::Transport(format!("metrics write failed: {err}")))?;
    stream
        .write_all(body)
        .await
        .map_err(|err| RelayError::Transport(format!("metrics write failed: {err}")))
}
```

After this change, remove `write_text_response` if it is unused.

- [ ] **Step 6: Run Rust dashboard asset tests**

Run:

```bash
cargo test -p tqsdk-relay --test binary_smoke relay_binary_serves_embedded_dashboard_assets
cargo test -p tqsdk-relay --test symbol_metrics
```

Expected: PASS.

- [ ] **Step 7: Commit Rust static asset service**

```bash
git add Cargo.toml crates/tqsdk-relay/Cargo.toml crates/tqsdk-relay/src/dashboard.rs crates/tqsdk-relay/src/metrics_http.rs crates/tqsdk-relay/tests/binary_smoke.rs
git commit -m "feat(relay): serve embedded dashboard dist"
```

***

### Task 6: Remove Legacy Dashboard Assets And Node VM Test

**Files:**

- Delete: `crates/tqsdk-relay/src/dashboard/index.html`
- Delete: `crates/tqsdk-relay/src/dashboard/app.js`
- Delete: `crates/tqsdk-relay/tests/dashboard_logic.mjs`
- Modify: `crates/tqsdk-relay/tests/binary_smoke.rs`
- [ ] **Step 1: Delete legacy files**

Run:

```bash
git rm crates/tqsdk-relay/src/dashboard/index.html crates/tqsdk-relay/src/dashboard/app.js crates/tqsdk-relay/tests/dashboard_logic.mjs
```

Expected: files staged for deletion.

- [ ] **Step 2: Search for stale references**

Run:

```bash
rg -n "dashboard/app.js|dashboard/index.html|dashboard_logic|DASHBOARD_HTML|DASHBOARD_JS" crates/tqsdk-relay docs
```

Expected: no matches.

- [ ] **Step 3: Run relevant tests**

Run:

```bash
cd crates/tqsdk-relay/dashboard-ui
pnpm run test
pnpm run check
pnpm run build
cd ../../..
cargo test -p tqsdk-relay --test binary_smoke relay_binary_serves_embedded_dashboard_assets
```

Expected: PASS.

- [ ] **Step 4: Commit legacy cleanup**

```bash
git add crates/tqsdk-relay/dashboard-ui crates/tqsdk-relay/src/dashboard-dist crates/tqsdk-relay/src/dashboard crates/tqsdk-relay/tests
git commit -m "refactor(relay): remove legacy dashboard assets"
```

***

### Task 7: Add Playwright Visual Smoke

**Files:**

- Create: `crates/tqsdk-relay/dashboard-ui/playwright.config.ts`
- Create: `crates/tqsdk-relay/dashboard-ui/tests/dashboard.spec.ts`
- Modify: `crates/tqsdk-relay/dashboard-ui/package.json`
- [ ] **Step 1: Add Playwright config**

Create `crates/tqsdk-relay/dashboard-ui/playwright.config.ts`:

```ts
import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './tests',
  timeout: 30_000,
  expect: { timeout: 5_000 },
  use: {
    baseURL: 'http://127.0.0.1:4173',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
  },
  webServer: {
    command: 'pnpm run preview -- --port 4173',
    url: 'http://127.0.0.1:4173/dashboard/',
    reuseExistingServer: !process.env.CI,
  },
  projects: [
    { name: 'chromium-desktop', use: { ...devices['Desktop Chrome'], viewport: { width: 1440, height: 900 } } },
    { name: 'chromium-compact', use: { ...devices['Desktop Chrome'], viewport: { width: 1180, height: 780 } } },
  ],
});
```

- [ ] **Step 2: Add route-intercepted visual smoke**

Create `crates/tqsdk-relay/dashboard-ui/tests/dashboard.spec.ts`:

```ts
import { expect, test } from '@playwright/test';
import { metrics, row, symbolSnapshot } from '../src/test/fixtures';

test('dashboard renders relay integrity view from intercepted snapshots', async ({ page }) => {
  await page.route('**/metrics', async (route) => {
    await route.fulfill({ json: metrics({ upstream_frames_received: 20, upstream_events_decoded: 40 }) });
  });
  await page.route('**/symbol-metrics?*', async (route) => {
    await route.fulfill({
      json: symbolSnapshot([
        row({ symbol: 'SHFE.au2602', instrument_name: '沪金2602', subscribed: true, quote_subscriber_count: 1 }),
        row({ symbol: 'DCE.m2609', instrument_name: '豆粕2609', status: 'stale', problem: true, problem_severity: 'warn', receive_gap_ms: 90_000 }),
        row({ symbol: 'CZCE.AP610', instrument_name: '苹果610', status: 'closed', problem: false, problem_severity: 'closed' }),
      ]),
    });
  });

  await page.goto('/dashboard/');

  await expect(page.getByText('tqsdk-relay 行情完整性监控中心')).toBeVisible();
  await expect(page.getByText('沪金2602')).toBeVisible();
  await expect(page.getByText('豆粕2609')).toBeVisible();
  await expect(page.getByTestId('continuity-timeline')).toBeVisible();
  await expect(page.getByTestId('score-gauge')).toBeVisible();
});
```

- [ ] **Step 3: Run Playwright smoke**

Run:

```bash
cd crates/tqsdk-relay/dashboard-ui
pnpm run build
pnpm run test:e2e
```

Expected: PASS. If Chromium browser is not installed, run:

```bash
cd crates/tqsdk-relay/dashboard-ui
pnpm exec playwright install chromium
pnpm run test:e2e
```

- [ ] **Step 4: Commit visual smoke**

```bash
git add crates/tqsdk-relay/dashboard-ui
git commit -m "test(relay): add dashboard visual smoke"
```

***

### Task 8: Update Documentation And Validation Matrix

**Files:**

- Modify: `crates/tqsdk-relay/README.md`
- Modify: `docs/architecture/validation.md`
- [ ] **Step 1: Update relay README dashboard section**

In `crates/tqsdk-relay/README.md`, replace the dashboard paragraph with:

````markdown
`/dashboard` 是内置只读运维页面，每 `2s` 轮询 `/symbol-metrics` 和 `/metrics`。它不连接 relay
market websocket，不创建下游订阅，也不会触发额外行情命令。页面由
`crates/tqsdk-relay/dashboard-ui/` 的 Svelte 5 + Vite + Tailwind CSS 4 工程构建，
生产产物提交在 `crates/tqsdk-relay/src/dashboard-dist/`，Rust 侧将该目录嵌入到
relay 二进制并服务 `/dashboard/` 与 `/dashboard/assets/*`。

开发 dashboard UI：

```bash
cd crates/tqsdk-relay/dashboard-ui
pnpm install
pnpm run dev
```

开发服务器会把 `/metrics` 和 `/symbol-metrics` 代理到 `127.0.0.1:7789`。发布或提交前：

```bash
cd crates/tqsdk-relay/dashboard-ui
pnpm run test
pnpm run check
pnpm run build
pnpm run test:e2e
cd ../../..
cargo test -p tqsdk-relay --test binary_smoke relay_binary_serves_embedded_dashboard_assets
```
````

- [ ] **Step 2: Update architecture validation**

In `docs/architecture/validation.md`, update the relay dashboard extra checks to:

````markdown
修改 relay dashboard、dashboard UI 或 symbol telemetry 时，补充运行：

```bash
cargo test -p tqsdk-relay --test symbol_metrics
cargo test -p tqsdk-relay --test binary_smoke relay_binary_serves_embedded_dashboard_assets
cd crates/tqsdk-relay/dashboard-ui
pnpm run test
pnpm run check
pnpm run build
pnpm run test:e2e
```
````

- [ ] **Step 3: Run doc checks**

Run:

```bash
git diff --check
```

Expected: PASS.

- [ ] **Step 4: Commit docs**

```bash
git add crates/tqsdk-relay/README.md docs/architecture/validation.md
git commit -m "docs(relay): document dashboard ui workflow"
```

***

### Task 9: Full Verification And Scope Review

**Files:**

- No code changes unless verification finds a defect.
- [ ] **Step 1: Run dashboard UI checks**

```bash
cd crates/tqsdk-relay/dashboard-ui
pnpm run test
pnpm run check
pnpm run build
pnpm run test:e2e
```

Expected: PASS.

- [ ] **Step 2: Run relay checks**

```bash
cargo test -p tqsdk-relay --tests
cargo clippy -p tqsdk-relay --all-targets -- -D warnings
cargo check -p tqsdk-relay --no-default-features
```

Expected: PASS.

- [ ] **Step 3: Run workspace formatting checks**

```bash
cargo fmt --all --check
git diff --check
```

Expected: PASS.

- [ ] **Step 4: Run GitNexus change detection before final commit/merge**

```bash
gitnexus detect-changes --repo tqsdk-rust
```

Expected: output shows only relay dashboard UI/static asset serving and docs impact. If unrelated flows appear, inspect before proceeding.

- [ ] **Step 5: Inspect final changed file set**

```bash
git status --short
git diff --stat
```

Expected changed areas:

```text
Cargo.toml
crates/tqsdk-relay/Cargo.toml
crates/tqsdk-relay/dashboard-ui/**
crates/tqsdk-relay/src/dashboard.rs
crates/tqsdk-relay/src/dashboard-dist/**
crates/tqsdk-relay/src/metrics_http.rs
crates/tqsdk-relay/tests/binary_smoke.rs
crates/tqsdk-relay/src/dashboard/** deleted
crates/tqsdk-relay/tests/dashboard_logic.mjs deleted
crates/tqsdk-relay/README.md
docs/architecture/validation.md
```

- [ ] **Step 6: Final commit if previous tasks were squashed or edited**

If prior tasks were not committed individually, commit all relevant files:

```bash
git add Cargo.toml crates/tqsdk-relay/Cargo.toml crates/tqsdk-relay/dashboard-ui crates/tqsdk-relay/src/dashboard.rs crates/tqsdk-relay/src/dashboard-dist crates/tqsdk-relay/src/metrics_http.rs crates/tqsdk-relay/tests/binary_smoke.rs crates/tqsdk-relay/README.md docs/architecture/validation.md
git add -u crates/tqsdk-relay/src/dashboard crates/tqsdk-relay/tests/dashboard_logic.mjs
git commit -m "refactor(relay): migrate dashboard to svelte"
```

***

## Risk Notes

- **Dependency footprint:** This introduces Node dev tooling and `include_dir`. Node stays under `crates/tqsdk-relay/dashboard-ui`; Rust runtime does not require Node.
- **Generated files:** `src/dashboard-dist/**` must be committed because relay crate compiles from embedded assets. The source of truth remains `dashboard-ui/src/**`.
- **Asset path drift:** Vite `base` must stay `/dashboard/`; Rust tests must assert `/dashboard/assets/app.js` and `/dashboard/assets/app.css`.
- **Runtime effect loops:** Svelte `$effect` should only depend on `view.paused` and stable filter state. If filters change, allow effect teardown/recreate; avoid writing to filter state inside polling effect.
- **False alarms:** Preserve backend `problem` and `problem_severity` semantics. Closed rows and quote-only rows must not become UI-only problems.
- **CI/network:** `pnpm install` and first Playwright browser install may need network. Rust tests must remain runnable without Node after built assets are committed.

## Self-Review

- Spec coverage:
  - Svelte 5 + Vite + Tailwind 4: covered by Tasks 1, 4, 7.
  - No SvelteKit / no independent frontend runtime: covered by Architecture Rules and Vite SPA layout.
  - Rust single binary: covered by Task 5 embedded dist and committed artifacts.
  - State layering: covered by Tasks 2 and 3.
  - Pause, replay/history, controls, details: covered by Tasks 3 and 4.
  - Tests: covered by Vitest, Playwright, Rust binary smoke, relay tests.
- Placeholder scan:
  - No placeholder markers or unowned edge-case language.
- Type consistency:
  - `SymbolStatus`, `ProblemSeverity`, `RelayMetrics`, `SymbolMetricsSnapshot`, `DashboardFilters`, `IntegrityModel`, `RuntimeHistory`, `TimelineHistory`, and `IncidentLedger` are defined before they are used by tasks.
