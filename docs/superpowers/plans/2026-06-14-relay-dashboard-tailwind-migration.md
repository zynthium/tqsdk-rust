# Relay Dashboard Tailwind Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 `crates/tqsdk-relay/dashboard-ui` 从“Tailwind 已接入但未成为主样式体系”的状态，迁移为“Tailwind utility + 少量保留 CSS 特效”的可维护样式架构。

**Architecture:** 保持当前 `Svelte 5 + Vite 8 + Tailwind v4` 技术栈，不引入 SvelteKit、额外 UI 库或 class 合并库。样式迁移采用“全局 token/utility 先行，简单组件先落地，复杂可视化组件分层迁移”的策略；Tailwind 负责布局、间距、排版和状态色，复杂渐变、伪元素、动画和仪表盘继续保留在 CSS。

**Tech Stack:** Svelte 5, Vite 8, Tailwind CSS v4, TypeScript 6, Vitest, Playwright, pnpm.

---

## Scope And Guardrails

- 本计划只覆盖 `crates/tqsdk-relay/dashboard-ui` 前端样式体系，不修改 Rust relay contract。
- 不引入新的 UI 组件库，不引入 `clsx` / `tailwind-merge`，优先用显式状态映射保证 class 可静态扫描。
- 复杂组件不要求“一次性纯 Tailwind 化”；`IntegrityHero`、`ContinuityTimeline`、`ScoreGauge` 允许保留局部 CSS。
- 修改任意 TS/Svelte 函数前，先跑 GitNexus upstream impact analysis；若返回 `HIGH` 或 `CRITICAL`，先停下汇报。
- 每个任务独立提交；只 stage 当前任务相关文件。
- 每个任务完成后至少执行一次 `pnpm run test`、`pnpm run check`、`pnpm run build` 中与改动范围对应的最小验证。

## File Structure

- Modify `crates/tqsdk-relay/dashboard-ui/src/styles/app.css`
  - 把全局 token、utility、component primitives 收敛到 Tailwind v4 `@theme` / `@utility`。
  - 只保留 `body` 背景、滚动条、伪元素、少量共享特效。
- Modify `crates/tqsdk-relay/dashboard-ui/src/styles/theme.css`
  - 保留 `--relay-*` 颜色和阴影 token，必要时补充缺失变量。
- Modify `crates/tqsdk-relay/dashboard-ui/src/App.svelte`
  - 把 shell/grid 布局迁移为 Tailwind utility 组合。
- Modify `crates/tqsdk-relay/dashboard-ui/src/components/MonitorHeader.svelte`
  - 用 Tailwind 取代头部布局、按钮、状态胶囊的大部分局部 CSS。
- Modify `crates/tqsdk-relay/dashboard-ui/src/components/MetricCard.svelte`
  - 改为 utility 驱动的轻量卡片组件，并显式映射 tone 样式。
- Modify `crates/tqsdk-relay/dashboard-ui/src/components/AttentionList.svelte`
  - 收敛列表项、空状态、计数 badge 的重复样式。
- Modify `crates/tqsdk-relay/dashboard-ui/src/components/IncidentTable.svelte`
  - 统一 badge、列表容器、空状态和计数器视觉。
- Modify `crates/tqsdk-relay/dashboard-ui/src/components/RelayPipeline.svelte`
  - 只迁移节点排版与状态视觉；保留连线或复杂特效 CSS。
- Modify `crates/tqsdk-relay/dashboard-ui/src/components/IntegrityTrend.svelte`
  - 迁移外层 panel/body/legend 布局；保留 SVG 图形样式。
- Modify `crates/tqsdk-relay/dashboard-ui/src/components/IntegrityHero.svelte`
  - 只迁移容器布局和 stat chips，保留 orb / orbit / tone gradient CSS。
- Modify `crates/tqsdk-relay/dashboard-ui/src/components/ContinuityTimeline.svelte`
  - 先只迁移工具栏、legend、meta、tooltip 外壳和 row 文本排版；保留 dense grid/timeline 细节 CSS。

---

### Task 1: Tailwind Foundation And Shared Primitives

**Files:**
- Modify: `crates/tqsdk-relay/dashboard-ui/src/styles/app.css`
- Modify: `crates/tqsdk-relay/dashboard-ui/src/styles/theme.css`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({ target: "App", direction: "upstream" })
gitnexus_impact({ target: "MonitorHeader", direction: "upstream" })
```

Expected: dashboard UI symbols only. If risk is `HIGH` or `CRITICAL`, stop and report.

- [ ] **Step 2: Add Tailwind v4 theme bridge**

At the top of `src/styles/app.css`, keep imports and add a `@theme` bridge using the existing relay CSS variables:

```css
@import "tailwindcss";
@import "./theme.css";

@theme {
  --color-relay-bg: var(--relay-bg);
  --color-relay-text: var(--relay-text);
  --color-relay-muted: var(--relay-muted);
  --color-relay-live: var(--relay-live);
  --color-relay-warn: var(--relay-warn);
  --color-relay-bad: var(--relay-bad);
  --color-relay-info: var(--relay-info);
  --color-relay-blue: var(--relay-blue);
  --color-relay-accent: var(--relay-accent);
  --shadow-relay-panel: var(--relay-shadow);
}
```

- [ ] **Step 3: Extract shared primitives into `@utility`**

In `src/styles/app.css`, replace part of the old global classes with a small set of reusable primitives:

```css
@utility panel-shell {
  @apply relative overflow-hidden rounded-[11px] border text-[color:var(--relay-text)];
  border-color: var(--relay-line);
  background: linear-gradient(180deg, var(--relay-panel), var(--relay-panel-soft));
  box-shadow: var(--relay-shadow);
}

@utility panel-head {
  @apply relative z-[1] flex min-h-9 items-center gap-2 text-[13px] font-extrabold tracking-[0.6px] text-[#bfeeff];
}

@utility toolbar-input {
  @apply h-[26px] min-w-0 rounded-md border bg-[#071929] px-[9px] text-[11px] text-[color:var(--relay-text)];
  border-color: var(--relay-line-soft);
}

@utility status-pill {
  @apply inline-flex items-center gap-2 rounded-full border px-3 py-[7px];
}
```

- [ ] **Step 4: Keep only the CSS that Tailwind should not own**

Retain or move only these parts in `src/styles/app.css`:

```css
body {
  background:
    radial-gradient(circle at 50% -12%, #18599c61, transparent 35%),
    radial-gradient(circle at 8% 55%, #0098c215, transparent 30%),
    linear-gradient(180deg, #030b16, var(--relay-bg) 72%);
}

body::before {
  content: "";
  position: fixed;
  inset: 0;
  pointer-events: none;
}
```

Delete or shrink the old `.panel`, `.panel-title`, `.badge`, `.status-dot`, `.dashboard-shell`, `.dashboard-main` blocks once the new utilities replace them.

- [ ] **Step 5: Verify the CSS entry still builds**

Run:

```bash
cd /Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-relay/dashboard-ui
pnpm run build
```

Expected: build passes and generated CSS still contains relay theme values.

- [ ] **Step 6: Commit the foundation**

```bash
git add src/styles/app.css src/styles/theme.css
git commit -m "refactor(dashboard-ui): add tailwind style primitives"
```

---

### Task 2: Shell Layout And Simple Components

**Files:**
- Modify: `crates/tqsdk-relay/dashboard-ui/src/App.svelte`
- Modify: `crates/tqsdk-relay/dashboard-ui/src/components/MonitorHeader.svelte`
- Modify: `crates/tqsdk-relay/dashboard-ui/src/components/MetricCard.svelte`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({ target: "MonitorHeader", direction: "upstream" })
gitnexus_impact({ target: "MetricCard", direction: "upstream" })
```

Expected: only dashboard root layout and KPI strip callers are affected.

- [ ] **Step 2: Convert `App.svelte` shell classes to Tailwind**

Replace the outer structure with utility classes that mirror the current grid:

```svelte
<main
  class="grid h-dvh min-h-0 overflow-hidden gap-2 px-3 pb-[10px] pt-2 text-[color:var(--relay-text)]"
  style="grid-template-rows: 44px auto auto minmax(0,1fr);"
  data-fullscreen={view.fullscreen}
>
```

For the three inner wrappers, move to inline utilities:

```svelte
<section class="grid items-stretch gap-2 [grid-template-columns:auto_minmax(0,1fr)]">
<div class="flex gap-2">
<section class="grid min-h-0 gap-2 [grid-template-columns:minmax(520px,1fr)_340px]">
```

- [ ] **Step 3: Convert `MonitorHeader.svelte` to utility-first markup**

Reshape the header markup so only `error-banner` remains in CSS:

```svelte
<header class="relative grid min-h-11 grid-cols-[1fr_auto_1fr] items-center gap-3" data-testid="monitor-header">
  <div class="flex items-center gap-3 whitespace-nowrap text-xs text-[color:var(--relay-muted)]">
  <h1 class="m-0 whitespace-nowrap text-[clamp(20px,1.7vw,28px)] font-[850] text-[color:var(--relay-text)] [text-shadow:0_0_20px_#50beff7a]">
  <div class="flex items-center justify-end gap-3 whitespace-nowrap text-xs text-[color:var(--relay-muted)]">
```

Use explicit tone mapping instead of nested ternaries inside long class strings:

```ts
const liveChipClass = {
  bad: 'status-pill border-[#ff536a80] bg-[#ff536a14] text-[#ffd2d8]',
  closed: 'status-pill border-[#58758a80] bg-[#58758a14] text-[#9eb9ce]',
  no_sample: 'status-pill border-[#4d789080] bg-[#4d789014] text-[#b8c8d3]',
  live: 'status-pill border-[#45ff9a66] bg-[#45ff9a0f] text-[#ccffe1]',
} as const;
```

- [ ] **Step 4: Convert `MetricCard.svelte` to explicit tone utilities**

Replace `.metric` / `.icon` CSS with markup-driven classes and a tone map:

```ts
const iconToneClass = {
  info: 'text-[color:var(--relay-info)]',
  live: 'text-[color:var(--relay-live)]',
  warn: 'text-[color:var(--relay-warn)]',
  bad: 'text-[color:var(--relay-accent)]',
  accent: 'text-[color:var(--relay-accent)]',
} as const;
```

```svelte
<article class="panel-shell grid min-w-[120px] grid-cols-[34px_1fr] items-center gap-2 px-[11px] py-2">
  <div class={`grid size-8 place-items-center rounded-full border border-current bg-[#20d8ff0d] text-[15px] shadow-[0_0_14px_currentColor] ${iconToneClass[tone]}`}>
```

- [ ] **Step 5: Run focused validation**

Run:

```bash
cd /Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-relay/dashboard-ui
pnpm run test -- --run App.test.ts
pnpm run check
pnpm run build
```

Expected: tests pass, `svelte-check` passes, build output is unchanged functionally.

- [ ] **Step 6: Commit shell and simple components**

```bash
git add src/App.svelte src/components/MonitorHeader.svelte src/components/MetricCard.svelte
git commit -m "refactor(dashboard-ui): migrate shell and header to tailwind"
```

---

### Task 3: List Panels And Badge Consolidation

**Files:**
- Modify: `crates/tqsdk-relay/dashboard-ui/src/components/AttentionList.svelte`
- Modify: `crates/tqsdk-relay/dashboard-ui/src/components/IncidentTable.svelte`
- Modify: `crates/tqsdk-relay/dashboard-ui/src/styles/app.css`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({ target: "AttentionList", direction: "upstream" })
gitnexus_impact({ target: "IncidentTable", direction: "upstream" })
```

- [ ] **Step 2: Replace duplicated count badge styles**

Add one shared count-chip utility in `src/styles/app.css`:

```css
@utility count-chip {
  @apply inline-flex h-[18px] min-w-[22px] items-center justify-center rounded-[9px] px-1.5 text-[11px] font-extrabold;
}
```

Use it in both components:

```svelte
<span class="count-chip bg-[#ff536a22] text-[color:var(--relay-bad)]">{rows.length}</span>
<span class="count-chip bg-[#42a7ff22] text-[color:var(--relay-blue)]">{incidents.length}</span>
```

- [ ] **Step 3: Convert list containers and items to utility markup**

In `AttentionList.svelte`:

```svelte
<aside class="panel-shell flex min-h-0 flex-col px-3 py-2.5" data-testid="attention-list">
  <div class="flex items-center justify-between gap-1.5">
  <div class="relative z-[1] mt-2 grid min-h-0 flex-1 content-start gap-[5px] overflow-y-auto pr-1">
```

Map item tone classes explicitly:

```ts
const itemToneClass = {
  live: 'border-[#45ff9a55] text-[#9cffc5]',
  warn: 'border-[#ffc44780] text-[#ffe08c] [box-shadow:inset_3px_0_#ffc447]',
  bad: 'border-[#ff536a80] text-[#ffadb8] [box-shadow:inset_3px_0_#ff536a]',
  closed: 'border-[#2a93c55e] text-[#91dcff]',
} as const;
```

In `IncidentTable.svelte`, use the same outer layout and keep only item-specific tiny CSS if needed.

- [ ] **Step 4: Remove old duplicated `.badge` ownership**

Delete or narrow the old global `.badge` rules from `src/styles/app.css` so they do not conflict with component-specific status labels. If a shared badge remains necessary, replace it with one `severity-badge` utility and explicit state classes:

```css
@utility severity-badge {
  @apply inline-flex min-w-11 items-center justify-center rounded px-1.5 py-0.5 text-[11px] font-extrabold;
  border: 1px solid var(--relay-line-soft);
}
```

- [ ] **Step 5: Run focused validation**

Run:

```bash
cd /Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-relay/dashboard-ui
pnpm run test
pnpm run check
```

Expected: existing component and model tests still pass.

- [ ] **Step 6: Commit list panel cleanup**

```bash
git add src/components/AttentionList.svelte src/components/IncidentTable.svelte src/styles/app.css
git commit -m "refactor(dashboard-ui): unify list panel styles"
```

---

### Task 4: Partial Migration Of Complex Visualization Components

**Files:**
- Modify: `crates/tqsdk-relay/dashboard-ui/src/components/RelayPipeline.svelte`
- Modify: `crates/tqsdk-relay/dashboard-ui/src/components/IntegrityTrend.svelte`
- Modify: `crates/tqsdk-relay/dashboard-ui/src/components/IntegrityHero.svelte`
- Modify: `crates/tqsdk-relay/dashboard-ui/src/components/ContinuityTimeline.svelte`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({ target: "RelayPipeline", direction: "upstream" })
gitnexus_impact({ target: "IntegrityHero", direction: "upstream" })
gitnexus_impact({ target: "ContinuityTimeline", direction: "upstream" })
```

- [ ] **Step 2: Migrate only containers, toolbars, and text layout**

For `ContinuityTimeline.svelte`, keep the dense grid CSS, but convert the easy parts:

```svelte
<section class="panel-shell flex min-h-0 flex-col px-3 py-2.5" data-testid="continuity-timeline">
  <div class="flex items-center justify-between gap-3">
  <div class="ml-auto flex items-center gap-2">
  <input class="toolbar-input w-[clamp(150px,14vw,220px)]" ... />
  <div class="flex gap-1 rounded-md border border-[color:var(--relay-line-soft)] bg-[#0f2130] p-[3px]">
```

For `IntegrityHero.svelte`, keep tone gradients and orbit animation in CSS, but move base grid and chips to utilities:

```svelte
<section class={`panel-shell grid items-center gap-4 px-[22px] py-3 [grid-template-columns:auto_minmax(0,1fr)_minmax(140px,180px)] ${tone}`}>
  <div class="flex items-center gap-[14px]">
  <div class="flex flex-wrap gap-2">
```

- [ ] **Step 3: Replace runtime-composed class fragments with explicit maps**

Avoid patterns like:

```svelte
class={`badge ${definition.row.status}`}
class={`status-dot ${node.severity}`}
```

Use explicit maps:

```ts
const severityDotClass = {
  live: 'bg-[color:var(--relay-live)] shadow-[0_0_10px_color-mix(in_srgb,var(--relay-live)_70%,transparent)]',
  warn: 'bg-[color:var(--relay-warn)] shadow-[0_0_10px_color-mix(in_srgb,var(--relay-warn)_70%,transparent)]',
  bad: 'bg-[color:var(--relay-bad)] shadow-[0_0_10px_color-mix(in_srgb,var(--relay-bad)_70%,transparent)]',
  closed: 'bg-[color:var(--relay-closed)] shadow-none',
  no_sample: 'bg-[color:var(--relay-muted)] shadow-none',
} as const;
```

- [ ] **Step 4: Keep visual complexity in CSS on purpose**

Do not rewrite these effects into giant class strings:

```css
.hero.warning { background: linear-gradient(90deg, #372708f5, #1f1b08f5); }
.orb::before,
.orb::after { animation: orbit 10s linear infinite; }
.gauge { background: conic-gradient(var(--relay-live) var(--angle), #143346 0); }
```

The success criterion is cleaner ownership, not zero CSS.

- [ ] **Step 5: Run full frontend validation**

Run:

```bash
cd /Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-relay/dashboard-ui
pnpm run test
pnpm run check
pnpm run build
pnpm run test:e2e
```

Expected: unit tests, type checks, production build, and dashboard browser smoke all pass.

- [ ] **Step 6: Commit complex component migration**

```bash
git add src/components/RelayPipeline.svelte src/components/IntegrityTrend.svelte src/components/IntegrityHero.svelte src/components/ContinuityTimeline.svelte
git commit -m "refactor(dashboard-ui): migrate complex panels incrementally"
```

---

## Definition Of Done

- 全局样式文件只保留 token、少量 `@utility`、背景特效和必要滚动条样式。
- `App.svelte`、`MonitorHeader.svelte`、`MetricCard.svelte`、`AttentionList.svelte`、`IncidentTable.svelte` 以 Tailwind utility 为主，不再依赖大块局部 `<style>`。
- `RelayPipeline.svelte`、`IntegrityTrend.svelte`、`IntegrityHero.svelte`、`ContinuityTimeline.svelte` 完成“布局交给 Tailwind、特效留给 CSS”的分层迁移。
- 运行时 class 不再依赖不可静态分析的动态拼接。
- `pnpm run test`、`pnpm run check`、`pnpm run build`、`pnpm run test:e2e` 全部通过。

## Notes

- 当前 `package.json` 已包含 `tailwindcss` 和 `@tailwindcss/vite`，无需新增依赖。
- 若后续发现 utility class 重复过长，再评估是否补一个极小的 `src/lib/ui.ts` 做状态类映射；当前计划先不增加抽象。
- 如果迁移过程中需要对截图或视觉回归做更强验证，可后续追加 Playwright 截图基线，但这不属于本轮必需项。

## Self-Review

- Scope coverage: 已覆盖全局 token、共享 primitive、简单组件、列表组件、复杂组件分层迁移和前端验证。
- Placeholder scan: 计划中未使用 `TODO`、`TBD` 或“后续补充”式步骤；每个任务都包含了明确文件、示例代码和验证命令。
- Type consistency: 所有示例都沿用现有组件名与状态名，如 `live` / `warn` / `bad` / `closed` / `no_sample`，未引入新的运行时状态协议。

Plan complete and saved to `docs/superpowers/plans/2026-06-14-relay-dashboard-tailwind-migration.md`. Two execution options:

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**
