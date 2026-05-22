# SDK Overdesign Audit Iteration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a decision-grade SDK overdesign audit and apply the first documentation subtraction pass so the default `tqsdk` path, advanced escape hatches, and performance-sensitive API boundaries are clear before any source-breaking public API changes.

**Architecture:** Keep the existing runtime/session/wait/stream/task/data layering and treat overdesign risk as a public presentation and stabilization problem first. The plan writes review evidence, canonical API path decisions, wait/stream boundary rules, and documentation cleanup without adding a second runtime, moving direct query out of `tqsdk-session`, or changing public Rust symbols in this batch.

**Tech Stack:** Rust 2024 workspace, Cargo examples as public API contracts, Markdown architecture/review docs, `tqsdk` facade docs, `tqsdk-wait` and `tqsdk-stream` consumption facade docs.

---

## Scope Decisions

- This batch is documentation and review output only. It does not remove Rust public symbols.
- The default path for ordinary users is `tqsdk` over `tqsdk-wait`.
- `tqsdk-wait` is the canonical single-owner strategy path, including high-performance full-universe quote workloads when the consumer wants one stable snapshot loop.
- `tqsdk-stream` remains justified only as an advanced async integration crate for multi-consumer fan-out, explicit lag/backpressure, filtering, and service-style `Stream` composition.
- The official Python SDK is the functional benchmark for mature trading semantics; its large single-class shape and implicit mutable data model are not copied into Rust.
- `pseudocodes/tqsdk-rs` is an ergonomics contrast showing the value of a compact first screen; its broad root re-export shape is not copied.
- Any source API removal, feature-gate reshaping, or crate ownership move requires a follow-up source-change plan after this audit is reviewed.

## File Map

Create:

- `docs/reviews/sdk-overdesign-audit-2026-05-22.md`: overall verdict, evidence model, external benchmarks, necessary complexity map, overdesign risk map, canonical API path matrix, wait/stream decision, validation findings, and next iteration batches.

Modify:

- `docs/reviews/public-api-disposition-matrix.md`: add the 2026-05-22 second-pass dispositions with `keep`, `keep-advanced`, `defer`, and `remove-from-active-docs` outcomes.
- `docs/reviews/public-api-scenario-review.md`: add a contract example classification section that separates stable default contracts, advanced contracts, and archived/non-core sketches.
- `docs/scenarios/user-layer-iteration-plan.md`: align user-layer sequencing with the default facade, wait single-owner path, stream advanced integration path, and source API quarantine.
- `README.md`: move first-read emphasis from crate taxonomy to ordinary `tqsdk` flow, then present advanced crate choices below.
- `crates/tqsdk/README.md`: clarify `tqsdk::advanced::*` as a curated escape hatch and direct full-power users to sibling crates.
- `crates/tqsdk-wait/README.md`: state that wait owns high-performance single-owner quote subscription and changed-object iteration.
- `crates/tqsdk-stream/README.md`: state that stream is not the primary quote-performance path and is kept for multi-consumer async integration.
- `crates/tqsdk-task/README.md`: remove the stale claim that S31 slow logs, WAL, and journal use a `tqsdk-stream` managed sink.
- `crates/tqsdk-data/README.md`: keep offline/cache/replay scope narrow and ensure live pipe/cache service claims stay excluded.
- `docs/architecture/README.md`: align the top-level architecture narrative with the now-landed `tqsdk` facade.
- `docs/architecture/crate-boundaries.md`: add the audit conclusion that the crate split is justified but most non-default surfaces are advanced.
- `docs/architecture/api-layers.md`: replace old `tqsdk-api-wait` and `tqsdk-api-stream` naming with current crate names and clarify historical V1 wording.
- `docs/architecture/api-wait.md`: rename the heading and add wait full-universe quote performance requirements.
- `docs/architecture/api-stream.md`: add the stream necessity decision and de-emphasize quote throughput as the reason for existence.
- `docs/architecture/api-task.md`: align durable sidecar language with the task README cleanup.
- `docs/architecture/api-data.md`: keep `MarketCacheStreamWriter` and live stream pipe out of current public API.
- `docs/architecture/validation.md`: add the validation baseline, known blockers, and commands for public API documentation batches.

Do not stage or commit incidental `AGENTS.md` or `CLAUDE.md` GitNexus metadata churn unless the user explicitly asks for those files to change.

## Task 1: Write the Decision-Grade Audit Report

**Files:**
- Create: `docs/reviews/sdk-overdesign-audit-2026-05-22.md`

- [ ] **Step 1: Create the audit report header and verdict**

Create `docs/reviews/sdk-overdesign-audit-2026-05-22.md` with this opening:

```markdown
# SDK Overdesign Audit 2026-05-22

## Verdict

The SDK is not overdesigned because it has multiple crates. The crate split is mostly justified by trading-SDK invariants: one runtime state tree, one commit/revision model, separate one-shot direct query, single-owner wait consumption, multi-consumer stream consumption, execution tooling, and offline data/replay concerns.

The active overdesign risk is public presentation and stabilization breadth. Too many advanced or foundation surfaces are documented as if they are ordinary SDK product promises, and several workflows can be reached through multiple public-looking paths without a clear canonical choice. The next iteration should subtract from first-read docs, quarantine advanced APIs, and stabilize only the smallest high-performance path for each user workflow.
```

- [ ] **Step 2: Add source and benchmark scope**

Append this exact section:

```markdown
## Evidence Scope

Primary in-repo evidence:

- `README.md`
- `docs/architecture/README.md`
- `docs/architecture/crate-boundaries.md`
- `docs/architecture/api-layers.md`
- `docs/architecture/api-wait.md`
- `docs/architecture/api-stream.md`
- `docs/reviews/public-api-scenario-review.md`
- `docs/reviews/public-api-disposition-matrix.md`
- `docs/scenarios/user-layer-iteration-plan.md`
- `crates/tqsdk/src/lib.rs`
- `crates/tqsdk/README.md`
- `crates/tqsdk-wait/README.md`
- `crates/tqsdk-stream/README.md`
- `crates/tqsdk-task/README.md`
- `crates/tqsdk-data/README.md`

External calibration:

- Official Python SDK: <https://github.com/shinnytech/tqsdk-python>
- Unofficial Rust contrast: <https://github.com/pseudocodes/tqsdk-rs/tree/main/tqsdk-rs>

Architecture docs and current code are authoritative. Existing review docs, scenario docs, archived plans, and external repositories are evidence, not overriding contracts.
```

- [ ] **Step 3: Add the necessary complexity map**

Append this table:

```markdown
## Necessary Complexity Map

| Area | Keep | Reason |
| --- | --- | --- |
| `RuntimeHandle -> StateStore -> CommitResult -> RuntimeReader/UpdateCursor` | Yes | A trading SDK needs stable snapshots, reconnect/resync barriers, causality, and commit-bound change interpretation. |
| `tqsdk-core` protocol/runtime substrate | Yes | It protects protocol completeness and avoids tying the kernel to one facade style. |
| `tqsdk-session` one-shot request/response/direct query | Yes | GraphQL, schema, metadata, calendar, ranking, settlement, EDB, auth refresh, and replay control are not live object consumption. |
| `tqsdk-wait` single-owner `wait_update()` facade | Yes | It is the closest Rust equivalent to the official Python strategy loop while preserving typed refs and explicit errors. |
| `tqsdk-stream` advanced multi-consumer facade | Yes, as advanced | It is justified by independent consumers, bounded lag, filtering, health, and `futures::Stream` composition, not by ordinary quote throughput alone. |
| `tqsdk-task` execution tools | Yes, narrowed | Target position, ownership guard, typed order builder, basic risk, strategy test harness, and local sim are mature trading workflow needs. |
| `tqsdk-data` research/offline data | Yes, narrowed | History page/series/download, CSV export, Greeks, cache, and replay are opt-in research/offline workflows that should not pollute live facades. |
| `tqsdk` top-level facade | Yes, thin | Ordinary users need one dependency and one obvious first path while advanced users can depend on sibling crates directly. |
```

- [ ] **Step 4: Add the overdesign risk map**

Append this table:

```markdown
## Overdesign Risk Map

| Risk | Evidence | Decision |
| --- | --- | --- |
| First-read docs teach crate taxonomy before user flow | `README.md` starts with workspace/crate explanation before a default facade workflow has become convincing. | Rewrite first-read docs around `tqsdk` install, connect, quote, wait, order/target, and history; move crate taxonomy below. |
| `tqsdk::advanced::*` is ambiguous | `crates/tqsdk/src/lib.rs` exposes curated subsets but docs can read as a full sibling-crate portal. | Document it as curated convenience only; tell full-power users to depend on sibling crates directly. |
| `tqsdk-stream` root surface is wide | `crates/tqsdk-stream/README.md` lists many object/event streams. | Keep `quote_batches`, commit stream, filters, lag, health, and shutdown as advanced contracts; mark broad object/event streams as advanced until use cases are proven stable. |
| `tqsdk-task` reads like a strategy platform | README and scenario docs include supervisor, deployment, replay, desk, telemetry, fake broker, and sim surfaces. | Keep task as execution tooling; remove wording that implies production daemon, OMS, durable audit, or managed sink ownership. |
| `tqsdk-data` presents a narrow goal with a long surface list | README lists history, cache, download, CSV, Greeks, replay, and cache maintenance types. | Keep as opt-in research/offline crate; ensure first-read docs do not imply live cache service or hot-path dependency. |
| Active docs still contain stale names or claims | `docs/architecture/api-layers.md` and `docs/architecture/api-wait.md` use `tqsdk-api-*`; `crates/tqsdk-task/README.md` still mentions stream managed sink/WAL/journal. | Rename current docs to `tqsdk-wait`/`tqsdk-stream` and remove stale sidecar ownership claims. |
```

- [ ] **Step 5: Add official Python benchmark classification**

Append this table:

```markdown
## Official Python SDK Benchmark

| Python SDK semantics | Rust classification | Rust direction |
| --- | --- | --- |
| One default API object that owns connection and state | Match behavior | `tqsdk::Tq` should be the ordinary entrypoint, but remain a thin facade over `tqsdk-wait`/`task`/`data`. |
| `wait_update()` as central progression point | Match behavior | `tqsdk-wait` and `tqsdk::Tq::next()` remain canonical for single-owner strategy loops. |
| Stable business snapshot after update | Match behavior and improve | Rust keeps revision-bound reads and typed refs instead of mutable dict/DataFrame-style access. |
| Reconnect resubscription and snapshot barrier | Match behavior | Startup/recovery barriers belong in wait/session on the shared runtime state tree. |
| `get_quote`, quote list, kline/tick serials | Match behavior and improve | Keep typed refs/handles and add step-bound changed quote helpers where needed for full-universe workloads. |
| Order insert/cancel, account/position/order/trade refs | Match behavior and improve | Keep typed order price, direction, offset, tickets, and explicit `Result` errors. |
| `TargetPosTask` one owner per account/symbol | Match behavior | Keep in `tqsdk-task` and expose ordinary wrapper through `tqsdk` only when it stays thin. |
| Local risk hooks | Match behavior and improve | Keep basic pre-trade risk in task with typed rejection reports; avoid global risk service claims. |
| Backtest/replay and downloader/data series | Match behavior with separate ownership | Keep local sim/replay in task and history/cache/download in data; do not make them default live-loop concerns. |
| Single huge `TqApi` class and broad root namespace | Reject shape | Rust should prefer typed builders, enums, newtypes, feature gates, and explicit advanced crates. |
| Implicit mutable pandas-like tables | Reject shape | Rust should keep borrowed/owned typed snapshots and low-copy hot paths. |
```

- [ ] **Step 6: Add unofficial Rust contrast classification**

Append this table:

```markdown
## Unofficial Rust Contrast

| Observation from `pseudocodes/tqsdk-rs` | Use for this SDK |
| --- | --- |
| Single `Client` / `ClientBuilder` is easy to explain | Keep the first `tqsdk` screen this direct. |
| README quickly shows market, history, trade, builder, callback, channel, and stream usage | Improve `tqsdk-rust` first-read examples, but avoid presenting all paradigms as equal defaults. |
| Broad root re-exports expose auth, WebSocket, logger, data manager, subscription, and transport-like concerns | Do not copy this; keep implementation details out of the default facade. |
| Callback/channel/stream options appear together in the ordinary path | Use as a cautionary example; `tqsdk-rust` should name one canonical path per workflow. |
```

- [ ] **Step 7: Run markdown sanity check for the new report**

Run:

```bash
rg -n "(T)BD|(T)ODO|fill[[:space:]]+in|implement[[:space:]]+later|add[[:space:]]+appropriate|similar[[:space:]]+to[[:space:]]+Task" docs/reviews/sdk-overdesign-audit-2026-05-22.md
```

Expected: no matches.

- [ ] **Step 8: Commit the report**

Run:

```bash
git add docs/reviews/sdk-overdesign-audit-2026-05-22.md
git commit -m "docs: add sdk overdesign audit report"
```

## Task 2: Add the Canonical API Path and Duplicate-Path Matrix

**Files:**
- Modify: `docs/reviews/sdk-overdesign-audit-2026-05-22.md`

- [ ] **Step 1: Append the canonical API path section**

Append this exact section to `docs/reviews/sdk-overdesign-audit-2026-05-22.md`:

```markdown
## Canonical High-Performance API Paths

| Workflow | Canonical public path | Advanced escape hatch | Paths to de-emphasize in first-read docs |
| --- | --- | --- | --- |
| Ordinary strategy setup | `tqsdk::prelude::*`, `Tq::futures().auth_env().connect().await?` | `tqsdk-wait::TqApiBuilder` for users who want direct wait facade ownership | Starting with `tqsdk-core`, raw session builders, or stream builders. |
| Single-owner quote subscription | `Tq::quote` / `Tq::quotes` through the default facade when available, otherwise `tqsdk-wait::TqApi::quote(s)` | `SessionClient::subscribe_quotes` plus `RuntimeReader` for low-level hot-path users | `tqsdk-stream` examples that imply stream is required for quote throughput. |
| Full-universe single-consumer quote workload | `tqsdk-wait::TqApi::quotes` plus step-bound changed quote iteration | `SessionClient + RuntimeReader::read_market_state()` for custom cursor loops | Per-commit full symbol scans, duplicate raw-session loops in ordinary docs, and stream-only performance framing. |
| Multi-consumer quote/event pipelines | `tqsdk-stream::TqStream::quote_batches` | `commit_stream` plus filters and `RuntimeReader` | `tqsdk-wait` loops with user-managed fan-out channels. |
| Direct metadata/query | `tqsdk-session::SessionClient` query helpers | Raw GraphQL value query in `tqsdk-session` | Duplicating direct query helpers into wait or stream. |
| Kline/tick live serials | `tqsdk-wait::TqApi::kline` / `tick` for single-owner strategies | `tqsdk-stream` row-batch streams for async integration | `tqsdk-data` history/cache docs as live serial source. |
| Order insert/cancel in a strategy loop | `tqsdk::Tq` thin wrappers where present, otherwise `tqsdk-wait` typed order helpers | `tqsdk-task::TaskHost::orders` when task ownership/risk is needed; raw session command only for low-level users | Multiple equivalent root/wait/task examples with no semantic distinction. |
| Target position | `tqsdk::TargetPos` thin wrapper for ordinary TQKQ flow; `tqsdk-task::TargetPosTask` for explicit task users | `TaskHost` for advanced ownership and scheduling | Raw order loops in beginner docs as the recommended target-position path. |
| History download and research data | `tqsdk::Tq::history()` thin helper for ordinary users; `tqsdk-data::DataClient` for explicit data workflows | Session-backed `DataClient::from_session` for shared auth/session | Direct query helpers used as substitutes for data download/cache APIs. |
| Backtest/replay/local sim | `tqsdk-task` and `tqsdk-data` examples for explicit offline workflows | Custom replay into `tqsdk-core`/`tqsdk-session` only for SDK developers | Presenting replay/deployment/supervisor as default first-run SDK usage. |
```

- [ ] **Step 2: Append the wait/stream decision**

Append this exact section:

```markdown
## Wait / Stream Boundary Decision

`tqsdk-stream` should not be justified by claiming it is the faster way to subscribe to all quotes. If `tqsdk-wait` can expose changed quote batches or step-bound changed snapshots without scanning all symbols, then full-universe quote subscription for one strategy loop belongs in the wait/default path.

`tqsdk-stream` remains necessary for workloads with different semantics:

- independent consumers that must advance at their own pace;
- bounded fan-out with explicit lag diagnostics;
- path/domain/object/field filters;
- commit/event pipelines;
- async service composition with `futures::Stream`;
- health, reconnect monitoring, retry policy, and graceful shutdown primitives for service integration.

The rule is consumption model first, throughput second. Single-owner stable snapshot consumers should start with `tqsdk`/`tqsdk-wait`; multi-consumer async systems should use `tqsdk-stream`.
```

- [ ] **Step 3: Append the wait-side performance follow-up**

Append this exact section:

```markdown
## Accepted Wait-Side Performance Follow-Up

The audit should create a follow-up source plan for wait quote iteration only after this docs batch lands. Candidate public shapes to evaluate:

| Candidate | Purpose | Constraint |
| --- | --- | --- |
| `QuoteSet::changed(&WaitStep)` | Iterate changed quote refs for the current step. | Must use the step commit/change set and avoid scanning every subscribed symbol. |
| `QuoteSet::changed_snapshots(&WaitStep)` | Return owned snapshots for changed symbols in the current step. | Must decode only touched symbols and preserve deterministic symbol order. |
| `WaitStep::changed_quote_symbols()` | Expose symbols touched by the current step. | Must not expose raw internal state paths as the ordinary API. |

These APIs should be additive and measured against the current `tqsdk-stream::QuoteBatchSubscription` performance shape before stabilization.
```

- [ ] **Step 4: Run report duplicate-path checks**

Run:

```bash
rg -n "stream is required|faster quote|full-universe.*stream" docs/reviews/sdk-overdesign-audit-2026-05-22.md
```

Expected: no match that frames `tqsdk-stream` as the primary single-consumer quote-performance path.

- [ ] **Step 5: Commit the canonical path matrix**

Run:

```bash
git add docs/reviews/sdk-overdesign-audit-2026-05-22.md
git commit -m "docs: define canonical sdk api paths"
```

## Task 3: Update the Public API Disposition Matrix

**Files:**
- Modify: `docs/reviews/public-api-disposition-matrix.md`

- [ ] **Step 1: Add second-pass disposition values**

In `docs/reviews/public-api-disposition-matrix.md`, extend the disposition table with these rows:

```markdown
| `keep-advanced` | Keep public but document outside the ordinary `tqsdk`/prelude path. |
| `defer` | Valid concept that should remain a plan, archived sketch, or future crate/tooling scope until evidence justifies a stable contract. |
| `remove-from-active-docs` | Remove or rewrite active docs that present this as a current SDK promise. |
```

- [ ] **Step 2: Append the 2026-05-22 matrix**

Append this exact section:

```markdown
## 2026-05-22 SDK Overdesign Audit Pass

| Crate | Symbol or Group | Current Export | Evidence | Disposition | Required Follow-up |
| --- | --- | --- | --- | --- | --- |
| `tqsdk` | `Tq`, `TqBuilder`, `TargetPos`, `prelude` | Root facade | Ordinary users need one obvious entrypoint. | `keep` | Keep thin and scenario-backed by default facade examples. |
| `tqsdk` | `advanced::*` curated aliases | Root module | Useful escape hatch, but not a full replacement for sibling crate dependencies. | `keep-advanced` | Document that full-power users should depend on sibling crates directly. |
| `tqsdk-session` | Direct query, metadata, schema, service query helpers | Crate root / traits | One-shot request/response belongs here, not in wait or stream. | `keep` | Keep out of wait/stream docs except through `session()` escape hatch. |
| `tqsdk-wait` | `TqApi`, `wait_update`, `quote(s)`, live refs, order helpers | Crate root | Canonical single-owner strategy and Python-style stable snapshot path. | `keep` | Add wait-side changed quote iteration plan for full-universe single-consumer workloads. |
| `tqsdk-stream` | `quote_batches`, commit stream, filters, lag diagnostics, health/shutdown | Crate root | Genuinely different async multi-consumer consumption model. | `keep-advanced` | Present as advanced integration, not default quote-performance path. |
| `tqsdk-stream` | Broad object/event stream families | Crate root | Potentially useful but broad for 0.1 stabilization. | `keep-advanced` | Keep outside first-read docs and review before semver stabilization. |
| `tqsdk-task` | `TargetPosTask`, ownership guard, basic risk, typed task order builder, test harness, TqSim | Crate root | Mature trading workflow support and Python benchmark parity. | `keep-advanced` | Expose ordinary thin wrappers through `tqsdk` only for common strategy tasks. |
| `tqsdk-task` | Supervisor/deployment/desk/telemetry/platform-like surfaces | Crate root | Useful foundations but risk reading as a production platform. | `keep-advanced` | Remove wording that implies OMS, daemon, HTTP, GUI, durable audit, or managed sink ownership. |
| `tqsdk-data` | History page/series/download, CSV export, Greeks, cache/replay | Crate root | Research/offline workflow support and Python benchmark parity. | `keep-advanced` | Keep opt-in; do not make live facades depend on history cache in hot paths. |
| Active docs | `MarketCacheStreamWriter`, live cache pipe, managed sink/WAL/journal claims | Removed or stale | These contradict current stream/data/task boundaries. | `remove-from-active-docs` | Rewrite active docs; archived historical sketches may keep them as historical context. |
| Architecture docs | `tqsdk-api-wait`, `tqsdk-api-stream`, historical V1 facade wording | Active docs | Names drifted after actual crate names and facade work landed. | `remove-from-active-docs` | Rename to `tqsdk-wait` / `tqsdk-stream` and clarify historical context. |
```

- [ ] **Step 3: Run matrix wording check**

Run:

```bash
rg -n "rollback|tqsdk-api-wait|tqsdk-api-stream|MarketCacheStreamWriter" docs/reviews/public-api-disposition-matrix.md
```

Expected: matches are either historical rows explicitly marked removed/rollback or the new second-pass stale-doc rows.

- [ ] **Step 4: Commit the disposition matrix update**

Run:

```bash
git add docs/reviews/public-api-disposition-matrix.md
git commit -m "docs: classify sdk public api dispositions"
```

## Task 4: Rewrite the First-Read Default Facade Story

**Files:**
- Modify: `README.md`
- Modify: `crates/tqsdk/README.md`

- [ ] **Step 1: Rewrite the top-level README project positioning**

In `README.md`, replace the `## 项目定位` opening paragraphs with:

```markdown
## 项目定位

普通用户优先从顶层 `tqsdk` crate 开始：连接账号、订阅行情、等待更新、读取账户/持仓、下单或设置目标持仓、按需访问历史数据。内部 crate 仍保持独立边界，但第一次阅读不需要先理解整个 workspace taxonomy。

`tqsdk-rust` 的核心约束是所有可见状态变化都经过同一套 runtime state tree、commit/revision 和 cursor 语义。`tqsdk` 只是默认 facade；它不会复制 direct query、stream、task 或 data 实现。

高级用户可以按需要下钻：
```

Keep the existing crate table below this text, but retitle it to:

```markdown
## 默认入口与高级 crate
```

- [ ] **Step 2: Move ordinary quick start before broad examples**

In `README.md`, ensure the first code example under `## 快速开始` is the `use tqsdk::prelude::*` default facade example. Move task harness, backtest, wait, stream, metadata, and data examples below it as advanced or verification examples.

Use this introduction immediately before the default facade snippet:

```markdown
最小普通策略入口：
```

- [ ] **Step 3: Clarify advanced paths in the root README**

In `README.md`, replace the current "一般使用建议" list with:

```markdown
一般使用建议：

- 普通策略、目标持仓和轻量历史访问：先用 `tqsdk`。
- 已明确需要 Python 风格单 owner 推进点：直接用 `tqsdk-wait`。
- 需要多个异步消费者、fan-out、lag diagnostics 或事件管道：用 `tqsdk-stream`。
- 只做合约、日历、metadata、schema 等一次性查询：用 `tqsdk-session`。
- 做历史数据、批量导出、离线 cache 和 replay：用 `tqsdk-data`。
- 做执行工具、风控、策略 host、fake broker 或本地 sim：用 `tqsdk-task`。
- 自建 facade 或极低层热路径：用 `tqsdk-core + tqsdk-session`。
```

- [ ] **Step 4: Clarify `advanced::*` in `crates/tqsdk/README.md`**

In `crates/tqsdk/README.md`, replace the advanced-user paragraph with:

```markdown
`tqsdk::advanced::*` 是 curated convenience，不是完整 sibling crate mirror。它只暴露默认 facade 常见下钻点：

```rust
use tqsdk::advanced::session::SessionClientBuilder;
use tqsdk::advanced::stream::TqStreamBuilder;
use tqsdk::advanced::runtime::RuntimeReader;
```

需要完整 stream、task、data、session 或 core surface 的用户应直接依赖对应 sibling crate。这样可以让 `tqsdk` 的 semver surface 保持小，同时不限制高级用户使用底层能力。
```

- [ ] **Step 5: Check first-read ordering**

Run:

```bash
rg -n "普通用户优先|workspace taxonomy|默认入口与高级 crate|curated convenience|完整 sibling crate" README.md crates/tqsdk/README.md
```

Expected: each phrase appears in the intended file and the root README presents `tqsdk` before the internal crate taxonomy.

- [ ] **Step 6: Commit the first-read rewrite**

Run:

```bash
git add README.md crates/tqsdk/README.md
git commit -m "docs: center the default tqsdk facade"
```

## Task 5: Clarify Wait and Stream Boundaries

**Files:**
- Modify: `docs/architecture/api-wait.md`
- Modify: `docs/architecture/api-stream.md`
- Modify: `docs/architecture/api-layers.md`
- Modify: `crates/tqsdk-wait/README.md`
- Modify: `crates/tqsdk-stream/README.md`

- [ ] **Step 1: Rename historical wait/stream architecture headings**

Make these replacements:

```text
docs/architecture/api-wait.md:
  # `tqsdk-api-wait` 专题设计
  -> # `tqsdk-wait` 专题设计

docs/architecture/api-wait.md:
  `tqsdk-api-wait` 必须只依赖这些基础能力：
  -> `tqsdk-wait` 必须只依赖这些基础能力：

docs/architecture/api-layers.md:
  ### `tqsdk-api-wait`
  -> ### `tqsdk-wait`

docs/architecture/api-layers.md:
  ### `tqsdk-api-stream`
  -> ### `tqsdk-stream`
```

- [ ] **Step 2: Add wait full-universe quote requirements**

In `docs/architecture/api-wait.md`, after the existing `quotes(...).await` bullet, add:

```markdown
- full-universe 或大批量 quote 订阅不应被迫走 `tqsdk-stream`。如果消费模型仍是单 owner `wait_update()`，`tqsdk-wait` 应提供 step-bound changed quote iteration，避免用户每个 commit 扫描全部 symbol。候选形状包括 `QuoteSet::changed(&WaitStep)`、`QuoteSet::changed_snapshots(&WaitStep)` 或等价薄 helper；它们必须解释当前 `WaitStep` 对应的 commit，不维护 facade 私有 revision。
```

- [ ] **Step 3: Add stream necessity rule**

In `docs/architecture/api-stream.md`, after `## 设计目标`, add:

```markdown
`tqsdk-stream` 的存在理由是异步多消费者消费模型，不是普通单消费者 quote 订阅更快。单 owner 策略即使订阅全市场，也应优先使用 `tqsdk` / `tqsdk-wait` 的高性能 changed quote path；只有当调用方需要独立 consumer 进度、bounded fan-out、lag diagnostics、过滤器、事件管道或 `futures::Stream` 组合时，才应把 `tqsdk-stream` 作为主要入口。
```

- [ ] **Step 4: Update wait README boundary**

In `crates/tqsdk-wait/README.md`, after the `quotes(...).await` boundary paragraph, add:

```markdown
如果一个策略循环需要订阅大量合约甚至全市场，但消费模型仍然是单 owner 稳定截面，性能优化应留在 wait facade：通过当前 `WaitStep` 的 changed symbols / changed snapshots 只读取本轮变化对象，而不是每轮扫描所有订阅合约。`tqsdk-stream` 不应成为单消费者 quote throughput 的默认答案。
```

- [ ] **Step 5: Update stream README boundary**

In `crates/tqsdk-stream/README.md`, after the opening responsibility list, add:

```markdown
`tqsdk-stream` 不是普通策略获取更快 quote 的默认路径。它适合多个 async consumer 共享同一 live session，并且需要独立进度、显式背压、lag diagnostics、过滤器、事件管道或 service-style shutdown/health/retry 语义的场景。单 owner `wait_update()` 策略应优先使用 `tqsdk` / `tqsdk-wait`。
```

- [ ] **Step 6: Verify no active docs frame stream as single-consumer performance default**

Run:

```bash
rg -n "更快 quote|quote throughput|全市场.*stream|stream.*全市场|tqsdk-api-wait|tqsdk-api-stream" docs/architecture/api-wait.md docs/architecture/api-stream.md docs/architecture/api-layers.md crates/tqsdk-wait/README.md crates/tqsdk-stream/README.md
```

Expected: no old `tqsdk-api-*` names and no wording that makes stream the primary single-consumer quote-performance path.

- [ ] **Step 7: Commit wait/stream boundary docs**

Run:

```bash
git add docs/architecture/api-wait.md docs/architecture/api-stream.md docs/architecture/api-layers.md crates/tqsdk-wait/README.md crates/tqsdk-stream/README.md
git commit -m "docs: clarify wait and stream api boundaries"
```

## Task 6: Remove Stale Platform and Sidecar Claims from Active Docs

**Files:**
- Modify: `crates/tqsdk-task/README.md`
- Modify: `crates/tqsdk-data/README.md`
- Modify: `docs/architecture/api-task.md`
- Modify: `docs/architecture/api-data.md`
- Modify: `docs/architecture/README.md`
- Modify: `docs/architecture/crate-boundaries.md`

- [ ] **Step 1: Remove stale S31 managed sink wording**

In `crates/tqsdk-task/README.md`, replace:

```markdown
  - 慢日志、WAL 和 journal 使用 `tqsdk-stream` sidecar managed sink 组合，sink
    不进入 profile public API
```

with:

```markdown
  - 慢日志、WAL、journal 和 audit sidecar 由调用方或上层服务拥有，不进入
    profile public API，也不由 `tqsdk-stream` 托管
```

- [ ] **Step 2: Align task architecture sidecar language**

In `docs/architecture/api-task.md`, ensure the S31/trading desk sections contain this sentence:

```markdown
慢日志、WAL、journal、落盘重试、audit sidecar 和跨进程恢复由调用方或上层服务拥有；`TradingDeskProfile` 不持有 sink、WAL、journal 或 cache writer。
```

- [ ] **Step 3: Keep data live-pipe exclusion explicit**

In `crates/tqsdk-data/README.md`, ensure the Market Cache Foundation section contains this sentence:

```markdown
当前 active public API 不包含 `MarketCacheStreamWriter`、live stream pipe、跨进程 cache service、daemon/supervisor orchestration 或 hot-path live cache dependency。
```

- [ ] **Step 4: Keep architecture data exclusion explicit**

In `docs/architecture/api-data.md`, ensure the cache section contains this sentence:

```markdown
`MarketCacheStreamWriter`、live stream pipe、stream feature、跨进程 cache service、daemon/supervisor orchestration 和 live hot-path cache dependency 均不属于当前 `tqsdk-data` public API。
```

- [ ] **Step 5: Add architecture-level overdesign conclusion**

In `docs/architecture/crate-boundaries.md`, after the current conclusion paragraph, add:

```markdown
2026-05-22 overdesign audit conclusion: the crate split remains justified, but the first-read product surface must be smaller than the internal workspace. `tqsdk` / `tqsdk-wait` carry the ordinary strategy path; `tqsdk-stream` / `tqsdk-task` / `tqsdk-data` should be documented as advanced or opt-in unless a scenario clearly belongs in the default SDK path.
```

- [ ] **Step 6: Verify stale active-doc references**

Run:

```bash
rg -n "MarketCacheStreamWriter|managed sink|sidecar managed sink|tqsdk-api-wait|tqsdk-api-stream" README.md crates/*/README.md docs/architecture docs/reviews docs/scenarios
```

Expected: active docs either have no matches or matches are explicit exclusions/historical rows. Matches under `docs/archive/` are acceptable historical context.

- [ ] **Step 7: Commit stale-doc cleanup**

Run:

```bash
git add crates/tqsdk-task/README.md crates/tqsdk-data/README.md docs/architecture/api-task.md docs/architecture/api-data.md docs/architecture/README.md docs/architecture/crate-boundaries.md
git commit -m "docs: remove stale sdk sidecar promises"
```

## Task 7: Classify Contract Examples and Scenario Scope

**Files:**
- Modify: `docs/reviews/public-api-scenario-review.md`
- Modify: `docs/scenarios/user-layer-iteration-plan.md`

- [ ] **Step 1: Add contract classification to scenario review**

In `docs/reviews/public-api-scenario-review.md`, after `## 核心能力边界`, add:

```markdown
## 2026-05-22 Contract Classification

| Class | Meaning | Example groups |
| --- | --- | --- |
| Default contract | Ordinary users can start here without understanding internal crate boundaries. | `tqsdk` facade examples, wait quote/order/target-position flows. |
| Core advanced contract | Stable enough to remain active, but aimed at users who chose a specific crate or consumption model. | session direct query, stream commit/quote batches/lag/health, data history/cache/export, task risk/test/local sim. |
| Foundation advanced contract | Useful foundation that should stay out of first-read docs until stabilization evidence is stronger. | broad stream object/event families, task supervisor/deployment/desk/telemetry surfaces. |
| Archived or non-core sketch | Historical design input or user-tooling/platform scope. | daemon orchestration, GUI/web helpers, managed sink/WAL/journal ownership, cross-process cache service, automatic hedge/flatten engines. |
```

- [ ] **Step 2: Add scenario sequencing rule**

In `docs/scenarios/user-layer-iteration-plan.md`, add this rule near the top-level user-layer section:

```markdown
2026-05-22 sequencing rule: a scenario can be active without being first-read default. Default docs should show only the smallest ordinary `tqsdk` / `tqsdk-wait` path. Stream, task, and data scenario contracts remain active when they prove distinct advanced workflows, but they should not expand the default facade or root prelude by default.
```

- [ ] **Step 3: Search active scenarios for non-core platform framing**

Run:

```bash
rg -n "GUI|HTTP health|daemon|durable queue|managed sink|WAL|journal|MarketCacheStreamWriter|automatic hedge|自动 hedge|自动补单|cache service" docs/reviews/public-api-scenario-review.md docs/scenarios/user-layer-iteration-plan.md crates/*/examples
```

Expected: active matches are explicitly framed as excluded, archived, advanced, or non-core. Contract examples should not require removed `MarketCacheStreamWriter` or managed sink types.

- [ ] **Step 4: Commit scenario classification**

Run:

```bash
git add docs/reviews/public-api-scenario-review.md docs/scenarios/user-layer-iteration-plan.md
git commit -m "docs: classify sdk contract example scope"
```

## Task 8: Update Validation Baseline and Known Blockers

**Files:**
- Modify: `docs/architecture/validation.md`
- Modify: `docs/reviews/sdk-overdesign-audit-2026-05-22.md`

- [ ] **Step 1: Add docs-batch validation requirements**

In `docs/architecture/validation.md`, add this section:

```markdown
## Public API Documentation Batch Validation

For docs-only public API audit batches, run:

```bash
git diff --check
cargo check --workspace --examples
```

If public API source, feature flags, or crate dependencies change, also run:

```bash
cargo fmt --all --check
cargo test --workspace
cargo check --workspace --no-default-features
cargo check --workspace --no-default-features --examples
cargo check --workspace --all-features --examples
```

Known branch risks that must be rechecked before a source API narrowing batch:

- `cargo test --workspace` previously exposed scheduler test failures in `crates/tqsdk-task/tests/scheduler.rs`.
- `cargo test -p tqsdk-session --no-default-features` previously exposed `tests/live_smoke.rs` compilation failures when service-gated methods were referenced without the corresponding feature surface.
```

- [ ] **Step 2: Add validation findings to the audit report**

Append this section to `docs/reviews/sdk-overdesign-audit-2026-05-22.md`:

```markdown
## Validation Baseline

Minimum verification for this docs audit batch:

- `git diff --check`
- `cargo check --workspace --examples`

Before any follow-up source API narrowing, re-run the broader matrix:

- `cargo fmt --all --check`
- `cargo test --workspace`
- `cargo check --workspace --no-default-features`
- `cargo check --workspace --no-default-features --examples`
- `cargo check --workspace --all-features --examples`

Known risks to verify before source changes:

- Scheduler tests in `tqsdk-task` have previously failed under full workspace tests.
- `tqsdk-session` no-default live smoke tests have previously referenced service-gated methods without a compatible feature surface.
```

- [ ] **Step 3: Run docs validation**

Run:

```bash
git diff --check
cargo check --workspace --examples
```

Expected: `git diff --check` passes. `cargo check --workspace --examples` should pass for a docs-only batch; if it fails, copy the exact command, failing crate, and first compiler error into the audit report under `## Validation Baseline`.

- [ ] **Step 4: Commit validation docs**

Run:

```bash
git add docs/architecture/validation.md docs/reviews/sdk-overdesign-audit-2026-05-22.md
git commit -m "docs: record sdk api validation baseline"
```

## Task 9: Final Review and Handoff

**Files:**
- Inspect: all changed files in this plan

- [ ] **Step 1: Check the complete changed-file set**

Run:

```bash
git status --short
```

Expected: only intentional docs from this plan are modified. `AGENTS.md` and `CLAUDE.md` GitNexus metadata churn should remain unstaged unless intentionally updated in a separate AI-workflow batch.

- [ ] **Step 2: Run stale-claim checks**

Run:

```bash
rg -n "tqsdk-api-wait|tqsdk-api-stream|MarketCacheStreamWriter|sidecar managed sink|stream.*faster quote|更快.*stream" README.md crates/*/README.md docs/architecture docs/reviews docs/scenarios
```

Expected: no active-doc matches except explicit exclusion rows, historical references, or archived context.

- [ ] **Step 3: Run final whitespace check**

Run:

```bash
git diff --check
```

Expected: pass.

- [ ] **Step 4: Run GitNexus change detection before final commit**

Run GitNexus staged or all-change detection after staging the final docs-only changes:

```bash
git add docs/reviews docs/scenarios docs/architecture README.md crates/tqsdk/README.md crates/tqsdk-wait/README.md crates/tqsdk-stream/README.md crates/tqsdk-task/README.md crates/tqsdk-data/README.md
```

Then call `gitnexus_detect_changes` with `scope: "staged"`.

Expected: docs-only changes and no unexpected execution-flow risk.

- [ ] **Step 5: Commit any remaining docs-only changes**

Run:

```bash
git commit -m "docs: complete sdk overdesign audit iteration"
```

If there are no remaining staged changes because earlier tasks were already committed, record that the plan finished with no final commit needed.

## Follow-Up Source Plans After This Batch

Create separate implementation plans for any accepted source changes. The strongest candidates are:

1. `tqsdk-wait` changed quote iteration for full-universe single-consumer performance.
2. `tqsdk::advanced::*` documentation-backed surface guard tests.
3. `tqsdk-stream` stabilization split between core advanced APIs and broad object/event families.
4. `tqsdk-task` report/status and platform-like surface quarantine.
5. `tqsdk-data` research/offline surface grouping and optional tabular adapters.

Each follow-up plan must run GitNexus impact analysis before editing Rust symbols and must include focused API contract examples or tests.
