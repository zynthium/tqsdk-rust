# 2026-04-29 Audit Remediation Roadmap

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 整合 `docs/archive/reviews/2026-04-29/public-api-overdesign-audit.md` 与 `docs/archive/reviews/2026-04-29/review-2026-04-29-pending.md`，按风险、依赖和架构影响拆出一条可持续推进的修复路线，先补护栏，再做内部收敛，最后处理 public API breaking changes。

**Architecture:** 本计划遵守 `docs/architecture/ai-workflow.md`、`docs/architecture/crate-boundaries.md` 和 `docs/architecture/api-*.md` 里已经冻结的 crate 边界。所有修复按三层推进：先处理不改 public contract 的安全/测试/注释问题，再处理不改 public shape 的内部重构，最后单独处理需要同步更新架构文档、README 和 examples 的 public API 收窄。任何与当前架构文档冲突的“收窄”建议，必须先做 disposition matrix，再决定是保留、废弃还是 internalize。

**Tech Stack:** Rust, Cargo workspace, crate-level integration tests, workspace examples, architecture docs under `docs/architecture/`, scenario contract docs under `docs/`.

---

## Roadmap Status

This document is the umbrella remediation roadmap, not a single agent-executable implementation unit. Each major task should be executed through a narrower child plan before code changes start.

Child plans:

- `docs/superpowers/plans/2026-04-29-public-api-disposition-matrix.md` covers Task 1.
- `docs/superpowers/plans/2026-04-29-audit-guardrail-fixes.md` covers Task 2.
- `docs/superpowers/plans/2026-04-29-test-harness-guardrails.md` covers Task 3.
- `docs/superpowers/plans/2026-04-29-session-runtime-command-status-refactor.md` covers Task 4 Step 1.
- `docs/superpowers/plans/2026-04-29-session-route-driving-refactor.md` covers Task 4 Step 2.
- `docs/superpowers/plans/2026-04-29-target-pos-control-flow-refactor.md` covers Task 4 Step 3.
- `docs/superpowers/plans/2026-04-29-data-internal-refactor.md` covers Task 5.

Disposition outputs:

- `docs/reviews/public-api-disposition-matrix.md` records the current symbol-level classification and is the gate for Task 2+ execution.

Execution rule:

- Do not execute Task 2 or later until Task 1 has produced a checked-in disposition matrix and the architecture-sensitive API decisions are explicit.
- Do not combine no-API-change fixes, internal refactors, and public API narrowing in the same implementation branch.
- Treat `docs/archive/reviews/2026-04-29/public-api-overdesign-audit.md` and `docs/archive/reviews/2026-04-29/review-2026-04-29-pending.md` as review inputs. They are not architecture authority when they conflict with `docs/architecture/*`.

## Review Findings Applied

This roadmap incorporates the follow-up review performed after the first draft:

- The original plan was too broad to execute as a single implementation plan. It is now explicitly an umbrella roadmap, and each major task must have a narrower child plan before edits begin.
- Task 1 is the required gate. It now has a dedicated child plan and an output matrix: `docs/reviews/public-api-disposition-matrix.md`.
- Unresolved repository license decisions must not block low-risk safety/comment fixes. Task 2 now requires a child plan that separates license metadata from no-API-change guardrail work unless the license has already been chosen.
- Test-harness work must name exact missing cases and helper paths before implementation. Task 3 now requires a child plan with concrete tests rather than a broad coverage mandate.
- The high-churn internal refactors in Task 4 must be split into separate child plans for `session_runtime`, route driving, and `TargetPosTask`, each with characterization tests before extraction.
- The `tqsdk-data` split in Task 5 must choose exact module paths before editing.
- `AuthContext` field privatization is a focused source-breaking change and should be split out unless the disposition matrix proves it must be bundled with broader `core` narrowing.

## File Structure

- Modify: `docs/archive/reviews/2026-04-29/public-api-overdesign-audit.md`
  - 审查项完成后回填状态，或在计划执行后归档为输入文档。
- Modify: `docs/archive/reviews/2026-04-29/review-2026-04-29-pending.md`
  - 审查项完成后回填状态，或在计划执行后归档为输入文档。
- Create: `docs/superpowers/plans/2026-04-29-review-remediation-plan.md`
  - 本计划主文档。
- Modify: `docs/architecture/ai-workflow.md`
  - 仅当 public API 边界、文档入口或 AI 执行规则发生变化时更新。
- Modify: `docs/architecture/README.md`
  - 仅当 public API 边界或 crate 角色说明发生变化时更新。
- Modify: `docs/architecture/crate-boundaries.md`
  - 仅当 `core/session/wait/stream/task/data` 的职责判断被本轮修复改变时更新。
- Modify: `docs/architecture/api-data.md`
  - 仅当 `tqsdk-data` 的公开类型收窄或说明调整时更新。
- Modify: `docs/architecture/api-stream.md`
  - 仅当 `tqsdk-stream` 的公开类型收窄或说明调整时更新。
- Modify: `docs/architecture/api-task.md`
  - 仅当 `tqsdk-task` 的公开类型收窄或说明调整时更新。
- Modify: `docs/architecture/api-layers.md`
  - 若 `RuntimeInput` / `NormalizedMutation` / `SnapshotReadGuard` / `CommitLog` 之类 contract 被调整，必须同步更新。
- Modify: `docs/architecture/validation.md`
  - 收窄 public API 或新增测试矩阵后同步回填。
- Modify: `README.md`
  - 仅当用户可见 crate 入口或文档入口变化时更新。
- Modify: `crates/tqsdk-core/src/lib.rs`
  - 仅在 disposition matrix 确认后收窄 public re-exports。
- Modify: `crates/tqsdk-data/src/lib.rs`
  - 仅在 disposition matrix 确认后收窄 MarketCache 相关 re-exports。
- Modify: `crates/tqsdk-stream/src/lib.rs`
  - 仅在 disposition matrix 确认后收窄 sink/WAL/journal 相关 re-exports。
- Modify: `crates/tqsdk-task/src/lib.rs`
  - 仅在 disposition matrix 确认后收窄 strategy/report/event 相关 re-exports。
- Modify: `crates/tqsdk-core/src/auth.rs`
  - 处理 `AuthContext` 字段可见性。
- Modify: `crates/tqsdk-session/src/tq_auth.rs`
  - 处理 OAuth client 常量注释或内部可配置默认值。
- Modify: `crates/tqsdk-core/src/diff_protocol/outbound.rs`
  - 补 `serde(skip)` 的安全意图注释。
- Modify: `crates/tqsdk-core/src/commands.rs`
  - 评估 `TradePreInsertOrderCommand` 组合化。
- Modify: `crates/tqsdk-core/src/session_runtime.rs`
  - 拆分 command status / reconnect / transport 重复逻辑。
- Modify: `crates/tqsdk-data/src/client.rs`
  - 拆分历史分页、连续主连、权限检查相关重复逻辑。
- Modify: `crates/tqsdk-data/src/download.rs`
  - 内部泛化 download inner/page 重复实现。
- Modify: `crates/tqsdk-task/src/target_pos.rs`
  - 拆分长函数、去掉重复清理、收敛 `Drop`/`finish` 重复。
- Modify: `crates/tqsdk-session/src/client/io.rs`
  - 收敛 route/pending-route 驱动重复逻辑。
- Modify: `crates/tqsdk-core/tests/*.rs`
  - 补 `// SAFETY:` 注释，并新增 runtime/read/state/order_lifecycle 关键测试。
- Modify: `crates/tqsdk-task/tests/*.rs`
  - 为 `TaskHost`、`StrategyHost`、`ExecutionGroup`、`Deployment` 等补测试。
- Modify: `crates/tqsdk-wait/tests/*.rs`
  - 为 `WaitDriver`、`change.rs`、window views 补测试。
- Modify: `crates/tqsdk-session/tests/*.rs`
  - 为 metadata / services helper 补测试。
- Modify: `crates/*/README.md`
  - 仅在 public surface 变化时同步更新。

---

## Task 1: Build the Public API Disposition Matrix First

Child plan:

- `docs/superpowers/plans/2026-04-29-public-api-disposition-matrix.md`

Output:

- `docs/reviews/public-api-disposition-matrix.md`

**Files:**
- Modify: `docs/superpowers/plans/2026-04-29-review-remediation-plan.md`
- Modify: `docs/archive/reviews/2026-04-29/public-api-overdesign-audit.md`
- Modify: `docs/architecture/api-layers.md`
- Modify: `docs/architecture/api-data.md`
- Modify: `docs/architecture/api-stream.md`
- Modify: `docs/architecture/api-task.md`
- Verify: `crates/tqsdk-core/src/lib.rs`, `crates/tqsdk-data/src/lib.rs`, `crates/tqsdk-stream/src/lib.rs`, `crates/tqsdk-task/src/lib.rs`

- [ ] **Step 1: Enumerate all disputed public symbols by crate**

从两份审查中抽出所有被点名的导出类型，至少覆盖以下集合：

- `tqsdk-core`
  - `AggregatedCommit`, `AggregatedCursor`, `AggregatedRuntimeReader`, `AggregatedSnapshotReadGuard`, `StateSourceId`
  - `CommitLog`, `CommitReadGuard`, `CursorLagged`, `OutboundEnvelope`, `SnapshotReadGuard`
  - `FieldMutation`, `NormalizedMutation`, `MutationSource`, `InputPayload`, `RuntimeInput`
  - `CausationMeta`, `CommandEnvelope`, `OutboundDispatch`, `OutboundFrame`, `OutboundRequest`, `HttpMethod`, `HttpRequest`, `ReplayRequest`, `InternalRequest`, `QueryRequest`
- `tqsdk-data`
  - 所有 `MarketCache*` re-exports，尤其是 election / recovery / compaction / queue / daemon / supervisor / report 类型
- `tqsdk-stream`
  - 所有 `StreamSinkWal*` 与 `StreamCommitJournal*` re-exports
- `tqsdk-task`
  - `Strategy*`、`MultiAccountOrder*`、`Execution*` 报表与状态类型

输出要求：
- 在计划文档中新增一张 disposition 表。
- 每个符号打上 `keep`, `deprecate`, `internalize`, `needs-arch-change` 四类之一。

- [ ] **Step 2: Mark architecture-protected symbols**

根据当前架构文档，先把以下符号标记为 `keep unless docs change`：

- `RuntimeInput`
- `NormalizedMutation`
- `SnapshotReadGuard`
- `CommitLog`
- `OutboundRequest`

理由：`docs/architecture/api-layers.md` 和 `docs/architecture/README.md` 已将它们写成当前 stable contract 或兼容原语，不能仅凭审查报告直接内收。

- [ ] **Step 3: Mark examples/README-protected symbols**

对以下类型先标记为 `needs-arch-change`，而不是立即 internalize：

- `MarketCacheWriterElection`
- `MarketCacheWriterElectionReport`
- `MarketCacheRecoveryScan`
- `MarketCacheCompactionOwnership`
- `StreamSinkWalCompaction`
- `StreamSinkWalRecovery`
- `StreamCommitJournal`
- `StrategySupervisorHealth`
- `StrategyTelemetryEvent`

理由：这些符号当前已经出现在 `README`、`docs/architecture/api-*.md` 或 `crates/*/examples/api_contract_sXX_*.rs`，属于已承诺给用户的 surface。

- [ ] **Step 4: Define batch boundaries**

在 disposition 表之后，明确三类批次：

- Batch A: no-API-change fixes
- Batch B: internal refactor with source-compatible public API
- Batch C: public API narrowing with doc and example sync

每一项审查建议必须归到某一批，禁止“边重构边顺手收 API”。

- [ ] **Step 5: Verify the matrix against workspace examples**

Run:

```bash
cargo check --workspace --examples
```

Expected:

```text
Finished `dev` profile ... or a successful workspace example check with no compile errors.
```

如果例子依赖了候选 internalize 类型，该类型不得放进 Batch A/B。

- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/plans/2026-04-29-review-remediation-plan.md docs/archive/reviews/2026-04-29/public-api-overdesign-audit.md docs/architecture/api-layers.md docs/architecture/api-data.md docs/architecture/api-stream.md docs/architecture/api-task.md
git commit -m "docs: classify audit findings by api disposition"
```

---

## Task 2: Ship the Low-Risk Guardrail Fixes

Before executing this task, create a child plan that excludes unresolved license decisions from the safety/comment-only batch unless the repository license has already been explicitly chosen.

Child plan:

- `docs/superpowers/plans/2026-04-29-audit-guardrail-fixes.md`

Output:

- Task 2 guardrail fixes were executed in worktree `.worktrees/audit-guardrails` and committed as `3109ff3 chore: add audit guardrails and safety documentation`.
- License metadata was explicitly excluded from Task 2 because the repository license has not been chosen.

**Files:**
- Modify: `crates/tqsdk-core/tests/runtime_contract_endpoint_config.rs`
- Modify: `crates/tqsdk-core/tests/runtime_contract_command_ledger.rs`
- Modify: `crates/tqsdk-core/tests/runtime_contract_session_cycle.rs`
- Modify: other `crates/tqsdk-core/tests/*.rs` using `noop_waker` or `std::env::set_var`
- Modify: `crates/tqsdk-core/src/diff_protocol/outbound.rs`
- Modify: `crates/tqsdk-session/src/tq_auth.rs`
- Do not modify: workspace `Cargo.toml` until the repository license is explicitly chosen.

- [x] **Step 1: Add `// SAFETY:` comments to every documented unsafe usage**

覆盖至少两类模式：

- `unsafe { std::env::set_var(...) }`
  - 说明由测试级互斥保护，且环境变量修改只在受控单线程/串行区间内发生。
- `unsafe { Waker::from_raw(noop_raw_waker()) }`
  - 说明 raw waker 的 vtable 不持有资源，不执行释放，不跨线程共享可变状态，仅用于轮询同步完成的测试 future。

禁止只修部分文件；本批的完成标准是仓库内相关 `unsafe` 全部都有 `// SAFETY:`。

- [x] **Step 2: Document the public OAuth client constants**

在 `crates/tqsdk-session/src/tq_auth.rs` 的 `CLIENT_ID` / `CLIENT_SECRET` 上方补注释，明确：

- 它们是天勤公开 OAuth2 client 标识，不是用户凭据。
- 用户密码和 access token 仍然来自运行时认证流程。
- 未来若平台轮换 client，可再评估 builder 注入；本批不对外扩大 auth public surface。

- [x] **Step 3: Add intent comment for `serde(skip)` risk rule merge**

在 `crates/tqsdk-core/src/diff_protocol/outbound.rs` 的 `rule` 字段上方补注释，说明：

- 该字段在 `into_value()` 中手工合并。
- 目的是阻止用户通过 `rule` 覆盖保留协议字段，如 `aid` 与 `user_id`。

- [x] **Step 4: Defer workspace license metadata to a separate license decision task**

当前仓库尚未正式决定 license，因此本项已移入独立 decision task；`unsafe` 注释、OAuth 注释和 `serde(skip)` 注释这些 no-API-change 修复不再被 license 决策阻塞。

- [x] **Step 5: Verify guardrail fixes**

Run:

```bash
cargo test -p tqsdk-core
cargo check --workspace
```

Expected:

```text
All targeted tests pass and workspace check succeeds.
```

- [x] **Step 6: Commit**

```bash
git add crates/tqsdk-core/tests crates/tqsdk-core/src/diff_protocol/outbound.rs crates/tqsdk-session/src/tq_auth.rs Cargo.toml
git commit -m "chore: add audit guardrails and safety documentation"
```

---

## Task 3: Add Test Harnesses Before Structural Refactors

Before executing this task, create a child plan with exact missing test names, exact helper file paths, and the behavior each helper must support. Existing tests such as `crates/tqsdk-core/tests/runtime_contract_order_lifecycle.rs` must be extended only after the missing cases are identified.

Child plan:

- `docs/superpowers/plans/2026-04-29-test-harness-guardrails.md`

Output:

- Task 3 test guardrails were executed in worktree `.worktrees/audit-guardrails` and committed as `a94c887 test: add guardrails for high-risk runtime and facade modules`.
- Existing task coverage for no-diff `TaskHost` advancement and StrategyTestHarness usage was retained; new code focused on missing core lifecycle, wait facade, and StrategyContext risk-gate gaps.

**Files:**
- Modify: `crates/tqsdk-core/tests/`
- Modify: `crates/tqsdk-task/tests/`
- Modify: `crates/tqsdk-wait/tests/`
- Modify: `crates/tqsdk-session/tests/`
- Create or modify: helper modules for `TestRuntimeBuilder`, `TestTqApi`, and expanded `StrategyTestHarness` usage

- [x] **Step 1: Add P0 coverage for `tqsdk-core` order lifecycle**

Add focused tests for:

- valid lifecycle transitions
- terminal idempotency
- rejected / failed / cancelled branches

Primary target:

- `crates/tqsdk-core/src/order_lifecycle.rs`

Suggested focused command:

```bash
cargo test -p tqsdk-core order_lifecycle
```

- [x] **Step 2: Add P0 coverage for `tqsdk-task` host and strategy loop**

至少补以下行为：

- `TaskHost` 在无新 diff 时也推进一次本地 task/scheduler
- `StrategyHost` 在同一稳定截面内读取 quote/account/position
- guarded submit 仍遵守 risk gate 和 ownership 约束

Primary targets:

- `crates/tqsdk-task/src/host.rs`
- `crates/tqsdk-task/src/strategy.rs`

- [x] **Step 3: Add P0 coverage for `tqsdk-wait` driver**

至少补以下行为：

- `wait_update()` 的驱动条件
- `is_changing()` / `is_changing_fields()` 的最小契约
- 视图读取不引入第二棵状态树

Primary target:

- `crates/tqsdk-wait/src/driver.rs`

- [x] **Step 4: Add reusable test helpers**

最低要求：

- `tqsdk-core` 增加 `TestRuntimeBuilder` 级别辅助，减少重复 session/runtime 装配
- `tqsdk-wait` 增加 `TestTqApi` 辅助，减少 wait facade 初始化样板
- `tqsdk-task` 尽量复用现有 `StrategyTestHarness` / `FakeMarket` / `FakeBroker`

这一步的目标是为 Task 4/5 的大文件重构铺测试护栏，不追求一次补齐 80% coverage。

- [x] **Step 5: Run targeted high-risk test suites**

Run:

```bash
cargo test -p tqsdk-core
cargo test -p tqsdk-task
cargo test -p tqsdk-wait
cargo test -p tqsdk-session
```

Expected:

```text
The newly added P0 suites pass; any pre-existing unrelated failures are documented explicitly.
```

- [x] **Step 6: Commit**

```bash
git add crates/tqsdk-core/tests crates/tqsdk-task/tests crates/tqsdk-wait/tests crates/tqsdk-session/tests
git commit -m "test: add guardrails for high-risk runtime and facade modules"
```

---

## Task 4: Refactor High-Churn Internals Without Changing Public Shape

Before executing this task, split it into three child plans: one for `session_runtime`, one for `tqsdk-session` route driving, and one for `TargetPosTask`. Each child plan must include characterization tests before extraction.

**Files:**
- Modify: `crates/tqsdk-core/src/session_runtime.rs`
- Modify: `crates/tqsdk-session/src/client/io.rs`
- Modify: `crates/tqsdk-task/src/target_pos.rs`
- Modify: any new internal split files under matching module directories

- [x] **Step 1: Split duplicated command-status derivation in `session_runtime`**

目标：

- 提取通用 helper，覆盖 `derive_trade_*_command_status`
- 不改 `CommandStatus` 语义
- 不改 `RuntimeHandle -> StateStore -> CommitResult -> RuntimeReader` 链路

建议拆分：

- `session_runtime/command_status.rs`
- `session_runtime/reconnect.rs`
- `session_runtime/transport.rs`

Output:

- Executed child plan `docs/superpowers/plans/2026-04-29-session-runtime-command-status-refactor.md`.
- Committed as `7e43df8 refactor: extract session command status derivation`.

- [x] **Step 2: Deduplicate route driving in `tqsdk-session`**

目标：

- 抽出 `drive_route_with_deadline` 类私有 helper
- 消掉 `drive_route_label_once` / `drive_route_once_locked`
- 消掉 `drive_pending_route_label_once` / `drive_pending_once_locked` 重复

限制：

- 不扩大 `tqsdk-session` public surface
- 不回灌 wait/stream 的消费层配置

Output:

- Executed child plan `docs/superpowers/plans/2026-04-29-session-route-driving-refactor.md`.
- Committed as `a7c42e8 refactor: deduplicate session route driving`.

- [x] **Step 3: Simplify `TargetPosTask` control flow**

目标：

- 抽出 `process_wait_update` 内的 cancel / plan / target-check / order-handling 阶段
- 只在入口统一调用一次 `prune_terminal_orders`
- 用 `HashSet::insert` 返回值简化 `cancel_pending_orders_filtered`
- 让 `Drop::drop()` 直接复用 `finish()`

Output:

- Executed child plan `docs/superpowers/plans/2026-04-29-target-pos-control-flow-refactor.md`.
- Committed as `49ff822 refactor: simplify target position control flow`.

限制：

- 不改变当前 target-pos planner 语义
- 不改变 `wait_target_reached()` 完成条件

- [x] **Step 4: Verify internal refactors**

Run:

```bash
cargo test -p tqsdk-core
cargo test -p tqsdk-session
cargo test -p tqsdk-task
```

Expected:

```text
All touched crate tests pass with no public API changes required in examples.
```

Output:

- `cargo test -p tqsdk-core` passed.
- `cargo test -p tqsdk-session` passed.
- `cargo test -p tqsdk-task` passed.

- [x] **Step 5: Commit**

```bash
git add crates/tqsdk-core/src crates/tqsdk-session/src crates/tqsdk-task/src
git commit -m "refactor: split high-churn runtime and task internals"
```

Output:

- Completed through the required child-plan commits instead of one aggregate commit:
- `7e43df8 refactor: extract session command status derivation`
- `a7c42e8 refactor: deduplicate session route driving`
- `49ff822 refactor: simplify target position control flow`

---

## Task 5: Refactor `tqsdk-data` Internals Behind Existing API

Before executing this task, create a child plan with exact module filenames and ownership. Do not leave internal split files as examples such as `page_types.rs`; choose the final paths before editing.

Child plan:

- `docs/superpowers/plans/2026-04-29-data-internal-refactor.md`

**Files:**
- Modify: `crates/tqsdk-data/src/client.rs`
- Modify: `crates/tqsdk-data/src/download.rs`
- Create: internal split files such as `page_types.rs`, `request_types.rs`, `chart_reader.rs`, `cont_quotes.rs`
- Modify: `crates/tqsdk-data/src/lib.rs` only if internal module declarations need updating

- [x] **Step 1: Split `client.rs` by responsibility**

最低拆分目标：

- 分页请求与分页类型
- ready chart/window 读取
- continuous quotes 和 trading days 辅助
- history download permission 检查

限制：

- `DataClient`, `KlineDataPageRequest`, `TickDataPageRequest`, `KlineDataPage`, `TickDataPage` 这些现有 public types 本批保持 source-compatible
- 不把 `query_his_cont_quotes` 或 `query_option_greeks` 下沉回 `tqsdk-session`

- [x] **Step 2: Remove sync/async permission-check duplication**

收敛：

- `require_history_download_permission`
- `require_history_download_permission_async`

可以共享核心校验逻辑，但保持当前同步/异步 public call sites 不变。

- [x] **Step 3: Internalize duplicated download machinery**

通过内部泛型或私有 helper 收敛：

- `KlineDataDownloadInner`
- `TickDataDownloadInner`
- `KlineDataDownloadPage`
- `TickDataDownloadPage`

限制：

- 若 generic 化会破坏 public alias 或 public type 名称，则只在内部泛型化，对外继续保留现有名字。

- [x] **Step 4: Verify `tqsdk-data` compatibility**

Run:

```bash
cargo test -p tqsdk-data
cargo check -p tqsdk-data --examples
```

Expected:

```text
Data crate tests and examples pass with unchanged public usage.
```

- [x] **Step 5: Commit**

```bash
git add crates/tqsdk-data/src crates/tqsdk-data/tests crates/tqsdk-data/examples
git commit -m "refactor(data): split history and download internals"
```

Output:

- Completed through child plan `docs/superpowers/plans/2026-04-29-data-internal-refactor.md`.
- Verification before commit:
  - `cargo test -p tqsdk-data`
  - `cargo check -p tqsdk-data --examples`
  - `cargo check --workspace --examples`
- Committed in worktree `.worktrees/audit-guardrails` as `8c11d14 refactor(data): split history and download internals`.

---

## Task 6: Narrow `tqsdk-core` Public API in a Dedicated Breaking-Change Batch

Before executing this task, split `AuthContext` field privatization into its own focused child plan unless the disposition matrix proves it must be bundled with broader `core` surface narrowing.

Child plans:

- `docs/superpowers/plans/2026-04-29-core-auth-context-privacy.md` covers the focused `AuthContext` field-privacy batch. Completed as `418f7ee refactor(core): privatize auth context fields`.
- `docs/superpowers/plans/2026-04-29-core-safe-surface-narrowing.md` covers Step 1 safe `tqsdk-core` surface narrowing for aggregation root exports and `OutboundEnvelope`. Completed as `1556e93 refactor(core): narrow runtime internal surface`.
- Task 6 docs sync completed as `33e1df5 docs(core): clarify runtime public surface`.

**Files:**
- Modify: `crates/tqsdk-core/src/lib.rs`
- Modify: `crates/tqsdk-core/src/auth.rs`
- Modify: `crates/tqsdk-core/src/commands.rs`
- Modify: `docs/architecture/api-layers.md`
- Modify: `docs/architecture/README.md`
- Modify: `docs/architecture/validation.md`
- Modify: `crates/tqsdk-core/README.md`
- Modify: any affected examples/tests/docs

- [x] **Step 1: Only internalize symbols marked safe in the disposition matrix**

可以进入本批的典型候选：

- 仅被 sibling crate 使用、未被架构文档承诺、未出现在 examples/README 的 runtime assembly 细节
- 明确属于 `internal` bridge 的 transport/session runtime types

不得在本批直接 internalize 的符号：

- `RuntimeInput`
- `NormalizedMutation`
- `SnapshotReadGuard`
- `CommitLog`
- `OutboundRequest`

除非先同步修改架构文档并确认 examples/consumers 不再依赖。

Output:

- Implemented through child plan `docs/superpowers/plans/2026-04-29-core-safe-surface-narrowing.md`.
- Only the disposition-matrix `internalize` candidates were narrowed:
  - aggregation root exports were removed and their test moved to private unit coverage
  - `OutboundEnvelope` was made crate-private and raw `drain_outbound()` removed
- Protected symbols remained public: `RuntimeInput`, `NormalizedMutation`, `SnapshotReadGuard`, `CommitLog`, `OutboundRequest`, `OutboundDispatch`.
- Verification before commit:
  - `cargo test -p tqsdk-core -q --test runtime_contract_v1_capability`
  - `cargo test -p tqsdk-core -q --test runtime_contract_reader_surface --test runtime_contract_surface`
  - `cargo test -p tqsdk-core`
  - `cargo check --workspace --examples`
- Committed in worktree `.worktrees/audit-guardrails` as `1556e93 refactor(core): narrow runtime internal surface`.

- [x] **Step 2: Privatize `AuthContext` fields without widening construction surface**

在 `crates/tqsdk-core/src/auth.rs`：

- 将 `access_token`, `auth_id`, `features` 改为私有
- 保留 `new`, `access_token()`, `auth_id()`, `features()`, `with_auth_id`, `with_feature`
- 修复所有内部与测试构造调用

Output:

- Implemented through child plan `docs/superpowers/plans/2026-04-29-core-auth-context-privacy.md`.
- RED verified with `cargo test -p tqsdk-core --doc` before field privatization.
- Verification before commit:
  - `cargo test -p tqsdk-core -q --test runtime_contract_v1_capability`
  - `cargo test -p tqsdk-core -q --test runtime_contract_reader_surface --test runtime_contract_surface`
  - `cargo test -p tqsdk-core`
  - `cargo check --workspace --examples`
  - `cargo test -p tqsdk-session`
- Committed in worktree `.worktrees/audit-guardrails` as `418f7ee refactor(core): privatize auth context fields`.

- [x] **Step 3: Evaluate `TradePreInsertOrderCommand` composition**

只有在以下条件同时满足时才落地：

- 不破坏现有 examples/tests 的构造 ergonomics，或已有明确迁移方案
- adapter/trade/outbound 逻辑可以更简单，而不是引入额外字段解包样板

如果迁移收益不足，本项允许降级为“记录但不做”。

Output:

- Evaluated via code search and adapter review.
- Current explicit construction sites are core contract tests; there is no README/example-backed migration path for changing the public struct literal shape in this batch.
- Adapter-side benefit is limited: `build_pre_insert_order_message()` already maps shared order fields into the internal `DiffOrderRequest`.
- Decision: no code change in Task 6. Any composition or builder redesign for `TradePreInsertOrderCommand` requires a future dedicated compatibility plan.

- [x] **Step 4: Verify core contract compatibility**

Run:

```bash
cargo test -p tqsdk-core -q --test runtime_contract_v1_capability
cargo test -p tqsdk-core -q --test runtime_contract_reader_surface --test runtime_contract_surface
cargo test -p tqsdk-core
cargo check --workspace --examples
```

Expected:

```text
Core contract tests and workspace examples still pass after the narrowing.
```

Output:

- Verification after Task 6 code changes:
  - `cargo test -p tqsdk-core -q --test runtime_contract_v1_capability`
  - `cargo test -p tqsdk-core -q --test runtime_contract_reader_surface --test runtime_contract_surface`
  - `cargo test -p tqsdk-core`
  - `cargo check --workspace --examples`

- [x] **Step 5: Sync docs**

必须同步更新：

- `docs/architecture/api-layers.md`
- `docs/architecture/README.md`
- `docs/architecture/validation.md`
- `crates/tqsdk-core/README.md`

如用户可见入口变化，再更新根 `README.md`。

Output:

- Updated affected architecture docs and core README:
  - `docs/architecture/api-layers.md`
  - `docs/architecture/runtime-core/session-auth.md`
  - `docs/architecture/README.md`
  - `docs/architecture/validation.md`
  - `crates/tqsdk-core/README.md`
- Root `README.md` was not changed because the root crate-role entry did not describe the narrowed raw outbox / aggregation details.
- Documentation-only sync committed as `33e1df5 docs(core): clarify runtime public surface`.

- [x] **Step 6: Commit**

```bash
git add crates/tqsdk-core docs/architecture crates/tqsdk-core/README.md README.md
git commit -m "refactor(core): narrow public contract surface"
```

Output:

- Completed through focused child-plan commits instead of one aggregate commit:
  - `418f7ee refactor(core): privatize auth context fields`
  - `1556e93 refactor(core): narrow runtime internal surface`
  - `33e1df5 docs(core): clarify runtime public surface`

---

## Task 7: Narrow `tqsdk-data`, `tqsdk-stream`, and `tqsdk-task` Public API With Full Doc Sync

Child plan:

- `docs/superpowers/plans/2026-04-29-data-stream-task-api-narrowing-triage.md` completed triage-only; no code changes were made because all non-keep candidates still require scenario/doc redesign or split compatibility plans.

**Files:**
- Modify: `crates/tqsdk-data/src/lib.rs`
- Modify: `crates/tqsdk-stream/src/lib.rs`
- Modify: `crates/tqsdk-task/src/lib.rs`
- Modify: `docs/architecture/api-data.md`
- Modify: `docs/architecture/api-stream.md`
- Modify: `docs/architecture/api-task.md`
- Modify: `docs/reviews/public-api-scenario-review.md`
- Modify: `crates/tqsdk-data/README.md`
- Modify: `crates/tqsdk-stream/README.md`
- Modify: `crates/tqsdk-task/README.md`
- Modify: affected `crates/*/examples/api_contract_sXX_*.rs`

- [x] **Step 1: Narrow `tqsdk-data` only after examples and docs are rewritten**

优先策略：

- 把 `MarketCacheWriter`, `MarketCacheReader`, `MarketCacheReplay`, `MarketCacheService`, `MarketCacheDaemon`, `MarketCacheSupervisor` 及必要 config 保持为主入口
- 对 election / recovery / compaction / report 细节，如果 examples 和 README 不再直接引用，再考虑下收

如 S18 examples 仍直接展示这些类型，则本项必须先做 example/README 迁移。

Output:

- S18 examples, `docs/architecture/api-data.md`, `crates/tqsdk-data/README.md`, and `docs/reviews/public-api-scenario-review.md` still directly document/import the candidate cache maintenance, manifest, recovery, election, queue/lock/index, compaction, service, daemon, and supervisor types.
- Decision: no `tqsdk-data` public API narrowing in this batch. A future S18 cache API redesign must rewrite scenario examples and docs first.

- [x] **Step 2: Narrow `tqsdk-stream` sink/WAL surface**

优先策略：

- 保留 `CommitSink`, `StreamSinkOptions`, `StreamSinkHandle`, `StreamSinkProfile`, `StreamSinkStatus`, `StreamSinkStats`
- 逐步将 `StreamSinkWalCompaction`, `StreamSinkWalRecovery`, `StreamCommitJournal*` 从主 re-export 中移出，前提是 `api_contract_s21_slow_consumer_isolation.rs` 与 README 不再直接依赖

Output:

- `api_contract_s21_slow_consumer_isolation.rs`, `docs/architecture/api-stream.md`, `crates/tqsdk-stream/README.md`, `docs/reviews/public-api-scenario-review.md`, and stream tests still directly use WAL and commit journal types.
- Decision: no `tqsdk-stream` public API narrowing in this batch. A future S21 durability API redesign must provide replacement high-level examples first.

- [x] **Step 3: Narrow `tqsdk-task` report/status explosion**

优先策略：

- 先把 `Reason` / `Signal` / `Kind` 内联到 report/event 中
- 只有在 examples 不再需要独立导入的前提下，才减少顶层 re-exports

对以下典型对先做评估：

- `StrategySupervisorHealth` + `StrategySupervisorHealthStatus`
- `StrategyRunReport` + `StrategyRunStopReason`
- `StrategyShutdownReport` + `StrategyShutdownSignal`
- `StrategyTelemetryEvent` + `StrategyTelemetryEventKind`
- `MultiAccountOrderState` + `MultiAccountOrderStatus`

Output:

- S15/S20 docs/examples still use `StrategyShutdownSignal` and strategy supervisor/telemetry concepts.
- S12/S13 architecture docs expose `ExecutionGroupStatus` and `MultiAccountOrderStatus` as status return types.
- `StrategyRunReport` / `StrategyRunStopReason` remains `split-plan`.
- Decision: no `tqsdk-task` public API narrowing in this batch. Future task API shape changes require dedicated compatibility plans.

- [x] **Step 4: Verify scenario contracts after narrowing**

Run:

```bash
cargo check --workspace --examples
cargo test --workspace
cargo clippy --workspace --examples --all-targets -- -D warnings
```

Expected:

```text
Workspace examples remain the authoritative public contract and pass after the narrowing.
```

Output:

- Because Task 7 made no code changes, verification confirms the current public scenario contracts still compile and test:
  - `cargo check --workspace --examples`
  - `cargo test --workspace`
  - `cargo clippy --workspace --examples --all-targets -- -D warnings`

- [x] **Step 5: Sync docs and scenario review**

必须同步更新：

- `docs/architecture/api-data.md`
- `docs/architecture/api-stream.md`
- `docs/architecture/api-task.md`
- `docs/reviews/public-api-scenario-review.md`
- `crates/tqsdk-data/README.md`
- `crates/tqsdk-stream/README.md`
- `crates/tqsdk-task/README.md`

Output:

- No docs/examples were rewritten in Task 7 because no public API narrowing was performed.
- Existing docs/examples remain the authoritative current contract and are the reason the candidates were deferred.
- Future redesign plans must update these docs/examples before changing re-exports.

- [x] **Step 6: Commit**

```bash
git add crates/tqsdk-data crates/tqsdk-stream crates/tqsdk-task docs/reviews/public-api-scenario-review.md docs/architecture README.md
git commit -m "refactor: narrow facade crate public surfaces"
```

Output:

- No Task 7 code/docs commit was created.
- Reason: the child triage plan found no safe data/stream/task re-export narrowing under the current scenario contracts.

---

## Task 8: Final Verification and Audit Closure

**Files:**
- Modify: `docs/archive/reviews/2026-04-29/public-api-overdesign-audit.md`
- Modify: `docs/archive/reviews/2026-04-29/review-2026-04-29-pending.md`
- Modify: `docs/architecture/validation.md`

- [x] **Step 1: Re-run the full validation matrix**

Run:

```bash
cargo check --workspace --examples
cargo test --workspace
cargo clippy --workspace --examples --all-targets -- -D warnings
cargo build -p tqsdk-session --no-default-features
cargo build -p tqsdk-wait --no-default-features
cargo build -p tqsdk-stream --no-default-features
cargo build -p tqsdk-task --no-default-features
cargo build -p tqsdk-data --no-default-features
```

Expected:

```text
Workspace build, tests, examples, and no-default-features matrix complete successfully.
```

Output:

- Completed:
  - `cargo check --workspace --examples`
  - `cargo test --workspace`
  - `cargo clippy --workspace --examples --all-targets -- -D warnings`
  - `cargo build -p tqsdk-session --no-default-features`
  - `cargo build -p tqsdk-wait --no-default-features`
  - `cargo build -p tqsdk-stream --no-default-features`
  - `cargo build -p tqsdk-task --no-default-features`
  - `cargo build -p tqsdk-data --no-default-features`

- [x] **Step 2: Update audit documents with disposition and closure status**

对每一条审查项明确标记：

- `done`
- `won't do`
- `moved to breaking-change batch`
- `blocked by architecture decision`

禁止留下模糊状态。

Output:

- Added closure tables to:
  - `docs/archive/reviews/2026-04-29/public-api-overdesign-audit.md`
  - `docs/archive/reviews/2026-04-29/review-2026-04-29-pending.md`
- Each item is classified as `done`, `won't do`, `moved to breaking-change batch`, or `blocked by architecture decision`.

- [x] **Step 3: Record any intentional non-fixes**

至少要对以下可能保留项给出书面理由：

- `TradePreInsertOrderCommand` 若未组合化
- 某些 `MarketCache*` / `StreamSinkWal*` / `Strategy*` 类型若因 public contract 仍保留
- `CLIENT_SECRET` 若只加注释未改 builder 注入

Output:

- `TradePreInsertOrderCommand`: no code change; future dedicated compatibility plan required.
- `MarketCache*`, `StreamSinkWal*`, `StreamCommitJournal*`, `Strategy*`, `Execution*`, and `MultiAccount*` public types: retained because current scenario docs/examples still define them as public contracts.
- `CLIENT_SECRET`: comment-only fix retained; builder injection deferred because the value is a public OAuth2 client identifier and no rotation requirement exists.

- [x] **Step 4: Commit**

```bash
git add docs/archive/reviews/2026-04-29/public-api-overdesign-audit.md docs/archive/reviews/2026-04-29/review-2026-04-29-pending.md docs/architecture/validation.md
git commit -m "docs: close out 2026-04-29 audit remediation"
```

Output:

- No `.worktrees/audit-guardrails` commit was created for Task 8 because `docs/archive/reviews/2026-04-29/public-api-overdesign-audit.md` and `docs/archive/reviews/2026-04-29/review-2026-04-29-pending.md` are currently untracked planning artifacts in the main workspace and absent from the worktree branch.
- The closure updates were written to the main workspace copies of those audit documents.
- The code worktree is clean after all committed remediation work.

---

## Execution Notes

- Batch order is mandatory: `Task 1 -> Task 2 -> Task 3 -> Task 4 -> Task 5 -> Task 6 -> Task 7 -> Task 8`.
- `Task 6` and `Task 7` are the only allowed breaking-change batches.
- 如果 `Task 1` 发现某些符号已被外部文档/example 冻结，则对应项必须从“立即修复”降级为“文档先行 + 迁移后修复”。
- 任何 public API 调整都必须同步检查 `docs/reviews/public-api-scenario-review.md` 和 `crates/*/examples/api_contract_sXX_*.rs`，否则视为计划未完成。

## 2026-05-01 Comprehensive Review Continuation

This section records the follow-up remediation driven by
`docs/reviews/comprehensive-review-2026-04-30.md`.

Additional checkpoints:

- `e2d9712 refactor: close review remediation surfaces`
- `213d04e refactor: continue review remediation cleanup`
- `42cb229 refactor: narrow test support public surface`
- `b0dc62e refactor: keep dyn auth bridge internal`

Additional completed items:

- Removed full-snapshot fallback from `record_command_status` by adding runtime partition reads.
- Reduced order lifecycle overlay cloning without expanding the typed-state migration scope.
- Shared stream WAL and commit journal JSONL writer plumbing.
- Added minimal crate-level doctests for all six crates and public docs for market cache service/daemon/supervisor types.
- Added `RiskEngine` property-style boundary tests for price tick alignment and net-position projection.
- Reduced safe hidden public surface by removing or replacing `TqApi::new_for_test`, `TqStream::new_for_test_with_capacity`, `DataClient::new_for_test_with_urls`, and the root `DynAuthProvider` re-export.
- Opened child plan `docs/superpowers/plans/2026-05-01-test-support-surface-migration.md` for the remaining `_for_test` surface. The first slice removed `TaskHost` hidden ownership hooks and the duplicate `TargetPosTask::applied_target_volume_for_test()` observer, replacing them with documented task-layer public APIs.
- Added `tqsdk_session::testing::ManualSession` as the explicit no-IO/manual session fixture, migrated session construction callers across session/wait/stream/task/data tests and helpers, and removed `SessionClient::new_for_test_with_handle()`.
- Removed `TqStream::handle_for_test()` after migrating stream test support to `stream.session().handle()`. Wait crate tests/support no longer call `TqApi::handle_for_test()`, but the shim remains until task fixture/test callers are migrated.

Remaining items are intentionally not part of this mixed remediation batch:

- `_for_test` feature-gating still needs a dedicated stable fake-harness migration because task/testing and multiple integration contracts rely on runtime injection. TaskHost ownership hooks, the duplicate TargetPos observer, and session hidden construction are now closed; the remaining scope is session dispatch draining, wait/stream manual test-driver hooks, and task fixture ingest/dispatch control.
- `Order.direction` / `offset` / `price_type` enum migration is source-breaking and needs a schema API migration plan.
- Global typed state migration and `CommitResult` ownership changes affect runtime contract and cursor semantics.
- `transport.rs`, `account_group.rs`, and full `sink.rs` module-directory splits require child plans with characterization tests.
- Broader public documentation coverage remains a quality batch, not a blocker for the bug/perf remediation already completed.

## Self-Review

- 两份输入审查的所有主项都已归类到 8 个任务里，没有未归属项。
- 计划明确区分了 `no-API-change`、`source-compatible refactor`、`breaking-change` 三类批次，避免和当前架构文档冲突。
- 所有高风险 public API 收窄项都要求同步更新 `docs/architecture/*`、README 和 examples，符合仓库的 AI 工作流守则。
