# AGENTS.md

本文件是本仓库面向 AI 代码代理的跨工具主入口。`CLAUDE.md` 只补充
Claude Code 专属工具说明；如果两者不一致，以本文件和
`docs/architecture/*` 的当前架构文档为准。

## 必读顺序

新 session 开始代码改动前，先读取：

1. [`docs/architecture/ai-workflow.md`](docs/architecture/ai-workflow.md)
2. [`README.md`](README.md)
3. [`docs/README.md`](docs/README.md)
4. [`docs/architecture/README.md`](docs/architecture/README.md)
5. 受影响 crate 的 `README.md`
6. 受影响专题文档，例如 `crate-boundaries.md`、`api-*.md`、
   `runtime-core/*.md`、`validation.md`

`docs/reviews/` 是当前审查和 public API 决策记录，`docs/archive/`
是历史审查输入，`docs/superpowers/` 是仍需跟踪的 agentic specs / plans。
这些目录只能作为上下文或计划证据，不能覆盖 `docs/architecture/*` 和当前代码。

## 项目概况

本仓库是 Rust 版 TQSDK 的 Cargo workspace，使用 Rust edition 2024，
MSRV 为 1.85。当前 workspace 成员：

| Crate | 角色 |
| --- | --- |
| `tqsdk` | 面向普通用户的默认 facade / prelude，总入口 |
| `tqsdk-core` | protocol-complete runtime substrate |
| `tqsdk-session` | shared session + one-shot request/response/direct-query 层 |
| `tqsdk-wait` | Python 风格 single-owner `wait_update()` facade |
| `tqsdk-stream` | Rust async-native multi-consumer commit stream facade |
| `tqsdk-task` | 执行工具层和策略/task foundation |
| `tqsdk-data` | research/offline data、history、cache、export 能力 |

仓库采用“稳定底座 + 可替换 facade”的分层。先保持统一 runtime contract，
再在上层 crate 中演进用户使用形态。

## 架构硬边界

- `tqsdk-core` 只负责命令、状态、commit/revision、cursor、adapter、
  schema types 与底层 session/runtime contract。不要把 high-level facade、
  direct query convenience、task/data/downloader 语义塞回 core。
- `tqsdk` 只做对外默认入口、prelude、轻量 ergonomic wrapper 和 curated
  re-export；不得拥有第二棵状态树、第二套 runtime、第二套 direct query /
  task / data 实现。
- `tqsdk-session` 负责 shared session 和 one-shot request/response/direct-query。
  GraphQL、schema、metadata、calendar、ranking、EDB、auth refresh、
  replay one-shot helper 属于这里。
- `tqsdk-wait` 与 `tqsdk-stream` 只做 diff-backed continuous consumption。
  它们可以通过 `session()` 复用底层 session，但不得复制 direct query API 归属。
- `tqsdk-task` 是执行工具层；`tqsdk-data` 是 research/offline data 层。
  不要把 task/data 能力下沉回 core/session/wait/stream。
- 所有可见状态变化必须经过
  `RuntimeHandle -> StateStore -> CommitResult -> RuntimeReader/UpdateCursor`。
  不得新增旁路通知、第二棵状态树或 facade 私有 revision。
- domain 状态写入必须经过 `MutationSource` 根路径防线。hot read 优先使用
  `read_market_state()`、`read_trade_state()`、`read_market_trade_state()` 等分区读面。
- command/order 状态必须遵守 runtime 状态机。不得用字符串或 adapter 本地判断绕过
  `record_command_status()` 的转换校验。
- core public surface 保持克制。不要重新导出 `TqAuthProvider`、
  `PasswordCredentials`、`BrokerInfo`、`TqKqAccountConfig`、
  `ReqwestHttpExecutor`、`ContractFuture` 等已收口的实现细节。

如果不确定改动类别，按更高风险类别处理；如果改动会移动 crate 归属、
public API、runtime contract、feature flags 或 facade 边界，必须同步更新
架构文档和受影响 crate README。

## 探索与工具优先级

本项目可能配置 `code-review-graph` MCP 知识图谱。具备这些工具时，先调用
`get_minimal_context_tool` 获取最小上下文，再按任务使用图谱工具；只有图谱不覆盖时
才回退到文本搜索。

| 场景 | 优先工具 |
| --- | --- |
| 首次进入任务 | `get_minimal_context_tool` |
| 构建或刷新图谱 | `build_or_update_graph_tool` |
| 探索代码 | `semantic_search_nodes_tool` 或 `query_graph_tool` |
| 理解影响面 | `get_impact_radius_tool`、`get_affected_flows_tool` |
| 代码审查 | `detect_changes_tool` + `get_review_context_tool` |
| 查调用、依赖、测试关系 | `query_graph_tool` |
| 架构问题 | `get_architecture_overview_tool` + `list_communities_tool` |
| 计划重构 | `refactor_tool` |
| 查结构风险 | `get_hub_nodes_tool`、`get_bridge_nodes_tool`、`get_knowledge_gaps_tool` |

不同客户端可能给 MCP 工具追加命名空间前缀，例如
`mcp__code-review-graph__query_graph_tool`；语义以 README 中的 `*_tool` 名称为准。
如果当前代理环境没有这些 MCP 工具，或图谱不覆盖目标文件，明确说明原因后使用
`rg` / `rg --files` / 文件读取。普通文本搜索优先用 `rg`，不要用慢速全仓
`grep` 扫描。

## 开发工作流

- 默认在现有 workspace 中保持小步、聚焦的改动；不要顺手重构无关模块。
- 开始实现前先确认工作树状态，不要覆盖用户已有改动。
- Rust 代码遵循现有模块边界、error 类型、async 风格和测试组织。
- core 保持纯 async substrate，不在内部创建 Tokio runtime，也不把 reqwest/base64
  等天勤实现依赖重新暴露成 core public API。
- 不要用 public re-export 解决 sibling crate 的内部协作问题；必要桥接保持最窄可见性。
- 需要外部账号、实盘权限或 live smoke 的验证必须保持 ignored 或显式环境变量门控。

完成并验证一个可提交的改动单元后，优先自动提交，再继续下一项任务。用于指导已完成
代码修改的 spec / plan / review 文档，在闭环后迁入 `docs/archive/superpowers/`；
当前架构权威文档不要自动归档。

## 常用验证命令

文档-only 或工作流入口改动：

```bash
git diff --check
```

Rust 代码改动默认验证：

```bash
cargo fmt --all --check
cargo check --workspace --examples
cargo test --workspace
cargo clippy --workspace --examples --all-targets -- -D warnings
```

修改 feature flags、workspace 依赖或 crate feature 传播时，补充：

```bash
cargo check --workspace --no-default-features
cargo check --workspace --no-default-features --examples
cargo test -p tqsdk-session --no-default-features
cargo check --workspace --all-features --examples
```

发布或 release-check 环境还应对齐 CI：

```bash
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
cargo deny check
cargo package --workspace --no-verify
```

场景驱动 public API example 是正式契约。改动 public API、crate 拆分、
feature flags 或 facade/runtime 消费方式后，必须保证
`crates/*/examples/api_contract_sXX_*.rs` 能继续清晰、可编译地表达目标场景。

## 架构更新规则

架构可以更新，但不能“顺手改”。以下行为属于架构更新：

- 新增、删除或重命名 crate
- 移动 direct query、live consumption、task/data 能力归属
- 改变 `RuntimeHandle`、`RuntimeReader`、`UpdateCursor`、`CommitResult` 的语义
- 改变状态树、domain partition、mutation guard、command lifecycle 规则
- 扩大或收窄 core/session/facade public surface
- 改变 feature flags 或依赖裁剪策略，导致用户选择路径变化

同一轮改动必须同步更新：

- [`docs/architecture/ai-workflow.md`](docs/architecture/ai-workflow.md)
- [`docs/architecture/README.md`](docs/architecture/README.md)
- 受影响的架构专题文档
- 受影响 crate 的 `README.md`
- 根 [`README.md`](README.md)，如果用户可见入口变化
- `AGENTS.md` / `CLAUDE.md`，如果 AI 工作流入口或硬约束变化
- [`docs/architecture/validation.md`](docs/architecture/validation.md)，如果验收命令、
  contract tests 或风险面变化

提交前说明是否改变了架构文档；如果没有更新文档，也要能解释本次改动为什么不属于
架构变更。

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **tqsdk-rust** (11507 symbols, 23076 relationships, 300 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> If any GitNexus tool warns the index is stale, run `npx gitnexus analyze` in terminal first.

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `gitnexus_detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `gitnexus_query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `gitnexus_context({name: "symbolName"})`.

## Never Do

- NEVER edit a function, class, or method without first running `gitnexus_impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `gitnexus_rename` which understands the call graph.
- NEVER commit changes without running `gitnexus_detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/tqsdk-rust/context` | Codebase overview, check index freshness |
| `gitnexus://repo/tqsdk-rust/clusters` | All functional areas |
| `gitnexus://repo/tqsdk-rust/processes` | All execution flows |
| `gitnexus://repo/tqsdk-rust/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->
