# AGENTS.md

本文件是本仓库面向 AI 代码代理的跨工具入口。它应该保持短、可执行、低重复；不要在这里承载个人客户端配置、完整架构说明或可由 lint/CI 强制的细碎规则。

## 作用域与优先级

- 本文件适用于整个仓库。以后若在 `crates/*/`、`docs/` 或其他子目录新增嵌套 `AGENTS.md`，更靠近目标文件的规则优先。
- `CLAUDE.md` 只补充 Claude Code 专属说明；若与本文件或 `docs/architecture/*` 冲突，以本文件和当前架构文档为准。
- `docs/architecture/*` 是当前架构权威；`docs/reviews/`、`docs/superpowers/` 和 `docs/archive/` 只能作为计划输入或历史记录，不能覆盖当前代码和架构文档。
- 个人工具配置、通知命令、客户端工具映射、一次性环境偏好不应写入仓库级 `AGENTS.md`；放到用户级配置或客户端专属文档中。

## 项目概况

`tqsdk-rust` 是面向天勤 / TQSDK 生态的 Rust Cargo workspace，使用 Rust edition 2024，MSRV 为 1.85。核心架构是“稳定底座 + 可替换 facade”：所有可见状态变化共享同一套 runtime state tree、commit/revision 和 cursor 语义，上层 crate 只演进用户使用形态。

| Crate | 角色 |
| --- | --- |
| `tqsdk` | 默认 facade、prelude、普通用户入口 |
| `tqsdk-core` | protocol-complete runtime substrate |
| `tqsdk-session` | shared session、one-shot request/response/direct-query |
| `tqsdk-wait` | Python 风格 single-owner `wait_update()` facade |
| `tqsdk-task` | 执行工具层、策略 host、risk gate、replay/backtest foundation |
| `tqsdk-data` | research/offline data、history、cache、export |
| `tqsdk-relay` | 可选 market relay/cache service，不改变 SDK 默认直连路径 |

## 开始任务前

- 修改文件前先查看 `git status --short`，识别用户已有改动；不要覆盖、回滚或提交未授权修改。
- 先分类任务：`docs-only`、局部实现、public API/facade、runtime contract、架构边界变更。
- 只读当前任务必要上下文。不要无目的通读大文档，不要全仓扫文件列表。
- 小步聚焦：只改完成当前任务必须触碰的文件，不顺手重构无关模块。
- 对外部账号、实盘、交易、转账、撤单或 live smoke 相关操作保持显式用户授权和环境变量门控。

## 最小上下文路由

| 触发条件 | 需要读取 |
| --- | --- |
| 只回答问题、不改文件 | 只读与问题直接相关的文档或代码 |
| 文档-only 或 agent 工作流入口 | 相关文档；涉及本文件时读 `docs/architecture/ai-workflow.md`、`README.md`、`docs/README.md` |
| 修改具体 crate | `docs/architecture/ai-workflow.md`、`README.md`、`docs/README.md`、对应 `crates/*/README.md`、目标代码 |
| crate 边界、API 归属或 facade 形态 | 加读 `docs/architecture/README.md`、`docs/architecture/crate-boundaries.md`、相关 `api-*.md` 和 contract examples |
| runtime、状态树、commit/revision、cursor、mutation、command lifecycle | 加读 `docs/architecture/runtime-core/*.md`、`docs/architecture/validation.md` |
| public API、feature flags 或用户入口 | 根 README、受影响 crate README、`crates/*/examples/api_contract_sXX_*.rs` |
| WebSocket 报文、DIFF 合并、状态树字段、行情/交易同步协议 | 按本文 DIFF 查阅规则定向读取 `docs/diff_protocol_spec.md` |
| relay dashboard、dashboard UI、symbol telemetry | 受影响 relay 文档和 `docs/architecture/validation.md` 中的 relay/dashboard 验证项 |

## 工具策略

- 可用时优先用图谱工具理解代码和影响面；不可用或不覆盖目标文件时，再回退到 `rg`、`sed -n` 等定向读取。
- 修改 Rust 函数、方法或类型前，先做 impact analysis；如果风险为 HIGH 或 CRITICAL，先向用户报告直接调用方、受影响流程和风险再继续。
- GitNexus 适合影响面、执行流和提交前 detect changes；CodeGraph 适合源码定位、调用关系和读取已索引源文件。
- 普通文本搜索用 `rg`；不要用慢速全仓 `grep`，不要用字符串替换做重命名。
- 只改文档且不修改 Rust 符号时，不需要符号级 impact analysis。
- 提交前若 GitNexus 可用，应运行 detect changes 检查影响范围；未提交时不必为了文档-only 改动强行运行。

## 工具自动管理区

下面的 marker 块由工具生成或更新。保持 marker 和内容完整；如果需要收敛说明，优先调整上方人工规则，不要手工删除自动块。

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **tqsdk-rust** (17263 symbols, 36568 relationships, 300 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

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

<!-- CODEGRAPH_START -->
## CodeGraph

In repositories indexed by CodeGraph (a `.codegraph/` directory exists at the repo root), reach for it BEFORE grep/find or reading files when you need to understand or locate code:

- **MCP tools** (when available): `codegraph_explore` answers most code questions in one call — the relevant symbols' verbatim source plus the call paths between them. `codegraph_node` returns one symbol's source + callers, or reads a whole file with line numbers. If the tools are listed but deferred, load them by name via tool search.
- **Shell** (always works): `codegraph explore "<symbol names or question>"` and `codegraph node <symbol-or-file>` print the same output.

If there is no `.codegraph/` directory, skip CodeGraph entirely — indexing is the user's decision.
<!-- CODEGRAPH_END -->

## 架构硬边界

- `tqsdk-core` 只负责命令、状态、commit/revision、cursor、adapter、schema types 与底层 runtime contract；不要把 facade、direct query、task/data/downloader 语义塞回 core。
- `tqsdk` 只做默认入口、prelude、轻量 wrapper 和 curated re-export；不得拥有第二棵状态树、第二套 runtime、第二套 direct query / task / data 实现。
- `tqsdk-session` 负责 shared session、one-shot request/response/direct-query、GraphQL、schema、metadata、calendar、ranking、EDB、auth refresh、replay control。
- `tqsdk-wait` 只做 single-owner diff-backed continuous consumption；可以通过 `session()` 复用底层 session，但不得复制 direct query API。
- `tqsdk-task` 是执行工具层；`tqsdk-data` 是 research/offline data 层；不要把 task/data 能力下沉回 core/session/wait 或调用方自建消费层。
- `tqsdk-relay` 是可选 market relay/cache service；它是 workspace member 但不属于 Cargo default-members；不要让现有 SDK crates 默认依赖 relay，也不要把 relay 扩展成通用天勤代理或多 provider 聚合框架。
- 所有可见状态变化必须经过 `RuntimeHandle -> StateStore -> CommitResult -> RuntimeReader/UpdateCursor`。不得新增旁路通知、第二棵状态树或 facade 私有 revision。
- domain 状态写入必须经过 `MutationSource` 根路径防线。hot read 优先使用 `read_market_state()`、`read_trade_state()`、`read_market_trade_state()`。
- command/order 状态必须遵守 runtime 状态机。不得用字符串或 adapter 本地判断绕过 `record_command_status()` 的转换校验。
- core public surface 保持克制。不要重新导出 `TqAuthProvider`、`PasswordCredentials`、`BrokerInfo`、`TqKqAccountConfig`、`ReqwestHttpExecutor`、`ContractFuture` 等已收口的实现细节。

如果不确定改动类别，按更高风险类别处理。任何移动 crate 归属、public API、runtime contract、feature flags 或 facade 边界的改动，都必须同步更新架构文档和受影响 crate README。

## 开发工作流

1. 分类：判断是 docs-only、局部实现、public API/facade、runtime contract，还是架构边界变更。
2. 探索：用图谱或定向搜索获取最小上下文；修改符号前先做影响面分析。
3. 实现：遵循现有模块边界、error 类型、async 风格和测试组织；不要新增无关抽象。
4. 同步：如果属于架构更新，按本文架构更新规则同轮更新文档。
5. 验证：选择最小但足够的验证命令；需要真实账号、实盘权限或 live smoke 的命令必须由用户明确要求。
6. 收尾：提交前确认暂存区只包含本轮改动；完成闭环的 spec / plan / review 文档迁入 `docs/archive/superpowers/`，当前架构权威文档不要自动归档。

## 环境与敏感配置

- 普通开发不需要真实天勤账号；live 行情、交易、query 和历史数据示例才需要 `TQ_AUTH_USER` / `TQ_AUTH_PASS` 及更细的 `TQ_*` 环境变量。
- 账号、密码、token、私钥、客户数据和敏感基础设施信息不得写入仓库、日志、测试快照或提交说明。
- 可能连接实盘或产生交易副作用的示例/测试必须保持 ignored 或显式环境变量门控。
- 任何可能真实下单、撤单、转账或访问实盘账号的命令，必须由用户明确要求并由环境变量门控。

## 常用验证命令

文档-only 或工作流入口改动：

```bash
git diff --check
```

Rust 代码快速自检：

```bash
cargo check --examples
```

可提交单元的默认验证：

```bash
cargo test
cargo clippy --examples --all-targets -- -D warnings
```

格式化相关改动：

```bash
cargo fmt --all --check
```

修改 feature flags、workspace 依赖或 crate feature 传播时，补充：

```bash
cargo check --no-default-features
cargo check --no-default-features --examples
cargo test -p tqsdk-session --no-default-features
cargo check --all-features --examples
```

release-check 环境还应对齐：

```bash
cargo test --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
cargo deny check
cargo package --no-verify
cargo test -p tqsdk-relay --tests
cargo clippy -p tqsdk-relay --all-targets -- -D warnings
cargo check -p tqsdk-relay --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc -p tqsdk-relay --no-deps --all-features
```

场景驱动 public API example 是正式契约。改动 public API、crate 拆分、feature flags 或 facade/runtime 消费方式后，必须保证 `crates/*/examples/api_contract_sXX_*.rs` 能继续清晰、可编译地表达目标场景。

## 架构更新规则

以下行为属于架构更新，不能“顺手改”：

- 新增、删除或重命名 crate
- 移动 direct query、live consumption、task/data 能力归属
- 改变 `RuntimeHandle`、`RuntimeReader`、`UpdateCursor`、`CommitResult` 的语义
- 改变状态树、domain partition、mutation guard、command lifecycle 规则
- 扩大或收窄 core/session/facade public surface
- 改变 feature flags 或依赖裁剪策略，导致用户选择路径变化
- 引入新的 session ownership、actor、cache、aggregation 或 multi-source 模型

同一轮改动必须同步更新相关权威文档：

- `docs/architecture/ai-workflow.md`
- `docs/architecture/README.md`
- 受影响的架构专题文档
- 受影响 crate 的 `README.md`
- 根 `README.md`，如果用户可见入口变化
- `AGENTS.md` / `CLAUDE.md`，如果 AI 工作流入口或硬约束变化
- `docs/architecture/validation.md`，如果验收命令、contract tests 或风险面变化

提交或最终说明中应明确是否属于架构更新；如果没有更新架构文档，也要说明原因。

## DIFF 协议查阅规则

只有当任务涉及修改 WebSocket 数据报文、状态树字段、DIFF 合并、行情/交易同步机制时，才查阅 `docs/diff_protocol_spec.md`。不要在 session 开始时或无针对性地全文读取。

查阅步骤：

1. 先用 `rg` 在 `docs/diff_protocol_spec.md` 中检索特定 `aid`、状态路径或字段名。
2. 再按相关章节定向读取小范围内容，例如 `sed -n '129,216p' docs/diff_protocol_spec.md`。
3. 只读取与当前改动相关的章节。

常用范围：

| 范围 | 内容 |
| --- | --- |
| L129-L216 | diff 合并规则 |
| L217-L255 | 客户端拉取主循环 |
| L256-L318 | 全局数据树结构 |
| L319-L1059 | 上行 `aid` 请求 |
| L1060-L1818 | 下行状态节点字段 |
| L1978-L2041 | 交易流程 |
| L2042-L2210 | 重连和错误处理 |
| L2211-L2552 | Replay / 无真实账户联调 |

## 维护规则

- 根 `AGENTS.md` 应保持为高信号入口，避免超过约 200 行；细节放入架构文档、crate README 或嵌套 `AGENTS.md`。
- 不要在本文件复制 README、完整验证矩阵、长协议索引、个人通知命令或客户端私有工具映射。
- 新增长期规则时，写成可执行约束；能由 formatter、clippy、CI 或 tests 强制的内容，不要用自然语言重复。
- 如果某个目录规则持续增长，优先新增该目录的嵌套 `AGENTS.md`，不要继续膨胀根文件。


<!-- headroom:rtk-instructions -->
# RTK (Rust Token Killer) - Token-Optimized Commands

When running shell commands, **always prefix with `rtk`**. This reduces context
usage by 60-90% with zero behavior change. If rtk has no filter for a command,
it passes through unchanged — so it is always safe to use.

## Key Commands
```bash
# Git (59-80% savings)
rtk git status          rtk git diff            rtk git log

# Files & Search (60-75% savings)
rtk ls <path>           rtk read <file>         rtk grep <pattern>
rtk find <pattern>      rtk diff <file>

# Test (90-99% savings) — shows failures only
rtk pytest tests/       rtk cargo test          rtk test <cmd>

# Build & Lint (80-90% savings) — shows errors only
rtk tsc                 rtk lint                rtk cargo build
rtk prettier --check    rtk mypy                rtk ruff check

# Analysis (70-90% savings)
rtk err <cmd>           rtk log <file>          rtk json <file>
rtk summary <cmd>       rtk deps                rtk env

# GitHub (26-87% savings)
rtk gh pr view <n>      rtk gh run list         rtk gh issue list

# Infrastructure (85% savings)
rtk docker ps           rtk kubectl get         rtk docker logs <c>

# Package managers (70-90% savings)
rtk pip list            rtk pnpm install        rtk npm run <script>
```

## Rules
- In command chains, prefix each segment: `rtk git add . && rtk git commit -m "msg"`
- For debugging, use raw command without rtk prefix
- `rtk proxy <cmd>` runs command without filtering but tracks usage
<!-- /headroom:rtk-instructions -->
