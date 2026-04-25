# AI 工作流守则

本仓库的架构约束以 [`docs/architecture/ai-workflow.md`](docs/architecture/ai-workflow.md) 为准。Codex 在任何新 session 中开始做代码改动前，必须先读取该文档；如果改动涉及 crate 边界、public API、runtime 状态/提交模型、session/query 归属、wait/stream/task/data facade 归属，还必须同步读取相关架构文档。

## 必守架构边界

- `tqsdk-core` 是 protocol-complete runtime substrate，只负责命令、状态、commit/revision、cursor、adapter、schema types 与底层 session/runtime contract；不得重新塞入 high-level facade、direct query convenience、task/data/downloader 语义。
- `tqsdk-session` 是 shared session + one-shot request/response/direct-query 层；GraphQL、schema、metadata、calendar、ranking、EDB、auth refresh、replay one-shot helper 属于这里。
- `tqsdk-wait` 与 `tqsdk-stream` 只做 diff-backed continuous consumption；它们可以通过 `session()` 复用底层 session，但不得复制 direct query API 归属。
- `tqsdk-task` 是执行工具层；`tqsdk-data` 是 research/offline data 层。不要把 task/data 能力下沉回 core/session/wait/stream。
- 所有可见状态变化必须通过 `RuntimeHandle -> StateStore -> CommitResult -> RuntimeReader/UpdateCursor`，不得新增旁路通知、第二棵状态树或 facade 私有 revision。
- domain 状态写入必须经过 `MutationSource` 根路径防线；hot read 应优先使用 `read_market_state()` / `read_trade_state()` 等分区读面，不要在热路径回退到全量 snapshot。
- command/order 状态必须遵守 runtime 状态机；不得用字符串或 adapter 本地判断绕过 `record_command_status()` 的转换校验。
- core public surface 要保持克制；不要重新导出 `TqAuthProvider`、`PasswordCredentials`、`BrokerInfo`、`TqKqAccountConfig`、`ReqwestHttpExecutor`、`ContractFuture` 等已收口的实现细节。

## 架构更新规则

架构可以更新，但不能“顺手改”。如果实现确实改变了架构边界或设计原则，同一轮改动必须同步更新：

- [`docs/architecture/ai-workflow.md`](docs/architecture/ai-workflow.md)
- [`docs/architecture/README.md`](docs/architecture/README.md)
- 受影响的架构专题文档，例如 `crate-boundaries.md`、`api-*.md`、`runtime-core/*.md`、`validation.md`
- 受影响 crate 的 `README.md`
- 根 [`README.md`](README.md)，如果用户可见的 crate 角色或文档入口发生变化
- `CLAUDE.md` 与本文件，如果 AI 工作流入口需要同步调整

提交前应说明是否改变了架构文档；如果没有更新文档，也要能解释本次改动为什么不属于架构变更。
