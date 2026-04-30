# AI 工作流与架构守则

## 文档定位

本文档是给 Codex、Claude Code 和其他代码代理的新 session 入口。它不是替代完整架构文档，而是把当前架构的硬边界、设计意图和变更同步规则固化下来，防止后续代理因为只看到局部代码或旧审查报告而随意改动边界。

权威阅读顺序：

1. 本文档
2. [`README.md`](../../README.md)
3. [`docs/README.md`](../README.md)
4. [`docs/architecture/README.md`](README.md)
5. [`crate-boundaries.md`](crate-boundaries.md)
6. 受影响 crate 的 `README.md`
7. 受影响专题文档，例如 `api-wait.md`、`api-stream.md`、`api-task.md`、`api-data.md`、`runtime-core/*.md`、`validation.md`

[`docs/reviews/`](../reviews/) 是当前审查和 public API 决策记录，[`docs/archive/`](../archive/) 是历史审查输入，[`docs/superpowers/`](../superpowers/) 是 specs/plans 执行记录。它们都不是当前架构的唯一权威来源。若审查报告、历史计划与已落地代码或 `docs/architecture` 不一致，必须先核对代码和架构文档，再决定是否把建议转化为新的计划。

## 当前总体架构

当前 workspace 采用“稳定底座 + 可替换 facade”的分层：

```text
tqsdk-core
    ^
    |
tqsdk-session
    ^
    |
tqsdk-wait / tqsdk-stream / tqsdk-data
    ^
    |
tqsdk-task
```

设计意图：

- 保留一个 protocol-complete runtime contract，先保证所有远端协议域共享同一套状态、revision、commit、causality 和 cursor 语义。
- 把用户使用形态放在上层 crate 中演进，避免 core 为某一种 facade 心智提前定型。
- 让高性能用户可以停留在 `tqsdk-core + tqsdk-session`，让 Python 心智用户使用 `tqsdk-wait`，让多消费者异步系统使用 `tqsdk-stream`，让执行和研究能力分别进入 `tqsdk-task` 与 `tqsdk-data`。
- 避免回退成单体 `TqApi` crate，也避免把 direct query、task、downloader、research helpers 塞回底层。

## Crate 职责边界

### `tqsdk-core`

职责：

- 统一命令模型、命令账本和命令状态机
- 统一 runtime state、domain partitions、commit/revision、causality、cursor/log
- protocol adapters、transport contracts、session runtime orchestration
- 官方对象的纯 schema/type contract
- `RuntimeHandle` / `RuntimeReader` / `UpdateCursor` / `CommitResult`

非职责：

- `wait_update()` / stream / callback / `TqApi` facade
- GraphQL / HTTP direct-query convenience wrappers
- downloader、DataFrame/polars、GUI/report、research workflow
- `TargetPosTask`、scheduler、业务执行工具
- 天勤账号认证、`TqKq`、reqwest HTTP executor 这类具体实现的 public core API

设计原因：

- core 是所有上层 facade 的公共底座，public surface 越宽，后续破坏成本越高。
- core 必须保持纯 async substrate，不在内部创建 Tokio runtime，也不强迫用户引入 reqwest/base64 等天勤实现依赖。
- `StateSnapshot` 和 `CommitLog` 可以作为兼容/底层原语存在，但主读契约应是 `RuntimeReader` 及其 read guard。

禁止回退：

- 不要恢复 `ContractFuture` public alias；trait async 边界使用 AFIT/RPITIT，boxing 只允许在显式 dyn erased boundary。
- 不要从 `tqsdk-core` 重新导出 `TqAuthProvider`、`PasswordCredentials`、`BrokerInfo`、`TqKqAccountConfig`、`ReqwestHttpExecutor`。
- 不要让 core 重新依赖 `reqwest` 或 `base64`。
- 不要在 `tqsdk_core::internal` 下新增面向用户的 API；它只是 session 吸收 runtime assembly 细节期间供 sibling crates 使用的临时桥接层，不能演变成第二套 public surface。

### `tqsdk-session`

职责：

- shared session owner
- lazy establish、route/pending-route driving、reconnect/resync control
- one-shot request/response helpers
- low-level market command helpers that remain one-shot command submission
- typed instrument metadata normalization
- GraphQL / HTTP query、schema refresh、metadata、calendar、settlement、ranking、EDB
- auth refresh、replay step/reset 这类 one-shot control-plane helper
- session-level error diagnostics / retry hints
- 天勤特定 auth/http/TqKq 实现的内部落点

设计原因：

- 这些能力是一轮请求/响应或“一次命令 -> 等待完成 -> 返回值”，不要求用户持有持续变化的 live object。
- `wait` 和 `stream` 都需要共享同一个底层 session，因此 session 是它们之前的薄层，而不是某个 facade 的内部实现细节。

禁止回退：

- 不要把 `get_quote`、`get_kline_serial`、live trade refs、`wait_update()`、object stream、task、downloader、research workflow 塞进 session。
- 不要把 wait/stream 共用的消费层配置塞回 session；消费形态配置应留在消费层。

### `tqsdk-wait`

职责：

- Python 风格单 owner `TqApi`
- `wait_update()` 主推进点
- `is_changing()` / field-level changing checks
- diff-backed market/trade live refs
- serial/window 视图
- trade command 的 wait 风格薄包装

设计原因：

- 它承载 Python 用户心智，但不是 Python 单体 `TqApi` 的全量复制。
- 它必须只消费 runtime contract，不拥有第二棵状态树。

禁止回退：

- 不要复制 direct query / schema / metadata API；需要时通过 `api.session()` 使用 `tqsdk-session`。
- 不要加入 downloader、task、DataFrame/polars、callback/stream 语义。

### `tqsdk-stream`

职责：

- shared-session multi-consumer commit fan-out
- fan-out capacity configuration、lag diagnostics、health status
- commit/path/scope/domain/object/field filters
- typed path stream、ready kline/tick window stream
- trade object/session event stream
- managed commit sink foundation for slow consumer isolation, finite retry, local JSONL WAL, and graceful shutdown

设计原因：

- 它面向高并发、多消费者、异步系统集成，价值在于薄薄包装同一套 commit/cursor 语义。
- 对象级 stream 和事件流应建立在 commit-first 内核之上，而不是重新定义 runtime。

禁止回退：

- 不要把 direct query / schema / metadata、task、downloader、DataFrame/polars、私有 object cache 或第二棵状态树塞进 stream。
- 需要读取 trade/market 热路径时，优先使用 partition read surface；只有 generic path stream、system 事件或尚无分区读面的窗口读取才保留 full snapshot。

### `tqsdk-task`

职责：

- `TaskHost`
- `TargetPosTask`
- `TargetPosScheduler`
- ownership / guarded order
- task-level typed order builder
- pre-trade risk gate
- execution group foundation
- account group / multi-account order foundation
- strategy host / strategy context / strategy environment / deployment / supervisor adapter
- strategy supervisor 的 typed health/metrics/shutdown report 和 telemetry/export hook；
  生产观测导出保持 transport-neutral，不内置 GUI、web helper 或 HTTP health/metrics endpoint
- strategy cache replay driver
- public fake market / fake broker test harness
- execution report
- planner/executor 的本地任务状态机

设计原因：

- 任务层维护业务执行状态，既不是协议 substrate，也不是通用消费 facade。
- 它可以依赖 `tqsdk-wait` 的稳定截面语义，但不得反向要求 core 改写提交模型。
- 它可以在 strategy replay driver 中消费 `tqsdk-data` 的 cache/history event。
  这是上层集成路径；不得把 cache storage 下沉进 task，也不得把 strategy
  execution 下沉进 data。

演进方向：

- 后续可继续把内部共享可变状态收敛为更清晰的 single-owner/actor 模型。
- 这类优化应保持 task 层内部收敛，除非有明确的 runtime contract 缺口。

### `tqsdk-data`

职责：

- research/offline data crate
- history page/series/download
- CSV export
- offline market cache record / JSONL reader-writer / ordered replay foundation
- history series -> market cache replay adapter
- Greeks、历史主连等研究派生能力

设计原因：

- 这些能力有批量、离线、tabular、缓存物化、衍生计算语义，不应污染 live session、wait 或 stream 的最小心智。

禁止回退：

- 不要把 downloader、DataFrame/polars 或研究级派生计算下沉到 core/session/wait/stream。

## Runtime 不变量

下面是不允许被局部重构破坏的系统级不变量。

### 单一提交源

所有对外可见状态必须通过 runtime core 提交：

```text
RuntimeCommand / RuntimeInput
    -> ProtocolAdapter
    -> NormalizedMutation
    -> RuntimeHandle
    -> StateStore
    -> CommitResult
    -> RuntimeReader / UpdateCursor
```

设计原因：

- 只有这样，wait、stream、task、data 以及低层用户才能共享同一套因果解释。
- 旁路 future、私有 watcher 或 adapter 直接通知上层都会破坏 revision 和 causality。

### 单一 revision / cursor 语义

- 只有 runtime core 可以推进 `Revision`。
- facade 只能消费 `RuntimeReader::cursor()` / `RuntimeReader::next()` / `RuntimeReader::next_view()`。
- `CommitLog` 是底层共享原语，不是新 facade 的首选 public contract。
- 慢消费者应得到明确 lag/closed/error surface，不得反向改变 commit 生成策略。

### 状态分区与兼容状态树并存

当前设计不是彻底删除全局状态树，而是：

- 对外仍保持一棵兼容的 runtime state tree 语义。
- 内部用 domain partitions 降低跨领域污染和热路径锁竞争。
- market/trade 热读优先走 `RuntimeReader::read_market_state()` / `RuntimeReader::read_trade_state()`。
- generic path、system、query/schema/replay 或尚无 typed partition view 的路径，可以继续通过 `RuntimeReader::read()` 读取 full snapshot。

设计原因：

- 天勤 DIFF 模型天然接近全局 data 字典，兼容状态树有助于覆盖官方稀疏对象和 query/schema/replay。
- domain partitions 是安全和性能防线，先按高风险、高频路径收敛，不必一次性引入完整强类型 state rewrite。

### MutationSource 根路径防线

runtime apply 前必须校验 mutation 来源和根路径：

- market 只允许行情根，例如 `quotes`、`trading_status`、`charts`、`klines`、`ticks`
- trade 只允许 `trade`
- query 只允许 `query`
- schema 只允许 `schema`
- replay 只允许 `replay`
- session control 只允许 `system` / `runtime`

设计原因：

- adapter 解码错误不能跨领域污染资金、持仓或行情状态。
- 这是完整强类型状态分区之前的低成本、高价值安全防线。

### Command / order 状态机

命令状态必须由 runtime 校验合法转换：

- `Queued -> Sent | Rejected | Failed | Cancelled`
- `Sent -> Acked | PartiallyApplied | Completed | Rejected | Failed | Cancelled`
- `Acked -> PartiallyApplied | Completed | Rejected | Failed | Cancelled`
- `PartiallyApplied -> Completed | Rejected | Failed | Cancelled`
- terminal 状态不可回退；相同 terminal 重复写入保持幂等

设计原因：

- 下单和撤单是实盘风险最高路径，不能依赖 adapter 本地顺序假设。
- 乱序消息不能把 `Completed` 回退到 `Sent` 或 `Acked`。

禁止回退：

- 不要用字符串终态判断替代 `CommandStatus` / `OrderLifecycle`。
- 不要绕过 `RuntimeHandle::record_command_status()`。

## 开始工作前的分类流程

每个新 session 开始改代码前，先把任务归类：

1. **局部实现 / bugfix**
   - 不改变 crate 归属、public API、runtime contract。
   - 读取相关 crate README 与局部代码即可。
   - 测试覆盖以受影响 crate 为主。
2. **facade 能力扩展**
   - 新 live object、stream、wait ref、task/data helper。
   - 必须检查本文件的 crate 归属表，确认能力落点。
   - 不得为了便利回改 core/session 边界。
3. **runtime contract 变更**
   - 涉及 `RuntimeHandle`、`RuntimeReader`、commit/revision、state store、mutation、command ledger、adapter contract。
   - 必须更新 `runtime-core/*.md` 与 `validation.md`。
   - 必须添加或更新 contract tests。
4. **架构边界变更**
   - 新 crate、移动模块、改变 direct query/live consumption/task/data 归属、扩大/收窄 public API。
   - 必须更新本文档、`crate-boundaries.md`、`docs/architecture/README.md`、根 README 和受影响 crate README。

如果无法判断类别，按更高风险类别处理。

## 修改约束

- 优先遵循现有 crate 边界，不要因为“更方便”移动职责。
- 不要新增 `tqsdk-protocol`、`tqsdk-tq` 或其他 crate，除非任务本身就是经过文档化的架构更新。
- 不要把审查报告中的长期建议直接落成大重构；先拆成和当前阶段一致的最小安全增量。
- 不要在 facade 中维护第二棵状态树、第二套 revision、第二套 command lifecycle。
- 不要用 public re-export 解决 sibling crate 的内部协作问题；必要的临时桥接应保持最窄可见性并明确标注。
- 不要把 live smoke 作为普通验证默认运行；需要外部账号或实盘权限的测试必须保持 ignored 或显式环境变量门控。

## 架构更新同步规则

架构可以演进，但文档必须同轮更新。以下任一行为都属于架构更新：

- 新增、删除或重命名 crate
- 移动能力归属，例如 direct query 从 session 移到 wait/stream，或 task/data 能力下沉
- 改变 `RuntimeHandle` / `RuntimeReader` / `UpdateCursor` / `CommitResult` 的语义
- 改变状态树、domain partition、mutation guard、command lifecycle 的规则
- 扩大或收窄 core/session/facade 的 public surface
- 引入新的 session ownership、actor、cache、aggregation 或 multi-source 模型
- 改变 feature flags 或依赖裁剪策略，导致用户选择路径变化

同一提交或同一 PR 必须同步更新：

- 本文档
- [`docs/architecture/README.md`](README.md)
- 受影响专题文档
- 受影响 crate README
- 根 [`README.md`](../../README.md)，如果用户可见入口变化
- `AGENTS.md` / `CLAUDE.md`，如果 AI 工作流入口或硬约束变化
- [`validation.md`](validation.md)，如果验收命令、contract tests 或风险面变化

提交说明中应明确：

- 是否属于架构更新
- 更新了哪些架构文档
- 新增或调整了哪些 contract/static acceptance tests

## 推荐验证

局部文档/工作流改动：

```bash
git diff --check
```

Rust 代码改动的默认验证：

```bash
cargo check --workspace --examples
cargo test --workspace
cargo clippy --workspace --examples --all-targets -- -D warnings
```

如果修改了 feature flags、workspace 依赖或 crate feature 传播，还必须验证：

```bash
cargo check --workspace --no-default-features
cargo check --workspace --all-features --examples
```

如果改动会影响格式化，提交前仍应补充：

```bash
cargo fmt --all --check
```

场景驱动 public API example 的处理原则：

1. 已经成为正式 API 契约的 `crates/*/examples/api_contract_sXX_*.rs`
   必须保持可编译。
2. 当前 API 尚不支持、或只能用绕路代码伪装表达的场景，只能作为
   desired API sketch 保存在 `docs/scenarios/api_gaps/`，不得放在正式
   examples 中伪装成已支持。
3. 一旦某个 gap 被修复，应将 sketch 提升为正式 example，并纳入
   `cargo check --workspace --examples` 与 CI。
4. 如果重构导致 example 变长、变绕、暴露更多内部细节，应优先判定为
   API 退化，而不是用户使用问题。

架构边界相关改动还应补充静态验收，例如：

```bash
rg "pub type ContractFuture|tqsdk_core::ContractFuture" crates
rg "TqAuthProvider|PasswordCredentials|TqKqAccountConfig|ReqwestHttpExecutor" crates/tqsdk-core
rg "reqwest|base64" crates/tqsdk-core/Cargo.toml
rg "reader\\.read\\(\\)" crates/tqsdk-wait/src crates/tqsdk-stream/src
```

这些静态检查不是固定全集；每次应按改动风险补充更贴近当前边界的检查。
