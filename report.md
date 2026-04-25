# tqsdk-rust 代码级架构审查报告（当前代码版）

## 0. 审查基线

- **核对日期**：2026-04-26
- **审查对象**：当前 workspace 代码与架构文档，而不是旧报告中的历史状态
- **主要依据**：`README.md`、`docs/architecture/ai-workflow.md`、`docs/architecture/README.md`、`docs/architecture/crate-boundaries.md`、各 crate `Cargo.toml`、`tqsdk-core` public surface、目标代码定向搜索结果
- **验证状态**：已完成各 crate 回归测试与 `docs/architecture/validation.md` 的 feature/no-default 构建矩阵；未运行 `cargo clippy`，也未做全仓逐文件审计

`report.md` 应被视为审查输入与路线图建议，不是架构权威来源。若本报告与 `docs/architecture` 或当前代码冲突，应以架构文档和代码为准。

---

## 1. 执行摘要

### 整体成熟度：中期，核心架构已明显收敛

当前 `tqsdk-rust` 已不是旧报告描述的“早中期骨架”。workspace 已经形成并落地了清晰的分层体系：

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

核心 runtime contract 已包含：

- 统一命令模型与命令状态转换校验
- domain partitions 与兼容全局状态树并存
- `MutationSource` 根路径防线
- `RuntimeReader::read_market_state()` / `read_trade_state()` 热路径读面
- `OrderLifecycle` 与 command/order 状态机
- AFIT/RPITIT 风格 async trait 边界，`ContractFuture` public alias 已移除
- DIFF 入站/出站协议模型层基础
- `DomainEvent` / `MarketEvent` / `TradeEvent` 领域事件基础
- `AggregatedRuntimeReader` / `StateSourceId` 多源聚合基础
- session/wait/stream/task/data feature flags 与可选 HTTP/auth 依赖，feature/no-default 验证矩阵已固化并通过

### 当前是否需要局部破坏性重构

**不建议按旧报告执行大规模破坏性重构。**

旧报告中的多个最高优先级问题已经在当前代码中落地修复或被架构文档收口。继续按旧优先级推进会产生反向破坏，例如：

- 不应再以“彻底删除全局状态树”为目标；当前设计是 domain partitions 与兼容状态树并存。
- 不应再把 `Transport` / `AuthProvider` AFIT 化列为待办；当前已经完成。
- 不应再说“无显式订单状态机”；当前已有 command 状态转换校验与 `OrderLifecycle`。
- 不应再把 `TqAuthProvider` / `ReqwestHttpExecutor` 这类实现细节列为 core root public API；它们已从 core root surface 收走。

当前更合理的策略是：**保持架构边界，继续做局部代码健康和协议模型收敛。**

### 当前最优先处理的优化点

| 序号 | 问题 | 级别 | 涉及 crate | 说明 |
| --- | --- | --- | --- | --- |
| 1 | typed partition 读面已覆盖 market/trade，query/schema/replay 仍主要依赖兼容树 | P2 | core | 不是缺陷，但后续可按热点继续扩展 typed view |
| 2 | `session/client.rs` 可继续按职责拆分，但不紧急 | P2 | session | 当前规模可控，仅在继续增长或出现修改冲突时拆分 |
| 3 | wait/stream builder 重复属于可接受薄封装 | P2 | wait/stream | 当前重复较薄，不建议为去重引入复杂泛型体系 |

### 是否存在 P0 实盘风险

**本次定向核对未发现仍应标为 P0 的架构风险。**

旧报告中的两项 P0 已被当前代码/架构设计吸收：

- 状态污染风险：已有 `MutationSource` 根路径防线与 market/trade domain partitions。
- 订单状态回退风险：已有 `record_command_status()` 转换校验与 `OrderLifecycle`。

剩余风险主要是维护性与边界清晰度问题，不是“立即可能导致实盘资金/持仓错误”的 P0。

---

## 2. 当前仓库拓扑摘要

### Crate 列表与职责

| Crate | 角色 | 当前判断 |
| --- | --- | --- |
| `tqsdk-core` | 低层 async protocol substrate：命令、状态、commit/revision、cursor、adapter、transport contract、schema types | 边界健康，需继续克制 public surface |
| `tqsdk-session` | shared session + one-shot request/response/direct-query 层 | 边界健康，是 wait/stream/data/task 共享底座 |
| `tqsdk-wait` | Python 风格 single-owner `wait_update()` facade | 边界健康，不应复制 direct query |
| `tqsdk-stream` | Rust async-native multi-consumer stream facade | 边界健康，已优先使用 trade/market partition 读面 |
| `tqsdk-task` | 执行工具层：`TaskHost`、`TargetPosTask`、scheduler、report | 当前最大代码健康优化点 |
| `tqsdk-data` | research/offline data：history page/series/download、CSV、Greeks | 边界健康，不应下沉回 session/core |

### 依赖方向

```text
tqsdk-core
    ↑
tqsdk-session
    ↑
tqsdk-wait / tqsdk-stream / tqsdk-data
    ↑
tqsdk-task
```

当前依赖方向与架构文档一致：无反向依赖、无单体 `TqApi` 回退迹象。

### Feature flags / optional dependencies

旧报告中“当前无 feature flags，所有依赖均为硬依赖”的结论已经过时。

当前实际情况：

- `tqsdk-core` 没有 feature flags，且不依赖 `reqwest` / `base64`。
- `tqsdk-session` 有 `default = ["live", "services"]`，并将 `reqwest`、`base64` 设为 optional。
- `tqsdk-wait`、`tqsdk-stream`、`tqsdk-task` 通过 `tqsdk-session` 转发 `live` / `services`。
- `tqsdk-data` 将 `reqwest` 设为 optional，并通过 `services` 启用。

当前剩余问题不是“没有 feature flags”，而是：

- feature/no-default 构建矩阵已写入 `docs/architecture/validation.md`，并已完成最终验证。
- README 与架构文档应明确哪些能力需要 `live`、`services`、`tq-auth`。
- examples 的 `required-features` 已存在，但需要和 CI/验证矩阵保持同步。

当前验证基线应与 `docs/architecture/validation.md` 的 `Feature / no-default build matrix` 保持一致：

1. `cargo build -p tqsdk-core`
2. `cargo build -p tqsdk-session --no-default-features`
3. `cargo build -p tqsdk-session --no-default-features --features live`
4. `cargo build -p tqsdk-session --no-default-features --features services`
5. `cargo build -p tqsdk-wait --no-default-features`
6. `cargo build -p tqsdk-stream --no-default-features`
7. `cargo build -p tqsdk-task --no-default-features`
8. `cargo build -p tqsdk-data --no-default-features`
9. `cargo test -p tqsdk-core`
10. `cargo test -p tqsdk-session --no-default-features`

### 核心状态与提交路径

当前仍保持单一提交源：

```text
RuntimeCommand / RuntimeInput
    -> ProtocolAdapter
    -> NormalizedMutation
    -> RuntimeHandle
    -> StateStore
    -> CommitResult
    -> RuntimeReader / UpdateCursor
```

重要变化是：这条路径不再只是“全局 `serde_json::Value` 树”。当前设计是：

- 兼容状态树继续存在，用于 DIFF 稀疏对象、generic path、system/query/schema/replay 等路径。
- market/trade 热路径有 domain partitions。
- `MutationSource` 在 runtime apply 前校验根路径，防止 adapter 解码错误跨领域污染。
- facade 热读优先走 `read_market_state()` / `read_trade_state()`。

---

## 3. 旧报告关键结论的当前状态

| 旧结论 | 当前状态 | 处理建议 |
| --- | --- | --- |
| P0：状态树为无类型全局树，无领域隔离 | **已过时**。当前已有 domain partitions、根路径校验、market/trade typed read surface | 改写为“继续扩展 typed partition 覆盖面” |
| P0：无显式订单状态机 | **已过时**。当前已有 `CommandStatus` 转换校验与 `OrderLifecycle` | 从 P0 移入“已解决历史问题” |
| P1：`Transport` / `AuthProvider` 使用 `Box::pin` / `ContractFuture` | **已过时**。当前架构禁止恢复 `ContractFuture` public alias，代码已采用 async trait 边界 | 从待办移除 |
| P1：core root re-export 过宽，暴露 TQ auth/http 实现细节 | **部分已解决**。root surface 已不再 re-export `TqAuthProvider` / `ReqwestHttpExecutor` 等 | 剩余关注 `tqsdk_core::internal` 逃生舱 |
| P1：adapter 直接硬编码出站协议 JSON | **部分已解决**。已有 `diff_protocol.rs` / `DiffProtocolMessage` | 剩余关注入站解析和 `adapter/common.rs` 收敛 |
| P2：stream/wait builder 重复 | **降级**。当前 builder 已是 `SessionClientBuilder` 的薄封装，并提供 `from_session_builder()` | 保持低优先级 |
| P2：`session/client.rs` 过大 | **降级**。当前约 675 行，不再是旧报告的 1600+ 行级别 | 仅在继续增长时拆分 |
| 多源聚合不支持 | **已过时**。当前已有 `AggregatedRuntimeReader` / `StateSourceId` 基础 | 改为评估完整性，而不是“从零新增” |
| 领域事件层缺失 | **已过时**。当前已有 `DomainEvent` / `MarketEvent` / `TradeEvent` | 改为补齐覆盖面 |

---

## 4. 当前架构优点

### 4.1 分层边界清晰

当前 crate 分层与架构文档一致：core 只做 substrate，session 做 shared session/direct-query，wait/stream 做 continuous consumption，task/data 分别承载执行与研究能力。

这条边界应继续作为后续改动的硬约束。

### 4.2 Runtime contract 已有安全防线

当前 runtime 不再只是无主见地写入一棵 `Value` 树。它已经具备：

- mutation source 与根路径对应关系校验
- command status 合法转换校验
- terminal status 幂等，不允许回退
- market/trade typed partition read surface
- 单一 revision/cursor/commit 语义

这几项直接解决了旧报告里最严重的两类实盘风险。

### 4.3 core 已从“实现细节公开”向 contract layer 收敛

`tqsdk-core` 当前不再把 `TqAuthProvider`、`PasswordCredentials`、`BrokerInfo`、`TqKqAccountConfig`、`ReqwestHttpExecutor` 作为 root public API 暴露。

`tqsdk-session` 承接了 live/auth/http 这些具体能力，并通过 optional feature 控制重型依赖。这符合 `docs/architecture/ai-workflow.md` 的边界要求。

### 4.4 feature flags 已有基础

`tqsdk-session`、`tqsdk-wait`、`tqsdk-stream`、`tqsdk-task`、`tqsdk-data` 都已有 default/live/services 等 feature 设计。

这使得“纯 core substrate”与“带 live/http/auth 能力的用户 facade”可以逐步分离，不再是旧报告描述的全量硬依赖状态。

### 4.5 wait/stream 已优先复用 partition 读面

当前 wait/stream 的 market/trade ref 和 event stream 已出现对 `read_market_state()` / `read_trade_state()` 的使用。这是正确方向：facade 不维护第二棵状态树，只消费 runtime contract。

---

## 5. 当前问题清单

### 已完成：`tqsdk-task` 内部拆分

- **涉及位置**：`crates/tqsdk-task/src/target_pos/`、`crates/tqsdk-task/src/scheduler/`
- **当前结论**：task 内部职责已拆分，执行状态、规划、调度、报告等职责不再集中在单一大文件中。
- **边界判断**：task 能力仍保留在 `tqsdk-task`，未下沉到 core/session/wait/stream，符合架构边界。
- **验证状态**：`cargo test -p tqsdk-task` 已通过。

### 已完成：DIFF 协议模型层继续收敛

- **涉及位置**：`crates/tqsdk-core/src/diff_protocol/`、`crates/tqsdk-core/src/adapter/`
- **当前结论**：DIFF 出站与入站协议模型均已进一步集中，adapter 更偏向 typed protocol event 到 `NormalizedMutation` / runtime input 的映射。
- **边界判断**：保留 `NormalizedMutation`、`MutationSource` 与 commit-first runtime contract，未破坏统一提交模型。
- **验证状态**：`cargo test -p tqsdk-core` 已通过。

### 已完成：`tqsdk_core::internal` bridge 继续收窄

- **涉及位置**：`crates/tqsdk-core/src/lib.rs`
- **当前结论**：core root public surface 继续保持克制，internal bridge exports 已进一步修剪，仍只作为 sibling crates 的低层组装桥。
- **边界判断**：不应把 internal bridge 当作用户稳定 API，也不应重新扩大 core root re-export。
- **验证状态**：`cargo build -p tqsdk-core` 与 `cargo test -p tqsdk-core` 已通过。

### 已完成：feature flags 验证矩阵已固化并通过

- **级别**：P2
- **涉及位置**：各 crate `Cargo.toml`、`docs/architecture/validation.md`、crate README
- **当前代码事实**：session/wait/stream/task/data 已有 `live` / `services` 等 feature，`reqwest` / `base64` 也已 optional 化。`docs/architecture/validation.md` 已列出 feature/no-default 验证矩阵，本轮最终验证已通过。
- **后续建议**：
  - 保持本报告与架构验证文档使用同一组命令，避免 CI、README 或审查路线图出现互相冲突的矩阵。
  - 对 live examples 保留 `required-features`，并在 README 中说明。
- **是否破坏 API**：否
- **是否建议立即整改**：否，后续重点是持续纳入 CI/发布前验证

### P2-2：typed partition 覆盖面可继续扩大

- **级别**：P2
- **涉及位置**：`tqsdk-core/src/state/`、wait/stream refs
- **当前代码事实**：market/trade 已有专用读面。query/schema/replay/system 仍主要通过兼容状态树与 generic path 读取。
- **为什么不是 P0**：当前架构明确允许兼容状态树与 partitions 并存。query/schema/replay 不是最高频热路径，不应为了“纯强类型”一次性重写。
- **建议**：
  - 只对热点或高风险路径新增 typed view。
  - 保留 generic path 作为官方稀疏对象和兼容层。
  - 任何新增状态写入仍必须通过 `MutationSource` 根路径校验。
- **是否破坏 API**：视新增读面而定，通常否
- **是否建议立即整改**：否，按需求推进

### P2-3：`session/client.rs` 可继续按职责拆分，但不紧急

- **级别**：P2
- **涉及位置**：`crates/tqsdk-session/src/client.rs`
- **当前代码事实**：当前文件约 675 行，已经不是旧报告中的 1600+ 行级别。它仍同时承载 session 建立、推进、命令等待、direct query 入口等职责。
- **建议**：
  - 只有在继续增长或出现修改冲突时再拆。
  - 合理拆法是 `session_io.rs`、`session_commands.rs`、`session_services.rs`。
  - 不要把 wait/stream 消费形态配置塞回 session。
- **是否破坏 API**：否
- **是否建议立即整改**：否

### P2-4：stream/wait builder 重复属于可接受薄封装

- **级别**：P2
- **涉及位置**：`crates/tqsdk-stream/src/builder.rs`、`crates/tqsdk-wait/src/builder.rs`
- **当前代码事实**：两个 builder 都是 `SessionClientBuilder` 的薄包装，并提供 `from_session_builder()`。
- **建议**：
  - 暂不为去重引入复杂泛型 builder。
  - 若未来 builder 方法继续增加，优先让用户显式构造 `SessionClientBuilder` 再传入 facade builder。
- **是否破坏 API**：否
- **是否建议立即整改**：否

---

## 6. 当前架构反推

### 当前系统实际分层

```text
用户代码
  |
  |-- TqApi (wait) / TqStream (stream) / DataClient (data) / TaskHost (task)
  |      |
  |      +-- SessionClient
  |            |
  |            +-- RuntimeHandle
  |            +-- RuntimeReader
  |            +-- UpdateCursor
  |            +-- SessionRuntime / route driving internals
  |            +-- direct query / schema / metadata / service helpers
  |
  +-- low-level users
         |
         +-- tqsdk-core + tqsdk-session
```

### 状态所有权是否清晰

**清晰度较旧报告显著提升。**

当前权威状态仍在 runtime core 中，facade 只消费 `RuntimeReader` / `UpdateCursor`。domain partitions 降低 market/trade 热读的锁竞争与跨领域污染风险，兼容状态树保留 DIFF 全局 data 字典语义。

后续需要继续坚持：

- 不新增 facade 私有 revision。
- 不新增第二棵 facade 状态树。
- 不绕过 `RuntimeHandle -> StateStore -> CommitResult`。

### 协议状态、业务状态、策略状态是否混在一起

当前状态是：

- protocol/runtime/session/query/schema/replay 状态仍可存在于兼容状态树。
- market/trade 高风险、高频路径已有分区读面。
- task 本地策略状态留在 `tqsdk-task`，没有下沉到 core。

这符合当前架构文档，不应被视为“必须彻底强类型化”的缺陷。

### 各模块实际归属

| 关注点 | 当前归属 | 判断 |
| --- | --- | --- |
| runtime command/state/commit/cursor | `tqsdk-core` | 正确 |
| transport contract | `tqsdk-core` | 正确 |
| 天勤 auth/http 实现 | `tqsdk-session` feature-gated 能力 | 正确 |
| direct query / schema / metadata | `tqsdk-session` | 正确 |
| live object / wait_update | `tqsdk-wait` | 正确 |
| stream fan-out / typed stream | `tqsdk-stream` | 正确 |
| target position / scheduler / reports | `tqsdk-task` | 正确，内部职责已拆分 |
| history/offline/research | `tqsdk-data` | 正确 |

---

## 7. 推荐目标架构

当前不建议立刻新增 `tqsdk-protocol` / `tqsdk-transport` / `tqsdk-tq` 等 crate。旧报告的拆 crate 建议在当时合理，但当前代码已经先通过 module、feature 和 public surface 收敛解决了主要问题。

更合理的目标是：

```text
tqsdk-core
  - runtime contract
  - state partitions + compatible tree
  - command/order lifecycle
  - protocol adapter trait
  - DIFF protocol model module
  - transport contract

tqsdk-session
  - shared session owner
  - live/auth/http optional implementation
  - one-shot request/response helpers
  - schema/metadata/calendar/ranking/EDB

tqsdk-wait / tqsdk-stream
  - continuous consumption facades
  - no direct query duplication
  - no private state tree

tqsdk-task
  - execution tooling only
  - target-pos and scheduler internal state machines
  - report aggregation

tqsdk-data
  - offline/research data workflows
```

只有在出现明确外部需求时，才考虑继续拆 crate：

- 多协议供应商需要独立协议包：再拆 `tqsdk-protocol`。
- 用户确实需要脱离 session/runtime 直接裸连 transport：再拆 `tqsdk-transport`。
- 天勤实现需要独立发布或替换：再拆 `tqsdk-tq`。

在没有这些压力之前，过早拆 crate 会增加版本、feature 和发布复杂度。

---

## 8. 推荐整改路线图

### 阶段 1：报告和验证基线同步（已完成）

| 步骤 | 内容 | 破坏 API | 验收标准 |
| --- | --- | --- | --- |
| 1.1 | 将旧 P0/P1 从路线图中移除或标记已解决 | 否 | 文档不再误导后续 AI/session |
| 1.2 | 在 `validation.md` 增加 feature/no-default 构建矩阵 | 否 | 验证命令覆盖最小构建和默认构建，本轮已通过 |
| 1.3 | 继续修剪并约束 `tqsdk_core::internal` bridge | 否 | core root surface 保持克制，internal 不扩展为用户稳定 API |

### 阶段 2：`tqsdk-task` 内部拆分（已完成）

| 步骤 | 内容 | 破坏 API | 验收标准 |
| --- | --- | --- | --- |
| 2.1 | 拆 `target_pos.rs` 为 state/planner/executor/report | 否 | public API 不变，文件职责清晰 |
| 2.2 | 拆 `scheduler.rs` 为 state/planner/runner | 否 | 调度状态和执行推进分离 |
| 2.3 | 保持 task 内部状态机边界测试 | 否 | `cargo test -p tqsdk-task` 已通过 |

### 阶段 3：协议模型继续收敛（已推进）

| 步骤 | 内容 | 破坏 API | 验收标准 |
| --- | --- | --- | --- |
| 3.1 | 将入站 `aid` 解析集中到 `diff_protocol.rs` | 否 | 入站/出站协议模型更集中 |
| 3.2 | 将 adapter common 职责收敛为 typed protocol event 到 runtime input/mutation 的映射 | 否 | adapter 更聚焦，测试更聚焦 |
| 3.3 | 保持 `NormalizedMutation` 和 `MutationSource` 作为 runtime contract | 否 | 不破坏 commit-first 内核 |

### 阶段 4：渐进式 public surface 收窄

| 步骤 | 内容 | 破坏 API | 验收标准 |
| --- | --- | --- | --- |
| 4.1 | 审计 `tqsdk_core::internal` 的上层真实使用 | 可能 | 确认哪些类型必须继续跨 crate 暴露 |
| 4.2 | 能移入 session 的实现细节逐步移入 session | 可能 | core 更接近 contract-only |
| 4.3 | 对无法收窄的 internal 类型增加明确不稳定说明 | 否 | semver 风险可控 |

---

## 9. 不建议执行的旧方案

以下旧方案不建议继续按原样执行：

- **彻底替换全局状态树**：当前架构明确要求兼容状态树与 partitions 并存。
- **把 adapter 直接产出强类型领域状态作为唯一数据面**：这会破坏 `NormalizedMutation` / commit-first runtime contract。
- **立即拆出 `tqsdk-protocol`、`tqsdk-transport`、`tqsdk-tq` 三个新 crate**：当前主要问题已通过 module/feature/public surface 收敛缓解，拆 crate 应等真实需求驱动。
- **把 task/data 能力下沉到 core/session/wait/stream**：这违反架构边界。
- **为去重重写 wait/stream builder 泛型体系**：当前重复是可接受薄封装，收益不高。
- **恢复 `ContractFuture` 或扩大 core root re-export**：这与当前架构守则冲突。

---

## 10. 最终结论

当前代码已经解决了旧报告中最严重的架构风险。新的主线不应是“破坏性重构 core”，而应是：

1. 保持 `tqsdk-core` 的 protocol-complete runtime substrate 边界。
2. 保持 `tqsdk-session` 对 one-shot request/response/direct-query 的归属。
3. 保持 wait/stream 只做 diff-backed continuous consumption。
4. 保持执行工具复杂度收敛在 `tqsdk-task` 内部。
5. 保持 DIFF 入站/出站协议细节集中到协议模型层。
6. 用已通过的验证矩阵持续固化 feature/no-default 构建能力。
7. 渐进式收窄 `tqsdk_core::internal`，不要一次性破坏上层 crate。

**当前建议**：不要再按旧报告启动破坏性 core 重构。后续应把已通过的验证矩阵纳入常规回归，并按需求渐进扩展 typed partition 覆盖面。
