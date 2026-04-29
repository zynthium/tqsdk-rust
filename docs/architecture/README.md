# tqsdk-rs 分层内核架构

## 文档定位
本文档目录描述的是“从头重写一个 Rust 版天勤 TqSdk”的基础架构主线。

这里的第一原则不是先做某种用户 API，而是先做一个足以承载所有远端协议与对象的统一 runtime contract。

重点回答：

- V1 到底交付什么
- 哪些能力必须进入 runtime kernel
- 为什么 `RuntimeReader` 而不是 `wait_update` / `stream-callback` 才是 V1 的主读契约
- 如何在不回改内核的前提下，同时承载 Python 风格和 Rust 风格的后续 facade
- `tqsdk-python` 与现有 `tqsdk-rs` 两种 facade 范式该如何取长补短

## V1 的总定位
V1 不是：

- `wait_update()` SDK
- `stream/callback` SDK
- `TqApi` SDK

V1 是：

- protocol-complete runtime contract
- 统一所有远端交互的提交模型
- 后续一切 facade 的公共底座

它必须覆盖：

- DIFF 协议对象
- trade 命令与状态
- replay/feed 推进
- auth/session/system 控制
- GraphQL / HTTP query
- schema / metadata / bootstrap 交互

它明确不提供：

- `TqApi`
- `wait_update()` facade
- stream facade
- callback facade
- 各类高层 view
- `TargetPosTask`
- DataFrame / polars / downloader / GUI / report

## 当前实现状态
当前仓库里的 V1 已经以“极简但协议完整”的 core contract 落地完成。

当前 public core 的稳定主线是：

- `RuntimeHandle`
  - 写入、命令提交、session/runtime 控制入口
- `RuntimeReader`
  - canonical read-side 入口
  - 提供 cursor 创建、commit 消费、zero-copy 状态读取
- `SnapshotReadGuard` / `StateReadView`
  - revision-bound 的借用读视图
  - 为 `wait_update`、stream/callback facade 提供共同读面
- `UpdateCursor`
  - 独立推进的 commit 消费游标

不属于稳定 public core 主线：

- raw outbox envelope（例如 `OutboundEnvelope`）是 runtime 内部队列细节；低层 route 消费者应使用 `OutboundDispatch`
- multi-source aggregation helper 不是 V1 public contract；需要时应先重新设计场景和文档

仍保留的兼容/底层原语：

- `StateSnapshot`
  - 需要 detached owned snapshot 时可直接使用
- `CommitLog`
  - 底层 commit buffer，可用于兼容层或测试

当前 public core 可以直接覆盖并验证：

- DIFF 协议对象
- trade 命令与状态
- replay/feed 推进
- auth/session/system 控制
- GraphQL / HTTP query
- schema / metadata / bootstrap 交互

验证入口见 [validation.md](validation.md) 与 `crates/tqsdk-core/tests/runtime_contract_v1_capability.rs`。

在 core 之上的第二层分拆也已经开始落地：

- `tqsdk-session`
  - shared session shell
  - lazy establish + route / pending-route 驱动原语
  - `progress_once()` 这个最小 substrate 推进原语
  - `subscribe_quotes()` / `unsubscribe_quotes()` 这类低层命令 helper
  - `wait_command_completed()` 这个最小 control-plane 等待原语
  - direct query / schema refresh 薄层入口
  - direct query surface 再细分为 `SessionRawQuery` / `SessionMetadataQuery` / `SessionServiceQuery`
  - `InstrumentSpec` / `InstrumentClass` 这类一次性 metadata 标准化对象
  - session-level error diagnostic / retry hint wrapper
  - session-scoped order intent ledger，供上层 facade 在同一 session 内对稳定
    client order id 做去重和命令关联
  - 保持“纯 async substrate，调用方自带 Tokio runtime”的约束
  - 供 `wait` / `stream` 共同依赖
- `tqsdk-wait`
  - `TqApi` 单推进点 facade
  - market/trade 对象引用
  - serial window 视图
  - 基于 shared session 的 live `wait_update()` 驱动链路
  - trade 命令的 wait 风格薄包装
  - 允许通过 `session()` 落回同一个底层 `SessionClient`，但不复制 direct query API
- `tqsdk-stream`
  - shared-session multi-consumer commit stream facade
  - root fan-out capacity 配置与 typed lag diagnostics
  - commit/path/scope/domain/object/field filters
  - typed path stream / ready kline-tick window / trade session events
  - managed commit sink foundation for slow consumer isolation, finite retry, local JSONL WAL, and graceful shutdown
  - health status / restart hint
  - `stream.session()` 仍然是一次性 direct query 的逃生舱，但不改变 direct query 的 crate 归属
- `tqsdk-task`
  - `TaskHost`
  - `TargetPosTask`
  - `TargetPosScheduler`
  - typed order builder / pre-trade risk gate
  - execution group foundation
  - account group foundation
  - strategy host / strategy context / strategy environment / deployment / supervisor adapter
  - supervisor typed health/metrics/shutdown report 和 telemetry/export hook；生产观测导出保持
    transport-neutral，不内置 GUI、web helper 或 HTTP health/metrics endpoint
  - strategy cache replay foundation
  - public fake market / fake broker test harness
  - ownership / guarded order / execution report（事件流 + 聚合摘要）
- `tqsdk-data`
  - research/offline data crate
  - `DataClient`
  - `query_his_cont_quotes`
  - `HistoricalContQuotesRow`
  - history page/series/download and CSV export substrate
  - offline market cache record / JSONL reader-writer / ordered replay foundation
  - history series -> market cache replay adapter

这两层当前仍然遵守同一个约束：

- 不反向修改 `tqsdk-core` 的 runtime contract
- 不在 facade 层复制第二棵状态树
- direct query 不重新塞回 `tqsdk-wait`
- `tqsdk-task` 可以消费 `tqsdk-data` cache/history event 构建 strategy replay
  driver；这是上层集成路径，不代表 cache storage 进入 task，也不代表 strategy
  execution 进入 data

## API 归属总表

为了避免后续实现时再次把“一次性 direct query”误塞进 `wait` 或 `stream`，当前架构采用下面这条硬边界：

- `tqsdk-session` 负责所有一次性 request/response 接口
- `tqsdk-wait` 和 `tqsdk-stream` 只负责 diff-backed 持续状态消费接口

| 接口类别 | 应归属的 crate | 原因 |
| :--- | :--- | :--- |
| GraphQL / HTTP query | `tqsdk-session` | 一次 `await` 请求/响应，不依赖 `wait_update()` 或 stream |
| schema refresh / fetch | `tqsdk-session` | 一次性拉取/刷新，不是持续变化对象 |
| 合约元数据查询 / `InstrumentSpec` 标准化 | `tqsdk-session` | 属于 direct query / metadata，不需要模式化消费 |
| 交易日历 | `tqsdk-session` | 一次性结果，不应绑定某种 diff 消费形状 |
| `SymbolSettlement` / `SymbolRanking` / 其他 metadata query | `tqsdk-session` | 都是 query 结果，不是 live object |
| session 内订单 intent ledger | `tqsdk-session` | 是 shared session substrate，帮助 wait/stream/task 复用同一 client order id 去重语义，但不拥有 live order object |
| 低层行情命令 helper | `tqsdk-session` | 是一次性 runtime command submission，不拥有 live quote object 或消费循环 |
| stream fan-out capacity / lag diagnostics / health status | `tqsdk-stream` | 属于 continuous consumption 的 consumer/channel 状态，不应下沉到 core/session |
| `get_quote` / `get_trading_status` | `tqsdk-wait` / `tqsdk-stream` | 返回持续变化对象，依赖 commit 持续推进 |
| `get_kline_serial` / `get_tick_serial` | `tqsdk-wait` / `tqsdk-stream` | 返回持续更新窗口，依赖后续 diff |
| `get_account` / `get_position` / `get_order` / `get_trade` | `tqsdk-wait` / `tqsdk-stream` | 读取的是同一棵状态树中的 live 对象 |
| `insert_order` / `cancel_order` / `confirm_settlement` | `tqsdk-wait` / `tqsdk-stream` | 属于 trade diff-backed 消费语义的一部分 |

对用户形态的含义也应明确：

- `tqsdk-session` 不是只给 facade 内部用，用户也可以直接使用它来做 direct query / schema / metadata 访问
- 对性能极致敏感、希望自己掌控 cursor/commit 驱动的用户，也可以直接使用 `tqsdk-session::SessionClient + progress_once() + RuntimeReader`
- `tqsdk-wait` 即便提供 `session()` 访问底层 session，也只是复用路径，不改变 direct query 的 crate 归属
- `tqsdk-stream` 现在也不是 direct query 的归属地，而是给高并发、多消费者、事件流场景提供一层现成但仍然很薄的 diff facade
- 对性能极致敏感的用户，仍然可以直接使用 `tqsdk-core + tqsdk-session`

在 `tqsdk-session` 这一层里，建议再按“薄包装 vs 高层研究工具”继续收一刀：

- 应当进入 `tqsdk-session` 的 thin wrapper：
  - `query_symbol_info`
  - `query_quotes`
  - `query_cont_quotes`
  - `query_options`
  - `query_atm_options`
  - `query_all_level_options`
  - `query_all_level_finance_options`
  - `get_trading_calendar`
  - `query_symbol_settlement`
  - `query_symbol_ranking`
  - `query_edb_data`
- 不应进入 `tqsdk-session` 的高层派生接口：
  - `query_his_cont_quotes`
  - `query_option_greeks`
  - DataFrame / polars 形状兼容层

原因也很简单：

- 前一组仍然只是“远端请求 -> 一次性结果”的薄包装
- 后一组已经开始包含研究工作流、衍生计算或 tabular/view 语义

## 参考仓库的使用方式
- `tqsdk-python` 是语义基准
  - 尤其是提交边界、对象一致性、初始截面、命令可见性、回放推进这些语义
- 现有 `tqsdk-rs` 适合参考工程经验
  - actor 化 I/O
  - market/trade/replay 分层
  - runtime 复用思路
- 但新的 V1 不应直接继承现有 `tqsdk-rs` 的宽 public surface

## 文档分工
本目录按“总架构 / diff core / runtime contract / future adapters / 验收矩阵”组织。

| 主题 | 当前落点 |
| :--- | :--- |
| AI 工作流与架构守则 | [ai-workflow.md](ai-workflow.md) |
| 总架构、阶段边界、路线图 | [README.md](README.md)、[roadmap.md](roadmap.md) |
| 当前 workspace crate 边界审计 | [crate-boundaries.md](crate-boundaries.md) |
| 未来 crate 蓝图与能力映射 | [crate-blueprint.md](crate-blueprint.md) |
| DIFF 协议的纯 merge 语义 | [diff-core.md](diff-core.md) |
| runtime contract：命令、状态、commit、cursor、adapter | [runtime-core/overview.md](runtime-core/overview.md)、[runtime-core/modules.md](runtime-core/modules.md)、[runtime-core/protocol-flow.md](runtime-core/protocol-flow.md)、[runtime-core/data-contracts.md](runtime-core/data-contracts.md)、[runtime-core/type-system.md](runtime-core/type-system.md)、[runtime-core/session-auth.md](runtime-core/session-auth.md) |
| Python / Rust facade 范式对比 | [facade-paradigms.md](facade-paradigms.md) |
| `wait_update` facade | [api-wait.md](api-wait.md) |
| stream facade | [api-stream.md](api-stream.md) |
| task facade / execution tool | [api-task.md](api-task.md) |
| data facade / research tooling | [api-data.md](api-data.md) |
| 未来 facade / adapter 的验收基线 | [validation.md](validation.md) |

## 建议的概念分层
1. `diff-core`
   - 只负责天勤 DIFF 协议的理解、递归合并与 mutation 归一化
   - 不关心 session、不关心 facade
2. `runtime-contract`
   - 负责统一所有协议域的命令、状态、提交、revision、cursor
   - 是 V1 唯一 canonical public contract
3. `protocol-adapters`
   - 将 market diff、trade、query/schema、replay、system 接入同一个 runtime
   - 只负责编解码与 mutation 归一化
   - 没有提交权
4. `shared session layer`
   - 负责会话生命周期、query/schema/direct-query 封装，以及后续 facade 共享的 session 入口
   - 是 `wait` / `stream` facade 之前的薄层
5. `consumption facades`
   - `wait_update`
   - stream
   - callback
   - 都只是消费 `RuntimeReader` / `UpdateCursor` 的后续适配层
6. `user facades`
   - `TqApi`
   - typed views
   - task/tooling

## 阅读顺序
1. [AI 工作流与架构守则](ai-workflow.md)
2. [diff-core](diff-core.md)
3. [runtime-core 总览](runtime-core/overview.md)
4. [Session/Auth](runtime-core/session-auth.md)
5. [协议交互](runtime-core/protocol-flow.md)
6. [模块清单](runtime-core/modules.md)
7. [数据契约](runtime-core/data-contracts.md)
8. [类型约束](runtime-core/type-system.md)
9. [Python / Rust facade 范式对比](facade-paradigms.md)
10. [当前 crate 边界审计](crate-boundaries.md)
11. [未来 crate 蓝图与能力映射](crate-blueprint.md)
12. [验收与测试矩阵](validation.md)
13. [wait facade](api-wait.md)
14. [stream facade](api-stream.md)
15. [task facade](api-task.md)
16. [data facade](api-data.md)
17. [演进路线](roadmap.md)

## 依赖方向
```text
diff-core
    ^
    |
runtime-contract
    ^
    |
protocol-adapters
    ^
    |
shared session layer
    ^
    |
consumption facades
    ^
    |
user facades / tools
```

## 当前总判断
- 真正的可复用底层不是原始 WebSocket 客户端，也不是某一种用户 API
- 真正的可复用底层是：`统一命令模型 + 统一状态树 + 统一 commit/revision/change 模型 + reader-first 读契约`
- `tqsdk-session` 会先承接 shared session、direct query、schema / metadata 这类薄层职责
- `wait_update` 和 `stream/callback` 的差异只能体现在“怎么消费 commit / 怎么读取同一棵状态树”，不能体现在“怎么生成 commit”
- V1 的完成标准是 contract 完整，不是 facade 完整
