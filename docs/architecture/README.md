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
  - 为未来 `wait_update` 与 stream/callback facade 提供共同读面
- `UpdateCursor`
  - 独立推进的 commit 消费游标

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
  - direct query / schema refresh 薄层入口
  - 供 `wait` / 未来 `stream` 共同依赖
- `tqsdk-wait`
  - `TqApi` 单推进点 facade
  - market/trade 对象引用
  - serial window 视图
  - trade 命令的 wait 风格薄包装

这两层当前仍然遵守同一个约束：

- 不反向修改 `tqsdk-core` 的 runtime contract
- 不在 facade 层复制第二棵状态树
- direct query 不重新塞回 `tqsdk-wait`

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
| 总架构、阶段边界、路线图 | [README.md](README.md)、[roadmap.md](roadmap.md) |
| DIFF 协议的纯 merge 语义 | [diff-core.md](diff-core.md) |
| runtime contract：命令、状态、commit、cursor、adapter | [runtime-core/overview.md](runtime-core/overview.md)、[runtime-core/modules.md](runtime-core/modules.md)、[runtime-core/protocol-flow.md](runtime-core/protocol-flow.md)、[runtime-core/data-contracts.md](runtime-core/data-contracts.md)、[runtime-core/type-system.md](runtime-core/type-system.md)、[runtime-core/session-auth.md](runtime-core/session-auth.md) |
| Python / Rust facade 范式对比 | [facade-paradigms.md](facade-paradigms.md) |
| 未来 `wait_update` adapter | [api-wait.md](api-wait.md) |
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
1. [diff-core](diff-core.md)
2. [runtime-core 总览](runtime-core/overview.md)
3. [Session/Auth](runtime-core/session-auth.md)
4. [协议交互](runtime-core/protocol-flow.md)
5. [模块清单](runtime-core/modules.md)
6. [数据契约](runtime-core/data-contracts.md)
7. [类型约束](runtime-core/type-system.md)
8. [Python / Rust facade 范式对比](facade-paradigms.md)
9. [验收与测试矩阵](validation.md)
10. [未来 wait adapter](api-wait.md)
11. [演进路线](roadmap.md)

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
