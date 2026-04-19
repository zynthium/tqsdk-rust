# tqsdk-rs 分层内核架构

## 文档定位
本文档目录描述的是“从头重写一个 Rust 版天勤 TqSdk”的基础架构主线。

这里的第一原则不是先做某种用户 API，而是先做一个足以承载所有远端协议与对象的统一 runtime contract。

重点回答：

- V1 到底交付什么
- 哪些能力必须进入 runtime kernel
- 为什么 `wait_update` 和 `stream/callback` 都不能成为 V1 的核心接口
- 如何在不回改内核的前提下，同时承载 Python 风格和 Rust 风格的后续 facade

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
4. `consumption-adapters`
   - `wait_update`
   - stream
   - callback
   - 都只是消费 commit log / cursor 的后续适配层
5. `user facades`
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
8. [验收与测试矩阵](validation.md)
9. [未来 wait adapter](api-wait.md)
10. [演进路线](roadmap.md)

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
consumption-adapters
    ^
    |
user facades / tools
```

## 当前总判断
- 真正的可复用底层不是原始 WebSocket 客户端，也不是某一种用户 API
- 真正的可复用底层是：`统一命令模型 + 统一状态树 + 统一 commit/revision/change 模型 + 统一 cursor/log`
- `wait_update` 和 `stream/callback` 的差异只能体现在“怎么消费 commit”，不能体现在“怎么生成 commit”
- V1 的完成标准是 contract 完整，不是 facade 完整
