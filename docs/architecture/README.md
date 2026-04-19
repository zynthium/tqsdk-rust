# tqsdk-rs 分层内核架构

## 文档定位
本文档目录是 `tqsdk-rs` 的总架构路线图，重点回答：

- 为什么要拆成 `tqsdk-diff-core`、`tqsdk-runtime-core`、`tqsdk-api-*`
- 为什么 `wait_update` 和 callback/stream 不应该成为底层核心
- 为什么 V1 应先交付 `Runtime Kernel`，再逐层叠加更高层 API

## 文档分工
本目录按“总架构 / runtime kernel / wait 专题 / 验收矩阵”组织，避免不同层次的设计混在同一页里。

| 主题 | 当前落点 |
| :--- | :--- |
| 高层评审稿、三层拆法、为什么底层核心不是 `wait_update` | [README.md](README.md)、[diff-core.md](diff-core.md)、[roadmap.md](roadmap.md) |
| 用户端 API、`TqApi`、`QuoteView`、`QuoteSnapshot`、`is_changing()` | [api-wait.md](api-wait.md) |
| 内部语义类型、`Revision` / `ChangeSet` / `CommitResult`、时序与状态机 | [runtime-core/overview.md](runtime-core/overview.md)、[runtime-core/data-contracts.md](runtime-core/data-contracts.md)、[runtime-core/protocol-flow.md](runtime-core/protocol-flow.md)、[runtime-core/type-system.md](runtime-core/type-system.md) |
| 语义验收标准与测试矩阵 | [validation.md](validation.md) |

## 三层拆法
1. `tqsdk-diff-core`
   - 负责 DIFF 协议解析、递归合并、路径定位、变更检测
   - 不关心 WebSocket、不关心用户 API
2. `tqsdk-runtime-core`
   - 负责连接驱动、认证、会话生命周期、commit 边界、revision、`ChangeSet`、通知
   - 是最底层可运行内核
3. `tqsdk-api-*`
   - `tqsdk-api-wait`
   - `tqsdk-api-stream`
   - `tqsdk-api-callback`
   - 都只是在消费同一个 runtime core

## 阅读顺序
1. [diff-core](diff-core.md)
2. [runtime-core 总览](runtime-core/overview.md)
3. [Session/Auth](runtime-core/session-auth.md)
4. [协议交互](runtime-core/protocol-flow.md)
5. [模块清单](runtime-core/modules.md)
6. [数据契约](runtime-core/data-contracts.md)
7. [类型约束](runtime-core/type-system.md)
8. [API 扩展层](api-layers.md)
9. [V2 wait 专题](api-wait.md)
10. [验收与测试矩阵](validation.md)
11. [演进路线](roadmap.md)

## 依赖方向
```text
tqsdk-diff-core
    ^
    |
tqsdk-runtime-core
    ^
    |
tqsdk-api-wait
tqsdk-api-stream
tqsdk-api-callback
```

## 当前总判断
- 真正的可复用底层不是原始 WebSocket 客户端，也不是某一种用户 API
- 真正的可复用底层是：`diff + runtime + commit boundary`
- `wait_update` 适合作为策略主范式，stream/callback 适合作为扩展消费范式
