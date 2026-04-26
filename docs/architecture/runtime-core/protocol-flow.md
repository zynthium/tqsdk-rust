# 协议交互与会话流程

## SessionBootstrap 的 6 个阶段
1. `Authenticate`
   - 调用 `AuthProvider`
   - 获取 `AuthContext`
2. `Connect`
   - 建立 transport 与必要的 HTTP/replay client
   - 启动 session runtime actor
3. `Register Adapters`
   - 装配 `System` / `Market` / `Trade` / `Query` / `Replay` adapter
   - 建立 adapter registry
4. `Bootstrap Remote State`
   - 发送 schema / metadata / bootstrap query
   - 发出必要的初始化命令
   - 接收初始输入并解码为 `NormalizedMutation`
5. `Ready Commit`
   - 状态树达到内部一致
   - 形成首个可见 `CommitResult`
   - `CommitScope = InitialReady`
6. `Running`
   - 进入 steady-state 的 command / input / commit 循环

## Running 阶段的一轮标准推进
1. runtime drain 一轮待执行 `RuntimeCommand`
2. 为每个命令选择 owning adapter
3. adapter 将命令编码成 `OutboundRequest`
4. runtime 发送请求或推进 replay/feed
5. runtime 接收 `RuntimeInput`
6. 将输入广播给 interested adapters
7. adapter 解码为 `NormalizedMutation`
8. `StateStore` 应用 mutation
9. `ProjectionEngine` 归并 path/object/field 命中
10. `CommitAssembler` 判断是否形成可见提交
11. 如形成提交，则推进 `Revision`，发布底层 `CommitLog`
12. 消费者通过 `RuntimeReader` / `UpdateCursor` 观察该提交

## 关键点
- 命令分发、输入解码、状态提交属于同一条 session runtime 链路
- raw input 到达不等于一定形成 commit
- adapter 可以保留短期协议态，但没有 commit 权
- 所有热路径读取都应通过 `RuntimeReader::read()` 获得 zero-copy 视图，而不是依赖快照 clone

## commit 触发规则
1. 原始 `RuntimeInput` 不直接触发 commit
2. 单独命令入队不直接触发 commit
3. 只有 mutation 被应用、projection 完成、并形成新的可见变化时，才推进 revision
4. bootstrap 期间允许内部多次 merge，但只在状态内部一致后产出 `InitialReady`
5. query/schema/trade/replay/system 都必须遵守同一规则

## 命令因果
- 每个命令都有 `CommandId`
- 一个 `CommitResult` 可以由一个或多个 `CommandId` 导致
- 命令状态和命令错误必须进入统一状态树，而不是挂在旁路 future 上

## 重连恢复顺序
1. session 进入 `Reconnecting`
2. transport / client 重建
3. 重新认证或恢复凭证
4. 重新装配 adapter 所需 bootstrap 状态
5. 进入 `Resyncing`
6. 接收恢复期输入并归一化为 mutation
7. 达到内部一致后形成 `ResyncRecovery` commit
8. 从 adapter 保留的协议意图生成 recovery commands，例如行情订阅与 chart
   请求，并重新进入 runtime outbound 队列
9. 回到 `Running`

adapter 可以暴露 recovery commands，但仍然没有提交权：这些命令必须回到
`RuntimeHandle::submit()` / outbound / dispatch 链路，继续使用统一 command ledger
和 route dispatch 规则。

## 与后续 adapter 的关系
- future `wait_update` adapter 只消费 `RuntimeReader` / `UpdateCursor`
- future stream adapter 只消费 `RuntimeReader` / `UpdateCursor`
- future callback adapter 只消费 `RuntimeReader` / `UpdateCursor`
