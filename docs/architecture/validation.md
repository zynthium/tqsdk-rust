# 验收标准与测试矩阵

## 文档定位
本文档定义的是 runtime contract 的验收标准，以及未来 facade/adapters 的派生验收基线。

重点服务于：

- V1 protocol-complete runtime contract
- V2+ wait / stream / callback adapters

相关文档：

- [总架构入口](README.md)
- [runtime-core 总览](runtime-core/overview.md)
- [协议交互](runtime-core/protocol-flow.md)
- [未来 wait adapter](api-wait.md)

## 本页覆盖范围
本页主要负责：

- 约束 V1 的 command-to-commit 完整性
- 约束统一状态树、统一 revision、统一 cursor/log 语义
- 为未来 `wait_update`、stream、callback adapter 提供同一套底层验收基线

## 验收原则
V1 的验收不应看 facade 好不好用，而应看 contract 是否完整。

必须同时满足：

1. 统一性成立
   - 所有远端交互都进入同一 runtime contract
2. 可见性成立
   - 所有上层可见结果都进入同一 `StateSnapshot`
3. 可解释性成立
   - 所有可见变化都能被 `Revision` / `ChangeSet` / causality 解释
4. 隔离性成立
   - adapter 不绕过 commit 模型直接通知上层

## V1 核心验收条目
### 统一命令链路
- 所有远端交互都必须经过：
  `RuntimeCommand -> RuntimeInput / NormalizedMutation -> CommitResult`
- `submit()` 只返回 `CommandId`，不返回完成态
- command-scoped 结果不得通过旁路 future 暴露

### 统一状态树
- market / trade / replay / query / schema / system 状态都必须进入同一 `StateSnapshot`
- 任意已提交 revision 都必须能提供内部一致的 snapshot
- query/schema 结果不得躲在独立 side cache 中绕开 snapshot

### 统一 revision / change 模型
- 只有形成可见 commit 时才推进 `Revision`
- `ChangeSet` 必须支持 path/object/field 三级命中
- 不同协议域的变化不能各自维护独立 revision

### 统一 causality
- 每个命令都必须可追踪到 `CommandId`
- `CommitResult` 必须能表达由哪些命令导致
- trade/replay/query/system 错误都必须进入同一 causality 模型

### 统一 cursor / log 语义
- 所有消费者都必须通过 `CommitLog` / `UpdateCursor` 读取提交结果
- runtime core 不得为不同 future facade 维护不同的提交通道
- 多个 cursor 必须能独立推进，不互相污染

### adapter 边界
- adapter 可以编解码和保留短期协议态
- adapter 不得直接推进 revision
- adapter 不得直接发通知给上层
- adapter 不得直接改 cursor

## V1 测试矩阵
| 场景 | 输入条件 | 预期行为 | 对应核心语义 |
| :--- | :--- | :--- | :--- |
| bootstrap schema commit | 初始 schema / metadata 拉取完成 | 产生 `InitialReady` commit，状态写入 snapshot | schema 进入统一状态树 |
| market diff commit | 一个有效 market diff | 形成新 revision，market 状态可见 | DIFF 对象进入统一提交 |
| trade command reject | 下单命令被远端拒绝 | 不走旁路 future；错误进入 snapshot 与 commit | trade 因果统一 |
| replay step commit | 一次 replay step 产生多对象变化 | 形成单轮或可解释多轮 commit，归属对应 `CommandId` | replay 因果统一 |
| query response commit | GraphQL / HTTP 查询返回结果 | 结果写入 `query/*`，形成可见 commit | query 结果进入 snapshot |
| session error commit | auth 失效或 transport 异常 | session 错误进入 `system/*` 并形成 commit | system 错误统一可见 |
| cursor isolation | 两个 cursor 从不同 revision 开始消费 | 各自独立推进 | cursor 独立性 |
| multi-adapter observation | 一个输入被多个 adapter 观察 | 只通过 mutation/commit 对外可见 | adapter 无提交权 |

## V2+ adapter 验收基线
### wait adapter
- 能只靠 `CommitLog` / `UpdateCursor` / `StateSnapshot` 实现 `wait_update()`
- 能只靠 `ChangeSet` 实现 `is_changing()`

### stream adapter
- 能只靠 cursor/log 形成连续 commit stream
- backpressure 策略不回灌 runtime core

### callback adapter
- 能只靠 cursor/log 实现回调 fan-out
- callback 慢消费者不改变 commit 生成逻辑

## 测试策略总表
| 测试层级 | 目标 |
| :--- | :--- |
| 单元测试 | 验证命令归一化、mutation 生成、state apply、change 归并 |
| 集成测试 | 验证 command-to-commit 全链路与 snapshot 一致性 |
| contract 测试 | 验证不同协议域共享同一 revision / causality / cursor 模型 |
| 重连专项 | 验证 session error、重连与 resync 仍走统一提交模型 |
| adapter 验证 | 验证 wait / stream / callback 只消费 contract，不回改 contract |
