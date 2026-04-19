# 验收标准与测试矩阵

## 文档定位
本文档定义 `tqsdk-rs` 的语义验收标准与测试矩阵，重点服务于：

- `V1 Runtime Kernel`
- `V2 tqsdk-api-wait`

相关文档：

- [总架构入口](README.md)
- [runtime-core 总览](runtime-core/overview.md)
- [协议交互](runtime-core/protocol-flow.md)
- [V2 wait 专题](api-wait.md)

## 本页覆盖范围
本页作为跨版本约束，主要负责：

- 约束 `V1 Runtime Kernel` 的提交、可见性与重连语义
- 约束 `V2 tqsdk-api-wait` 的 `wait_update()` / `snapshot()` / `is_changing()`
- 为后续 `V3/V4` 的 stream/callback 适配层提供同一套验收基线

## 验收原则
`tqsdk-rs` 的验收不应只看“有没有返回值”或“测试是否全绿”，而应同时满足：

1. 一致性成立
   - 单次成功 `wait_update()` 后，所有读取都对应同一轮 revision
2. 可解释性成立
   - `is_changing()` 的结果能被 `ChangeSet` 和当轮提交解释
3. 边界条件成立
   - 初始化、超时、重连等特殊路径不会破坏前两条

## 核心语义验收条目
### `wait_update()`
- 当且仅当存在新的已提交 revision 可供当前调用方消费时，`wait_update()` 返回 `true`
- 仅收到网络消息、心跳或无效补丁时，不应因为底层活动而返回 `true`
- `wait_update(timeout)` 返回 `false` 时，不应推进调用方可见 revision
- 若同一轮 revision 已被当前调用方消费，再次调用时必须等待下一轮新 revision

### `snapshot()`
- 单次成功 `wait_update()` 返回后，同一调用方读取的 quote、position、account、order 等视图应属于同一逻辑 revision
- 在没有新的成功 `wait_update()` 之前，多次调用同一 view 的 `snapshot()` 应保持逻辑稳定
- `snapshot()` 不得暴露半提交状态
- 对尚未初始化完成的对象，`snapshot()` 可以返回 `None` 或空结构，但语义必须一致且可预期

### `is_changing()`
- `is_changing(target, None)` 判断对象级命中
- `is_changing(target, Some(field))` 判断字段级命中
- `is_changing()` 的结果必须来自最近一次成功 `wait_update()` 对应 revision 的 `ChangeSet`
- 若本轮未命中该对象或字段，则即使对象当前值非空，也必须返回 `false`
- 若 `wait_update()` 因超时返回 `false`，则 `is_changing()` 不应提前暴露后台已发生但尚未成功交付给调用方的变化

### 初始化
- 初始化完成后，用户应能读取首份可用状态
- 首份可用状态不应默认被视为一轮业务更新命中
- “对象已可读”与“对象在最近一轮发生变化”必须分开表达
- 首次初始化后的下一轮真实更新，才应正常驱动 `wait_update(true)` 和 `is_changing()`

### 超时
- 超时只表示“本次没有成功拿到新的可见 revision”，不表示系统内部完全静止
- 超时后，调用方可见 revision 不得前进
- 超时后，后续 `snapshot()` 仍应反映上一次成功可见 revision
- 超时后，下一次真正成功的 `wait_update()` 仍应能一次性交付新的 revision 及对应变化

### 重连
- 断线期间不应对用户伪造连续更新
- 重连恢复后，应先形成新的完整或足够一致的状态提交，再允许 `wait_update()` 返回 `true`
- 重连后的首个可见 revision 应被视为新的状态切换边界
- `is_changing()` 对重连后首轮 revision 的判断，应基于重同步后的新 `ChangeSet`

## Phase 1 测试矩阵
Phase 1 只实现 quote-only 兼容闭环，因此测试矩阵也应刻意收敛，确保最关键语义先被证明。

| 场景 | 输入条件 | 预期行为 | 对应核心语义 |
| :--- | :--- | :--- | :--- |
| 无输入超时 | 无 `RuntimeInput`，等待超时 | `wait_update()` 返回 `false` | 超时不推进可见 revision |
| 单次 quote 提交 | 一个有效 `QuotePatch` | `wait_update()` 返回 `true`，quote 可读 | 提交后唤醒 |
| 无效补丁 | 输入补丁不改变任何值 | `wait_update()` 返回 `false` 或继续等待 | 无有效变化不产生新 revision |
| 字段命中 | 仅修改 `last_price` | `is_changing(&quote, Some("last_price")) == true` | 字段级变更命中 |
| 非命中字段 | 仅修改 `last_price` | `is_changing(&quote, Some("datetime")) == false` | 字段级精确性 |
| 对象命中 | quote 任一字段变化 | `is_changing(&quote, None) == true` | 对象级变更命中 |
| 快照稳定 | 一次成功更新后，多次读取 quote | `snapshot()` 结果稳定 | 同 revision 稳定性 |
| 超时后保持旧值 | 一次成功更新后，再次超时 | `snapshot()` 仍是上一轮可见状态 | 超时不暴露新状态 |
| 多轮顺序更新 | 连续两次有效 quote 提交 | 两次 `wait_update()` 分别交付不同 revision | revision 单调前进 |

最小闭环：
`QuotePatch -> StateStore::commit -> Revision/ChangeSet -> wait_update(true) -> quote.snapshot()/is_changing()`

## 后续扩展测试矩阵
| 后续阶段 | 新增测试重点 | 关键风险 |
| :--- | :--- | :--- |
| K 线阶段 | 新 bar、补丁更新、多合约对齐、序列 `is_changing()` | K 线不是简单字段更新 |
| 交易阶段 | 下单、撤单、订单状态流转、持仓与账户联动 | 交易状态需要进入同一提交模型 |
| 重连阶段 | 重同步、断线恢复、首轮新 revision 语义 | 旧状态与新状态边界不清 |
| 回测阶段 | 虚拟时钟、重放步进、撮合结果交付 | 回测 runtime 不能破坏 wait_update 语义 |
| 多对象一致性阶段 | quote/position/account/order 同轮读取 | 跨对象 revision 不一致 |

建议顺序：

1. 先证明 quote-only 语义闭环
2. 再进入 K 线
3. 然后接交易对象
4. 最后接回测与重连专项

## 测试策略总表
| 测试层级 | 目标 |
| :--- | :--- |
| 单元测试 | 验证状态合并、revision 推进、diff metadata 生成规则 |
| 集成测试 | 启动模拟服务端，验证 `wait_update()` 的唤醒时机与快照稳定性 |
| 语义一致性测试 | 对照 Python TqSdk，验证 `is_changing()`、初始化截面、超时与重连行为 |
| K 线专项测试 | 覆盖新 bar、补丁、多包合并、多合约对齐 |
| 压力测试 | 评估高频更新下的提交延迟、多策略并发读取吞吐量和内存占用 |
