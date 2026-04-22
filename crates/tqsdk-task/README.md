# `tqsdk-task`

`tqsdk-task` 是建立在 `tqsdk-wait` 之上的执行工具层。

它的目标不是提供新的协议层能力，而是承接：

- `TargetPosTask`
- scheduler
- task registry
- symbol ownership
- 手动下单冲突保护

当前已落地的最小能力：

- `TaskHost`
  - 托管单一 `wait_update()` 推进点
  - 提供 guarded `insert_order` / `cancel_order`
- `TargetPosTask`
  - 注册 `account_id + symbol` ownership
  - `set_target_volume()` 与 `wait_target_reached()`
  - `cancel()` 与 `wait_finished()`
  - 保留 `price_mode / offset_priority / split_policy` 配置壳
- `TargetPosScheduler`
  - 基于 `TaskHost::wait_update()` 的 step 驱动推进
  - 独立 execution report
  - 取消与 ownership 释放
  - 保留 `offset_priority / split_policy` 配置壳
- 内部 registry
  - 阻止重复 ownership
  - 阻止任务运行期间的手动下单

当前仍未完成：

- 实际调仓规划与拆单算法
- 基于交易时段的 deadline 计算
- quote hint 与配置驱动的真实执行语义

设计基线见 [../../docs/architecture/api-task.md](../../docs/architecture/api-task.md)。
