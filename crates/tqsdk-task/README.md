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
  - 每次 `wait_update()` 调用都会推进 task/scheduler，即使底层 `api.wait_update()` 本轮返回 `false`
  - 提供 guarded `insert_order` / `cancel_order`
- `TargetPosTask`
  - 注册 `account_id + symbol` ownership
  - `set_target_volume()` 与 `wait_target_reached()`
  - `cancel()` 与 `wait_finished()`
    - `cancel()` 只登记取消请求，实际撤单与结束仍由后续 `TaskHost::wait_update()` 推进
  - `execution_report()`
    - 暴露 command-level 事件流，当前包含 insert/cancel/trade/order finished/target reached
    - 同时提供稳定聚合摘要：
      - trades buffer
      - per-order outcome report
      - 已提交委托/撤单/终态订单计数
      - 累计成交手数与成交额
      - 最后一次 target reached 记录
  - `execution_events_since()` / `execution_trades_since()`
    - 提供 cursor-style 增量读取，避免高频轮询时反复 clone 整份 report
  - `last_error()`
    - 若委托/撤单命令本地提交失败，会记录错误并结束任务，不做静默重试
  - `price_mode / offset_priority / split_policy` 配置 surface 已冻结
  - 内部纯规划器已覆盖 `OpenOnly` / `今昨开` / `今昨,开` / `昨开` 的基础 offset 语义
  - 最小真实 planner 已接入全部 offset priority：
    - 基于当前净持仓与目标手数差额按 planner 结果推进
    - 每次 `wait_update()` 最多提交一个 planner batch，batch 内可连续提交多笔委托
    - batch 与 batch 之间仍等待持仓或挂单状态推进
    - `Active/Passive` 价格模式生效
    - `split_policy` 已接入最小确定性拆单
    - 只有当目标持仓匹配且挂单都进入终态后，`wait_target_reached()` 才会完成
    - 同一请求在净持仓未变化前不会重复发单
    - 若挂单进入终态但持仓未变化，会在同一目标请求下重新发单
    - 若当前 live order 与最新期望 batch 不一致，会优先只撤 stale 子集；仍匹配新计划的 live order 会被保留
    - stale live order 进入终态后，会在后续 `wait_update()` 中按最新价格/最新计划补齐或重发
    - SHFE/INE 与非 SHFE 的 `CloseToday` / `Close` 差异已落到执行层测试
    - 当前执行策略仍是保守串行 batch：
      - 每次 `wait_update()` 最多提交一个 planner batch
      - 同一 batch 内可连续提交多笔委托
      - batch 与 batch 之间仍等待持仓或挂单状态推进后再继续
- `TargetPosScheduler`
  - 基于 `TaskHost::wait_update()` 的 step 驱动推进
  - 会为当前 step 驱动内部无 ownership 的 `TargetPosTask`
  - `execution_events()`
    - 聚合内部 task 的最小 command-level 事件流，并带 `step_index`
  - `execution_events_since()` / `execution_trades_since()`
    - 提供 scheduler 级 cursor-style 增量读取
  - 支持 step 级 `price_mode`
  - 支持 pause step
  - 非最后一步按 interval 到期后会先发真实撤单，并在挂单进入终态后再切到下一步
  - 最后一步会等待目标持仓真正达到后再 finished
  - 独立 execution report
    - 聚合 step 内部 task 的 trades buffer 与命令计数摘要
  - `last_error()`
    - 若内部 step task 的命令本地提交失败，错误会向 scheduler 冒泡
  - `cancel()` 同样遵循 `wait_update()` 驱动的撤单后收尾语义
  - 保留 `offset_priority / split_policy` 配置 surface
- 内部 registry
  - 阻止重复 ownership
  - 阻止任务运行期间的手动下单

当前仍未完成：

- 多笔同批次并发提交
- 更复杂的多单/多批次主动撤单后重规划
- 基于交易时段的 deadline 计算
- 更细的 per-step outcome report

设计基线见 [../../docs/architecture/api-task.md](../../docs/architecture/api-task.md)。

## 示例

当前提供一个最小 task example：

- [examples/target_pos.rs](examples/target_pos.rs)
- [examples/target_pos_scheduler.rs](examples/target_pos_scheduler.rs)

运行它需要：

- `TQ_AUTH_USER`
- `TQ_AUTH_PASS`

它默认会使用官方内置的 `TqKq` 主模拟账户做 trade login 和账户 ready 检查，不会下单。

如需切换辅模拟账户，可选设置：

- `TQ_TRADE_ACCOUNT_NO=<1..99>`

如需显式覆盖为其他交易账户，也可以同时设置：

- `TQ_TRADE_BROKER_ID`
- `TQ_TRADE_ACCOUNT_ID`
- `TQ_TRADE_PASSWORD`

只有显式设置下面两个环境变量时，才会真正创建 `TargetPosTask` 并进入调仓循环：

- `TQ_TASK_ALLOW_ORDERS=1`
- `TQ_TARGET_VOLUME=<目标手数>`

可选环境变量：

- `TQ_TASK_SYMBOL`
- `TQ_TASK_TIMEOUT_SECS`

`target_pos_scheduler.rs` 默认不会下单，而是先演示一个 pause-only scheduler step，验证 `TaskHost::wait_update()` 即使在没有新 diff 时也会推进 scheduler。

如果显式设置：

- `TQ_TASK_ALLOW_ORDERS=1`
- `TQ_TARGET_VOLUME=<目标手数>`

它会改为演示“先 pause，再进入一个 target step”的最小 scheduler 路径。
