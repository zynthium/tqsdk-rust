# `tqsdk-task`

`tqsdk-task` 是执行工具层。`TaskHost`、`TargetPosTask`、scheduler 和 strategy
host 以 `tqsdk-wait` 为 canonical substrate；S31 低延迟 trading desk profile
则是薄的 `tqsdk-session + RuntimeReader` hot-path profile。

它的目标不是提供新的协议层能力，而是承接：

- `TargetPosTask`
- scheduler
- task registry
- symbol ownership
- 手动下单冲突保护
- typed task-level order builder
- pre-trade risk gate
- execution group foundation
- account group / multi-account order foundation
- strategy host / strategy context
- strategy environment adapter
- strategy cache replay driver
- low-latency trading desk profile
- public fake market / fake broker test harness

当前已落地的最小能力：

- `TaskHost`
  - 托管单一 `wait_update()` 推进点
  - 每次 `wait_update()` 调用都会推进 task/scheduler，即使底层 `api.wait_update()` 本轮返回 `false`
  - 提供 guarded `insert_order` / `cancel_order`
  - 提供 `orders(account_id)` typed 下单入口，返回 wait 层 `OrderTicket`
  - 可配置 `RiskEngine`，让 typed order builder 和 guarded insert 在提交前统一经过 risk gate
- `RiskEngine`
  - 当前覆盖单笔最大手数、交易日内开仓次数、交易日内单合约开仓手数、合约组累计开仓手数、订单操作频率、最低可用资金、最大净持仓、quote 价格偏离、tick size 校验和 contract multiplier notional projection
  - 开仓限额和订单频率是 task host 本进程内用量计数；不承诺跨进程持久审计或服务端风控替代
  - 拒绝结果通过 typed `RiskRejection` 暴露
  - 读取现有 account / position / quote refs，不维护第二份资金或持仓状态
- `TradingDeskProfile`
  - 面向 S31 自研低延迟柜台主循环，使用 shared `SessionClient + RuntimeReader`
    消费行情 commit 并提交 trade command
  - builder 支持 `subscribe_quotes(...)`、`risk_engine(...)` 和
    `latency_probe(...)`
  - `read_market_trade_state()` 返回同 revision 的 market + trade 分区读 guard，
    让 risk check、position/quote 读取和下单决策共享一致截面
  - `precheck_order(...)` 复用 `TaskOrderIntent` 与 `RiskEngine`
    `check_report_on_state` / `project_order_on_state`
  - `submit_prechecked_order(...)` 使用 session-scoped order intent ledger 做
    client order id 去重；重复 client id 返回 existing ticket，不重复提交订单
  - `TradingDeskOrderTicket::status(&desk)` 返回 typed
    `TradingDeskOrderStatusReport` / `TradingDeskOrderState`，不要求用户解析字符串
  - `TradingLatencyProbe` / `TradingLatencyCycle` / `TradingLatencyReport` 提供 typed
    本进程 cycle marker；marker 不完整时 `report()` 返回 `None`
  - 慢日志、WAL 和 journal 使用 `tqsdk-stream` sidecar managed sink 组合，sink
    不进入 profile public API
- `ExecutionGroup`
  - 通过 typed group id 管理两腿订单 intent
  - 所有腿在提交前统一经过 ownership guard、risk gate 和本地参数校验
  - 每条腿复用 wait 层 `OrderTicket` 和 session-scoped client intent ledger
  - 暴露 group outcome / exposure report；当前只报告裸露风险，不自动 hedge
- `AccountGroup`
  - 通过 typed account group 和 `Ratio` 表达多账户比例拆单
  - 多账户订单在提交前统一经过 ownership guard、risk gate 和本地参数校验
  - 每个账户订单复用 wait 层 `OrderTicket` 和 session-scoped client intent ledger
  - 暴露 per-account outcome report；当前只报告需要人工处理的账户差异，不自动补单或跨账户 hedge
- `StrategyHost`
  - 包装 `TaskHost`，保持单 owner / 单推进点策略心智
  - `StrategyContext` 在同一稳定推进点内读取 quote / account / position
  - 同一 context 复用 typed order builder、target-pos builder 和 risk gate
- `StrategyEnvironment`
  - 提供 live/sim task host 与 replay 的最小统一构建入口
  - `StrategyEnvironmentContext` 让同一策略步骤函数复用 quote/position/orders/target-pos/risk context 方法
  - replay metadata 通过可选 `replay_event()` / `replay_time_ns()` 暴露，不要求 live/sim 策略分叉
- `StrategyDeploymentConfig` / `StrategyDeployment` / `StrategyLifecycle`
  - 提供 provider-backed TQKQ sim 和 live trade 的 typed deployment config
  - provider-backed sim 的账号派生与登录由 builder 处理，不向策略泄漏内部协议
  - 统一 fake/replay/live deployment wrapper、run loop、typed stop reason 和 graceful shutdown report
- `StrategySupervisor` / `StrategyRetryPolicy` / `StrategyShutdownSignal`
  - 在 deployment 之上提供 task-layer supervisor foundation
  - 暴露 typed health/metrics snapshot、transport-neutral telemetry/export hook、显式有限 retry、ctrl-c shutdown hook 和 typed shutdown report
  - retry 默认不隐藏启用，避免策略步骤有下单副作用时被 SDK 静默重复执行
- `StrategyReplay`
  - 使用 `tqsdk-data::MarketCacheReplay` 作为离线 market event source
  - 将 cache quote/kline/tick 转成正常 runtime market commit
  - 可接收由 `KlineDataSeries` / `TickDataSeries` adapter 生成的 history replay
  - 暴露 deterministic replay time、`StrategyReplayCheckpoint` 和 `resume_from(...)`
  - 暴露 `StrategyReplaySpeed`，支持最快、real-time 和 scaled replay pacing
  - 暴露 `StrategyReplayCheckpointStore`，支持 JSON file-backed checkpoint persistence
  - 暴露 `StrategyReplaySourceBuilder`，支持多个 history/cache event series 合并
  - 让 replay strategy 复用 `StrategyContext`、typed order builder 和 fake broker
- `tqsdk-task::testing`
  - 提供 public `StrategyTestHarness` / `FakeMarket` / `FakeBroker` / `StrategyTestClock`
  - 测试策略时不需要真实网络、hidden `*_for_test` API、runtime handle、channel 或 `Arc<Mutex<_>>`
  - 当前支持 fake quote/account/position seed、全成、拒单、单步/跨 step 部分成交、deterministic fake broker clock、step latency 和 broker disconnect/reconnect 注入
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
  - 若本地还没有该 symbol 的可定价 quote，会自动发起一次 `subscribe_quote`
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
    - 若已有 live order 与最新计划方向/offset/价格兼容但手数不足，会保留已有订单并只补齐缺口
    - stale live order 进入终态后，会在后续 `wait_update()` 中按最新价格/最新计划补齐或重发
    - 已提交但尚未出现在本地状态树的 tracked order 会被视为 pending，不会被当作空挂单提前达成目标或重复发单
    - 重复设置相同目标不会重置等待中的提交
    - SHFE/INE 与非 SHFE 的 `CloseToday` / `Close` 差异已落到执行层测试
    - 当前执行策略仍是保守串行 batch：
      - 每次 `wait_update()` 最多提交一个 planner batch
      - 同一 batch 内可连续提交多笔委托
      - batch 与 batch 之间仍等待持仓或挂单状态推进后再继续
- `TargetPosScheduler`
  - 基于 `TaskHost::wait_update()` 的 step 驱动推进
  - 会为当前 step 驱动内部无 ownership 的 `TargetPosTask`
    - 因此也继承内部 task 的按需 quote 自动订阅语义
  - `execution_events()`
    - 聚合内部 task 的最小 command-level 事件流，并带 `step_index`
  - `execution_events_since()` / `execution_trades_since()`
    - 提供 scheduler 级 cursor-style 增量读取
  - 支持 step 级 `price_mode`
  - 支持 pause step
  - 非最后一步会按“交易时段内累计 elapsed”判断 interval 是否到期
    - 当前实现基于 `quote.trading_time` + `TradingDayCalendar`
    - 可通过 `TaskHost::refresh_trading_calendar()` 显式预取官方交易日历，也可通过 `TaskHost::set_trading_calendar()` 注入本地 calendar
    - calendar 缺失某天时会回退到 weekday 规则，避免查询失败导致 scheduler 卡死
    - 若拿不到有效 trading session，则保守回退到现有 wall-clock 行为
  - 到期后会先发真实撤单，并在挂单进入终态后再切到下一步
  - 最后一步会等待目标持仓真正达到后再 finished
  - 独立 execution report
    - 聚合 step 内部 task 的 trades buffer 与命令计数摘要
    - 提供稳定的 per-step outcome report：
      - `step_index` / `target_volume`
      - 每步的 submitted/cancel/finished 计数
      - 每步的成交手数/成交额/成交笔数
      - 每步是否已 target reached
  - `last_error()`
    - 若内部 step task 的命令本地提交失败，错误会向 scheduler 冒泡
  - `cancel()` 同样遵循 `wait_update()` 驱动的撤单后收尾语义
  - 保留 `offset_priority / split_policy` 配置 surface
- 内部 registry
  - 阻止重复 ownership
  - 阻止任务运行期间的手动下单

当前仍保持的边界：

- 执行策略仍是保守串行 batch，不追求在同一轮内激进并发所有后续 batch
- 已覆盖多笔 live order 中只撤 stale 子集、保留兼容订单，并在 stale 终态后继续补齐缺口的重规划路径
- 已覆盖未物化 tracked order 在 retarget / 重复同目标调用下的保守处理，避免提前 target reached 或重复发单
- `RiskEngine` 仍是最小 pre-trade gate，组合级保证金 what-if、涨跌停/品种级规则和多腿 / 多账户联合限额仍是后续工作
- `ExecutionGroup` 仍是 foundation，自动 hedge / flatten、timed cancel / replace、group resume / audit 仍是后续工作
- `AccountGroup` 仍是 foundation，自动补单 / 跨账户 TargetPos 编排、resume / audit 仍是后续工作
- `StrategySupervisor` 仍是 foundation，完整 reconnect orchestration、跨进程 daemon 管理、多 provider environment 和 durable sink isolation 仍是后续工作；Rust SDK 不规划 GUI、web helper 或内置 HTTP health/metrics endpoint
- `TradingDeskProfile` 仍是低延迟柜台薄 profile，不做 OMS、自动 hedge /
  flatten、补单引擎、GUI 或 HTTP endpoint；hot path 不进入 `tqsdk-data` 或历史
  mmap cache
- `StrategyTestHarness` 仍是 foundation，更完整 broker 行为和持久化测试 fixture 恢复仍是后续工作

设计基线见 [../../docs/architecture/api-task.md](../../docs/architecture/api-task.md)。

## 多账户订单 foundation

```rust
use std::time::Duration;

use tqsdk_task::{AccountFailurePolicy, MultiAccountOrderOutcome, Ratio, TaskHost};

# async fn run(mut host: TaskHost) -> tqsdk_task::Result<()> {
let accounts = host
    .account_group()
    .add("sim-a", Ratio::new(7, 10)?)
    .add("sim-b", Ratio::new(3, 10)?)
    .min_volume_per_account(1)
    .build()?;

let ticket = host
    .multi_account_order(accounts)
    .client_group_id("alloc-au-001")
    .max_unhedged(Duration::from_secs(2))
    .on_account_failed(AccountFailurePolicy::ReportExposure)
    .buy_open("SHFE.au2602", 10)
    .limit(480.0)
    .send_once()
    .await?;

match ticket.outcome(host.api())? {
    Some(MultiAccountOrderOutcome::AllFilled { accounts }) => {
        println!("all accounts filled: {accounts:?}");
    }
    Some(MultiAccountOrderOutcome::NeedsAttention {
        filled_accounts,
        unfilled_accounts,
        ..
    }) => {
        println!("filled={filled_accounts:?}, unfilled={unfilled_accounts:?}");
    }
    _ => {}
}
# Ok(())
# }
```

## 示例

当前提供一个最小 task example：

- [examples/target_pos.rs](examples/target_pos.rs)
- [examples/target_pos_scheduler.rs](examples/target_pos_scheduler.rs)
- [examples/api_contract_s11_simple_strategy.rs](examples/api_contract_s11_simple_strategy.rs)
- [examples/api_contract_s12_spread_arbitrage.rs](examples/api_contract_s12_spread_arbitrage.rs)
- [examples/api_contract_s13_multi_account_ordering.rs](examples/api_contract_s13_multi_account_ordering.rs)
- [examples/api_contract_s15_live_sim_replay_switch.rs](examples/api_contract_s15_live_sim_replay_switch.rs)
- [examples/api_contract_s19_pre_trade_risk.rs](examples/api_contract_s19_pre_trade_risk.rs)
- [examples/api_contract_s20_strategy_supervisor.rs](examples/api_contract_s20_strategy_supervisor.rs)
- [examples/api_contract_s24_testable_strategy.rs](examples/api_contract_s24_testable_strategy.rs)
- [examples/api_contract_s29_target_pos_ownership.rs](examples/api_contract_s29_target_pos_ownership.rs)
- [examples/api_contract_s31_low_latency_trading_desk.rs](examples/api_contract_s31_low_latency_trading_desk.rs)

`api_contract_s24_testable_strategy.rs` 使用 public fake harness，不需要真实账号或网络。

`api_contract_s29_target_pos_ownership.rs` 单独覆盖 `TargetPosTask` /
`TargetPosScheduler` ownership 契约。它默认 dry-run，只创建 task host 并验证同账户同合约
owner 冲突、手动下单 guard 和 scheduler 通过 `TaskHost::wait_update()` 推进；只有显式设置
`TQ_TASK_ALLOW_ORDERS=1` 与 `TQ_TARGET_VOLUME=<目标手数>` 时才会登录 TQKQ 并进入真实调仓 loop。

`api_contract_s31_low_latency_trading_desk.rs` 单独覆盖低延迟柜台 thin profile：
session 自驱动 quote hot path、同 revision market/trade 分区读、risk precheck、
typed order ticket/status、typed latency cycle，以及 `tqsdk-stream` sidecar managed
sink / WAL / journal 的慢消费者隔离。它默认不会发单；只有显式设置
`TQ_DESK_ALLOW_ORDER=1` 才会尝试提交示例订单。

`target_pos.rs`、`target_pos_scheduler.rs` 和 live API contract examples 运行时需要：

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
