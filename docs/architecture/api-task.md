# `tqsdk-task` 最小 API 草图

## 文档定位

本文档描述当前 `tqsdk-task` crate 的最小稳定边界，以及这层执行工具在实现后冻结下来的设计判断。

它回答的是：

- `TargetPosTask`、scheduler、ownership 这些能力应该落在哪里
- 它和 `tqsdk-core` / `tqsdk-session` / `tqsdk-wait` / `tqsdk-stream` 的依赖关系应是什么
- 第一版应该先冻结哪些 public surface，哪些暂时不做

它不回答：

- 具体调仓算法的每一个细节
- DataFrame / 下载器 / 回测报告能力
- callback / GUI 形态

相关文档：

- [总架构入口](README.md)
- [crate 边界审计](crate-boundaries.md)
- [crate 蓝图](crate-blueprint.md)
- [wait facade](api-wait.md)
- [stream facade](api-stream.md)
- [路线图](../ROADMAP.md)

## 先给结论

`tqsdk-task` 应该是一个独立的高层执行工具 crate，而不是继续向 `wait` / `stream` 塞功能。

第一版的最小结论是：

- `tqsdk-core` 不承接任务语义
- `tqsdk-session` 不承接任务语义
- `tqsdk-stream` 不承接任务 ownership / scheduler
- `tqsdk-task` 先以 `tqsdk-wait` 为 canonical substrate
- `tqsdk-task` 第一版只做：
  - `TargetPosTask`
  - `TargetPosScheduler`
  - task registry / symbol ownership
  - 手动下单冲突保护
  - task-level typed order builder
  - 最小 pre-trade risk gate
  - execution group foundation
  - account group / multi-account order foundation
  - strategy host / strategy context
  - strategy cache replay driver
  - public fake market / fake broker test harness
  - 事件流 + 稳定聚合摘要的 execution report

原因很直接：

- `TargetPosTask` 的核心不是“多消费者 stream”，而是“单个稳定推进点上的规划与执行”
- Python 官方语义也是绑定在 `wait_update()` 心智上的
- 这类能力一旦放进 `wait` 或 `stream`，会立刻把 facade 从“消费层”污染成“执行层”

当前仓库里的落地状态：

- `TaskHost`
- `TargetPosTask`
- guarded `insert_order` / `cancel_order`
- typed `TaskHost::orders(...)` order builder
- `RiskEngine` / `RiskRejection` 最小前置风控
- `ExecutionGroup` / `ExecutionGroupOutcome` 两腿执行组 foundation
- `AccountGroup` / `MultiAccountOrderTicket` 多账户执行 foundation
- `StrategyHost` / `StrategyContext`
  - 复用 `TaskHost::wait_update()` 作为单推进点
  - 在同一稳定 context 内读取 quote/account/position
  - 通过同一个 context 进入 typed order builder、target-pos 和 risk gate
- `StrategyReplay`
  - 消费 `tqsdk-data::MarketCacheReplay` 的有序 quote/kline/tick cache event
  - 将 cache event 推进为正常 runtime market commit
  - 暴露 deterministic replay time、checkpoint 和 resume-from foundation
  - 暴露 `StrategyReplaySpeed`，支持最快、real-time 和 scaled replay pacing
  - 暴露 `StrategyReplayCheckpointStore`，支持 JSON file-backed checkpoint persistence
  - 暴露 `StrategyReplaySourceBuilder`，支持多个 history/cache event series 合并
  - 让 replay strategy 复用 `StrategyContext`、typed order builder 和 fake broker
  - 这是 task/data 的上层集成路径，不把 cache storage 搬入 task，也不把
    strategy execution 搬入 data
- `tqsdk-task::testing`
  - public `StrategyTestHarness` / `FakeMarket` / `FakeBroker` / `StrategyTestClock`
  - 允许用户不用真实网络、不调用 hidden `*_for_test` API 测试策略
  - 当前覆盖 quote/account/position seed、全成、拒单、单步部分成交、deterministic fake broker clock 和 step latency
- `TaskHost::wait_update()` 现在把“用户显式调用了一次推进点”和“底层本轮是否收到新 diff”区分开：
  - 即使内层 `api.wait_update()` 返回 `false`，task/scheduler 也会在当前快照上推进一次
- `TargetPosScheduler` 已能驱动内部 `TargetPosTask`
- `TargetPosTask::execution_report()` 已暴露稳定 execution report
  - 原始 command-level 事件流当前包含 insert/cancel/trade/order finished/target reached
  - 同时维护 trades buffer、per-order outcome report、委托/撤单/终态订单计数、累计成交手数/成交额、最后一次 target reached
- `TargetPosTask` 在缺少可定价 quote 时，会按 symbol 自动发起一次 `subscribe_quote`
- `TargetPosTask::last_error()` 会暴露本地命令提交失败
  - 第一版不对本地提交失败做静默重试，而是记录错误并结束任务
- `TargetPosScheduler::execution_events()` 已按 `step_index` 聚合内部 task 事件
- `TargetPosScheduler::execution_report()` 已聚合内部 step task 的 trades buffer、命令计数摘要与稳定 per-step outcome report
- `TargetPosScheduler::last_error()` 会向外冒泡内部 step task 的本地提交失败
- `price_mode / offset_priority / split_policy` 的配置 surface 已冻结为 task 层 public types
- 内部纯规划器已经覆盖 `OpenOnly` / `今昨开` / `今昨,开` / `昨开` 的基础 offset 计划语义
- `TargetPosTask` 已接入最小真实 planner：
  - `OpenOnly` / `今昨开` / `今昨,开` / `昨开` 都会按 planner 结果推进
  - `PriceMode::Active / Passive` 会影响委托价格
  - `split_policy` 已接入最小确定性拆单
  - 只有当目标持仓匹配且挂单都进入终态后，`wait_target_reached()` 才会完成
  - 同一请求序号在净持仓未变化前不会重复发单
  - 若挂单进入终态但持仓未变化，会在同一目标请求下重新发单
  - 若 live order 与最新期望 batch 不一致，会优先只撤 stale 子集，保留仍匹配新计划的 live order
  - 若已有 live order 与最新计划方向/offset/价格兼容但手数不足，会保留已有订单并只补齐缺口
  - stale live order 进入终态后，再在后续 `wait_update()` 里按新计划补齐或重发
  - 多笔 live order 中 stale 子集撤单后，仍会保留兼容订单，并在 stale 终态后继续提交缺口 batch
  - 已提交但尚未出现在本地状态树的 tracked order 会被视为 pending：
    - 重复设置相同目标时继续等待原提交，不重复发单
    - retarget 到当前持仓或不兼容目标时，会先提交撤单，不会提前 target reached
  - SHFE/INE 与非 SHFE 的 `CloseToday` / `Close` 差异已落到执行层集成测试
  - 当前实现仍是保守串行 batch：
    - 每次 `wait_update()` 最多提交一个 planner batch
    - 同一 batch 内可连续提交多笔委托
    - batch 与 batch 之间仍等待持仓/挂单状态推进后再继续
- `TargetPosScheduler` 当前最小执行语义：
  - 每个 step 都会创建并驱动内部无 ownership 的 `TargetPosTask`
  - 因此也继承内部 task 的按需 quote 自动订阅语义
  - 已支持 step 级 `price_mode`
  - 已支持 pause step
  - 非最后一步会按“交易时段内累计 elapsed”判断 interval 是否到期
    - 当前最小实现基于 `quote.trading_time` + `TradingDayCalendar`
    - `TaskHost::refresh_trading_calendar()` 允许显式预取官方交易日历；`TaskHost::set_trading_calendar()` 允许调用方注入本地 calendar
    - 缺少某天 calendar 数据时会回退 weekday 规则，避免网络查询失败导致任务卡死
    - 若拿不到有效 trading session，则保守回退到 wall-clock
  - 到期后会先发真实撤单，并在挂单进入终态后再切到下一步
  - 最后一步要等目标持仓真正达到后才 finished

当前还未落地：

- 更复杂的多单/多批次主动撤单后重规划
- 自动 hedge / flatten、timed cancel / replace、group/account resume / audit
- 跨账户 TargetPos 编排、自动补单 / 跨账户对冲
- 合约 metadata 规则、组合级 what-if 保证金试算、多账户联合风控
- 完整 live / sim / replay environment adapter
- fake reconnect / 跨 step 部分成交推进 / 更完整 broker 行为

## 为什么它必须独立成 crate

### 不是协议层

`TargetPosTask`、scheduler、ownership 都不属于协议 contract：

- 它们不是远端 wire/schema 的一部分
- 它们维护的是本地任务内部状态
- 它们会引入 task lifecycle、symbol ownership、冲突处理等高层语义

所以它们不能进入 `tqsdk-core`。

### 不是 one-shot request/response

`TargetPosTask` 的本质是：

- 持续读 live state
- 持续发命令
- 持续维护内部任务状态

这显然不是 `tqsdk-session` 的“一次 await 请求/响应”范畴。

### 也不只是 facade 便利层

`tqsdk-wait` / `tqsdk-stream` 当前只负责“如何消费同一棵状态树”。

`TargetPosTask` 则要负责：

- 谁拥有某个账户某个 symbol 的执行权
- 用户手动下单是否允许穿透
- 任务取消、重规划、失败、收尾
- scheduler 每一步的 deadline 与执行报告

这已经是更高一层的执行工具，而不是 facade 本身。

## 参考实现带来的约束

## `tqsdk-python`

Python 的 `TargetPosTask` 给出了三个关键约束：

1. 同一账户 + 同一合约应视为单例 ownership。
2. `set_target_volume()` 之后，真正的下单动作依赖后续 `wait_update()` 推进。
3. 不允许和手动 `insert_order()` 混用，否则语义会失控。

值得继承的部分：

- 单 owner / 单推进点心智非常稳定
- ownership 冲突规则明确
- 任务与 `wait_update()` 的推进关系自然

不应照搬的部分：

- 把单例和任务资源管理写死在全局对象里
- 把 task/tooling 挤回单体 `TqApi`

## `tqsdk-rs`

现有 `tqsdk-rs` 给出了另外三点工程经验：

- task registry / manual order guard 非常重要
- scheduler 应独立于单步 `TargetPosTask`
- execution report / task progress 应独立保存，不污染 live state

值得吸收的部分：

- task registry
- symbol ownership
- scheduler step gating
- execution report / trades buffer

不应照搬的部分：

- 把 `TqRuntime` 做成一个很宽的总入口
- 让 task runtime 顺手承接 downloader / data manager / callback 等无关能力

## 为什么第一版先绑定 `tqsdk-wait`

`tqsdk-task` 理论上可以建立在 `wait` 或 `stream` 之上，但第一版不应同时抽象两套 substrate。

推荐第一版先绑定 `tqsdk-wait`，原因：

- `TargetPosTask` 的 canonical 语义本来就依赖单推进点
- planner 需要基于稳定 commit 截面做决策
- ownership / 手动下单冲突控制在单 owner 模式下最容易做对
- 这条路径最接近 Python 官方行为

为什么第一版不直接建立在 `tqsdk-stream` 上：

- stream 适合多消费者与事件投影，不天然提供“谁是唯一推进点”
- 如果为了 task 再在 stream 上造 registry / planner loop / command guard，很容易重做一遍 `TqRuntime`
- 这会把 `tqsdk-stream` 从消费层重新变胖

结论：

- 第一版 `tqsdk-task` 只依赖 `tqsdk-wait`
- 后续若确实需要 stream 驱动的执行任务，再追加单独 adapter，而不是先做泛化抽象

## 最小 canonical API 草图

### root host

推荐第一版提供一个显式 host，而不是把 task 直接塞进 `TqApi`：

```rust
pub struct TaskHost {
    /* private */
}

impl TaskHost {
    pub fn new(api: tqsdk_wait::TqApi) -> Self;

    pub fn api(&self) -> &tqsdk_wait::TqApi;
    pub fn api_mut(&mut self) -> &mut tqsdk_wait::TqApi;

    pub fn with_risk(self, risk: RiskEngine) -> Self;
    pub fn set_risk(&mut self, risk: RiskEngine);
    pub fn risk(&self) -> Option<&RiskEngine>;

    pub async fn wait_update(
        &mut self,
        deadline: Option<tokio::time::Instant>,
    ) -> tqsdk_task::Result<bool>;

    pub fn orders(&mut self, account_id: impl AsRef<str>) -> TaskOrderBuilder<'_>;

    pub fn execution_group(
        &mut self,
        account_id: impl AsRef<str>,
    ) -> ExecutionGroupBuilder<'_>;

    pub fn account_group(&self) -> AccountGroupBuilder;

    pub fn multi_account_order(
        &mut self,
        accounts: AccountGroup,
    ) -> MultiAccountOrderBuilder<'_>;

    pub fn target_pos(
        &mut self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> TargetPosBuilder<'_>;

    pub fn target_pos_scheduler(
        &mut self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> TargetPosSchedulerBuilder<'_>;

    pub async fn insert_order_guarded(
        &mut self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
        direction: tqsdk_core::TradeDirection,
        offset: Option<tqsdk_core::TradeOffset>,
        volume: i64,
        limit_price: Option<serde_json::Value>,
    ) -> tqsdk_task::Result<tqsdk_wait::OrderRef>;

    pub async fn cancel_order_guarded(
        &mut self,
        account_id: impl AsRef<str>,
        order_id: impl AsRef<str>,
    ) -> tqsdk_task::Result<()>;
}
```

设计意图：

- `TaskHost` 拥有单一推进点
- 用户继续通过 `api()` 读取 live refs
- 任务相关命令通过 host 本身走，便于做 ownership guard
- `orders(...)` 是 task 层 typed 手动下单入口，复用 wait 层 `OrderTicket`，
  不创建第二套订单状态
- 配置 `RiskEngine` 后，typed order builder 与 legacy guarded insert 都必须经过同一套 risk gate
- `execution_group(...)` 是 task 层多腿 foundation，复用相同 risk/ownership preflight 和 wait 层
  `OrderTicket`，只报告 group outcome/exposure，不自动提交 hedge 单
- guarded cancel 需要先从本地状态解析订单对应 symbol；若订单尚未进入状态树，第一版应保守拒绝
- 不要求 `tqsdk-wait` 反向感知 task registry

`api_mut()` 只是 escape hatch，不应成为常规命令入口。

如果用户绕过 guarded API 直接对底层 `TqApi` 或 `SessionClient` 发单，视为主动绕过 ownership 保护，第一版不保证语义。

### task-level order builder

```rust
pub struct TaskOrderIntent {
    pub account_id: String,
    pub symbol: String,
    pub direction: tqsdk_core::TradeDirection,
    pub offset: Option<tqsdk_core::TradeOffset>,
    pub volume: i64,
    pub limit_price: Option<f64>,
}

pub struct TaskOrderBuilder<'a> {
    /* private */
}

impl<'a> TaskOrderBuilder<'a> {
    pub fn buy_open(self, symbol: impl AsRef<str>, volume: i64) -> TaskOrderDraft<'a>;
    pub fn sell_open(self, symbol: impl AsRef<str>, volume: i64) -> TaskOrderDraft<'a>;
    pub fn buy_close(self, symbol: impl AsRef<str>, volume: i64) -> TaskOrderDraft<'a>;
    pub fn sell_close(self, symbol: impl AsRef<str>, volume: i64) -> TaskOrderDraft<'a>;
}

pub struct TaskOrderDraft<'a> {
    /* private */
}

impl TaskOrderDraft<'_> {
    pub fn limit(self, price: f64) -> Self;
    pub fn intent(&self) -> &TaskOrderIntent;
    pub async fn send_once(
        self,
        client_order_id: impl Into<tqsdk_wait::ClientOrderId>,
    ) -> tqsdk_task::Result<tqsdk_wait::OrderTicket>;
}
```

设计意图：

- task 层下单应表达用户意图，而不是暴露 `serde_json::Value` 价格字段。
- `send_once()` 继续委托 `tqsdk-wait::LimitOrderIntent`，因此订单去重、
  command ledger 和 terminal wait 都复用 wait/session substrate。
- `TaskOrderIntent` 是 risk gate 的稳定输入快照；它不是一棵新的订单状态树。

### pre-trade risk gate

```rust
pub struct RiskEngine {
    /* private */
}

impl RiskEngine {
    pub fn new() -> Self;
    pub fn max_order_volume(self, max: i64) -> Self;
    pub fn min_available(self, min_available: f64) -> Self;
    pub fn max_net_position(self, max_abs_net: i64) -> Self;
    pub fn max_price_deviation(self, max_abs_deviation: f64) -> Self;

    pub fn check(
        &self,
        api: &tqsdk_wait::TqApi,
        intent: &TaskOrderIntent,
    ) -> tqsdk_task::Result<RiskDecision>;
}

pub enum RiskDecision {
    Accepted,
    Rejected(RiskRejection),
}

pub enum RiskRejection {
    MaxOrderVolumeExceeded { /* typed fields */ },
    MissingAccount { /* typed fields */ },
    AvailableBelowMinimum { /* typed fields */ },
    MissingPosition { /* typed fields */ },
    NetPositionLimitExceeded { /* typed fields */ },
    MissingQuote { /* typed fields */ },
    PriceDeviationExceeded { /* typed fields */ },
}
```

设计意图：

- 风控属于执行工具层，不下沉到 `tqsdk-core` 或 `tqsdk-session`。
- 规则读取 `TqApi` 的 account / position / quote refs，使用同一 runtime 状态树和
  partition read 面，不维护私有资金或持仓状态。
- 风控拒绝必须是 typed reason，不能要求业务代码解析字符串拒单原因。
- 这一版只覆盖最小 pre-trade gate；合约 metadata、组合保证金 what-if、
  多腿 / 多账户联合限额属于后续 execution/risk layer 扩展。

### execution group foundation

```rust
pub enum HedgePolicy {
    ReportExposure,
    FlattenFilledLegs,
}

pub struct ExecutionGroupBuilder<'a> {
    /* private */
}

impl<'a> ExecutionGroupBuilder<'a> {
    pub fn client_group_id(self, group_id: impl Into<String>) -> Self;
    pub fn max_unhedged(self, duration: std::time::Duration) -> Self;
    pub fn on_leg_failed(self, policy: HedgePolicy) -> Self;
    pub fn leg(self, symbol: impl AsRef<str>) -> ExecutionLegBuilder<'a>;
    pub async fn send_once(self) -> tqsdk_task::Result<ExecutionGroupTicket>;
}

pub struct ExecutionGroupTicket {
    /* private */
}

impl ExecutionGroupTicket {
    pub fn group_id(&self) -> &str;
    pub fn legs(&self) -> &[ExecutionLegTicket];
    pub fn status(&self, api: &tqsdk_wait::TqApi) -> tqsdk_task::Result<ExecutionGroupStatus>;
    pub fn outcome(
        &self,
        api: &tqsdk_wait::TqApi,
    ) -> tqsdk_task::Result<Option<ExecutionGroupOutcome>>;
    pub async fn wait_finished(
        &self,
        host: &mut TaskHost,
        deadline: tokio::time::Instant,
    ) -> tqsdk_task::Result<ExecutionGroupOutcome>;
}
```

设计意图：

- execution group 属于 `tqsdk-task`，因为它维护的是业务执行意图、腿状态汇总和裸露风险解释。
- 每条腿仍通过 wait 层 `OrderTicket` 和 session-scoped client intent ledger 提交，不创建 task 私有订单状态树。
- group submit 会先对所有腿做 ownership/risk/local validation，避免“第一腿已发、第二腿本地拒绝”的 P0 风险。
- 当前 `HedgePolicy::ReportExposure` 是唯一已支持策略；`FlattenFilledLegs` 明确返回 unsupported，
  不伪装自动 hedge。
- `ExecutionGroupOutcome::NeedsHedge` 是显式风险信号，调用方可以人工或后续 policy 层处理。

### account group foundation

```rust
pub struct Ratio {
    /* private */
}

impl Ratio {
    pub fn new(numerator: u32, denominator: u32) -> tqsdk_task::Result<Self>;
}

pub struct AccountGroup {
    /* private */
}

impl AccountGroup {
    pub fn builder() -> AccountGroupBuilder;
    pub fn allocate(&self, total_volume: i64) -> tqsdk_task::Result<AccountAllocationPlan>;
}

pub enum AccountFailurePolicy {
    ReportExposure,
    FlattenFilledAccounts,
}

pub struct MultiAccountOrderBuilder<'a> {
    /* private */
}

impl<'a> MultiAccountOrderBuilder<'a> {
    pub fn client_group_id(self, group_id: impl Into<String>) -> Self;
    pub fn max_unhedged(self, duration: std::time::Duration) -> Self;
    pub fn on_account_failed(self, policy: AccountFailurePolicy) -> Self;
    pub fn buy_open(self, symbol: impl AsRef<str>, total_volume: i64) -> MultiAccountOrderDraft<'a>;
    pub fn sell_open(self, symbol: impl AsRef<str>, total_volume: i64) -> MultiAccountOrderDraft<'a>;
}

pub struct MultiAccountOrderTicket {
    /* private */
}

impl MultiAccountOrderTicket {
    pub fn group_id(&self) -> &str;
    pub fn orders(&self) -> &[MultiAccountOrderLegTicket];
    pub fn status(&self, api: &tqsdk_wait::TqApi) -> tqsdk_task::Result<MultiAccountOrderStatus>;
    pub fn outcome(
        &self,
        api: &tqsdk_wait::TqApi,
    ) -> tqsdk_task::Result<Option<MultiAccountOrderOutcome>>;
    pub async fn wait_finished(
        &self,
        host: &mut TaskHost,
        deadline: Option<tokio::time::Instant>,
    ) -> tqsdk_task::Result<MultiAccountOrderOutcome>;
}
```

设计意图：

- account group 属于 `tqsdk-task`，因为它表达的是组合执行意图、比例拆单和账户级结果汇总。
- 每个账户 allocation 仍通过 wait 层 `OrderTicket` 和 session-scoped client intent ledger 提交。
- multi-account submit 会先对所有账户订单做 ownership/risk/local validation，避免“部分账户已发、另一账户本地拒绝”的资金安全风险。
- 当前 `AccountFailurePolicy::ReportExposure` 是唯一已支持策略；`FlattenFilledAccounts` 明确返回 unsupported，
  不伪装自动补单或跨账户 hedge。
- `MultiAccountOrderOutcome::NeedsAttention` 是显式人工介入信号，调用方可以据此人工处理或接入后续 policy 层。

### target position task

```rust
pub struct TargetPosBuilder<'a> {
    /* private */
}

impl<'a> TargetPosBuilder<'a> {
    pub fn price_mode(self, mode: PriceMode) -> Self;
    pub fn offset_priority(self, priority: OffsetPriority) -> Self;
    pub fn split_policy(self, policy: VolumeSplitPolicy) -> Self;
    pub fn build(self) -> tqsdk_task::Result<TargetPosTask>;
}

pub struct TargetPosTask {
    /* private */
}

impl TargetPosTask {
    pub fn symbol(&self) -> &str;
    pub fn account_id(&self) -> &str;
    pub fn is_finished(&self) -> bool;
    pub fn last_error(&self) -> Option<TaskError>;
    pub fn execution_report(&self) -> TargetPosTaskExecutionReport;
    pub fn execution_events_since(&self, start: usize) -> (usize, Vec<TargetPosTaskExecutionEvent>);
    pub fn execution_trades_since(&self, start: usize) -> (usize, Vec<TargetPosTaskTradeFill>);

    pub fn set_target_volume(&self, volume: i64) -> tqsdk_task::Result<()>;
    pub fn current_target_volume(&self) -> Option<i64>;
    pub async fn cancel(&self) -> tqsdk_task::Result<()>;
    pub async fn wait_target_reached(&self) -> tqsdk_task::Result<()>;
    pub async fn wait_finished(&self) -> tqsdk_task::Result<()>;
}
```

设计意图：

- `set_target_volume()` 只登记请求，不要求立刻推进
- 真正的规划与执行在后续 `TaskHost::wait_update()` 中发生
- `TargetPosTask` 自己只暴露任务态信息，不暴露底层 live refs

### target position scheduler

```rust
pub struct TargetPosScheduleStep {
    pub interval: std::time::Duration,
    pub target_volume: i64,
    pub price_mode: Option<PriceMode>,
}

pub struct TargetPosSchedulerBuilder<'a> {
    /* private */
}

impl<'a> TargetPosSchedulerBuilder<'a> {
    pub fn steps(self, steps: Vec<TargetPosScheduleStep>) -> Self;
    pub fn offset_priority(self, priority: OffsetPriority) -> Self;
    pub fn split_policy(self, policy: VolumeSplitPolicy) -> Self;
    pub fn build(self) -> tqsdk_task::Result<TargetPosScheduler>;
}

pub struct TargetPosScheduler {
    /* private */
}

impl TargetPosScheduler {
    pub fn symbol(&self) -> &str;
    pub fn account_id(&self) -> &str;
    pub fn is_finished(&self) -> bool;
    pub fn execution_report(&self) -> TargetPosExecutionReport;
    pub fn execution_events(&self) -> Vec<TargetPosSchedulerExecutionEvent>;
    pub fn execution_events_since(
        &self,
        start: usize,
    ) -> (usize, Vec<TargetPosSchedulerExecutionEvent>);
    pub fn execution_trades_since(
        &self,
        start: usize,
    ) -> (usize, Vec<TargetPosSchedulerTradeFill>);
    pub fn last_error(&self) -> Option<TaskError>;
    pub async fn cancel(&self) -> tqsdk_task::Result<()>;
    pub async fn wait_finished(&self) -> tqsdk_task::Result<()>;
}
```

设计意图：

- scheduler 是 `TargetPosTask` 的编排器，不应与单步调仓任务混为一个类型
- execution report 属于任务自身，不应写回 runtime state tree
- `execution_events_since()` / `execution_trades_since()` 为高频消费场景保留 cursor-style 增量读取
- 当前第一版已实现 `steps()` / `build()` / `cancel()` / `wait_finished()` / `execution_report()`
- `offset_priority` / `split_policy` 已进入真实最小执行路径

## ownership 与冲突策略

第一版必须明确三条规则：

1. 同一 `account_id + symbol` 同时最多一个活动中的 target task / scheduler。
2. 当某个 symbol 被 scheduler 或 target task 占有时，guarded manual order 默认拒绝。
3. 取消任务后 ownership 需要通过后续 `wait_update()` 显式释放，不能依赖 drop 时机碰运气。

推荐最小 registry 信息：

- `task_id`
- `account_id`
- `symbol`
- `task_kind`
- `config_fingerprint`
- `state`:
  - idle
  - active
  - cancelling
  - finished

还应记录：

- 最近一次请求序号
- 最近一次已完成目标序号
- 失败信息

## wait 驱动模型

第一版推荐的 `TaskHost::wait_update()` 流程：

1. 先调用内层 `api.wait_update(deadline)`，拿到当前稳定 commit
2. 基于该稳定截面运行所有活跃 task 的 planner
3. planner 可以提交撤单 / 下单命令，但这些命令的结果应在后续 `wait_update()` 中再变成用户可见状态
4. 本次 `wait_update()` 返回给用户的仍然是“当前这一轮是否推进到了新 commit”

关键约束：

- task 内部等待不能吞掉用户可见 commit
- task 提交命令不能伪造额外的状态树
- task 状态与 live state tree 必须分离

## 第一版明确不做

- 不做 stream-based task substrate
- 不做 callback bridge
- 不做 downloader / DataFrame / polars
- 不做回测报告
- 不做 GUI / drawing
- 不把 `TargetPosTask` 直接塞进 `tqsdk-wait`
- 不先造一个宽而泛的 `TqRuntime`

## 第一版验收标准

如果 `tqsdk-task` 第一版完成，至少应能验证：

1. 同一 `account + symbol` 的重复 `TargetPosTask` 构造会被 registry 明确处理。
2. `set_target_volume()` 后，后续 `TaskHost::wait_update()` 能驱动任务推进。
3. 任务持有 symbol ownership 时，guarded manual order 会被拒绝。
4. `cancel()` / `wait_finished()` / `wait_target_reached()` 的任务生命周期清晰可验证。
5. `TaskHost::orders(...)` 下单不暴露 `serde_json::Value` 价格字段，并返回 wait 层
   `OrderTicket`。
6. 配置 `RiskEngine` 后，typed order builder 与 `insert_order_guarded` 都会在提交前返回
   typed `RiskRejection`。
7. `ExecutionGroup` 能用 typed group id 提交两腿订单、先做 all-leg preflight，并给出
   typed group outcome/exposure report。
8. `AccountGroup` 能用 typed group id 提交比例拆分后的多账户订单、先做全账户 preflight，
   并给出 typed per-account outcome report。
9. `StrategyHost` 能提供同一 strategy context 读取 quote/account/position，并复用
   typed order、risk 和 target-pos 入口。
10. public test harness 能用 fake market / fake broker 测试策略，不暴露 runtime
    handle、provider protocol、channel 或 `Arc<Mutex<_>>`。
11. scheduler 能按步骤推进并给出独立 execution report。
12. `tqsdk-core` / `tqsdk-session` / `tqsdk-wait` 无需为了 task 反向改写主 contract。

## 下一步建议

当前 `tqsdk-task` 已经进入稳固阶段，下一步不应继续扩宽 surface，而应优先：

1. 增加真实联机 smoke 与 replay/模拟场景回归。
2. 在 `StrategyHost` / `StrategyReplay` 之上继续设计 live / sim / replay
   environment adapter。
3. 继续压测 `TargetPosTask` 在部分成交、撤单失败、价格跳变下的保守重规划。
4. 保持 task runtime 独立，不把 strategy host、test harness、scheduler、report、
   stream adapter、callback 倒灌进 core/session/wait。

本轮已补充未物化 tracked order 的回归：

- order ref 已经由本地提交返回、但远端 order diff 尚未进入状态树时，retarget 不会把该订单当作不存在。
- 重复设置相同目标不会清空等待状态，也不会在 order diff 到达前重复提交同方向订单。
