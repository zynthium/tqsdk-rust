# 使用者分层驱动的 Public API 迭代计划

## 文档定位

本文档把场景驱动审查、官方 `tqsdk-python` 调研结果和当前 Rust
crate 分层合并成一份迭代计划。

这里对齐的对象不是 Python SDK 的 public API 名称，而是不同类型终端用户的工作流：

- 策略作者需要低样板、稳定状态截面和清晰的交易一致性。
- 系统集成方需要 async-native、多消费者、背压和健康状态。
- 高频或基础设施用户需要薄底座、可控推进和热路径读面。
- 执行工具用户需要订单 intent、任务 ownership、风控和多账户隔离。
- 研究用户需要历史数据、批处理、缓存和回放。
- 测试用户需要 fake market / fake broker / deterministic clock。

官方 Python SDK 是成熟使用者语义的证据来源，不是 Rust API 的复制模板。

本计划的硬边界：`tqsdk-rust` 是核心交易 SDK，不是策略平台、生产守护平台、
行情中台或自动执行系统。Rust 分层可以提供比 Python 更清晰的 typed substrate
和 escape hatch，但不能把官方 Python 没有承诺的高级系统能力默认升级为核心
public API。

## 调研依据

本轮主要参考：

- `~/Projects/GitHub/tqsdk-python/tqsdk/api.py`
  - `TqApi.__init__` 的初始快照等待语义
  - `wait_update()` 的单推进点与稳定截面语义
  - `get_quote` / `get_kline_serial` / `get_tick_serial`
  - `insert_order` / `cancel_order`
  - `get_account` / `get_position` / `get_order` / `get_trade`
- `~/Projects/GitHub/tqsdk-python/tqsdk/connect.py`
  - 重连后记录请求、重发订阅、暂停向上输出、等待完整快照后恢复
- `~/Projects/GitHub/tqsdk-python/tqsdk/lib/target_pos_task.py`
  - 目标持仓任务、拆单、撤单、追价和同账户同合约任务唯一性
- `~/Projects/GitHub/tqsdk-python/tqsdk/multiaccount.py`
  - 多账户模式下显式 account 参数与状态隔离
- `~/Projects/GitHub/tqsdk-python/tqsdk/risk_manager.py`
  - 下单前统一风控检查入口
- `~/Projects/GitHub/tqsdk-python/tqsdk/backtest.py`
  - 实盘、模拟、回测、回放共享策略心智
- `~/Projects/GitHub/tqsdk-python/tqsdk/tools/downloader.py`
  - 历史 tick / K线下载、进度和 CSV 落盘
- `~/Projects/GitHub/tqsdk-python/tqsdk/scenario/tqscenario.py`
  - 保证金和持仓变动的 what-if 场景试算

结合当前 Rust 侧审查材料：

- [`docs/reviews/public-api-scenario-review.md`](../reviews/public-api-scenario-review.md)
- [`docs/scenarios/api_gaps/`](api_gaps/)
- [`docs/architecture/crate-boundaries.md`](../architecture/crate-boundaries.md)
- [`docs/architecture/facade-paradigms.md`](../architecture/facade-paradigms.md)

## 使用者分层

| 使用者 | 主要需求 | Rust 推荐入口 | 对应场景 | 迭代判断 |
| --- | --- | --- | --- | --- |
| 低层 / 高频用户 | 自带 Tokio runtime、自己推进 session、热路径读取行情 | `tqsdk-core` + `tqsdk-session` | 5, 23, 27 | 维持薄底座，不上移厚 facade |
| 单策略作者 | 低样板、`wait_update()`、稳定状态截面、交易状态易懂 | `tqsdk-wait` | 1, 3, 6, 7, 8, 9, 10, 25, 26 | 继承 Python 语义，不复制 Python 单体 |
| async 系统集成方 | 多消费者、stream、背压、错误事件、健康状态 | `tqsdk-stream` + `tqsdk-session` | 2, 4, 20, 21, 22 | 强化事件和恢复语义 |
| 执行工具用户 | 目标持仓、订单 intent、撤补、两腿套利、风控、多账户 | `tqsdk-task` | 10, 11, 12, 13, 19, 29 | 建立执行层抽象，不下沉到 core |
| 研究 / 数据用户 | 历史数据、批处理、缓存、CSV、离线分析 | `tqsdk-data` | 16, 17, 18, 28 | 独立数据层，不污染 session/wait |
| 测试 / 回放用户 | fake market、fake broker、同策略 live/sim/replay 切换 | `tqsdk-task` + 测试支持层 | 15, 16, 24 | 面向策略可测试性设计 |
| 多 provider 基础设施用户 | 多行情源聚合、标准事件、provider 隔离 | 用户层 facade / 后续独立项目 | 14 | 暂缓，非核心 SDK 目标 |

## 从 Python SDK 对齐的语义，不对齐的形状

应对齐的成熟语义：

- 初始化后先获得可用状态截面，再运行用户策略。
- 重连时自动恢复订阅和交易同步，恢复完成前不让用户误读半截面。
- 下单 intent 和订单状态必须可恢复、可对账、可解释。
- 同账户同合约的执行任务需要 ownership 约束。
- 多账户模式必须显式账户归属，不能共享模糊状态。
- 风控应在下单入口统一执行，而不是散落在策略代码里。
- 实盘、模拟、回放、测试应尽可能共享同一套策略事件模型。
- 历史数据、缓存、下载和研究批处理应有独立用户入口。

不应照搬的 API 形状：

- 不把所有能力塞回一个 Rust 版单体 `TqApi`。
- 不要求普通用户直接使用 Python 式 `TqChan` / task 编排心智。
- 不在 `tqsdk-core` 拥有 event loop 或暴露 provider 私有协议。
- 不用原地更新 DataFrame 作为 Rust 研究层的唯一表达。
- 不为了 API 名字兼容牺牲 crate 边界和类型安全。

## 能力准入与降级规则

后续新增 public API 进入正式 `examples/*.rs` 之前，必须满足以下准入条件：

- 能在官方 Python SDK 的核心使用者语义中找到对应工作流，或是 Rust 分层必需的
  薄基础设施补强。
- 能清晰落在 `tqsdk-core` / `tqsdk-session` / `tqsdk-wait` /
  `tqsdk-stream` / `tqsdk-task` / `tqsdk-data` 的既有职责内。
- 不要求用户理解 provider protocol、私有 session、raw channel、内部 command
  pack、手写 Tokio task 编排或 `Arc<Mutex<_>>`。
- 不把策略决策、自动补偿交易、生产部署、跨进程协调、HTTP/GUI 运维入口塞进
  SDK 核心。
- 能保持单一 runtime commit / revision / command lifecycle。

以下能力统一降级为 desired sketch、用户层工具或外部系统职责，除非后续有新的
官方能力边界或明确用户需求重新立项：

- S12 自动 hedge / flatten / 补单引擎；
- S13 自动资产配置、多账户失败补偿和跨进程审计；
- S14 多 provider 行情聚合；
- S15 多 provider environment 和部署平台化配置；
- S16 生产级 daemon reconnect orchestration；
- S18 跨进程行情 cache service / cache daemon 管理；
- S19 组合保证金引擎、全局风控服务和 durable audit；
- S20 内置 HTTP health/metrics endpoint、GUI、web helper、进程管理器；
- S21 durable distributed queue 和 runtime state snapshot recovery 平台；
- S24 完整仿真交易所或生产级测试 fixture 持久恢复。

降级不是删除场景，而是防止把高级编排伪装成核心 SDK 缺口。降级后的内容可以
保留在 `docs/scenarios/api_gaps/`，用于说明用户如何在 SDK primitives 之上自行
构建。

## 推荐迭代顺序

### P0：启动 / 重连恢复语义

服务的使用者：

- 单策略作者
- async 系统集成方
- 执行工具用户

目标：

- 建立启动 ready barrier。
- 建立重连 resync barrier。
- 订阅意图在重连后自动恢复。
- 交易账户登录后能等待订单、成交、持仓、资金同步完成。
- 恢复期间对外暴露 typed recovery event，而不是 provider protocol 细节。

建议落点：

- `tqsdk-session`：恢复 substrate、订阅意图记录、route/trade sync 状态。
- `tqsdk-wait`：单策略用户的 `wait_ready()` / `recover_ready()` 风格薄包装。
- `tqsdk-stream`：多消费者用户的 `recovery_events()` / ready stream。

优先提升的场景：

- `api_contract_s09_startup_state_recovery`（已提升为正式 wait example）
- `api_contract_s02_dynamic_subscriptions`（已具备 reconnect 订阅恢复契约）
- `api_contract_s20_production_daemon` 的健康状态子集（已新增 typed health snapshot）
- `api_contract_s25_wait_serial_trading_status`（新增）：覆盖 wait 风格 trading status、
  K线 serial、tick serial 和 `is_changing` 契约，确认实时窗口不回流到 session/data。
- `api_contract_s26_trade_system_refs`（新增）：覆盖 notification、settlement、risk management、
  证券交易对象 ref 与 `confirm_settlement`，确认这些对象是 wait live refs，不是 session direct query。

已落地：

- 启动 ready barrier 已通过 `TqApi::startup_recovery` 与 `TqStream::recover_state`
  表达。
- Market adapter 会保留当前 quote / trading-status / chart 订阅意图；session
  reconnect/resync 完成后，runtime 会根据 adapter recovery commands 重新排队发送订阅，
  因此 `QuoteSubscription` 用户不需要维护第二份订阅集合。
- `TqStream::health()` 返回 `StreamHealthSnapshot`，覆盖 session phase、最近一次
  reconnect diagnostics、driver closed 和 revision；`TqStream::reconnect_monitor()`
  可以等待并报告 existing session reconnect 的恢复、耗尽、超时或关闭结果；
  strategy supervisor 的稳定 telemetry/export hook 已落在 `tqsdk-task`；managed commit sink foundation 已落在
  `tqsdk-stream`，有限重试和本地 JSONL WAL foundation 也已落在 `tqsdk-stream`；
  stream driver 关闭与 managed sink flush 的 graceful shutdown foundation 也已落在
  `tqsdk-stream`；WAL fsync policy 和本地 compaction 已落在 `tqsdk-stream`；
  WAL recovery report 和 commit metadata journal replay 已落在 `tqsdk-stream`；
  durable daemon queue / runtime state snapshot recovery 仍在后续 daemon/tooling 层。

### P0：Session direct-query / metadata pack

服务的使用者：

- 低层 / 高频用户
- direct-query 用户
- 需要一次性 metadata/service 查询的系统集成方

目标：

- 一次性 metadata / service request/response 明确归属 `tqsdk-session`。
- 合约列表、主连、期权、交易日历、结算价、排名和 EDB 有正式可编译契约。
- wait/stream 只通过 `session()` 复用底层 session，不复制 direct-query API。

建议落点：

- `tqsdk-session`：metadata/service one-shot direct query 与 raw GraphQL escape hatch。
- `tqsdk-wait` / `tqsdk-stream`：只保留 live refs / continuous consumption。
- `tqsdk-data`：历史下载、Greeks、DataFrame/polars 和研究派生。

优先提升的场景：

- `api_contract_s23_contract_metadata`
- `api_contract_s27_metadata_service_queries`（新增）：覆盖 metadata/service query
  pack，确认合约列表、主连、期权、交易日历、结算价、排名和 EDB 仍是 session
  one-shot request/response。

已落地：

- `api_contract_s27_metadata_service_queries` 已提升为正式 session example，覆盖
  `SessionClient::{query_quotes,query_cont_quotes,query_options,query_atm_options,query_all_level_options,query_all_level_finance_options,get_trading_calendar,query_symbol_settlement,query_symbol_ranking,query_edb_data}`。
- `SessionRawQuery::query_graphql_value` 仍是低层 escape hatch；示例主路径不要求用户解析
  `serde_json::Value`。

### P0：订单 intent 与断线一致性

服务的使用者：

- 单策略作者
- 执行工具用户
- 生产系统用户

目标：

- 把用户下单意图建模为 typed `OrderIntent`，而不是一次性函数调用副作用。
- client order id 成为一等类型。
- command ledger 能从本地意图、发送状态、交易回报和恢复对账解释订单状态。
- 重连后能区分：未发送、已发送未确认、已确认、被拒、未知待对账、终态。

建议落点：

- `tqsdk-core`：只在现有 command/order 状态机确有缺口时补最小 contract。
- `tqsdk-session`：保存可恢复命令意图和对账 substrate。
- `tqsdk-wait` / `tqsdk-stream`：提供不同消费形状下的 `OrderRef` / 订单事件。
- `tqsdk-task`：执行任务只消费 intent/result，不私造第二套订单状态。

优先提升的场景：

- `api_contract_s10_reconnect_order_consistency`（已提升为正式 wait example）
- `api_contract_s06_limit_order`
- `api_contract_s07_cancel_partial_fill`

已落地：

- `tqsdk-wait` 增加 `ClientOrderId`、`LimitOrderIntent` 和 `OrderTicket`。
- `TqApi::limit_order(...).client_intent(...).send_once()` 会把稳定 intent id
  映射为 runtime `order_id`，并在同一个 `SessionClient` 内避免相同 intent
  重复提交；同一 session 被重新包装成新的 facade 后仍保留该 intent 记录。
- `tqsdk-session` 增加 session-scoped `OrderIntentRecord` ledger，作为 wait/stream/task
  可复用的轻量执行一致性 substrate。
- `OrderTicket::status()` / `wait_reconnect_safe_terminal*()` 返回 typed
  `OrderTicketState`，业务代码不需要解析 command status 或 `order.status` 字符串。
- `OrderTicket::wait_partially_filled*()` / `cancel_remaining()` 直接复用内部
  `OrderRef` helper，让 stable intent 下单路径也能自然完成部分成交撤剩余量。

仍未完成、不可伪装为已支持：

- intent ledger 尚未跨进程持久化。
- `OrderTicketState` 已覆盖 command/order 级别的 typed 对账；trade fill 明细聚合仍需后续执行层补齐。

### P1：执行层抽象

服务的使用者：

- 自动调仓用户
- 套利和多腿策略用户
- 多账户交易用户

目标：

- 稳固 `TargetPosTask` ownership，避免手动下单与任务下单互相踩状态。
- 用 execution group 表达两腿 / 多腿订单生命周期和用户可审计 outcome。
- 支持最大裸露量的 typed report，让用户策略决定撤补、对冲和人工介入。
- 用 account group 明确多账户状态隔离和比例拆单，但不提供自动资产配置平台。

已落地：

- `ExecutionGroup` foundation 支持 typed group id、两腿订单、all-leg preflight、
  session-scoped retry idempotency、group outcome/exposure report 和
  revision-bound `ExecutionGroupReport`。
- `AccountGroup` foundation 支持 typed account group、比例拆单、全账户 preflight、
  session-scoped retry idempotency、per-account outcome report 和
  revision-bound `MultiAccountOrderGroupReport`。

已降级为用户层执行系统职责：

- 自动 hedge / flatten；
- timed cancel / replace；
- 自动补单 / 跨账户对冲；
- 跨账户 TargetPos 编排；
- group/account resume / audit log。

建议落点：

- `tqsdk-task`：维护当前 `ExecutionGroup` / `AccountGroup` 薄 foundation，
  不继续向自动 hedge/flatten、timed cancel、跨账户 TargetPos 和 audit policy
  膨胀。
- `tqsdk-wait` / `tqsdk-stream`：只提供所需 live state 和 order event。

优先提升的场景：

- `api_contract_s11_simple_strategy`
- `api_contract_s12_spread_arbitrage`（foundation 已提升为正式 task example）
- `api_contract_s13_multi_account_ordering`（foundation 已提升为正式 task example）
- `api_contract_s29_target_pos_ownership`（新增）：把 TargetPosTask / scheduler ownership
  从 S11 策略示例中独立出来，确认同账户同合约 owner、手动下单 guard 和
  `TaskHost::wait_update()` 统一推进。

### P1：风控前置与 what-if 试算

服务的使用者：

- 实盘策略作者
- 多账户执行用户
- 生产部署用户

目标：

- 下单前统一检查资金、持仓、价格、合约、限额和频率。
- 风控规则能组合、能解释拒单原因。
- 提供轻量 what-if 试算，用于开仓前估算保证金和持仓变化。

已落地：

- `tqsdk-task::RiskEngine` 提供最小 typed pre-trade gate：
  - 单笔最大手数；
  - 交易日内开仓次数；
  - 交易日内单合约开仓手数；
  - 合约组累计开仓手数；
  - 按账户 + 交易所的一秒订单操作频率；
  - 最低可用资金；
  - 当前持仓截面上的最大净持仓；
  - 基于当前 quote last price 的价格偏离限制；
  - 基于 `InstrumentSpec` 的 tick size 校验和 contract multiplier notional projection。
- `TaskHost::with_risk` / `set_risk` 将风控挂到执行 host 上。
- `TaskHost::orders(account).buy_open(...).limit(...).send_once(...)` 提供 typed
  task-level order builder，并复用 `tqsdk-wait::OrderTicket` 和 session-scoped
  client intent 去重。
- legacy `insert_order_guarded` 在配置 risk 后也会经过同一套 risk gate。
- guarded `cancel_order_guarded` 会经过订单操作频率限制。
- 开仓限额与订单频率是 `TaskHost` 本进程内用量计数，用于对齐官方 Python SDK
  的基础风控规则形态；不伪装为跨进程持久审计或服务端风控替代。
- 风控拒绝通过 `TaskError::RiskRejected(RiskRejection)` 返回 typed reason。
- `RiskEngine::check_report(...)` 返回 revision-bound `RiskCheckReport`，用于
  typed 审计风控通过或拒绝原因。
- `RiskEngine::project_order(...)` 返回轻量 revision-bound `RiskProjectionReport`，
  提供当前净持仓、投影净持仓和 price-volume estimate foundation。
- `RiskEngine::instrument_specs(...)` 可接入 `tqsdk_session::InstrumentSpec`，
  提供合约 tick size 校验和 contract multiplier notional projection foundation。

已降级为用户风控系统或上层工具职责：

- 涨跌停和交易所品种级规则。
- 组合级保证金 / 组合持仓 what-if simulation。当前仅有单笔订单轻量投影。
- 多账户 / 多腿执行组上的联合限额、最大裸露量和频率控制。
- 风控审计日志落库与热更新。

建议落点：

- `tqsdk-task`：维护当前 `RiskEngine` 的基础规则、typed rejection 和单笔轻量
  projection，不扩成组合保证金或全局风控服务。
- 用户上层工具：需要组合级 what-if、热更新或 durable audit 时，在 SDK
  primitives 之上自行构建。

优先提升的场景：

- `api_contract_s19_pre_trade_risk`（最小 foundation 已提升为正式 task example）

### P1：策略运行时与可测试性

服务的使用者：

- 希望同一策略跑 live / sim / replay 的用户
- 希望单元测试策略的用户

目标：

- 提供统一策略事件模型。
- fake market / fake broker / deterministic clock 成为 public test support。
- 用户不需要在测试里手动搭 runtime state、channel 或 provider protocol。

建议落点：

- `tqsdk-task`：策略运行时与执行桥接。
- 后续可评估独立 `tqsdk-testing`，但不应过早拆 crate。
- replay/history 数据来源复用 `tqsdk-session` / `tqsdk-data`。

优先提升的场景：

- `api_contract_s15_live_sim_replay_switch`
- `api_contract_s16_history_replay_strategy`
- `api_contract_s24_testable_strategy`

已落地：

- `tqsdk-task` 已建立最小 `StrategyHost` / `StrategyContext`，策略步骤可以在同一
  task/wait 推进点内读取 quote/account/position，并复用 typed order、
  target-pos 和 risk gate。
- `tqsdk-task::StrategyEnvironment` / `StrategyEnvironmentContext` 已提供
  live/sim task host、public fake harness 和 replay builder 的最小统一 context
  adapter；S15 environment foundation 已提升为正式 task example。
- `tqsdk-task::StrategyDeploymentConfig` / `StrategyDeployment` /
  `StrategyLifecycle` 已提供 S15 provider-backed TQKQ sim config、live trade
  config、统一 run loop、typed stop reason 和 graceful shutdown report；策略步骤
  仍只依赖 `StrategyEnvironmentContext`。
- `tqsdk-task::StrategySupervisor` / `StrategyRetryPolicy` /
  `StrategyShutdownSignal` 已提供 task-layer supervisor foundation，覆盖 typed
  health/metrics snapshot、显式有限 retry、ctrl-c shutdown hook 和 typed
  shutdown report；S15 example 已改为通过 supervisor lifecycle 运行。
- `tqsdk-task::testing` 已提供 public `StrategyTestHarness`、`FakeMarket`
  和 `FakeBroker`，支持全成、拒单和单步/跨 step 部分成交测试；`StrategyTestClock`
  与 `FakeBroker::latency_steps` 已提供 deterministic fake broker time 和
  step latency；`FakeBroker::partial_fills` 已提供跨 step 部分成交推进；
  `FakeBroker::disconnect_for_steps` 与
  `FakeBrokerConnectionStatus` 已提供 broker disconnect/reconnect 注入；用户不需要
  hidden `*_for_test` API、runtime handle、channel 或 provider protocol。
- `tqsdk-task::StrategyReplay` 已连接 `tqsdk-data::MarketCacheReplay`，
  cache quote/kline/tick event 可以按时间顺序推进到同构 `StrategyContext`，
  并复用 typed order builder 与 fake broker。
- `StrategyReplayCheckpoint` / `StrategyReplayBuilder::resume_from` 已提供
  deterministic replay clock 与内存级 checkpoint/resume foundation。
- `StrategyReplaySpeed` / `StrategyReplayBuilder::speed` 已提供最快、
  real-time 和 scaled replay speed policy。
- `StrategyReplayCheckpointStore` / `StrategyReplayBuilder::resume_from_store`
  已提供 JSON file-backed durable checkpoint persistence foundation。
- `StrategyReplaySourceBuilder` 已提供多序列 event source 合并入口，用户不需要
  手写 vector 拼接和排序。
- `KlineDataSeries` / `TickDataSeries` 已提供 history series -> cache replay
  adapter，S16 可以从 `DataClient` 历史序列直接进入 `StrategyReplay`。
- S11 简单策略已提升为正式 task example；S24 最小可测试策略已新增正式
  task example；S16 history series -> strategy context 子集已提升为正式 task
  example。

边界收口：

- S15 配置文件反序列化可作为薄便利能力评估；完整 reconnect orchestration 和
  多 provider environment 降级为用户层部署/基础设施能力。
- S24 保持 fake market / fake broker / deterministic clock 测试 primitive；
  更完整 broker 行为、复杂撮合模型和生产级 fixture 持久恢复不进入核心 SDK。
- 跨进程 intent / test fixture 持久恢复由用户测试平台或外部存储实现。

### P2：生产守护、慢消费者隔离和错误诊断

服务的使用者：

- async 系统集成方
- 生产部署用户
- 写库、日志、监控组件作者

目标：

- 健康状态、恢复状态、错误分类、重试策略成为 typed event。
- 慢消费者策略明确：drop、lag error、可靠队列、专用 sink。
- 优雅关闭和 metrics hook 有稳定入口。
- S20 完成标准不包含 Rust GUI、web helper 或内置 HTTP health/metrics endpoint；
  SDK 只提供 transport-neutral 的 typed snapshot / hook，由用户按部署环境接入
  tracing、日志、进程守护或外部指标系统。

建议落点：

- `tqsdk-stream`：health/recovery/error event stream、sink isolation。
- `tqsdk-session`：底层连接和登录错误分类。

已落地：

- `tqsdk-core::ContractError` 提供 stable `ContractErrorKind` 与 `RetryHint`，
  只表达底层错误类别和重试提示，不承载 stream/sink 策略。
- `tqsdk-session::SessionFacadeError::diagnostic()` 将 core 错误映射为
  session 级 typed diagnostic，并提供 `is_retryable()`。
- `tqsdk-stream::StreamFacadeError::diagnostic()` 覆盖 session/contract 错误、
  `Lagged`、`Closed` 和 missing value，慢消费者 lag 不再需要字符串判断。
- `tqsdk-stream::StreamRetryPolicy` 提供 stream-facing retry decision 和最小
  async backoff runner；它不执行 reconnect，也不解释业务拒单。
- `TqStreamBuilder::commit_channel_capacity(...)` 暴露 root fan-out buffer 配置；
  `CommitStream` 继续使用 bounded broadcast，落后 consumer 通过 typed `Lagged`
  观察背压。
- `StreamHealthSnapshot::status()` / `should_restart()` 补齐生产 health snapshot
  的最小状态判定。
- `TqStream::reconnect_monitor()` 提供 typed reconnect wait/report foundation，
  返回 recovered / exhausted / timed out / closed 等结果，不要求用户自己轮询
  session phase。
- `TqStream::spawn_commit_sink(...)` 提供 managed commit sink foundation，写库/日志
  sink 可以由 SDK 托管在独立 consumer task 中，并通过 `StreamSinkStats` /
  `StreamSinkShutdownReport` 观察 processed / lagged / errors / retry_attempts /
  wal_records 与 flush 结果。
- `TqStream::spawn_commit_sink_with_options(...)` + `StreamSinkOptions` /
  `StreamSinkRetryPolicy` 提供 per-sink 有限重试和本地 JSONL WAL foundation。
- `StreamSinkProfile` 提供 reusable sink profile，常见 JSONL WAL + commit
  journal + retry 配置不再要求用户手拼全部 options。
- `StreamSinkWalFsyncPolicy` 提供本地 WAL 每条记录 `sync_data` 策略；
  `StreamSinkWalCompaction` 提供按 revision 裁剪 JSONL WAL 的本地维护入口和
  typed report。
- `StreamSinkWalRecovery` 提供旧 WAL 的 delivered / pending / failed revision、
  lagged records 和 flush failures 扫描报告；该 report 不提供 commit payload
  重放。
- `StreamSinkOptions::jsonl_commit_journal(...)` 和 `StreamCommitJournal` 提供
  commit metadata journal 写入、读取和按 revision checkpoint 重放到 `CommitSink`
  的底层能力；该能力不恢复 runtime state snapshot。
- `TqStream::graceful_shutdown()` 提供 stream driver close + managed sink flush
  orchestration，返回 `StreamGracefulShutdownReport`，避免用户依赖 drop 隐式关闭。
- `tqsdk-task::StrategySupervisor::telemetry_reporter(...)` 暴露
  transport-neutral typed telemetry/export hook，用户可以接入 tracing、日志或外部
  指标系统，不需要 SDK 内置 HTTP endpoint。

已降级为用户运维系统职责：

- 跨进程 daemon orchestration；
- durable daemon queue、跨进程锁和 runtime state snapshot recovery；
- order/business retry audit 与业务拒单审计。

优先提升的场景：

- `api_contract_s20_production_daemon`
- `api_contract_s21_slow_consumer_isolation`（bounded fan-out/lag、managed commit sink、有限重试、JSONL WAL、fsync policy、本地 compaction、recovery report 和 commit metadata journal replay 已提升为正式 stream example）
- `api_contract_s22_error_diagnosis_retry`（low-level diagnostics 和 stream-facing retry policy 子集已提升为正式 stream example）

已落地：

- `tqsdk-stream::TqStream::health()` 已提供 typed health snapshot、health status
  和 restart hint 子集；`TqStream::reconnect_monitor()` 已提供 typed reconnect
  wait/report foundation。
- `tqsdk-task::StrategySupervisor` 已新增正式 S20 task example，覆盖 strategy
  deployment 的 typed health/metrics snapshot、显式 retry policy、ctrl-c
  shutdown signal、typed shutdown report 和 typed telemetry/export hook。

已降级为用户运维系统职责：

- durable daemon queue、跨进程锁和 runtime state snapshot recovery。
- 跨进程 daemon orchestration 和跨进程 daemon 管理。

### P2：本地行情缓存与研究闭环

服务的使用者：

- 研究用户
- 多策略共享行情用户
- 回放和测试用户

目标：

- live 行情可以写入本地缓存。
- 其他进程或策略可以读取缓存快照和增量。
- 历史数据、缓存数据和 replay driver 能接到同一策略事件模型。

建议落点：

- `tqsdk-data`：cache writer / reader / export / import。
- `tqsdk-stream`：live sink adapter。
- `tqsdk-task`：策略 replay driver 只消费标准事件。

已落地：

- `tqsdk-data::MarketCacheEvent` 定义标准 `Quote` / `Kline` / `Tick`
  cache record。
- `MarketCacheWriter` / `MarketCacheReader` 提供最薄 JSONL 离线读写
  foundation。
- `MarketCacheReplay` 提供按事件时间、接收时间排序的 deterministic
  offline replay iterator。
- `MarketCacheStreamWriter` 提供单进程 live `MarketEvent` -> cache writer
  pipe foundation，明确不承诺 durable daemon orchestration。
- `MarketCacheQueue` 提供本地 JSONL queue/spool foundation，可将 live 或
  offline cache event 先写入可重放队列，再 drain 到 cache writer。
- `MarketCacheLock` 提供原子 lock file foundation，用于单机多进程写入前的
  互斥防线；`MarketCacheLockOptions` / `MarketCacheLock::renew` 提供显式
  stale lease recovery / lease renewal foundation。
- `MarketCacheIndex` / `MarketCacheCompaction` 提供本地 cache 统计索引与
  保留策略 compaction foundation；`compact_file_in_place` 提供 in-place
  rotation foundation。
- `MarketCacheDaemonConfig` / `MarketCacheDaemon` 提供同步、process-local
  daemon foundation，覆盖 lock lease、queue flush progress、compaction
  rotation 和 shutdown report；明确不内置 HTTP endpoint 或 GUI。
- `MarketCacheSupervisorConfig` / `MarketCacheSupervisor` 提供 process-local
  background supervisor foundation，覆盖 periodic rotating flush、lease renewal
  和 graceful shutdown report；明确不承诺跨进程 cache 管理服务。
- `MarketCacheReaderManifest` / `MarketCacheReaderCheckpoint` 提供本地 reader
  checkpoint、compaction floor 和 typed reader lag report foundation；明确不承诺
  writer election 或跨进程 service facade。
- `MarketCacheRecoveryScan` 提供本地 cache / queue / processing queue /
  compaction staging recovery scan foundation；作为底层 helper 不承诺完整跨进程
  service orchestration。
- `MarketCacheWriterElection` / `MarketCacheWriterLease` 提供本地 writer
  election 和 lease ownership substrate；`MarketCacheRecoveryAction` 要求已获得
  writer lease 后恢复 processing queue / queue，明确不承诺跨进程 service facade
  或 compaction ownership。
- `MarketCacheCompactionOwnership` 提供本地 reader-protected compaction
  ownership substrate；它要求 writer lease，读取 reader manifest 的 compaction
  floor，并拒绝 reader-protected source / symbol / payload filters，明确不承诺跨进程
  service orchestration。
- `MarketCacheServiceConfig` / `MarketCacheService` 提供同步、本地 file service
  facade foundation，组合 writer election、recovery action、reader manifest、
  queue flush 和 reader-protected compaction ownership；明确不拥有 live session，
  不内置 HTTP endpoint、GUI 或系统级进程管理器，也不承诺完整跨进程 daemon
  orchestration。
- `tqsdk-task::StrategyReplay` 已消费 `MarketCacheReplay` 并推进同构
  `StrategyContext`，覆盖 cache replay -> strategy runtime foundation。
- `StrategyReplayCheckpoint` / `StrategyReplayBuilder::resume_from` 已覆盖
  S16 replay clock 与内存级 checkpoint/resume foundation。
- `StrategyReplaySpeed` / `StrategyReplayBuilder::speed` 已覆盖 S16 最小
  speed policy foundation。
- `StrategyReplayCheckpointStore` / `StrategyReplayBuilder::resume_from_store`
  已覆盖 S16 最小 durable checkpoint persistence foundation。
- `StrategyReplaySourceBuilder` 已覆盖 S16 多序列 replay convenience builder
  foundation。
- `KlineDataSeries` / `TickDataSeries` 已提供 `into_market_cache_events`
  与 `into_market_cache_replay`，覆盖 history series -> cache replay
  adapter foundation。
- `api_contract_s18_local_market_cache` 已提升为正式 data example，覆盖
  cache record / reader-writer / replay foundation。
- `api_contract_s18_cache_maintenance` 已提升为正式 data example，覆盖
  queue / lock / index / compaction foundation。
- `api_contract_s18_cache_daemon_foundation` 已提升为正式 data example，
  覆盖 lease / queue / rotation / shutdown report foundation。
- `api_contract_s18_cache_supervisor_foundation` 已提升为正式 data example，
  覆盖 process-local periodic flush / lease renewal / graceful shutdown
  foundation。
- `api_contract_s18_cache_reader_manifest` 已提升为正式 data example，覆盖
  reader checkpoint / compaction floor / reader lag report foundation。
- `api_contract_s18_cache_recovery_scan` 已提升为正式 data example，覆盖
  cache / queue / processing queue / compaction staging recovery scan
  foundation。
- `api_contract_s18_cache_writer_recovery` 已提升为正式 data example，覆盖
  writer election / lease ownership / recovery action foundation。
- `api_contract_s18_cache_compaction_ownership` 已提升为正式 data example，覆盖
  reader-protected compaction ownership foundation。
- `api_contract_s18_cross_process_cache_service` 已补充为 desired API sketch，
  明确剩余跨进程 service facade 不应直接扩展 core/session。

已降级为用户层工具或独立项目职责：

- 完整跨进程 daemon orchestration / 多进程 cache 管理服务实现；

优先提升的场景：

- `api_contract_s18_local_market_cache`（cache record/replay foundation 已提升为正式 data example）
- `api_contract_s18_cache_maintenance`（cache maintenance foundation 已提升为正式 data example）
- `api_contract_s18_cache_daemon_foundation`（process-local daemon foundation 已提升为正式 data example）
- `api_contract_s18_cache_supervisor_foundation`（process-local supervisor foundation 已提升为正式 data example）
- `api_contract_s18_cache_reader_manifest`（reader manifest foundation 已提升为正式 data example）
- `api_contract_s18_cache_recovery_scan`（recovery scan foundation 已提升为正式 data example）
- `api_contract_s18_cache_writer_recovery`（writer election/recovery action foundation 已提升为正式 data example）
- `api_contract_s18_cache_compaction_ownership`（reader-protected compaction ownership foundation 已提升为正式 data example）
- `api_contract_s18_cache_service_foundation`（本地 file service foundation 已提升为正式 data example）
- `api_contract_s18_cross_process_cache_service`（desired API sketch 已补齐；暂停
  作为核心 SDK 目标，后续仅在明确用户层工具需求下重新评估）
- `api_contract_s16_history_replay_strategy`
- `api_contract_s17_research_kline_batch`
- `api_contract_s28_download_export` 与 `api_contract_s28_option_greeks`（新增）：
  覆盖历史主连、下载进度、CSV materialization 和 Greeks research query，确认这些
  能力不进入 session/wait/stream。

### P3：多 provider 行情聚合（暂缓）

服务的使用者：

- 基础设施用户
- 低延迟或高可用行情系统

目标：

- 标准化 provider id、source timestamp、接收 timestamp、质量状态。
- 明确冲突合并策略和 provider 级健康状态。
- 不影响单 provider 用户的简单路径。

边界判断：

- 官方 Python SDK 没有多行情源聚合作为核心 public API。
- 该能力更像用户层基础设施或行情中台，不是核心交易 SDK 的默认职责。
- 当前保持 desired API sketch，不继续拆 public API 或 crate。

建议落点：

- 仅在未来有明确用户需求时，作为 `tqsdk-stream` 之上的独立用户层 facade
  或独立项目重新评估。
- 不下沉到 `tqsdk-core` / `tqsdk-session`，也不作为近期场景驱动批次目标。

优先提升的场景：

- `api_contract_s14_multi_provider_market_aggregation`

## 每个场景后续要补的元信息

后续新增或提升 `api_contract_sXX_*.rs` 时，除原有文件头模板外，建议增加：

- Primary user layer：该示例主要服务哪类用户。
- Intended crate path：推荐使用哪些 crate 组合。
- Lower-level escape hatch：高级用户是否可以用更低层 API 完成。
- Non-goal：明确本层不承诺什么。

这样 review 的重点会从“像不像 Python API”转向“是否满足该类 Rust 用户的合理路径”。

## 验收原则

每次修复 gap 时必须同时回答：

- 这个能力服务哪类使用者？
- 是否放在了该使用者应使用的 crate 层？
- 是否把更低层用户不需要的抽象强加给了他们？
- 是否让高层用户暴露了 provider protocol、runtime command 或手写异步编排？
- 是否维持单一 runtime commit / revision / command lifecycle？
- 是否能把对应 gap sketch 提升为正式 example？
- 新增或提升场景 example 后运行 `scripts/check_api_contract_examples.sh`，
  确认正式 examples 和 gap sketches 都保留完整场景契约头。

如果答案不清楚，应先补文档和 example sketch，再写实现。
