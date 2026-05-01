# 场景驱动 Public API 设计审查

本文档审查 `api_contract_sXX_*.rs` 所表达的终端用户场景。正式
`crates/*/examples/api_contract_sXX_*.rs` 文件是 public API 契约样本：能自然表达的场景使用当前
public API 写成可编译示例，并纳入 `cargo check --workspace --examples` 与 CI。

不能自然表达的场景只保留理想用户代码草案，放在
`docs/scenarios/api_gaps/`，并显式标记 API gap；这些 sketch 不参与 Cargo
example 自动发现，不用底层绕路代码伪装通过。

本报告记录的 public surface 位于 `tqsdk-session` / `tqsdk-wait` /
`tqsdk-stream` / `tqsdk-task` / `tqsdk-data` facade：它们继续复用既有 session、
runtime commit、reader/cursor 与 domain partition 读面；未改变 crate 边界或
runtime contract。场景设计审查只判断用户 API 表达能力，不把非核心平台能力
伪装成 SDK 缺口。

本报告中的“当前 API 是否自然表达”应按 Rust 分层使用者判断，而不是按官方
Python SDK 的 public API 名称判断。Python SDK 提供成熟用户语义证据；Rust
版本通过 `core/session/wait/stream/task/data` 分层服务不同用户。后续 gap
修复顺序见 [`../scenarios/user-layer-iteration-plan.md`](../scenarios/user-layer-iteration-plan.md)。

## 核心能力边界

截至 2026-05-01，再次核对官方 `tqsdk-python` 后，本仓库的 public API
边界按以下规则冻结：

- `tqsdk-rust` 是核心交易 SDK，不是策略平台、生产守护平台、行情中台或自动
  执行系统。
- 可作为核心能力继续维护的范围是：行情订阅/快照/序列、下单/撤单/账户/持仓/
  委托/成交、`wait_update` 风格稳定截面、stream fan-out、重连/恢复、基础
  目标持仓、显式多账户归属、基础本地风控、回测/回放/历史数据、研究批处理、
  本地离线 cache、typed health/telemetry hook、慢消费者隔离、错误诊断、合约
  标准化和最小 fake market / fake broker 测试支持。
- 不进入核心 SDK 的范围是：多 provider 行情聚合、跨进程 cache service、
  分布式或生产级 daemon、内置 HTTP health/metrics endpoint、GUI/web helper、
  自动 hedge / flatten / 补单引擎、自动资产配置、多账户失败补偿系统、组合
  保证金引擎、全局风控服务、durable audit/resume 平台。
- 高级能力可以保留 desired API sketch 或用户层示例；除非能证明它是官方 Python
  核心工作流的直接语义等价物，或是 Rust 分层必需的薄基础设施，否则不得提升为
  正式 `examples/*.rs` 契约。

后续新增 public API 必须先回答：该能力服务哪个使用者层、是否落在正确 crate、
是否避免 provider protocol / 手写 task / channel / `Arc<Mutex<_>>` 泄漏、是否
维持单一 runtime commit / revision、是否属于 SDK 核心而不是用户策略或运维系统。

## 边界复核口径

本报告按 [`../scenarios/user-layer-iteration-plan.md`](../scenarios/user-layer-iteration-plan.md)
约定的使用者分层复核场景，而不是按 Python SDK 方法名逐项扩张 Rust API：

- `tqsdk-wait` 覆盖单策略作者的 `wait_update()`、稳定截面、live ref 和交易一致性。
- `tqsdk-stream` 覆盖 async 系统集成方的多消费者、背压、错误诊断、健康状态和 sink isolation。
- `tqsdk-session` 覆盖低层 / 高频用户和 direct-query 用户的一次性 request/response、metadata、calendar、settlement、ranking、EDB、auth 和 replay control-plane。
- `tqsdk-task` 覆盖执行工具用户的目标持仓、订单 intent、ownership、基础风控、多账户隔离、策略运行时和测试支持。
- `tqsdk-data` 覆盖研究 / 数据用户的历史数据、批处理、下载、CSV、Greeks、本地 cache 和 replay 数据源。

因此，下面这些能力虽然仍可保留 desired API sketch，但不再作为核心 SDK 缺口推动：

- S12 自动 hedge / flatten / timed cancel / replace / 补单执行引擎；
- S13 自动资产配置、多账户失败补偿、跨账户 TargetPos 编排和 durable audit；
- S14 多 provider 行情聚合；
- S15 多 provider environment 和平台化 deployment config；
- S16 生产级 daemon reconnect orchestration；
- S18 跨进程 cache service / cache daemon 管理；
- S19 组合保证金引擎、全局风控服务、风控热更新和 durable audit；
- S20 内置 HTTP health/metrics endpoint、GUI、web helper、进程管理器；
- S21 durable distributed queue 和 runtime state snapshot recovery 平台；
- S24 完整仿真交易所或生产级测试 fixture 持久恢复。

同时，下面这些属于核心 SDK 能力边界内的“场景契约覆盖不足”，不是底层能力一定缺失。
后续应优先补正式 `api_contract_sXX_*.rs` 或把已有普通 example 提升为 contract：

- `tqsdk-wait`：wait 风格 `get_trading_status`、`get_kline_serial`、`get_tick_serial`
  场景已补正式 S25 contract；这些属于单策略作者的稳定截面和行情序列心智。
- `tqsdk-wait`：`NotificationRef`、`SettlementInfoRef`、`RiskManagementRuleRef`、
  `RiskManagementDataRef` 与 `confirm_settlement` 场景已补正式 S26 trade/system
  contract；证券账户 / 持仓 / 委托 / 成交 ref 已拆为正式 S26 security trade
  contract。这两组都属于 live trade/system refs 的覆盖完整性。
- `tqsdk-session`：`query_quotes`、`query_cont_quotes`、options、calendar、
  settlement、ranking、EDB 的 direct-query pack 场景已补正式 S27 contract；
  这些属于一次性 metadata/service query，不应下沉到 wait/stream。
- `tqsdk-data`：`query_his_cont_quotes`、tick/K线 download、CSV export、
  `query_option_greeks` 场景已补正式 S28 contract；这些属于研究 / 离线数据层，
  不应进入 session/wait。
- `tqsdk-task`：独立 `TargetPosTask` / scheduler ownership 场景已补正式 S29
  contract；S11 仍展示策略内复用，S29 单独确认目标持仓 ownership 与手动下单 guard。

## 批次状态

截至 2026-05-01，近期场景驱动批次已经完成并验证：

- S12 跨合约套利的核心 execution-group foundation 已能自然表达：用户可以通过
  `TaskHost::execution_group(...)` 表达两腿 typed 下单、全腿 preflight、
  session-scoped retry idempotency、observed `max_unhedged` exposure timeout 和
  group outcome / exposure report；`ExecutionGroupTicket::report(...)` 返回
  revision-bound `ExecutionGroupReport`，用户可以在同一 runtime revision 上审计
  group status 与各腿状态。自动 hedge / flatten、timed cancel / replace、
  group resume / persistent audit log 属于用户策略 / 执行系统扩展，不再作为核心
  SDK 近期目标；历史 gap sketch 已归档到
  `docs/archive/scenarios/2026-05-02/api_contract_s12_spread_arbitrage.rs`。
- S13 多账户下单的核心 foundation 已落地：用户现在可以通过
  `TaskHost::account_group()` / `TaskHost::multi_account_order(...)` 表达 typed
  account group、比例拆单、全账户 preflight、session-scoped retry idempotency
  和 per-account outcome report；账户间裸露持续超过 `max_unhedged` 后返回 typed
  `NeedsAttention`；`MultiAccountOrderTicket::report(...)` 返回 revision-bound
  `MultiAccountOrderGroupReport`，用户可以在同一 runtime revision 上审计账户组状态。
  自动资产配置、多账户失败补偿、跨账户 TargetPos 编排和 durable audit 属于
  用户层执行系统，不再作为核心 SDK 近期目标。
- S11 简单策略保持自然表达：用户现在可以通过
  `StrategyHost` / `StrategyContext` 在同一稳定 task/wait 推进点内读取
  quote/account/position，并复用 `TaskHost::orders(...)`、`RiskEngine` 和
  `TargetPosTask` 表达入场与止盈止损平仓。
- S24 最小可测试策略的核心 foundation 已可自然表达：`tqsdk-task::testing`
  提供 public `StrategyTestHarness`、`FakeMarket`、`FakeBroker` 和
  `StrategyTestClock`，测试代码不再需要 hidden `*_for_test` API、runtime
  handle、channel 或 provider protocol；fake broker 已支持全成、拒单、单步/跨 step
  部分成交、deterministic clock、step latency 和 disconnect/reconnect 注入；
  public test contract 断言 `OrderLifecycle`，不再依赖 raw `"ALIVE"` /
  `"FINISHED"` 状态字符串；完整仿真交易所和生产级 fixture 持久恢复不进入核心 SDK。
- S19 基础风控前置已进入核心自然表达范围：`RiskEngine::check_report(...)` 返回
  revision-bound `RiskCheckReport`，风控检查可在同一 runtime snapshot 中读取账户、
  持仓与 quote，并通过 `RiskDecision` / `RiskRejection` 输出 typed 审计信息；
  `RiskEngine::project_order(...)` 返回 revision-bound `RiskProjectionReport`，
  提供当前净持仓、投影净持仓、轻量 price-volume estimate、合约乘数和
  notional estimate；`RiskEngine::instrument_specs(...)` 可接入
  `tqsdk_session::InstrumentSpec` 做 tick size 校验和合约乘数试算 foundation；
  `daily_open_count_limit(...)`、`daily_open_volume_limit(...)`、
  `accumulated_open_volume_limit(...)` 和 `order_rate_limit_per_second(...)`
  对齐官方 Python SDK 的基础开仓/频率规则形态，并由 `TaskHost` 在成功报单或
  guarded 撤单后记录本进程内用量；
  组合级保证金 what-if、涨跌停/品种级规则、风控热更新和 durable audit 不作为
  核心 SDK 近期目标。
- S16 历史行情回放 foundation 已进入核心自然表达范围：`tqsdk-task::StrategyReplay`
  已能消费 `tqsdk-data::MarketCacheReplay` 的有序 quote/kline/tick cache
  event，并复用 `StrategyContext`、typed order builder 和 fake broker；
  `KlineDataSeries` / `TickDataSeries` 也已提供 history series -> cache replay
  adapter；`StrategyReplay` 已提供 deterministic replay clock、checkpoint 与
  `resume_from` foundation，并通过 `StrategyReplaySpeed` 提供最快 / real-time /
  scaled replay speed policy；`StrategyReplayCheckpointStore` 已提供 JSON file-backed
  durable checkpoint persistence foundation；`StrategyReplaySourceBuilder` 已提供
  多序列 event source 合并入口。完整 daemon reconnect orchestration 属于生产
  运维/用户层工具，不作为核心 SDK 近期目标。
- S25/S26 的 wait 契约已补齐：wait 风格 trading status、K 线 serial、tick serial、
  notification、settlement、risk management 已由
  `api_contract_s26_trade_system_refs` 覆盖，证券 account/position/order/trade
  live refs 已由 `api_contract_s26_security_trade_refs` 覆盖，`confirm_settlement`
  仍保留在 wait trade command wrapper 边界内。
- S18 本地行情缓存 foundation 已进入核心自然表达范围：`MarketCacheWriter` /
  `MarketCacheReader` / `MarketCacheReplay` 已覆盖离线 cache record、JSONL
  reader/writer 和 ordered replay；`MarketCacheStreamWriter` 已提供单进程 live
  `MarketEvent` -> cache writer pipe foundation；`MarketCacheQueue` /
  `MarketCacheLock` / `MarketCacheIndex` / `MarketCacheCompaction` 已提供本地
  JSONL queue、lock lease、索引、保留策略 compaction 与 in-place rotation
  foundation；`MarketCacheDaemon` 已提供 process-local cache daemon foundation，
  覆盖 stale lock recovery、queue flush progress 和 shutdown report；
  `MarketCacheSupervisor` 已提供 process-local background supervisor
  foundation，覆盖 periodic rotating flush、lease renewal 与 graceful shutdown
  report；`MarketCacheReaderManifest` 已提供本地 reader checkpoint、compaction
  floor 与 reader lag report foundation；`MarketCacheRecoveryScan` 已提供本地
  cache / queue / processing queue / compaction staging recovery scan foundation；
  `MarketCacheWriterElection` / `MarketCacheWriterLease` 已提供 typed writer
  election / lease ownership substrate；`MarketCacheRecoveryAction` 要求 writer
  lease 后恢复 processing queue / queue；`MarketCacheCompactionOwnership` 已提供
  reader-protected compaction ownership foundation，会结合 reader manifest floor
  和 writer lease 运行 atomic compaction；`MarketCacheService` 已提供同步、
  本地 file service facade foundation，组合 writer election、recovery、reader
  checkpoint、queue flush 和 reader-protected compaction；完整跨进程 daemon
  orchestration 已降级为非核心用户层/工具层能力，暂停继续作为 S18 核心目标推进。
- S15 实盘 / 模拟 / 回放切换的核心 deployment/lifecycle 子集已进入自然表达范围。
  `StrategyDeploymentConfig` 支持 live trade
  与 TQKQ sim provider 配置，`StrategyDeployment` / `StrategyLifecycle` 提供统一
  run loop、typed stop reason 和 graceful shutdown report；`StrategySupervisor` /
  `StrategyRetryPolicy` / `StrategyShutdownSignal` 提供 task-layer supervisor、
  typed health/metrics snapshot、有限 retry 和 ctrl-c shutdown hook；策略步骤仍只依赖
  `StrategyEnvironmentContext`。配置文件反序列化可作为薄便利能力评估；多 provider
  environment 已降级为非核心能力，随 S14 暂停。
- S5 低层裸行情直通继续保持“自然”：低层用户可以用
  `SessionClient::subscribe_quotes(...)` 减少 raw `RuntimeCommand` 样板，同时仍通过
  `RuntimeReader::read_market_state()` 走热路径分区读面。
- S20 生产运行所需的 SDK primitive 已进入核心自然表达范围：`TqStream::health()` 现在返回
  `StreamHealthSnapshot::status()` / `should_restart()`，`TqStream::reconnect_monitor()`
  可等待并报告 existing session reconnect 的 recovered / exhausted / timed out /
  closed outcome；`tqsdk-task` 新增
  `StrategySupervisor` foundation，提供 typed health/metrics snapshot、显式
  retry policy、ctrl-c shutdown signal、typed shutdown report 和稳定 typed
  telemetry/export hook；`tqsdk-stream` 已提供 managed commit sink、有限重试和
  JSONL WAL foundation，并提供 `TqStream::graceful_shutdown()` 做 stream driver
  关闭与 managed sink flush 的 typed report。
  S20 完成标准不包含 Rust GUI、web helper、内置 HTTP health/metrics endpoint、
  跨进程 daemon orchestration 或跨进程 daemon 管理；后两者已降级为用户层运维
  系统职责。
- S21 慢消费者隔离的 bounded fan-out / lag 诊断 / sink policy 子集已经自然表达：`TqStreamBuilder`
  可配置 root fan-out capacity，`StreamFacadeError::diagnostic()` 暴露 typed
  lag 诊断；`TqStream::spawn_commit_sink(...)` / `spawn_commit_sink_with_options(...)`
  已提供 managed commit sink、有限重试、JSONL WAL、typed stats 和 shutdown
  flush report、WAL fsync policy、本地 JSONL compaction、WAL recovery report
  和 commit metadata journal replay；`StreamSinkProfile` 提供常见 sink 配置
  profile，减少用户手拼 WAL/retry/journal options；完整 durable daemon queue
  与 runtime state snapshot 恢复已降级为用户运维系统职责。
- S22 错误诊断与重试的 error diagnostic / retry hint / stream retry policy 子集已经自然表达：core/session/stream
  均有 typed error kind 和 retry hint；`StreamRetryPolicy` 提供 stream-facing
  retry decision / backoff runner；业务拒单仍应通过订单/风控 public API 判断，
  order/business retry audit 属于用户执行审计系统职责。
- S23 合约信息查询与标准化继续保持“自然”：`SessionClient::query_instrument_specs`
  返回 `InstrumentSpec`，用户不再把 live `Quote` 当作合约规格对象。
- S27 Session metadata 与 service query pack 继续保持“自然”：`SessionClient`
  直接提供合约列表、主连、期权、交易日历、结算价、排名和 EDB 的 typed
  one-shot request/response；raw GraphQL 仍只是 `SessionRawQuery::query_graphql_value`
  低层逃生舱，direct query 不复制到 wait/stream。
- S29 TargetPosTask ownership 已补正式 task contract：`TaskHost::target_pos(...)`
  / `TargetPosTask` 覆盖同账户同合约 owner 注册、重复 owner 拒绝、手动下单
  guard 和 command-level execution events；`TaskHost::target_pos_scheduler(...)` /
  `TargetPosScheduler` 覆盖 scheduler ownership，并继续由 `TaskHost::wait_update()`
  统一推进。跨账户 TargetPos 编排、自动 hedge / flatten / 补单和 durable audit
  仍保持在用户层执行系统之外，不进入核心 SDK。
- S30 看盘软件历史序列缓存仍无法自然表达：`tqsdk-data` 已有 history
  page/series/download、CSV export、JSONL `MarketCache*` event cache 和
  history series -> replay adapter，但没有 typed history series range cache、
  manifest、schema version、缺口下载、mutable tail refresh 与损坏恢复 contract。
  该能力服务看盘软件和研究/回放用户，应作为 `tqsdk-data` 显式 opt-in
  materialization/cache foundation 重新评估；mmap/memmap 只是 backend 选择，不应
  提前冻结为 public contract。
- S31 高频交易柜台低延迟 profile 仍无法自然表达为单一 contract：S5/S6/S7/S10/S19/S21
  分别覆盖裸行情、下单、订单一致性、风控和慢消费者隔离，但还没有一个示例把
  market/trade partition hot read、typed risk gate、order intent、latency report
  和 slow sink isolation 放在同一条低延迟链路里。该 profile 不应依赖
  `tqsdk-data` 或历史序列缓存。

| 场景 | 当前 API 表达能力 | 样板代码量 | 内部细节泄漏 | 手动异步管理 | 状态一致性风险 | 热路径性能风险 | 建议处理方式 | 证据位置 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1. 零门槛行情订阅 | 自然 | 低 | 无 | 无 | 低 | 无 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s01_zero_barrier_quote.rs`; `tqsdk_wait::TqApi::{get_quote, wait_update, is_changing}` |
| 2. 多合约动态订阅 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-stream/examples/api_contract_s02_dynamic_subscriptions.rs`; `tqsdk_stream::TqStream::quotes`; `QuoteSubscription::{add, remove, symbols}`; `runtime_contract_session_reconnect::session_runtime_recovery_requeues_market_subscription_intent` |
| 3. 行情快照读取 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s03_quote_snapshot.rs`; `tqsdk_wait::TqApi::quote_snapshot` |
| 4. Tick / Quote / K线混合订阅 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-stream/examples/api_contract_s04_mixed_market_streams.rs`; `TqStream::market_events`; `MarketEventStream`; `MarketEvent` |
| 5. 高频裸行情直通 | 自然 | 中 | 少量 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-session/examples/api_contract_s05_bare_market_fast_path.rs`; `SessionClient::{subscribe_quotes, progress_once}`; `RuntimeReader::read_market_state` |
| 6. 普通限价下单 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s06_limit_order.rs`; `TqApi::{login_trade_account, limit_order}`; `LimitOrderIntent::send_once`; `OrderTicket::wait_terminal` |
| 7. 撤单与部分成交 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s07_cancel_partial_fill.rs`; `OrderTicket::{wait_partially_filled, cancel_remaining, wait_terminal}`; `OrderRef::{wait_partially_filled, cancel_remaining, wait_terminal}`; `Order.lifecycle`; `Order.volume_left` |
| 8. 账户 / 资金 / 持仓查询 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s08_account_position_updates.rs`; `TqApi::{login_trade_account, get_account, get_position}` |
| 9. 启动后状态恢复 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s09_startup_state_recovery.rs`; `tqsdk_session::StartupRecoverySpec`; `TqApi::startup_recovery`; `TqStream::recover_state` |
| 10. 断线重连中的订单一致性 | 自然 | 中 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s10_reconnect_order_consistency.rs`; `tqsdk_session::OrderIntentRecord`; `TqApi::limit_order`; `OrderTicket`; `OrderTicketState`; `OrderTicket::{wait_partially_filled,cancel_remaining}`; session-scoped reconnect is covered, cross-process persistence remains out of scope |
| 11. 简单策略 | 自然 | 中 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-task/examples/api_contract_s11_simple_strategy.rs`; `StrategyHost`; `StrategyContext`; `TaskHost::orders`; `RiskEngine`; `TargetPosTask` |
| 12. 跨合约套利 | 自然（核心 foundation） | 中 | 无 | 无 | 中 | 中 | 维护边界 | `crates/tqsdk-task/examples/api_contract_s12_spread_arbitrage.rs`; `docs/archive/scenarios/2026-05-02/api_contract_s12_spread_arbitrage.rs`; `ExecutionGroupBuilder`; `ExecutionGroupOutcome`; `ExecutionGroupReport`; revision-bound group report; observed `max_unhedged` exposure timeout; automatic hedge / flatten / 补单引擎不进入核心 SDK，历史 sketch 仅作为非核心用户层执行系统上下文 |
| 13. 多账户下单 | 自然（核心 foundation） | 中 | 无 | 无 | 中 | 中 | 维护边界 | `crates/tqsdk-task/examples/api_contract_s13_multi_account_ordering.rs`; `docs/archive/scenarios/2026-05-02/api_contract_s13_multi_account_ordering.rs`; `AccountGroup`; `MultiAccountOrderTicket`; `MultiAccountOrderGroupReport`; revision-bound account group report; observed `max_unhedged` account exposure timeout; advanced failure policy/resume/audit 不进入核心 SDK，历史 sketch 仅作为非核心用户层执行系统上下文 |
| 14. 多 provider 行情聚合 | 无法表达 | 高 | 严重 | 严重 | 中 | 高 | 暂缓 | `docs/scenarios/api_gaps/api_contract_s14_multi_provider_market_aggregation.rs`; no public provider aggregation facade; 官方 Python 无对应核心 API，非近期核心目标 |
| 15. 实盘 / 模拟 / 回放切换 | 自然（核心 lifecycle） | 中 | 无 | 无 | 低 | 低 | 维护边界 | `crates/tqsdk-task/examples/api_contract_s15_live_sim_replay_switch.rs`; `docs/archive/scenarios/2026-05-02/api_contract_s15_live_sim_replay_switch.rs`; `StrategyEnvironment`; `StrategyEnvironmentContext`; `StrategyDeploymentConfig`; `StrategyDeployment`; `StrategyLifecycle`; `StrategySupervisor`; `StrategyRetryPolicy`; `StrategyShutdownSignal`; `StrategyEnvironment::from_config`; `StrategyEnvironment::{from_task_host,from_test_harness,from_replay_builder}`; config loader 可评估为薄便利能力；multi-provider environment 随 S14 暂缓，历史 sketch 仅作为非核心部署平台上下文 |
| 16. 历史行情回放 | 自然（核心 replay foundation） | 中 | 无 | 无 | 低 | 中 | 维护边界 | `crates/tqsdk-task/examples/api_contract_s16_history_replay_strategy.rs`; `docs/archive/scenarios/2026-05-02/api_contract_s16_history_replay_strategy.rs`; `KlineDataSeries::into_market_cache_events`; `TickDataSeries::into_market_cache_events`; `StrategyReplay`; `StrategyReplaySourceBuilder`; `StrategyReplayCheckpoint`; `StrategyReplaySpeed`; `StrategyReplayCheckpointStore`; `StrategyReplayBuilder::{resume_from,resume_from_store,speed}`; daemon reconnect orchestration 不进入核心 SDK，历史 sketch 仅作为非核心运维上下文 |
| 17. 研究场景 | 自然 | 低 | 无 | 无 | 无 | 低 | API 微调 | `crates/tqsdk-data/examples/api_contract_s17_research_kline_batch.rs`; `DataClient::get_kline_data_series` |
| 18. 本地行情缓存读写 | 自然（核心 file/cache foundation） | 中 | 无 | 无 | 低 | 中 | 维护边界 | `crates/tqsdk-data/examples/api_contract_s18_local_market_cache.rs`; `crates/tqsdk-data/examples/api_contract_s18_cache_maintenance.rs`; `crates/tqsdk-data/examples/api_contract_s18_cache_daemon_foundation.rs`; `crates/tqsdk-data/examples/api_contract_s18_cache_supervisor_foundation.rs`; `crates/tqsdk-data/examples/api_contract_s18_cache_reader_manifest.rs`; `crates/tqsdk-data/examples/api_contract_s18_cache_recovery_scan.rs`; `crates/tqsdk-data/examples/api_contract_s18_cache_writer_recovery.rs`; `crates/tqsdk-data/examples/api_contract_s18_cache_compaction_ownership.rs`; `crates/tqsdk-data/examples/api_contract_s18_cache_service_foundation.rs`; `crates/tqsdk-data/examples/api_contract_s18_live_market_cache_pipe.rs`; `docs/archive/scenarios/2026-05-02/api_contract_s18_local_market_cache.rs`; `docs/scenarios/api_gaps/api_contract_s18_cross_process_cache_service.rs`; `MarketCacheWriter`; `MarketCacheReader`; `MarketCacheReplay`; `MarketCacheReaderManifest`; `MarketCacheRecoveryScan`; `MarketCacheWriterElection`; `MarketCacheWriterLease`; `MarketCacheRecoveryAction`; `MarketCacheCompactionOwnership`; `MarketCacheService`; `MarketCacheStreamWriter`; `MarketCacheQueue`; `MarketCacheLock`; `MarketCacheIndex`; `MarketCacheCompaction`; `MarketCacheDaemon`; `MarketCacheSupervisor`; local file/cache foundation 足够；cross-process cache service 继续保留为 active non-core desired sketch |
| 19. 风控前置 | 自然（基础风控） | 中 | 无 | 无 | 低 | 低 | 维护边界 | `crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs`; `docs/archive/scenarios/2026-05-02/api_contract_s19_pre_trade_risk.rs`; `RiskEngine`; `RiskCheckReport`; `RiskProjectionReport`; `RiskDecision`; `RiskRejection`; `TaskHost::orders`; `InstrumentSpec`; guarded insert/cancel risk integration; daily open count / symbol open volume / accumulated open volume / order rate limit; tick-size validation; lightweight single-order projection; portfolio margin what-if / durable audit 不进入核心 SDK，历史 sketch 仅作为非核心风控系统上下文 |
| 20. 生产守护进程 | 自然（SDK runtime primitives） | 中 | 无 | 无 | 中 | 低 | 维护边界 | `crates/tqsdk-stream/examples/api_contract_s20_production_daemon_health.rs`; `crates/tqsdk-task/examples/api_contract_s20_strategy_supervisor.rs`; `docs/archive/scenarios/2026-05-02/api_contract_s20_production_daemon.rs`; `TqStream::health`; `TqStream::reconnect_monitor`; `TqStream::graceful_shutdown`; `StreamHealthSnapshot::{status, should_restart}`; `StreamReconnectMonitor`; `StreamReconnectOutcome`; `StreamReconnectReport`; `StreamGracefulShutdownReport`; `StrategySupervisor`; `StrategySupervisorHealth`; `StrategySupervisorMetrics`; `StrategyTelemetryEvent`; `StrategyTelemetryReporter`; `StrategyRetryPolicy`; `StrategyShutdownSignal`; S20 完成标准止于 typed health / telemetry / graceful shutdown primitives；Rust GUI、HTTP endpoint 和跨进程 daemon 管理均 out of scope |
| 21. 慢消费者隔离 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-stream/examples/api_contract_s21_slow_consumer_isolation.rs`; `docs/archive/scenarios/2026-05-01/api_contract_s21_slow_consumer_isolation.rs`; `TqStream::spawn_commit_sink`; `TqStream::spawn_commit_sink_with_options`; `CommitSink`; `StreamSinkOptions`; `StreamSinkProfile`; `StreamSinkRetryPolicy`; `StreamSinkHandle`; `StreamSinkStats`; `StreamSinkShutdownReport`; `StreamSinkWalRecord`; `StreamSinkWalFsyncPolicy`; `StreamSinkWalCompaction`; `StreamSinkWalRecovery`; `StreamCommitJournal`; bounded fan-out / typed lag diagnostic / managed commit sink / finite retry / JSONL WAL / reusable sink profile / fsync policy / local compaction / recovery report / commit metadata journal replay 自然；durable distributed queue 和 runtime state snapshot recovery 不进入核心 SDK |
| 22. 错误诊断与重试 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-stream/examples/api_contract_s22_error_diagnosis_retry.rs`; `docs/archive/scenarios/2026-05-01/api_contract_s22_error_diagnosis_retry.rs`; `StreamFacadeError::diagnostic`; `StreamRetryPolicy`; `StreamRetryDecision`; error kind / retry hint / stream-facing retry decision / backoff runner 自然；order/business retry audit 由用户层执行审计系统实现 |
| 23. 合约信息查询与标准化 | 自然 | 低 | 无 | 无 | 无 | 无 | API 微调 | `crates/tqsdk-session/examples/api_contract_s23_contract_metadata.rs`; `SessionClient::query_instrument_specs`; `InstrumentSpec`; `InstrumentClass` |
| 24. 最小可测试策略 | 自然（核心 test foundation） | 中 | 无 | 无 | 低 | 低 | 维护边界 | `crates/tqsdk-task/examples/api_contract_s24_testable_strategy.rs`; `docs/archive/scenarios/2026-05-02/api_contract_s24_testable_strategy.rs`; `StrategyTestHarness`; `FakeMarket`; `FakeBroker`; `StrategyTestClock`; `OrderLifecycle`; `FakeBroker::partial_fills`; `FakeBroker::latency_steps`; `FakeBroker::disconnect_for_steps`; `FakeBrokerConnectionStatus`; durable fixtures and richer broker behavior remain non-core testing-tooling scope |
| 25. Wait 行情序列与交易状态 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s25_wait_serial_trading_status.rs`; `TqApi::{get_trading_status,get_kline_serial,get_tick_serial,wait_update,is_changing,is_changing_fields}`; 实时序列窗口属于 wait，不属于 data download 或 session direct query |
| 26. Wait trade 与 system live refs | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s26_trade_system_refs.rs`; `crates/tqsdk-wait/examples/api_contract_s26_security_trade_refs.rs`; `TqApi::{get_notification,get_settlement_info,get_risk_management_rule,get_risk_management_data,get_security_account,get_security_position,get_security_order,get_security_trade,confirm_settlement}`; notification / settlement / risk 与证券 trade refs 继续归属 wait live refs |
| 27. Session metadata 与 service query pack | 自然 | 低 | 无 | 无 | 无 | 无 | API 微调 | `crates/tqsdk-session/examples/api_contract_s27_metadata_service_queries.rs`; `SessionClient::{query_quotes,query_cont_quotes,query_options,query_atm_options,query_all_level_options,query_all_level_finance_options,get_trading_calendar,query_symbol_settlement,query_symbol_ranking,query_edb_data}`; direct query 继续归属 session |
| 28. Data 下载 / 导出 / Greeks | 自然 | 低 | 无 | 无 | 无 | 低 | API 微调 | `crates/tqsdk-data/examples/api_contract_s28_download_export.rs`; `crates/tqsdk-data/examples/api_contract_s28_option_greeks.rs`; `DataClient::{query_his_cont_quotes,kline_data_download,tick_data_download,export_kline_data_csv,export_tick_data_csv,query_option_greeks}`; research/download/Greeks 继续归属 data |
| 29. TargetPosTask ownership | 自然 | 中 | 无 | 无 | 中 | 低 | 维护边界 | `crates/tqsdk-task/examples/api_contract_s29_target_pos_ownership.rs`; `TaskHost::{target_pos,target_pos_scheduler,check_manual_order_allowed,wait_update}`; `TargetPosTask`; `TargetPosScheduler`; 同账户同合约 ownership 属于 task，跨账户 TargetPos 编排和 durable audit 不进入核心 SDK |
| 30. 看盘软件历史序列缓存 | 无法表达 | 高 | 中 | 中 | 中 | 低 | 补场景后设计 | `docs/scenarios/api_gaps/api_contract_s30_history_series_cache.rs`; `tqsdk-data` 已有 history series/download 和 JSONL market cache foundation，但缺少 typed history series range cache、manifest、schema version、缺口下载、mutable tail refresh 与损坏恢复；mmap/memmap 仅作为后续 backend 评估 |
| 31. 高频交易柜台低延迟 profile | 无法表达（跨 primitive 契约不足） | 中 | 中 | 中 | 中 | 中 | 补场景后设计 | `docs/scenarios/api_gaps/api_contract_s31_low_latency_trading_desk.rs`; S5/S6/S7/S10/S19/S21 的 primitive 分散存在，但缺少同一低延迟链路 contract；hot path 应保持在 core/session/task/stream，不进入 data/history cache |

## 主要结论

1. 按当前 SDK 边界，S1-S25 中除 S14 外都已经有核心 SDK 路径或 foundation
   可表达；历史上被归为“勉强”的场景，不应再解释为 core SDK 必须补平台能力，
   而应解释为已有 foundation 之上的用户层系统能力未承诺。
2. 当前补齐的是核心能力的契约覆盖，而不是继续扩大能力边界：wait serial /
   trading-status 已由 S25 补正式 contract；wait trade/system live refs 已由
   S26 拆分为 trade/system 与 security trade 两个正式 contract；session
   metadata/service query pack 已由 S27 补正式 contract；data
   download/export/Greeks 已由 S28 补正式 contract；独立 TargetPosTask /
   scheduler ownership 已由 S29 补正式 contract。
3. 交易相关核心能力已经覆盖普通登录、限价单、部分成交撤单、
   session-scoped reconnect-safe order intent、基础前置风控、官方同类基础开仓 /
   频率限额、revision-bound execution/risk report、轻量单笔 what-if projection、
   execution group foundation、account group foundation、TargetPosTask 和最小
   strategy context；自动对冲、组合级保证金 what-if 风控、多账户高级执行策略、
   跨进程持久恢复不再作为核心 SDK 近期目标。
4. `tqsdk-stream` 的核心底座已经覆盖 quote 订阅、动态 quote handle、混合 market
   event、health snapshot、health status、typed reconnect monitor、graceful
   shutdown、fan-out capacity、typed lag/error diagnostics、managed commit sink、
   有限重试、JSONL WAL、WAL fsync policy、本地 compaction、WAL recovery report
   和 commit metadata journal replay；`tqsdk-task::StrategySupervisor` 已提供
   transport-neutral typed telemetry/export hook；durable daemon queue、runtime
   state snapshot recovery 和跨进程 daemon orchestration 属于用户运维系统。
5. 多 provider 聚合、完整 production daemon 和 durable cross-process cache daemon
   都是用户层 facade/tooling 问题，当前暂停作为核心 SDK 目标；它们不得下沉到
   `tqsdk-core`、`tqsdk-session`，也不得通过继续扩张 `tqsdk-data` /
   `tqsdk-task` 伪装为核心能力。后续优先维护已落地的薄基础设施，不继续向平台化能力膨胀。
6. 面向看盘软件与高频柜台的产品级使用方式需要补充独立场景锚点：S30 将历史
   序列缓存限定为 `tqsdk-data` 的 opt-in materialization/cache 能力，S31 将低延迟
   交易柜台限定为 core/session/task/stream 的 hot-path profile。两者边界不同，
   不能用 memmap 历史缓存替代高频 hot path 设计。
