# 场景驱动 Public API 设计审查

本文档审查 `api_contract_sXX_*.rs` 所表达的终端用户场景。正式
`crates/*/examples/api_contract_sXX_*.rs` 文件是 public API 契约样本：能自然表达的场景使用当前
public API 写成可编译示例，并纳入 `cargo check --workspace --examples` 与 CI。

不能自然表达的场景只保留理想用户代码草案，放在
`docs/scenarios/api_gaps/`，并显式标记 API gap；这些 sketch 不参与 Cargo
example 自动发现，不用底层绕路代码伪装通过。

本轮新增和微调的 public surface 位于 `tqsdk-session` / `tqsdk-wait` /
`tqsdk-stream` / `tqsdk-task` facade：它们继续复用既有 session、runtime commit、
reader/cursor 与 domain partition 读面；未改变 crate 边界或 runtime contract。
对应架构文档与 crate README 已随 public API 调整同步更新。

本报告中的“当前 API 是否自然表达”应按 Rust 分层使用者判断，而不是按官方
Python SDK 的 public API 名称判断。Python SDK 提供成熟用户语义证据；Rust
版本通过 `core/session/wait/stream/task/data` 分层服务不同用户。后续 gap
修复顺序见 [`../scenarios/user-layer-iteration-plan.md`](../scenarios/user-layer-iteration-plan.md)。

## 核心能力边界

截至 2026-04-29，再次核对官方 `tqsdk-python` 后，本仓库的 public API
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

## 批次状态

截至 2026-04-29，近期场景驱动批次已经完成并验证：

- S12 跨合约套利从“无法表达”推进到“勉强”：用户现在可以通过
  `TaskHost::execution_group(...)` 表达两腿 typed 下单、全腿 preflight、
  session-scoped retry idempotency、observed `max_unhedged` exposure timeout 和
  group outcome / exposure report；`ExecutionGroupTicket::report(...)` 返回
  revision-bound `ExecutionGroupReport`，用户可以在同一 runtime revision 上审计
  group status 与各腿状态。
- S12 仍不能标记为“自然”：自动 hedge / flatten、timed cancel / replace、
  group resume / persistent audit log 属于用户策略 / 执行系统扩展，不再作为
  核心 SDK 近期目标；保留在
  `docs/scenarios/api_gaps/api_contract_s12_spread_arbitrage.rs`。
- S13 多账户下单从“无法表达”推进到“勉强”：用户现在可以通过
  `TaskHost::account_group()` / `TaskHost::multi_account_order(...)` 表达 typed
  account group、比例拆单、全账户 preflight、session-scoped retry idempotency
  和 per-account outcome report；账户间裸露持续超过 `max_unhedged` 后返回 typed
  `NeedsAttention`；`MultiAccountOrderTicket::report(...)` 返回 revision-bound
  `MultiAccountOrderGroupReport`，用户可以在同一 runtime revision 上审计账户组状态。
  执行计划见
  [`../superpowers/plans/2026-04-27-task-account-group-allocation.md`](../superpowers/plans/2026-04-27-task-account-group-allocation.md)。
- S11 简单策略从“勉强”推进到“自然”：用户现在可以通过
  `StrategyHost` / `StrategyContext` 在同一稳定 task/wait 推进点内读取
  quote/account/position，并复用 `TaskHost::orders(...)`、`RiskEngine` 和
  `TargetPosTask` 表达入场与止盈止损平仓。
- S24 最小可测试策略从“无法表达”推进到“勉强”：`tqsdk-task::testing`
  提供 public `StrategyTestHarness`、`FakeMarket`、`FakeBroker` 和
  `StrategyTestClock`，测试代码不再需要 hidden `*_for_test` API、runtime
  handle、channel 或 provider protocol；fake broker 已支持全成、拒单、单步/跨 step
  部分成交、deterministic clock、step latency 和 disconnect/reconnect 注入；public
  test contract 断言 `OrderLifecycle`，不再依赖 raw `"ALIVE"` / `"FINISHED"` 状态字符串。
- S19 风控前置继续推进但仍为“勉强”：`RiskEngine::check_report(...)` 返回
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
- S16 历史行情回放从“不自然”推进到“勉强”：`tqsdk-task::StrategyReplay`
  已能消费 `tqsdk-data::MarketCacheReplay` 的有序 quote/kline/tick cache
  event，并复用 `StrategyContext`、typed order builder 和 fake broker；
  `KlineDataSeries` / `TickDataSeries` 也已提供 history series -> cache replay
  adapter；`StrategyReplay` 已提供 deterministic replay clock、checkpoint 与
  `resume_from` foundation，并通过 `StrategyReplaySpeed` 提供最快 / real-time /
  scaled replay speed policy；`StrategyReplayCheckpointStore` 已提供 JSON file-backed
  durable checkpoint persistence foundation；`StrategyReplaySourceBuilder` 已提供
  多序列 event source 合并入口。完整 daemon reconnect orchestration 属于生产
  运维/用户层工具，不作为核心 SDK 近期目标。
- S18 本地行情缓存读写继续推进但仍为“勉强”：`MarketCacheWriter` /
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
- S15 实盘 / 模拟 / 回放切换继续推进：“勉强”但已覆盖 provider-backed sim /
  deployment config / lifecycle 子集。`StrategyDeploymentConfig` 支持 live trade
  与 TQKQ sim provider 配置，`StrategyDeployment` / `StrategyLifecycle` 提供统一
  run loop、typed stop reason 和 graceful shutdown report；`StrategySupervisor` /
  `StrategyRetryPolicy` / `StrategyShutdownSignal` 提供 task-layer supervisor、
  typed health/metrics snapshot、有限 retry 和 ctrl-c shutdown hook；策略步骤仍只依赖
  `StrategyEnvironmentContext`。配置文件反序列化可作为薄便利能力评估；多 provider
  environment 已降级为非核心能力，随 S14 暂停。
- S5 低层裸行情直通继续保持“自然”：低层用户可以用
  `SessionClient::subscribe_quotes(...)` 减少 raw `RuntimeCommand` 样板，同时仍通过
  `RuntimeReader::read_market_state()` 走热路径分区读面。
- S20 生产守护进程继续推进但仍为“勉强”：`TqStream::health()` 现在返回
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
| 12. 跨合约套利 | 勉强 | 中 | 无 | 无 | 中 | 中 | 边界收口 | `crates/tqsdk-task/examples/api_contract_s12_spread_arbitrage.rs`; `docs/scenarios/api_gaps/api_contract_s12_spread_arbitrage.rs`; `ExecutionGroupBuilder`; `ExecutionGroupOutcome`; `ExecutionGroupReport`; revision-bound group report; observed `max_unhedged` exposure timeout; automatic hedge / flatten / 补单引擎不进入核心 SDK |
| 13. 多账户下单 | 勉强 | 中 | 无 | 无 | 中 | 中 | 边界收口 | `crates/tqsdk-task/examples/api_contract_s13_multi_account_ordering.rs`; `docs/scenarios/api_gaps/api_contract_s13_multi_account_ordering.rs`; `AccountGroup`; `MultiAccountOrderTicket`; `MultiAccountOrderGroupReport`; revision-bound account group report; observed `max_unhedged` account exposure timeout; advanced failure policy/resume/audit 不进入核心 SDK |
| 14. 多 provider 行情聚合 | 无法表达 | 高 | 严重 | 严重 | 中 | 高 | 暂缓 | `docs/scenarios/api_gaps/api_contract_s14_multi_provider_market_aggregation.rs`; no public provider aggregation facade; 官方 Python 无对应核心 API，非近期核心目标 |
| 15. 实盘 / 模拟 / 回放切换 | 勉强 | 中 | 无 | 无 | 低 | 低 | 边界收口 | `crates/tqsdk-task/examples/api_contract_s15_live_sim_replay_switch.rs`; `docs/scenarios/api_gaps/api_contract_s15_live_sim_replay_switch.rs`; `StrategyEnvironment`; `StrategyEnvironmentContext`; `StrategyDeploymentConfig`; `StrategyDeployment`; `StrategyLifecycle`; `StrategySupervisor`; `StrategyRetryPolicy`; `StrategyShutdownSignal`; `StrategyEnvironment::from_config`; `StrategyEnvironment::{from_task_host,from_test_harness,from_replay_builder}`; config loader 可评估为薄便利能力；multi-provider environment 随 S14 暂缓 |
| 16. 历史行情回放 | 勉强 | 中 | 无 | 无 | 低 | 中 | 边界收口 | `crates/tqsdk-task/examples/api_contract_s16_history_replay_strategy.rs`; `docs/scenarios/api_gaps/api_contract_s16_history_replay_strategy.rs`; `KlineDataSeries::into_market_cache_events`; `TickDataSeries::into_market_cache_events`; `StrategyReplay`; `StrategyReplaySourceBuilder`; `StrategyReplayCheckpoint`; `StrategyReplaySpeed`; `StrategyReplayCheckpointStore`; `StrategyReplayBuilder::{resume_from,resume_from_store,speed}`; daemon reconnect orchestration 不进入核心 SDK |
| 17. 研究场景 | 自然 | 低 | 无 | 无 | 无 | 低 | API 微调 | `crates/tqsdk-data/examples/api_contract_s17_research_kline_batch.rs`; `DataClient::get_kline_data_series` |
| 18. 本地行情缓存读写 | 勉强 | 中 | 无 | 无 | 低 | 中 | 边界收口 | `crates/tqsdk-data/examples/api_contract_s18_local_market_cache.rs`; `crates/tqsdk-data/examples/api_contract_s18_cache_maintenance.rs`; `crates/tqsdk-data/examples/api_contract_s18_cache_daemon_foundation.rs`; `crates/tqsdk-data/examples/api_contract_s18_cache_supervisor_foundation.rs`; `crates/tqsdk-data/examples/api_contract_s18_cache_reader_manifest.rs`; `crates/tqsdk-data/examples/api_contract_s18_cache_recovery_scan.rs`; `crates/tqsdk-data/examples/api_contract_s18_cache_writer_recovery.rs`; `crates/tqsdk-data/examples/api_contract_s18_cache_compaction_ownership.rs`; `crates/tqsdk-data/examples/api_contract_s18_cache_service_foundation.rs`; `crates/tqsdk-data/examples/api_contract_s18_live_market_cache_pipe.rs`; `docs/scenarios/api_gaps/api_contract_s18_local_market_cache.rs`; `docs/scenarios/api_gaps/api_contract_s18_cross_process_cache_service.rs`; `MarketCacheWriter`; `MarketCacheReader`; `MarketCacheReplay`; `MarketCacheReaderManifest`; `MarketCacheRecoveryScan`; `MarketCacheWriterElection`; `MarketCacheWriterLease`; `MarketCacheRecoveryAction`; `MarketCacheCompactionOwnership`; `MarketCacheService`; `MarketCacheStreamWriter`; `MarketCacheQueue`; `MarketCacheLock`; `MarketCacheIndex`; `MarketCacheCompaction`; `MarketCacheDaemon`; `MarketCacheSupervisor`; local file/cache foundation 足够；cross-process cache service 暂停为非核心用户层能力 |
| 19. 风控前置 | 勉强 | 中 | 无 | 无 | 低 | 低 | 边界收口 | `crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs`; `docs/scenarios/api_gaps/api_contract_s19_pre_trade_risk.rs`; `RiskEngine`; `RiskCheckReport`; `RiskProjectionReport`; `RiskDecision`; `RiskRejection`; `TaskHost::orders`; `InstrumentSpec`; guarded insert/cancel risk integration; daily open count / symbol open volume / accumulated open volume / order rate limit; tick-size validation; lightweight single-order projection; portfolio margin what-if / durable audit 不进入核心 SDK |
| 20. 生产守护进程 | 勉强 | 中 | 无 | 无 | 中 | 低 | 边界收口 | `crates/tqsdk-stream/examples/api_contract_s20_production_daemon_health.rs`; `crates/tqsdk-task/examples/api_contract_s20_strategy_supervisor.rs`; `docs/scenarios/api_gaps/api_contract_s20_production_daemon.rs`; `TqStream::health`; `TqStream::reconnect_monitor`; `TqStream::graceful_shutdown`; `StreamHealthSnapshot::{status, should_restart}`; `StreamReconnectMonitor`; `StreamReconnectOutcome`; `StreamReconnectReport`; `StreamGracefulShutdownReport`; `StrategySupervisor`; `StrategySupervisorHealth`; `StrategySupervisorMetrics`; `StrategyTelemetryEvent`; `StrategyTelemetryReporter`; `StrategyRetryPolicy`; `StrategyShutdownSignal`; S20 完成标准止于 typed health / telemetry / graceful shutdown primitives；Rust GUI、HTTP endpoint 和跨进程 daemon 管理均 out of scope |
| 21. 慢消费者隔离 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-stream/examples/api_contract_s21_slow_consumer_isolation.rs`; `docs/scenarios/api_gaps/api_contract_s21_slow_consumer_isolation.rs`; `TqStream::spawn_commit_sink`; `TqStream::spawn_commit_sink_with_options`; `CommitSink`; `StreamSinkOptions`; `StreamSinkProfile`; `StreamSinkRetryPolicy`; `StreamSinkHandle`; `StreamSinkStats`; `StreamSinkShutdownReport`; `StreamSinkWalRecord`; `StreamSinkWalFsyncPolicy`; `StreamSinkWalCompaction`; `StreamSinkWalRecovery`; `StreamCommitJournal`; bounded fan-out / typed lag diagnostic / managed commit sink / finite retry / JSONL WAL / reusable sink profile / fsync policy / local compaction / recovery report / commit metadata journal replay 自然；durable distributed queue 和 runtime state snapshot recovery 不进入核心 SDK |
| 22. 错误诊断与重试 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-stream/examples/api_contract_s22_error_diagnosis_retry.rs`; `docs/scenarios/api_gaps/api_contract_s22_error_diagnosis_retry.rs`; `StreamFacadeError::diagnostic`; `StreamRetryPolicy`; `StreamRetryDecision`; error kind / retry hint / stream-facing retry decision / backoff runner 自然；order/business retry audit 由用户层执行审计系统实现 |
| 23. 合约信息查询与标准化 | 自然 | 低 | 无 | 无 | 无 | 无 | API 微调 | `crates/tqsdk-session/examples/api_contract_s23_contract_metadata.rs`; `SessionClient::query_instrument_specs`; `InstrumentSpec`; `InstrumentClass` |
| 24. 最小可测试策略 | 勉强 | 中 | 无 | 无 | 低 | 低 | 局部重构 | `crates/tqsdk-task/examples/api_contract_s24_testable_strategy.rs`; `docs/scenarios/api_gaps/api_contract_s24_testable_strategy.rs`; `StrategyTestHarness`; `FakeMarket`; `FakeBroker`; `StrategyTestClock`; `OrderLifecycle`; `FakeBroker::partial_fills`; `FakeBroker::latency_steps`; `FakeBroker::disconnect_for_steps`; `FakeBrokerConnectionStatus`; durable fixtures and richer broker behavior remain gap |

## 主要结论

1. 当前最自然的终端用户场景是：零门槛 wait quote、低层裸行情直通、研究 K线批处理、合约 metadata 查询。
2. 交易相关场景的主要 API gap 不是 core command 能力缺失，而是用户级 execution/risk abstraction 的边界需要收口。普通登录、限价单、部分成交撤单、session-scoped reconnect-safe order intent、最小前置风控、官方同类基础开仓/频率限额、revision-bound execution/risk report、轻量单笔 what-if projection、execution group foundation、account group foundation 和最小 strategy context 已具备薄 facade；自动对冲、组合级保证金 what-if 风控、多账户高级执行策略、跨进程持久恢复不再作为核心 SDK 近期目标。
3. `tqsdk-stream` 的底座方向正确，quote 订阅、动态 quote handle、混合 market event、health snapshot、health status、typed reconnect monitor、graceful shutdown、fan-out capacity、typed lag/error diagnostics、managed commit sink、有限重试、JSONL WAL、WAL fsync policy、本地 compaction、WAL recovery report 和 commit metadata journal replay 已有薄 facade；`tqsdk-task::StrategySupervisor` 已提供 transport-neutral typed telemetry/export hook；durable daemon queue、runtime state snapshot recovery 和跨进程 daemon orchestration 仍停留在底层组合能力之外。
4. 多 provider 聚合、完整 production daemon 和 durable cross-process cache daemon 都是用户层 facade/tooling 问题，当前暂停作为核心 SDK 目标；它们不得下沉到 `tqsdk-core`、`tqsdk-session`，也不得通过继续扩张 `tqsdk-data` / `tqsdk-task` 伪装为核心能力。本地行情 cache record / JSONL reader-writer / ordered replay foundation、reader manifest、recovery scan、writer election、recovery action、compaction ownership、本地 file service foundation、history series replay adapter、单进程 live stream pipe、JSONL queue、lock lease、index、compaction、process-local daemon 与 process-local supervisor foundation 已落在 `tqsdk-data`，cache replay -> strategy context foundation、strategy environment/deployment/supervisor foundation、provider-backed TQKQ sim config、replay source builder、replay speed policy、checkpoint store、test clock、fake broker latency、fake broker reconnect 与跨 step partial fill 已落在 `tqsdk-task`；后续优先维护这些薄基础设施，不继续向平台化能力膨胀。
