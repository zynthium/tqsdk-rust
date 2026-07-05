# 场景契约

当请求要求按角色给示例、覆盖大范围场景、提供 scenario contract 或 public API 证据时，使用本文件。优先引用正式 `crates/*/examples/api_contract_sXX_*.rs` 示例，不要把 archived sketch 当成当前 API。S14 是唯一 active gap，并且不是近期核心 SDK 目标。

## 目录

- 用户角色
- 角色回答模板
- 契约地图
- 覆盖规则

## 用户角色

| 用户角色 | 首选 crate | 契约示例 | 说明 |
| --- | --- | --- | --- |
| 单策略作者 | `tqsdk`, `tqsdk-wait`, `tqsdk-task`, `tqsdk-monitor` | S33, S37-S41, S43-S48, S1, S3, S6-S11, S25-S26, S29, S34, S36 | 默认 `tqsdk` facade / prelude；明确 Python-style 时用稳定 `wait_update()` 循环、live refs、薄 order wrapper、startup recovery、reconnect-safe order intent、target-pos ownership、batch quote interest、live/backtest same-body loop、cache-backed backtest、显式 live tick recording、共享 cache policy 和可选 same-process monitoring。 |
| Async 系统集成方 | `tqsdk-session`, `tqsdk-core` | S5, S23, S27, S31; S2/S4/S21/S22/S35 removed | Multi-consumer event/fan-out 是调用方层：复用 shared session、`RuntimeReader`、`UpdateCursor`，自建 filters、bounded channels、lag diagnostics 和 sidecar。 |
| 低层 / 低延迟用户 | `tqsdk-session`, `tqsdk-core`, `tqsdk-task` | S5, S23, S27, S31 | Thin session substrate、direct metadata query、hot-path `RuntimeReader`、same-revision market/trade reads、low-latency desk profile。 |
| 执行工具用户 | `tqsdk-task`, `tqsdk-wait` | S6-S13, S19, S29, S31 | Typed order tickets、cancel/partial-fill helpers、risk gates、execution groups、account groups、target-pos ownership。 |
| 研究 / 行情数据用户 | `tqsdk-data`, `tqsdk-session`, `tqsdk-task`, `tqsdk` | S16-S17, S23, S27-S30, S32, S43-S47 | Historical series、downloads、CSV export、option Greeks、TQBN history cache、tick-only backtest cache、显式 live tick recording、共享 live/backtest cache policy、task-owned replay source、Python-compatible 本地回测模拟账户。 |
| 测试 / 回放用户 | `tqsdk`, `tqsdk-task`, `tqsdk-data`, `tqsdk-wait` | S15-S16, S24, S30, S32, S36-S41, S43-S47 | Live/sim/replay environment、deterministic fake market/broker、task-owned replay sources、history-row-backed tests、Python-compatible sim backtest、default facade same-body backtest、same-body wait backtest loop、cache-backed backtest 和 shared cache policy。 |
| 生产 runtime 构建者 | `tqsdk-session`, `tqsdk-core`, `tqsdk-task`, `tqsdk-monitor` | S5, S15, S20 task, S31, S48; S21/S22 removed | Session progress、runtime cursor、typed strategy supervisor、caller-owned bounded fan-out 和 lag diagnostics。需要同进程只读运行面板时用可选 `tqsdk-monitor`；不内置 daemon manager、event facade 或 managed sink/WAL。 |
| Multi-provider 基础设施用户 | 用户层 facade / 未来独立项目 | S14 gap only | Multi-provider aggregation 不是当前 core SDK API。不要把它推入 core/session。 |

## 角色回答模板

回答“我该用什么？”或“给每类角色示例”这类宽问题时，按以下模板组织。

### 单策略作者

普通策略从 `tqsdk` facade 起步。明确需要 Python-style `wait_update()` / `WaitStep` 时下钻 `tqsdk-wait`；需要 target-position ownership、risk gates、strategy context 或 test harness 内部能力时，再加 `tqsdk-task`。

- 首选示例：S33 default facade、S1 quote loop、S3 snapshot、S25 serial/status、S6-S7 order lifecycle、S8 account/position、S9 startup recovery、S10 reconnect order intent。
- 执行层升级：S11 simple strategy、S29 target-pos ownership、S34 batch quote interest、S36 wait live/backtest same-body loop、S37-S41 default facade live/backtest same-body loop、S43-S47 cache-backed backtest/live tick recording/shared cache policy。
- 运行观测：S48 启用 `monitoring` feature，通过 `.monitoring(MonitoringConfig::localhost(port))` 读取同进程 snapshot；cache inventory 由 `.market_cache(...)` 或 backtest cache 配置自动接入，也可显式 `with_cache_inventory(path)`。
- 避免：在 `TqApi` 上复制 direct metadata helpers、本地 order overlay、解析 order status 字符串、隐藏实盘账户副作用。

### Async 系统集成方

有多个 consumer、event pipeline、slow sink 或 production health 诉求时，从 `tqsdk-session + RuntimeReader/UpdateCursor` 起步。one-shot metadata 复用同一个 session；event bus、fan-out、过滤、队列、lag report 和持久化 sidecar 由调用方实现。

- 首选示例：S5 bare market fast path、S23/S27 metadata query、S31 same-revision market/trade hot path。
- 已删除场景：S2 dynamic subscriptions、S4 mixed market events、S21 slow consumer isolation、S22 retry diagnostics、S35 quote batches 不再对应内置 SDK facade。
- 避免：每个 consumer clone full snapshot、无界 channel、重复 metadata session、把 durable sink/WAL 或 event bus 挪进 SDK facade。

### 低层 / 低延迟用户

需要细粒度控制 session progress 和 hot reads 时，从 `tqsdk-session + RuntimeReader` 起步。只有 hot path 也是执行路径时，才使用 `tqsdk-task` 的 S31。

- 首选示例：S5 bare market fast path、S23 contract metadata、S27 metadata/service query pack。
- 交易柜台示例：S31 same-revision market/trade read 和 prechecked order submission。
- 避免：普通用法从 `tqsdk-core` 起步、hot path 读取 full snapshot、用 history cache 做 live hot-path decision。

### 执行工具用户

订单需要 ownership、grouping、risk、scheduling 或 multi-account accounting 时，从 `tqsdk-task` 起步。wait order wrapper 只用于薄下单。

- 首选示例：S6 limit order、S7 cancel/partial fill、S10 reconnect order consistency。
- Task 示例：S11 simple strategy、S12 execution group、S13 account group、S19 risk、S29 target-pos ownership。
- 避免：承诺 automatic hedge/flatten、声称 cross-process durable audit、绕过 task ownership guard。

### 研究 / 行情数据用户

owned historical rows、downloads、CSV、Greeks 和 history series cache 从 `tqsdk-data` 起步；deterministic replay source 从 `tqsdk-task` 起步。只有用于框定查询的一次性 metadata 才使用 `tqsdk-session`。

- 首选示例：S17 research K-line batch、S28 download/export and Greeks。
- Cache/replay 示例：S30 history series cache、S16 replay integration、S43-S45 cache-backed backtest。
- live tick recording：S46 通过 `Tq::record_ticks(...)` 把指定 symbol 的 live tick 写入共享 `BacktestTickCache`；S47 用 `MarketCachePolicy` 把 live recording 和 cache-backed backtest 的 cache 目录/symbol 集合统一到同一份 policy；泛化 live event/K 线/commit persistence 仍用调用方 sidecar。
- Metadata 示例：S23、S27。
- 避免：把 history 建模成 live refs、把 DataFrame/polars 语义塞进 session/wait、确定性测试依赖 live credentials。

### 测试 / 回放用户

确定性策略测试从 `tqsdk-task::testing` 起步。Replay input 使用 `tqsdk-data` cache/history records。

- 首选示例：S24 testable strategy、S15 live/sim/replay switch、S16 history replay、S30 history cache、S32 Python-compatible backtest sim、S36 wait same-body backtest loop、S37-S41 / S43-S47 default facade backtest/cache loop。
- 避免：hidden `*_for_test` API、provider protocol fixture、`Arc<Mutex<_>>` 测试编排、unit test 依赖 live services。

### 生产 runtime 构建者

session progress、runtime cursor、bounded fan-out 和 lag diagnostics 从 `tqsdk-session + RuntimeReader/UpdateCursor` 起步；fan-out 是调用方层。策略生命周期再加 `tqsdk-task` supervisor。

- 首选示例：S5 bare market fast path、S20 strategy supervisor、S31 low-latency desk。
- 可选观测：`tqsdk-monitor` 只提供同进程 read-only dashboard / snapshot / cache inventory projection；不要把它当作 session owner、HTTP write API、daemon manager 或 cache 管理器。
- 已删除场景：S21 slow consumer isolation、S22 retry diagnostics 不再对应内置 SDK facade。
- 避免：声称内置 HTTP health endpoint、GUI、process manager、distributed queue、event facade、runtime state snapshot recovery 或 cross-process daemon orchestration。

### Multi-provider 基础设施用户

把这个场景视为当前 core SDK 不支持的场景。只把 S14 作为边界 sketch 引用。

- 支持的回答：“当前 SDK 暴露的是 single-session/session-backed facade；multi-provider aggregation 属于用户层 facade 或未来独立项目。”
- 避免：把 provider aggregation 加进 `tqsdk-core`、把多棵 state tree 混进普通 wait 或 caller-owned fan-out API、暗示已有 public support。

## 契约地图

| 场景 | 正式示例或 gap | 用户问什么时使用 | 主要 API / 边界 |
| --- | --- | --- | --- |
| S1 Zero-barrier quote | `crates/tqsdk-wait/examples/api_contract_s01_zero_barrier_quote.rs` | 基础 live quote loop、Python-like step loop | `TqApi::quote`、`step`、`WaitStep::is_changing`；live refs 属于 wait。 |
| S2 Dynamic subscriptions | Removed / caller-owned layer | async consumer 中动态增删多个 symbol | 当前无内置 event facade；调用方用 `SessionClient::subscribe_quotes`、shared `RuntimeReader` 和自有 subscription intent 重排。 |
| S3 Quote snapshot | `crates/tqsdk-wait/examples/api_contract_s03_quote_snapshot.rs` | 用局部 helper 只取一次 ready quote snapshot | `TqApi::quote`、`step_until`、`QuoteRef::load`；仍是 wait facade，不是 session metadata。 |
| S4 Mixed market events | Removed / caller-owned layer | Quote/tick/kline event bus | 当前无内置 event bus；调用方基于 commit boundary、state partition reads 和自有 typed event model 组装。 |
| S5 Bare market fast path | `crates/tqsdk-session/examples/api_contract_s05_bare_market_fast_path.rs` | 低层行情订阅和 hot reads | `SessionClient::{subscribe_quotes, progress_once}`、`RuntimeReader::read_market_state`；避免高层 facade 开销。 |
| S6 Limit order | `crates/tqsdk-wait/examples/api_contract_s06_limit_order.rs` | 普通下单 | `login_trade_account`、`limit_order`、`LimitOrderIntent::send_once`、`OrderTicket::wait_terminal`；副作用必须显式。 |
| S7 Cancel / partial fill | `crates/tqsdk-wait/examples/api_contract_s07_cancel_partial_fill.rs` | 等待部分成交、撤剩余、等待终态 | `OrderTicket` / `OrderRef` helpers；不要解析 raw status 字符串。 |
| S8 Account / position | `crates/tqsdk-wait/examples/api_contract_s08_account_position_updates.rs` | 资金、账户、持仓 live refs | `account`、`position`；wait live state，不是 direct query。 |
| S9 Startup recovery | `crates/tqsdk-wait/examples/api_contract_s09_startup_state_recovery.rs` | 启动或重连后的 ready barrier | `StartupRecoverySpec`、`TqApi::startup_recovery`；调用方 event layer 自己基于 session/cursor 建 ready barrier。 |
| S10 Reconnect order consistency | `crates/tqsdk-wait/examples/api_contract_s10_reconnect_order_consistency.rs` | 单 session 内幂等 order intent | `OrderIntentRecord`、`OrderTicketState`、stable client intent；cross-process persistence 是 out of scope。 |
| S11 Simple strategy | `crates/tqsdk-task/examples/api_contract_s11_simple_strategy.rs` | 策略内读取 quote/account/position 并下单 | `StrategyHost`、`StrategyContext`、`TaskHost::orders`、`RiskEngine`、`TargetPosTask`。 |
| S12 Spread arbitrage | `crates/tqsdk-task/examples/api_contract_s12_spread_arbitrage.rs` | 两腿执行 foundation | `ExecutionGroupBuilder`、`ExecutionGroupOutcome`、revision-bound report；automatic hedge/flatten 是用户层能力。 |
| S13 Multi-account ordering | `crates/tqsdk-task/examples/api_contract_s13_multi_account_ordering.rs` | account group、比例拆单、per-account outcome | `AccountGroup`、`MultiAccountOrderTicket`、revision-bound account report；advanced compensation/audit 是用户层能力。 |
| S14 Multi-provider aggregation | `docs/scenarios/api_gaps/api_contract_s14_multi_provider_market_aggregation.rs` | 多行情 provider、failover、dedupe | Active gap 且非核心 SDK 目标。只作为边界样本；不要移动到 core/session。 |
| S15 Live / sim / replay switch | `crates/tqsdk-task/examples/api_contract_s15_live_sim_replay_switch.rs` | 同一策略在 live、sim、replay 间切换 | `StrategyEnvironment`、`StrategyDeployment`、`StrategySupervisor`；multi-provider environment 仍是 out of scope。 |
| S16 History replay strategy | `crates/tqsdk-task/examples/api_contract_s16_history_replay_strategy.rs` | 用历史 rows / replay events 跑策略 | `ReplayMarketSource`、`StrategyReplay`、`StrategyReplaySourceBuilder`、checkpoint/speed controls；production daemon reconnect 是 out of scope。 |
| S17 Research kline batch | `crates/tqsdk-data/examples/api_contract_s17_research_kline_batch.rs` | 批量历史 K 线研究 | `DataClient::get_kline_data_series`；owned rows，不是 live refs。 |
| S18 Local market cache | Removed / non-core | JSONL cache record/replay | Removed from current core SDK public API；live pipe、JSONL cache 和 cross-process cache daemon 是用户层能力。 |
| S19 Pre-trade risk | `crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs` | 下单前本地 risk gates | `RiskEngine`、`RiskCheckReport`、`RiskProjectionReport`、`RiskDecision`；portfolio margin engine 和 durable audit 是 out of scope。 |
| S20 Production primitives | `crates/tqsdk-task/examples/api_contract_s20_strategy_supervisor.rs` | strategy supervisor、graceful shutdown foundation | task 层提供 typed supervisor/shutdown foundation；event health endpoint、GUI、HTTP endpoint 或 process manager 属于调用方系统。 |
| S21 Slow consumer isolation | Removed / caller-owned layer | bounded fan-out、lag diagnostics | 调用方在 `RuntimeReader` / `UpdateCursor` 之上自建 bounded channel、lag policy、sink/WAL/journal 或 distributed queue。 |
| S22 Error diagnosis / retry | Removed / caller-owned layer | retryable errors、backoff、typed diagnostics | session/progress error 可由调用方分类；event retry policy、business retry audit 和 durable diagnostics 属于用户执行系统。 |
| S23 Contract metadata | `crates/tqsdk-session/examples/api_contract_s23_contract_metadata.rs` | symbol info、instrument specs、contract class、normalized metadata | `SessionClient::{query_symbol_info,query_instrument_specs}`、`SymbolInfo`、`InstrumentSpec`、`InstrumentClass`；one-shot session query。 |
| S24 Testable strategy | `crates/tqsdk-task/examples/api_contract_s24_testable_strategy.rs` | 不依赖 live services 的策略单测 | `StrategyTestHarness`、`FakeMarket`、`FakeBroker`、`StrategyTestClock`；完整 exchange simulator 是 out of scope。 |
| S25 Wait serial/status | `crates/tqsdk-wait/examples/api_contract_s25_wait_serial_trading_status.rs` | trading status、K-line serial、tick serial | `trading_status`、`kline`、`tick`、`WaitStep::is_changing_fields`；不属于 session/data。 |
| S26 Wait trade/system refs | `crates/tqsdk-wait/examples/api_contract_s26_trade_system_refs.rs`; `crates/tqsdk-wait/examples/api_contract_s26_security_trade_refs.rs` | notifications、settlement、risk refs、security trade refs | Wait live refs 和 `confirm_settlement` 这类 command wrapper；不是 direct query。 |
| S27 Metadata/service query pack | `crates/tqsdk-session/examples/api_contract_s27_metadata_service_queries.rs` | quotes list、main contracts、options、calendar、settlement、ranking、EDB | `SessionClient` typed one-shot metadata/service APIs；不要复制到 wait 或 caller-owned event layer。 |
| S28 Download/export/Greeks | `crates/tqsdk-data/examples/api_contract_s28_download_export.rs`; `crates/tqsdk-data/examples/api_contract_s28_option_greeks.rs` | history downloads、CSV export、option Greeks | `DataClient` research/download APIs；不是 live session refs。 |
| S29 TargetPos ownership | `crates/tqsdk-task/examples/api_contract_s29_target_pos_ownership.rs` | 同 account+symbol task ownership 和 scheduler ownership | `TaskHost::{target_pos,target_pos_scheduler,check_manual_order_allowed}`；cross-account target-pos orchestration 是用户层能力。 |
| S30 History series cache | `crates/tqsdk-data/examples/api_contract_s30_history_series_cache.rs` | opt-in TQBN history cache for data_series | `DataClientBuilder::history_cache_enabled`、cache reports、`HistorySeriesCache::read_*_data_series`、scan/maintenance；live tick recording 属于 S46，泛化 live serial cache 仍是 out of scope。 |
| S31 Low-latency desk | `crates/tqsdk-task/examples/api_contract_s31_low_latency_trading_desk.rs` | same-revision market/trade hot path 和 prechecked orders | `TradingDeskProfile`、`RuntimeReader::read_market_trade_state`、typed latency/order reports；不是 OMS 或 auto-hedger。 |
| S32 Python-compatible backtest sim | `crates/tqsdk-task/examples/api_contract_s32_python_backtest_sim.rs` | 本地 quote replay + `TqSim` 回测模拟账户 | 不连接真实服务；用于 Python-compatible 本地回测账户闭环。 |
| S33 Default facade | `crates/tqsdk/examples/api_contract_s33_default_facade.rs` | 默认 `tqsdk` facade / prelude | `Tq` / `TqBuilder`、resolved TQKQ target-position helper、`TargetPos` intent API、curated `advanced::*`。 |
| S34 Wait batch quote subscription | `crates/tqsdk-wait/examples/api_contract_s34_batch_quote_subscription.rs` | wait facade 批量 quote interest 和 step-bound changed snapshots | 单 owner `wait_update()` / `step()` 消费模型；不是 multi-consumer API。 |
| S35 Quote batches | Removed / caller-owned layer | async multi-consumer quote batch consumption | 当前无内置 quote batch event facade；调用方基于 `RuntimeReader` / `UpdateCursor` 聚合 quote changes。 |
| S36 Wait live/backtest same-body loop | `crates/tqsdk-wait/examples/api_contract_s36_wait_live_backtest_same_body.rs` | 同一段 wait 策略主体用于 live 和 backtest builder | 策略主体只依赖 handles 和 `step()` 推进；live/backtest 差异留在 builder 配置。 |
| S37 Default facade no-cache server backtest | `crates/tqsdk/examples/api_contract_s37_facade_server_backtest.rs` | 默认 `tqsdk` facade 的 `.backtest(...)` 在无缓存时切换官方服务端回测 | `TqBuilder::backtest(start_ns,end_ns).connect()`；策略主体继续用 `Tq::next()` / `quote()`。 |
| S38 Default facade local replay backtest | `crates/tqsdk/examples/api_contract_s38_facade_local_backtest.rs` | 默认 `tqsdk` facade 本地 replay + `TqSim` 回测 | `TqBuilder::replay_backtest(replay)`、`quote_symbol(...)`、`price_tick(...)`；不连接真实服务。 |
| S39 Default facade same-body strategy | `crates/tqsdk/examples/api_contract_s39_facade_same_body.rs` | 同一段 `&mut Tq` 策略主体用于 live、服务端回测和本地回测 | builder 决定运行模式；策略函数不分叉。 |
| S40 Default facade local backtest TargetPos | `crates/tqsdk/examples/api_contract_s40_facade_local_backtest_target_pos.rs` | 本地 replay 回测中复用 `TargetPos` wrapper | 同一 `Tq::next()` 策略主体读取持仓并调仓。 |
| S41 Default facade server replay | `crates/tqsdk/examples/api_contract_s41_facade_server_replay.rs` | 官方单日复盘行情接入默认 facade | `server_replay(date)?`、replay endpoint 和 heartbeat。 |
| S43 Cache-backed backtest | `crates/tqsdk/examples/api_contract_s43_facade_backtest_history_cache.rs` | 默认 facade 通过持久 tick cache 做本地撮合回测 | `.backtest(...).cache_dir(...).cache_only().universe(...)`，复用 `BacktestTickCache`。 |
| S44 Remote-on-miss backtest cache fill | `crates/tqsdk/examples/api_contract_s44_facade_backtest_remote_on_miss.rs` | 缓存缺口用官方 server-side backtest tick stream 填补 | 需要账号但不需要专业历史下载权限；cache hit 不需要 auth。 |
| S45 Backtest cache warmup | `crates/tqsdk/examples/api_contract_s45_facade_backtest_cache_warmup.rs` | 只预热缓存，不创建策略 runtime | `.warmup().await?`，先跳过完整缓存，再用内部有界远端调度器填补缺口。 |
| S46 Live tick recording | `crates/tqsdk/examples/api_contract_s46_facade_record_ticks.rs` | 显式把指定 live tick 写入回测共享缓存 | `Tq::record_ticks(cache_dir, symbols)`；由 `next()` / `wait_update()` 推进，跳号保留 coverage 缺口。 |
| S47 Shared market cache policy | `crates/tqsdk/examples/api_contract_s47_facade_market_cache_policy.rs` | 用同一份配置维护 live tick recording 和 cache-backed backtest 输入 | `MarketCachePolicy::new(cache_dir).record_ticks(symbols)`、`.market_cache(policy)`、`record_ticks_health()`、`recorded_market_cache_policy()`；补洞仍需显式 `.auth_env()?` + `.warmup()` / `.remote_on_miss()`。 |
| S48 Embedded monitoring dashboard | `crates/tqsdk/examples/api_contract_s48_facade_monitoring_dashboard.rs` | 启用和查看同进程监控面板、snapshot、cache inventory projection | `MonitoringConfig::localhost(port)`、`.monitoring(...)`、`Tq::monitor_addr()`、`Tq::monitor_snapshot()`、`with_cache_inventory(path)`；默认离线 replay demo，不需要账号。 |

## 覆盖规则

- 宽回答先覆盖用户角色，再只列出该角色自然涉及的场景。
- active formal examples 是事实来源。Archived scenario files 只是历史上下文，除非正式 example 明确指向它们来说明边界决策。
- 对 S14 要明确说明 desired sketch 只用于保留边界样本；不要暗示已支持。
- 如果某场景的高级功能是 out of scope，要在同一段里说明已支持的 foundation 和用户层责任。
- 如果某工作流没有 contract example，指出最接近的已支持场景，并判断这是 new gap、用户层系统，还是现有 API 的正常组合。
