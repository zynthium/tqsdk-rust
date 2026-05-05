# 场景契约

当请求要求按角色给示例、覆盖大范围场景、提供 scenario contract 或 public API 证据时，使用本文件。优先引用正式 `crates/*/examples/api_contract_sXX_*.rs` 示例，不要把 archived sketch 当成当前 API。S14 是唯一 active gap，并且不是近期核心 SDK 目标。

## 用户角色

| 用户角色 | 首选 crate | 契约示例 | 说明 |
| --- | --- | --- | --- |
| 单策略作者 | `tqsdk-wait`, `tqsdk-task` | S1, S3, S6-S11, S25-S26, S29 | Python-style 稳定 `wait_update()` 循环、live refs、薄 order wrapper、startup recovery、reconnect-safe order intent、target-pos ownership。 |
| Async 系统集成方 | `tqsdk-stream`, `tqsdk-session` | S2, S4, S20-S22 | Multi-consumer streams、dynamic subscriptions、market events、health、retry diagnostics、slow-consumer isolation、managed sinks。 |
| 低层 / 低延迟用户 | `tqsdk-session`, `tqsdk-core`, `tqsdk-task` | S5, S23, S27, S31 | Thin session substrate、direct metadata query、hot-path `RuntimeReader`、same-revision market/trade reads、low-latency desk profile。 |
| 执行工具用户 | `tqsdk-task`, `tqsdk-wait` | S6-S13, S19, S29, S31 | Typed order tickets、cancel/partial-fill helpers、risk gates、execution groups、account groups、target-pos ownership。 |
| 研究 / 行情数据用户 | `tqsdk-data`, `tqsdk-session`, `tqsdk-task` | S16-S18, S23, S27-S30 | Historical series、downloads、CSV export、option Greeks、本地 cache/replay、Python-compatible history cache。 |
| 测试 / 回放用户 | `tqsdk-task`, `tqsdk-data` | S15-S16, S18, S24, S30 | Live/sim/replay environment、deterministic fake market/broker、replay sources、history/cache-backed tests。 |
| 生产 runtime 构建者 | `tqsdk-stream`, `tqsdk-task` | S20-S22 | Typed health/telemetry、graceful shutdown、retry policy、bounded fan-out、WAL/sink foundation。不内置 HTTP endpoint、GUI 或 daemon manager。 |
| Multi-provider 基础设施用户 | 用户层 facade / 未来独立项目 | S14 gap only | Multi-provider aggregation 不是当前 core SDK API。不要把它推入 core/session。 |

## 角色回答模板

回答“我该用什么？”或“给每类角色示例”这类宽问题时，按以下模板组织。

### 单策略作者

live loop 从 `tqsdk-wait` 起步。只有用户需要 target-position ownership、risk gates、strategy context 或 test harness 时，再加 `tqsdk-task`。

- 首选示例：S1 quote loop、S3 snapshot、S25 serial/status、S6-S7 order lifecycle、S8 account/position、S9 startup recovery、S10 reconnect order intent。
- 执行层升级：S11 simple strategy、S29 target-pos ownership。
- 避免：在 `TqApi` 上复制 direct metadata helpers、本地 order overlay、解析 order status 字符串、隐藏实盘账户副作用。

### Async 系统集成方

有多个 consumer、event pipeline、slow sink 或 production health 诉求时，从 `tqsdk-stream` 起步。one-shot metadata 复用 `stream.session()`。

- 首选示例：S2 dynamic subscriptions、S4 mixed market events、S21 slow consumer isolation。
- 生产示例：S20 stream health/graceful shutdown、S22 retry diagnostics。
- 避免：每个 consumer clone full snapshot、无界 channel、重复 metadata session、把 stream sink durability 挪进 `tqsdk-data`。

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

owned historical rows、downloads、CSV、Greeks、cache/replay materialization 从 `tqsdk-data` 起步。只有用于框定查询的一次性 metadata 才使用 `tqsdk-session`。

- 首选示例：S17 research K-line batch、S28 download/export and Greeks。
- Cache/replay 示例：S18 local market cache、S30 history series cache、S16 replay integration。
- Metadata 示例：S23、S27。
- 避免：把 history 建模成 live refs、把 DataFrame/polars 语义塞进 session/wait、确定性测试依赖 live credentials。

### 测试 / 回放用户

确定性策略测试从 `tqsdk-task::testing` 起步。Replay input 使用 `tqsdk-data` cache/history records。

- 首选示例：S24 testable strategy、S15 live/sim/replay switch、S16 history replay、S18 cache replay、S30 history cache。
- 避免：hidden `*_for_test` API、provider protocol fixture、`Arc<Mutex<_>>` 测试编排、unit test 依赖 live services。

### 生产 runtime 构建者

stream health、reconnect monitoring、bounded fan-out 和 sink 从 `tqsdk-stream` 起步。策略生命周期再加 `tqsdk-task` supervisor。

- 首选示例：S20 stream health 和 strategy supervisor、S21 slow consumer isolation、S22 retry diagnostics。
- 避免：声称内置 HTTP health endpoint、GUI、process manager、distributed queue、runtime state snapshot recovery 或 cross-process daemon orchestration。

### Multi-provider 基础设施用户

把这个场景视为当前 core SDK 不支持的场景。只把 S14 作为边界 sketch 引用。

- 支持的回答：“当前 SDK 暴露的是 single-session/session-backed facade；multi-provider aggregation 属于用户层 facade 或未来独立项目。”
- 避免：把 provider aggregation 加进 `tqsdk-core`、把多棵 state tree 混进普通 wait/stream API、暗示已有 public support。

## 契约地图

| 场景 | 正式示例或 gap | 用户问什么时使用 | 主要 API / 边界 |
| --- | --- | --- | --- |
| S1 Zero-barrier quote | `crates/tqsdk-wait/examples/api_contract_s01_zero_barrier_quote.rs` | 基础 live quote loop、Python-like `wait_update` | `TqApi::get_quote`、`wait_update`、`is_changing`；live refs 属于 wait。 |
| S2 Dynamic subscriptions | `crates/tqsdk-stream/examples/api_contract_s02_dynamic_subscriptions.rs` | async stream 中动态增删多个 symbol | `TqStream::quotes`、`QuoteSubscription::{add, remove, symbols}`；reconnect 会重排 subscription intent。 |
| S3 Quote snapshot | `crates/tqsdk-wait/examples/api_contract_s03_quote_snapshot.rs` | 不手写循环，只取一次 ready quote snapshot | `TqApi::quote_snapshot`；仍是 wait facade，不是 session metadata。 |
| S4 Mixed market streams | `crates/tqsdk-stream/examples/api_contract_s04_mixed_market_streams.rs` | Quote/tick/kline event bus | `TqStream::market_events`、`MarketEventStream`、typed market events。 |
| S5 Bare market fast path | `crates/tqsdk-session/examples/api_contract_s05_bare_market_fast_path.rs` | 低层行情订阅和 hot reads | `SessionClient::{subscribe_quotes, progress_once}`、`RuntimeReader::read_market_state`；避免高层 facade 开销。 |
| S6 Limit order | `crates/tqsdk-wait/examples/api_contract_s06_limit_order.rs` | 普通下单 | `login_trade_account`、`limit_order`、`LimitOrderIntent::send_once`、`OrderTicket::wait_terminal`；副作用必须显式。 |
| S7 Cancel / partial fill | `crates/tqsdk-wait/examples/api_contract_s07_cancel_partial_fill.rs` | 等待部分成交、撤剩余、等待终态 | `OrderTicket` / `OrderRef` helpers；不要解析 raw status 字符串。 |
| S8 Account / position | `crates/tqsdk-wait/examples/api_contract_s08_account_position_updates.rs` | 资金、账户、持仓 live refs | `get_account`、`get_position`；wait live state，不是 direct query。 |
| S9 Startup recovery | `crates/tqsdk-wait/examples/api_contract_s09_startup_state_recovery.rs` | 启动或重连后的 ready barrier | `StartupRecoverySpec`、`TqApi::startup_recovery`、`TqStream::recover_state`。 |
| S10 Reconnect order consistency | `crates/tqsdk-wait/examples/api_contract_s10_reconnect_order_consistency.rs` | 单 session 内幂等 order intent | `OrderIntentRecord`、`OrderTicketState`、stable client intent；cross-process persistence 是 out of scope。 |
| S11 Simple strategy | `crates/tqsdk-task/examples/api_contract_s11_simple_strategy.rs` | 策略内读取 quote/account/position 并下单 | `StrategyHost`、`StrategyContext`、`TaskHost::orders`、`RiskEngine`、`TargetPosTask`。 |
| S12 Spread arbitrage | `crates/tqsdk-task/examples/api_contract_s12_spread_arbitrage.rs` | 两腿执行 foundation | `ExecutionGroupBuilder`、`ExecutionGroupOutcome`、revision-bound report；automatic hedge/flatten 是用户层能力。 |
| S13 Multi-account ordering | `crates/tqsdk-task/examples/api_contract_s13_multi_account_ordering.rs` | account group、比例拆单、per-account outcome | `AccountGroup`、`MultiAccountOrderTicket`、revision-bound account report；advanced compensation/audit 是用户层能力。 |
| S14 Multi-provider aggregation | `docs/scenarios/api_gaps/api_contract_s14_multi_provider_market_aggregation.rs` | 多行情 provider、failover、dedupe | Active gap 且非核心 SDK 目标。只作为边界样本；不要移动到 core/session。 |
| S15 Live / sim / replay switch | `crates/tqsdk-task/examples/api_contract_s15_live_sim_replay_switch.rs` | 同一策略在 live、sim、replay 间切换 | `StrategyEnvironment`、`StrategyDeployment`、`StrategySupervisor`；multi-provider environment 仍是 out of scope。 |
| S16 History replay strategy | `crates/tqsdk-task/examples/api_contract_s16_history_replay_strategy.rs` | 用历史/cache events 跑策略 | `StrategyReplay`、`StrategyReplaySourceBuilder`、checkpoint/speed controls；production daemon reconnect 是 out of scope。 |
| S17 Research kline batch | `crates/tqsdk-data/examples/api_contract_s17_research_kline_batch.rs` | 批量历史 K 线研究 | `DataClient::get_kline_data_series`；owned rows，不是 live refs。 |
| S18 Local market cache | `crates/tqsdk-data/examples/api_contract_s18_local_market_cache.rs`; `crates/tqsdk-data/examples/api_contract_s18_live_market_cache_pipe.rs` | JSONL cache record/replay 或单进程 live pipe | `MarketCacheWriter`、`MarketCacheReader`、`MarketCacheReplay`、`MarketCacheStreamWriter`；cross-process cache daemon 是用户层能力。 |
| S19 Pre-trade risk | `crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs` | 下单前本地 risk gates | `RiskEngine`、`RiskCheckReport`、`RiskProjectionReport`、`RiskDecision`；portfolio margin engine 和 durable audit 是 out of scope。 |
| S20 Production primitives | `crates/tqsdk-stream/examples/api_contract_s20_production_daemon_health.rs`; `crates/tqsdk-task/examples/api_contract_s20_strategy_supervisor.rs` | Health、reconnect monitor、graceful shutdown、strategy supervisor | 只提供 typed health/telemetry/shutdown primitives；不内置 GUI、HTTP endpoint 或 process manager。 |
| S21 Slow consumer isolation | `crates/tqsdk-stream/examples/api_contract_s21_slow_consumer_isolation.rs` | bounded fan-out、lag diagnostics、WAL/sink foundation | `spawn_commit_sink`、`StreamSinkOptions`、retry/WAL/recovery/journal types；distributed queue 是 out of scope。 |
| S22 Error diagnosis / retry | `crates/tqsdk-stream/examples/api_contract_s22_error_diagnosis_retry.rs` | retryable errors、backoff、typed diagnostics | `StreamFacadeError::diagnostic`、`StreamRetryPolicy`、retry decisions；business retry audit 属于用户执行系统。 |
| S23 Contract metadata | `crates/tqsdk-session/examples/api_contract_s23_contract_metadata.rs` | instrument specs、contract class、normalized metadata | `SessionClient::query_instrument_specs`、`InstrumentSpec`、`InstrumentClass`；one-shot session query。 |
| S24 Testable strategy | `crates/tqsdk-task/examples/api_contract_s24_testable_strategy.rs` | 不依赖 live services 的策略单测 | `StrategyTestHarness`、`FakeMarket`、`FakeBroker`、`StrategyTestClock`；完整 exchange simulator 是 out of scope。 |
| S25 Wait serial/status | `crates/tqsdk-wait/examples/api_contract_s25_wait_serial_trading_status.rs` | trading status、K-line serial、tick serial | `get_trading_status`、`get_kline_serial`、`get_tick_serial`、`is_changing_fields`；不属于 session/data。 |
| S26 Wait trade/system refs | `crates/tqsdk-wait/examples/api_contract_s26_trade_system_refs.rs`; `crates/tqsdk-wait/examples/api_contract_s26_security_trade_refs.rs` | notifications、settlement、risk refs、security trade refs | Wait live refs 和 `confirm_settlement` 这类 command wrapper；不是 direct query。 |
| S27 Metadata/service query pack | `crates/tqsdk-session/examples/api_contract_s27_metadata_service_queries.rs` | quotes list、main contracts、options、calendar、settlement、ranking、EDB | `SessionClient` typed one-shot metadata/service APIs；不要复制到 wait/stream。 |
| S28 Download/export/Greeks | `crates/tqsdk-data/examples/api_contract_s28_download_export.rs`; `crates/tqsdk-data/examples/api_contract_s28_option_greeks.rs` | history downloads、CSV export、option Greeks | `DataClient` research/download APIs；不是 live session refs。 |
| S29 TargetPos ownership | `crates/tqsdk-task/examples/api_contract_s29_target_pos_ownership.rs` | 同 account+symbol task ownership 和 scheduler ownership | `TaskHost::{target_pos,target_pos_scheduler,check_manual_order_allowed}`；cross-account target-pos orchestration 是用户层能力。 |
| S30 History series cache | `crates/tqsdk-data/examples/api_contract_s30_history_series_cache.rs` | opt-in Python-compatible mmap history cache | `DataClientBuilder::history_cache_enabled`、cache reports、cache-only readers；Python/Rust 同时写是 non-goal。 |
| S31 Low-latency desk | `crates/tqsdk-task/examples/api_contract_s31_low_latency_trading_desk.rs` | same-revision market/trade hot path 和 prechecked orders | `TradingDeskProfile`、`RuntimeReader::read_market_trade_state`、typed latency/order reports；不是 OMS 或 auto-hedger。 |

## 覆盖规则

- 宽回答先覆盖用户角色，再只列出该角色自然涉及的场景。
- active formal examples 是事实来源。Archived scenario files 只是历史上下文，除非正式 example 明确指向它们来说明边界决策。
- 对 S14 要明确说明 desired sketch 只用于保留边界样本；不要暗示已支持。
- 如果某场景的高级功能是 out of scope，要在同一段里说明已支持的 foundation 和用户层责任。
- 如果某工作流没有 contract example，指出最接近的已支持场景，并判断这是 new gap、用户层系统，还是现有 API 的正常组合。
