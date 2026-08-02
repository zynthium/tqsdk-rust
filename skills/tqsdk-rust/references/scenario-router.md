# 场景路由

先使用本文件。按用户想持有或消费的对象分类。Python TqSdk 是语义参考，但 Rust 会把 Python 单体 `TqApi` surface 拆到不同 crate。

## 目录

- 快速决策
- 各场景调用模式
- 仍不明确时

## 快速决策

| 用户说 | 大概率需要 | 入口 / Crate | 主要调用 |
| --- | --- | --- | --- |
| "普通策略", "默认入口", "先跑起来", "目标持仓", "轻量历史", "一个 crate" | Ordinary strategy facade | `tqsdk` | `tqsdk::prelude::*`, `Tq::futures`, `auth_env`, `trade_target_tqkq`, `connect`, `quote`, `target_pos_tqkq`, `next`, `history` |
| "实时行情", "quote", "批量订阅", "盘口", "价格变化", "按 selector 订阅" 且没有明确要内部 crate | Default live strategy loop | `tqsdk` first; `tqsdk-wait` only when explicit Python-style wait API is needed | `Tq::futures`, `quote`, `quotes`, `quotes_universe`, `next`, `QuoteRef::load`; advanced wait: `TqApiBuilder`, `step`, `WaitStep::is_changing` |
| "像 Python TqApi", "wait_update", "is_changing", "step_until" | Explicit Python-style live quote/trade loop | `tqsdk-wait` | `TqApiBuilder`, `quote`, `quotes`, `step`, `WaitStep::is_changing`, `QuoteRef::load`, `QuoteSet::changed_snapshot` |
| "K线 serial", "tick serial", "窗口", "bar 更新", "trading_status" | Live serial/window/status view | `tqsdk-wait` | `kline`, `tick`, `trading_status`, `step_until`, window/ref load methods |
| "多消费者", "事件流", "stream", "fan-out", "异步管道", "lag" | Caller-owned event/fan-out layer | `tqsdk-session + tqsdk-core` | `SessionClient`, `progress_once`, `RuntimeReader`, `UpdateCursor`, caller-owned filters/channels/lag diagnostics |
| "查合约", "查品种", "合约列表", "所有合约代码", "主连", "连续合约", "期权链", "交易日历", "结算价", "排名", "EDB", "schema", "metadata" | One-shot metadata/service query | `tqsdk-session` | `SessionClientBuilder`, `enable_query`, `query_quotes`, `query_instrument_specs`, `query_cont_quotes`, `get_trading_calendar` |
| "下单", "撤单", "目标持仓", "调仓", "策略下单", "风控", "scheduler", "多账户", "fake broker" | Strategy execution layer | `tqsdk` for ordinary target-position path; `tqsdk-task` when ownership/risk/task internals are needed; `tqsdk-wait` for thin direct order wrappers | `Tq::target_pos_tqkq`, `TargetPos`; advanced: `TaskHost`, `TargetPosTask`, `RiskEngine`, typed order builders, `OrderTicket`, strategy/test harness APIs |
| "回测", "策略回测", "TqBacktest", "TqSim", "本地模拟账户", "同一策略跑实盘和回测" | Strategy backtest | `tqsdk` first for ordinary same-body facade; `tqsdk-wait` for explicit Python-style wait builder; `tqsdk-task` + `tqsdk-data` for local deterministic internals | facade: `.backtest(...)`, `.backtest(...).cache_dir(...)`, `.replay_backtest(...)`, `quote_symbol`, `price_tick`, `Tq::next`, `backtest_summary`; wait: `TqApiBuilder::futures_backtest`, `TqBacktest`, `step`; local internals: `StrategyBacktest`, `TqSim`, `ReplayMarketSource`, `finish_sim_step` |
| "实时 tick 写缓存", "record_ticks", "record_universe", "维护指定合约持久化 tick 缓存", "维护 selector 集合缓存", "实盘增量填充回测缓存" | Shared live/backtest tick cache | `tqsdk` first; `tqsdk-data` only for pure row writer | facade: `MarketCachePolicy::new(cache_dir).record_ticks(symbols)` 或 `.record_universe(expression)?`, `TqBuilder::market_cache(policy)`, `Tq::record_ticks(cache_dir, symbols)`, `record_ticks_health`, `recorded_market_cache_policy`, `Tq::next`; data writer: `LiveTickCacheWriter::push_ticks` |
| "通过回测/回测缓存取历史", "按区间读取回测 Tick/K线", "优先 official backtest stream" | Cache-backed backtest history rows | `tqsdk::advanced::data` | `BacktestHistoryClient::builder`, `BacktestHistoryPolicy::{RemoteOnMiss, CacheOnly}`, `BacktestHistoryRequest::{tick, kline}`, `query`, `collect` / chunk events |
| "缓存盘点", "cache inventory", "补历史缓存", "cache fill", "cache verify", "cache doctor" | Historical tick cache operator workflow | optional `tqsdk-cache` binary | `inventory`, `inspect`, closed-day `fill`, `verify --report`, `doctor`; 默认摘要、`--output-format json` 按需 JSON，远端 miss 才需 auth，不提供 daemon/purge/refresh/compact |
| "监控面板", "dashboard", "latency", "历史缓存统计", "订单监控" | Caller-owned observability | `tqsdk` facade cache APIs or `tqsdk-data`; relay dashboard only with relay | `.inspect_cache()`、`.warmup()`、`record_ticks_health()`、`BacktestTickCache::inventory()`；通用 dashboard、告警和进程管理由调用方 sidecar 提供 |
| "历史K线", "历史 tick", "下载", "CSV", "离线研究", "缓存", "回放", "Greeks", "data_series"，但未明确优先回测缓存 | Historical/offline research | `tqsdk-data` for generic rows/cache/export; `tqsdk-task` for replay source | data: `DataClient`, `get_*_data_series`, `*_data_download`, `export_*_csv`, `HistorySeriesCache`, `BacktestTickCache`; task replay: `ReplayMarketSource`, `StrategyReplaySourceBuilder` |
| "低延迟", "同一 revision", "cursor", "commit", "runtime", "adapter", "command status" | Low-level substrate or custom facade | `tqsdk-session` plus `tqsdk-core` | `SessionClient`, `progress_once`, `RuntimeReader`, `cursor`, `read_market_trade_state` |

请求涉及角色覆盖或 public API 证据时，继续读 `references/scenario-contracts.md`，并把回答锚定到对应 `api_contract_sXX_*.rs` 示例。

## 各场景调用模式

### 1. 在策略循环中监控一个合约

普通策略使用 `tqsdk`；只有用户明确要 Python-style wait API 时使用 `tqsdk-wait`。

调用顺序：

1. 用 `Tq::futures().auth_env()?.connect().await?` 构造默认 facade。
2. 调用 `quote(symbol).await`。
3. 循环调用 `next().await?`。
4. commit 后加载 quote snapshot。把 ref 当作 live handle，不要当作 owned snapshot。
5. 明确需要 `WaitStep::is_changing()` 时，下钻 `tqsdk-wait`。

继续读：`references/code-patterns.md` 的 Default Tq Strategy Loop 或 Wait Quote Loop 示例。

### 2. 策略启动前查询合约 metadata

使用 `tqsdk-session`。

调用顺序：

1. 用正确 market route 构造 `SessionClient`。`SessionClientBuilder::build()` 是同步的。
2. 需要官方 query 语义时添加 `enable_query()`。
3. 调用具体 one-shot helper。
4. 如果已经在 wait facade 或 session-backed loop 中，复用 shared session，不要另建 query client。

典型 helper：`query_symbol_info`、`query_instrument_specs`、`query_quotes`、`query_cont_quotes`、`query_options`、`query_atm_options`、`query_all_level_options`、`query_all_level_finance_options`、`get_trading_calendar`、`query_symbol_settlement`、`query_symbol_ranking`、`query_edb_data`。

品种/合约查询映射：

- 某交易所某品种的合约代码列表：`query_quotes(Some("FUTURE"), Some("SHFE"), Some("au"), Some(false), None)`。
- 主连/连续合约：`query_cont_quotes(Some("SHFE"), Some("au"), None)`。
- 已知合约的窄规格字段：`query_instrument_specs(&symbols)`；完整官方合约信息表
  typed 结果用 `query_symbol_info(&symbols)` / `SymbolInfo`，包括交易时间段、
  涨跌停、昨结算、开仓限额、到期/行权字段。
- 期权链：`query_options(underlying, &OptionQueryFilter::new())`；按 ATM 或档位查询用 `query_atm_options`、`query_all_level_options`、`query_all_level_finance_options`。

### 3. 构建 live data bus

使用 `tqsdk-session + RuntimeReader/UpdateCursor` 作为 SDK substrate；event bus、fan-out、慢 consumer 隔离和持久化 sidecar 属于调用方集成层。

调用顺序：

1. 构造 `SessionClient`，显式 subscribe/login。
2. 用 `session.reader().clone()` 创建 shared `RuntimeReader`。
3. 每个 consumer 自己持有 `UpdateCursor`，用 `reader.next(&mut cursor)` 消费 commit 边界。
4. 用 `read_market_state()` / `read_trade_state()` / `read_market_trade_state()` 读取需要的分区，不要 clone full snapshot。
5. 过滤、bounded channel、lag/closed/error report、重放和持久化 sidecar 都由调用方实现。
6. metadata 使用同一个 session 的 query support，不要另开 query client。
7. 指定 symbol 或 selector 集合的 live tick 要写入回测共享缓存时，普通策略优先使用 `MarketCachePolicy` + `.market_cache(...)`，运行中临时开启用 `Tq::record_ticks(cache_dir, symbols)`；泛化 live events/K 线/commit persistence 使用调用方自有 sidecar。

### 4. 实现 target-position 策略

普通 target-position 策略使用 `tqsdk`。需要 execution ownership、risk gate、scheduler、multi-account 或 test harness 内部能力时使用 `tqsdk-task`。

调用顺序：

1. 普通路径用 `Tq::futures().trade_target_tqkq().connect().await?`。
2. 创建 `target_pos_tqkq(symbol).await?`。
3. 用 `Tq::next()` 推进策略。
4. 高级路径从 `tqsdk-wait` 构造 `TqApi`，包装成 `TaskHost`。
5. 需要时配置 `RiskEngine`，创建 `TargetPosTask` 或 typed order builder。
6. 让 `TaskHost::wait_update()` 推进 task。
7. 使用 typed tickets/reports，不要解析 status 字符串。
8. real-account order placement 必须 opt-in；示例要显式展示副作用。

### 5. 获取历史数据用于研究或回测输入

用户明确要求优先复用回测或回测缓存时，先使用 `tqsdk::advanced::data::BacktestHistoryClient`：

1. 用 `BacktestHistoryClient::builder(cache_dir)` 构造 client；首次或有缺口时选 `BacktestHistoryPolicy::RemoteOnMiss` 并配置 `.auth_env()`。
2. 用 `BacktestHistoryRequest::tick(...)` 或 `.kline(...)` 声明半开区间 `[start_ns, end_ns)`。
3. 调用 `query()` 消费 chunk/terminal event，或单请求使用 `collect()` 拿 owned rows。命中缓存不联网；缺口通过官方 server-side backtest stream 补齐并写回同一 root。
4. 成功预热后，普通 reader 改用 `BacktestHistoryPolicy::CacheOnly`，让缺口显式失败。
5. 这是 raw history rows 路径；策略回放仍用 `.backtest(start_ns, end_ns)`。

只有用户明确需要 generic history download、page/series、CSV export、Greeks，或来源/周期不受回测缓存合同覆盖时，才使用 `tqsdk-data::DataClient` 或 `DataClient::from_session(session)`。输出保持 owned/materialized；不要建模成 live refs。确定性策略测试尽量用 task-owned replay source 或 fake harness，而不是 live credentials。`HistorySeriesCache` 只用于 offline data-series cache；如果用户要求指定 live tick 或 selector 集合写入回测共享缓存，路由到 `MarketCachePolicy` / `Tq::record_ticks(...)` 或 `LiveTickCacheWriter`。如果要求 live K 线/任意 window/commit 写入持久化，说明当前 SDK 不提供这个 public API，使用调用方 sidecar。

### 6. 运行策略回测

按用户想要的回测形态分三条入口：

1. 普通用户想让同一段 `Tq::next()` / `quote()` 策略主体跑 live、官方服务端补洞的 cache-backed 本地回测或自定义 replay 回测时，优先使用默认 `tqsdk` facade：统一用 `.backtest(start_ns, end_ns)`；它默认共享持久 tick cache + `RemoteOnMiss`，已知或静态解析 symbol 的完整 cache 不需 auth，缺口才用官方 server-side stream 补齐并落盘。`.cache_dir(...)` 或 `.market_cache(...)` 覆盖 cache；`.cache_only()` 用于预热后的严格本地消费者；只有 `.disabled_cache()` 才直接使用官方服务端行情且不落盘。多客户端用一个定时 `.warmup()` writer，避免重复远端填充；自定义 replay 用 `.replay_backtest(replay)`，kline replay 需要 `price_tick(symbol, tick)`。不要使用 `server_backtest(...)`，它已经不是 public API。契约锚点是 S37-S39、S43-S47。
2. 如果用户明确要像 Python `TqApi(backtest=TqBacktest(...))` 那样直接操作 wait facade，使用 `tqsdk-wait` 的 `TqApiBuilder::{futures_backtest,stock_backtest}` 或 `TqBacktest`。策略主体只依赖 `quote` / `kline` handles 和 `step()`；backtest 结束时 `step()` 返回 `None`。契约锚点是 S36。
3. 如果用户要不连接真实服务、用本地历史行情或显式 replay event 和 Python-compatible `TqSim` 撮合账户跑确定性回测内部能力，使用 `tqsdk-task::{ReplayMarketSource,StrategyBacktest,TqSim}`；历史 rows 可由 `tqsdk-data` 拉取并通过 `StrategyReplaySourceBuilder` 转成 replay source。契约锚点是 S32。
4. 如果只是准备历史输入、导出或缓存，才单独路由到 `tqsdk-data`；不要把“策略回测”回答成单纯历史下载。
5. 当前本地 `StrategyBacktest` 最小闭环支持 quote/tick/kline replay event、futures 单账户、基础限价/市价撮合、保证金和手续费配置、kline `price_tick(...)` quote synthesis 和轻量 `summary()`；完整回测报告、自动分钟线、主连合约表、股票/期权完整账户语义仍是后续范围。

### 7. 编写低延迟自定义循环

使用 `tqsdk-session + RuntimeReader`；如果任务偏 execution，用 `tqsdk-task` trading desk profile。

调用顺序：

1. 构造 `SessionClient`，并显式 subscribe/login。
2. 用 `progress_once` 或 profile loop primitive 推进。
3. 用 `RuntimeReader::cursor()` / `next()` 消费 commits。
4. 通过 `read_market_state`、`read_trade_state` 或 `read_market_trade_state` 读取 hot state。
5. logging、journal、metrics 不要进入 hot decision path。
6. 不要在 runtime mutation path 之外修改 domain state。

## 仍不明确时

只问一个按形状分类的问题，而不是实现细节：

“你需要一个带 refs 的 live loop、多个事件消费者、one-shot metadata query、trading task abstraction，还是 historical/offline data？”
