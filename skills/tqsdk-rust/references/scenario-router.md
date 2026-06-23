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
| "实时行情", "quote", "批量订阅", "盘口", "价格变化" 且没有明确要内部 crate | Default live strategy loop | `tqsdk` first; `tqsdk-wait` only when explicit Python-style wait API is needed | `Tq::futures`, `quote`, `quotes`, `next`, `QuoteRef::load`; advanced wait: `TqApiBuilder`, `step`, `WaitStep::is_changing` |
| "像 Python TqApi", "wait_update", "is_changing", "step_until" | Explicit Python-style live quote/trade loop | `tqsdk-wait` | `TqApiBuilder`, `quote`, `quotes`, `step`, `WaitStep::is_changing`, `QuoteRef::load`, `QuoteSet::changed_snapshot` |
| "K线 serial", "tick serial", "窗口", "bar 更新", "trading_status" | Live serial/window/status view | `tqsdk-wait` | `kline`, `tick`, `trading_status`, `step_until`, window/ref load methods |
| "多消费者", "事件流", "stream", "fan-out", "异步管道", "lag" | Multi-consumer event pipeline | `tqsdk-stream` | `TqStreamBuilder`, `commit_stream`, filters, `quote_batches`, `quote_stream`, `market_events`, row-batch kline/tick streams, trade/session event streams |
| "查合约", "查品种", "合约列表", "所有合约代码", "主连", "连续合约", "期权链", "交易日历", "结算价", "排名", "EDB", "schema", "metadata" | One-shot metadata/service query | `tqsdk-session` | `SessionClientBuilder`, `enable_query`, `query_quotes`, `query_instrument_specs`, `query_cont_quotes`, `get_trading_calendar` |
| "下单", "撤单", "目标持仓", "调仓", "策略下单", "风控", "scheduler", "多账户", "fake broker" | Strategy execution layer | `tqsdk` for ordinary target-position path; `tqsdk-task` when ownership/risk/task internals are needed; `tqsdk-wait` for thin direct order wrappers | `Tq::target_pos_tqkq`, `TargetPos`; advanced: `TaskHost`, `TargetPosTask`, `RiskEngine`, typed order builders, `OrderTicket`, strategy/test harness APIs |
| "回测", "策略回测", "TqBacktest", "TqSim", "本地模拟账户", "同一策略跑实盘和回测" | Strategy backtest | `tqsdk` first for ordinary same-body facade; `tqsdk-wait` for explicit Python-style wait builder; `tqsdk-task` + `tqsdk-data` for local deterministic internals | facade: `TqBuilder::backtest`, `local_backtest`, `quote_symbol`, `price_tick`, `Tq::next`, `backtest_summary`; wait: `TqApiBuilder::futures_backtest`, `TqBacktest`, `step`; local internals: `StrategyBacktest`, `TqSim`, `ReplayMarketSource`, `finish_sim_step` |
| "历史K线", "历史 tick", "下载", "CSV", "离线研究", "缓存", "回放", "Greeks", "data_series" | Historical/offline research | `tqsdk-data` for rows/cache/export; `tqsdk-task` for replay source | data: `DataClient`, `get_*_data_series`, `*_data_download`, `export_*_csv`, `HistorySeriesCache`; task replay: `ReplayMarketSource`, `StrategyReplaySourceBuilder` |
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
4. 如果已经在 wait/stream 中，复用 `api.session()` 或 `stream.session()`。

典型 helper：`query_symbol_info`、`query_instrument_specs`、`query_quotes`、`query_cont_quotes`、`query_options`、`query_atm_options`、`query_all_level_options`、`query_all_level_finance_options`、`get_trading_calendar`、`query_symbol_settlement`、`query_symbol_ranking`、`query_edb_data`。

品种/合约查询映射：

- 某交易所某品种的合约代码列表：`query_quotes(Some("FUTURE"), Some("SHFE"), Some("au"), Some(false), None)`。
- 主连/连续合约：`query_cont_quotes(Some("SHFE"), Some("au"), None)`。
- 已知合约的窄规格字段：`query_instrument_specs(&symbols)`；完整官方合约信息表
  typed 结果用 `query_symbol_info(&symbols)` / `SymbolInfo`，包括交易时间段、
  涨跌停、昨结算、开仓限额、到期/行权字段。
- 期权链：`query_options(underlying, &OptionQueryFilter::new())`；按 ATM 或档位查询用 `query_atm_options`、`query_all_level_options`、`query_all_level_finance_options`。

### 3. 构建 live data bus

使用 `tqsdk-stream`。

调用顺序：

1. 构造 `TqStream`。
2. 订阅或选择 typed stream/event API。
3. 能过滤时使用 commit/path/scope/domain/object/field filters。
4. 慢持久化放在调用方自有 sidecar。
5. metadata 使用 `stream.session()`，不要另开 query client。
6. 显式处理 lag/closed/error report；fan-out 是 bounded。
7. 需要持久化 live events 时，使用调用方自有 sidecar；不要把 Python-compatible history mmap cache 接入 live 热路径。

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

### 5. 下载历史数据用于研究或回测输入

使用 `tqsdk-data`。

调用顺序：

1. websocket-backed history 需要 session 时，构造或复用 session。
2. 创建 `DataClient` 或 `DataClient::from_session(session)`。
3. 选择 page、series、download、CSV export、cache 或 replay API。
4. 输出保持 owned/materialized；不要建模成 live refs。
5. 确定性策略测试尽量用 task-owned replay source 或 fake harness，而不是 live credentials。
6. `HistorySeriesCache` 只用于 offline data_series mmap cache；如果用户要求 live window 写入该缓存，说明当前 SDK 不提供这个 public API。

### 6. 运行策略回测

按用户想要的回测形态分三条入口：

1. 普通用户想让同一段 `Tq::next()` / `quote()` 策略主体跑 live、服务端回测或本地回测时，优先使用默认 `tqsdk` facade：服务端路径用 `TqBuilder::backtest(start_ns, end_ns)`，本地路径用 `TqBuilder::local_backtest(replay)`，kline replay 需要 `price_tick(symbol, tick)`。契约锚点是 S37-S39。
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

“你需要一个带 refs 的 live loop、multi-consumer event stream、one-shot metadata query、trading task abstraction，还是 historical/offline data？”
