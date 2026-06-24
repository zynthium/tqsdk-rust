# `tqsdk-data` 最小 API 草图

## 文档定位
本文档描述的是建立在现有 `tqsdk-core + tqsdk-session + replay/history contract` 之上的研究/离线数据工具层。

它回答的是：

- `tqsdk-data` 应该承接哪些能力
- 为什么这些能力不应继续塞进 `tqsdk-session` / `tqsdk-wait` / `tqsdk-stream`
- 第一阶段只起脚手架时，哪些 public surface 先不要冻结

它不回答：

- 具体 downloader 的并发调度细节
- DataFrame / polars 的最终数据形状
- 回测报告、策略分析、GUI 展示

相关文档：

- [总架构入口](README.md)
- [crate 边界审计](crate-boundaries.md)
- [未来 crate 蓝图](crate-blueprint.md)
- [路线图](../../ROADMAP.md)

## 先给结论

`tqsdk-data` 应该是一个独立的研究/离线数据 crate，而不是继续扩大 `tqsdk-session` 或 `tqsdk-wait`。

第一阶段的原则应当非常保守：

1. 先冻结 crate 边界和依赖方向
2. 先提供 compileable crate 骨架
3. 先只开放最薄的一次性研究接口，再逐步扩展

换句话说，当前最稳定的做法不是急着导出一堆研究接口，而是先明确：

- live continuous-consumption 继续留在 `tqsdk-wait` / `tqsdk-stream`
- one-shot direct query 继续留在 `tqsdk-session`
- 离线批量拉取、研究型表格视图、缓存落盘统一放进 `tqsdk-data`

## 为什么不能继续放在现有 crate

### 不是 `tqsdk-core`

`tqsdk-core` 负责的是协议底座、状态树和 commit 语义。

而 `tqsdk-data` 需要承接的是：

- 批量历史数据拉取
- downloader
- 研究型 tabular/view 形状
- 文件缓存与导出

这些都不是 protocol substrate。

### 不是 `tqsdk-session`

`tqsdk-session` 只负责一次性 request/response 和 shared session。

如果把下面这些能力继续塞进去：

- `get_kline_data_page`
- `get_tick_data_page`
- `get_kline_data_series`
- `get_tick_data_series`
- `query_his_cont_quotes`
- `query_option_greeks`
- polars / DataFrame 兼容层

那么 `session` 会从“thin wrapper”变成“研究入口”，边界会明显变胖。

### 不是 `tqsdk-wait` / `tqsdk-stream`

这两层负责的是 diff-backed live 对象消费形状。

而 `tqsdk-data` 面向的是：

- 离线批量请求
- 历史窗口拼接
- 文件落盘
- 表格视图

这不是 `wait_update()` 或 stream/event 模式选择的问题。

## 第一阶段推荐范围

当前已经落地的第一阶段范围是：

- crate skeleton
- README 与 crate-level docs
- 后续实现的依赖方向约束
- `DataClient`
- `DataClientBuilder`
- `DataClient::new()`
- `DataClient::from_session(...)`
- `query_his_cont_quotes(symbols, days, end_date)`
- `query_his_cont_underlyings(symbol, days, end_date)`
- `query_his_cont_underlying_segments(symbol, days, end_date)`
- `query_trading_calendar(start_date, end_date)`
- `query_trading_days(start_date, end_date)`
- `historical_cont_underlying_segments(rows)`
- `HistoricalContQuotesRow`
- `HistoricalContUnderlyingRow`
- `HistoricalContUnderlyingSegment`
- `TradingCalendarRow`
- `get_kline_data_page(KlineDataPageRequest)`
- `get_tick_data_page(TickDataPageRequest)`
- `get_kline_data_series(KlineDataSeriesRequest)`
- `get_tick_data_series(TickDataSeriesRequest)`
- `KlineDataSeries::integrity_report()`
- `TickDataSeries::integrity_report()`
- `DataClientBuilder::history_cache_enabled(true)`
- `DataClientBuilder::history_cache_dir(...)`
- `DataClientBuilder::history_cache_max_bytes(...)`
- `DataClientBuilder::history_cache_retention_days(...)`
- `HistorySeriesCache`
- `HistorySeriesCacheBackend`
- `HistorySeriesCacheReport`
- `HistorySeriesCacheMiss`
- `HistorySeriesCacheScanReport`
- `HistorySeriesCacheFileReport`
- `HistorySeriesCacheFileKind`
- `HistorySeriesCacheFileStatus`
- `HistorySeriesCacheMaintenanceReport`
- `kline_data_download(KlineDataSeriesRequest)`
- `tick_data_download(TickDataSeriesRequest)`
- `KlineDataDownload::collect_remaining()`
- `TickDataDownload::collect_remaining()`
- `query_option_greeks(OptionGreeksRequest)`
- `export_kline_data_csv(KlineDataSeriesRequest, &mut impl AsyncWrite)`
- `export_tick_data_csv(TickDataSeriesRequest, &mut impl AsyncWrite)`
- `KlineDataPage`
- `KlineDataPageRequest`
- `TickDataPage`
- `TickDataPageRequest`
- `KlineDataSeries`
- `KlineDataSeriesRequest`
- `TickDataSeries`
- `TickDataSeriesRequest`
- `DataDownloadProgress`
- `KlineDataDownload`
- `KlineDataDownloadPage`
- `TickDataDownload`
- `TickDataDownloadPage`
- `OptionGreeksRequest`
- `OptionGreeksResult`
- `OptionGreeksRow`
- `KlineCsvExportSummary`
- `TickCsvExportSummary`

当前明确先不做：

- DataFrame / polars public API
- 路径管理型文件导出 API
- 后台 downloader task
- live stream bridge / live serial cache writer
- live durable cache sink daemon/runtime
- 多进程 cache 管理服务与跨进程 daemon orchestration
- Python 与 Rust 进程同时写同一历史序列缓存目录

## 后续能力落点

当开始真正实现 `tqsdk-data` 时，建议能力按下面顺序推进：

1. history/query substrate
   - 复用 `tqsdk-session` 的 one-shot query
   - 复用 `tqsdk-core` 的 market history/chart contract
2. batch fetch surface
   - `get_kline_data_page`
   - `get_tick_data_page`
   - `get_kline_data_series`
   - `get_tick_data_series`
   - `kline_data_download`
   - `tick_data_download`
   - 扩展 `query_his_cont_quotes`
   - `query_his_cont_underlyings` 提供单主连 date -> underlying 映射薄 helper
   - `query_his_cont_underlying_segments` / `historical_cont_underlying_segments` 提供相邻交易日同一 underlying 的 segment 压缩基础能力
   - `query_trading_calendar` / `query_trading_days` 提供交易日历基础能力，供主连分段和回测日期语义复用
   - `query_option_greeks`
3. local materialization
   - 已有最薄的 owned Vec materialization：`collect_remaining`
   - 已有最薄的 `AsyncWrite` CSV export
   - 已有显式 opt-in 的 Python 兼容 mmap 历史序列缓存：
     `DataClientBuilder::history_cache_enabled(true)` 让
     `get_kline_data_series` / `get_tick_data_series` 在同一 API 上隐式读写
     `~/.tqsdk/data_series_1` 或自定义目录；cache miss 使用官方
     `DataSeries` 的 `set_chart` / `focus_datetime` / `left_kline_id`
     下载序列补齐缺口
   - 已有 cache-only history series reader、schema/version scan report、
     typed cache miss，以及最薄的容量/保留期清理策略；Python/Rust 文件格式
     互通但不承诺同目录同时写
   - 后续再考虑路径管理型文件导出
   - deterministic replay / local backtest event source 由 `tqsdk-task` 拥有；
     `tqsdk-data` 只提供 history rows，不提供 JSONL market cache public surface
   - 跨进程 daemon orchestration、跨进程 cache 管理服务、queue/lock/election/recovery/compaction ownership 等编排表面不属于当前 `tqsdk-data` public API
   - 可选 tabular adapters

## 依赖方向

稳定的依赖方向应当是：

```text
tqsdk-core
    ^
    |
tqsdk-session
    ^
    |
-----------------------------
|            |              |
tqsdk-wait  tqsdk-stream  tqsdk-data
                ^
                |
            tqsdk-task
```

这里的关键约束是：

- `tqsdk-data` 可以依赖 `tqsdk-session`
- 如有必要，`tqsdk-data` 也可以直接依赖 `tqsdk-core`
- `tqsdk-session` / `tqsdk-wait` / `tqsdk-stream` 不应反向依赖 `tqsdk-data`

## 当前落地策略

当前仓库里，`tqsdk-data` 已经以“窄 public surface”的方式落地。

其中比较关键的一个点是：

- `get_kline_data_page` / `get_tick_data_page` 已经落在 `tqsdk-data`
- `get_kline_data_series` 已经落在 `tqsdk-data`
- `get_tick_data_series` 也已经落在 `tqsdk-data`
- `DataClientBuilder` / `HistorySeriesCache` 提供显式 opt-in 的 Python 兼容
  mmap 历史序列缓存，也已经落在 `tqsdk-data`
- `HistorySeriesCache::read_kline_data_series` /
  `HistorySeriesCache::read_tick_data_series` 提供 cache-only 读取，
  `HistorySeriesCache::scan` 和 `HistorySeriesCache::enforce_limits`
  提供 schema/损坏报告与容量/保留期维护，也已经落在 `tqsdk-data`
- `kline_data_download` / `tick_data_download` 也已经落在 `tqsdk-data`
- `query_option_greeks` 也已经落在 `tqsdk-data`
- `tqsdk-data` 不提供 `MarketCacheEvent` / `MarketCacheWriter` /
  `MarketCacheReader` / `MarketCacheReplay` 这类 JSONL market cache public API；
  deterministic replay / local backtest 输入属于 `tqsdk-task`
- live stream pipe、stream feature、跨进程 cache service、daemon/supervisor orchestration 和 live hot-path cache dependency 均不属于当前 `tqsdk-data` public API。
- `tqsdk-data` 不再提供 `LiveHistoryCacheWriter`；live diff consumption 留在
  `tqsdk-stream`，hot-path persistence 由调用方或独立上层服务拥有，Python-compatible
  mmap history cache 只服务 `get_*_data_series` 的离线时间范围读取
- queue、lock、reader manifest、recovery scan、writer election、compaction
  ownership、service、daemon 和 supervisor 等编排表面已经从当前 public API
  回退；它们不属于 `tqsdk-data` 的稳定边界
- `KlineDataSeries` / `TickDataSeries` 到 replay event/source 的 adapter
  已经移到 `tqsdk-task::StrategyReplaySourceBuilder`
- `HistoryIntegrityReport` 已经落在 `tqsdk-data`，作为 owned history series 的本地质量报告：
  K 线按 duration 做 calendar-agnostic cadence 缺口检查，tick 不假设固定间隔；
  报告只暴露 requested/returned range、缺口、重复行、时间倒退、越界行、
  cache hit/miss/downloaded 和权限检查状态，不绑定外部数据库或 tabular 框架
- 它不是新的 session facade
- 它也不是 live ref / live stream；当前没有 stream window 写 mmap history cache
  bridge，也不拥有 live 消费 facade
- `data_page` 是对底层 chart/history contract 的显式单页封装
- `data_series` 是建立在 `data_page` 之上的时间范围快照封装，语义固定为 `[start_datetime_ns, end_datetime_ns)`
- `data_download` 是建立在同一时间范围语义上的 pull-based 渐进式下载 substrate
- `data_page` 会保留 chart 的 `more_data`，上层分页不能用“当前页行数小于 view_width”来推断远端结束
- `data_series` / `data_download` 的终止条件统一为：`more_data=false`、无 next id、next id 重复，或已推进到请求窗口右边界
- `query_option_greeks` 内部复用了 session-backed 的一次性 live quote snapshot，但暂时没有把这层 snapshot helper 冻结成新的 public surface
- 当依赖的 live quote symbols 缺少行情权限时，`query_option_greeks` 会尽早返回 permission error，而不是等订阅超时
- `query_option_greeks` 对 live quote price 会做 best-effort canonicalization：优先 `last_price`，缺失时回退到盘口中间价 / 单边盘口 / `pre_close`
- `collect_remaining` 是建立在 `data_download` 之上的最薄 owned Vec materialization helper，只收集尚未消费的剩余页，不新增后台任务或缓存语义
- `export_*_csv` 是建立在 `data_download` 之上的纯 async materialization helper，本身不拥有路径、缓存或后台线程语义
- 历史序列 mmap cache 与 Python `DataSeries` 文件格式兼容，适合迁移和交替使用；
  同目录同时写仍是 non-goal，因为 Python 官方实现自身也没有承诺同一合约周期
  多进程/线程/协程并发写
- 跨进程 cache 管理若后续需要，应作为用户 tooling 或独立 service 重新设计，
  而不是把 live session、进程管理、HTTP endpoint、GUI 或底层文件编排表面
  下沉进 `tqsdk-data`
- async history 相关入口会主动获取 auth context 并校验 `tq_dl`，避免把权限问题拖到 chart/websocket timeout
- `data_download` 这类同步构造入口仍然只做 best-effort 预检，真正的 async 读取阶段会再次强校验
- 默认 SHFE 历史示例会显式切到 `futures_market()`，避免把 futures history 请求发到 stock market route

这样做的意义是：

- 不把研究/批量历史接口继续塞进 `tqsdk-session`
- 不把历史数据读取和 `wait_update()` / stream 模式耦合在一起
- 给 file writer / export / dataframe 预留稳定的 `page -> download -> materialization` 递进路径
- 后续可以在 `tqsdk-data` 上继续叠加路径管理型文件导出、tabular adapters 或
  history cache maintenance tooling，而不污染 core/session/live facade 的边界；
  deterministic replay / local backtest event source 继续留在 `tqsdk-task`

这样做的收益是：

- 先给研究/下载能力一个明确落点
- 不用为了继续扩功能而过早冻结宽 API
- 后续实现时不需要重新讨论能力归属

## 最终判断

`tqsdk-data` 值得独立存在，但当前阶段最合理的动作是：

1. 先保持 `DataClient + query_his_cont_quotes` 足够窄
2. 在此基础上继续保持 `DataClient + data_page + data_series + data_download` 也只是底层 substrate
3. 继续按 history/query -> batch fetch -> materialization/history cache 的顺序迭代；deterministic replay / local backtest 输入由 `tqsdk-task` 拥有，后续重点是路径管理型 materialization，跨进程 cache 管理服务不作为当前核心 public API 推进
4. 避免为了兼容 DataFrame 形状而提前做宽 surface
