# `tqsdk-data`

`tqsdk-data` 是 `tqsdk-rust` workspace 里预留给研究、离线数据和批量拉取能力的 crate。

当前阶段它只开放几层很窄的能力：

- `DataClient::new().query_his_cont_quotes(...)`
- `DataClient::new().query_his_cont_underlyings(...)`
- `DataClient::new().query_his_cont_underlying_segments(...)`
- `DataClient::new().query_trading_calendar(...)`
- `DataClient::new().query_trading_days(...)`
- `historical_cont_underlying_segments(...)`
- `DataClient::from_session(...).get_kline_data_page(...)`
- `DataClient::from_session(...).get_tick_data_page(...)`
- `DataClient::from_session(...).get_kline_data_series(...)`
- `DataClient::from_session(...).get_tick_data_series(...)`
- `KlineDataSeries::integrity_report()`
- `TickDataSeries::integrity_report()`
- `DataClientBuilder::new().history_cache_enabled(true).build()?.get_kline_data_series(...)`
- `BacktestTickCache::open(...).store_ticks(...)`
- `BacktestTickCache::open(...).load_series(...)`
- `DataClient::from_session(...).kline_data_download(...)`
- `DataClient::from_session(...).tick_data_download(...)`
- `KlineDataDownload::collect_remaining()`
- `TickDataDownload::collect_remaining()`
- `DataClient::from_session(...).query_option_greeks(...)`
- `DataClient::from_session(...).export_kline_data_csv(...)`
- `DataClient::from_session(...).export_tick_data_csv(...)`

其中：

- `query_his_cont_quotes` / `query_his_cont_underlyings` / `query_his_cont_underlying_segments` 是纯 HTTP 的一次性 direct query，不需要 live session；分别返回多主连表格、单主连 date -> underlying 映射，以及同一 underlying 相邻交易日压缩后的连续 segment
- `query_trading_calendar` / `query_trading_days` 复用同一份交易日历 HTTP 数据，分别返回自然日交易标记和只含交易日的列表
- `get_*_data_page` 是最底层的 chart/history page substrate，并显式暴露 chart 的 `more_data` 分页信号
- `get_*_data_series` 是建立在 page substrate 之上的时间范围历史快照，语义对齐官方 `data_series`，范围为 `[start_datetime_ns, end_datetime_ns)`，分页继续与否以 `more_data` 为准
- `integrity_report()` 是对已返回 owned series 的本地质量报告；K 线按 duration 做
  calendar-agnostic cadence 缺口检查，tick 不假设固定间隔
- `DataClient::from_session(...)` 默认不启用历史序列缓存；通过
  `DataClientBuilder::history_cache_enabled(true)` 显式开启后，
  `get_*_data_series` 会隐式读写 `HistorySeriesCache`
- 未指定缓存目录时使用 `~/.tqsdk/data_series_1`；可以通过
  `DataClientBuilder::history_cache_dir(...)` 指定目录
- 可通过 `DataClientBuilder::history_cache_max_bytes(...)` 和
  `history_cache_retention_days(...)` 配置最薄的容量/保留期清理策略
- `HistorySeriesCache` 是稳定 facade，底层通过 `HistorySeriesStore` 抽象隔离；
  默认 `BinaryHistorySeriesStore` 继续使用 Python `DataSeries` 兼容的文件名与二进制列布局：
  `symbol.duration_ns.start_id.end_id`，并通过 mmap 读取大文件窗口
- Python/Rust 可交替使用默认二进制 store 的同一目录文件，但首版不承诺同目录
  同时写；Python 官方 `DataSeries` 本身也不支持同一合约周期多进程/线程/协程并发写
- `HistorySeriesCache::read_kline_data_series` /
  `HistorySeriesCache::read_tick_data_series` 是显式 cache-only reader，
  缺口返回 typed `DataError::CacheMiss`，不会联网补齐
- `BacktestTickCache` 是 tick-only semantic facade，复用同一个
  `HistorySeriesCache` / `HistorySeriesStore`，用于回测覆盖检查、tick 写入和 tick
  replay 读取；它不持久化 K 线，也不引入第二套 tick cache 文件格式
- `HistorySeriesCache::scan()` 输出 schema version、segment 状态、未完成写入
  和 row-width 损坏报告；首版不额外写 manifest 文件，以保持 Python 目录互通
- cache miss 复用官方 `DataSeries` 的 `set_chart` 序列：首包使用
  `focus_datetime=start_datetime_ns`、`focus_position=0`、`view_width=2000`，
  后续用 `left_kline_id=current_id` 翻页，结束后释放 chart
- `*_data_download` 是纯 async、pull-based 的范围下载 substrate，按页推进，不内建文件写盘或后台线程，终止条件同样以远端 chart pagination signal 为准，而不是用当前页行数推断
- `KlineDataDownload::collect_remaining()` / `TickDataDownload::collect_remaining()` 是最薄的 owned Vec materialization helper，只收集尚未消费的剩余页
- `query_option_greeks` 是一次性 owned 研究接口，内部会临时拉起 live quote snapshot 并做本地 Black-Scholes / 隐波计算
- `export_*_csv` 是建立在 `*_data_download` 之上的纯 async materialization helper，要求调用方提供 `AsyncWrite`
- async history 入口会主动拉取 auth context 并校验 `tq_dl`，避免把权限错误拖到 websocket timeout
- `kline_data_download` / `tick_data_download` 这类同步构造入口仍然只做 best-effort 预检，真正的 history 读取会在首个 async page/export 调用时再次强校验
- 当 `query_option_greeks` 依赖的 live quote symbols 缺少行情权限时，也会在 facade 层尽早拒绝，而不是等到订阅超时
- `query_option_greeks` 对 live quote price 会做 best-effort canonicalization：优先 `last_price`，缺失时回退到买一卖一中间价 / 单边盘口 / `pre_close`

除此之外，它仍然刻意保持极窄，不提前承诺宽 public API。

## 依赖方式

Cargo 包名是 `tqsdk-data`，代码里的 crate 路径是 `tqsdk_data`。

正式发布到 crates.io 前，workspace 外项目可以先使用 Git dependency：

```toml
[dependencies]
tqsdk-data = { git = "https://github.com/zynthium/tqsdk-rust" }
tokio = { version = "1", features = ["fs", "macros", "rt", "time"] }
```

在本仓库内做 crate 间开发时使用 `path = "../tqsdk-data"`；正式发布后把 Git
dependency 换成版本号即可。默认 feature 包含 live history/query 与 service query
支持；本 crate 不提供 live bridge，也不为实时行情热路径引入
Python-compatible mmap 缓存。

## 当前已稳定的 surface

- `DataClient`
- `DataClientBuilder`
- `HistoricalContQuotesRow`
- `KlineDataPageRequest`
- `KlineDataPage`
- `TickDataPageRequest`
- `TickDataPage`
- `KlineDataSeriesRequest`
- `KlineDataSeries`
- `TickDataSeriesRequest`
- `TickDataSeries`
- `HistoryIntegrityCheck`
- `HistoryIntegrityReport`
- `HistoryCacheStatus`
- `HistoryPermissionStatus`
- `BacktestCachePolicy`
- `BacktestTickCache`
- `BacktestTickCoverage`
- `BacktestTickCacheWriteReport`
- `HistorySeriesCache`
- `HistorySeriesCacheBackend`
- `HistorySeriesCacheReport`
- `HistorySeriesCacheMiss`
- `HistorySeriesCacheScanReport`
- `HistorySeriesCacheFileReport`
- `HistorySeriesCacheFileKind`
- `HistorySeriesCacheFileStatus`
- `HistorySeriesCacheMaintenanceReport`
- `HistorySeriesStore`
- `HistorySeriesCoverageRequest`
- `HistorySeriesCoverageReport`
- `HistorySeriesKind`
- `HistorySeriesReadRequest`
- `HistorySeriesReader`
- `HistorySeriesRow`
- `HistorySeriesSegmentReport`
- `HistorySeriesWriteRows`
- `HistorySeriesWriteSegment`
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

## `data_page` / `data_series` / `data_download` 的定位

这几层接口适合承接：

- 历史 K 线 / tick 一次性拉取
- page 级分页读取
- 按时间范围组装完整历史序列
- 显式 opt-in 的 `HistorySeriesCache` 历史序列缓存
- tick-only `BacktestTickCache` 回测加速 facade
- 大时间范围按页推进的批量读
- research/offline 侧的渐进式 materialization
- 后续更高层 CSV writer / DataFrame / polars / downloader tool 的底座

它当前明确不做：

- live 自动推进
- 引用型 diff-backed 对象
- `wait_update()` API
- callback / fan-out API

这些仍然属于 `tqsdk-wait` 或调用方自建 reader/cursor 消费层。

`KlineDataSeries::integrity_report()` / `TickDataSeries::integrity_report()`
提供最薄的数据质量报告，包括 requested/returned range、缺口、重复行、时间倒退、
越界行、cache hit/miss/downloaded 状态和权限检查状态。它只检查 SDK 已经拿到的
owned rows，不联网、不读取额外 calendar，也不绑定 DolphinDB、Parquet 或 DataFrame。

## 后续仍应承接的能力

- 路径管理型导出与落盘
- 可选的 DataFrame / polars 适配层

当前“文件导出、落盘、历史序列缓存”已经有最薄的一层：

- `KlineDataDownload::collect_remaining`
- `TickDataDownload::collect_remaining`
- `export_kline_data_csv`
- `export_tick_data_csv`
- `DataClientBuilder::history_cache_enabled(true)`
- `HistorySeriesCache::open(...)`
- `HistorySeriesCache::read_kline_data_series(...)`
- `HistorySeriesCache::read_tick_data_series(...)`
- `HistorySeriesCache::scan()`
- `HistorySeriesCache::enforce_limits(...)`
- `BacktestTickCache::open(...)`
- `BacktestTickCache::store_ticks(...)`
- `BacktestTickCache::load_series(...)`

但它仍然只负责把下载结果收敛到调用方可接管的 `Vec`、写入调用方给定的
`AsyncWrite`，或在 `get_*_data_series` 上复用 Python 兼容历史序列缓存；
不负责 live serial 缓存、后台 downloader、GUI viewport 状态、Python/Rust 同目录
同时写、跨进程 cache service 或高频交易 hot path。

## 当前明确不做

- live session owner
- `wait_update()` facade
- event/fan-out facade
- task runtime
- 回测报告与 GUI

## 当前关于 live quote snapshot 的取舍

`query_option_greeks` 依赖一次性 live quote snapshot，但这块底层能力目前仍然保持为 crate 内部实现，没有单独冻结为 public API。

原因是现在的 quote 订阅 contract 还是 shared-session 全局集合语义：

- 内部 helper 可以安全地为 `query_option_greeks` 服务
- 但如果直接公开成通用 snapshot API，就必须同时明确“临时订阅是否自动撤销”“与其他 live consumer 如何共存”这类更稳定的语义

当前阶段先把研究接口落地，而不提前承诺一层还不够干净的通用 market snapshot surface。

## 为什么现在先保持极窄

因为这层一旦开始对外暴露研究型 API，就很容易把：

- 批量下载
- tabular 视图
- 文件缓存
- 兼容层

一起绑进第一版 surface。

当前更稳的做法，是先把能力边界固定在独立 crate 里，等具体实现时再按阶段逐步开放 API。

## 示例

最小可编译示例见：

- [examples/his_cont_quotes.rs](examples/his_cont_quotes.rs)
- [examples/kline_data_download.rs](examples/kline_data_download.rs)
- [examples/kline_export_csv.rs](examples/kline_export_csv.rs)
- [examples/tick_data_download.rs](examples/tick_data_download.rs)
- [examples/tick_export_csv.rs](examples/tick_export_csv.rs)
- [examples/api_contract_s28_download_export.rs](examples/api_contract_s28_download_export.rs)
- [examples/api_contract_s28_option_greeks.rs](examples/api_contract_s28_option_greeks.rs)
- [examples/api_contract_s30_history_series_cache.rs](examples/api_contract_s30_history_series_cache.rs)

session-backed 的历史分页示例见 [examples/kline_data_page.rs](examples/kline_data_page.rs)。
默认示例符号是 `SHFE.ao2609`，因此示例里会显式使用 `SessionClientBuilder::futures_market()` 走 futures market route。

session-backed 的时间范围历史示例见 [examples/kline_data_series.rs](examples/kline_data_series.rs)。

session-backed 的按页下载示例见 [examples/kline_data_download.rs](examples/kline_data_download.rs)。

session-backed 的期权 Greeks 示例见 [examples/option_greeks.rs](examples/option_greeks.rs)。
S28 contract 把这两类能力拆成两个正式场景文件：
[examples/api_contract_s28_download_export.rs](examples/api_contract_s28_download_export.rs)
覆盖历史主连、K线/tick pull-based download、`collect_remaining()` 和 CSV
materialization；[examples/api_contract_s28_option_greeks.rs](examples/api_contract_s28_option_greeks.rs)
覆盖 session-backed Greeks research query。它们都继续归属 `tqsdk-data`：
历史下载、导出和 Greeks 不回流到 `tqsdk-session`、`tqsdk-wait` 或
调用方自建 live 消费层。

S30 contract
[examples/api_contract_s30_history_series_cache.rs](examples/api_contract_s30_history_series_cache.rs)
覆盖看盘软件 / 交易终端的历史序列 mmap 缓存。该能力只在 builder 显式开启后
影响 `get_kline_data_series` / `get_tick_data_series`；默认 `DataClient::from_session`
仍保持无缓存行为。首版支持 Python 兼容目录和文件格式，但不承诺 Python 与 Rust
进程同时写同一目录。

相关设计文档见 [../../docs/architecture/api-data.md](../../docs/architecture/api-data.md)。
