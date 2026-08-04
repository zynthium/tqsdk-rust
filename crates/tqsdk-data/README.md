# `tqsdk-data`

`tqsdk-data` 是 `tqsdk-rust` workspace 里预留给研究、离线数据和批量拉取能力的 crate。

当前阶段它只开放几层很窄的能力：

- `DataClient::new().query_his_cont_quotes(...)`
- `DataClient::new().query_his_cont_underlyings(...)`
- `DataClient::new().query_his_cont_underlying_segments(...)`
- `DataClient::new().query_trading_calendar_holidays(...)`
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
- `BacktestTickCache::mark_provisional(...)` / `provisional_coverage(...)`
- `BacktestTickCache::open(...).compact_symbol_ticks(...)`
- `BacktestTickCache::open_read_only(...).fast_inventory()`
- `BacktestTickCache::diagnose()` / `try_acquire_remote_fill_lock()` /
  `try_acquire_consistency_read_lock()`
- `LiveTickCacheWriter::new(...).push_ticks(...)` / `flush()`
- `MinuteKlineCache::open(...)` / `open_read_only(...)` / `coverage(...)`
- `MinuteKlineCache::store_final_range(...)` / `open_reader(...)` / `purge_range(...)`
- `MinuteKlineCache::fast_inventory()` / `diagnose()`
- `MinuteKlineCacheInventory` / `MinuteKlineCacheInventorySymbol`
- `MinuteKlineCacheDiagnosticReport` / `MinuteKlineCacheDiagnosticFile` /
  `MinuteKlineCacheDiagnosticStatus`
- `DataClient::run_configured_history_cache_maintenance()`
- `UniverseExpression::parse(...)`
- `resolve_futures_universe_symbols(...)`
- `DataClient::from_session(...).kline_data_download(...)`
- `DataClient::from_session(...).tick_data_download(...)`
- `KlineDataDownload::collect_remaining()`
- `TickDataDownload::collect_remaining()`
- `DataClient::from_session(...).query_option_greeks(...)`
- `DataClient::from_session(...).export_kline_data_csv(...)`
- `DataClient::from_session(...).export_tick_data_csv(...)`
- `BacktestHistoryClient::builder(...).query(...)` / `query_batch(...)`
- `BacktestHistoryRun::next()` / `finish()` / `collect()` / `collect_all(max_total_bytes)`
- `BacktestHistoryMetadataCache` / `BacktestHistoryMaintenanceClient`

## 回测历史查询

`BacktestHistoryClient` 是 local-backtest durable history 的异步入口，不是 `DataClient` 专业历史下载
API 的别名。它统一拥有 metadata sidecar、CacheOnly/RemoteOnMiss planner、official server-backtest
fill、跨请求 single-flight、bounded cache reader 与 K 线聚合；`tqsdk-session` 仅提供 Tick/60s
server-history chart substrate，`tqsdk-task` 仅消费结果来安排 replay event。

| 请求 | durable source | 是否新建 K 线文件 |
| --- | --- | --- |
| Tick | CST trading-day TQBN v2 tick partition | 否 |
| `15s` / 其他 `<60s` K | Tick partition | 否，按 session 临时聚合 |
| `60s` K | final canonical-minute v4 `logical symbol × trading month` partition | 是，唯一 durable K |
| `N × 60s` | canonical-minute partition | 否，按 closed minutes 在固定 CST `18:00` trading-day grid 临时聚合 |

`61s` / `90s` 被拒绝。Tick 与 60s partition 均没有自动 retention、max-byte eviction 或后台清理；
派生 K 永不落盘，refresh/purge 是显式 destructive operation。`RemoteOnMiss` 只在 coverage 缺口时
读取 `TQ_AUTH_*` 并调用官方 futures server-backtest source；`CacheOnly` 永不联网。

`<60s` K 的 metadata trading-session window 仍是聚合边界；相反，`N × 60s` 的盘中 break 只留下
source-minute 空洞，不会关闭、重开或重置高周期 bucket，因此一根 bar 可以跨越 break。

每个 request 有 caller-supplied id。`Chunk` 在对应 `RequestCompleted` 前均为 provisional；
`RequestFailed` 只隔离该请求。`collect()` 使用 builder 的默认 512 MiB 限制，批量
`collect_all(max_total_bytes)` 需要调用方指定总内存预算。`KQ.m@...` 的 session/calendar/physical
mapping 是带 snapshot hash 的持久 sidecar；CacheOnly 需要本地 sidecar 覆盖窗口，绝不会查询线上 mapping。
当前 durable fill 只支持 futures，股票使用 facade `.disabled_cache()` 官方回测路径。

canonical-minute 月文件记录其写入时的 immutable metadata snapshot。active pointer 后续移动时，
`BacktestHistoryClient` 只会在保留的历史 snapshot 覆盖整个请求窗口、schema/session identity 与 active
snapshot 相同、且该 snapshot 精确验证现存月文件时读取历史分区；这不会触发认证或网络访问。缺失 sidecar、
session 变化、损坏文件或不能由同一个 snapshot 解释的混合分区一律 fail closed，不会自动清理、重写或合并。

storage orchestration 是 async，但 TQBN 解压/解码仍由有界 `spawn_blocking` worker 执行；不提供把
`tokio::fs` 当作性能开关的第二条 production path。

其中：

- `query_his_cont_quotes` / `query_his_cont_underlyings` / `query_his_cont_underlying_segments` 是纯 HTTP 的一次性 direct query，不需要 live session；分别返回多主连表格、单主连 date -> underlying 映射，以及同一 underlying 相邻交易日压缩后的连续 segment
- `query_trading_calendar_holidays` 是无需凭证的原始节假日查询，返回带 source URL 的排序去重 `NaiveDate` 集合及其支持年份；`query_trading_calendar` / `query_trading_days` 保持兼容，仍由同一原始集合派生自然日交易标记和只含交易日的列表
- `get_*_data_page` 是最底层的 chart/history page substrate，并显式暴露 chart 的 `more_data` 分页信号
- `get_*_data_series` 是建立在 page substrate 之上的时间范围历史快照，语义对齐官方 `data_series`，范围为 `[start_datetime_ns, end_datetime_ns)`，分页继续与否以 `more_data` 为准
- `integrity_report()` 是对已返回 owned series 的本地质量报告；K 线按 duration 做
  calendar-agnostic cadence 缺口检查，tick 不假设固定间隔
- `DataClient::from_session(...)` 默认不启用历史序列缓存；通过
  `DataClientBuilder::history_cache_enabled(true)` 显式开启后，
  `get_*_data_series` 会隐式读写 `HistorySeriesCache`
- 未指定缓存目录时使用 `~/.tqsdk/data_series_1`；可以通过
  `TQSDK_HISTORY_CACHE_DIR` 覆盖默认 root，或通过
  `DataClientBuilder::history_cache_dir(...)` 指定单个 client 的目录
- 可通过 `DataClientBuilder::history_cache_max_bytes(...)` 和
  `history_cache_retention_days(...)` 配置显式
  `DataClient::run_configured_history_cache_maintenance()` 的容量/保留期策略；history
  reads/writes 不会自动清理 tick 或 K 线数据
- `HistorySeriesCache` 是稳定 facade，底层 store adapter 是 crate 内部实现细节；
  `HistorySeriesCache::open(root_dir)` 使用 canonical TQBN daily v2 history cache format。
  TQBN 是 tqsdk-specific DBN-like binary format，使用 fixed-width records、fixed-point
  price storage、self-describing metadata、explicit final coverage records、non-final
  provisional checkpoint records 和 forward-compatible record lengths；market-data records
  block 以 8 MiB 未压缩 payload 为目标上限，并紧跟
  crate-internal `TQRI` 时间索引，使范围读取只解压相交 block；新建/compact 日分区还会维护
  coverage index chain，使完整且未追加尾部 rows 的 coverage inspection 无需解压行情 block。
  records index 缺失或不匹配时逐 block 回退解码，coverage index 不完整、旧文件或任何校验失败时回退完整扫描；
  旧 `.tqseries` 和旧单文件 `.tqbn` layout 不是默认 backend，也没有兼容读取或迁移 store。
  默认 Cargo features 启用 `tqbn-zstd`：hot append 的 TQBN records block 使用 zstd level 1，
  append-log compaction 重写 records block 时使用 zstd level 3；两者都只在压缩后更小时写入
  压缩 block。`--no-default-features` 可关闭该支持，`tqsdk` / `tqsdk-task` facade 提供同名
  feature 转发。
  旧 Python `DataSeries` binary/mmap cache
  不再作为 public surface 暴露，已有旧文件不会自动迁移
- `BacktestTickCache::open(...)` 复用同一个 store adapter；默认 tick 日分区文件路径是
  `series/<YYYYMMDD>/tick/<escaped-symbol>.tqbn`
- TQBN store 支持递归 `scan()`、按保留期/总大小 `enforce_limits(...)` 清理和格式损坏报告；
  `enforce_limits(...)` 也会执行 append-log compaction，合并重复 rows 并保留 last-write-wins 语义
- `HistorySeriesCache::read_kline_data_series` /
  `HistorySeriesCache::read_tick_data_series` 是显式 cache-only reader，
  缺口返回 typed `DataError::CacheMiss`，不会联网补齐
- `HistorySeriesCache::write_kline_range(...)` / `write_tick_range(...)`
  是 typed range writer，会把 rows 与 `[start, end)` coverage 一起写入；
  `kline_coverage(...)` / `tick_coverage(...)`、`kline_series_path(...)` /
  `tick_series_path(...)` 和 typed purge 方法提供 coverage / 路径 / 清理运维入口；
  generic kind/request、segment writer、coverage commit 和 row reader 都是 crate 内部实现细节
- `BacktestTickCache` 是 tick-only semantic facade，复用同一个
  `HistorySeriesCache` 存储接口，用于回测覆盖检查、tick 写入和 tick
  replay 读取；TQBN store 会把覆盖元数据和 tick rows 写进同一个交易日分区文件，支持
  partial row append、盘中 provisional checkpoint 和最终 coverage commit。provisional checkpoint
  只用于恢复当前交易日的增量填充，不进入普通 coverage/cache-hit；其范围、高水位和 as-of
  必须属于同一 TQBN 日分区。盘中追加 checkpoint 不触发全历史 compaction，final coverage
  覆盖后才在 compaction 中淘汰。它不持久化 K 线，也不引入第二套 tick cache 文件格式
- cache-backed facade backtest 的唯一 durable K 线是 `MinuteKlineCache` 的 final 60s series。
  它只接受官方 server-side backtest terminal 确认完成的 range（合法的零行 range 也可以 final），
  不回退到 `DataClient` 历史下载路径。当前 format id 是
  `tqsdk.minute-kline.monthly.v4`，文件按 `logical symbol × trading month` 分区，路径仍为
  `minute-kline-v3/trading-YYYYMM/<escaped-symbol>.tqmk`。`BacktestHistoryClient` 的 `<60s` K
  由 tick rows 按 session 聚合，`N × 60s` K 由 closed canonical minutes 按固定 CST `18:00`
  trading-day grid 临时聚合；盘中 break 不重置 bucket。task 仅把结果安排为 replay event。facade
  不读取/写入 native higher-period `HistorySeriesCache` K 线，`61s` / `90s` 会拒绝
- `MinuteKlineCache` 以 calendar/session snapshot hash fail closed，只有完成的远端 60s range
  才可记录 final coverage；当前/未来 trading day 不可 claim final。v3 文件不会自动迁移、覆盖或
  被当作 cache hit；`diagnose()` 会将其列为 `LegacyUnsupported`。它没有 retention、max-byte
  eviction 或自动清理，`Refresh`、`purge_range` / `purge_symbol` 都是显式 destructive maintenance
- `BacktestTickCache::inspect(...)` 输出 backend format、缓存目录、series 文件路径、完整性、
  cached/missing ranges；`tick_series_path(...)` 返回逻辑 series 路径，`purge_symbol_ticks(...)` 和
  `compact_symbol_ticks(...)` 是按 `(symbol, tick)` 的全部日分区文件粒度的运维入口，供回测 warmup、
  refresh、远端补缓存后的碎块合并和磁盘清理复用
- `BacktestTickCache::open_read_only(...)` 不创建 root，也拒绝任何写入；`fast_inventory()` 只读取
  daily file metadata / magic，`diagnose()` 解码全部 tick partitions 并返回文件级状态。root-scoped
  `try_acquire_remote_fill_lock()` 与 `try_acquire_consistency_read_lock()` 用于协调远端 fill owner
  和稳定检查视图；它们是 advisory lock，不替代单 TQBN 文件写锁
- 可选 `tqsdk-cache` binary 只编排上述 data/facade 能力。它以 `--kind tick|minute|all`
  选择 cache family（默认 tick），为 minute 提供 final-only closed-day fill、report-bound verify、
  fast inventory / deep doctor 和显式 purge；它不属于本 crate 的 runtime、store adapter 或 live
  writer 边界，详见 [回测缓存 CLI](../../docs/architecture/backtest-tick-cache-cli.md)
- `LiveTickCacheWriter` 是纯数据层 writer：调用方或 `tqsdk` facade 传入已经收到的 live tick
  rows，它负责追加 rows、按连续 tick id 推进 coverage，并在跳号处留下缺口。连续单 tick push
  默认聚合到 128 行；批量输入、跳号、约 250 ms 后的下一次 push、显式 `flush()` 或最后一个
  clone 销毁会提交短尾。report 的 `appended_rows` 只统计实际落盘行数；它不拥有 session、订阅、
  后台线程或跨进程协调
- `HistorySeriesCache::scan()` 输出 schema version、series 文件状态、未完成写入
  和格式损坏报告；当前 TQBN store 不额外写 manifest 文件，并保持 crate-internal
  store adapter 语义
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
- 共享期货 universe selector 语法由 `UniverseExpression` 和 `FuturesUniverseResolver`
  承载；relay 和 facade backtest 使用同一套解析语义。静态 selector 不需要 auth；
  动态 selector 可通过 `SessionFuturesUniverseResolver` 调用 session metadata/query 能力解析。

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
Python-compatible mmap 缓存；旧 binary/mmap history cache 已从 public surface 废弃。

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
- `BacktestHistoryClient`
- `BacktestHistoryClientBuilder`
- `BacktestHistoryRequest` / `BacktestHistoryPolicy`
- `BacktestHistoryEvent` / `BacktestHistoryRun` / `BacktestHistoryBatchReport`
- `BacktestHistoryMetadataCache` / `BacktestHistoryMaintenanceClient`
- `BacktestTickCache`
- `BacktestTickCacheFastInventory`
- `BacktestTickCacheFastInventorySymbol`
- `BacktestTickCacheInventory`
- `BacktestTickCacheInventorySymbol`
- `BacktestTickCacheDiagnostic`
- `BacktestTickCacheDiagnosticReport`
- `BacktestTickCacheOperationLock`
- `BacktestTickTradingDayRange`
- `BacktestTickCoverage`
- `BacktestTickCacheWriteReport`
- `BacktestTickFill`
- `BacktestTickFillReport`
- `LiveTickCacheWriter`
- `LiveTickCacheWriteReport`
- `HistorySeriesCache`
- `HistorySeriesCacheReport`
- `HistorySeriesCacheMiss`
- `HistorySeriesCacheScanReport`
- `HistorySeriesCacheFileReport`
- `HistorySeriesCacheFileStatus`
- `HistorySeriesCacheMaintenanceReport`
- `HistorySeriesCoverageReport`
- `HistorySeriesPurgeReport`
- `HISTORY_SERIES_CACHE_FORMAT_ID`
- `backtest_tick_trading_day_for_timestamp_ns`
- `backtest_tick_trading_day_range`
- `UniverseExpression`
- `FuturesContract`
- `FuturesUniverseResolver`
- `StaticFuturesUniverseResolver`
- `SessionFuturesUniverseResolver`
- `resolve_futures_universe_symbols`
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
- tick-only `BacktestTickCache::inventory()` 聚合持久缓存文件、行数、字节数和问题文件
- shared futures universe selector / resolver
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
- `HistorySeriesCache::write_kline_range(...)`
- `HistorySeriesCache::write_tick_range(...)`
- `HistorySeriesCache::read_kline_data_series(...)`
- `HistorySeriesCache::read_tick_data_series(...)`
- `HistorySeriesCache::scan()`
- `HistorySeriesCache::enforce_limits(...)`
- `DataClient::run_configured_history_cache_maintenance()`
- `BacktestTickCache::open(...)`
- `BacktestTickCache::store_ticks(...)`
- `BacktestTickCache::load_series(...)`
- `BacktestTickCache::compact_symbol_ticks(...)`
- `LiveTickCacheWriter::new(...)`
- `LiveTickCacheWriter::push_ticks(...)`
- `LiveTickCacheWriter::flush(...)`
- `MinuteKlineCache::open(...)`
- `MinuteKlineCache::store_final_range(...)`
- `MinuteKlineCache::open_reader(...)`
- `UniverseExpression::parse(...)`
- `resolve_futures_universe_symbols(...)`

但它仍然只负责把下载结果收敛到调用方可接管的 `Vec`、写入调用方给定的
`AsyncWrite`，或在 `get_*_data_series` 上复用 `HistorySeriesCache`；TQBN daily v2
(`.tqbn`) 是该缓存的当前默认和 canonical 格式，旧 `.tqseries` 和旧单文件 `.tqbn`
layout 不提供兼容读取或迁移 store；
不负责 live session ownership、后台 downloader、GUI viewport 状态、旧 binary/mmap cache
迁移、跨进程 cache service 或高频交易 hot path；live 订阅到 writer 的桥接由 `tqsdk`
facade 或未来可选 relay host 拥有。

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
覆盖看盘软件 / 交易终端的历史序列持久化缓存。该能力只在 builder 显式开启后
影响 `get_kline_data_series` / `get_tick_data_series`；默认 `DataClient::from_session`
仍保持无缓存行为。TQBN daily v2 (`.tqbn`) 是当前默认和 canonical 格式，使用
`series/<YYYYMMDD>/tick/<escaped-symbol>.tqbn` 和
`series/<YYYYMMDD>/kline/<duration_ns>/<escaped-symbol>.tqbn` 日分区布局。旧 `.tqseries`
和旧单文件 `.tqbn` layout 直接废弃为默认缓存格式，不提供兼容读取或迁移 store；旧 Python 兼容 binary/mmap cache
同样不做自动迁移，也不承诺 Python 与 Rust 进程同目录互写。默认 features 启用
`tqbn-zstd`，只改变 TQBN internal block payload，不新增用户可选 store API。

`history_series_cache_microbench` 默认生成 synthetic ticks。要对本地完整 cache range 复测，设置
`TQSDK_HISTORY_CACHE_BENCH_INPUT_CACHE_DIR`、`TQSDK_HISTORY_CACHE_BENCH_INPUT_SYMBOL`、
`TQSDK_HISTORY_CACHE_BENCH_INPUT_START_NS` 和 `TQSDK_HISTORY_CACHE_BENCH_INPUT_END_NS`；benchmark
会以该 range 的 cache-only ticks 作为 write/read 样本，并同时报告完整读取与 `read_ticks_1pct`
小范围读取；不联网、不修改输入 cache。

相关设计文档见 [../../docs/architecture/api-data.md](../../docs/architecture/api-data.md)。
