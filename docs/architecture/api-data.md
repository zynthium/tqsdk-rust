# `tqsdk-data` 最小 API 草图

## 文档定位
本文档描述的是建立在现有 `tqsdk-core + tqsdk-session + replay/history contract` 之上的研究/离线数据工具层。

它回答的是：

- `tqsdk-data` 应该承接哪些能力
- 为什么这些能力不应继续塞进 `tqsdk-session` / `tqsdk-wait`
- 第一阶段只起脚手架时，哪些 public surface 先不要冻结

它不回答：

- 具体 downloader 的并发调度细节
- DataFrame / polars 的最终数据形状
- 回测报告、策略分析、GUI 展示

相关文档：

- [总架构入口](README.md)
- [crate 边界审计](crate-boundaries.md)
- [TQBN 历史缓存格式](history-cache-format.md)
- [未来 crate 蓝图](crate-blueprint.md)
- [路线图](../../ROADMAP.md)

## 先给结论

`tqsdk-data` 应该是一个独立的研究/离线数据 crate，而不是继续扩大 `tqsdk-session` 或 `tqsdk-wait`。

第一阶段的原则应当非常保守：

1. 先冻结 crate 边界和依赖方向
2. 先提供 compileable crate 骨架
3. 先只开放最薄的一次性研究接口，再逐步扩展

换句话说，当前最稳定的做法不是急着导出一堆研究接口，而是先明确：

- live continuous-consumption 继续留在 `tqsdk-wait` 或调用方自建 reader/cursor 消费层
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

### 不是 `tqsdk-wait`

这一层负责的是 diff-backed live 对象消费形状。

而 `tqsdk-data` 面向的是：

- 离线批量请求
- 历史窗口拼接
- 文件落盘
- 表格视图

这不是 `wait_update()` 或 live event 模式选择的问题。

## 回测历史查询与缓存来源

`BacktestHistoryClient` 是回测历史数据的公共异步查询入口。它拥有 metadata sidecar、source
planner、official server-backtest cache fill、single-flight 协调、bounded cache scan 与 K 线聚合；
`tqsdk-session` 只提供 server-history chart substrate，`tqsdk-task` 只拥有 replay/backtest event
语义，`tqsdk-wait` 不参与 data fill。

| 用户请求 | durable source | 派生和持久化 |
| --- | --- | --- |
| Tick | CST trading-day TQBN v3 tick partition | 原样返回；不复制 |
| `15s` 与其他 `<60s` K | 同一 Tick partition | 按 metadata session 聚合；仅内存中存在 |
| `60s` K | `logical symbol × trading month` canonical-minute v4 partition | 唯一 durable K 线 |
| `N × 60s`（`N > 1`） | 同一 canonical-minute partition | 只从 closed 60s rows 按固定 CST `18:00` trading-day grid 聚合；仅内存中存在 |

`61s`、`90s` 等既非 sub-minute、也非 60s 整数倍的周期会被拒绝。Tick 与 canonical-minute
partition 都没有 automatic retention、max-byte eviction 或后台清理；refresh/purge 是显式 destructive
operation，派生 K 从不落盘。

`<60s` K 仍以 metadata trading-session window 划 bucket，不能跨 break；`N × 60s`（`N > 1`）
则以官方固定 CST `18:00` trading-day grid 划 bucket。后者的盘中 break 只造成 source 60s row
空洞，不会关闭、重开或重置高周期 bar，所以一根高周期 bar 可以跨越 break。

请求使用稳定的 caller-supplied request id：`query()` / `query_batch()` 返回 `BacktestHistoryRun`，
其中 `next()` 产生 ordered `Chunk`、`RequestCompleted`、`RequestFailed` 事件。Chunk 在相应
`RequestCompleted` 到达前都是 provisional；一个请求失败不会取消 batch 里其他请求。`finish()` 会排空
未消费事件并返回所有 terminal report。单请求 `collect()` 使用 builder 的默认 512 MiB 上限；批量
`collect_all(max_total_bytes)` 必须显式给出总内存预算，避免把大范围 Tick/15s 查询无界物化。

`RemoteOnMiss` 先检查 durable coverage，只有缺口时才 lazy-load auth 并使用官方 futures
server-backtest source；`CacheOnly` 不联网。`KQ.m@...` 的 calendar、session 和 physical segment
mapping 以 versioned snapshot sidecar 持久化，terminal report 携带 snapshot hash；CacheOnly 需要已有
且覆盖请求窗口的 sidecar，不会向公开 metadata service 查询。当前 cache-backed fill 只支持 futures；
股票回测必须使用 facade 的 `.disabled_cache()` 官方路径。

一个 client 最多保留 `logical_concurrency` 个 clean server-backtest source lanes。Tick trading-day
slice 与 canonical-minute bounded window 仍分别提交 coverage，但 clean terminal 后 lane 保留底层
session 供下一 slice 顺序复用；服务端 10,000-row page 不重建 session。pool 饱和时 overflow session
不等待且不回池；取消、source error 或显式 chart cleanup 失败时 lane 也直接销毁。

每个 `RemoteOnMiss` run 在 planner、fill 和 row scan 生命周期内持有 shared cache-root gate；facade
已经取得 shared/exclusive gate 时把同一个实际锁守卫传给 data run，不重复加锁。不同 symbol 可以并行，
同一 `family × cache symbol` 的重叠请求通过进程内 shared fill 和跨进程 lease 合并，并在取得 lease 后再次
检查 coverage。refresh、stale repair、verify、doctor 和真实 purge 使用 exclusive gate；这些都是
advisory protocol，不保证旧版本或绕过 API 的进程安全混跑。

可选 `tqsdk-cache query` 只是这个 public query surface 的 CLI adapter：它使用
`BacktestHistoryClient::query_batch(...).collect_all(...)` 和既有 `BacktestHistoryMetadataCache`，不新增
data API、cache format、direct TQBN reader 或独立 session owner。它的 `cache-only` / `remote-on-miss`
语义与 client 对齐；`jsonl` 适合作为 lossless row stream，`tqllm-csv/3` 则是 CLI-only 的 token-aware
presentation contract：默认以 `Asia/Shanghai` 的显式 `+08:00` 紧凑 ISO（可覆写为 UTC）、声明单位的
相对整数时间和一次性 columns mapping 服务模型上下文，
不会在 `tqsdk-data` 中引入模型依赖或 prompt API。
CLI 只会把通过 `Final` 与完整 coverage 校验的 terminal report materialize 为 block；它不把
`BacktestHistoryRun` 的 provisional `Chunk` 扩展成另一种 streaming query contract。

`tqsdk-cache metadata-refresh` 是 `BacktestHistoryMaintenanceClient::refresh_metadata(...)` 的显式 operator
adapter，不属于 query/read path：它在 exclusive root remote-fill gate 内从官方 source 保存 immutable sidecar，
不改写 Tick 或 minute cache，也不使 `CacheOnly` 联网或接受未覆盖的 metadata。

每个 canonical-minute 月文件绑定写入时的 immutable metadata snapshot。active pointer 变更不单独使
旧分区不可读：当且仅当保留 snapshot 覆盖整个请求窗口、schema/session identity 与 active snapshot
一致，并能精确验证现存月文件时，planner 才选择它。缺少保留 snapshot、session 变化、损坏文件或不能由
同一 snapshot 解释的混合分区保持 fail-closed；此路径不自动 purge、重写或合并数据。

执行图是 async orchestration 加有界 `spawn_blocking` reader：文件读取、TQBN 解压和记录解码仍是
CPU/blocking 工作，不能仅把 API 换成 `tokio::fs` 就宣称性能提升。
materialize/fill-only run 在 coverage 提交后直接返回物理写入计数，不扫描 rows 回内存；cache hit
报告 0，同一 shared fill 的物理 rows 只由一个 subscriber 计数。Tick fill 按交易日顺序切片并以
8192 行缓冲追加，普通 final facade fill 只 compact 本轮实际远端回填且去重后的日分区，provisional 不 compact。

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
- `query_trading_calendar_holidays()`
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
- `DataClient::run_configured_history_cache_maintenance()`
- `BacktestCachePolicy`
- `BacktestHistoryClient`
- `BacktestHistoryClientBuilder`
- `BacktestHistoryRequest`
- `BacktestHistoryPolicy`
- `BacktestHistoryEvent` / `BacktestHistoryRun` / `BacktestHistoryBatchReport`
- `BacktestHistoryMetadataCache` / `BacktestHistoryMaintenanceClient`
- `BacktestTickCache`
- `BacktestTickCacheLockRepairMode`
- `BacktestTickCacheLockRepairStatus`
- `BacktestTickCacheLockRepairFile`
- `BacktestTickCacheLegacyPartitionLockRepair`
- `BacktestTickCacheLockRepairReport`
- `BacktestTickCoverage`
- `BacktestTickCacheWriteReport`
- `LiveTickCacheWriter`
- `LiveTickCacheWriteReport`
- `MinuteKlineCache`
- `MinuteKlineCacheSnapshot`
- `MinuteKlineCacheStatus`
- `MinuteKlineCachePurgeReport`
- `MinuteKlineCacheInventory`
- `MinuteKlineCacheInventorySymbol`
- `MinuteKlineCacheDiagnosticReport`
- `MinuteKlineCacheDiagnosticFile`
- `MinuteKlineCacheDiagnosticStatus`
- `HistorySeriesCache`
- `HistorySeriesCacheReport`
- `HistorySeriesCacheMiss`
- `HistorySeriesCacheScanReport`
- `HistorySeriesCacheFileReport`
- `HistorySeriesCacheFileStatus`
- `HistorySeriesCacheMaintenanceReport`
- `HistorySeriesCoverageReport`
- `HistorySeriesPurgeReport`
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
- live session owner / subscription bridge
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
- `query_trading_calendar_holidays` 提供 credential-free、带 source URL 的排序去重原始节假日集合与支持年份；`query_trading_calendar` / `query_trading_days` 保持原有按日/交易日接口，均从该集合派生，供主连分段和回测日期语义复用；
  cache-backed facade 的 `KQ.m@...` 回测据此把逻辑主连分段投影到具体合约 tick cache，
  data 层只提供 mapping/rows，不拥有 replay
- `PreparedBacktest::tick_sources()` 向调用方自有回放器暴露 facade 已验证的投影计划：
  `replay_symbol`、`cache_symbol` 和权威半开有效区间；调用方可以并行读取区间，但不得自行重建
  主连映射或把物理合约扩展到整个请求窗口，跨品种 barrier/截面调度仍归调用方
- `query_option_greeks`
3. local materialization
   - 已有最薄的 owned Vec materialization：`collect_remaining`
   - 已有最薄的 `AsyncWrite` CSV export
   - 已有显式 opt-in 的 `HistorySeriesCache` 历史序列缓存：
     `DataClientBuilder::history_cache_enabled(true)` 让
     `get_kline_data_series` / `get_tick_data_series` 在同一 API 上隐式读写
     `~/.tqsdk/data_series_1`、`TQSDK_HISTORY_CACHE_DIR` 覆盖目录或自定义目录；
     cache miss 使用官方
     `DataSeries` 的 `set_chart` / `focus_datetime` / `left_kline_id`
     下载序列补齐缺口
   - 已有 cache-only history series reader、schema/version scan report、
     typed cache miss，以及最薄的容量/保留期清理策略；旧 Python `DataSeries`
     binary/mmap cache 不再作为 public surface 暴露，也不自动迁移
   - `HistorySeriesCache` 是 public facade，底层 store adapter 只保留为 crate
     内部 seam；`HistorySeriesCache::open(root_dir)` 使用 canonical TQBN daily v3 history cache
     format，格式合同见 [history-cache-format.md](history-cache-format.md)。TQBN 是
     tqsdk-specific DBN-like binary format，使用 fixed-width records、fixed-point price
     storage、self-describing metadata、explicit coverage records 和 forward-compatible
     record lengths；旧 `.tqseries` 和旧单文件 `.tqbn` layout 不再作为默认格式，没有兼容读取或迁移 store，
     也不应重新扩成多 backend public surface；coverage/path/purge
     对外使用 typed kline/tick 方法，generic kind/request 只保留为 crate 内部存储语义
   - `BacktestTickCache` 已作为 tick-only semantic facade 落在 `tqsdk-data`；
     它复用 `HistorySeriesCache` 做回测覆盖检查、tick 写入和 tick 读取，不新增第二套
     JSONL tick cache，也不持久化 K 线
   - `BacktestTickCache::repair_tick_locks(BacktestTickCacheLockRepairMode)` 是唯一公开的 tick
     companion-lock remediation：`DryRun` 为每个唯一 Tick 分区检查 legacy `<partition>/.tqbn.lock`，并
     逐文件检查 `<file>.tqbn.lock`；`Apply` 先以 non-truncating open 创建缺失 legacy lock，再通过
     normal exclusive TQBN/file lock 创建缺失逐文件 sidecar。直接调用者必须先停止同一 root 的
     reader/writer，并在调用期间持有 `try_acquire_consistency_read_lock()`；`DryRun` 可用 read-only cache，
     `Apply` 必须使用 writable cache。该 API 不改 TQBN bytes、rows、coverage 或 index，不访问 remote/auth，
     不做 fill 或 compaction；目录级和逐文件失败分别保留在 report，仍继续其余目标
   - `LiveTickCacheWriter` 是 `BacktestTickCache` 上的一层纯 writer；它接收调用方已经消费到的
     live tick rows，追加 rows，并只在 tick id 连续时推进 coverage。连续单 tick push 默认合并到
     128 行；批量、跳号、到期后的下一次 push、显式 `flush()` 或最后一个 clone 销毁会提交短尾。
     session ownership、`wait_update()` 消费、订阅、timer task 和后台运行不属于这个 writer
   - 后续再考虑路径管理型文件导出
   - deterministic replay / local backtest event source 由 `tqsdk-task` 拥有；
     `tqsdk-data` 只提供 history rows，不提供 JSONL market cache public surface
   - 跨进程 daemon、queue/election 或通用 cache 管理服务不属于当前 `tqsdk-data` public API；
     cache-root gate、per-series lease、TQBN 文件锁与 tail recovery 只是当前 store/fill 的窄协调合同
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
tqsdk-wait        tqsdk-data
                ^
                |
            tqsdk-task
```

这里的关键约束是：

- `tqsdk-data` 可以依赖 `tqsdk-session`
- 如有必要，`tqsdk-data` 也可以直接依赖 `tqsdk-core`
- `tqsdk-session` / `tqsdk-wait` 不应反向依赖 `tqsdk-data`

## 当前落地策略

当前仓库里，`tqsdk-data` 已经以“窄 public surface”的方式落地。

其中比较关键的一个点是：

- `get_kline_data_page` / `get_tick_data_page` 已经落在 `tqsdk-data`
- `get_kline_data_series` 已经落在 `tqsdk-data`
- `get_tick_data_series` 也已经落在 `tqsdk-data`
- `DataClientBuilder` / `HistorySeriesCache` 提供显式 opt-in 的历史序列持久化缓存；
  TQBN daily v3 (`.tqbn`) 是当前默认和 canonical 格式，使用交易日分区 layout；
  旧 `.tqseries` 和旧单文件 `.tqbn` layout 不再作为默认格式，不提供兼容读取或迁移 store。
  旧 Python 兼容 mmap backend 已废弃，也已经落在
  `tqsdk-data`
- TQBN 当前每个文件使用独立 `<file>.tqbn.lock`；read-only reader 在其缺失时可回退同分区 legacy
  `<partition>/.tqbn.lock`。writer 在独占锁下原子初始化、append/repair/compact；reader 在共享锁内打开文件、验证 prefix/tail checkpoint 并固定确认长度，然后用 opened file handle
  在锁外解压和流式消费。checkpoint 记录 confirmed length、尾部 checksum 与最新 coverage index head；
  读侧忽略其后的未确认 suffix，下一 writer 可截断截断块或坏 checksum suffix 后继续。没有 checkpoint
  的旧文件仍全量校验，不把真实损坏静默当成可恢复尾部。缺失 companion lock 的专用 repair path 不属于
  writer 的 data repair：它由 owner 的 exclusive root gate 串行，`Apply` 先为每个唯一 Tick 分区创建缺失的
  regular legacy lock，再创建缺失逐文件 sidecar，不替换 invalid/non-regular lock；单目标失败继续扫描并报告，详见
  [回测缓存 CLI](backtest-tick-cache-cli.md)
- `HistorySeriesCache::read_kline_data_series` /
  `HistorySeriesCache::read_tick_data_series` 提供 cache-only 读取，
  `HistorySeriesCache::scan` 和 `HistorySeriesCache::enforce_limits`
  提供 schema/损坏报告与容量/保留期维护，也已经落在 `tqsdk-data`
- `history_cache_max_bytes(...)` 与 `history_cache_retention_days(...)` 只配置显式
  `DataClient::run_configured_history_cache_maintenance()`；普通 history read/write 不会自动清理。
  尤其回测 `BacktestTickCache` 与 `MinuteKlineCache` 没有自动 retention、max-byte eviction 或后台清理
- `HistorySeriesCache` 的底层存储通过 crate 内部 store adapter 隔离；`BacktestTickCache`
  复用这套内部实现承接回测 tick 缓存，不再维护独立 tick replay cache 实现
- facade cache-backed backtest 读取同一 cache root：tick 输入经 `BacktestTickCache`；唯一持久
  K 线输入是独立 `MinuteKlineCache` 的 canonical final `60s` files。minute fill 只使用官方
  server-side backtest Kline stream，并且只在该 stream terminal 成功后写 final coverage。
  `<60s` K 从 tick 按 session 本地合成，`>60s` 仅允许 `N × 60s`，由 `tqsdk-data` 从已关闭分钟线
  按固定 CST `18:00` trading-day grid 聚合；盘中 break 不重置 bucket。`61s` / `90s` 会拒绝，
  facade 不读取或写入 native higher-period `HistorySeriesCache` K 线
- facade 对 `KQ.m@...` 的 tick 使用 data-owned persisted metadata sidecar 把 physical tick cache
  映射到 dated underlying；CacheOnly 读取 sidecar，不访问网络。minute cache 始终以 logical symbol
  为 key，不复制 physical minute files
- `MinuteKlineCache::fast_inventory()` 不解码月文件、也不创建缺失 root；`diagnose()` 以只读方式
  深检每个月文件，区分 readable v4、legacy v3、unsupported version 和 corruption。这些是 data
  layer 的 typed operator API，不会迁移、修复或删除缓存
- `HistorySeriesCache` 公开 typed range writer / cache-only reader；generic segment writer、
  coverage commit 和 row reader 只作为 crate 内部 seam，避免 task/facade 直接绑定底层 store shape
- `BacktestTickCache::compact_symbol_ticks(...)` 是 tick-only、按 symbol 的全部日分区文件粒度维护 API；
  `mark_provisional(...)` / `provisional_coverage(...)` 只暴露当前交易日的 durable
  non-final checkpoint。该 checkpoint 不进入 `BacktestTickCoverage`，也不能满足普通 cache hit；
  final coverage 覆盖后读取端立即忽略，compaction 负责物理淘汰；
  remote-on-miss / warmup 通过官方 server-side backtest 流回填。warmup 会先跳过完整缓存，
  再把所有缺失 symbol 交给内部有界远端调度器；默认不做时间切片，`TQSDK_REMOTE_FILL_SLICE_SECS`
  只作为长区间 fallback。默认不设置整批墙钟超时，持续有进展的长区间由 60 秒 idle watchdog
  保护；`TQSDK_REMOTE_FILL_BATCH_TIMEOUT_SECS` 仅在显式设为正数时启用诊断/作业预算限时。
  每个成功 slice 先验证 tick id 连续性并独立提交 coverage，全部 slice 成功后才 compact 本次实际
  远端回填范围相交的 tick 日分区；失败或未确认 slice 只会留下未覆盖范围，不触发全缓存重写
- `LiveTickCacheWriter::push_ticks(...)` / `flush()` 也已经落在 `tqsdk-data`，作为纯数据层 writer
  支持 live/session host 将指定 symbol 的实时 tick 行写入同一份回测缓存；它不创建 session，
  不订阅行情，也不负责后台守护或跨进程协调
- `kline_data_download` / `tick_data_download` 也已经落在 `tqsdk-data`
- `query_option_greeks` 也已经落在 `tqsdk-data`
- `tqsdk-data` 不提供 `MarketCacheEvent` / `MarketCacheWriter` /
  `MarketCacheReader` / `MarketCacheReplay` 这类 JSONL market cache public API；
  deterministic replay / local backtest 输入属于 `tqsdk-task`
- live pipe、live consumer feature、跨进程 cache service、daemon/supervisor orchestration 和 live hot-path cache dependency 均不属于当前 `tqsdk-data` public API。
- `tqsdk-data` 不拥有 live diff consumption；`LiveTickCacheWriter` 只接收已解码 tick rows。
  订阅、`wait_update()` 驱动和实时策略 host 留在 `tqsdk` / `tqsdk-wait` 或调用方自建 reader/cursor
  消费层，跨进程持久化服务应作为可选上层 host 复用 writer。
- queue、lock、reader manifest、recovery scan、writer election、compaction
  ownership、service、daemon 和 supervisor 等编排表面已经从当前 public API
  回退；它们不属于 `tqsdk-data` 的稳定边界
- `KlineDataSeries` / `TickDataSeries` 到 replay event/source 的 adapter
  已经移到 `tqsdk_task::replay::StrategyReplaySourceBuilder`
- `HistoryIntegrityReport` 已经落在 `tqsdk-data`，作为 owned history series 的本地质量报告：
  K 线按 duration 做 calendar-agnostic cadence 缺口检查，tick 不假设固定间隔；
  报告只暴露 requested/returned range、缺口、重复行、时间倒退、越界行、
  cache hit/miss/downloaded 和权限检查状态，不绑定外部数据库或 tabular 框架
- 它不是新的 session facade
- 它也不是 live ref / live consumer；当前没有 live window 写 history cache
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
- TQBN daily v3 (`.tqbn`) 是当前 Rust history cache 默认和 canonical 格式，路径形如
  `series/<YYYYMMDD>/tick/<escaped-symbol>.tqbn` 和
  `series/<YYYYMMDD>/kline/<duration_ns>/<escaped-symbol>.tqbn`。旧 `.tqseries`、旧单文件
  `.tqbn` layout 和旧 Python `DataSeries` binary/mmap 文件格式不再支持迁移、兼容读取或交替使用。同目录同时写仍是
  non-goal，因为 Python 官方实现自身也没有承诺同一合约周期多进程/线程/协程并发写
- 回测 canonical-minute v4 是并列、独立的月分区格式：format id 为
  `tqsdk.minute-kline.monthly.v4`，路径仍是
  `minute-kline-v3/trading-YYYYMM/<escaped-symbol>.tqmk`。它仅接受 server-side backtest
  terminal 成功的 final 60s coverage，以 calendar/session snapshot hash fail closed；当前交易日
  不能 claim final coverage。旧 v3 文件不会自动迁移或当作命中，`diagnose()` 会报告
  `LegacyUnsupported`。它没有自动 retention 或 max-byte eviction；`Refresh`、`purge_range` /
  `purge_symbol` 都是显式 destructive maintenance
- 默认 feature `tqbn-zstd` 对 hot append 的 TQBN internal records block 使用 zstd level 1，
  对 append-log compaction 重写的 records block 使用 zstd level 3；`--no-default-features`
  可关闭该支持；不引入用户自选 store、manifest 或第二套 cache API
- market-data records block 使用 8 MiB 未压缩目标 payload 和 crate-internal `TQRI` 时间索引；
  range reader 只解压相交 block，旧文件或缺失/不匹配索引逐 block 回退，不扩大 public store API
- 跨进程 cache 管理若后续需要，应作为用户 tooling 或独立 service 重新设计，
  而不是把 live session、进程管理、HTTP endpoint、GUI 或底层文件编排表面
  下沉进 `tqsdk-data`
- async history 相关入口会主动获取 auth context 并校验 `tq_dl`，避免把权限问题拖到 chart/websocket timeout
- `data_download` 这类同步构造入口仍然只做 best-effort 预检，真正的 async 读取阶段会再次强校验
- 默认 SHFE 历史示例会显式切到 `futures_market()`，避免把 futures history 请求发到 stock market route

这样做的意义是：

- 不把研究/批量历史接口继续塞进 `tqsdk-session`
- 不把历史数据读取和 `wait_update()` / live consumer 模式耦合在一起
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
