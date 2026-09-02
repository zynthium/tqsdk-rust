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
- `BacktestTickCache::purge_symbol_ticks_in_range(...)`
- `BacktestTickCache::diagnose()` / `try_acquire_remote_fill_shared_lock()` /
  `try_acquire_remote_fill_lock()` / `try_acquire_consistency_read_lock()`
- `BacktestTickCache::repair_tick_locks(BacktestTickCacheLockRepairMode)`
- `LiveTickCacheWriter::new(...).push_ticks(...)` / `flush()`
- `MinuteKlineCache::open(...)` / `open_read_only(...)` / `coverage(...)`
- `MinuteKlineCache::store_final_range(...)` / `open_reader(...)` / `purge_range(...)`
- `MinuteKlineCache::fast_inventory()` / `diagnose()`
- `DailyKlineCache::open(...)` / `open_read_only(...)` / `coverage(...)` / `purge_symbol(...)`
- `DailyKlineCache::fast_inventory()` / `diagnose_all()`
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
- `BacktestHistoryClient::orchestrate_fill(...)` / `BacktestHistoryFillConfig`
- `BacktestHistoryRun::next()` / `finish()` / `collect()` / `collect_all(max_total_bytes)`
- `BacktestHistorySnapshot::open(...)` / `inspect(...)` / `query(...)`（lease-pinned read-only
  generation；manifest metadata hash 是 generation 级 inventory SHA-256，request report hash 仍是
  per-symbol metadata identity；query lease 会保留到 detached blocking scan 完全退出）
- `BacktestHistorySnapshotRun::next()` / `collect()` / `finish()`（terminal failure 保留
  `BacktestHistoryFailureReason`，不要求调用方解析 legacy error string）
- publisher-facing manifest seam：`BacktestHistorySnapshotManifestBuilder`、
  `classify_backtest_history_snapshot_cache_path(...)` 与
  `BacktestHistorySnapshot::open_generation(...)`；canonical identity、file-role allowlist、staging/
  retained validation 仍只在 data 实现，`tqsdk-cache` 不复制 manifest parser
- `BacktestHistoryMetadataCache` / `BacktestHistoryMaintenanceClient`

## Universe Language V2 与历史 artifact

`tqsdk-data` 是 Universe 语言和历史 plan 的唯一 owner：

- legacy `UniverseExpression` / `HistoricalFillUniverseSpec` 及其原执行语义保持不变；
- `UniverseSpec::parse_v2`、`UniverseInput` 与 `compile_static_futures_universe_v2` 提供纯 V2
  snapshot 编译；需要 metadata/ranking 时通过 `FuturesUniverseResolver` capability adapter；
- `compile_historical_universe_resolution_v4` 只接受 `timeline(...)`，固定 provider-data membership、
  dependency closure 和 tick/minute/daily targets；
- `HistoricalUniversePlanV5` 是 current immutable plan；`HistoricalUniversePlanArtifact` 保留 flat
  v1–v5 dispatch 以支持迁移与受控兼容，旧 `HistoricalUniversePlan` 结构和
  `publish_plan/load_plan` v1–v3 API 保持；
- `HistoricalUniverseArtifactStore` normal V5 路径验证 acquisition、semantic catalog 与 plan；
  `preview_v4_migration/migrate_v4_plan` 先验证 V4/V3 rollback chain，且永不删除 source artifact。

```rust
use tqsdk_data::{UniverseInput, UniverseSpec, compile_static_futures_universe_v2};

let spec = UniverseSpec::parse_v2(
    "snapshot(symbol:SHFE.au2606,KQ.i@SHFE.au;!symbol:SHFE.au2506)",
)?;
let input = UniverseInput::from_spec(spec).expand()?;
let compiled = compile_static_futures_universe_v2(&input)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

省略 wrapper 默认 snapshot。`main` 是当前具体主力，`continuous` 是 `KQ.m@...` 逻辑主连；
Universe 只选 instrument，不选择数据流。V2 `file:` 被拒绝，外部 exact-symbol 文件通过
`UniverseInput::universe_symbol_file(s)` 组合，每个文件只读一次且内容 identity 可重现。
完整语法、兼容 dispatcher、排除矩阵和 V5 migration 见
[Universe Language V2](../../docs/architecture/universe-language.md) 与
[历史 Universe Catalog](../../docs/architecture/historical-universe-catalog.md)。

批量排除可写 `except(contract:CFFEX.*,CZCE.ZC,CZCE.CY)`；若要对全部 view 生效，写 `except(all:CFFEX.*,CZCE.ZC,CZCE.CY)`。它们都会规范化为既有 `!` 表示，因此不会改变 V2 AST/hash 或历史 artifact identity。
同一交易所的 target 可写成分组语法，例如
`except(all:CFFEX.*,CZCE.{ZC,CY,RI,RS},DCE.{rr,lg,fb,bb})`；它在 parser 层展开为既有逐项 target，
同样不改变 AST/hash 或 artifact identity。

## 回测历史查询

`BacktestHistoryClient` 是 local-backtest durable history 的异步入口，不是 `DataClient` 专业历史下载
API 的别名。它统一拥有 metadata sidecar、CacheOnly/RemoteOnMiss planner、official server-backtest
fill、进程内 single-flight、跨进程 per-series fill lease、shared cache-root gate、bounded cache reader
与 K 线聚合；`tqsdk-session` 仅提供 Tick/60s/native-1d
server-history chart substrate，`tqsdk-task` 仅消费结果来安排 replay event。

历史全合约/动态 membership 不复用 current universe grammar。`HistoricalFillUniverseSpec`
接受 `physical:all` 或受限的 `timeline(...)`。默认 acquisition 先记录稳定的完整 provider roster；
V2 timeline 再根据 normalized AST 投影出 physical bootstrap closure，仅为保留 physical contract 和
保留 derived view 的必要 underlying 进行 `[1990-01-01, as_of)` native-1d 观测，并升级为
`provider_history_observed`：第一条
日线是数据 membership 起点，终态空区间和隔离的 `provider_unavailable` 候选不进入 universe；
默认路径不读取或推断交易所挂牌日期。tick/minute 从 `max(用户起点, membership 起点)` 发起请求，
实际首行和空前缀仍由各自 cache coverage 记录。`HistoricalUniverseArtifactStore` 内容寻址保存 acquisition、semantic catalog 和
v3 plan；旧 `authoritative_lifecycle` 路径保持原语义。v3 固定 visible membership、dependency
closure 和 kind-specific targets。详细合同见
[`historical-universe-catalog.md`](../../docs/architecture/historical-universe-catalog.md)。

每个 roster 候选都必须有显式 observation。complete rows、terminal empty 和
`provider_unavailable` 是三个不同结果；后者仅允许来自 symbol batch size 1 的精确 timeout，并受
初始 bootstrap 的独立重试和全局失败比例熔断保护；它是 provider 数据可用性事实，不是“从未挂牌”的声明。
后续维护将 attempts/next-due 放入独立、版本化且内容寻址的 retry receipt（绑定 immutable acquisition
hash），不改写 proof-bearing observation。receipt 只在稳定 roster、相同 cutoff 和 provider-health
canary 成功后前进；发现首行或 terminal empty 才发布新的 acquisition/catalog，普通 `fill --universe`
再负责生成 plan。

同一个 client 是 tick/minute/daily fill scheduling 的唯一 owner：默认 symbol batch size 1、concurrency 2、
idle timeout 60 秒、无 batch timeout；batch size/concurrency 都只接受 `1..=4`。它统一产生 planning、
batch、telemetry、terminal progress，facade 与 CLI 不再各自实现调度。

| 请求 | durable source | 是否新建 K 线文件 |
| --- | --- | --- |
| Tick | CST trading-day TQBN v3 tick partition | 否 |
| `15s` / 其他 `<60s` K | Tick partition | 否，按 session 临时聚合 |
| `60s` K | final canonical-minute v5 `logical symbol × trading month` partition | 是 |
| `N × 60s`（`N > 1` 且 `<1d`） | canonical-minute partition | 否，按 closed minutes 在固定 CST `18:00` trading-day grid 临时聚合 |
| `1d` K | final native-daily v1 `logical symbol` single file | 是，不按时间分区 |
| `2d` 到 `28d` K | same native-daily file | 否，按 native 1d timestamp phase 临时聚合 |

`61s` / `90s`、非整数日和大于 `28d` 的日周期被拒绝。Tick、60s 与 1d cache 均没有自动 retention、
max-byte eviction 或后台清理；2d 到 28d 派生 K 永不落盘，1d 的显式 `purge_symbol` 与其他 refresh/purge
均是 destructive operation。`RemoteOnMiss` 只在 coverage 缺口时
读取 `TQ_AUTH_*` 并调用官方 futures server-backtest source；`CacheOnly` 永不联网。
daily 缓存缺失、损坏或 coverage 不完整时必须失败，不允许从 minute 回退聚合。

`<60s` K 的 metadata trading-session window 仍是聚合边界；相反，`N × 60s` 的盘中 break 只留下
source-minute 空洞，不会关闭、重开或重置高周期 bucket，因此一根 bar 可以跨越 break。

每个 request 有 caller-supplied id。`Chunk` 在对应 `RequestCompleted` 前均为 provisional；
`RequestFailed` 只隔离该请求。`collect()` 使用 builder 的默认 512 MiB 限制，批量
`collect_all(max_total_bytes)` 需要调用方指定总内存预算。`KQ.m@...` 的 session/calendar/physical
mapping 是带 snapshot hash 的持久 sidecar；CacheOnly 需要本地 sidecar 覆盖窗口，绝不会查询线上 mapping。
当前 durable fill 只支持 futures，股票使用 facade `.disabled_cache()` 官方回测路径。

canonical-minute 月文件与 native-daily single file 都记录写入时的 immutable metadata snapshot。active pointer
后续移动时，`BacktestHistoryClient` 会从 content-addressed retained sidecar 解析旧、新 snapshot，并在每个已有
coverage range 内比较 schema、market、logical symbol、session、交易日和 physical mapping。只有全部证明一致时，
daily reader 才能复用旧 coverage；写入新缺口会在 per-symbol lock 内原子 reheader 到新 snapshot。任一 sidecar
缺失/损坏或历史映射不一致都会 fail closed，绝不降级为 cache miss 或重写文件。
缺失 sidecar、session/交易日/映射变化、损坏文件或语义冲突的混合分区一律 fail closed，不会自动清理、
重写或合并。

storage orchestration 是 async，但 TQBN 解压/解码仍由有界 `spawn_blocking` worker 执行；不提供把
`tokio::fs` 当作性能开关的第二条 production path。

普通 `RemoteOnMiss` fill/query 持 shared root gate；refresh、stale repair 和稳定维护持 exclusive gate。
实际缺口再以 `cache family × cache symbol` 的跨进程 lease 串行化，等待者重查 coverage 后复用 owner
结果。Tick fill 按 trading day 顺序消费并以 8192 rows 缓冲；取消会 flush 已接受短尾但不提交未 terminal
coverage。fill-only materialization 不回读刚写入的 cache，物理写入计数在 shared fill 中只累计一次。
一个 client 最多保留 `logical_concurrency` 个 clean server-backtest source lanes；clean terminal 与
chart cleanup 成功后，同一 session 可顺序服务后续 trading-day/minute slices。pool 饱和时 overflow
不等待且不回池；取消、source error 或 cleanup error 也会丢弃 lane。coverage 仍按 slice 独立提交，
因此连接复用不改变中断恢复粒度。

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
 `HistorySeriesCache::open(root_dir)` 使用 canonical TQBN daily v3 history cache format。Tick v3
 首 snapshot 完整持久化，后续 snapshot 只写变化字段与 id/time delta；每条接收的 snapshot 都保留，
 不按 tick 频率填充，也不删除无成交或重复盘口。v2 文件写入前须执行
 `tqsdk-cache migrate --apply --backup-dir DIR`。
  TQBN 是 tqsdk-specific DBN-like binary format，使用 fixed-width records、fixed-point
  price storage、self-describing metadata、explicit final coverage records、non-final
  provisional checkpoint records 和 forward-compatible record lengths；market-data records
  block 以 8 MiB 未压缩 payload 为目标上限，并紧跟
  crate-internal `TQRI` 时间索引，使范围读取只解压相交 block；新建/compact 日分区还会维护
  coverage index chain。每个 `.tqbn.lock` sidecar 还记录已确认 file length、bounded tail checksum 和
  最新 coverage-index head；coverage/range reader 只读取该 confirmed prefix，不要求物理文件尾恰好是
  coverage index。reader 在 shared lock 内打开 data file 并固定 snapshot，checkpoint 有效时可释放锁后
  从 opened file handle 解码；并发 append/atomic-rename compaction 不改变该 snapshot。首次初始化写临时
  文件并 sync 后原子 rename。checkpoint 后未确认的截断或坏 checksum suffix 不阻止下次 writer 恢复；
  无有效 checkpoint 的旧文件按锁内捕获的完整物理长度严格校验，不能忽略坏 suffix，但 snapshot planning
  完成后无需把 shared lock 持有到解码结束。coverage/provisional record 与紧邻、引用它的 `TQCI` 构成恢复
  原子对；孤立 record 属于未确认 tail，恢复时从其起点截断。records index 缺失或不匹配时逐 block 回退
  解码，coverage index 不完整时回退扫描 confirmed prefix；
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
- cache-backed facade backtest 的 durable K 线包括 `MinuteKlineCache` 的 final 60s series 和
  `DailyKlineCache` 的 native final 1d series。两者只接受官方 server-side backtest terminal 确认完成的
  range（合法的零行 range 也可以 final），不回退到 `DataClient` 历史下载路径。minute format id 是
  `tqsdk.minute-kline.monthly.v5`，文件按 `logical symbol × trading month` 分区，路径仍为
  `minute-kline-v3/trading-YYYYMM/<escaped-symbol>.tqmk`。v5 的 row payload 仅在 zstd 更小时
  无损压缩，保留所有 Kline row；旧 v4 只能显式迁移，普通 reader/fill 会 fail closed。daily format id 是
  `tqsdk.daily-kline.single-file.v1`，路径为 `daily-kline-v1/<escaped-symbol>.tqdk`；它按 logical
  symbol 单文件原子替换，不按时间分区。`BacktestHistoryClient` 的 `<60s` K 由 tick rows 按 session
  聚合，`N × 60s` K 由 closed canonical minutes 按固定 CST `18:00` trading-day grid 临时聚合，`2d` 至
  `28d` K 由 final native 1d rows 临时聚合。task 仅把结果安排为 replay event。1d row 目前只含 Kline
  OHLC、volume、open/close OI；结算价和涨跌停价未支持。facade 不读取/写入 native higher-period
  `HistorySeriesCache` K 线，`61s` / `90s`、非整数日和大于 `28d` 的日周期会拒绝
- `MinuteKlineCache` 以 immutable metadata snapshot fail closed；hash 不同只在双方 sidecar 均存在且实际
  cached range 的 schema/market/symbol/session/交易日/physical mapping 完全相同时兼容。只有完成的远端
  60s range 才可记录 final coverage；当前/未来 trading day 不可 claim final。v3 文件不会自动迁移、覆盖或
  被当作 cache hit；`diagnose()` 会将其列为 `LegacyUnsupported`。它没有 retention、max-byte
  eviction 或自动清理，`Refresh`、`purge_range` / `purge_symbol` 都是显式 destructive maintenance
- `DailyKlineCache` 同样以 immutable metadata snapshot fail closed；snapshot hash/identity 不同只有 retained
  sidecar 对每个已有 coverage 的 schema/market/symbol/session/trading-day/physical mapping 全部证明一致时才能复用，
  next-gap write 在 per-symbol lock 内原子 reheader。缺 sidecar、损坏或不一致仍是错误而非 cache miss。只有已
  terminal 的原生 1d range 才能写 final coverage；当前或未来 CST trading day 直接拒绝。
  `fast_inventory()` 只读 fixed header 与 embedded logical symbol，`diagnose_all()` 完整校验 checksum/rows；
  只能由显式 `purge_symbol()` 删除整个 logical-symbol 文件
- `BacktestTickCache::inspect(...)` 输出 backend format、缓存目录、series 文件路径、完整性、
  cached/missing ranges；`tick_series_path(...)` 返回逻辑 series 路径，`purge_symbol_ticks(...)`、
  `purge_symbol_ticks_in_range(...)` 和
  `compact_symbol_ticks(...)` 是按 `(symbol, tick)` 的全部日分区文件粒度的显式运维入口；facade
  final fill 使用范围版本，只重写本轮实际远端回填范围相交的日分区，避免 cache-hit 历史被重复 compact
- `BacktestTickCache::repair_tick_locks(BacktestTickCacheLockRepairMode)` 是只针对既有 tick
  `.tqbn` companion lock 的运维 API：`DryRun` 按唯一 Tick 分区检查 legacy
  `<partition>/.tqbn.lock`，并逐文件检查 `<file>.tqbn.lock`；`Apply` 先以非截断方式创建缺失的
  legacy lock，再通过正常逐文件排他锁创建缺失 sidecar。调用方必须在停止同一 root 的 reader/writer 后持有
  `try_acquire_consistency_read_lock()`；`Apply` 还要求可写 cache。它不改 TQBN bytes、rows、coverage 或
  index，不访问远端/认证，也不是 fill 或 compaction。目录级和逐文件结果都保留
  `Missing` / `AlreadyPresent` / `Created` / `Failed` 状态；单个失败不会阻止其余目标继续处理
- `BacktestTickCache::open_read_only(...)` 不创建 root，也拒绝任何写入；`fast_inventory()` 只读取
  daily file metadata / magic，`diagnose()` 解码全部 tick partitions 并返回文件级状态。root-scoped
  `try_acquire_remote_fill_shared_lock()` 允许普通 fill/query 并发，
  `try_acquire_remote_fill_lock()` / `try_acquire_consistency_read_lock()` 提供与普通操作互斥的 exclusive
  maintenance/stable view；它们是 advisory lock，不替代单 TQBN 文件写锁。每个 series 的远端补洞另有
  跨进程 lease。锁协议只保证当前实现之间协作，不承诺新旧 binary 进程长期混跑
- 可选 `tqsdk-cache` binary 只编排上述 data/facade 能力。它以 `--kind tick|minute|daily|all`
  选择 cache family（默认 tick），为三类提供统一 fill progress/schema-v3 report、inventory、inspect、
  verify、deep doctor 和显式 purge；它不属于本 crate 的 runtime、store adapter 或 live
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
- `BacktestHistoryRequestFailure`（既有 query 兼容面）/ `BacktestHistoryFailureReason`（strict snapshot seam typed failure）
- `BacktestHistorySnapshot` / `BacktestHistorySnapshotRun` / `BacktestHistorySnapshotQueryResources`
- `BacktestHistorySnapshotResourceBudget` / `BacktestHistorySnapshotResourceReservation`（daemon-owned scan budget 与 opaque RAII guard）
- `BacktestHistoryMetadataCache` / `BacktestHistoryMaintenanceClient`
- `DailyKlineCache` / `DailyKlineCoverage` / `DailyKlineCacheStatus` /
  `DailyKlineCacheDiagnosticReport` / `DailyKlineCachePurgeReport`
- `BacktestTickCache`
- `BacktestTickCacheFastInventory`
- `BacktestTickCacheFastInventorySymbol`
- `BacktestTickCacheInventory`
- `BacktestTickCacheInventorySymbol`
- `BacktestTickCacheDiagnostic`
- `BacktestTickCacheDiagnosticReport`
- `BacktestTickCacheLockRepairMode`
- `BacktestTickCacheLockRepairStatus`
- `BacktestTickCacheLockRepairFile`
- `BacktestTickCacheLegacyPartitionLockRepair`
- `BacktestTickCacheLockRepairReport`
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
`AsyncWrite`，或在 `get_*_data_series` 上复用 `HistorySeriesCache`；TQBN daily v3
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
- [examples/api_contract_s49_tick_lock_repair.rs](examples/api_contract_s49_tick_lock_repair.rs)

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
仍保持无缓存行为。TQBN daily v3 (`.tqbn`) 是当前默认和 canonical 格式，使用
`series/<YYYYMMDD>/tick/<escaped-symbol>.tqbn` 和
`series/<YYYYMMDD>/kline/<duration_ns>/<escaped-symbol>.tqbn` 日分区布局。旧 `.tqseries`
和旧单文件 `.tqbn` layout 直接废弃为默认缓存格式，不提供兼容读取或迁移 store；旧 Python 兼容 binary/mmap cache
同样不做自动迁移，也不承诺 Python 与 Rust 进程同目录互写。默认 features 启用
`tqbn-zstd`，只改变 TQBN internal block payload，不新增用户可选 store API。

S49 contract
[examples/api_contract_s49_tick_lock_repair.rs](examples/api_contract_s49_tick_lock_repair.rs)
展示 `BacktestTickCache::repair_tick_locks(...)` 的显式 root gate：默认 `DryRun`，只有设置
`TQ_CACHE_REPAIR_LOCKS_APPLY=1` 才调用 `Apply`。它只补既有 tick `.tqbn` 缺失的 companion lock；运行前
必须停止同一 root 的 reader/writer。报告将 legacy 分区级 `<partition>/.tqbn.lock` 与逐文件
`<file>.tqbn.lock` 分开呈现；不能把它当作 fill、数据修复或 compaction。

`history_series_cache_microbench` 默认生成 synthetic ticks。要对本地完整 cache range 复测，设置
`TQSDK_HISTORY_CACHE_BENCH_INPUT_CACHE_DIR`、`TQSDK_HISTORY_CACHE_BENCH_INPUT_SYMBOL`、
`TQSDK_HISTORY_CACHE_BENCH_INPUT_START_NS` 和 `TQSDK_HISTORY_CACHE_BENCH_INPUT_END_NS`；benchmark
会以该 range 的 cache-only ticks 作为 write/read 样本，并同时报告完整读取与 `read_ticks_1pct`
小范围读取；不联网、不修改输入 cache。

相关设计文档见 [../../docs/architecture/api-data.md](../../docs/architecture/api-data.md)。
