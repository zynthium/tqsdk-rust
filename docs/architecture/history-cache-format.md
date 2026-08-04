# History Cache Format

## 文档定位

本文档定义 `tqsdk-data` 历史序列缓存当前默认的 TQBN daily v2 (`.tqbn`) 格式。
它只约束本仓库 Rust cache 的默认持久化合同，不扩大 public API，也不承诺兼容旧 Python
`DataSeries` binary/mmap cache、旧 `.tqseries` cache 或旧单文件 `.tqbn` layout。

相关文档：

- [data facade / research tooling](api-data.md)
- [backtest tick cache operations](backtest-tick-cache-operations.md)
- [backtest cache operator CLI](backtest-tick-cache-cli.md)
- [crate 边界审计](crate-boundaries.md)
- [验收标准与测试矩阵](validation.md)

## Current Decision

TQBN daily v2 是 `tqsdk-rust` history cache 当前默认和 canonical 格式。

TQBN daily v2 是一个 DBN-like 的内部二进制记录流格式，由 `tqsdk-data` 的
crate-internal codec 和 store adapter 实现。每个交易日分区文件仍是 append-only TQBN
record stream；store layout 按交易日拆分，避免扩展回填区间时重写单个大型 series 文件。
旧 `.tqseries` 和旧单文件 `.tqbn` layout 不是默认格式，不作为新增缓存文件目标，
也不提供兼容读取或迁移 store。

默认构建启用 Cargo feature `tqbn-zstd`。hot append writer 对 records block 使用 zstd level 1；
append-log compaction 重写 records block 时使用 zstd level 3。两种路径都只有压缩后 payload
更小时才写入压缩 block；metadata prefix、file identity、schema version 和 public facade 均不改变。
`--no-default-features` 可关闭该支持，此时 writer 写未压缩 blocks。
market-data records block 的未压缩 payload 目标上限为 8 MiB。该粒度避免日分区只形成一个超大
zstd frame，同时把 frame/header 开销和压缩率损失控制在小范围；它不是新的 public tuning knob。

## Public Interface

public cache interface 保持为：

- `HistorySeriesCache`
- `BacktestTickCache`
- `LiveTickCacheWriter`
- `MinuteKlineCache`

TQBN 的 record struct、metadata struct 和 codec helper 都是 `tqsdk-data` 的
crate-internal 实现细节。调用方不直接构造、匹配或持有 TQBN record；对外只暴露 typed
history series、coverage、scan report、purge report、backtest tick cache 和 live tick
row writer 语义。
`BacktestTickCache::mark_provisional(...)` /
`provisional_coverage(...)` 通过 `BacktestTickProvisionalCoverage` 暴露当前交易日的
非最终高水位；它不进入普通 coverage，也不能让 CacheOnly 命中。
`LiveTickCacheWriter::push_ticks(...)` 会合并连续单 tick 调用，`flush()` 显式提交不足一批的尾部；
这只改变纯 writer 的批写时机，不把 session、timer task 或后台线程下沉到 data crate。
`BacktestTickCache::compact_symbol_ticks(...)` 是 tick-only 运维入口，用于只重写指定
symbol 的全部 tick 日分区 append-log；默认远端回测补缓存成功后会走该路径合并本次写入产生的碎块。

后续如果 TQBN 的内部 record layout 需要演进，应先保持这些 public facade 不变；只有当
用户可见语义改变时，才同步调整 public API 文档和 contract examples。

## CLI 原始节假日日历 sidecar

`tqsdk-cache fill` 的 closed-day 选择和进度可使用一个独立的、非行情数据 sidecar。它不改变
TQBN / `.tqmk` 的 coverage、session 或 finality 合同，且 coverage 仍是数据完整性的唯一权威。
sidecar 复用 `tqsdk-data::DataClient::query_trading_calendar_holidays()` 的 credential-free Shinny
holiday source，按 cache root 持久化为：

```text
meta/trading-calendar-holidays-v1/
  active.json
  snapshots/<content-hash>.json
```

snapshot 包含 schema version、source URL、`fetched_at`、排序去重后的 raw holiday dates、content hash
和支持年份；文件按 content hash 创建后不再覆盖。`active.json` 是原子更新的 pointer，允许相同内容的
forced refresh 只推进 pointer，而不重写历史 snapshot。reader 必须验证 pointer、snapshot hash 和
排序/年份一致性，任一缺失或损坏都不能作为 `--last-trading-days` 的 weekday fallback。

早期 `meta/trading-calendar-v1.json` 的 daily expansion 是 legacy sidecar：它不会自动删除、迁移或
覆盖，但新的 closed-day resolver 不读取它。report 只输出新 snapshot 的 source URL、fetch 时间、hash、
支持年份和 holiday count，不输出完整 raw list。

## Backtest Canonical-minute v4

本地 facade 回测不再把任意周期的 K 线写入 TQBN history-series cache。它使用独立的
`MinuteKlineCache`。其唯一持久 K 线输入是官方 server-side backtest 确认 terminal 完成的
`60s` bar；不回退到 `DataClient` 历史下载路径，也不持久化原生高周期 K 线：

| 请求 | 历史来源 | 持久化 / 回放 |
| --- | --- | --- |
| tick、quote、`<60s` K | tick cache | 按 tick 本地合成 |
| `60s` K | server-side backtest Kline stream | v4 monthly minute cache |
| `N × 60s` K (`N > 1`) | 已关闭的 canonical 60s K | `tqsdk-data` 按固定 CST `18:00` trading-day grid 本地聚合；盘中 break 不重置 bucket |

`61s`、`90s` 等不是整数分钟的周期会在 facade 规划阶段拒绝。K-only `>=60s` 不会隐式请求
tick history；若仅需要 quote fallback，则隐式使用 canonical 60s K，也不会回退到 tick。

v4 文件身份如下：

| 项 | 值 |
| --- | --- |
| format id | `tqsdk.minute-kline.monthly.v4` |
| schema version | `4` |
| file extension | `.tqmk` |
| root layout | `minute-kline-v3/trading-YYYYMM/<escaped-symbol>.tqmk` |
| time basis | CST trading day，`18:00` 后归入下一交易日 |

每个文件只属于一个 `logical symbol × trading month`。写入前必须验证 calendar/session
snapshot hash；不匹配、损坏或不完整覆盖一律 fail closed，不能降级为近似命中。完成的远端
range（包括合法的零行 range）才可标记 final coverage；当前或未来交易日不得标记为 final。
文件更新按单月原子重写，reader 以流式方式读取，不必把整月 materialize 到内存。

月文件绑定的是写入时的 immutable metadata snapshot，而不是将来某次 refresh 写入的 active pointer。
因此 active pointer 后续移动不会单独使已完成分区失效。读取方可以选择保留的历史 snapshot，但必须同时
证明它覆盖完整请求窗口、schema version 与 session identity 仍和 active snapshot 一致，并用它精确验证
现存月文件。缺失保留 snapshot、session 变化、损坏文件或不能由同一个 snapshot 解释的混合分区仍 fail
closed；该回退绝不自动 purge、重写或拼接数据。remote-on-miss 的 metadata refresh 会扩展到涉及的完整
CST trading month，并保留更宽的 active pointer；若 retained snapshot 覆盖请求且 schema/session 与 active
兼容，可直接复用它，避免短查询把同一月变成不兼容 identity。仅 operator 显式使用
`tqsdk-cache --kind minute fill --repair-stale` 时，才会在 active snapshot 覆盖窗口时删除其冲突的整月分区，
再由该次 remote fill 补齐；这不改变普通 reader 的 fail-closed 合同。

目录名继续保留 `minute-kline-v3`，但它承载的是 v4 文件身份。这是刻意的诊断兼容策略：
旧 v3 文件不会被静默忽略、迁移或覆盖；读取/coverage 会 fail closed，`diagnose()` 将其报告为
`LegacyUnsupported`。如需移除旧文件，必须由操作者显式 purge。

`fast_inventory()` 是只读的 filesystem inventory：它不解码月文件，也不创建缺失 root。
`diagnose()` 是只读的深度检查，会逐文件报告 `Readable`、`LegacyUnsupported`、
`UnsupportedVersion` 或 `Corrupt`；它是排查格式问题的入口，不进行迁移或修复。

`KQ.m@...` 的 minute cache key 始终是逻辑主连 symbol。按日期解析得到的实际合约只作为 replay
event 的 `underlying_symbol` metadata，用于撮合和 quote 解释；它不会造成按 physical symbol
复制 minute 文件。

minute cache 没有 retention、max-byte eviction 或后台清理。`Refresh` 是显式的破坏性操作，
仅删除与请求窗口相交的 monthly files；显式 `purge_range` / `purge_symbol` 才可删除数据。CLI 的
`fill --repair-stale` 也是显式确认的窄范围维护操作，而非自动 reader recovery。
`CacheOnly` inspection 使用 read-only open，不创建 namespace、目录或文件。

## TQBN daily v2 File Identity

| 项 | 值 |
| --- | --- |
| format id | `tqsdk.tqbn.daily.v2` |
| schema version | `2` |
| file extension | `.tqbn` |
| root layout | `series/<YYYYMMDD>/tick/<escaped-symbol>.tqbn` 和 `series/<YYYYMMDD>/kline/<duration_ns>/<escaped-symbol>.tqbn` |

路径语义：

- `series/<YYYYMMDD>/tick/<escaped-symbol>.tqbn` 存储某一交易日的 tick history series，
  也是 `BacktestTickCache` 的 tick-only 持久缓存分区文件。
- `series/<YYYYMMDD>/kline/<duration_ns>/<escaped-symbol>.tqbn` 存储某一交易日的 K 线 history series；
  `duration_ns` 是该 K 线周期的纳秒整数值。
- `<YYYYMMDD>` 是中国市场交易日；18:00 CST 之后归下一交易日，周末归并到下一个周一交易日。
- `<escaped-symbol>` 是用于文件系统路径的 symbol escape 结果；它是 cache 内部路径合同，
  不是新的 public symbol 表示法。
- `HistorySeriesCache::tick_series_path(...)` / `kline_series_path(...)` 返回逻辑 series
  路径，不代表单个物理文件；`scan()`、coverage、purge 和 compact 会遍历匹配的全部日分区文件。

## Binary Contract

TQBN 文件是按记录顺序解码的二进制 record stream。

- 所有 scalar fields 都使用 little-endian 编码。
- 每条 record 都以 `TqbnRecordHeader` 开头。
- `TqbnRecordHeader.length_words` 表示整条 record 的长度，单位是 4 字节 word。
- 一条 record 的 byte span 是 `length_words * 4`，从 `TqbnRecordHeader` 起算。
- reader 遇到未知 record type 时，必须使用 `length_words * 4` 跳过整条 record。
- `length_words` 不能让 record 短于 `TqbnRecordHeader`，也不能越过文件尾；这类输入按格式损坏处理。

record stream 可以包含 metadata、coverage 和 data rows 等内部 record。record type 的枚举值、
metadata layout 和 codec helper 不进入 public API。

每个 block 使用 `TQBB` block header 包裹 record payload。block header 中的 flags byte 当前只定义
`0x01 = zstd records payload`。checksum 始终覆盖实际落盘 payload：未压缩 block 校验原始 records，
zstd block 校验压缩后的 bytes；reader 校验 checksum 后再按 flags 解码 records。未启用
`tqbn-zstd` 的 reader 遇到 zstd block 必须返回明确的格式错误，不能静默返回坏数据。

### Records Range Index

新写入和 append-log compaction 会在每个 market-data records block 后紧跟一个 crate-internal
`TQRI` `Index` block。entry 记录前一个 records block 的 offset 和其行时间范围 `[start, end)`；
records block 先写、index 后写，异常中断最多留下无索引 block。旧 reader 会忽略 `Index` block，
新 reader 遇到旧文件、缺失索引、offset/range 不合法或不认识的 index 时，对该 records block
回退完整解码，因此 format id 和 schema version 保持不变。

范围 reader 顺序读取小型 block header/index，只读取、校验并解压与请求范围相交的 market-data
payload。metadata、coverage 和 index 自身仍校验；未知 flags 仍必须拒绝。`scan()`、`diagnose()`、
compaction 等完整性路径继续解码整个文件，因此范围读取跳过无关 payload 不会替代深度诊断。

### Coverage Index Chain

新建日分区和 append-log compaction 会写入 crate-internal `Index` block 链：文件首个 block 是固定
`TQCI` root，之后每个 coverage record block 后紧跟一个 index block。每个 entry 指向其紧邻的、未压缩的
固定宽度 coverage block，并记录前一个 index offset 与同一 `[start, end)` range。它不改变 format id 或
schema version，普通 record reader 可以忽略 `Index` block。coverage chain 与 `TQRI` records index
可以交错存在；coverage tail 查找会忽略合法 `TQRI` entry。

coverage inspection 只有在文件尾是完整 `TQCI` 链、链最终回到首 block root，且每个引用 block 的
type、offset、checksum、coverage record 和 range 都匹配时才走小型索引读取。旧日文件、覆盖写入中断、尾部
后来追加 rows，或任一 index/coverage 校验失败时必须回退到完整 block stream 校验，绝不能把该分区判断为
complete。coverage 永远在 rows 已 `sync_data()` 后写入；异常崩溃可以留下 coverage gap，但不能让 coverage
比其 rows 更早持久化。

### Provisional Coverage Checkpoints

当前交易日盘中回填使用独立的 `ProvisionalCoverage` record（rtype `19`），记录
`range_start_ns`、`complete_through_ns`、`as_of_ns`、row 数和可选 tick id 范围。
对应 `TQCI` entry 使用 `0x02` provisional flag，并与 final coverage 共用同一条索引链。

provisional checkpoint 的合同是：

- 只表示“截至 `as_of_ns`，`[range_start_ns, complete_through_ns)` 已完成一次远端快照”；
  它永远不合并进普通 final coverage，也不能令 `BacktestTickCoverage::is_complete()` 为真。
- `range_start_ns`、`complete_through_ns - 1` 和 `as_of_ns - 1` 必须映射到同一个
  TQBN trading-day partition；跨分区 checkpoint 必须在写入前拒绝。
- 重跑盘中 fill 可以从 checkpoint 前固定 overlap 处继续，以覆盖边界迟到数据；新 checkpoint
  只有在本轮 rows 已持久化且远端 range 成功结束后才追加。远端明确成功结束的空增量也可以
  推进 checkpoint；取消、超时或未确认结束不能推进。
- provisional fill 只追加 rows/checkpoint，不在每次盘中续填后重写全历史；final reconcile
  才执行 compaction，以合并碎片、提高压缩率并清理失效 checkpoint。
- 一旦 final coverage 完整覆盖 checkpoint，读取端立即忽略它；后续 compaction 会物理淘汰
  已被 final coverage 取代的 checkpoint，并只保留每个起点最新的有效 checkpoint。
- 旧 reader 不认识 rtype `19` 时按 `length_words` 跳过 record；不认识 `0x02` 索引 flag 时
  回退扫描 record stream。因此 schema version 保持 `2`，旧 reader 最多失去续填加速，
  不能把 provisional 错判为 final。

## Price Encoding

价格字段使用固定小数点 `i64` 编码：

```text
FIXED_PRICE_SCALE = 1_000_000_000
UNDEF_PRICE = i64::MAX
```

有限价格按 `price * FIXED_PRICE_SCALE` 存储为 `i64` 固定小数点值；读取时按同一 scale
还原。写入端必须避免溢出。

未设置价格、SDK unset price sentinel，以及非有限浮点值（`NaN`、`+inf`、`-inf`）统一写为
`UNDEF_PRICE`。读取端遇到 `UNDEF_PRICE` 时必须还原为对应的 unset / undefined price 语义，
不能把它当作真实市场价格。

## Compatibility

TQBN reader 的兼容规则是：

1. 已知 v1 record type 按 v1 struct 的已知 prefix 解码。
2. 如果 `length_words * 4` 大于 v1 struct 长度，reader 解码 v1 已知字段后跳过尾部额外 bytes。
3. 如果已知 record type 的长度短于 v1 struct 所需长度，reader 必须拒绝该 record，除非有明确的
   compat module 负责该旧 layout。
4. 未知 record type 一律按 `length_words * 4` 跳过，不影响同一文件内后续已知 record 的读取。

5. 已知 block flags 按 feature-gated path 处理；未知 block flags 必须拒绝。
6. `TQRI` 是可选加速结构；缺失或无效时必须回退解码对应 records block，不能静默漏行。

这些规则允许后续 record 尾部追加字段，但不允许 silent truncation。任何需要读取旧 layout 的逻辑都应
集中在 compat module 中，不能散落在 normal decode path。

layout 兼容性单独处理：当前 store 只识别 daily v2 路径。旧单文件 `.tqbn`
(`series/<escaped-symbol>/tick.tqbn` / `series/<escaped-symbol>/<duration_ns>.tqbn`) 和旧
`.tqseries` 文件不会参与 coverage/read/purge/compact，也不会自动迁移。
