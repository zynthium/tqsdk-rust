# History Cache Format

## P2 分区与编码决策

Daily、Minute、Tick 共用 `TQHIST01` envelope、双提交槽、checksum、index/extent/slice 和原子发布协议，但不共用 payload codec：Daily/Minute 使用 Kline codec，Tick 使用 `TickXorV1`。`.tqdk`、`.tqmk`、`.tqbn` 继续作为数据族和物理布局提示；magic/index 才是编码身份，因此不为统一后缀做全盘重命名。

物理分区按实测规模固定如下：

| family | 热/冷分区 | 原因 |
| --- | --- | --- |
| Tick | 开放月按交易日；封闭月按合约×交易月 | 默认缓存约 289,599 个日文件；月包预计约 15,549 个，历史最大约 47.5 MiB、p99 约 18.9 MiB |
| Minute | 合约×交易月，不增加年包 | 每年最多 12 个文件；年包会放大局部刷新、修复与锁冲突 |
| Daily | 每逻辑合约一个全历史文件 | 每日只追加最新日期；全量 payload 校验只属于 doctor/显式迁移 |

Tick 开放月份写入 `series/<YYYYMMDD>/tick/<escaped-symbol>.tqbn`
（`partition_scheme=1`）；封闭月份合并到
`series/monthly/<YYYYMM>/tick/<escaped-symbol>.tqbn`
（`partition_scheme=2`）。月包内仍按交易日组织 extent；server chart Tick id
只在同一交易日内参与 replay 去重，不能跨日比较。

封存按“候选深验 → 原子发布 → 删除日源文件”执行。月包发布后即为该月权威；若进程在发布与
清理之间中断，reader 优先月包，重跑只删除 row key/coverage 已被月包包含的残留日文件，
绝不把残留反向合并进月包。已封存月的迟到写入直接更新月包。范围 purge 重写相交月包并同步
删除相交残留日文件，避免旧日数据复活。

锁粒度绑定物理 mutation 单元：开放 Tick 日文件、封闭 Tick 月包、Minute 月文件、
Daily 合约文件各自使用逐文件 sidecar lock。普通 fill/query 持 root shared gate；
lazy reader 在候选路径全部打开/固定前同样持 root shared gate；封存、格式迁移和破坏性维护
持 root exclusive gate，再按稳定路径顺序取得文件锁。目录级 Tick lock 已退役。

Daily 只追加最新日期，Minute 只处理受影响交易月；正常续填依赖 header/index/coverage，
不会重读全历史 payload。深度 checksum/row 验证只属于 doctor、显式 verify 或迁移。

Tick schema 4 使用 `TickXorV1`（payload `TX01`）可变宽编码：首行完整，后续行编码
id/time delta 和变化字段。每条接收 snapshot 都持久化；canonicalization 只消除已证明的
跨批重放。pre-schema-4 Tick 文件在 read/coverage/write/compact 路径统一 fail closed，
只能用冻结迁移器处理。

新容器使用双提交槽、独立压缩/校验块、物理 block 表和独立的逻辑 extent 表。
extent 明确记录 coverage、metadata identity、finality 和有效行切片；零切片表示
已确认空区间，不能按最后一行时间推断 coverage。新 Kline81 编码在九个 72-byte
market fields 后写入 epoch presence byte 和 8-byte epoch，保留 `None`、
`Some(i64::MIN)`、整数极值、NaN payload 和负零。

daily 短查询只校验相交块；doctor 深度校验全部块。reader 在共享 companion 锁内
固定已打开 FD、索引和提交长度，释放锁后不重新打开路径。写者在恢复/追加前核对
canonical 路径与 FD 身份并拒绝 leaf symlink；此协议依赖合作式写者持有稳定 companion
锁，不宣称抵御外部恶意进程任意替换目录。共享 data inode 在原地修改前分离。

daily 离线迁移接收旧 raw/KLOG，先备份，在未发布候选文件上逐字段比较 rows 位值、
epoch、coverage、symbol 和 snapshot，再原子替换；运行时不回退旧格式。
snapshot manifest 必须声明 `history-container-v1`，有压缩块时还须声明 `tqbn-zstd`；
未提交 suffix 不得发布。现阶段路径后缀仍是 `.tqdk`，统一后缀属于后续受控布局迁移。

分配预算按索引声明的解码字节与 Rust 行对象计算，不按压缩文件长度估算。
daily 暂以 64 blocks/extents 或 256 KiB retired index bytes 触发压实；这只是索引
增长保护，最终块粒度、索引方案和阈值必须通过 P3 测量确定。

Linux 候选文件在发布前同步文件，rename 后同步父目录。Windows 已有文件身份与
多链接保护的类型检查，但 non-Unix 父目录同步仍为空操作；尚未证明 Windows 断电
持久性、原生替换和旧 FD 行为，不能宣称与 Linux 等强的断电恢复保证。

## TQBN 共享 inode 写保护

TQBN 追加入口在验证 prefix/schema 后、恢复截断和写入之前，检查 data inode 的共享状态。
Unix / Windows 多链接文件在已有 companion 和旧 data-inode 排他锁内复制到独占创建的临时文件，
同步数据后原子替换并同步父目录；旧 reader 和保留硬链接继续观察原 inode。
复制保留完整物理字节，因此原 companion checkpoint 对新文件仍有效；未提交 suffix
仅在新 inode 上恢复。Unix / Windows 单链接追加不复制；其他平台无法证明私有性时明确拒绝修改。

Companion checkpoint 若自身存在多个硬链接，追加和压实在修改数据前明确失败。
不能原子替换该 lock inode 来绕过问题，否则既有等待者和新打开者可能进入不同锁域。
这类布局必须停机修复。普通 purge 仅 unlink，不因此拒绝共享文件或增加复制；
compaction 保留原有临时文件替换路径。Windows 通过已打开句柄查询链接数，查询失败即拒绝写入。
新 snapshot 仍不得通过 hardlink 共享可变行情 inode；写保护不是允许重新启用该优化。

验证：`cargo test -p tqsdk-data --lib hardlink_tests`。

COW 发布前崩溃遗留的 `<symbol>.tqbn.cow-<pid>-<nanoseconds>-<sequence>`
只作为私有临时产物处理。三个后缀字段必须均为合法非空十进制整数；snapshot 不复制也不创建
占位文件，其他未知文件仍拒绝。源目录遗留文件不由 snapshot 自动删除。

日线和分钟线的九个 market fields 现在由内部 `kline_codec` 统一编码，保留 little-endian
整数和浮点原始位模式；现有 daily/minute envelope 的 epoch 位置不变。本次抽取不改变磁盘版本、
后缀或分区，也不宣称统一容器迁移已完成。

分钟 fill 私有 journal 位于 `.backtest-history-staging/minute-v1/`，不是 KLOG 分区，
不改变正式缓存格式；terminal、重启验证、grace 和回滚见 [Fill 恢复](history-fill-recovery.md)。

## Canonical Kline append envelope v1

分钟线仍按合约、交易月分区，日线仍按逻辑合约保存。两种 canonical payload（TQMK v5、
TQDK v1）必须放入独立的 `TQKLOG01` 物理封装；下文定义的是片段内原始 payload 布局，
不是普通 reader 仍支持的 standalone 文件布局。发布 manifest 必须声明 `kline-append-v1`。
旧 raw 文件仅供显式离线迁移；普通 reader/fill/doctor/snapshot 不再兼容。所有访问程序须同步升级。

封装包含 8-byte magic、两个 48-byte commit slot，以及独立编码的 payload 片段和 JSON 索引。
slot 的 generation、index offset、index length、committed length、index checksum、slot checksum
为六个 little-endian u64；checksum 使用 FNV-1a 64。writer 持 canonical 分区 exclusive lock，
先写新增 payload 和索引并 fsync，再写交替 slot 并 fsync。reader 选择校验有效的最新 slot，
固定 committed prefix；有效 slot 指向的索引损坏必须报错，不能回退为缓存缺失。
损坏的未提交 slot 可保留上一提交。普通读取忽略未确认 suffix，下一次 writable fill preflight
在锁内截断 suffix；doctor 和 snapshot manifest 构建拒绝带 suffix 的文件。

普通 inspect/coverage 只验证提交索引、元数据身份和覆盖区间，不重新证明旧 payload 未发生
磁盘损坏。查询读取片段时验证 checksum、行顺序与覆盖归属；完整读取还验证片段覆盖并集和
行数与索引严格一致。doctor 和 CLI verify 深度验证，不能用 coverage 命中代替完整性审计。
doctor 持分区共享锁，等待在途追加完成后再判断尾部。分钟 reader 按片段流式解压；
日线有界读取在分配片段前重新核对已打开文件大小和预算。

同 snapshot、严格位于旧覆盖之后的续填只追加新增数据。重叠修订或 snapshot 变化仍完整
验证、合并并原子重写为 KLOG。最多保留 64 个片段，之后由下一次写入压实，限制历史索引
累计空间。新建文件直接生成 KLOG，不再先写 raw；fill preflight 只恢复尾部，不隐式升级。
日线远端片段最多 32 天；取消后仅重新请求未提交片段，不将未完成 rows 声称为 final。

旧 raw 的迁移、外部备份与回滚规则见 [离线格式收缩](kline-cache-migration.md)。
迁移持 root 排他锁和分区排他锁，先备份、在临时文件中逐字节和深读验证，再原子替换。
以前的 `.kline-append-backups/` 只保留作回滚材料，不参与 live coverage 或 snapshot。

新 snapshot 的 `.tqmk`/`.tqdk` 只允许 reflink/copy，禁止 hardlink；KLOG generation 多硬链接
一律拒绝。旧 raw generation 需私有克隆、迁移后重新发布，不能原地修改。
writer 追加前对已有多链接 inode 做 copy-on-write；非 Unix 平台保守复制，保护回滚备份。

本文件只定义底层 cache 文件格式、file lock 和 opened-file snapshot 语义。history generation 的
manifest、CURRENT、lease、发布/恢复/GC 合同见 [history-snapshot-manifest.md](history-snapshot-manifest.md)。

## 文档定位

本文定义 `tqsdk-data` 历史缓存当前格式与恢复合同，不承诺兼容 Python
`DataSeries` binary/mmap 文件。Daily、Minute、Tick 均使用 `TQHIST01` envelope；
payload codec 和物理后缀按数据族区分。

Tick 新建、读取、追加、coverage、provisional、compact 与月包封存都只接受 common schema 4。
pre-schema-4 Tick 文件 fail closed；旧默认缓存已由冻结迁移器完成一次性迁移。Kline TQBN
codec 仍服务其现有路径，但不能借此重新引入 Tick v2/v3 fallback。

默认构建启用 Cargo feature `tqbn-zstd`。hot append writer 对 records block 使用 zstd level 1；
append-log compaction 重写 records block 时使用 zstd level 3。两种路径都只有压缩后 payload
更小时才写入压缩 block；同一 v3 文件内的 zstd 选择不改变 metadata prefix、file identity、schema
version 或 public facade。
`--no-default-features` 可关闭该支持，此时 writer 写未压缩 blocks。
market-data records block 的未压缩 payload 目标上限为 8 MiB。该粒度避免日分区只形成一个超大
zstd frame，同时把 frame/header 开销和压缩率损失控制在小范围；它不是新的 public tuning knob。

## Public Interface

public cache interface 保持为：

- `HistorySeriesCache`
- `BacktestTickCache`
- `LiveTickCacheWriter`
- `MinuteKlineCache`
- `DailyKlineCache`

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
symbol 的全部 tick 日分区 append-log；范围版本只重写相交日分区。默认远端 final 回测补缓存会按
`symbol × trading day` 对本轮实际远端回填范围去重后 compact 相交日分区，provisional fill 不 compact。

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

## Backtest Canonical-minute v6

本地 facade 回测不再把任意周期的 K 线写入 TQBN history-series cache。它使用独立的
`MinuteKlineCache` 与 `DailyKlineCache`。持久 K 线输入只接受官方 server-side backtest 确认 terminal
完成的 `60s` 或 native `1d` bar；不回退到 `DataClient` 历史下载路径：

持久源分层是强合同：tick 服务 tick 与 `<60s`，canonical minute 服务 `60s..<1d`，native daily
服务 `1d..=28d`。daily 缓存缺失、损坏或 coverage 不完整时直接失败，不得回退 minute 合成。

| 请求 | 历史来源 | 持久化 / 回放 |
| --- | --- | --- |
| tick、quote、`<60s` K | tick cache | 按 tick 本地合成 |
| `60s` K | server-side backtest Kline stream | v5 monthly minute cache |
| `N × 60s` K (`N > 1` 且 `<1d`) | 已关闭的 canonical 60s K | `tqsdk-data` 按固定 CST `18:00` trading-day grid 本地聚合；盘中 break 不重置 bucket |
| `1d` K | server-side backtest native daily chart | v1 logical-symbol single file |
| `2d` 到 `28d` K | 已关闭的 native 1d K | `tqsdk-data` 按 native timestamp phase 本地聚合 |

`61s`、`90s` 等不是整数分钟的周期、非整数日和大于 `28d` 的日周期会在 facade 规划阶段直接
validation error。K-only `>=60s` 不会隐式请求 tick history；若仅需要 quote fallback，则隐式使用
canonical 60s K，也不会回退到 tick。

v5 文件身份如下：

| 项 | 值 |
| --- | --- |
| format id | `tqsdk.minute-kline.monthly.v6` |
| schema version | `6` |
| file extension | `.tqmk` |
| root layout | `minute-kline-v3/trading-YYYYMM/<escaped-symbol>.tqmk` |
| time basis | CST trading day，`18:00` 后归入下一交易日 |

每个文件只属于一个 `logical symbol × trading month`。写入前必须验证 calendar/session
snapshot。hash 相同是快速路径；hash 不同时，只有双方 content-addressed sidecar 都存在，且实际 cached
range 内 schema、market、logical symbol、session、交易日和 physical mapping 完全相同，才能复用旧
coverage。缺失 sidecar、语义不匹配、损坏或不完整覆盖一律 fail closed，不能降级为近似命中。完成的远端
range（包括合法的零行 range）才可标记 final coverage；当前或未来交易日不得标记为 final。
原始文件与重叠修订按单月原子重写；普通续填使用上述追加封装。reader 保持流式读取。
每个文件的 metadata 与 coverage 保持原始二进制布局；row payload 仅在 zstd 压缩后更小时才以
zstd frame 保存。压缩是无损的：它保留全部 Kline row 的 id、datetime、OHLC、volume、OI 和 epoch，
不得因零成交或重复字段删除、合成或按固定频率填充 row。reader 流式解压并按未压缩 payload checksum
验证所有 row。

月文件绑定的是写入时的 immutable metadata snapshot，而不是将来某次 refresh 写入的 active pointer。
因此 active pointer 后续移动不会单独使已完成分区失效。滚动 refresh 仅延长尾部日期时，读取方逐个已缓存
range 对比旧、新 immutable sidecar；重叠区间语义完全相同即可继续命中，新增日期保持 miss。写入新增 final
range 前必须再验证该月全部既有 coverage，全部兼容后才把整月 header 原子迁移到新 snapshot，避免用新 hash
错误背书旧数据。缺失 sidecar、session/交易日/映射变化、损坏文件或语义冲突的混合分区仍 fail closed；
该兼容路径绝不自动 purge 或拼接冲突数据。remote-on-miss 的 metadata refresh 会扩展到涉及的完整
CST trading month，并保留更宽的 active pointer；若 retained snapshot 覆盖请求且 schema/session 与 active
兼容，可直接复用它，避免短查询把同一月变成不兼容 identity。仅 operator 显式使用
`tqsdk-cache fill --kind minute --repair-stale` 时，才会在 active snapshot 覆盖窗口时删除其冲突的整月分区，
再由该次 remote fill 补齐；这不改变普通 reader 的 fail-closed 合同。

目录名继续保留 `minute-kline-v3`，但新建文件身份是 monthly v6；路径版本不代替文件 magic/index 身份。
旧 v4 文件不会被普通 reader/fill 静默读取、迁移或覆盖；读取/coverage 会 fail closed，`diagnose()`
将其报告为 `LegacyUnsupported`。operator 如需升级，必须显式执行
`tqsdk-cache migrate --kind minute --apply --backup-dir DIR`：该命令先深度校验全部 v4 input，
将原 `.tqmk` 及其 `.tqmk.lock` 备份到 cache root 外的同一文件系统，再逐月原子重写为 v6 并用 doctor
复检。v3 及其他版本仍不可迁移；如需移除必须显式 purge。

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

## Backtest Native-daily v1

native daily cache 与 minute cache 独立；它只保存 official server-backtest `set_chart` 的
`duration=86400000000000` terminal stream，而不是从 minute/tick 合成的日线。文件身份如下：

| 项 | 值 |
| --- | --- |
| format id | `tqsdk.daily-kline.single-file.v1` |
| schema version | `1` |
| file extension | `.tqdk` |
| root layout | `daily-kline-v1/<escaped-logical-symbol>.tqdk` |
| file granularity | 一个 logical futures symbol 的所有 final 1d coverage；不按时间分区 |

每次 remote fill 只请求和写入实际 missing `[start, end)` 区间；只有 stream terminal、chart cleanup
成功且 range 在当前 CST trading day 之前，才可提交追加索引或原子替换文件。合法零行 range
同样可以 final。取消、超时、协议错误、当前/未来交易日都不能提交 coverage。

文件同时保存 immutable metadata snapshot、coverage、Kline rows 和 checksum。snapshot identity 不同时，reader
只能从 content-addressed retained metadata sidecar 加载两端 snapshot，并对每个已有 coverage range 严格比较
schema、market、logical symbol、session、交易日和 physical mapping；任何 sidecar 缺失/损坏或比较失败均 fail
closed，绝不降级为 cache miss。若比较全部通过，旧 coverage 可以读取；下一次写入新缺口必须在 per-symbol lock
内原子 reheader 到新 snapshot。checksum 错、未知 schema version、损坏或 symbol 不一致同样 fail closed，不能自动
修复或拼接。`fast_inventory()` 只读取 fixed header 与 embedded logical symbol，不以转义文件名推断 symbol；
`diagnose_all()` 完整解码所有文件并校验 checksum/rows，单 symbol `inspect()` / `diagnose()` 同样只读。
唯一 destructive recovery 是显式 `purge_symbol()` 删除整个 logical-symbol 文件。每次 atomic replace
都 fsync file 后 rename，再 fsync parent directory。该 cache 没有
retention、TTL、max-byte eviction、后台 refresh 或自动 cleanup。

`KQ.m@...` 仍以 logical symbol 为文件 key，metadata sidecar 负责验证请求窗口的 calendar/session/
mapping identity；不会按 dated physical symbol 复制文件。native 1d row 只保存 Kline 的 OHLC、volume、
open/close OI。结算价、涨跌停价目前不在 Kline 或 daily cache schema 中，未支持。

`2d` 至 `28d` K 只从完整 final 1d rows 按第一个 native timestamp 的稳定 phase 在内存中聚合，不创建新
cache 文件。此 phase 与 official high-period chart 的实际一致性不由固定 CST 假设推断；tag CI 必须以外置、
哈希验证的 official `tqsdk-python` golden packet 验证物理夜盘/假日、`KQ.m` roll、`KQ.i` 的 1d/2d/5d/28d。

## Tick schema 4 File Identity

| 项 | 值 |
| --- | --- |
| format id | `tqsdk.history-container.tick.v1` |
| schema version | `4` |
| file magic | `TQHIST01` |
| file extension | `.tqbn` |
| hot layout | `series/<YYYYMMDD>/tick/<escaped-symbol>.tqbn` |
| sealed layout | `series/monthly/<YYYYMM>/tick/<escaped-symbol>.tqbn` |

schema 4 只定义 Tick common-container。Kline 虽保留同一 `.tqbn` 后缀，但仍使用下文独立的
Kline TQBN backend；后缀名不能作为格式分派依据。

热分区按交易日保存。闭月维护在 root-exclusive gate 下先验证同一 symbol 的全部分区，再原子发布
月包并删除已经包含的日源；当前月继续按日追加。18:00 CST 后归下一交易日，周末归并到下一个
周一交易日。逻辑 series path 不代表单个物理文件，range reader、coverage、purge 和 compact 都按
请求区间枚举日文件与月包。

## 文件锁与 opened-file snapshot

`.tqsdk-cache-operation.lock` 是 cache-root generation gate：普通读写持 shared；迁移、purge、
closed-month sealing、repair apply 与 destructive maintenance 持 exclusive。调用方已经附加同根
exclusive token 时，只能通过显式 caller-held bypass 进入内部操作，不能再次取得 shared lock 自锁。

每个当前 Tick 文件只使用同路径 `<file>.tqbn.lock` 做 advisory per-file lock。退役的
`<partition>/.tqbn.lock` 不再读取或创建；public repair report 中相应字段仅保留为空/零的兼容壳。
writer 在锁内完成 append 或 copy-on-write 发布；reader 在 root gate 内枚举并打开所有候选 FD，随后
读取已固定的 inode generation。硬链接数据文件在 mutation 前必须分离，避免改写 snapshot inode。

## Binary Contract

### Tick schema 4 common-container

文件以 `TQHIST01` 开始，包含有界 metadata、两个固定 commit slot、不可变 data block 与 embedded
index/extent。有效 commit slot 指向一个完整且 checksum 正确的 index generation；reader 选择最新有效
commit。append 先写 data/index 并持久化，再切换 commit slot，因此未提交尾部不可见，torn slot 可回退
到上一 generation。coverage 与 provisional checkpoint 都属于 committed embedded state，不使用 Kline
TQBN 的 `TQCI` chain 或 sidecar tail checkpoint。

### TickXorV1 sparse payload

schema 4 Tick data block 使用 `TX01` sparse payload：首行保存完整 keyframe，后续行保存 id/time 的
zigzag-varint delta、changed-field bitmask 与变化字段值。depth、row count、payload 长度、checksum 和
字段 mask 都有上限；损坏或未知编码 fail closed。

writer 以 8,192 行为目标块大小。顺序 append 只解码可能重叠的边界块；ID 回退或跨批重放改用
交易日级稳定 canonicalization，维持 last-write-wins 与 payload replay 语义。实现不再创建 SQLite
spill，也不保留 pre-schema-4 Tick reader。普通范围读取只访问相交块及必要 index；doctor、verify 与
compaction 才深验全部块。

### Coverage and provisional state

final coverage、row count、id bounds 与单调性证明随 schema 4 embedded index 一起提交。provisional
checkpoint 显式记录 `range_start_ns`、`complete_through_ns`、`as_of_ns`、rows 与可选 id range；它不计入
final cache hit，且不能覆盖或降级已经提交的 final coverage。

### Kline TQBN backend（保留）

`series/<YYYYMMDD>/kline/<duration_ns>/<escaped-symbol>.tqbn` 继续使用 Kline TQBN framing。
`TQBB` record blocks、`TQCI` coverage-index chain、`TQRI` range index、
`TqbnRecordHeader.length_words` 与 sidecar tail checkpoint 的合同仅适用于该 backend，不属于 Tick
schema 4。启用 `tqbn-zstd` 时，只有压缩后更小才设置压缩 flag；checksum 始终覆盖实际落盘 payload，
未启用该 feature 的 reader 遇到压缩 block 必须明确报错。
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

Kline TQBN reader 继续遵循其 record/block 向前兼容规则；这不构成 Tick schema 兼容承诺：

1. 已知 record type 按已知 prefix 解码，额外尾部 bytes 跳过。
2. 已知 record type 短于所需长度时拒绝，除非有明确 compat module。
3. 未知 record type 按声明长度跳过，不影响后续已知 record。
4. 已知 block flags 按 feature-gated path 处理；未知 flags 拒绝。
5. `TQRI` 仅是加速索引；缺失或无效时回退完整解码对应 records block。
6. tail checkpoint 无效时严格扫描 Kline TQBN，不得静默丢 coverage。

Tick runtime 只接受 common schema 4。旧 Tick TQBN 以及旧单文件布局不会进入
coverage/read/write/purge/compact，也不会被当作 cache miss 自动覆盖。
