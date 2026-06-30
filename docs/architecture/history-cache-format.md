# History Cache Format

## 文档定位

本文档定义 `tqsdk-data` 历史序列缓存当前默认的 TQBN v1 (`.tqbn`) 格式。
它只约束本仓库 Rust cache 的默认持久化合同，不扩大 public API，也不承诺兼容旧 Python
`DataSeries` binary/mmap cache 或旧 `.tqseries` cache。

相关文档：

- [data facade / research tooling](api-data.md)
- [crate 边界审计](crate-boundaries.md)
- [验收标准与测试矩阵](validation.md)

## Current Decision

TQBN v1 是 `tqsdk-rust` history cache 当前默认和 canonical 格式。

TQBN v1 是一个 DBN-like 的内部二进制记录流格式，由 `tqsdk-data` 的 crate-internal
codec 和 store adapter 实现。旧 `.tqseries` 不是默认格式，不作为新增缓存文件目标，
也不提供兼容读取或迁移 store。

默认构建写入未压缩 TQBN blocks。显式启用 Cargo feature `tqbn-zstd` 时，writer 会对
records block 使用 zstd level 1 做 per-block 压缩，且只有压缩后 payload 更小时才写入压缩
block；metadata prefix、file identity、schema version 和 public facade 均不改变。

## Public Interface

public cache interface 保持为：

- `HistorySeriesCache`
- `BacktestTickCache`

TQBN 的 record struct、metadata struct 和 codec helper 都是 `tqsdk-data` 的
crate-internal 实现细节。调用方不直接构造、匹配或持有 TQBN record；对外只暴露 typed
history series、coverage、scan report、purge report 和 backtest tick cache 语义。
`BacktestTickCache::compact_symbol_ticks(...)` 是 tick-only 运维入口，用于只重写指定
symbol 的 `tick.tqbn` append-log；默认远端回测补缓存成功后会走该路径合并本次写入产生的碎块。

后续如果 TQBN 的内部 record layout 需要演进，应先保持这两个 public facade 不变；只有当
用户可见语义改变时，才同步调整 public API 文档和 contract examples。

## File Identity

| 项 | 值 |
| --- | --- |
| format id | `tqsdk.tqbn.v1` |
| schema version | `2` |
| file extension | `.tqbn` |
| root layout | `series/<escaped-symbol>/tick.tqbn` 和 `series/<escaped-symbol>/<duration_ns>.tqbn` |

路径语义：

- `series/<escaped-symbol>/tick.tqbn` 存储 tick history series，也是
  `BacktestTickCache` 的 tick-only 持久缓存文件。
- `series/<escaped-symbol>/<duration_ns>.tqbn` 存储 K 线 history series；
  `duration_ns` 是该 K 线周期的纳秒整数值。
- `<escaped-symbol>` 是用于文件系统路径的 symbol escape 结果；它是 cache 内部路径合同，
  不是新的 public symbol 表示法。

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

这些规则允许后续 record 尾部追加字段，但不允许 silent truncation。任何需要读取旧 layout 的逻辑都应
集中在 compat module 中，不能散落在 normal decode path。
