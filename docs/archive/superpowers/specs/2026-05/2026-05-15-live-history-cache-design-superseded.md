# Live History Cache Design

## 背景

当前 workspace 已经具备三条相关能力：

- `tqsdk-core` 把官方 DIFF 归一化为 runtime mutation，并维护统一状态树。
- `tqsdk-wait` / `tqsdk-stream` 从状态树投影 K 线和 Tick live window。
- `tqsdk-data` 提供 Python `DataSeries` 兼容的 mmap 历史序列缓存。

本次改动补齐三类缺口：

- 官方 K 线 / Tick row diff 可能不带 `id` 字段，Rust typed row 解码需要从 map key 补齐。
- live window 读取必须尊重 chart 的 `left_id` / `right_id`，不能读取全局最新 N 条。
- 外部接入需要把 live stream 显式喂入 `HistorySeriesCache`，并能读取最近缓存行。

## 目标

- 在 `tqsdk-core` diff normalization 中，为 `klines/.../data/{id}` 与
  `ticks/.../data/{id}` row 自动注入 `id` 字段。
- 在 `tqsdk-stream` 与 `tqsdk-wait` 的 K 线 / Tick window 投影中，只返回 chart
  bounds 内 rows。
- 在 `tqsdk-data` 扩展 `HistorySeriesCache`：
  - `append_kline_rows(symbol, duration_ns, rows)`
  - `append_tick_rows(symbol, rows)`
  - `read_latest_kline_rows(symbol, duration_ns, limit)`
  - `read_latest_tick_rows(symbol, limit)`
- 在 `tqsdk-data` 的 `stream` feature 下新增：
  - `LiveHistoryCacheWriter`
  - `LiveHistoryCacheOptions`
  - `LiveHistoryCacheWriteReport`
  - `LiveHistoryCacheWriter::new(cache, options)`
  - `write_kline_window(&tqsdk_stream::KlineWindow)`
  - `write_tick_window(&tqsdk_stream::TickWindow)`
  - `write_market_event(tqsdk_stream::MarketEvent)`
- 保持 `DataClient::from_session(...)` 默认行为不变。live cache 写入必须由用户显式创建 writer。

## 非目标

- 不新增 live session owner、后台任务、daemon、queue、manifest、writer election 或跨进程 cache service。
- 不改变 Python 兼容 mmap 文件布局。
- 不让 `tqsdk-session`、`tqsdk-wait` 或 `tqsdk-stream` 反向依赖 `tqsdk-data`。
- 不暴露“写入 Kline 可变尾 bar”的选项。本版直接对齐官方 tqsdk-python 语义：默认且固定跳过窗口最高 id 的 Kline bar。

## 设计

### Core Diff Row ID Normalization

`tqsdk-core::adapter::common::flatten_object` 已经根据路径推断
`ObjectKey::Kline` / `ObjectKey::Tick`。本次在生成 row-level mutation fields 前补一层
row id 注入：

- 路径匹配 `klines/{symbol}/{duration}/data/{id}` 时，如果 row 对象没有 `id` 字段，向 mutation fields 中加入 `id = {id}`。
- 路径匹配 `ticks/{symbol}/data/{id}` 时同理。
- 如果官方 diff 已带 `id`，保留官方字段，不覆盖。
- 无法解析 id 的路径不注入，保持现有宽容行为。

这样 typed `Kline` / `Tick` 解码在官方 row payload 缺少 `id` 时仍能得到稳定 id。

### Live Window Projection

窗口 projection 仍从 runtime market partition 读取，不引入第二棵状态树。

读取步骤：

1. 读取 `charts/{chart_id}/ready` 与 `more_data`，沿用当前 ready 判定。
2. 读取 `charts/{chart_id}/left_id` 与 `right_id`。
3. 如果 bounds 不存在或 `left_id > right_id`，返回空 window。
4. 只遍历 `[left_id, right_id]` 内的 row id。
5. 对 K 线从 `klines/{symbol}/{duration}/data/{id}` 解码。
6. 对 Tick 从 `ticks/{symbol}/data/{id}` 解码。
7. 跳过缺失或无法解码的 row，保持现有 live 稀疏 diff 容忍度。

`view_width` 继续作为 chart 请求参数和 window metadata，而不是从全局 rows 截断最新 N 条的依据。

### HistorySeriesCache Append

现有 cache 使用文件名 `symbol.duration_ns.start_id.end_id`，其中 id range 为半开区间
`[start_id, end_id)`。本次继续使用这个布局。

Append 算法：

1. 校验 rows 非空后按 `id` 去重排序；同一批 rows 内较后的 row 覆盖较早 row。
2. 持有现有 `lock_series(symbol, duration_ns)`，保证同进程和文件锁语义不变。
3. 找出与 incoming id range 重叠或相邻的现有 segment。
4. 读取这些 segment 的 rows，与 incoming rows 合并。
5. 按 `id` 去重，incoming rows 覆盖已有 rows。
6. 删除被合并的旧 segment。
7. 写出新的 Python 兼容 segment 文件。
8. 如果 incoming 与任何现有 segment 断档，则只写独立 segment，不强行填补缺口。

重复、重叠、相邻和断档 append 都保持幂等。写盘仍走 temp file + fsync + rename，不引入 manifest。

### Latest Read API

`read_latest_kline_rows(symbol, duration_ns, limit)` 与
`read_latest_tick_rows(symbol, limit)` 只读本地 cache，不联网。

行为：

- `limit == 0` 返回空 Vec。
- 扫描 matching segment 文件，按 id range 从大到小读取，直到收集足够 rows。
- 对收集到的 rows 按 id 去重。
- 返回最近 N 条，按 id 升序排列。
- 不要求缓存连续；断档时仍返回实际存在的最近 N 条。

### LiveHistoryCacheWriter

`LiveHistoryCacheWriter` 是 `tqsdk-data` 在 `stream` feature 下的显式 opt-in bridge。
它不驱动 stream，不拥有 session，只把调用方传入的 typed stream window/event 写入
`HistorySeriesCache`。

Public shape：

```rust
pub struct LiveHistoryCacheOptions;

pub struct LiveHistoryCacheWriteReport {
    pub rows_seen: usize,
    pub rows_written: usize,
    pub skipped_mutable_tail: bool,
}

impl LiveHistoryCacheWriter {
    pub fn new(cache: HistorySeriesCache, options: LiveHistoryCacheOptions) -> Self;
    pub fn write_kline_window(
        &mut self,
        window: &tqsdk_stream::KlineWindow,
    ) -> Result<LiveHistoryCacheWriteReport>;
    pub fn write_tick_window(
        &mut self,
        window: &tqsdk_stream::TickWindow,
    ) -> Result<LiveHistoryCacheWriteReport>;
    pub fn write_market_event(
        &mut self,
        event: tqsdk_stream::MarketEvent,
    ) -> Result<LiveHistoryCacheWriteReport>;
}
```

Kline semantics：

- 读取 window rows。
- 找出最高 id。
- 跳过最高 id row，视为可变尾 bar。
- 其余 rows append 到 `HistorySeriesCache`。
- 如果 window 只有一根 bar，则不写入，并报告 `skipped_mutable_tail = true`。

Tick semantics：

- 写入 window 中全部 Tick rows。
- append API 按 id 去重，重复窗口不会重复写。

MarketEvent semantics：

- `MarketEvent::KlineWindow(update)` 写入 update.value。
- `MarketEvent::TickWindow(update)` 写入 update.value。
- `MarketEvent::Quote(_)` 返回零写入 report。

## 错误处理

- 所有写入失败通过 `DataError` 返回。
- malformed cache file、IO、invalid response 继续使用现有 typed error。
- live writer 不 panic，不吞掉 append 失败。

## 文档影响

本次属于架构可见的 public API 扩展，但不改变 crate 归属或 runtime contract：

- 更新 `docs/architecture/api-data.md`，说明 live stream 到 history series cache 的 opt-in bridge。
- 更新 `crates/tqsdk-data/README.md` 与 crate docs，列出新增 API。
- 如 window projection 语义需要对用户说明，更新 `crates/tqsdk-stream/README.md` 和
  `crates/tqsdk-wait/README.md` 的 serial/window 描述。
- 根 README 只在用户入口示例需要新增时更新；本次不强制新增 root-level 示例。

## 测试计划

- `tqsdk-core`：官方 diff row 不带 id 时，解码 Kline/Tick 后 id 来自 map key。
- `tqsdk-stream`：状态树含 chart bounds 外 rows 时，window 只返回 bounds 内 rows。
- `tqsdk-wait`：同样覆盖 chart bounds 外 rows。
- `tqsdk-data`：
  - append rows 对重复、重叠、相邻、断档都幂等。
  - latest read API 返回按 id 升序排列的最近 N 条。
  - Kline live writer 跳过可变尾 bar，下一窗口推进后写入上一根完成 bar。
  - Tick live writer 重复窗口不重复写。

建议验证命令：

```bash
cargo test -p tqsdk-core
cargo test -p tqsdk-stream
cargo test -p tqsdk-wait
cargo test -p tqsdk-data --features stream
```
