# `tqsdk-data`

`tqsdk-data` 是 `tqsdk-rust` workspace 里预留给研究、离线数据和批量拉取能力的 crate。

当前阶段它只开放三层很窄的能力：

- `DataClient::new().query_his_cont_quotes(...)`
- `DataClient::from_session(...).get_kline_data_page(...)`
- `DataClient::from_session(...).get_tick_data_page(...)`
- `DataClient::from_session(...).get_kline_data_series(...)`
- `DataClient::from_session(...).get_tick_data_series(...)`

其中：

- `query_his_cont_quotes` 是纯 HTTP 的一次性 direct query，不需要 live session
- `get_*_data_page` 是最底层的 chart/history page substrate
- `get_*_data_series` 是建立在 page substrate 之上的时间范围历史快照，语义对齐官方 `data_series`，范围为 `[start_datetime_ns, end_datetime_ns)`

除此之外，它仍然刻意保持极窄，不提前承诺宽 public API。

## 当前已稳定的 surface

- `DataClient`
- `HistoricalContQuotesRow`
- `KlineDataPageRequest`
- `KlineDataPage`
- `TickDataPageRequest`
- `TickDataPage`
- `KlineDataSeriesRequest`
- `KlineDataSeries`
- `TickDataSeriesRequest`
- `TickDataSeries`

## `data_page` 与 `data_series` 的定位

这两层接口适合承接：

- 历史 K 线 / tick 一次性拉取
- page 级分页读取
- 按时间范围组装完整历史序列
- research/offline 侧的批量读
- 后续更高层 DataFrame / polars / downloader 的底座

它当前明确不做：

- live 自动推进
- 引用型 diff-backed 对象
- `wait_update()` API
- stream / callback API

这些仍然属于 `tqsdk-wait` / `tqsdk-stream`。

## 未来应承接的能力

- downloader
- 历史数据批量拉取
- `query_his_cont_quotes`
- `query_option_greeks`
- 文件缓存、导出、落盘
- 可选的 DataFrame / polars 适配层

## 当前明确不做

- live session owner
- `wait_update()` facade
- stream/event facade
- task runtime
- `query_option_greeks`
- 回测报告与 GUI

## 为什么现在先保持极窄

因为这层一旦开始对外暴露研究型 API，就很容易把：

- 批量下载
- tabular 视图
- 文件缓存
- 兼容层

一起绑进第一版 surface。

当前更稳的做法，是先把能力边界固定在独立 crate 里，等具体实现时再按阶段逐步开放 API。

## 示例

最小可编译示例见 [examples/his_cont_quotes.rs](examples/his_cont_quotes.rs)。

session-backed 的历史分页示例见 [examples/kline_data_page.rs](examples/kline_data_page.rs)。

session-backed 的时间范围历史示例见 [examples/kline_data_series.rs](examples/kline_data_series.rs)。

相关设计文档见 [../../docs/architecture/api-data.md](../../docs/architecture/api-data.md)。
