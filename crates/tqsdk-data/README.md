# `tqsdk-data`

`tqsdk-data` 是 `tqsdk-rust` workspace 里预留给研究、离线数据和批量拉取能力的 crate。

当前阶段它只开放两类很窄的能力：

- `DataClient::new().query_his_cont_quotes(...)`
- `DataClient::from_session(...).get_kline_data_series(...)`

其中：

- `query_his_cont_quotes` 是纯 HTTP 的一次性 direct query，不需要 live session
- `get_kline_data_series` 复用 `tqsdk-session` 的 market history contract，但返回的是一次性 owned 结果，不引入 `wait_update()` 或 stream facade

除此之外，它仍然刻意保持极窄，不提前承诺宽 public API。

## 当前已稳定的 surface

- `DataClient`
- `HistoricalContQuotesRow`
- `KlineDataSeriesRequest`
- `KlineDataSeries`

## `get_kline_data_series` 的定位

这条接口适合承接：

- 历史 K 线一次性拉取
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
- `get_tick_data_series`
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

## 为什么现在先保持为空

因为这层一旦开始对外暴露研究型 API，就很容易把：

- 批量下载
- tabular 视图
- 文件缓存
- 兼容层

一起绑进第一版 surface。

当前更稳的做法，是先把能力边界固定在独立 crate 里，等具体实现时再按阶段逐步开放 API。

## 示例

最小可编译示例见 [examples/his_cont_quotes.rs](examples/his_cont_quotes.rs)。

session-backed 的历史 K 线示例见 [examples/kline_data_series.rs](examples/kline_data_series.rs)。

相关设计文档见 [../../docs/architecture/api-data.md](../../docs/architecture/api-data.md)。
