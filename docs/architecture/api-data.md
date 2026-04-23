# `tqsdk-data` 最小 API 草图

## 文档定位
本文档描述的是建立在现有 `tqsdk-core + tqsdk-session + replay/history contract` 之上的研究/离线数据工具层。

它回答的是：

- `tqsdk-data` 应该承接哪些能力
- 为什么这些能力不应继续塞进 `tqsdk-session` / `tqsdk-wait` / `tqsdk-stream`
- 第一阶段只起脚手架时，哪些 public surface 先不要冻结

它不回答：

- 具体 downloader 的并发调度细节
- DataFrame / polars 的最终数据形状
- 回测报告、策略分析、GUI 展示

相关文档：

- [总架构入口](README.md)
- [crate 边界审计](crate-boundaries.md)
- [未来 crate 蓝图](crate-blueprint.md)
- [路线图](../../ROADMAP.md)

## 先给结论

`tqsdk-data` 应该是一个独立的研究/离线数据 crate，而不是继续扩大 `tqsdk-session` 或 `tqsdk-wait`。

第一阶段的原则应当非常保守：

1. 先冻结 crate 边界和依赖方向
2. 先提供 compileable crate 骨架
3. 先只开放最薄的一次性研究接口，再逐步扩展

换句话说，当前最稳定的做法不是急着导出一堆研究接口，而是先明确：

- live continuous-consumption 继续留在 `tqsdk-wait` / `tqsdk-stream`
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

### 不是 `tqsdk-wait` / `tqsdk-stream`

这两层负责的是 diff-backed live 对象消费形状。

而 `tqsdk-data` 面向的是：

- 离线批量请求
- 历史窗口拼接
- 文件落盘
- 表格视图

这不是 `wait_update()` 或 stream/event 模式选择的问题。

## 第一阶段推荐范围

当前已经落地的第一阶段范围是：

- crate skeleton
- README 与 crate-level docs
- 后续实现的依赖方向约束
- `DataClient`
- `DataClient::new()`
- `DataClient::from_session(...)`
- `query_his_cont_quotes(symbols, days, end_date)`
- `HistoricalContQuotesRow`
- `get_kline_data_page(KlineDataPageRequest)`
- `get_tick_data_page(TickDataPageRequest)`
- `get_kline_data_series(KlineDataSeriesRequest)`
- `get_tick_data_series(TickDataSeriesRequest)`
- `KlineDataPage`
- `KlineDataPageRequest`
- `TickDataPage`
- `TickDataPageRequest`
- `KlineDataSeries`
- `KlineDataSeriesRequest`
- `TickDataSeries`
- `TickDataSeriesRequest`

当前明确先不做：

- downloader public API
- DataFrame / polars public API
- 历史数据缓存格式
- 并发下载调度器
- Python 兼容层
- `query_option_greeks`

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
   - 扩展 `query_his_cont_quotes`
   - `query_option_greeks`
3. local materialization
   - 文件缓存
   - 导出
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
tqsdk-wait  tqsdk-stream  tqsdk-data
                ^
                |
            tqsdk-task
```

这里的关键约束是：

- `tqsdk-data` 可以依赖 `tqsdk-session`
- 如有必要，`tqsdk-data` 也可以直接依赖 `tqsdk-core`
- `tqsdk-session` / `tqsdk-wait` / `tqsdk-stream` 不应反向依赖 `tqsdk-data`

## 当前落地策略

当前仓库里，`tqsdk-data` 已经以“窄 public surface”的方式落地。

其中比较关键的一个点是：

- `get_kline_data_page` / `get_tick_data_page` 已经落在 `tqsdk-data`
- `get_kline_data_series` 已经落在 `tqsdk-data`
- `get_tick_data_series` 也已经落在 `tqsdk-data`
- 它不是新的 session facade
- 它也不是 live ref / live stream
- `data_page` 是对底层 chart/history contract 的显式单页封装
- `data_series` 是建立在 `data_page` 之上的时间范围快照封装，语义固定为 `[start_datetime_ns, end_datetime_ns)`

这样做的意义是：

- 不把研究/批量历史接口继续塞进 `tqsdk-session`
- 不把历史数据读取和 `wait_update()` / stream 模式耦合在一起
- 给 downloader 预留稳定的 `page -> series -> downloader` 递进路径
- 后续可以在 `tqsdk-data` 上继续叠加 downloader、tabular adapters、缓存与导出，而不污染 core/session/live facade 的边界

这样做的收益是：

- 先给研究/下载能力一个明确落点
- 不用为了继续扩功能而过早冻结宽 API
- 后续实现时不需要重新讨论能力归属

## 最终判断

`tqsdk-data` 值得独立存在，但当前阶段最合理的动作是：

1. 先保持 `DataClient + query_his_cont_quotes` 足够窄
2. 在此基础上继续保持 `DataClient + data_page + data_series` 也只是 one-shot substrate
3. 继续按 history/query -> batch fetch -> materialization 的顺序迭代
4. 避免为了兼容 DataFrame 形状而提前做宽 surface
