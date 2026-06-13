# Relay 合约级实时监控面板设计

## 背景

`tqsdk-relay` 已提供全局 `/health` 和 `/metrics`，能判断 relay 进程、下游监听、上游连接、合约集合刷新和整体数据 freshness 是否正常。但这些全局字段不能回答两个运维问题：

1. 上游 universe 中每个合约是否实际收到过 tick。
2. 某个下游订阅关注的合约是否已经断流或明显延迟。

本设计为 `tqsdk-relay` 增加内置的合约级实时监控面板。它只服务 relay 自身运维观测，不改变 SDK 默认直连路径，不代理 trade/query/auth/schema/metadata，也不改变 `tqsdk-core` runtime contract。

## 目标

- 在 relay 内置 `/dashboard` 页面，使用表格优先的运维排障界面展示每个合约的数据状态。
- 同时覆盖上游合约全集和下游实际订阅合约：
  - 上游全集来自 `TQSDK_RELAY_FUTURES_UNIVERSE` 解析出的最终合约集合。
  - 下游订阅来自当前 `InterestRegistry` 中的 quote/chart interest。
- 同时展示两类延迟：
  - 接收间隔延迟：`now - relay_last_receive_time`，作为状态判定主口径。
  - 行情时间延迟：`now - tick.datetime`，作为辅助排障字段。
- 保证监控不会影响 relay 行情转发热路径性能。

## 非目标

- 不在首版实现交易时段 aware 的 stale 判断。首版使用统一 freshness 阈值，默认沿用当前 `30s`。
- 不在首版实现 SSE 增量推送；先使用前端 2 秒轮询。
- 不把 per-symbol 指标导出为 Prometheus 高基数 label。
- 不为 dashboard 额外创建下游订阅，也不让 dashboard 连接 relay market websocket。
- 不实现 tick 历史曲线、落盘、跨重启恢复或单合约历史查询。

## 方案选择

采用 relay-native per-symbol telemetry：

- `RelayEngine::ingest_tick(symbol, row)` 同步更新该 symbol 的轻量 telemetry。
- universe refresh 成功时记录应监控的合约全集。
- HTTP snapshot 请求进来时再扫描 telemetry 生成 `/symbol-metrics` JSON。
- 内置 `/dashboard` 静态页面轮询 `/symbol-metrics`。

这个方案比“dashboard 当下游客户端”更可靠，因为它能显示从未收到数据的 universe 合约，也不会改变下游订阅行为。它也比只做外部 Prometheus/Grafana 更贴合首版“内置面板”的目标。

## 数据模型

新增合约级 telemetry 结构，归属 relay 观测层。建议命名：

- `SymbolTelemetry`
- `SymbolTelemetrySnapshot`
- `SymbolMetricsSnapshot`
- `SymbolStatus`

每个合约维护字段：

```text
symbol
in_universe
quote_subscriber_count
chart_subscriber_count
ticks_ingested
last_receive_unix_millis
last_tick_datetime_ns
last_price
last_volume
last_open_interest
invalid_rows
last_invalid_row_error
```

状态枚举：

```text
live      在 freshness 阈值内收到过 tick
stale     收过 tick，但接收间隔超过 freshness 阈值
missing   在上游 universe 中，但从未收到 tick
inactive  不在上游 universe 中，但被下游订阅
```

`inactive` 用于暴露配置或订阅异常：下游正在关注某合约，但 relay 上游 universe 不包含它。

## 性能边界

监控不得影响行情转发主路径。

硬约束：

- `ingest_tick` 只做当前 symbol 的 O(1) telemetry 更新。
- `ingest_tick` 不扫描 universe，不排序，不生成 JSON，不计算全局统计，不计算 p95。
- universe 集合只在配置解析、静态 chart 构建或产品发现刷新成功时更新。
- `/symbol-metrics` 请求侧可以 O(N) 扫描合约全集和 telemetry，N 为当前 relay 上游 universe 规模。
- snapshot 排序、过滤、limit 都发生在 HTTP/API 层，不发生在 tick ingest 热路径。
- HTTP snapshot 持有 `RelayEngine` 锁的时间要短。实现时优先在锁内复制轻量 snapshot，再在锁外排序、过滤和 JSON 序列化。
- 前端默认每 2 秒轮询一次，不因输入搜索或本地排序触发高频请求。

验收时必须能说明 dashboard 不会改变下游订阅，不会触发额外 market command。

## HTTP API

保留现有端点不变：

- `/health`
- `/metrics`

新增：

- `/dashboard`：返回内置 HTML 页面。
- `/dashboard/app.js`：返回前端 JS。
- `/symbol-metrics`：返回 per-symbol JSON 快照。

首版 `/symbol-metrics` 响应：

```json
{
  "now_unix_millis": 1780949000000,
  "data_stale_after_millis": 30000,
  "summary": {
    "total": 935,
    "live": 912,
    "stale": 18,
    "missing": 5,
    "inactive": 0,
    "subscribed": 27,
    "p95_receive_gap_ms": 2400
  },
  "symbols": [
    {
      "symbol": "SHFE.au2602",
      "status": "live",
      "in_universe": true,
      "subscribed": true,
      "quote_subscriber_count": 2,
      "chart_subscriber_count": 1,
      "ticks_ingested": 5890,
      "receive_gap_ms": 820,
      "market_time_lag_ms": 1200,
      "last_receive_unix_millis": 1780948999180,
      "last_tick_datetime_ns": 1780948998800000000,
      "last_price": 610.2,
      "last_volume": 123456,
      "last_open_interest": 78910,
      "invalid_rows": 0,
      "last_invalid_row_error": null
    }
  ]
}
```

查询参数：

```text
status=live,stale,missing,inactive
subscribed=1
q=au
sort=receive_gap_ms_desc
limit=200
```

首版支持的排序至少包括：

- `symbol_asc`
- `status_asc`
- `receive_gap_ms_desc`
- `market_time_lag_ms_desc`
- `ticks_ingested_desc`

无效参数返回明确 JSON 错误，避免静默忽略导致排障误解。

## UI 设计

首屏采用表格优先，不做复杂图表。

顶部摘要条：

- live
- stale
- missing
- inactive
- subscribed
- p95 receive gap

主表格列：

- status
- symbol
- subscribed
- receive gap
- market time lag
- last receive
- last tick time
- tick count
- last price
- quote subs
- chart subs
- invalid rows

过滤器：

- 状态多选
- 只看 subscribed
- symbol 搜索
- 排序下拉
- limit 选择

状态颜色：

- `live`：绿色
- `stale`：黄色
- `missing`：红色
- `inactive`：灰色

行详情：

- 最近错误
- quote/chart 订阅计数
- 最后一条 tick 的关键字段

首版行详情只展示当前 snapshot 字段，不展示 tick 历史曲线。

## 数据流

```text
Upstream websocket tick
    -> WebSocketUpstreamTickSource decode
    -> RelayServer::pump_upstream_once / pump_upstream_until
    -> RelayEngine::ingest_tick(symbol, row)
    -> MarketCache + KlineSynthesis + SymbolTelemetry O(1) update
    -> Downstream frames

Browser /dashboard
    -> GET /symbol-metrics every 2s
    -> RelayEngine snapshot
    -> sort/filter/limit
    -> JSON response
    -> table render
```

下游订阅标记来自 `InterestRegistry`：

- quote 订阅用 `quote_interest_count(symbol)`。
- chart 订阅需要新增按 symbol 聚合的轻量读取 helper，或在 snapshot 层扫描 chart mappings 生成每个 symbol 的 chart subscriber count。

## 错误处理

- `/dashboard` 和 `/dashboard/app.js` 只返回静态内容，失败时返回 500 JSON/文本错误。
- `/symbol-metrics` 参数错误返回 400 JSON，例如 `{"error":"invalid sort"}`。
- snapshot 过程中 engine lock poison 返回 500 JSON。
- 单合约 invalid row 不影响有效 tick 摄入；合约级 invalid 计数与最近错误进入 telemetry。
- 全局上游状态仍通过 `/health` 和 `/metrics` 查看，dashboard 只补充 per-symbol 可视化。

## 测试计划

新增或扩展测试：

- telemetry unit tests：
  - universe 中但未收到 tick 为 `missing`。
  - 收到 tick 后在阈值内为 `live`。
  - 超过 freshness 阈值为 `stale`。
  - 不在 universe 但被下游订阅为 `inactive`。
- subscription tests：
  - quote 订阅计数准确。
  - chart 订阅计数准确。
  - 客户端断开后订阅计数下降。
- HTTP tests：
  - `/symbol-metrics` 返回 summary 和 symbols。
  - `status`、`subscribed`、`q`、`sort`、`limit` 正常。
  - 无效参数返回 400。
- binary smoke：
  - relay 启动后 `/dashboard` 返回 HTML。
  - `/dashboard/app.js` 返回 JS。
  - `/symbol-metrics` 返回 JSON。
- performance guard：
  - 代码结构保证 `ingest_tick` 不扫描全集。
  - snapshot 排序/过滤只在 API 层执行。

验证命令：

```bash
cargo fmt --all --check
cargo test -p tqsdk-relay --tests
cargo check -p tqsdk-relay --no-default-features
cargo clippy -p tqsdk-relay --all-targets -- -D warnings
git diff --check
```

## 后续可选增强

- `/symbol-metrics/stream` SSE，仅推送节流后的状态变化和 summary。
- 交易时段 aware freshness，结合 `TradingSessionSchedule` 或后续更完整交易日历。
- 产品/交易所分组视图。
- 最近 N 条状态变化事件流。
- 外部告警 webhook，但不要在首版增加。

## 决策记录

- 选择表格优先，热力图和事件流只作为后续增强。
- 同时监控上游全集和下游实际订阅。
- 状态主判定使用接收间隔延迟，同时展示行情时间延迟。
- 内置在 `tqsdk-relay` 的 HTTP 观测服务里，不新增独立 dashboard 进程。
- 首版使用轮询，不使用 SSE。
- 性能优先：tick ingest 热路径只能 O(1) 更新当前合约 telemetry。
