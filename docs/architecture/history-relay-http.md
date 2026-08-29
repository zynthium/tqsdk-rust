# Relay History HTTP v1 Contract

## 范围

本文固定 `tqsdk-relay` 私有 history 模块的 HTTP/1 wire contract。服务只读、CacheOnly，并部署在
受控网关后。ownership 和隔离理由见 [history-relay.md](history-relay.md)。

支持三个 endpoint：

- `GET /v1/history/query`
- `GET /v1/history/coverage`
- `GET /v1/history/schema`

不支持 POST、批量 symbol、分页、cursor 或 streaming response。未知 path 返回 404；已知 path 的
非 GET 方法返回 400 stable error。OPTIONS 不产生 CORS opt-in。

## 通用请求规则

query 与 coverage 接受：

| 参数 | 规则 |
| --- | --- |
| `symbol` | 必需，恰好一个 concrete、`KQ.i@` index 或 `KQ.m@` main symbol |
| `series` | 必需，`tick` 或 `kline` |
| `period` | kline 必需、tick 禁止；支持现有全部合法周期 |
| `start` | 必需，任意 offset 的 RFC3339 |
| `end` | 必需，任意 offset 的 RFC3339，必须严格晚于 start |

合法 Kline period 是：

- 小于 60 秒的现有合法 sub-minute period；
- `1m` 起的整数分钟日内 period；
- `1d` 至 `28d` 的整数日 period。

区间固定为 `[start, end)`。服务把输入时间转换到内部纳秒，但输出固定使用
`Asia/Shanghai` 的 `+08:00` 表示。

`query` 另接受：

| 参数 | 规则 |
| --- | --- |
| `fields` | 可选、逗号分隔；按请求顺序 projection，alias canonicalize 后禁止重复 |
| `include` | 可选；唯一合法值为 `provenance` |

`coverage` 禁止 `fields` 和 `include`。`schema` 不接受 query parameter。未知、重复、空值或
endpoint 不支持的参数均为 400，不允许 silently ignore。

受控网关必须提供配置指定的 trusted identity header。header 缺失、重复、非法 UTF-8/长度或请求头
超过限制返回 400。服务不解析、记录或转发账号密码/token。

## 字段 schema

字段白名单、长 alias、canonical 短名、值类型、顺序和默认 projection 由
`tqsdk-data::backtest_history` 的 typed schema 唯一定义，并与 `tqsdk-cache query` 共用。

默认 Kline 字段：

```text
t,o,h,l,c,v,oi
```

默认 Tick 字段：

```text
t,lp,ap1,av1,bp1,bv1,v,oi
```

`tns` 可选并返回原始纳秒十进制字符串；Tick `id` 可选。毫秒时间不是 Tick 唯一键。

JSON cell 编码：

- 所有整数，包括 volume、open interest、tick id 和 `tns`，使用十进制 JSON string；
- 有限浮点使用 JSON number；
- 缺失值和非有限浮点使用 JSON null；
- 不输出 NaN、Infinity 或实现相关整数精度。

时间字段：

- Tick `t`：RFC3339，固定毫秒精度和 `+08:00`；
- 秒/分钟 Kline `t`：RFC3339 到秒，固定 `+08:00`；
- `1d` 及以上 Kline `t`：`YYYY-MM-DD`；
- Kline 时间表示 bar start；`1d` 是 trading day；`2d`–`28d` 是 bucket 的第一个 trading day。

## 成功响应

### Query

```json
{
  "snapshot_id": "s-20260829-8d19c4af",
  "columns": ["t", "o", "h", "l", "c", "v", "oi"],
  "rows": [
    ["2026-08-01T09:00:00+08:00", 3100.0, 3110.0, 3090.0, 3105.0, "12345", "67890"]
  ]
}
```

列名必须是 canonical 短名，rows 中 cell 的位置与 `columns` 完全一致。完整 final coverage 但没有
row 时返回 200 和空 `rows`。

具体和指数 symbol 不返回映射段。main symbol 仅在 `include=provenance` 时增加顶层：

```json
{
  "provenance": {
    "logical_symbol": "KQ.m@SHFE.au",
    "segments": [
      {
        "symbol": "SHFE.au2612",
        "start": "2026-08-01T00:00:00+08:00",
        "end": "2026-09-01T00:00:00+08:00"
      }
    ]
  }
}
```

segment 使用半开区间且只出现一次，不逐行重复。

### Coverage

coverage 必须调用与 query 相同的 strict planner/source inspection。manifest 中的 coverage 摘要不是
判断依据。成功返回：

```json
{
  "snapshot_id": "s-20260829-8d19c4af",
  "symbol": "SHFE.au2612",
  "series": "kline",
  "period": "1m",
  "start": "2026-08-01T00:00:00+08:00",
  "end": "2026-08-02T00:00:00+08:00",
  "complete": true,
  "final": true,
  "metadata_snapshot_hash": "sha256:..."
}
```

缺口、provisional 或 metadata 不足返回 409，不返回 `complete: false` 的 200。

### Schema

schema 返回 wire version、可用 series、合法 period class、每个 canonical field 的 alias/value kind，
以及默认字段。schema 内容来自 data typed schema，不需要有效 snapshot；history listener 未启动时仍
不存在该 endpoint。

## All-or-nothing 与限制

relay 增量消费 run chunks，但在 terminal success 前不发送 body。terminal report 必须同时证明：

- 请求 coverage 完整；
- 全部 row final；
- metadata snapshot hash 与 pinned generation 一致；
- concrete/index/main physical plan 与 pinned authoritative catalog 一致；
- 响应未超过 row、byte、timeout 和 daemon-global buffer 限制。

失败时丢弃所有 buffered rows 并发送一个完整错误响应。不得把 partial rows 与错误混合。

默认限制为 active 8、queue 100 ms、total timeout 10 s、Kline 10,000 rows、Tick 50,000 rows、
uncompressed 32 MiB、daemon-global buffer 512 MiB。

## Stable errors

错误体固定为：

```json
{
  "error": {
    "code": "coverage_incomplete",
    "message": "requested range is not fully covered",
    "request_id": "r-...",
    "details": {
      "missing_ranges": [
        {"start": "2026-08-01T01:00:00+08:00", "end": "2026-08-01T02:00:00+08:00"}
      ]
    }
  }
}
```

status 与 code：

| HTTP | code |
| ---: | --- |
| 400 | `invalid_request`, `missing_identity` |
| 404 | `route_not_found`, `symbol_not_found` |
| 409 | `coverage_incomplete`, `provisional_data`, `metadata_incomplete` |
| 413 | `row_limit_exceeded`, `response_too_large` |
| 429 | `history_overloaded` |
| 500 | `snapshot_corrupt`, `history_internal` |
| 503 | `history_unavailable`, `snapshot_unhealthy` |
| 504 | `history_timeout` |

只有 manifest 明确声明 complete authoritative catalog，且 data strict inspection 也确认 symbol 不存在时
才能返回 404。普通 cache miss、metadata 缺失或无法证明 catalog 完整时返回 409/503，不得猜测 404。
`route_not_found` 只表示 URL path 不属于上述三个 endpoint，不表示 symbol 或 cache 状态。

active generation 首次检测到运行时损坏并赢得 relay-local atomic `healthy -> unhealthy` 转换的
请求返回 500 `snapshot_corrupt`；同 generation 的其他并发和后续请求返回 503
`snapshot_unhealthy`。health state 不写入 immutable snapshot，这不改变 market readiness。

`message` 供人阅读但不作为机器判断；调用方只依赖 `code` 和 typed `details`。

## ETag 与 gzip

- 每个完整 representation 使用 strong ETag；
- ETag 对选定 representation 的精确 bytes 计算，因此 identity 与 gzip ETag 不同；
- 支持 `If-None-Match: *`、单值和逗号分隔列表；
- strong match 返回 304，无 body；
- 有压缩协商的响应始终带 `Vary: Accept-Encoding`；
- 只有 client 明确接受 gzip、body 至少 64 KiB、两个专用 worker 有空位且 CPU affinity gate 成功时，
  才使用 gzip level 1；
- compression queue 满时使用 identity，不等待另一个隐藏队列；
- gzip 时间属于同一个 10 秒 total timeout。

`snapshot_id` 仅用于诊断与审计。API 不提供 `generation=` 或其他 pin parameter。

## CORS、审计与取消

- 默认不发送 `Access-Control-Allow-*`；
- client disconnect 或 total timeout 必须取消 run，但 snapshot shared lease 保留到 coordinator 和
  blocking scan 全部结束；
- audit 不使用 symbol 作为 metrics label，不写入 secret；
- request id 由可信网关 header 或 relay 生成，并在成功 audit 与错误体中保持一致。

每个已接收连接恰好产生一条结构化 audit。字段固定为 request id、trusted identity、stable
endpoint、snapshot id、symbol、series、period、range、projected field list/count、rows、所选
response representation 的实际写出 bytes、duration、HTTP status 与 stable error code。304、
写失败、timeout 和 client cancellation 的实际写出 bytes 为 0；未知 path 只记录 stable
`unknown` endpoint，不记录原始 path、原始 header、secret 或内部错误字符串。

market `/health` 和 `/metrics` 的既有顶层字段不变；启用 history listener 时只增加嵌套
`history` 对象。history readiness 独立于 market readiness。metrics 只使用 endpoint、status
class 与 stable error code 等低基数维度；不得使用 symbol、identity、snapshot id 或 range 作为 label。

### Affinity 与 gzip 的具体规则

当且仅当 `TQSDK_RELAY_MARKET_CPU_SET` 与 `TQSDK_RELAY_HISTORY_CPU_SET` 都存在、非空、
互斥且启动时实际绑定成功时，history 启用 gzip；两者均缺失则只提供 identity。单边、
空、无效、重复、不可用、重叠或绑定失败均 fail-fast。market current thread、history
supervisor、所有 history Tokio workers 与两个 dedicated gzip workers 都必须完成绑定
握手后才报告 ready。

gzip 使用 level 1，响应体至少 64 KiB 才进入恰好两个 worker 的专用有界池。提交使用
non-blocking try admission；池满时直接发送 identity，不等待压缩。压缩和 JSON buffer
都计入 512 MiB daemon-global budget；10 s total deadline 覆盖 query、序列化、压缩和写出。

若服务启用了 gzip 协商，identity 与 gzip 响应均带 `Vary: Accept-Encoding`。ETag 对所选
representation 的精确 bytes 计算，因此两者不同；`If-None-Match` 只匹配所选 representation，
返回 304 且保留该 representation 的 `Content-Encoding`/`Vary` 等 headers。未明确接受 gzip
（包括 q=0）时发送 identity。
