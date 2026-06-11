# tqsdk-relay

`tqsdk-relay` 是 `tqsdk-rust` 的可选行情中继和内存缓存服务。它位于 SDK 客户端
与天勤行情 websocket 之间，让多个本地 SDK 进程共享一个上游期货 tick 源，而不是
各自展开 quote 和 K 线订阅。

当单个天勤连接可以承载全期货 tick 合约集合，但多客户端或多周期 K 线直连订阅会
触发订阅限制时，可以使用 relay。

> [!IMPORTANT]
> relay 是显式启用的基础设施。除非你通过 `.market_relay(...)` 明确将行情端点指向
> relay，否则现有 SDK 客户端仍然直连天勤。

> [!WARNING]
> V1 只覆盖行情路由，范围刻意收窄。它不代理 trade、query、auth、schema、
> metadata 或 direct-query 流量。

## 位置与边界

```text
SDK 进程 A ─┐
SDK 进程 B ─┼─ ws://127.0.0.1:7788/market ─ tqsdk-relay ─ 天勤行情 websocket
SDK 进程 C ─┘
```

relay 不改变 SDK 运行时模型：

- SDK 状态仍然走正常的 `RuntimeHandle -> StateStore -> CommitResult ->
  RuntimeReader/UpdateCursor` 路径。
- 现有 SDK crate 不依赖 `tqsdk-relay`。
- 是否使用 relay 是部署选择，不是默认行为变化。

## 当前能力

| 范围 | 状态 |
| --- | --- |
| 下游 websocket 服务 | 在本地地址接受 SDK 行情 websocket 连接。 |
| 下游命令子集 | 处理 `subscribe_quote`、`set_chart` 和 `peek_message`。未知行情命令会明确失败。 |
| 上游数据源 | 动态发现当前活跃期货合约，打开一个天勤行情 websocket，并为每个合约发送一个 duration 为 `0` 的 tick `set_chart`。 |
| 合约集合刷新 | 支持按全部期货品种或产品代码列表生成当前活跃合约集合，也可限制为每品种活跃度排名前 N 的合约；产品发现模式下按本地每日固定时间重建上游 tick chart 集合，默认 `08:30:00`。 |
| 订阅长度防线 | 在连接上游前统计单个上游 tick chart 的最大 `ins_list` 长度；超过 hard limit 会拒绝订阅，超过 warn threshold 会体现在 metrics 中。 |
| quote 分发 | 将最新 tick 投影成 quote frame，并发送给已订阅的下游客户端。 |
| 固定周期 K 线合成 | 从上游 tick 合成正周期 K 线，并向图表订阅者发送已完成的 K 线；新订阅会先用内存 tick ring 回放已完成 K 线。 |
| 缓存 | 保留内存 tick ring 和 quote 快照。当前二进制程序尚未启用磁盘持久化。 |
| bootstrap 队列 | 在 relay 内部合并并限流 chart bootstrap 请求。远端 K 线回填和 oracle 对比尚未实现。 |
| 上游恢复 | 下游监听保持运行；上游 websocket 连接失败后会重试。 |
| 启动自检 | `TQSDK_RELAY_DRY_RUN=1` 会解析配置、解析或发现上游合约集合、输出 JSON 诊断后退出，不绑定下游或 metrics 监听，也不连接上游 market websocket。 |
| HTTP 观测 | `metrics_listen` 提供 `/health` 和 `/metrics` JSON 端点；库 API 也暴露 health、metrics 和 source status 快照。 |

duration 为 `0` 的下游 tick chart 兼容不是 V1 已完成的主要能力面：relay 会摄入并
缓存上游 tick，但已经验证的实时下游分发当前聚焦 quote 和正周期 K 线。

## 快速开始

推荐让 relay 自己查询当前活跃期货合约集合。订阅全期货品种时：

```bash
export TQ_AUTH_USER="your-account"
export TQ_AUTH_PASS="your-password"
TQSDK_RELAY_FUTURES_PRODUCTS="ALL" \
cargo run -p tqsdk-relay
```

如果只想尽量减少启动时的上游 tick chart 历史补齐，可以把上游 `view_width` 调小：

```bash
TQSDK_RELAY_FUTURES_PRODUCTS="ALL" \
TQSDK_RELAY_UPSTREAM_TICK_VIEW_WIDTH=1 \
cargo run -p tqsdk-relay
```

`view_width=1` 仍会发送 tick chart 请求，但只要求最小窗口；当前 relay 不允许 `0`，因为上游是否接受完全不取历史尚未作为稳定协议验证。

如果只需要每个品种的主力和次主力，可以限制每个产品保留的活跃合约数：

```bash
TQSDK_RELAY_FUTURES_PRODUCTS="ALL" \
TQSDK_RELAY_FUTURES_ACTIVE_CONTRACTS_PER_PRODUCT=2 \
TQSDK_RELAY_UPSTREAM_TICK_VIEW_WIDTH=1 \
cargo run -p tqsdk-relay
```

只订阅每个品种的主力合约时，可以使用更直接的快捷方式：

```bash
TQSDK_RELAY_FUTURES_PRODUCTS="ALL" \
TQSDK_RELAY_FUTURES_MAIN_ONLY=true \
TQSDK_RELAY_UPSTREAM_TICK_VIEW_WIDTH=1 \
cargo run -p tqsdk-relay
```

`TQSDK_RELAY_FUTURES_MAIN_ONLY=true` 等价于
`TQSDK_RELAY_FUTURES_ACTIVE_CONTRACTS_PER_PRODUCT=1`，只走合约服务
`query_cont_quotes()` 获取主力标的，不订阅 quote，也不发送 `set_chart`。`2` 或更大的
N 表示主力加活跃度排名补足；此时 relay 会先用 `query_cont_quotes()` 获取全市场主力标的，
再对候选在市期货做一次批量 quote 快照订阅，按 `product_id` 分组后以主力优先，其余按
`open_interest` 降序、`volume` 降序补足前 N。

只订阅指定产品代码时：

```bash
export TQ_AUTH_USER="your-account"
export TQ_AUTH_PASS="your-password"
TQSDK_RELAY_FUTURES_PRODUCTS="SHFE.au,DCE.m,CZCE.MA" \
cargo run -p tqsdk-relay
```

`SHFE.au` 表示交易所限定的产品代码；`MA` 表示不限定交易所的产品代码。relay 会在
启动时通过天勤 metadata 查询当前未过期合约，再按批调用 `query_symbol_info` 获取
`exchange_id`、`product_id`、`expired` 和 `trading_time` 等 typed metadata。relay 用
这些字段过滤合约、组成上游 tick chart 集合，并把官方交易时间段写入合约级
telemetry。

默认情况下，进程会在 `127.0.0.1:7788` 监听 SDK 行情 websocket 客户端，并连接
上游 `wss://openmd.shinnytech.com/t/md/front/mobile`。
产品发现模式会按本地墙钟每天 `08:30:00` 重新发现合约集合；可通过
`TQSDK_RELAY_FUTURES_UNIVERSE_REFRESH_AT=HH:MM[:SS]` 改到你的开盘前刷新窗口。
metadata 查询默认每批 `500` 个合约；如果天勤服务或本地网络对单次 metadata query
更敏感，可以用 `TQSDK_RELAY_FUTURES_METADATA_BATCH_SIZE` 调小批次。

启动前检查配置和上游订阅规模：

```bash
TQSDK_RELAY_DRY_RUN=1 \
TQSDK_RELAY_FUTURES_SYMBOLS="SHFE.au2602,DCE.m2609" \
cargo run -p tqsdk-relay
```

dry-run 会向 stdout 输出一行 JSON，例如：

```json
{"event":"relay_startup","dry_run":true,"upstream_source":"static-symbols","downstream_listen":"127.0.0.1:7788","metrics_listen":"127.0.0.1:7789","refresh_schedule":"daily:08:30:00","futures_metadata_batch_size":500,"futures_active_contracts_per_product":null,"upstream_symbols":2,"upstream_tick_view_width":10000,"upstream_ins_list_chars":11,"upstream_ins_list_warn_chars":32000,"upstream_ins_list_max_chars":null,"upstream_ins_list_over_warn":false,"upstream_ins_list_over_max":false,"suggested_relay_instances":null}
```

如果 dry-run 使用 `TQSDK_RELAY_FUTURES_PRODUCTS`，它会执行一次 metadata 查询来得到当前
活跃合约集合；如果设置了每品种活跃合约数，还会短暂订阅候选 quote 来计算活跃度。
它仍不会绑定监听地址，也不会连接上游 tick websocket。

让 SDK 客户端连接 relay：

```rust
let mut tq = tqsdk::Tq::futures()
    .auth_env()?
    .market_relay("ws://127.0.0.1:7788/market")
    .connect()
    .await?;
```

不调用 `.market_relay(...)` 时，同一个 SDK 客户端会使用正常的天勤直连行情端点。

小规模冒烟测试或临时排查也可以直接用完整合约列表：

```bash
TQSDK_RELAY_FUTURES_SYMBOLS="SHFE.au2602,DCE.m2609" \
cargo run -p tqsdk-relay
```

完整合约文件仍保留为兼容入口，但不推荐作为长期部署方式，因为合约会随上市/退市
变化：

```bash
TQSDK_RELAY_FUTURES_SYMBOLS_FILE="./futures-symbols.txt" \
cargo run -p tqsdk-relay
```

如果既没有设置 `TQSDK_RELAY_FUTURES_PRODUCTS`，也没有设置完整合约覆盖入口，relay
只会启动下游服务，不连接上游。这个模式适合做本地协议冒烟测试，但不会产生实时行情
数据。

## 配置

二进制程序读取以下环境变量：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `TQSDK_RELAY_FUTURES_PRODUCTS` | 空 | 推荐入口。设置为 `ALL` / `all` / `*` 表示动态查询全部活跃期货合约；也可传逗号分隔产品代码，例如 `SHFE.au,DCE.m,CZCE.MA`。 |
| `TQ_AUTH_USER` | 空 | 产品发现需要的天勤账号。只有使用 `TQSDK_RELAY_FUTURES_PRODUCTS` 时必需。 |
| `TQ_AUTH_PASS` | 空 | 产品发现需要的天勤密码。只有使用 `TQSDK_RELAY_FUTURES_PRODUCTS` 时必需。 |
| `TQSDK_RELAY_DRY_RUN` | `false` | 设置为 `1` / `true` / `yes` / `on` 时执行启动自检并输出 JSON 诊断后退出。 |
| `TQSDK_RELAY_FUTURES_UNIVERSE_REFRESH_AT` | `08:30:00` | 产品发现模式下每日重建上游合约集合的本地时间，格式为 `HH:MM[:SS]`。建议配置到开盘前。 |
| `TQSDK_RELAY_FUTURES_UNIVERSE_REFRESH_SECS` | 空 | 兼容入口。设置后使用固定秒数间隔刷新；不能和 `TQSDK_RELAY_FUTURES_UNIVERSE_REFRESH_AT` 同时设置。新部署优先使用每日固定时间。 |
| `TQSDK_RELAY_FUTURES_METADATA_BATCH_SIZE` | `500` | 产品发现时 `query_symbol_info` metadata 查询的批大小；必须大于 `0`。 |
| `TQSDK_RELAY_FUTURES_MAIN_ONLY` | `false` | 产品发现模式的快捷入口。设置为 `1` / `true` / `yes` / `on` 时只保留每品种主力合约；与 `TQSDK_RELAY_FUTURES_ACTIVE_CONTRACTS_PER_PRODUCT` 互斥。 |
| `TQSDK_RELAY_FUTURES_ACTIVE_CONTRACTS_PER_PRODUCT` | 空 | 产品发现模式的可选活跃度限制。设置为 `1` 表示每品种只保留主力，`2` 表示主力和次主力；其余合约按 `open_interest`、`volume` 排名补足前 N。必须大于 `0`。 |
| `TQSDK_RELAY_UPSTREAM_INS_LIST_WARN_CHARS` | `32000` | 单个上游 tick chart `ins_list` 字符串长度告警阈值。当前 relay 按一合约一 tick chart 发送，通常等于最长合约代码长度；超过后不阻止连接，但 `MetricsSnapshot.upstream_ins_list_over_warn` 会变为 `true`。 |
| `TQSDK_RELAY_UPSTREAM_INS_LIST_MAX_CHARS` | 空 | 单个上游 tick chart `ins_list` 字符串硬上限。设置后超过上限会在连接上游前返回配置错误；默认不启用硬失败。 |
| `TQSDK_RELAY_UPSTREAM_TICK_VIEW_WIDTH` | `10000` | 发给每个上游 tick chart 的 `view_width`。调小可减少启动 backfilling 历史窗口；必须大于 `0`。若希望近似只要最新 tick，可先设为 `1`。 |
| `TQSDK_RELAY_TICK_RING_CAPACITY` | `200000` | 每个合约保留的内存 tick ring 行数；必须大于 `0`。全品种持久运行建议调低到 `10000` / `20000` 级别。 |
| `TQSDK_RELAY_KLINE_RING_CAPACITY` | `10000` | relay 内部 K 线 ring 容量配置；必须大于 `0`。当前二进制主要用于保留配置边界，K 线合成热状态仍按订阅 source 保存当前 bar。 |
| `TQSDK_RELAY_FUTURES_SYMBOLS` | 空 | 兼容入口。逗号分隔完整期货合约列表；relay 会为每个合约创建一个上游 tick chart。与 `TQSDK_RELAY_FUTURES_PRODUCTS` / `TQSDK_RELAY_FUTURES_SYMBOLS_FILE` 互斥。 |
| `TQSDK_RELAY_FUTURES_SYMBOLS_FILE` | 空 | 兼容入口。完整合约文件路径；文件可用换行或逗号分隔。长期部署不推荐依赖静态文件。 |
| `TQSDK_RELAY_UPSTREAM_MARKET_URL` | `wss://openmd.shinnytech.com/t/md/front/mobile` | 上游天勤行情 websocket URL。 |
| `TQSDK_RELAY_DOWNSTREAM_LISTEN` | `127.0.0.1:7788` | 下游 SDK websocket 监听地址。 |
| `TQSDK_RELAY_METRICS_LISTEN` | `127.0.0.1:7789` | HTTP health / metrics 监听地址。 |

库用户也可以直接构造 `RelayConfig`，调整默认值：

- `tick_ring_capacity`：默认每个合约 `200_000` 行。
- `kline_ring_capacity`：默认 `10_000` 行。
- `futures_product_filter`：动态产品发现过滤器，可选全部期货或产品代码列表。
- `futures_universe_refresh`：默认每日本地 `08:30:00` 刷新，也可设置兼容 interval。
- `futures_metadata_batch_size`：默认 `500`，控制产品发现时 metadata 查询分批大小。
- `futures_active_contracts_per_product`：默认 `None`，设置后限制产品发现结果为每品种活跃度排名前 N。
- `upstream_tick_view_width`：默认 `10_000`，控制每个上游 tick chart 的 `view_width`。
- `upstream_ins_list_limits`：默认 warn threshold 为 `32_000` 字符，hard max 关闭；检查口径是单个上游 tick chart 的 `ins_list` 长度。
- `bootstrap.max_concurrent_remote_charts`：默认 `4`。
- `bootstrap.min_remote_request_interval`：默认 `250ms`。
- `bootstrap.per_series_cooldown`：默认 `30s`。

## 行情行为

### 上游订阅

对于动态发现或显式配置得到的期货合约集合，relay 会在同一个上游 websocket 上为每个
合约创建一个 duration 为 `0` 的 tick chart。多合约单 chart 属于对齐 K 线用法，不用于
relay 的上游 tick 源：

```json
{
  "aid": "set_chart",
  "chart_id": "relay-upstream-tick-DCE_m2609-10000",
  "ins_list": "DCE.m2609",
  "duration": 0,
  "view_width": 10000
}
```

每个 `set_chart` 后 relay 会发送一次 `{"aid":"peek_message"}`，与直连 `tick(symbol, width)`
路径保持一致。其中 `view_width` 来自 `TQSDK_RELAY_UPSTREAM_TICK_VIEW_WIDTH` /
`RelayConfig.upstream_tick_view_width`；`TQSDK_RELAY_TICK_RING_CAPACITY` 只影响 relay
本地保留多少 tick，不影响上游补历史窗口。

发送前 relay 会计算单个上游 tick chart 的最大 `ins_list` 长度。超过
`TQSDK_RELAY_UPSTREAM_INS_LIST_MAX_CHARS` 时不会连接上游；超过 warn threshold 时连接
仍会继续，但启动诊断和 metrics 会暴露当前最大长度和告警状态。

### 启动日志与 HTTP 观测

正常启动会向 stderr 输出一行 `relay_startup` JSON。静态完整合约入口会直接包含
`upstream_symbols` 和 `upstream_ins_list_chars`；产品发现入口在正常启动日志中先记录配置
视角，首次 metadata 刷新成功后的实际合约数和最大单 chart `ins_list` 长度以 `/metrics` 为准。需要在
真正启动服务前得到精确订阅规模时，使用 `TQSDK_RELAY_DRY_RUN=1`。

二进制程序会在 `TQSDK_RELAY_METRICS_LISTEN` 绑定一个极简 HTTP JSON 服务：

```bash
curl http://127.0.0.1:7789/health
curl http://127.0.0.1:7789/metrics
curl http://127.0.0.1:7789/symbol-metrics
open http://127.0.0.1:7789/dashboard
```

`/health` 示例：

```json
{
  "ready": true,
  "market_data_ready": false,
  "process_started": true,
  "downstream_listening": true,
  "upstream_status": "connecting",
  "upstream_stage": "connecting",
  "upstream_connected": false,
  "upstream_transport_connected": false,
  "upstream_subscription_sent": false,
  "universe_ready": false,
  "data_fresh": false,
  "downstream_clients": 0,
  "upstream_symbols": 0,
  "ticks_ingested": 0,
  "upstream_frames_received": 0,
  "upstream_events_decoded": 0,
  "upstream_invalid_tick_rows": 0,
  "lifetime_invalid_rows": 0,
  "recent_invalid_rows_1m": 0,
  "current_decode_health": "healthy",
  "last_upstream_invalid_tick_row_error": null,
  "last_invalid_row_unix_secs": null,
  "last_universe_refresh_unix_secs": null,
  "last_universe_refresh_error": null,
  "last_tick_unix_secs": null,
  "last_upstream_frame_unix_secs": null,
  "last_decoded_event_unix_secs": null,
  "upstream_frame_idle_ms": null,
  "upstream_frame_idle_health": "no_sample",
  "upstream_event_idle_ms": null,
  "upstream_event_idle_health": "no_sample",
  "data_stale_after_secs": 30
}
```

`/health` 返回分层 readiness JSON。`ready` 只表示 relay 进程和下游监听已经可用，保持
对早期监控的兼容；`market_data_ready` 表示上游已连通、合约集合已刷新成功，并且最近
行情更新活跃时间没有超过默认 `30s` freshness 窗口。关键字段包括：

- `process_started`：relay engine 已启动。
- `downstream_listening`：下游 market websocket 监听已可接入。
- `upstream_connected` / `upstream_status`：兼容字段，表示上游是否已经进入可用行情状态；只有收到有效 tick 或 quote 后才会变为 `up` / `true`。
- `upstream_stage`：更细的上游阶段，可能为 `connecting`、`subscribing`、`backfilling`、`live`、`degraded` 或 `down`。`backfilling` 表示订阅命令已发送，relay 正在等待 tick chart bootstrap / 历史补齐产出可用 tick 或 quote；首个上游 frame 到达前 `upstream_frames_received` 仍可能为 0。
- `upstream_stage_started_unix_secs`：当前上游阶段开始时间。dashboard 用它计算 backfilling 已持续多久；由于上游不暴露补历史总量，这不是百分比进度。
- `upstream_transport_connected`：上游 websocket transport 已建立。
- `upstream_subscription_sent`：relay 已向上游发送每合约 `set_chart` 和对应 `peek_message`。
- `upstream_frames_received` / `upstream_events_decoded`：已收到的上游 frame 数和解出的 tick / quote event 数，可用于区分“未建连”和“正在补历史但尚无可用行情”。
- `last_upstream_frame_unix_secs`：最近收到任意上游 frame 的 relay 本地 Unix 秒时间。
- `last_decoded_event_unix_secs`：最近解出有效 tick / quote event 的 relay 本地 Unix 秒时间。
- `upstream_frame_idle_ms` / `upstream_frame_idle_health`：最近上游 frame 静默时长和状态，阈值为 warning `2s`、critical `5s`。
- `upstream_event_idle_ms` / `upstream_event_idle_health`：最近有效 event 静默时长和状态，阈值为 warning `3s`、critical `8s`。
- `universe_ready`：合约集合刷新已成功，且最近一次刷新没有错误。
- `data_fresh`：最近一次 tick 或 quote 活跃时间未超过 freshness 窗口。
- `market_data_ready`：`upstream_connected && universe_ready && data_fresh`。
- `upstream_invalid_tick_rows` / `lifetime_invalid_rows`：已跳过的上游坏 tick row 生命周期累计。
- `recent_invalid_rows_1m` / `current_decode_health`：最近 1 分钟坏行数和可恢复的当前解码健康状态。
- `last_upstream_invalid_tick_row_error` / `last_invalid_row_unix_secs`：最近一条解码错误和时间。

`/metrics` 返回 `RelayEngine::metrics_snapshot()` 的完整 JSON。

`/symbol-metrics` 返回合约级 telemetry 快照，当前健康集合固定为“当前上游 universe ∪
当前下游订阅”。已经退出 universe 且当前未被订阅的历史 telemetry 不再进入当前健康；
仍被下游订阅的旧合约会以 `coverage=uncovered` 保留为覆盖问题。

状态主口径是 relay 接收间隔延迟，并优先使用 `query_symbol_info` 返回的官方
`trading_time`；若未配置产品发现或交易时段暂不可用，再考虑 quote 中的交易时间表，
最后按期货交易所 / 品种代码使用内置交易时段兜底。交易时段按固定 Asia/Shanghai
时区解释，不受 host 本地时区影响。无接收样本时 `session=unknown`、`flow=no_sample`，
不会把未知样本误判为休盘。响应保留兼容 `status`，同时返回正交状态字段：
`coverage=covered|uncovered`、`session=open|closed|unknown`、
`flow=flowing|silent|no_sample`、`integrity=intact|suspected|confirmed_gap`。

每个合约还会返回 `problem` 和 `problem_severity`，作为 dashboard 关注列表、风险排序、
数据解码告警和完整性异常计数的统一口径；响应同时包含 `market_time_lag_ms`，用于辅助
判断行情时间与本地时间的差距；`ticks_ingested` 仍只统计 tick row，用于区分 quote-only
远月合约。tick row 还会按当前 source epoch 检查行号连续性，并暴露 `source_epoch`、
`last_tick_id`、`gap_event_count`、`estimated_missing_rows`、`duplicate_rows`、
`out_of_order_rows` 和 `last_gap_unix_millis`。这些是原始 TQ DIFF row-id 诊断字段：
DIFF 可以后续 patch / refill 稀疏 row，因此跳号、重复或倒序不单独证明市场数据缺失，
也不进入 `problem`、`problem_severity` 或 `integrity=confirmed_gap`。上游重新发送 tick
chart 订阅时会推进 source epoch，避免重连后首条 row id 跳变污染诊断计数。

`/dashboard-snapshot` 返回 dashboard 使用的原子 JSON 快照：同一次响应内包含
`metrics`、未过滤的 `global` 汇总、未过滤的 `global_symbols` 事件/时间带输入，以及
按当前筛选、排序、分页裁剪后的 `page` 列表，以及进程内固定容量 `events` 事件账本。
事件账本只保存在内存，当前记录 universe refresh 成功/失败、上游 flow incident 和
decode incident。`/symbol-metrics` 继续作为合约列表调试端点；它的 `summary` 仍是过滤
前的全局汇总，`symbols` 只代表当前查询页。

`/dashboard` 是内置只读运维页面，每 `2s` 串行轮询 `/dashboard-snapshot`。它不连接 relay
market websocket，不创建下游订阅，也不会触发额外行情命令。页面由
`crates/tqsdk-relay/dashboard-ui/` 的 Svelte 5 + Vite + Tailwind CSS 4 工程构建，
生产产物提交在 `crates/tqsdk-relay/src/dashboard-dist/`，Rust 侧将该目录嵌入到
relay 二进制并服务 `/dashboard/` 与 `/dashboard/assets/*`。页面顶部会展示上游阶段、
transport 连接、订阅发送、frame 接收数、解码事件数、backfilling 已持续时间、frame
速率、最近 frame/event idle、decode health 和最近 frame 时间；tick / quote ingest 热路径
只更新当前合约的轻量 telemetry。HTTP snapshot 路径只在 `RelayEngine` mutex 内复制
metrics、symbol read model、订阅快照和事件账本，随后在锁外完成合约分类、汇总、过滤、
排序、裁剪和 JSON 序列化。dashboard 的全局健康、覆盖率、评分、时间带和事件账本使用
未过滤 global 数据，搜索/状态筛选只影响可见列表，不会把异常过滤成健康。dashboard 会把
tick row-id 跳号、重复和倒序显示为中性 DIFF 诊断，不作为确认的行情完整性异常、事件账本
告警或评分扣分；当前健康判断仍以接收间隔、上游阶段、订阅影响和解码健康为主。页面不展示静态假 sparkline；全屏按钮调用浏览器 fullscreen API，不支持时
禁用；完整合约表展示当前过滤页内全部行。backfilling 进度只基于 relay 已观测到的时间
和 frame/event 计数，不推断上游补历史百分比。

### 观测字段

`RelayEngine::metrics_snapshot()` 和 `/metrics` 会返回当前下游客户端、quote/chart
订阅、tick 摄入和 bootstrap 队列指标，也会返回上游订阅规模：

- `upstream_stage`：当前上游阶段；比 `upstream_status` 更适合排查启动期是否停在连接、订阅发送或补历史阶段。
- `upstream_stage_started_unix_secs`：当前阶段开始时间，主要用于 dashboard 计算 backfilling 已持续时间。
- `upstream_transport_connected` / `upstream_subscription_sent`：上游 websocket 建连和订阅命令发送进度。
- `upstream_frames_received` / `upstream_events_decoded`：上游 frame 与有效 tick / quote event 计数。
- `last_upstream_frame_unix_secs`：最近收到任意上游 frame 的本地 Unix 秒时间。
- `last_decoded_event_unix_secs`：最近解出有效 tick / quote event 的本地 Unix 秒时间。
- `upstream_frame_idle_ms` / `upstream_frame_idle_health`：frame 静默状态，阈值为 warning `2s`、critical `5s`。
- `upstream_event_idle_ms` / `upstream_event_idle_health`：有效 event 静默状态，阈值为 warning `3s`、critical `8s`。
- `upstream_symbols`：当前上游 tick chart 合约数。
- `upstream_ins_list_chars`：当前单个上游 tick chart 的最大 `ins_list` 字符串长度。
- `upstream_ins_list_warn_chars` / `upstream_ins_list_max_chars`：配置阈值。
- `upstream_ins_list_over_warn`：当前长度是否超过 warn threshold。
- `upstream_invalid_tick_rows` / `lifetime_invalid_rows`：上游 tick row 解码失败后被跳过的生命周期累计；有效 tick 仍会继续摄入。
- `recent_invalid_rows_1m` / `current_decode_health`：最近窗口坏行数和当前 decode health；历史坏行不会永久锁死当前健康。
- `last_upstream_invalid_tick_row_error` / `last_invalid_row_unix_secs`：最近一条解码错误和时间。
- `last_universe_refresh_unix_secs` / `last_universe_refresh_error`：最近一次合约集合刷新尝试的时间和错误。
- `last_tick_unix_secs`：最近一次摄入 tick 或 quote 的 relay 本地 Unix 秒时间。

### 下游命令子集

| 命令 | relay 行为 |
| --- | --- |
| `subscribe_quote` | 为客户端注册 quote 订阅，并发送由最新 tick 派生的 quote update。 |
| 正 `duration` 的 `set_chart` | 注册 K 线 chart 订阅，记录 bootstrap 请求；如果 tick ring 已有可完成窗口，会先回放冷启动 K 线，随后在 tick 跨入后续窗口时发送新完成的合成 K 线。 |
| `duration <= 0` 的 `set_chart` | 会解析和注册，但 duration 为 `0` 的实时 tick chart 分发尚未在 V1 服务面完成。 |
| `peek_message` | 作为兼容命令接受，不执行额外动作。 |

### K 线合成

合成固定周期 K 线使用 `[start, end)` 窗口。时间戳等于 end 边界的 tick 归属到
下一根 K 线。

relay 只有在后续窗口的 tick 到来后，才会发送上一根已完成 K 线。它不会为没有
tick 的窗口创建空 K 线，也不会使用本地墙钟强行收 K 线。

新客户端订阅正周期 K 线时，relay 会用该合约的内存 tick ring 临时重建合成器，并把
已经完整闭合的 K 线先发给该客户端，同时更新对应 chart 的 `right_id`。这个冷启动回放
只来自当前进程内存，不跨 relay 重启；当前实现聚焦单合约 chart。

## 运维注意事项

- 将下游监听保持在 loopback、私有网络或你自己的访问控制之后。relay 不认证下游
  客户端。
- 大规模合约集合推荐使用 `TQSDK_RELAY_FUTURES_PRODUCTS=ALL`，让 relay 在启动和
  每日本地固定刷新时间动态查询当前活跃合约。
- relay 的目标是通过共享一个上游 websocket 和本地 tick/quote/K 线缓存降低多进程订阅压力；不要把它当成通用
  天勤代理。
- 如果 metrics 显示 `upstream_ins_list_over_warn=true`，说明单个上游 tick chart 的
  `ins_list` 长度已经接近你设定的风险区间；可以设置 hard max 让进程 fail fast。
- 上游连接失败会将 source 标记为 degraded 并重试。仅因上游临时不可用，已有下游
  连接不会被主动断开。
- 产品发现模式依赖 `TQ_AUTH_USER` / `TQ_AUTH_PASS` 做 relay 内部 metadata 查询；这些
  凭证不会下发给下游 SDK 客户端。
- 当前缓存是内存态。重启 relay 会丢失 tick、quote 和 K 线物化状态。

## 开发

开发 relay crate 时常用的检查命令：

```bash
cargo test -p tqsdk-relay --tests
cargo clippy -p tqsdk-relay --all-targets -- -D warnings
cargo check -p tqsdk-relay --no-default-features
```

websocket 测试使用 loopback 测试服务；不需要天勤凭证或实时行情访问。

开发 dashboard UI：

```bash
cd crates/tqsdk-relay/dashboard-ui
pnpm install
pnpm run dev
```

开发服务器会把 `/dashboard-snapshot`、`/metrics` 和 `/symbol-metrics` 代理到
`127.0.0.1:7789`。提交前需要刷新
嵌入式静态产物并运行 UI / Rust 双侧检查：

```bash
cd crates/tqsdk-relay/dashboard-ui
pnpm run test
pnpm run check
pnpm run build
pnpm run test:e2e
cd ../../..
cargo test -p tqsdk-relay --test binary_smoke relay_binary_serves_embedded_dashboard_assets
```
