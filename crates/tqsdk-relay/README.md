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
| 上游数据源 | 动态发现当前活跃期货合约，打开一个天勤行情 websocket，并发送 duration 为 `0` 的 `set_chart`。 |
| 合约集合刷新 | 支持按全部期货品种或产品代码列表生成当前活跃合约集合；产品发现模式下按本地每日固定时间重建上游 tick chart，默认 `08:30:00`。 |
| 订阅长度防线 | 在连接上游前统计 `ins_list` 长度；超过 hard limit 会拒绝订阅，超过 warn threshold 会体现在 metrics 中。 |
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

只订阅指定产品代码时：

```bash
export TQ_AUTH_USER="your-account"
export TQ_AUTH_PASS="your-password"
TQSDK_RELAY_FUTURES_PRODUCTS="SHFE.au,DCE.m,CZCE.MA" \
cargo run -p tqsdk-relay
```

`SHFE.au` 表示交易所限定的产品代码；`MA` 表示不限定交易所的产品代码。relay 会在
启动时通过天勤 metadata 查询当前未过期合约，再用 `query_symbol_info` 返回的
`exchange_id`、`product_id` 和 `expired` 等 typed metadata 过滤，最后把结果组成一个
上游 tick chart。

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
{"event":"relay_startup","dry_run":true,"upstream_source":"static-symbols","downstream_listen":"127.0.0.1:7788","metrics_listen":"127.0.0.1:7789","refresh_schedule":"daily:08:30:00","futures_metadata_batch_size":500,"upstream_symbols":2,"upstream_ins_list_chars":21,"upstream_ins_list_warn_chars":32000,"upstream_ins_list_max_chars":null,"upstream_ins_list_over_warn":false,"upstream_ins_list_over_max":false,"suggested_relay_instances":null}
```

如果 dry-run 使用 `TQSDK_RELAY_FUTURES_PRODUCTS`，它会执行一次 metadata 查询来得到当前
活跃合约集合；它仍不会绑定监听地址，也不会连接上游行情 websocket。

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
| `TQSDK_RELAY_FUTURES_METADATA_BATCH_SIZE` | `500` | 产品发现时 `query_symbol_info` 的批大小；必须大于 `0`。 |
| `TQSDK_RELAY_UPSTREAM_INS_LIST_WARN_CHARS` | `32000` | 上游 tick chart `ins_list` 字符串长度告警阈值。超过后不阻止连接，但 `MetricsSnapshot.upstream_ins_list_over_warn` 会变为 `true`。 |
| `TQSDK_RELAY_UPSTREAM_INS_LIST_MAX_CHARS` | 空 | 上游 tick chart `ins_list` 字符串硬上限。设置后超过上限会在连接上游前返回配置错误，并提示至少需要拆成几个 relay 实例；默认不启用硬失败。 |
| `TQSDK_RELAY_FUTURES_SYMBOLS` | 空 | 兼容入口。单个上游 tick chart 使用的逗号分隔完整期货合约列表。与 `TQSDK_RELAY_FUTURES_PRODUCTS` / `TQSDK_RELAY_FUTURES_SYMBOLS_FILE` 互斥。 |
| `TQSDK_RELAY_FUTURES_SYMBOLS_FILE` | 空 | 兼容入口。完整合约文件路径；文件可用换行或逗号分隔。长期部署不推荐依赖静态文件。 |
| `TQSDK_RELAY_UPSTREAM_MARKET_URL` | `wss://openmd.shinnytech.com/t/md/front/mobile` | 上游天勤行情 websocket URL。 |
| `TQSDK_RELAY_DOWNSTREAM_LISTEN` | `127.0.0.1:7788` | 下游 SDK websocket 监听地址。 |
| `TQSDK_RELAY_METRICS_LISTEN` | `127.0.0.1:7789` | HTTP health / metrics 监听地址。 |

库用户可以直接构造 `RelayConfig`，调整当前尚未通过环境变量暴露的默认值：

- `tick_ring_capacity`：默认每个合约 `200_000` 行。
- `kline_ring_capacity`：默认 `10_000` 行。
- `futures_product_filter`：动态产品发现过滤器，可选全部期货或产品代码列表。
- `futures_universe_refresh`：默认每日本地 `08:30:00` 刷新，也可设置兼容 interval。
- `futures_metadata_batch_size`：默认 `500`，控制产品发现时 metadata 查询分批大小。
- `upstream_ins_list_limits`：默认 warn threshold 为 `32_000` 字符，hard max 关闭。
- `bootstrap.max_concurrent_remote_charts`：默认 `4`。
- `bootstrap.min_remote_request_interval`：默认 `250ms`。
- `bootstrap.per_series_cooldown`：默认 `30s`。

## 行情行为

### 上游订阅

对于动态发现或显式配置得到的期货合约集合，relay 会创建一个上游 chart：

```json
{
  "aid": "set_chart",
  "chart_id": "relay-upstream-all-futures-ticks",
  "ins_list": "DCE.m2609,SHFE.au2602",
  "duration": 0,
  "view_width": 10000
}
```

随后它会发送 `{"aid":"peek_message"}`，并从上游 websocket 解码 `rtn_data` tick
片段。

发送前 relay 会计算最终 `ins_list` 长度。超过 `TQSDK_RELAY_UPSTREAM_INS_LIST_MAX_CHARS`
时不会连接上游，并在错误里提示按当前长度和阈值至少需要拆成几个 relay 实例；超过
warn threshold 时连接仍会继续，但启动诊断和 metrics 会暴露当前长度、告警状态和
建议拆分数量，便于你判断是否需要拆分 relay 实例或收窄产品列表。

### 启动日志与 HTTP 观测

正常启动会向 stderr 输出一行 `relay_startup` JSON。静态完整合约入口会直接包含
`upstream_symbols` 和 `upstream_ins_list_chars`；产品发现入口在正常启动日志中先记录配置
视角，首次 metadata 刷新成功后的实际合约数和 `ins_list` 长度以 `/metrics` 为准。需要在
真正启动服务前得到精确订阅规模时，使用 `TQSDK_RELAY_DRY_RUN=1`。

二进制程序会在 `TQSDK_RELAY_METRICS_LISTEN` 绑定一个极简 HTTP JSON 服务：

```bash
curl http://127.0.0.1:7789/health
curl http://127.0.0.1:7789/metrics
```

`/health` 返回分层 readiness JSON。`ready` 只表示 relay 进程和下游监听已经可用，保持
对早期监控的兼容；`market_data_ready` 表示上游已连通、合约集合已刷新成功，并且最近
tick 活跃时间没有超过默认 `30s` freshness 窗口。关键字段包括：

- `process_started`：relay engine 已启动。
- `downstream_listening`：下游 market websocket 监听已可接入。
- `upstream_connected` / `upstream_status`：上游行情连接是否已进入可用状态。
- `universe_ready`：合约集合刷新已成功，且最近一次刷新没有错误。
- `data_fresh`：最近一次 tick 活跃时间未超过 freshness 窗口。
- `market_data_ready`：`upstream_connected && universe_ready && data_fresh`。

`/metrics` 返回 `RelayEngine::metrics_snapshot()` 的完整 JSON。

### 观测字段

`RelayEngine::metrics_snapshot()` 和 `/metrics` 会返回当前下游客户端、quote/chart
订阅、tick 摄入和 bootstrap 队列指标，也会返回上游订阅规模：

- `upstream_symbols`：当前上游 tick chart 合约数。
- `upstream_ins_list_chars`：当前上游 `ins_list` 字符串长度。
- `upstream_ins_list_warn_chars` / `upstream_ins_list_max_chars`：配置阈值。
- `upstream_ins_list_over_warn`：当前长度是否超过 warn threshold。
- `last_universe_refresh_unix_secs` / `last_universe_refresh_error`：最近一次合约集合刷新尝试的时间和错误。
- `last_tick_unix_secs`：最近一次摄入 tick 的 relay 本地 Unix 秒时间。

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
- relay 的目标是通过共享一个上游 tick chart 降低订阅字符串膨胀；不要把它当成通用
  天勤代理。
- 如果 metrics 显示 `upstream_ins_list_over_warn=true`，说明当前订阅字符串已经接近你设定的
  风险区间；可以设置 hard max 让进程 fail fast，并按错误或启动诊断里的
  `suggested_relay_instances` 拆分多个 relay。
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
