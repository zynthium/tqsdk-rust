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
| 合约集合刷新 | 支持按全部期货品种或产品代码列表生成当前活跃合约集合；产品发现模式下会按刷新间隔重建上游 tick chart。 |
| quote 分发 | 将最新 tick 投影成 quote frame，并发送给已订阅的下游客户端。 |
| 固定周期 K 线合成 | 从上游 tick 合成正周期 K 线，并向图表订阅者发送已完成的 K 线。 |
| 缓存 | 保留内存 tick ring 和 quote 快照。当前二进制程序尚未启用磁盘持久化。 |
| bootstrap 队列 | 在 relay 内部合并并限流 chart bootstrap 请求。远端 K 线回填和 oracle 对比尚未实现。 |
| 上游恢复 | 下游监听保持运行；上游 websocket 连接失败后会重试。 |
| 观测结构 | 库 API 暴露 health、metrics 和 source status 快照。`metrics_listen` 地址当前仅预留，尚未提供 HTTP metrics 端点。 |

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
启动时通过天勤 metadata 查询当前未过期合约，再把结果组成一个上游 tick chart。

默认情况下，进程会在 `127.0.0.1:7788` 监听 SDK 行情 websocket 客户端，并连接
上游 `wss://openmd.shinnytech.com/t/md/front/mobile`。

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
| `TQSDK_RELAY_FUTURES_UNIVERSE_REFRESH_SECS` | `86400` | 产品发现模式下重建上游合约集合的间隔秒数。默认每日一次；生产部署建议在开盘前启动 relay，或用外部 supervisor 将进程启动/重启时间对齐到开盘前刷新窗口。 |
| `TQSDK_RELAY_FUTURES_SYMBOLS` | 空 | 兼容入口。单个上游 tick chart 使用的逗号分隔完整期货合约列表。与 `TQSDK_RELAY_FUTURES_PRODUCTS` / `TQSDK_RELAY_FUTURES_SYMBOLS_FILE` 互斥。 |
| `TQSDK_RELAY_FUTURES_SYMBOLS_FILE` | 空 | 兼容入口。完整合约文件路径；文件可用换行或逗号分隔。长期部署不推荐依赖静态文件。 |
| `TQSDK_RELAY_UPSTREAM_MARKET_URL` | `wss://openmd.shinnytech.com/t/md/front/mobile` | 上游天勤行情 websocket URL。 |
| `TQSDK_RELAY_DOWNSTREAM_LISTEN` | `127.0.0.1:7788` | 下游 SDK websocket 监听地址。 |
| `TQSDK_RELAY_METRICS_LISTEN` | `127.0.0.1:7789` | 预留 metrics 监听地址。当前二进制程序会打印该地址，但不会绑定 HTTP metrics 服务。 |

库用户可以直接构造 `RelayConfig`，调整当前尚未通过环境变量暴露的默认值：

- `tick_ring_capacity`：默认每个合约 `200_000` 行。
- `kline_ring_capacity`：默认 `10_000` 行。
- `futures_product_filter`：动态产品发现过滤器，可选全部期货或产品代码列表。
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

### 下游命令子集

| 命令 | relay 行为 |
| --- | --- |
| `subscribe_quote` | 为客户端注册 quote 订阅，并发送由最新 tick 派生的 quote update。 |
| 正 `duration` 的 `set_chart` | 注册 K 线 chart 订阅，记录 bootstrap 请求，并在 tick 跨入后续窗口时发送已完成的合成 K 线。 |
| `duration <= 0` 的 `set_chart` | 会解析和注册，但 duration 为 `0` 的实时 tick chart 分发尚未在 V1 服务面完成。 |
| `peek_message` | 作为兼容命令接受，不执行额外动作。 |

### K 线合成

合成固定周期 K 线使用 `[start, end)` 窗口。时间戳等于 end 边界的 tick 归属到
下一根 K 线。

relay 只有在后续窗口的 tick 到来后，才会发送上一根已完成 K 线。它不会为没有
tick 的窗口创建空 K 线，也不会使用本地墙钟强行收 K 线。

## 运维注意事项

- 将下游监听保持在 loopback、私有网络或你自己的访问控制之后。relay 不认证下游
  客户端。
- 大规模合约集合推荐使用 `TQSDK_RELAY_FUTURES_PRODUCTS=ALL`，让 relay 在启动和
  刷新时动态查询当前活跃合约。
- relay 的目标是通过共享一个上游 tick chart 降低订阅字符串膨胀；不要把它当成通用
  天勤代理。
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
