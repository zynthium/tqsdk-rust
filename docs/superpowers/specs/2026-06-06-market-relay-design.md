# Market Relay Service Design

## 背景与结论

本轮迭代的真实目标不是在 SDK 内部增加一种新的 K 线来源模式，而是降低对天勤服务器的
行情订阅压力，尤其是“全期货品种 * 多周期 K 线”带来的订阅字符串长度和订阅合约数量
限制风险。

已确认的实测前提是：单个天勤连接可以承载订阅所有期货合约的 tick 数据，但无法承载
全品种多周期远端 K 线。因此当前主线是可选的行情中继和缓存服务：

- 默认 `tqsdk-rust` 行为不变，未配置 relay 时继续直连天勤。
- relay 是显式 opt-in 的部署组件，不通过环境变量隐式接管 SDK 流量。
- relay 只代理 market route，不代理 trade、query、auth 或账号交易状态。
- relay 内部用单个上游期货 tick 源维护 tick cache、quote cache 和多周期 K 线合成，
  再向多个下游 SDK 客户端 fan-out。

## 为什么采用 Relay

本轮目标是降低多进程、多策略、多周期订阅对天勤服务器的集中压力。这个目标需要共享
行情输入和跨客户端去重，不能只在单个 SDK session 内做本地处理。

采用 relay 有三点直接收益：

1. 多个 SDK 进程可以共享同一个上游 tick 输入，订阅压力集中去重。
2. 远端 K 线 oracle 对齐可以放在 relay 内部限流执行，不暴露给下游运行时选择。
3. bootstrap / resync 可以通过队列合并和限流，避免启动阶段把全品种多周期远端 K 线
   一次性展开。

relay-first 的设计把问题移到一个共享 market service 内部处理：SDK 客户端只连接 relay，
relay 只用一个主上游 tick 源承接全期货行情，并对多客户端、多周期请求做本地缓存、
合成和去重。

## 目标

1. 新增可选的 `tqsdk-relay` market relay 服务，作为 workspace 内独立 crate / binary
   交付，但不成为 SDK 默认依赖路径。
2. 保持未使用 relay 时的直连天勤行为完全不变。
3. relay 下游协议优先兼容 SDK 已使用的 market DIFF 帧，让现有 wait / stream /
   session 消费路径继续走同一套 runtime commit。
4. relay 上游第一版只支持期货行情，使用单个主上游 tick chart / tick source 订阅全期货
   tick。
5. relay 内部维护 tick cache、quote cache 和 K 线合成状态，并支持多个下游客户端共享。
6. K 线 bootstrap 优先使用本地 tick/cache；只有缓存不足、row id 未知或需要远端对齐时，
   才进入限流队列发起远端 K 线 bootstrap / resync。
7. 远端 K 线 bootstrap / resync 必须可合并、限流、使用后取消，不能按下游全量订阅一次性
   展开。
8. relay 提供基本运行观测：上游连接状态、下游客户端数、订阅数、cache 命中、bootstrap
   队列、resync 状态、best-effort duration 和错误统计。

## 非目标

- 不把 trade、query、auth、schema、metadata 或 direct query 代理进 relay。
- 不改变 `tqsdk-core` 的 runtime contract、状态树、commit/revision/cursor 语义。
- 不在 `tqsdk-wait` 或 `tqsdk-stream` 内新增第二棵状态树或 facade 私有行情缓存。
- 不把 `tqsdk-data` 的 Python-compatible mmap history cache 改造成 live cache。
- 第一版不做上游 shard pool；如果未来其他市场或天勤限制需要分片，再作为独立迭代设计。
- 第一版不做 app-level auth；部署边界依赖内网、VPN、防火墙或上层反向代理。
- 第一版不承诺多 provider 行情聚合；relay 是单天勤上游的 market fan-out/cache 服务。

## 总体架构

`tqsdk-relay` 是独立进程，逻辑上位于 SDK 客户端和天勤 market server 之间：

```text
tqsdk-wait / tqsdk-stream / tqsdk-session
        |
        | market websocket only
        v
tqsdk-relay
        |
        | upstream market websocket
        v
TQ market server
```

SDK 侧只需要显式把 market endpoint 指向 relay。第一版优先复用现有 market URL 配置能力；
如果现有入口不足，再补一个很薄的 `market_relay(url)` builder helper。该 helper 只能是
endpoint 配置糖，不引入 SDK 内部行情来源策略，也不影响未配置 relay 的直连路径。

relay 内部由以下组件组成：

| 组件 | 职责 |
| --- | --- |
| Downstream market server | 接收 SDK market websocket 连接，解析 `subscribe_quote`、`set_chart`、`peek_message` 等 market 命令，返回兼容 `rtn_data` / DIFF 的帧 |
| Upstream market client | 使用单个天勤行情账号连接上游 market server，维护主期货 tick source |
| Instrument universe loader | 获取或配置期货合约全集，生成上游 tick 订阅集合 |
| Interest registry | 记录下游客户端的 quote / tick / kline interest，做引用计数和 fan-out 路由 |
| Chart id mapper | 为每个下游客户端隔离 chart id，把下游 chart id 映射到 relay 内部共享 source，再把响应改写回客户端 chart id |
| Tick store | 保存最近 tick，内存 ring buffer 为主，可配置磁盘缓存 |
| Quote projector | 从 tick 和必要字段维护 quote snapshot，并向 `subscribe_quote` interest 推送更新 |
| Kline synthesizer | 从 tick store / live tick 合成多周期 K 线，并维护每个 series 的可变尾 bar 和已完成 bars |
| Bootstrap / resync queue | 对远端 K 线 bootstrap、oracle 对齐和缺口修复做合并、排队、限流和使用后取消 |
| Observability endpoint | 提供 `/healthz`、`/metrics`、`/sources` 等运行状态接口 |

## 下游协议范围

relay 第一版只实现 SDK 当前 live market 消费必需的 market 子集：

- `subscribe_quote`
- `set_chart`
- `peek_message`
- market DIFF / `rtn_data` 返回

trade、query、auth 和 schema/metadata route 不经过 relay。调用方如果需要这些能力，仍然
直连天勤对应 endpoint，或沿用现有 SDK route 配置。

未知 market 命令第一版返回明确错误并计入指标，不做静默透传。这样可以避免误以为 relay
已经完整代理天勤所有 market 语义。

## 上游订阅策略

上游第一版采用单连接、单主 tick source：

1. relay 启动后连接天勤 market server。
2. instrument universe loader 得到期货合约全集。
3. upstream market client 建立全期货 tick source。用户已验证该订阅规模可由单连接承载。
4. relay 不把下游多周期 K 线订阅展开成上游全量远端 K 线订阅。
5. 远端 K 线只作为 bootstrap / resync / oracle 对齐的临时资源，进入有上限的队列。

这个策略的核心约束是：主上游只长期持有 tick，不长期持有全品种多周期 K 线。

## K 线合成语义

relay 合成 K 线时使用交易所时间或天勤 tick payload 中可确认的行情时间作为 bar 归属依据。
固定周期 K 线窗口采用前闭后开语义：`[start, end)`。

这意味着：

- tick 时间 `t == start` 属于当前 bar。
- tick 时间 `t == end` 属于下一根 bar。
- 当前 bar 在第一笔落入下一窗口的 tick 到达时变为 completed。
- 最新一根 bar 是可变尾 bar；下游仍可用现有 `last_completed()` /
  `completed_rows()` 心智跳过它。

relay 不用本地墙钟强行补空 bar。没有 tick 的窗口是否输出空 bar，必须以远端 oracle
bootstrap/resync 或已验证的官方语义为准；否则该 duration 标记为 best-effort。

### 支持的 duration

第一版把 duration 分成三类：

| 类别 | 处理方式 |
| --- | --- |
| `duration=0` tick | 直接来自 tick store / live tick fan-out |
| 固定时长日内 K 线 | 从 tick 精确合成，默认覆盖 1s、5s、10s、15s、30s、1m、2m、3m、5m、10m、15m、30m、1h、2h、4h |
| 其他正 duration | 尽力按固定窗口合成，并默认打 diagnostic tag；关闭 tag 必须显式配置 |

交易日、周线、月线这类依赖交易日历或非固定自然周期的 K 线，不在第一版 exact synthesis
承诺内。它们可以通过远端 bootstrap/resync 提供结果，或在配置了明确 calendar alignment
规则后进入后续 exact synthesis 范围。

## Bootstrap 与 Resync

下游请求某个 kline series 时，relay 按下面顺序处理：

1. 查内存 tick / K 线 ring buffer。
2. 如果启用了磁盘缓存，查磁盘缓存。
3. 如果本地数据足够覆盖请求窗口，直接返回本地合成 / 缓存 rows。
4. 如果 row id、左边界、右边界或远端对齐状态不足，把请求合并到
   `symbol + duration + range` 级别的 bootstrap / resync queue。
5. queue 按全局并发上限和速率上限临时订阅远端 K 线。
6. 远端 rows 进入 cache，并与本地 tick-derived completed bars 做对齐检查。
7. 对齐或回填完成后，取消临时远端 K 线订阅。

这个流程保证下游即使订阅“全品种多周期”，也不会在 bootstrap 阶段把所有远端 K 线一次性
展开成上游订阅字符串。队列的并发上限是 relay 的硬保护，不由下游客户端数量放大。

## Oracle 对齐

运行时下游客户端只会选择连接直连天勤或 relay 其中之一，但 relay 内部可以同时持有
tick-derived 本地结果和临时远端 oracle K 线。

因此一致性检查放在 relay 内部：

- 对 completed bars 做抽样或按需比对。
- 只比较已经完成且不再变化的 bar。
- 差异按 `symbol + duration + bar_id/time` 记录指标和结构化日志。
- 差异达到阈值时，把对应 source 标记为 degraded，并触发 resync。
- relay 不把 oracle 订阅长期展开；验证任务仍然走限流队列。

这让一致性检查成为 relay 的内部运维能力，而不是下游 SDK 客户端的运行时选择问题。

## Chart ID 与多客户端隔离

下游客户端的 chart id 必须保持私有：

- 客户端 A 的 `chart_id=foo` 与客户端 B 的 `chart_id=foo` 不冲突。
- relay 内部用 `client_id + downstream_chart_id` 做映射 key。
- 多个客户端请求相同 `symbol + duration + view_width` 时，共享内部 source。
- 返回给客户端的 payload 必须改写回原始 downstream chart id。
- 某个客户端取消或断开时，只释放该客户端 interest；内部 source 只有在没有任何下游
  interest 后才释放。

## Cache 策略

第一版 cache 分两层：

1. 内存 ring buffer：默认开启，服务最近 tick、quote 和 K 线窗口。
2. 磁盘 cache：可选开启，用于跨 relay 重启保留最近 tick / K 线 materialization。

磁盘 cache 的目标是减少 bootstrap 远端请求，不是替代 `tqsdk-data` 的历史下载和研究缓存。
它只服务 relay 的 live market fan-out；文件格式、保留周期和容量上限由 relay 配置控制。

缓存记录至少保留：

- symbol
- source timestamp
- relay receive timestamp
- tick / kline payload
- duration
- source quality tag

## 错误与降级

| 场景 | 行为 |
| --- | --- |
| 上游 tick 连接断开 | relay 标记 source degraded，保留已有缓存，尝试重连；下游连接不立即断开 |
| tick 缺口可确认 | 暂停 exact synthesis，触发 resync；完成前相关 duration 标记 degraded |
| bootstrap 队列积压 | 新请求排队；下游只收到已可用的兼容 DIFF，pending 只作为 relay 内部状态和指标暴露 |
| 远端 oracle 与本地 completed bar 不一致 | 记录差异，触发 resync，必要时标记 source degraded |
| 下游慢消费者 | relay 对单客户端做缓冲上限和断开保护，不影响其他客户端 |
| 未支持 market 命令 | 返回明确错误，不静默透传 |

relay 不自动切换 SDK 到直连天勤。是否绕过 relay 由部署和客户端配置决定。

## 配置

第一版 relay 配置应覆盖：

- 上游天勤 market endpoint 和行情账号凭证。
- 是否启用 futures universe 自动加载，以及合约全集刷新周期。
- 下游 listen address。
- tick / K 线内存 ring buffer 容量。
- 是否启用磁盘 cache、cache 目录、容量上限和保留周期。
- bootstrap / resync 最大并发、每秒请求上限和单 series 冷却时间。
- best-effort duration diagnostic tag 是否开启。
- metrics / health endpoint listen address。

SDK 客户端配置只需要显式 market endpoint 指向 relay。未配置时走原样直连天勤。

## 运行观测

relay 至少提供以下观测面：

- `/healthz`：进程存活、上游连接、主 tick source 状态。
- `/metrics`：Prometheus 风格指标，覆盖连接数、订阅数、cache 命中率、fan-out 延迟、
  bootstrap 队列、resync 次数、oracle mismatch、下游慢消费者断开。
- `/sources`：结构化返回当前上游 source、合约全集版本、活跃内部 series、degraded source
  和 best-effort duration。

日志必须避免输出账号密码、token 和完整敏感配置。

## 安全边界

第一版不内置下游 app-level auth。推荐部署方式是：

- relay 只监听内网地址。
- 通过 VPN、防火墙或 sidecar proxy 控制访问。
- 上游天勤账号凭证只存在 relay 进程配置中，不下发给下游客户端。
- 下游客户端只获得 market 数据，不获得 trade/query/auth 代理能力。

## 验收方向

后续 implementation plan 应至少覆盖这些可验证行为：

1. 未配置 relay 时，现有 SDK direct-to-TQ 路径无行为变化。
2. SDK market endpoint 指向 relay 后，`subscribe_quote`、tick chart、kline chart 能走
   relay 下游协议并进入现有 runtime commit。
3. 单个主上游 tick source 可服务多个下游客户端的 quote / tick / kline 请求。
4. 多客户端相同 chart id 不冲突，返回 payload 改写回各自 chart id。
5. 下游全品种多周期请求不会导致 relay 同时订阅全量远端 K 线。
6. bootstrap / resync queue 的并发上限能被测试证明。
7. K 线窗口边界为 `[start, end)`，边界 tick 进入后一根 bar。
8. completed bar 的本地合成结果可以与远端 oracle completed bar 做抽样比对。
9. tick gap、oracle mismatch、上游断线和慢消费者都有结构化指标。
10. relay 可选磁盘 cache 关闭时仍能仅以内存模式运行。

## 设计取舍

这个设计承认 relay 是用户层基础设施，但仍把它放进 workspace，是为了让协议兼容、
测试 fixture、SDK 示例和未来部署文档能共享同一套类型与验证工具。关键边界是：

- relay 可以作为可选 binary 发布。
- 现有 SDK crates 不依赖 relay。
- relay 未启用时，SDK 的默认行为、public API 心智和 runtime contract 都不改变。

因此，本轮迭代的意义是提供一个可选的共享行情中继，用单上游 tick 输入兑现减少天勤
订阅压力的目标。
