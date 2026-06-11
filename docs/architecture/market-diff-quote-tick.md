# Market DIFF、Quote 与 Tick

本文档说明天勤 market DIFF 协议在 `Quote`、tick serial 和 K 线 serial 上的
状态形态，并固定 relay / dashboard 判断实时性的字段口径。

它不改变 crate 边界：

- `tqsdk-core` 只表达协议对象、状态树、adapter、commit / reader contract。
- `tqsdk-session` 负责提交 market command 和维护 shared session。
- `tqsdk-wait` / `tqsdk-stream` 负责 diff-backed live object 消费形态。
- `tqsdk-relay` 是可选 market relay，不改变默认直连路径。

## DIFF 协议心智模型

天勤 market websocket 返回的是状态树 diff。客户端不是把每一帧当成完整行情对象，
而是把 `rtn_data.data` 里的状态片段递归合并到本地状态树。

基本流程：

```text
client -> subscribe_quote / set_chart
client -> peek_message
server -> rtn_data
client merge rtn_data into local state
client -> peek_message
server -> rtn_data
...
```

关键 `aid`：

| `aid` | 用途 | 典型状态路径 |
| :--- | :--- | :--- |
| `subscribe_quote` | 订阅 quote 快照对象 | `quotes/{symbol}` |
| `set_chart` with `duration = 0` | 订阅 tick serial 窗口 | `ticks/{symbol}/data/{id}`、`charts/{chart_id}` |
| `set_chart` with `duration > 0` | 订阅 K 线 serial 窗口 | `klines/{symbol}/{duration}/data/{id}`、`charts/{chart_id}` |
| `peek_message` | 推进下一批消息 | 无状态对象 |
| `rtn_data` | 服务端返回 diff 数据 | 按 `data` 中的路径合并 |

`subscribe_quote` 示例：

```json
{
  "aid": "subscribe_quote",
  "ins_list": "SHFE.au2608,DCE.m2609"
}
```

返回的 quote diff 可能只包含变化字段：

```json
{
  "aid": "rtn_data",
  "data": [
    {
      "quotes": {
        "SHFE.au2608": {
          "datetime": "20260610100000000",
          "last_price": 900.0,
          "bid_price1": 899.8,
          "bid_volume1": 10,
          "ask_price1": 900.0,
          "ask_volume1": 5,
          "volume": 123456,
          "open_interest": 88888
        }
      }
    }
  ]
}
```

tick serial 使用 `set_chart(duration = 0)`：

```json
{
  "aid": "set_chart",
  "chart_id": "tick-au",
  "ins_list": "SHFE.au2608",
  "duration": 0,
  "view_width": 10
}
```

服务端会维护 chart 窗口边界，并把 tick 行放在 `ticks` 分区：

```json
{
  "aid": "rtn_data",
  "data": [
    {
      "charts": {
        "tick-au": {
          "left_id": 120,
          "right_id": 129,
          "more_data": false,
          "ready": true
        }
      },
      "ticks": {
        "SHFE.au2608": {
          "last_id": 129,
          "data": {
            "129": {
              "id": 129,
              "datetime": 1781023021000000000,
              "last_price": 900.0,
              "volume": 123456,
              "open_interest": 88888
            }
          }
        }
      }
    }
  ]
}
```

K 线 serial 也是 `set_chart`，但 `duration > 0`：

```json
{
  "aid": "set_chart",
  "chart_id": "kline-au-1m",
  "ins_list": "SHFE.au2608",
  "duration": 60000000000,
  "view_width": 200
}
```

对应状态路径是：

```text
charts/kline-au-1m
klines/SHFE.au2608/60000000000/data/{id}
```

多合约 K 线 serial 仍然是一个 `set_chart`，但 `ins_list` 是按传入顺序逗号拼接的
合约列表，且第一个合约是主合约：

```json
{
  "aid": "set_chart",
  "chart_id": "kline-spread-1m",
  "ins_list": "SHFE.au2608,DCE.m2609",
  "duration": 60000000000,
  "view_width": 10000
}
```

服务端会把副合约 K 线通过主合约分区里的 `binding` 对齐到主合约 id。客户端读取
multi window 时必须以主合约 `charts/{chart_id}.left_id/right_id` 为驱动，并按：

```text
klines/{primary}/{duration}/binding/{secondary}/{primary_id}
klines/{secondary}/{duration}/data/{secondary_id}
```

取得副合约行。没有 binding 或缺少副合约 row 的主合约 id 不进入 multi window。
Tick serial 不使用这套 binding 语义，仍然只支持单合约 chart。

## Quote 与 Tick 字段对比

`Quote` 是合约行情和合约元信息的最新快照；`Tick` 是 serial 行。两者都来自
market DIFF，但语义不同：

- `Quote` 没有行 `id`，不能当作逐条 tick 序列。
- `Quote.datetime` 是字符串；`Tick.datetime` 是 `i64` 纳秒时间戳。
- `Quote` 额外包含合约规则、昨值、期权/股票扩展等低频字段。
- `Tick` 只表达 tick 行本身，适合做 tick serial、K 线合成和逐行回放。

| 字段类别 | Quote | Tick | 说明 |
| :--- | :---: | :---: | :--- |
| `id` | 无 | 有 | Tick 行序号；Quote 是快照，没有行 id。 |
| `datetime` | 有，`String` | 有，`i64` | Tick 通常是纳秒时间戳；Quote 类型中保留协议字符串。 |
| `last_price` | 有 | 有 | 最新价。 |
| `average` | 有 | 有 | 均价。 |
| `highest` / `lowest` | 有 | 有 | 日内最高/最低。 |
| `ask_price1..5` | 有 | 有 | 卖一到卖五。 |
| `ask_volume1..5` | 有 | 有 | 卖量一到五。 |
| `bid_price1..5` | 有 | 有 | 买一到买五。 |
| `bid_volume1..5` | 有 | 有 | 买量一到五。 |
| `volume` | 有 | 有 | 累计成交量。 |
| `amount` | 有 | 有 | 累计成交额。 |
| `open_interest` | 有 | 有 | 持仓量。 |
| `open` / `close` | 有 | 无 | Quote 有开盘/收盘价。 |
| `settlement` | 有 | 无 | 结算价。 |
| `upper_limit` / `lower_limit` | 有 | 无 | 涨跌停价。 |
| `pre_open_interest` | 有 | 无 | 昨持仓。 |
| `pre_settlement` / `pre_close` | 有 | 无 | 昨结算/昨收。 |
| `pre_volume` | 有 | 无 | 昨成交量。 |
| `price_tick` / `price_decs` | 有 | 无 | 最小变动价位与价格精度。 |
| `volume_multiple` | 有 | 无 | 合约乘数。 |
| 下单数量限制字段 | 有 | 无 | 例如 `max_limit_order_volume`、`min_market_order_volume`。 |
| 合约基础信息 | 有 | 无 | `instrument_id`、`instrument_name`、`exchange_id`、`product_id` 等。 |
| 期权相关字段 | 有 | 无 | `underlying_symbol`、`strike_price`、`option_class`、`exercise_type` 等。 |
| 到期/交割字段 | 有 | 无 | `expired`、`expire_datetime`、`delivery_year`、`delivery_month` 等。 |
| 股票/基金扩展 | 有 | 无 | `iopv`、流通股本、分红比例等。 |
| `change` / `change_percent` | 有 | 无 | 涨跌与涨跌幅。 |
| `margin` / `commission` | 有 | 无 | 保证金和手续费参考字段。 |
| `_epoch` / `epoch` | 有 | 有 | 内部版本/时间戳字段，不建议作为业务行情字段。 |

## Quote 字段更新频率

`Quote` 字段按 diff 下发，服务端可以只发送变化字段。下表描述字段值通常的变化频率，
不是 wire protocol 的强制频率。

| 类别 | 字段 | 更新特征 |
| :--- | :--- | :--- |
| 高频实时行情 | `datetime` | 行情时间，适合判断是否持续收到行情。 |
| 高频实时行情 | `last_price` | 最新价，随成交变化。 |
| 高频实时行情 | `bid_price1..5` / `bid_volume1..5` | 买盘五档，随盘口变化。 |
| 高频实时行情 | `ask_price1..5` / `ask_volume1..5` | 卖盘五档，随盘口变化。 |
| 高频/中频实时行情 | `volume` | 累计成交量，成交时变化。 |
| 高频/中频实时行情 | `amount` | 累计成交额，成交时变化。 |
| 高频/中频实时行情 | `open_interest` | 持仓量，随成交和持仓变化更新。 |
| 中频日内行情 | `highest` / `lowest` | 只有创新高/新低时变化。 |
| 中频日内行情 | `average` | 通常随成交变化。 |
| 中频派生行情 | `change` / `change_percent` | 通常随最新价变化。 |
| 低频日内字段 | `open` | 开盘后基本固定。 |
| 低频日内/收盘字段 | `close` | 通常低频或收盘后变化。 |
| 低频日级字段 | `settlement` | 通常日级更新。 |
| 低频日级字段 | `upper_limit` / `lower_limit` | 通常交易日前或日内固定。 |
| 低频昨值字段 | `pre_open_interest`、`pre_settlement`、`pre_close`、`pre_volume` | 通常日级固定。 |
| 合约静态字段 | `instrument_id`、`instrument_name`、`exchange_id`、`product_id`、`ins_class` | 合约身份信息，基本不随行情变化。 |
| 合约交易规则 | `price_tick`、`price_decs`、`volume_multiple` | 低频或静态。 |
| 下单限制 | `open_limit`、`max_*_volume`、`min_*_volume` | 低频交易规则字段。 |
| 期权/到期/交割 | `underlying_symbol`、`strike_price`、`expire_datetime` 等 | 低频或静态。 |
| 股票/基金扩展 | `iopv`、`public_float_share_quantity`、分红比例 | `iopv` 可能随行情变化，其他多为低频。 |
| 展示/分类字段 | `categories`、`product_short_name`、`underlying_product`、`py`、`trading_time` | 元数据或展示字段，低频。 |
| 费用/风控参考 | `position_limit`、`margin`、`commission` | 低频配置类字段。 |

对行情 freshness 和 dashboard 监控，应优先使用：

- `datetime`
- `last_price`
- `bid_price1` / `ask_price1`
- `bid_volume1` / `ask_volume1`
- `volume`
- `open_interest`

低频字段长期不变不代表行情断流。

relay dashboard 的监控口径进一步拆成四类，不再把所有异常压缩进一个 `status`：

- `coverage`：合约是否属于当前上游 universe。下游仍在订阅但已退出 universe 的合约是
  `uncovered`，即使当前交易时段休盘也仍是覆盖问题。
- `session`：基于官方 `trading_time`、quote trading time 或内置期货时段推导的
  `open` / `closed` / `unknown`。计算固定使用 Asia/Shanghai 时区，不受 host 本地时区影响。
- `flow`：基于 relay 接收时间的 `flowing` / `silent` / `no_sample`。没有接收样本时是
  `no_sample`，不得把它误判为 `closed`。
- `integrity`：基于 tick row id 连续性的 `intact` / `suspected` / `confirmed_gap`。
  `gap_event_count`、`duplicate_rows`、`out_of_order_rows` 是确认的 tick 完整性问题；
  仅长时间未收到数据属于 suspected silence。

dashboard 的当前健康集合只能是“当前上游 universe ∪ 当前下游订阅”。旧 telemetry 如果既
不在当前 universe、也没有当前订阅，不进入当前健康和默认列表；需要追溯时应进入事件 /
历史账本，而不是污染当前断流判断。

## 订阅历史窗口与 quote-only

`quote` / `quotes` 只表达最新 quote interest，不需要 tick 历史窗口。它走：

```text
ensure_quotes(...)
  -> subscribe_quote
  -> quotes/{symbol}
```

`tick` serial 走 chart 窗口。它走：

```text
ensure_chart(duration_ns = 0, view_width = N)
  -> set_chart
  -> charts/{chart_id}
  -> ticks/{symbol}/data/{id}
```

`view_width` 控制服务端 tick 窗口宽度。`view_width = 1` 仍然是 tick serial，只是要求
最小窗口；当前代码不把 `0` 当作稳定的“完全不取历史”契约。

如果一个进程只需要最新报价和 freshness 监控，应使用 quote-only 订阅，不发送
`set_chart`。这样不会产生 `ticks/{symbol}/data`，也不能提供 tick serial 或由 tick
合成的 K 线。

## 对 tqsdk-relay 的含义

当前 `tqsdk-relay` 上游会同时发送：

```text
subscribe_quote(ins_list)
set_chart(duration = 0, view_width = upstream_tick_view_width)
peek_message
```

因此 relay 的上游启动存在 tick chart backfilling 阶段。把
`TQSDK_RELAY_UPSTREAM_TICK_VIEW_WIDTH=1` 只能把 tick 历史窗口调到最小，不能完全禁用
tick chart。

如果未来增加 quote-only relay 模式，应同步调整这些语义：

- 上游只发送 `subscribe_quote`，不发送 `set_chart`。
- `/health` 和 `/metrics` 的 market-data readiness 可以由 quote event 推进。
- `/symbol-metrics` 的 freshness 应继续以 quote 接收时间和 quote `datetime` 为准，并继续输出
  `coverage`、`session`、`flow`、`integrity` 四个正交状态字段。
- `ticks_ingested` 在 quote-only 模式下可以长期为 `0`，不能用它单独判断断流。
- 下游 tick serial 和 tick-derived K 线必须返回明确不支持，或切换到 synthetic source。
- dashboard 应把 quote freshness 和 tick serial 状态拆开展示，避免把 quote-only 误判为
  “远月合约没有收到消息”。

## 实现检查点

修改 market DIFF 或 relay 行情行为时，至少检查：

- `crates/tqsdk-core/src/types/market.rs` 的 `Quote` / `Tick` 字段契约。
- `crates/tqsdk-core/src/adapter/market.rs` 的 `subscribe_quote` / `set_chart` 编码路径。
- `crates/tqsdk-core/src/adapter/common.rs` 的 chart request 构造与 diff 归一化。
- `crates/tqsdk-wait/src/api.rs` 的 `quote` / `quotes` / `tick` / `kline` facade 行为。
- `crates/tqsdk-relay/src/upstream.rs` 的上游订阅命令。
- `crates/tqsdk-relay/src/symbol_metrics.rs` 的 freshness 与延迟计算。
