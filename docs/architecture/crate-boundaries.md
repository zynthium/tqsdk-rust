# 当前 Crate 边界审计

## 文档定位
本文档用于审计当前 workspace 五个已落地子 crate 的职责边界是否合理，以及它们是否足以承载后续继续对齐 `tqsdk-python` 与现有 `tqsdk-rs` 的能力。

讨论的不是“现在还能加什么功能”，而是下面几个更关键的问题：

- 当前边界是否符合高性能底座的目标
- 常见用户场景会不会把能力推向错误的 crate
- 哪些能力应继续留在当前五层
- 哪些能力应明确后移到未来新 crate

## 当前结论

当前五层边界整体判断为：

- 方向正确
- 可以继续稳定演进
- 不应回退成单体 `TqApi` crate
- 也不应把 direct query / task / downloader 重新塞回 `tqsdk-wait`

一句话总结：

- `tqsdk-core` 是 protocol-complete runtime substrate
- `tqsdk-session` 是 shared session + one-shot request/response
- `tqsdk-wait` 是 Python 风格单推进点的 continuous-consumption facade
- `tqsdk-stream` 是 shared-session multi-consumer stream facade
- `tqsdk-task` 是高层执行工具与任务编排层

这五层依然是按“语义层”切分，而不是按 market / trade / replay / query 协议域切分。对于天勤这种多协议域共享同一 session、同一状态树、同一 commit 语义的系统，这是更稳的切法。

## 审计标准

本次判断使用下面几条标准：

- 是否保护同一棵 runtime state tree 和同一套 commit / revision 语义
- 是否让高性能用户可以停留在足够低的层面
- 是否让 Python 风格用户可以获得稳定的 `wait_update()` 心智
- 是否避免把研究工具、执行任务系统和 protocol substrate 混在一起
- 是否为 `tqsdk-stream`、`tqsdk-task`、downloader 等能力留出清晰落点

## `tqsdk-core`

### 正确职责

`tqsdk-core` 应继续承担：

- 统一命令模型
- 统一状态树
- 统一 commit / revision / causality 语义
- protocol adapter
- auth / bootstrap / transport / session runtime orchestration
- typed schema contract
- trade / replay / query / schema / system 的底层 wire/state 语义

这些职责当前与实现一致，见：

- `RuntimeHandle` / `RuntimeReader` / `UpdateCursor`
- `SessionRuntime`
- `AdapterRegistry`
- `EndpointConfig` / `SessionConfig`
- typed objects in `types::*`

### 不应承担的职责

`tqsdk-core` 不应继续吸收：

- `wait_update()` facade
- stream / callback facade
- direct query convenience wrapper
- downloader
- `TargetPosTask`
- DataFrame / polars / report / GUI
- 用户态任务系统

### 判断

这一层当前边界是健康的。

真正要继续保持警惕的不是“core 太底层”，而是未来因为它最稳定、最通用，导致大家顺手往里面塞 convenience。只要守住“不新增高层用户语义”这条线，它就仍然是可复用的高性能底座。

## `tqsdk-session`

### 正确职责

`tqsdk-session` 当前最准确的定义不是“query 层”，而是：

- shared session owner
- one-shot control-plane helper
- one-shot request/response facade

它当前承担下面这些职责是合理的：

- lazy establish
- `flush_outbound()` / `drive_pending_once()` / `drive_route_once()`
- GraphQL / schema refresh
- metadata query
- calendar / settlement / ranking / EDB
- auth refresh
- session-scoped order intent ledger（只记录 client order id 与 runtime order id
  的进程内/session 内对应关系，不做订单状态 overlay）
- replay step / reset 的 one-shot helper

这些能力都具有同一个特征：

- 它们不要求用户持续持有一个 live object 并等待后续 diff
- 它们本质上是一次 `await` 请求/响应，或“一次命令 -> 等待完成 -> 返回值”

### 继续留在 `tqsdk-session` 的能力

下面这些接口应该继续留在 `tqsdk-session`：

- `query_graphql*`
- `refresh_schema*`
- `query_symbol_info`
- `query_quotes`
- `query_cont_quotes`
- `query_options`
- `query_atm_options`
- `query_all_level_options`
- `query_all_level_finance_options`
- `get_trading_calendar`
- `query_symbol_settlement`
- `query_symbol_ranking`
- `query_edb_data`
- `refresh_auth*`
- `remember_order_intent`
- `order_intent`
- `replay_step*`
- `replay_reset*`

### 不应吸收的能力

`tqsdk-session` 不应继续吸收：

- `get_quote`
- `get_trading_status`
- `get_kline_serial`
- `get_tick_serial`
- live trade refs
- `wait_update()`
- object stream / callback
- downloader
- `TargetPosTask`
- DataFrame / polars 形状
- `query_his_cont_quotes`
- `query_option_greeks`

### 当前边界提醒

此前 `tqsdk-session` 里曾出现过 `SessionFacadeConfig/default_view_width` 这类更像 consumer facade 的配置项。

这部分已经被移出 session substrate。后续仍应继续保持同一条约束：

- `tqsdk-session` 不应演化成“大家都顺手塞一点公共配置”的地方
- 如果出现更多 wait / stream 共用的消费层配置，应在消费层单独提炼，而不是回灌到 session

### 判断

总体上，`tqsdk-session` 的边界是合理的，而且正好承担了 `tqsdk-python` 单体 `TqApi` 中最适合拆出来的一层。

## `tqsdk-wait`

### 正确职责

`tqsdk-wait` 当前的位置非常清楚：

- 它不是 Python `TqApi` 的全量复制
- 它只是 Python `wait_update()` 范式在 Rust 中的承载层

它当前承担下面这些职责是合理的：

- 单 owner `TqApi`
- `wait_update()` 主推进点
- `is_changing()` / `is_changing_fields()`
- diff-backed live object `Ref`
- serial/window 视图
- trade command 的 wait 风格薄包装

### 应继续进入 `tqsdk-wait` 的能力

凡是满足下面条件的对象，都适合继续进入 `tqsdk-wait`：

- 它存在于 runtime state tree 中
- 它依赖后续 diff 持续推进
- 用户需要在稳定 commit 边界上读取它

这意味着适合进入 `tqsdk-wait` 的对象包括：

- `PreInsertOrder`
- `RiskManagementRule`
- `RiskManagementData`
- `Notification`
- `SettlementInfo`
- `SecurityAccount`
- `SecurityPosition`
- `SecurityOrder`
- `SecurityTrade`

这些对象的 typed contract 都已经存在于 core 中，并且当前都已经有 wait facade live refs。对于证券账户这组对象，虽然路径仍然复用 `trade/{account_id}/...`，但其 facade 通过独立 decode 类型与独立 `Ref` 名称保持了 futures / securities schema 的边界清晰。

### 不应吸收的能力

`tqsdk-wait` 不应继续吸收：

- GraphQL / HTTP direct query
- schema refresh / metadata facade
- downloader
- `TargetPosTask`
- stream / callback
- DataFrame / polars / offline analysis helper
- 本地 overlay 状态树

### 判断

这一层当前边界是健康的，而且是最接近你目标用户体验的部分。

后续最大的风险不是“功能不够”，而是重新把 Python 单体 `TqApi` 的其他便利接口全部塞回来。

## `tqsdk-stream`

### 正确职责

`tqsdk-stream` 当前的职责也已经清楚：

- shared-session 多消费者 commit fan-out
- path / scope / domain / object / field 过滤
- typed path stream
- kline / tick window stream
- trade 相关事件流

它的价值不在于“再造一套 runtime”，而在于让高性能、多消费者、异步系统集成方可以直接消费同一套 commit 语义。

### 不应吸收的能力

`tqsdk-stream` 不应继续吸收：

- GraphQL / HTTP direct query
- schema / metadata facade
- downloader
- `TargetPosTask`
- DataFrame / polars
- 本地 overlay 状态树

### 判断

这一层已经有明确而健康的落点。

真正要继续防止的，是把它重新做成一个宽而胖的“第二个总入口”。

## `tqsdk-task`

### 正确职责

`tqsdk-task` 当前应继续承担：

- `TaskHost`
- `TargetPosTask`
- `TargetPosScheduler`
- ownership / guarded order
- execution report
- strategy host / strategy context
- strategy cache replay driver
- public fake market / fake broker test harness
- 规划与执行之间的本地任务状态机

它是执行工具层，不是消费 facade，也不是协议 substrate。
它可以消费 `tqsdk-data` cache/history event 构建 strategy replay driver；这是
上层集成路径，不代表 cache storage 进入 task，也不代表 strategy execution
进入 data。

### 不应吸收的能力

`tqsdk-task` 不应继续吸收：

- direct query / schema / metadata
- downloader / DataFrame / polars
- 回测报告 / GUI
- 反向要求 `tqsdk-core` 改写提交模型

### 判断

这一层当前边界也是合理的。

后续主要工作不是继续拓宽 public surface，而是继续稳固 planner、ownership 和执行报告语义。

## 常见场景下的边界合理性

### 场景 1：高性能 live 交易用户

需求：

- 自带 Tokio runtime
- 订阅实时行情
- 读取账户/持仓/订单
- 发单/撤单
- 尽量少 facade 抽象损耗

合理路径：

- `tqsdk-core + tqsdk-session`
- 如需现成但仍很薄的用户 API，再加 `tqsdk-stream`

判断：

- 当前边界合理
- 不应强制这类用户走 `tqsdk-wait`

### 场景 2：Python 心智的策略研究用户

需求：

- `wait_update()` 循环
- 稳定状态截面
- `is_changing()` 解释最近一轮 commit

合理路径：

- `tqsdk-wait`
- 需要一次性 query 时通过 `api.session()` 回落到 `tqsdk-session`

判断：

- 当前边界合理
- 这正是 `tqsdk-wait` 的正确使命

### 场景 3：中间件 / 多消费者异步系统

需求：

- 共享 live session
- 多任务并发消费
- 事件流 / stream
- 背压可控

合理路径：

- `tqsdk-session`
- `tqsdk-stream`

判断：

- 当前边界合理
- 已经有合适的 stream facade 落点

### 场景 4：只做 metadata / calendar / settlement / ranking 查询

需求：

- 不消费实时 diff
- 只做一次性 request/response

合理路径：

- `tqsdk-session`

判断：

- 当前边界合理
- 这类能力不应进入 `tqsdk-wait`

### 场景 5：执行任务与自动调仓

需求：

- 持续读 live state
- 持续发交易命令
- 维护任务内部状态和 ownership

合理路径：

- `tqsdk-task`

判断：

- 当前三层都不应直接承接

### 场景 6：研究、历史下载、DataFrame

需求：

- 批量历史数据拉取
- 本地行情 cache record / reader-writer / ordered replay foundation
- history series -> market cache replay adapter
- DataFrame / polars
- 衍生计算
- 离线分析

合理路径：

- `tqsdk-data`

判断：

- 当前三层都不应直接承接

## 与参考实现的对比结论

### 相比 `tqsdk-python`

当前边界比 Python 单体 `TqApi` 更清晰。

Python 的优势是：

- 用户心智统一
- `wait_update()` 稳定截面非常强

Python 的问题是：

- query、task、GUI、DataFrame、drawing、simulation、backtest 都聚集在单个入口

当前 workspace 已经成功把最应该拆开的部分拆开了：

- one-shot request/response -> `tqsdk-session`
- `wait_update()` continuous consumption -> `tqsdk-wait`
- protocol substrate -> `tqsdk-core`

### 相比现有 `tqsdk-rs`

当前边界比现有 `tqsdk-rs` 的 public surface 更克制。

现有 `tqsdk-rs` 同时暴露了：

- `Client`
- `TradeSession`
- `ReplaySession`
- `TqRuntime`
- `TargetPosTask`
- `DataDownloader`
- `DataManager`
- optional polars

这让用户很强大，但边界会持续变宽。

当前 workspace 则把“稳定底座”和“工具层能力”明确分开，这更适合作为长期可发布基础库。

## 当前总判断

保持当前三层边界，不建议回退或重划：

- `tqsdk-core` 继续只做底层统一 contract
- `tqsdk-session` 继续做 shared session + one-shot control/query
- `tqsdk-wait` 继续做 Python 风格单推进点 continuous-consumption facade
- `tqsdk-stream` 继续做多消费者 continuous-consumption facade
- `tqsdk-task` 继续做执行工具层

接下来真正要补的不是重新划分这些已落地 crate，而是继续稳固 `stream/task`，并在合适时新增 `data/research` 等更上层 crate。
