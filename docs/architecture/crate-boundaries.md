# 当前 Crate 边界审计

## 文档定位
本文档用于审计当前 workspace 已落地 crate 的职责边界是否合理，以及它们是否足以承载后续继续对齐 `tqsdk-python` 与现有 `tqsdk-rs` 的能力。

讨论的不是“现在还能加什么功能”，而是下面几个更关键的问题：

- 当前边界是否符合高性能底座的目标
- 常见用户场景会不会把能力推向错误的 crate
- 哪些能力应继续留在当前内部语义层
- 哪些能力应明确后移到未来新 crate

## 当前结论

当前边界整体判断为：

- 方向正确
- 可以继续稳定演进
- 不应回退成单体 `TqApi` crate
- 也不应把 direct query / task / downloader 重新塞回 `tqsdk-wait`

一句话总结：

- `tqsdk` 是对外默认 facade / prelude，不是内部物理合并
- `tqsdk-core` 是 protocol-complete runtime substrate
- `tqsdk-session` 是 shared session + one-shot request/response
- `tqsdk-wait` 是 Python 风格单推进点的 continuous-consumption facade
- `tqsdk-task` 是高层执行工具与任务编排层
- `tqsdk-data` 是 research/offline data、history、cache、export 能力层

内部层依然是按“语义层”切分，而不是按 market / trade / replay / query 协议域切分。对于天勤这种多协议域共享同一 session、同一状态树、同一 commit 语义的系统，这是更稳的切法。`tqsdk` 只把这些能力组织成默认用户入口。

## 审计标准

本次判断使用下面几条标准：

- 是否保护同一棵 runtime state tree 和同一套 commit / revision 语义
- 是否让高性能用户可以停留在足够低的层面
- 是否让 Python 风格用户可以获得稳定的 `wait_update()` 心智
- 是否避免把研究工具、执行任务系统和 protocol substrate 混在一起
- 是否为 `tqsdk-task`、downloader、自建多消费者消费层等能力留出清晰落点

## `tqsdk`

### 正确职责

`tqsdk` 应继续承担：

- 默认安装入口
- `prelude`
- `Tq` 主循环和常用 live refs 的轻量包装
- 零分支跨模态入口：`backtest` 持久缓存本地撮合回测、`server_backtest` 官方服务端回测、`server_replay` 官方单日复盘、`replay_backtest` 高级自定义 replay
- `TargetPos` / backtest cache builder / replay metadata helper 这类低样板组合入口
- Python-style `.backtest(start_ns, end_ns)` 的 cache policy、lazy auth、remote-on-miss
  官方回测流补缓存、`.warmup()` 缓存预热 runner 和 `.universe(...)` 选择器接线；
  cache hit 不要求 auth，cache fill 不使用专业历史下载接口
- `record_ticks(cache_dir, symbols)` 这类显式 live/session 到持久 tick cache 的组合入口；
  订阅和 `wait_update()` 驱动留在 facade，实际 rows/coverage 写入复用 `tqsdk-data`
- `advanced::*` 下钻到底层 crate

### 不应承担的职责

`tqsdk` 不应继续吸收：

- runtime contract 或状态树实现
- direct query / metadata 的真实实现
- multi-consumer fan-out 的真实实现
- task/data 内部状态机或存储能力

### 判断

这一层解决的是用户入口复杂度，不是重新划分内部架构。它可以存在，但必须保持薄。

## `tqsdk-core`

### 正确职责

`tqsdk-core` 应继续承担：

- 统一命令模型
- 统一状态树
- 统一 commit / revision / causality 语义
- market/trade 分区读面，以及同 revision 的 market+trade 组合读 guard
- protocol adapter
- auth / bootstrap / transport / session runtime orchestration
- typed schema contract
- trade / replay / query / schema / system 的底层 wire/state 语义
- `websocket-transport` 默认 feature 下的 yawc-backed websocket adapter；关闭默认 feature
  时仍保留 command / state / schema / cursor / transport trait contract，但不拉取 `yawc`

这些职责当前与实现一致，见：

- `RuntimeHandle` / `RuntimeReader` / `UpdateCursor`
- `SessionRuntime`
- `AdapterRegistry`
- `EndpointConfig` / `SessionConfig`
- `tqsdk_core::transport::*` transport namespace
- typed objects in `types::*`

### 不应承担的职责

`tqsdk-core` 不应继续吸收：

- `wait_update()` facade
- callback / fan-out facade
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
- `SymbolInfo` 这类官方合约信息表 typed 结果，以及 `InstrumentSpec` 这类窄规格对象
- calendar / settlement / ranking / EDB
- 官方单日 replay session 创建、endpoint 解析、facade 自动 heartbeat 和显式控速/terminate
- auth refresh
- session-scoped order intent ledger（只记录 client order id 与 runtime order id
  的进程内/session 内对应关系，不做订单状态 overlay）
- typed command status helper（只解析 runtime command ledger 状态，不绕过状态机）
- replay step / reset 的 one-shot helper

这些能力都具有同一个特征：

- 它们不要求用户持续持有一个 live object 并等待后续 diff
- 它们本质上是一次 `await` 请求/响应，或“一次命令 -> 等待完成 -> 返回值”

### 继续留在 `tqsdk-session` 的能力

下面这些接口应该继续留在 `tqsdk-session`：

- `query_graphql*`
- `refresh_schema*`
- `query_symbol_info`
- `query_instrument_specs`
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
- `ServerReplayBuilder`
- `ServerReplaySession`
- `remember_order_intent`
- `order_intent`
- `replay_step*`
- `replay_reset*`

### 不应吸收的能力

`tqsdk-session` 不应继续吸收：

- wait facade 的 `quote` / `trading_status` live handles
- wait facade 的 `kline` / `tick` live serial handles
- live trade refs
- `step()` / `step_until(...)`
- object fan-out / callback
- downloader
- `TargetPosTask`
- DataFrame / polars 形状
- `query_his_cont_quotes`
- `query_option_greeks`

### 当前边界提醒

此前 `tqsdk-session` 里曾出现过 `SessionFacadeConfig/default_view_width` 这类更像 consumer facade 的配置项。

这部分已经被移出 session substrate。后续仍应继续保持同一条约束：

- `tqsdk-session` 不应演化成“大家都顺手塞一点公共配置”的地方
- 如果出现更多 wait / 自建消费层共用的消费配置，应在消费层单独提炼，而不是回灌到 session

### 判断

总体上，`tqsdk-session` 的边界是合理的，而且正好承担了 `tqsdk-python` 单体 `TqApi` 中最适合拆出来的一层。

## `tqsdk-wait`

### 正确职责

`tqsdk-wait` 当前的位置非常清楚：

- 它不是 Python `TqApi` 的全量复制
- 它只是 Python `wait_update()` 范式在 Rust 中的承载层

它当前承担下面这些职责是合理的：

- 单 owner `TqApi`
- `step()` / `step_until(...)` 主推进点
- `WaitStep::is_changing()` / `WaitStep::is_changing_fields()`
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
- callback / fan-out
- DataFrame / polars / offline analysis helper
- 本地 overlay 状态树

### 判断

这一层当前边界是健康的，而且是最接近你目标用户体验的部分。

后续最大的风险不是“功能不够”，而是重新把 Python 单体 `TqApi` 的其他便利接口全部塞回来。

## `tqsdk-task`

### 正确职责

`tqsdk-task` 当前应继续承担：

- `TaskHost`
- `TargetPosTask`
- `TargetPosScheduler`
- ownership / guarded order
- execution report
- strategy host / strategy context / strategy environment / deployment / supervisor adapter
- strategy replay driver with task-owned replay market source
- 低延迟 trading desk thin profile：
  - hot path 使用 shared `SessionClient + RuntimeReader`
  - 风控与下单契约复用 `RiskEngine` / `TaskOrderIntent`
  - 订单状态和 latency report 保持 typed API
- public fake market / fake broker test harness
- 规划与执行之间的本地任务状态机

它是执行工具层，不是消费 facade，也不是协议 substrate。
它拥有 `replay::ReplayMarketEvent` / `replay::ReplayMarketSource` 这类 replay/backtest 输入类型；
也可以通过 `BacktestMarketStream` 接收 caller-owned 或 cache-backed streaming market source。
cached tick replay 由 `HistoryTickReplayStream` 对 owned `TickDataSeries` 做有界 heap merge。
持久 tick 覆盖检查和文件格式归 `tqsdk-data::BacktestTickCache` / `HistorySeriesCache`，strategy
execution 与本地撮合归 `tqsdk-task`。
这是上层集成路径，不代表 strategy execution 进入 data，也不代表 task 拥有 durable history
cache 文件格式。
S31 trading desk profile 是例外的低延迟薄 profile：它属于 task 的执行契约，
但不复用 `TaskHost::wait_update()` hot path。慢日志、WAL、journal、落盘重试、
audit sidecar 和跨进程恢复由调用方或上层服务拥有；`TradingDeskProfile` 不持有
sink、WAL、journal 或 cache writer。

### 不应吸收的能力

`tqsdk-task` 不应继续吸收：

- direct query / schema / metadata
- downloader / DataFrame / polars
- 回测报告 / GUI
- 反向要求 `tqsdk-core` 改写提交模型

### 判断

这一层当前边界也是合理的。

后续主要工作不是继续拓宽 public surface，而是继续稳固 planner、ownership 和执行报告语义。

## `tqsdk-data`

### 正确职责

`tqsdk-data` 当前应继续承担：

- history page / series / download / export substrate
- `HistorySeriesCache` public facade 和 crate 内部 store adapter seam
- `HistorySeriesCache::open(root_dir)` 使用 canonical TQBN v1 history cache format；
  TQBN 是 tqsdk-specific DBN-like binary format，使用 fixed-width records、fixed-point
  price storage、self-describing metadata、explicit coverage records 和 forward-compatible
  record lengths；embedded coverage commit 和 tick-only `BacktestTickCache`
- TQBN cache scan / retention / size-limit maintenance；`enforce_limits(...)` 会执行 append-log compaction，
  合并重复 rows 并保留 last-write-wins 语义；
  旧 `.tqseries` 不再作为默认 backend，且没有兼容读取或迁移 store
- remote backtest cache fill 的完整性 accumulator / report 类型
- `LiveTickCacheWriter` 这类纯数据层 live tick row writer：只接收已解码 tick rows，按连续
  tick id 推进 coverage，不拥有 session、订阅、wait loop 或后台进程
- cache inspect / purge / compact 运维 API：输出 backend、文件路径、coverage/missing ranges，并按
  `(symbol, tick)` 文件粒度清除或合并回测 tick 缓存
- relay-compatible futures universe selector parser 与 resolver 抽象

`HistorySeriesCache::open(...)` 和 `BacktestTickCache::open(...)` 仍是 public facade
入口，并默认使用 crate-internal TQBN store。`.tqseries` 不是 public long-term format target；
旧 Python `DataSeries` 兼容 binary/mmap backend 已从 public surface 废弃。
`BacktestTickCache` 是回测加速主存储的 data facade：它只缓存 tick，K 线由 tick
回放/合成路径派生。

### 不应吸收的能力

`tqsdk-data` 不应继续吸收：

- strategy execution / 本地撮合
- wait-update live object facade
- remote-on-miss 的 session 推进 loop
- relay 进程、dashboard 或多客户端 market service

## `tqsdk-relay`

### 正确职责

- 可选独立进程 / binary
- 代理 market route 子集
- 维护共享上游 tick source、内存行情 cache、K 线合成、bootstrap/resync 队列
- 在 relay 内部做期货产品到当前活跃合约集合的 typed metadata 发现与每日固定时间刷新
- 产品发现按批调用 `query_symbol_info`，并使用返回的 `trading_time` 作为合约交易时间段
  判断的优先来源
- metadata 查询按批执行，避免动态产品发现本身制造过大的单次 query 请求
- 在连接上游前检查 `ins_list` 长度阈值，避免已知过长订阅字符串进入天勤行情连接；
  超出 hard limit 时给出 relay 实例拆分建议
- 提供 relay 自身 dry-run 启动自检、结构化启动日志、HTTP health / metrics 观测；
  health 必须区分进程/下游监听、上游连接、合约集合刷新和数据 freshness
- 新 K 线订阅可用 relay 内存 tick ring 回放已闭合的合成 K 线；这不代表远端 K 线回填
  或跨重启持久化已进入 V1

### 不应承担的职责

- 不向下游 SDK 客户端代理 trade / query / auth / schema / metadata
- 不进入现有 SDK crate 的默认依赖路径
- 不进入 Cargo default-members；relay Rust 和 dashboard validation 必须显式运行
- 不改变 `tqsdk-core` runtime contract
- 不作为多 provider 行情聚合框架

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
- 如需在同一 hot path 上做 typed risk / order intent / latency report，可使用
  `tqsdk_task::trading_desk::TradingDeskProfile`

判断：

- 当前边界合理
- 不应强制这类用户走 `tqsdk-wait`

### 场景 2：Python 心智的策略研究用户

需求：

- `step()` 循环
- 稳定状态截面
- `WaitStep::is_changing()` 解释最近一轮 commit

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
- 事件投递 / fan-out
- 背压可控

合理路径：

- `tqsdk-session + RuntimeReader/UpdateCursor`
- 调用方自建 fan-out、背压、事件投影和 health surface

判断：

- 当前边界刻意不提供内置 fan-out facade
- 不应为了多消费者需求回灌 core/session 或扩宽 `tqsdk-wait`

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
- 历史数据质量报告 / integrity report
- TQBN v1 (`.tqbn`) 当前默认和 canonical 格式
- 可选 `tqbn-zstd` feature 只压缩 TQBN internal records block；`tqsdk-data` 是实现点，
  `tqsdk-task` / `tqsdk` 仅做同名 feature 转发
- 旧 `.tqseries` 不是默认 backend，也不提供兼容读取或迁移 store；旧 Python-compatible
  binary/mmap cache 已废弃
- 回测 tick 持久缓存、coverage 检查和 shared universe selector
- history page/series/download/export
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

保持当前边界，不建议回退或重划：

- `tqsdk` 继续做默认 facade / prelude
- `tqsdk-core` 继续只做底层统一 contract
- `tqsdk-session` 继续做 shared session + one-shot control/query
- `tqsdk-wait` 继续做 Python 风格单推进点 continuous-consumption facade
- `tqsdk-task` 继续做执行工具层
- `tqsdk-data` 继续做 research/offline data、history、cache、export 能力层

接下来真正要补的不是重新划分这些已落地 crate，而是继续稳固默认 `tqsdk` 入口和内部 `task/data` 能力边界。

2026-06-25 removal conclusion: the former multi-consumer facade crate has been removed. Multi-consumer async systems should build on `tqsdk-session + RuntimeReader/UpdateCursor`; `tqsdk` / `tqsdk-wait` carry the ordinary strategy path, while `tqsdk-task` and `tqsdk-data` remain advanced or opt-in.
