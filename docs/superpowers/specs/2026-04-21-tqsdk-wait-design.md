# `tqsdk-wait` / `tqsdk-stream` / `tqsdk-session` 设计文档

## 文档定位
本文档用于锁定 Rust 版 TQSDK 在 V2 facade 层的职责边界，避免后续实现时再次把 facade 语义反向压进 `tqsdk-core`。

状态说明：

- 这份文档形成于 `tqsdk-stream` 与 `tqsdk-task` 落地之前
- 当前 `tqsdk-wait`、`tqsdk-stream`、`tqsdk-session`、`tqsdk-task` 都已实现
- 文中涉及“未来 `tqsdk-stream`”的表述，代表的是实现前的设计判断

这份设计文档只回答三类问题：

- `tqsdk-core` 之上应该再分出哪些 crate，各自负责什么
- `tqsdk-wait` 首版到底做哪些能力，不做哪些能力
- 模式无关的 direct query / schema / metadata 接口应该放在哪里

它不是实现计划，也不是逐任务施工说明。

## 背景
当前仓库已经完成了 V1 core：

- `tqsdk-core`
  - 统一命令模型
  - 统一状态树
  - 统一 commit / revision / causality
  - `RuntimeReader + UpdateCursor` 读契约
  - market / trade / replay / query / schema / auth / session 的 protocol-complete substrate

此前已经完成两轮关键判断：

1. `tqsdk-python` 的核心优势不在“同步”，而在“单推进点 + 单稳定截面”的 `wait_update()` 语义
2. 现有 `tqsdk-rs` 并非纯 callback/stream SDK，而是“async state ref + per-subscription wait + event stream”的混合范式

因此，V2 facade 层不能简单复制任何一个现有项目的外观，而需要按职责重新切边界。

## 总体目标

### 目标 1
让 `tqsdk-wait` 和 `tqsdk-stream` 都只是 `tqsdk-core` 之上的便利包装，而不是新的底层。

### 目标 2
把“状态化 diff 消费接口”和“一次性 direct query 接口”彻底分开，不混入同一个 facade 语义里。

### 目标 3
让高级工具层，例如 downloader、`TargetPosTask`、策略辅助、报表、DataFrame/polars 集成，不属于 `wait`/`stream`，而属于后续独立 crate。

## 非目标

- 不回改 `tqsdk-core` 的 public contract
- 不在 facade 层维护第二棵状态树
- 不在 `tqsdk-wait` 首版中实现 downloader / `TargetPosTask` / callback / stream
- 不追求对 Python 所有表层行为逐项一比一兼容

## 关键设计判断

### 判断 1：按“状态化 diff 消费 vs 一次性 request/response”切边界
这是整个 V2 分层的第一原则。

#### 应归为状态化 diff 消费的接口
这些接口虽然在用户侧看上去像“getter”，但本质上依赖持续推进的状态树，必须留在 `tqsdk-wait` / `tqsdk-stream` 这类模式化 facade 内：

- `get_quote`
- `get_trading_status`
- `get_kline_serial`
- `get_tick_serial`
- `get_account`
- `get_position`
- `get_order`
- `get_trade`
- `insert_order`
- `cancel_order`
- `confirm_settlement`

这些接口都有共同特征：

- 返回的不是一次性结果，而是“当前状态树中的某个持续变化对象”
- 它们的正确使用方式依赖后续 commit 推进
- 它们的变化解释需要 `wait_update` 或 stream/callback 语义配合

#### 应归为一次性 direct query 的接口
这些接口没有“模式选择”的问题，只是一次 `await` 请求/响应，因此不应绑在 `tqsdk-wait` 或 `tqsdk-stream` 上：

- GraphQL / HTTP query
- schema refresh / fetch
- 合约元数据查询
- 交易日历
- `SymbolSettlement`
- `SymbolRanking`
- 其他一次性 metadata / query 接口

这些接口虽然底层仍可通过 `tqsdk-core` 走统一 command / pending route / commit 链路，但在对用户的 API 形状上，不需要 `wait_update` 或 stream 风格去重新表达。

这里需要再强调一层：

- 它们的 crate 归属应当固定在 `tqsdk-session`
- `tqsdk-wait` 和 `tqsdk-stream` 都只复用 `tqsdk-session` 提供的 direct query 能力，不重新包装成自己的主 surface

### 判断 2：需要一个模式无关的共享薄层
如果 `tqsdk-wait` 和 `tqsdk-stream` 都各自重复 auth、bootstrap、query、schema、session 装配，就会出现两类问题：

- 重复实现同一套模式无关逻辑
- direct query 接口被迫“寄居”在 wait 或 stream 某一边

因此在 `tqsdk-core` 之上，需要增加一个共享薄层，本文档暂定命名为：

- `tqsdk-session`

命名后续可调整，但职责边界不应变化。

## 目标分层

```text
tqsdk-core
    ^
    |
tqsdk-session      # 模式无关的 shared thin layer
    ^
    |
-----------------------------
|                           |
tqsdk-wait              tqsdk-stream
    ^                        ^
    |                        |
------ higher-level tools / tasks / helpers ------
```

## 各 crate 的职责边界

### `tqsdk-core`
职责保持不变：

- protocol adapters
- runtime state tree
- commit / revision
- session runtime substrate
- transport / auth / HTTP executor
- schema types

`tqsdk-core` 明确不做：

- `wait_update`
- stream facade
- callback facade
- downloader
- `TargetPosTask`
- 用户态 typed convenience API

### `tqsdk-session`
这是一个模式无关的 shared thin layer，负责：

- auth / bootstrap / endpoint / config 组装
- 建立并持有底层 live session owner
- 暴露 direct query / schema / metadata 接口
- 为上层 facade 暴露驱动 session 所需的低层句柄与 helper

它的定位不是新的用户主 facade，而是 `wait` / `stream` 共同依赖的会话壳。

#### `tqsdk-session` 应提供
- `SessionClient` 或同等 owner 类型
- `SessionClientBuilder`
- `RuntimeHandle` / `RuntimeReader` 访问入口
- `SessionRuntime` / `SessionRun` 驱动原语
- direct query：
  - `query_graphql(...).await`
  - `query_* metadata ... .await`
  - `refresh_schema(...).await`
- 低层驱动 helper：
  - `submit(...)`
  - `flush_outbound(...)`
  - `recv_and_ingest(...)`
  - pending route 执行能力

它也应当成为两类用户的共享 direct-query 入口：

- 研究员主要用 `tqsdk-wait`，但做 metadata/query 时直接调用 `tqsdk-session`
- 高性能用户主要用 `tqsdk-stream`，但做 metadata/query 时同样直接调用 `tqsdk-session`

#### `tqsdk-session` 不应提供
- `wait_update`
- `is_changing`
- stream fan-out
- callback 注册
- quote/kline/account/order 这种模式化对象 facade

### `tqsdk-wait`
`tqsdk-wait` 是建立在 `tqsdk-core + tqsdk-session` 之上的单推进点 facade。

它的职责是：

- 单 owner `wait_update()` 语义
- Python 风格“稳定状态截面”消费
- `is_changing()` 解释
- diff-backed 对象句柄与窗口视图
- trade 命令的 wait 风格包装

#### `tqsdk-wait` 首版范围
- `TqApiBuilder`
- `TqApi`
- `wait_update(deadline).await -> Result<bool>`
- `is_changing(...)`
- `is_changing_fields(...)`
- `get_quote(...).await`
- `get_trading_status(...).await`
- `get_kline_serial(...).await`
- `get_tick_serial(...).await`
- `get_account(...)`
- `get_position(...)`
- `get_order(...)`
- `get_trade(...)`
- `insert_order(...).await`
- `cancel_order(...).await`
- `confirm_settlement(...).await`

#### `tqsdk-wait` 不做
- GraphQL / HTTP direct query facade
- schema / metadata direct facade
- downloader
- `TargetPosTask`
- callback / stream
- pandas / polars 形状兼容
- 本地伪造订单状态的 overlay

### `tqsdk-stream`
`tqsdk-stream` 是对同一批 diff-backed 能力的 stream 风格包装。

它的职责应当是：

- 从 `tqsdk-core` commit / revision 语义导出异步流
- 提供按对象 / 按路径 / 按域的 stream 消费
- 可选引入背压与丢弃策略
- 可对订单/成交类对象保留可靠事件流

但它不应：

- 绕开 `tqsdk-core` 自建另一套更新语义
- 直接持有 raw websocket / raw diff 层能力
- 吸纳 GraphQL / HTTP query、schema refresh、metadata query 这类一次性接口

## API 归属清单

下面这张表是后续实现 `tqsdk-stream` 与高层工具时的硬约束：

| API 形态 | 典型接口 | 应归属的 crate |
| --- | --- | --- |
| 一次性 direct query | `query_graphql`、`query_* metadata`、`refresh_schema` | `tqsdk-session` |
| 一次性 metadata 结果 | 交易日历、`SymbolSettlement`、`SymbolRanking` | `tqsdk-session` |
| wait 风格持续状态消费 | `get_quote`、`get_kline_serial`、`get_account`、`insert_order` | `tqsdk-wait` |
| stream 风格持续状态消费 | quote/kline/tick/account/order/trade 的 stream / event API | `tqsdk-stream` |
| 高级任务/工具 | downloader、`TargetPosTask`、scheduler、DataFrame/polars | 独立高级工具 crate |

这张表背后的原则只有一条：

- “是不是持续依赖同一棵状态树的后续 commit 才成立”
  - 如果是，就属于 `wait` / `stream`
  - 如果不是，只是一次 `await` 请求/响应，就属于 `tqsdk-session`

再具体到 Python / 现有 Rust 参考实现里的方法名，建议分成下面三档：

### 应优先补进 `tqsdk-session` 的 thin wrappers

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

### 已有 raw substrate，暂时可只保留原始入口

- `query_graphql`
- `refresh_schema`

原则是：如果某类查询的稳定返回形状、过滤参数和 typed contract 还没完全收敛，就先停留在 raw `query_graphql` / `refresh_schema`，不要急着在 facade 层做半成品 convenience API。

### 不应放进 `tqsdk-session` 的研究/工具层接口

- `query_his_cont_quotes`
- `query_option_greeks`
- DataFrame / polars 兼容包装

这些接口已经不只是“薄的 request/response 包装”，而是在 direct query 结果之上叠加了研究工作流与派生计算语义。

### 高级工具层
这些能力属于独立高级工具 crate，而不属于 `wait`/`stream`：

- downloader
- `TargetPosTask`
- 调仓器 / scheduler
- dataframe / polars 集成
- 报表 / GUI / helper 工具

原因很简单：它们不是“diff 协议对象的用户消费形状”，而是建立在消费形状之上的更高层工具。

## `tqsdk-wait` 公开模型

### 入口对象
`tqsdk-wait` 首版公开一个单入口对象：

- `TqApi`

它内部持有：

- `SessionClient`（来自 `tqsdk-session`）
- `RuntimeReader`
- 自己的 `UpdateCursor`
- facade 级本地 bookkeeping
  - `active_commands`
  - `deferred_commits`
  - `last_commit`
  - `last_diagnostic`

### 句柄模型
所有 `Ref` 都是轻量句柄：

- `QuoteRef`
- `TradingStatusRef`
- `KlineSerialRef`
- `TickSerialRef`
- `AccountRef`
- `PositionRef`
- `OrderRef`
- `TradeRef`

共同约束：

- 只保存对象路径 / object key / 共享读上下文
- 不持有 task
- 不持有 channel
- 不持有 watcher
- 不拥有状态本体

### 窗口模型
serial 类型对象不直接暴露 DataFrame 语义，而是先做 Rust 原生窗口视图：

- `KlineWindow`
- `TickWindow`

提供：

- `len`
- `is_empty`
- `last`
- `get`
- `iter`
- chart/window 元信息

## `tqsdk-wait` 的关键语义

### `wait_update()`
`wait_update(deadline)` 的职责是推进一次 facade 可见 commit。

一次调用按以下顺序工作：

1. 先吐出 `deferred_commits`
2. 再 `flush_outbound`
3. 再收远端输入并 `ingest`
4. 一旦 `RuntimeReader + UpdateCursor` 消费到一条新的 `CommitResult`，返回 `Ok(true)`
5. 到达 deadline 仍无新 commit，则返回 `Ok(false)`

关键约束：

- `wait_update()` 返回 `true` 的条件是“有新的 commit 被用户看见”
- 不要求一定是行情更新
- trade / session / query / schema 只要进入同一棵状态树，也算有效更新

### 内部等待不得吞掉用户更新
像 `get_kline_serial()` 这种需要先等待初始 ready 的方法，内部消费到的 commit 必须进入 `deferred_commits`，由之后的外部 `wait_update()` 再按顺序暴露给用户。

这样可以保证：

- facade 内部 helper 不会悄悄吞掉变化
- `is_changing()` 始终只解释“最后一次对用户可见的 commit”

### `is_changing()`
`is_changing()` 只解释最后一次成功暴露给用户的 commit。

其实现建立在 `CommitResult.changes` 之上：

- 对象级命中：`object_hits`
- 路径级命中：`path_hits`
- 字段级命中：`field_hits`

不重新保存 diff，也不重扫整棵状态树。

### timeout
timeout 不是错误，而是：

- `Ok(false)`

只有真正的 transport / auth / contract 失败才返回 `Err(...)`。

### ready
`ready` 语义按对象类型定义，而不是搞一个模糊的全局 ready：

- `QuoteRef`
  - quote 节点已有服务端有效数据
- `KlineSerialRef`
  - chart `ready=true`
  - `more_data=false`
  - 且请求窗口内有可读 bar
- `TickSerialRef`
  - 窗口已建立且至少有可读 tick
- `AccountRef` / `PositionRef` / `OrderRef` / `TradeRef`
  - 对应对象路径已存在且能 decode

### 并发约束
同一个 `TqApi` 上不允许并发 `wait_update()`。

原因不是实现方便，而是语义上 `tqsdk-wait` 只能有一个外部推进点。

允许并发：

- `Ref::snapshot()`
- `Ref::load()`
- `Ref::is_ready()`

不允许并发：

- `TqApi::wait_update()`

## `tqsdk-wait` 的 API 同步/异步分界

### 同步方法
只读当前状态树，不推进远端：

- `quote_ref`
- `kline_ref`
- `tick_ref`
- `account_ref`
- `position_ref`
- `order_ref`
- `trade_ref`
- `last_commit`
- `is_changing`

以及各类 `Ref` 的：

- `snapshot`
- `load`
- `is_ready`

### 异步方法
需要提交命令、驱动远端或等待初始 ready：

- `get_quote(...).await`
- `get_trading_status(...).await`
- `get_kline_serial(...).await`
- `get_tick_serial(...).await`
- `insert_order(...).await`
- `cancel_order(...).await`
- `confirm_settlement(...).await`
- `wait_update(...).await`
- `close().await`

## `get_*` / trade API 的具体语义

### `get_quote(...).await`
- 提交 quote 订阅命令
- 返回 `QuoteRef`
- 不等待第一笔行情

### `get_trading_status(...).await`
- 提交 trading status 订阅命令
- 返回 `TradingStatusRef`
- 不等待第一笔状态

### `get_kline_serial(...).await`
- 提交 `SetChart`
- 等待初始窗口 ready 后返回
- 内部消费到的 commit 进入 `deferred_commits`

### `get_tick_serial(...).await`
- 与 `get_kline_serial` 同语义

### `get_account/get_position/get_order/get_trade`
- 纯句柄获取
- 不隐式查询
- 不额外等待

### `insert_order(...).await`
- 只负责向 core 提交命令
- 返回 `OrderRef`
- 不做 Python 式本地预填充订单 overlay
- 真正发送发生在下一次 `wait_update()`

### `cancel_order(...).await`
- 只负责提交撤单命令
- 真正发送发生在下一次 `wait_update()`

### `confirm_settlement(...).await`
- 同样只是提交命令

## 模块划分建议

### `tqsdk-session`

```text
crates/tqsdk-session/
  Cargo.toml
  README.md
  src/
    lib.rs
    builder.rs
    client.rs
    query.rs
    schema.rs
    market.rs
    transport.rs
    error.rs
```

### `tqsdk-wait`

```text
crates/tqsdk-wait/
  Cargo.toml
  README.md
  src/
    lib.rs
    builder.rs
    api.rs
    driver.rs
    change.rs
    error.rs
    refs/
      mod.rs
      quote.rs
      kline.rs
      tick.rs
      trading_status.rs
      trade.rs
    views/
      mod.rs
      kline_window.rs
      tick_window.rs
  tests/
    wait_api_surface.rs
    wait_api_market.rs
    wait_api_trade.rs
    wait_api_is_changing.rs
```

关键边界：

- `api.rs + driver.rs`
  - 单 owner 驱动逻辑
- `refs/* + views/*`
  - 只读消费层
- `change.rs`
  - 变化解释逻辑

## 验收标准

### `tqsdk-session`
完成标准：

- 能建立 live session
- 能对外提供 direct query / schema / metadata 接口
- 不暴露 `wait_update` / stream 风格方法
- 能被 `tqsdk-wait` 和 `tqsdk-stream` 共同依赖

### `tqsdk-wait`
首版完成标准：

- 基于单 `TqApi` 驱动一个 live market + trade session
- `wait_update()` 行为符合“单推进点 + 单稳定截面”
- `is_changing()` 直接解释最近一次用户可见 commit
- `get_quote()`、`get_kline_serial()`、`get_tick_serial()` 跑通
- `get_account()/get_position()/get_order()/get_trade()` 跑通
- `insert_order()/cancel_order()/confirm_settlement()` 跑通
- 内部 helper 消费到的 commit 不会被吞掉

### `tqsdk-stream`
完成标准：

- 不回改 `tqsdk-core`
- 只从同一个 commit/revision 语义导出 stream
- 可按对象/按域做异步消费

## 风险与应对

### 风险 1
如果把 direct query 又塞回 `tqsdk-wait`，会重新把模式无关接口绑死在 wait 语义里。

应对：

- 在 crate 边界上强制区分 `tqsdk-session` 和 `tqsdk-wait`

### 风险 2
如果 `tqsdk-wait` 为了图省事在内部再造本地状态 overlay，会破坏“状态唯一来源是 core”的原则。

应对：

- 首版明确不做订单预填充 overlay

### 风险 3
如果 `tqsdk-stream` 直接复用 raw watcher/epoch 模型而不是 core commit，后续两种 facade 会产生语义漂移。

应对：

- 强制 stream 只从 `tqsdk-core` commit/revision 读面导出

## 最终结论

这次设计收敛后的核心结论是：

1. `tqsdk-core` 继续只做 substrate
2. direct query / schema / metadata 放进共享薄层 `tqsdk-session`
3. `tqsdk-wait` 和 `tqsdk-stream` 只处理 diff-backed 状态消费形状
4. downloader、`TargetPosTask` 等能力保持为更高层独立工具 crate

这能同时满足三个目标：

- 保住 `tqsdk-core` 的稳定与高性能
- 让 `tqsdk-wait` 和 `tqsdk-stream` 真正只是便利包装
- 避免模式无关接口被错误地绑进某一种 facade 风格
