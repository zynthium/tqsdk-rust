# 未来 Crate 蓝图与能力映射

## 文档定位
本文档给出未来继续对齐 `tqsdk-python` 与现有 `tqsdk-rs` 能力时，推荐的 crate 版图和能力落点。

目标不是把所有功能都提前拆出来，而是先锁定未来新增能力应该落到哪里，避免后续实现时污染已经稳定的 `tqsdk-core` / `tqsdk-session` / `tqsdk-wait`。

## 先给结论

当前三层应保持不动：

- `tqsdk-core`
- `tqsdk-session`
- `tqsdk-wait`

未来建议按需要继续扩展成下面这组 crate：

1. `tqsdk-core`
2. `tqsdk-session`
3. `tqsdk-wait`
4. `tqsdk-stream`
5. `tqsdk-task`
6. `tqsdk-data`
7. `tqsdk-backtest`（可选）
8. `tqsdk-callback`（可选）

其中：

- 前 3 个是当前已经稳定的基础层
- `tqsdk-stream` 是最明确的下一层
- `tqsdk-task` 与 `tqsdk-data` 是承接 Python / 现有 Rust 高层能力外溢的主要位置
- `tqsdk-backtest` 与 `tqsdk-callback` 是否独立，取决于后续实际复杂度

## 推荐依赖方向

```text
tqsdk-core
    ^
    |
tqsdk-session
    ^        ^
    |        |
tqsdk-wait  tqsdk-stream
    ^        ^        ^
    |        |        |
    |     tqsdk-callback
    |        ^
    |        |
    +---- tqsdk-task
    |
    +---- tqsdk-data
             ^
             |
       tqsdk-backtest
```

说明：

- `tqsdk-stream` 与 `tqsdk-wait` 是并列消费 facade
- `tqsdk-task` 依赖 diff-backed facade，但不应反向进入底层
- `tqsdk-data` 偏离线与研究工具，依赖底层 query / replay / history 能力
- `tqsdk-backtest` 如果独立，应站在 `core/session + wait/stream + data` 之上

## 各 crate 的推荐边界

## `tqsdk-core`

继续只负责：

- protocol-complete runtime contract
- command / input / mutation / commit
- state tree / revision / change model
- auth / bootstrap / transport / session runtime
- typed schema contract

永远不建议进入：

- `wait_update()`
- stream / callback
- direct query convenience
- downloader
- task runtime
- DataFrame / polars

## `tqsdk-session`

继续只负责：

- shared session owner
- one-shot control plane helper
- one-shot request/response API
- metadata / schema / service query

后续仍适合继续进入：

- 更多 direct query typed wrapper
- 更多 auth / replay / schema 一次性 helper

不应进入：

- live object refs
- stream / callback
- downloader
- task orchestration

## `tqsdk-wait`

继续只负责：

- 单 owner `wait_update()` facade
- diff-backed live object refs
- wait 风格命令包装
- commit-bound `is_changing()` 解释

后续适合继续进入：

- `PreInsertOrderRef`
- `RiskManagementRuleRef`
- `RiskManagementDataRef`
- `NotificationRef`
- `SettlementInfoRef`
- security 系列 refs

不应进入：

- query/schema/metadata direct facade
- task runtime
- downloader / dataframe
- callback / stream fan-out

## `tqsdk-stream`

这是当前最明确的下一层。

它应负责：

- diff-backed live object 的异步 stream 消费
- 多消费者等待点
- 按对象 / 按协议域 / 按路径的 stream facade
- 背压和订阅生命周期管理
- 可靠事件流与状态流分层

它不应负责：

- direct query / schema / metadata
- downloader
- 目标持仓任务
- DataFrame / polars

它的设计应吸收现有 `tqsdk-rs` 的工程经验，但不得重定义 `tqsdk-core` 的 commit 语义。

## `tqsdk-task`

这个 crate 用来承接“持续消费状态 + 持续发命令 + 维护内部任务状态”的能力。

应放入：

- `TargetPosTask`
- 调仓 scheduler
- 任务 ownership / symbol ownership
- 执行规划器
- quote hint / offset priority / volume split policy
- 与 task 相关的执行报告

不应放入：

- 纯协议 contract
- 纯 direct query
- DataFrame / 离线研究工具

原因：

- 这类能力已经不是 facade 本身，而是建立在 facade 之上的执行工具层

## `tqsdk-data`

这个 crate 用来承接离线/研究/批处理数据能力。

应放入：

- downloader
- 历史数据批量拉取
- `get_kline_data_page`
- `get_tick_data_page`
- `get_kline_data_series`
- `get_tick_data_series`
- `kline_data_download`
- `tick_data_download`
- `query_his_cont_quotes`
- `query_option_greeks`
- `export_kline_data_csv`
- `export_tick_data_csv`
- pandas/polars/DataFrame 兼容层
- 文件落盘、缓存、导出

不应放入：

- live session owner
- wait/stream continuous facade
- task runtime

原因：

- 这些能力面向研究与数据处理工作流，而不是高性能 substrate

## `tqsdk-backtest`

是否独立取决于后续复杂度。

如果未来回放/回测只需要：

- replay control
- wait/stream 读取

那么保持 replay contract 在 `core/session` 即可。

如果未来还要继续扩展：

- 回测执行编排
- 回测报告
- 指标统计
- 资金曲线
- 策略结果归档

那么建议独立为 `tqsdk-backtest`。

这个 crate 应建立在底层 replay contract 之上，而不是把 replay 协议重新实现一遍。

## `tqsdk-callback`

如果后续确实需要 UI / 监控 / handler-style 集成，可以独立存在。

适合进入：

- callback / handler facade
- bridge to GUI / observer systems

不适合进入：

- query
- downloader
- task runtime

如果 callback 需求很弱，也可以作为 `tqsdk-stream` 的薄包装，而不是马上独立。

## 按能力分桶映射 `tqsdk-python`

下面按 `tqsdk-python/tqsdk/api.py` 的公开能力做目标映射。

## A. 协议与底层 substrate

目标 crate：

- `tqsdk-core`
- `tqsdk-session`

能力：

- DIFF merge
- auth / bootstrap / session lifecycle
- replay 推进
- query / schema 原始交互

## B. 一次性 direct query / metadata / service

目标 crate：

- `tqsdk-session`

能力：

- `query_graphql`
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

## C. diff-backed continuous consumption

目标 crate：

- `tqsdk-wait`
- `tqsdk-stream`

能力：

- `get_quote`
- `get_quote_list` 的 live quote 语义
- `get_trading_status`
- `get_kline_serial`
- `get_tick_serial`
- `get_account`
- `get_position`
- `get_order`
- `get_trade`
- `get_risk_management_rule`
- `get_risk_management_data`
- `insert_order`
- `cancel_order`
- `set_risk_management_rule`

说明：

- wait/stream 只是消费形状不同
- direct query 不因消费形状改变归属

## D. 任务与执行工具

目标 crate：

- `tqsdk-task`

能力：

- `TargetPosTask`
- 多账户调仓任务
- 各类自动执行 task

## E. 数据研究与离线工具

目标 crate：

- `tqsdk-data`

能力：

- `get_kline_data_series`
- `get_tick_data_series`
- `kline_data_download`
- `tick_data_download`
- `query_his_cont_quotes`
- `query_option_greeks`
- `export_kline_data_csv`
- `export_tick_data_csv`
- pandas/DataFrame 相关对象
- 历史数据下载和导出

## F. GUI / drawing / web helper

目标：

- 不进入当前底座主线
- 后续如确有需要，独立为 integration crate

能力：

- `draw_text`
- `draw_line`
- `draw_box`
- `draw_report`
- `web_gui`

## 按能力分桶映射现有 `tqsdk-rs`

现有 `tqsdk-rs` 的 public surface 很宽，未来对齐时不应原样复制，而应按语义重新落位。

## 应归入 `tqsdk-wait` / `tqsdk-stream`

- `Client` 的 live market facade
- quote / kline / tick live subscriptions
- `TradeSession` 的 live 状态消费能力
- 订单/成交/通知的可靠事件流语义

## 应归入 `tqsdk-task`

- `TqRuntime`
- `AccountHandle`
- `TargetPosTask`
- scheduler
- runtime task registry / ownership
- execution adapter 级工具

## 应归入 `tqsdk-data`

- `DataDownloader`
- `polars_ext`
- DataManager 的研究/缓存/离线路径

## 应保留在底层

- 协议、typed object、route/session/bootstrap 语义

## 推荐的增长顺序

建议按照下面顺序扩张，而不是并行膨胀：

1. 稳定当前 `core/session/wait`
2. 补 `tqsdk-stream`
3. 补 `tqsdk-task`
4. 补 `tqsdk-data`
5. 视复杂度决定是否独立 `tqsdk-backtest`
6. 最后再看 `tqsdk-callback`

原因：

- `tqsdk-stream` 直接决定当前底座能否同时承载 Python 风格与 Rust 风格
- `tqsdk-task` 是高层执行工具最明确的承接点
- `tqsdk-data` 是研究与下载类能力的主要外溢点
- `tqsdk-backtest` 与 `tqsdk-callback` 的独立价值要等前面几层稳定后再判断

## 当前总建议

未来不要再围绕“一个全能 `TqApi`”组织 crate，也不要围绕“一个宽而全的 `tqsdk-rs` 根导出”组织 crate。

正确方向是：

- 底层只保留 `core + session`
- continuous-consumption 按 `wait` / `stream` 分形状
- task、data、backtest、callback 都作为上层独立能力继续长

这样既能对齐 `tqsdk-python` 的语义基准，也能吸收现有 `tqsdk-rs` 的工程经验，同时不破坏你现在这层高性能底座。
