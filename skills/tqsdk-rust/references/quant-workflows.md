# 量化工作流

需要按角色或场景做宽覆盖时，读取 `scenario-contracts.md`，并引用对应正式 example。

## Live Monitoring

单策略循环或 notebook-like live monitoring 使用 `tqsdk-wait`。通过 `get_quote`、`get_trading_status`、`get_kline_serial` 或 `get_tick_serial` 订阅，然后调用 `wait_update()`；只有 `is_changing()` 表示相关 commit 后才加载 ref。Ref 是 live handle；snapshot 要在 commit 后加载。

契约锚点：S1、S3、S8-S10、S25-S26。

## Event Pipeline

多个独立 consumer 需要同一份 session state 时使用 `tqsdk-stream`：logging、metrics、signal calculation、persistence、order monitoring。使用 commit filter 或 typed stream，不要在每个 consumer 里 clone snapshot。

契约锚点：S2、S4、S20-S22。

## One-Shot Research Query

返回单个结果的 metadata 和 service call 使用 `tqsdk-session`。需要时启用 query support；在 wait/stream facade 中复用 session，不要创建重复连接。不要因为 Python 在一个 `TqApi` 上暴露很多 helper，就把 symbol metadata 路由到 live `QuoteRef`。

契约锚点：S23、S27。低层 live substrate：S5。

## Historical Research

history pages、time-range series、pull-based downloads、CSV export 和 option Greeks 使用 `tqsdk-data`。historical materialization 要和 live refs 分开。大规模重复读取时显式使用 history cache。

契约锚点：S17-S18、S28-S30。Replay integration：S16。

## Strategy Execution

target-position execution、order ownership、risk checks、schedulers、multi-account allocation、strategy context、replay 和 fake broker tests 使用 `tqsdk-task`。优先使用 typed builders 和 typed tickets。让 `TaskHost` 拥有 wait loop。没有 ownership 的一次性 order wrapper 可以用 `tqsdk-wait`，但必须说明 live-order 副作用。

契约锚点：S6-S13、S19、S29。Production lifecycle：S15、S20。

## Low-Latency Desk Loop

hot path 使用 `tqsdk-session + RuntimeReader`，或使用 `tqsdk-task` trading desk profile。一个决策需要同 revision 的 market 和 trade partition 时，用 `read_market_trade_state()`。慢日志或持久化使用 stream sidecar。

契约锚点：S5、S31。

## Replay and Testing

离线 event source 使用 `tqsdk-data` market cache records；确定性策略测试使用 `tqsdk-task` replay/fake broker tools。除非用户明确要求 integration smoke test，否则 unit-level strategy test 不应需要 live credentials。live smoke test 保持 ignored 或环境变量门控。

契约锚点：S15-S16、S18、S24、S30。
