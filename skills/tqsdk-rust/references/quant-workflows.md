# 量化工作流

需要按角色或场景做宽覆盖时，读取 `scenario-contracts.md`，并引用对应正式 example。

## Live Monitoring

普通单策略循环优先使用 `tqsdk`：通过 `Tq::futures()` 构造默认 facade，用 `quote` / `quotes_universe`、`target_pos_tqkq` 和 `next()` 写策略主体。需要维护指定合约或 selector 集合的回测共享 tick cache 时，优先用 `MarketCachePolicy::new(cache_dir).record_ticks(symbols)` 或 `.record_universe(expression)?` + `.market_cache(policy)`，让 live recording 和 cache-backed backtest 复用同一份配置；运行中临时开启可显式调用 `record_ticks(cache_dir, symbols)`。首次初始化或失败重扫之外，recording 在每个更新只收集变更集命中的 rows；连续 rows 每 symbol 按 `128` 行或约 `250 ms` 批量落盘，首批、跳号和正常 `Tq` 销毁会 flush；异常退出的未提交尾部会保持 coverage 缺口。明确需要 Python-style `WaitStep::is_changing()`、serial/status wait handles 或 notebook-like wait facade 时使用 `tqsdk-wait`。Ref 是 live handle；snapshot 要在 commit 后加载。

契约锚点：S33、S46-S47、S1、S3、S8-S10、S25-S26。

## Event Pipeline

多个独立 consumer 需要同一份 session state 时使用 `tqsdk-session + RuntimeReader/UpdateCursor` 作为 substrate：logging、metrics、signal calculation、persistence、order monitoring。调用方自建 commit filter、typed events、bounded channel 和 lag diagnostics，不要在每个 consumer 里 clone snapshot。指定 symbol 或 selector 集合的 live tick 持久化优先用 `MarketCachePolicy` / `Tq::record_ticks(...)` 或数据层 `LiveTickCacheWriter`；泛化 commit/event/K 线持久化仍使用调用方自有 sidecar。

契约锚点：S5、S23、S27、S31；S2、S4、S21、S22、S35 已删除为调用方层能力。

## One-Shot Research Query

返回单个结果的 metadata 和 service call 使用 `tqsdk-session`。需要时启用 query support；在 wait facade 或调用方 event consumer 中复用 session，不要创建重复连接。不要因为 Python 在一个 `TqApi` 上暴露很多 helper，就把 symbol metadata 路由到 live `QuoteRef`。

契约锚点：S23、S27。低层 live substrate：S5。

## Historical Research

用户明确要按区间获取历史 Tick/Kline，且优先走回测或回测缓存时，先用 `tqsdk::advanced::data::BacktestHistoryClient`。`RemoteOnMiss` 先读 shared backtest cache，缺口才通过官方 server-side backtest stream 获取并持久化；`CacheOnly` 用于后续离线 reader。它返回 owned chunk/rows，不是 live ref，也不替代策略 `.backtest(...)` 回放。只有需要 generic history page/series/download、CSV export、option Greeks，或回测缓存合同不覆盖的来源时，才用 `tqsdk-data::DataClient`。`HistorySeriesCache` 是 offline `data_series` cache 和 cache-only reader，不是 live 最新行情 API；`BacktestTickCache` 是 tick-only 回测共享缓存，普通 live/backtest 组合入口在 `tqsdk::MarketCachePolicy`，纯 row writer 才下钻 `LiveTickCacheWriter`。

用户明确要 shell/CLI 导出或 token-aware 模型行情输入时，改用可选 `tqsdk-cache query`：它复用同一
`BacktestHistoryClient` 合同，`jsonl` 给无损 rows，`llm-csv` 给已验证 coverage/session metadata 的紧凑
blocks；先由 `scenario-router.md` 选择 `cache-only` 或 `remote-on-miss`，不要手读 `.tqbn`。

契约锚点：S17、S28-S30。Replay integration：S16；S18 JSONL local market cache 已移出当前核心 SDK public API。

## Strategy Execution

普通 target-position path 使用 `tqsdk` 的 `TargetPos` wrapper。需要 execution ownership、order internals、risk checks、schedulers、multi-account allocation、strategy context、replay 和 fake broker tests 时使用 `tqsdk-task`。优先使用 typed builders 和 typed tickets。让 `TaskHost` 拥有 wait loop。没有 ownership 的一次性 order wrapper 可以用 `tqsdk-wait`，但必须说明 live-order 副作用。

契约锚点：S6-S13、S19、S29。Production lifecycle：S15、S20。

## Low-Latency Desk Loop

hot path 使用 `tqsdk-session + RuntimeReader`，或使用 `tqsdk-task` trading desk profile。一个决策需要同 revision 的 market 和 trade partition 时，用 `read_market_trade_state()`。慢日志或持久化使用调用方 event sidecar。

契约锚点：S5、S31。

## Replay and Testing

离线 deterministic event source 使用 `tqsdk-task::ReplayMarketSource`；历史 rows 可由 `tqsdk-data` 拉取后通过 `StrategyReplaySourceBuilder` 转成 replay source。确定性策略测试使用 `tqsdk-task` replay/fake broker tools。除非用户明确要求 integration smoke test，否则 unit-level strategy test 不应需要 live credentials。live smoke test 保持 ignored 或环境变量门控。

契约锚点：S15-S16、S24、S30；S18 JSONL local market cache 已移出当前核心 SDK public API。

## Backtest

官方 Python 回测心智是 `TqApi(account=TqSim(), backtest=TqBacktest(...))`：策略主体继续围绕 live refs 编写，回测配置只在构造阶段切换。Rust 有三条入口要分清：

- 普通策略优先使用默认 `tqsdk` facade。`.backtest(start_ns, end_ns)` 是统一回测入口，默认共享持久 tick cache + `RemoteOnMiss`：已知或静态解析 symbol 的完整缓存本地回放且不需 auth，缺口才通过官方 server-side backtest 流补齐并写入 cache。`.cache_dir(...)` 或 `.market_cache(policy)` 覆盖默认 root；预热作业使用 `.remote_on_miss().warmup()`，预热后的策略使用 `.cache_only()`；只有 `.disabled_cache()` 才禁用持久缓存并直接使用官方行情。`.replay_backtest(replay)` 走 caller-owned replay source + `TqSim`。不要再使用或迁移到 `server_backtest(...)`；旧代码应替换成默认 `.backtest(...)`，仅在确实需要纯远端流时显式 `.disabled_cache()`。策略主体保持 `Tq::next()` / `quote()`，live/backtest 差异留在 builder。契约锚点：S37-S39、S43-S47。
- `tqsdk-wait` 的 `TqApiBuilder::{futures_backtest,stock_backtest}` / `TqBacktest` 用于明确要求直接操作 Python-style wait facade 的 same-body loop。策略主体只依赖 `quote` / `kline` handles 和 `step()`，回测结束时 `step()` 返回 `None`。契约锚点：S36。
- `tqsdk-task` 的 `StrategyBacktest + TqSim` 消费 task-owned `ReplayMarketSource`，用于不连接真实服务的本地确定性回测模拟账户内部能力；历史 rows 可由 `tqsdk-data` 拉取并通过 `StrategyReplaySourceBuilder` 转成 replay source。当前覆盖 quote/tick/kline replay event、futures 单账户、基础限价/市价撮合、保证金和手续费配置、kline `price_tick(...)` quote synthesis 和轻量 `summary()`；完整回测报告、自动分钟线、主连合约表、股票/期权完整账户语义还不是当前最小闭环。契约锚点：S32。

不要把“回测策略程序”只路由成 `tqsdk-data` 历史下载；`tqsdk-data` 在这里负责历史 rows，不负责 replay source、策略执行或模拟账户撮合。
