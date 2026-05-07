# 场景路由

先使用本文件。按用户想持有或消费的对象分类。Python TqSdk 是语义参考，但 Rust 会把 Python 单体 `TqApi` surface 拆到不同 crate。

## 目录

- 快速决策
- 各场景调用模式
- 仍不明确时

## 快速决策

| 用户说 | 大概率需要 | Crate | 主要调用 |
| --- | --- | --- | --- |
| "实时行情", "quote", "盘口", "价格变化", "像 Python TqApi", "wait_update", "is_changing" | Single-owner live quote/trade loop | `tqsdk-wait` | `TqApiBuilder`, `get_quote`, `wait_update`, `is_changing`, `QuoteRef::load` |
| "K线 serial", "tick serial", "窗口", "bar 更新", "trading_status" | Live serial/window/status view | `tqsdk-wait` | `get_kline_serial`, `get_tick_serial`, `get_trading_status`, `wait_update`, window/ref load methods |
| "多消费者", "事件流", "stream", "fan-out", "写 WAL", "异步管道", "lag" | Multi-consumer event pipeline | `tqsdk-stream` | `TqStreamBuilder`, `commit_stream`, filters, `quote_stream`, `market_events`, trade/session event streams, sink APIs |
| "查合约", "查品种", "合约列表", "所有合约代码", "主连", "连续合约", "期权链", "交易日历", "结算价", "排名", "EDB", "schema", "metadata" | One-shot metadata/service query | `tqsdk-session` | `SessionClientBuilder`, `enable_query`, `query_quotes`, `query_instrument_specs`, `query_cont_quotes`, `get_trading_calendar` |
| "下单", "撤单", "目标持仓", "调仓", "策略下单", "风控", "scheduler", "多账户", "fake broker" | Strategy execution layer | `tqsdk-task` when ownership/risk/task semantics are needed; `tqsdk-wait` for thin direct order wrappers | `TaskHost`, `TargetPosTask`, `RiskEngine`, typed order builders, `OrderTicket`, strategy/test harness APIs |
| "历史K线", "历史 tick", "下载", "CSV", "离线研究", "缓存", "回放", "Greeks", "data_series" | Historical/offline research | `tqsdk-data` | `DataClient`, `get_*_data_series`, `*_data_download`, `export_*_csv`, cache/replay APIs |
| "低延迟", "同一 revision", "cursor", "commit", "runtime", "adapter", "command status" | Low-level substrate or custom facade | `tqsdk-session` plus `tqsdk-core` | `SessionClient`, `progress_once`, `RuntimeReader`, `cursor`, `read_market_trade_state` |

请求涉及角色覆盖或 public API 证据时，继续读 `references/scenario-contracts.md`，并把回答锚定到对应 `api_contract_sXX_*.rs` 示例。

## 各场景调用模式

### 1. 在策略循环中监控一个合约

使用 `tqsdk-wait`。

调用顺序：

1. 用凭证构造 `TqApi`。
2. 调用 `get_quote(symbol).await`。
3. 循环调用 `wait_update(None).await?`。
4. 使用 `is_changing(&quote)?`。
5. 只有相关变化后才加载 quote snapshot。把 ref 当作 live handle，不要当作 owned snapshot。

继续读：`references/code-patterns.md` 的 Wait Quote Loop 示例。

### 2. 策略启动前查询合约 metadata

使用 `tqsdk-session`。

调用顺序：

1. 用正确 market route 构造 `SessionClient`。`SessionClientBuilder::build()` 是同步的。
2. 需要官方 query 语义时添加 `enable_query()`。
3. 调用具体 one-shot helper。
4. 如果已经在 wait/stream 中，复用 `api.session()` 或 `stream.session()`。

典型 helper：`query_symbol_info`、`query_instrument_specs`、`query_quotes`、`query_cont_quotes`、`query_options`、`query_atm_options`、`query_all_level_options`、`query_all_level_finance_options`、`get_trading_calendar`、`query_symbol_settlement`、`query_symbol_ranking`、`query_edb_data`。

品种/合约查询映射：

- 某交易所某品种的合约代码列表：`query_quotes(Some("FUTURE"), Some("SHFE"), Some("au"), Some(false), None)`。
- 主连/连续合约：`query_cont_quotes(Some("SHFE"), Some("au"), None)`。
- 已知合约的规格字段：`query_instrument_specs(&symbols)`；底层 metadata 表则用 `query_symbol_info(&symbols)`。
- 期权链：`query_options(underlying, &OptionQueryFilter::new())`；按 ATM 或档位查询用 `query_atm_options`、`query_all_level_options`、`query_all_level_finance_options`。

### 3. 构建 live data bus

使用 `tqsdk-stream`。

调用顺序：

1. 构造 `TqStream`。
2. 订阅或选择 typed stream/event API。
3. 能过滤时使用 commit/path/scope/domain/object/field filters。
4. 慢持久化放在 stream sink 或 sidecar。
5. metadata 使用 `stream.session()`，不要另开 query client。
6. 显式处理 lag/closed/error report；fan-out 是 bounded。

### 4. 实现 target-position 策略

使用 `tqsdk-task`。

调用顺序：

1. 从 `tqsdk-wait` 构造 `TqApi`。
2. 包装成 `TaskHost`。
3. 需要时配置 `RiskEngine`。
4. 创建 `TargetPosTask` 或 typed order builder。
5. 让 `TaskHost::wait_update()` 推进 task。
6. 使用 typed tickets/reports，不要解析 status 字符串。
7. real-account order placement 必须 opt-in；示例要显式展示副作用。

### 5. 下载历史数据用于研究或回测输入

使用 `tqsdk-data`。

调用顺序：

1. websocket-backed history 需要 session 时，构造或复用 session。
2. 创建 `DataClient` 或 `DataClient::from_session(session)`。
3. 选择 page、series、download、CSV export、cache 或 replay API。
4. 输出保持 owned/materialized；不要建模成 live refs。
5. 确定性策略测试尽量用 cache/replay API，而不是 live credentials。

### 6. 编写低延迟自定义循环

使用 `tqsdk-session + RuntimeReader`；如果任务偏 execution，用 `tqsdk-task` trading desk profile。

调用顺序：

1. 构造 `SessionClient`，并显式 subscribe/login。
2. 用 `progress_once` 或 profile loop primitive 推进。
3. 用 `RuntimeReader::cursor()` / `next()` 消费 commits。
4. 通过 `read_market_state`、`read_trade_state` 或 `read_market_trade_state` 读取 hot state。
5. logging、journal、metrics 不要进入 hot decision path。
6. 不要在 runtime mutation path 之外修改 domain state。

## 仍不明确时

只问一个按形状分类的问题，而不是实现细节：

“你需要一个带 refs 的 live loop、multi-consumer event stream、one-shot metadata query、trading task abstraction，还是 historical/offline data？”
