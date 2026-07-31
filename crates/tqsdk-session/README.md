# `tqsdk-session`

共享的 session / direct-query 薄层。

这个 crate 负责把会话生命周期、route 驱动、schema / metadata / direct query 这类和具体 facade 无关的能力先抽出来，作为 `tqsdk-wait` 和调用方自建消费层的共同底座。

它不是只给 facade 内部复用的隐藏层。对需要“一次性 query / metadata / schema 访问”的用户，`tqsdk-session` 本身就是正确入口。

它同时保持一个明确约束：

- 它是纯 async substrate，不内置 runtime
- 调用方必须自己提供 Tokio runtime
- direct service helper（交易日历、结算价、排名、EDB）也要求当前已经处于 Tokio runtime 中

HTTP auth / direct-query client 明确走直连路径：内部 reqwest client 使用 `no_proxy()`
和 HTTP/1.1，不读取系统 proxy。少数网络环境下如果 Rust resolver 对官方域名解析不稳定，
可以显式注入直连 DNS 结果：

```bash
export TQSDK_DIRECT_RESOLVE_AUTH_SHINNYTECH_COM=<auth-ip>
export TQSDK_DIRECT_RESOLVE_API_SHINNYTECH_COM=<api-ip>
export TQSDK_DIRECT_RESOLVE_FILES_SHINNYTECH_COM=<files-ip>
```

这些变量只覆盖对应 host 的 reqwest 解析结果，不启用 proxy，也不影响 WebSocket route
选择。IP 应按运行环境实际解析结果设置。

## 依赖方式

Cargo 包名是 `tqsdk-session`，代码里的 crate 路径是 `tqsdk_session`。

正式发布到 crates.io 前，workspace 外项目可以先使用 Git dependency：

```toml
[dependencies]
tqsdk-session = { git = "https://github.com/zynthium/tqsdk-rust" }
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

在本仓库内做 crate 间开发时使用 `path = "../tqsdk-session"`；正式发布后把 Git
dependency 换成版本号即可。默认 feature 包含 live session 与 service query 支持。

它当前已经提供：

- `SessionClientBuilder`
- `SessionClient`
- lazy establish 的 live session owner
- `progress_once(deadline).await`
- `subscribe_quotes(...).await`
- `unsubscribe_quotes(...).await`
- `flush_outbound()`
- `drive_pending_once()`
- `drive_route_once()`
- `wait_command_completed(command_id).await`
- `command_state()`
- `command_status()`
- `command_status_typed()`
- `auth_context()`
- `refreshed_auth()`
- `has_feature(...).await`
- `check_md_grants(...).await`
- `replay_state()`
- `query_graphql(...).await`
- `query_graphql_value(...).await`
- `refresh_schema(...).await`
- `refresh_schema_value(...).await`
- `query_symbol_info(...).await`
- `query_instrument_specs(...).await`
- `query_quotes(...).await`
- `query_cont_quotes(...).await`
- `query_options(...).await`
- `query_atm_options(...).await`
- `query_all_level_options(...).await`
- `query_all_level_finance_options(...).await`
- `OptionQueryFilter`
- `AtmOptionQuery`
- `AllLevelOptionQuery`
- `FinanceOptionLevelQuery`
- `OptionLevelQuotes`
- `SymbolInfo`
- `InstrumentSpec`
- `InstrumentClass`
- `SessionRawQuery`
- `SessionMetadataQuery`
- `SessionServiceQuery`
- `SessionDirectQuery`
- `SessionFacadeError::diagnostic()`
- `SessionFacadeError::is_retryable()`
- `refresh_auth(...).await`
- `refresh_auth_value(...).await`
- `replay_step(...).await`
- `replay_step_value(...).await`
- `replay_reset(...).await`
- `replay_reset_value(...).await`
- `ServerReplayBuilder`
- `ServerReplaySession`
- `StartupRecoverySpec`
- `startup_recovery_status(...)`
- `OrderIntentRecord`
- `OrderIntentSpec`
- `OrderIntentRegistration`
- `remember_order_intent(...)`
- `update_order_intent_command(...)`
- `forget_order_intent(...)`
- `order_intent(...)`
- `get_trading_calendar(...).await`
- `query_symbol_settlement(...).await`
- `query_symbol_ranking(...).await`
- `query_edb_data(...).await`
- `SymbolRankingType`
- `EdbDataAlign`
- `EdbDataFill`

默认会预置官方静态文件域名 `https://files.shinnytech.com` 作为 schema/file-backed metadata 的基地址，因此文件型 schema/metadata 刷新不需要额外传入环境变量。

对于一次性的官方 metadata / query 服务，当前直接内置官方端点：

- 交易日历：`https://files.shinnytech.com/shinny_chinese_holiday.json`
- 结算价：`https://md-settlement-system-fc-api.shinnytech.com/mss`
- 持仓排名：`https://symbol-ranking-system-fc-api.shinnytech.com/srs`
- EDB：`https://edb.shinnytech.com/data/index_data`

对于官方单日复盘服务，`ServerReplayBuilder::new(user, pass, date)?.create().await`
会向 `replay.api.shinnytech.com` 创建 replay session，并返回
`ServerReplaySession` 中的 `session_url`、`instrument_url` 和 `market_url`。
默认 `tqsdk` facade 的 `.server_replay(date)?` 会用这个 `market_url` 接入正常
行情 loop，并在 facade 层自动发送 replay heartbeat。底层
`ServerReplaySession::set_speed(...)` / `heartbeat()` / `terminate()` 仍提供显式控制。

`ServerBacktestHistoryStream` 是同一 session 层提供给 data 的 server-backtest history chart
substrate：它只负责连接、chart 分页和 terminal signal，可读取 Tick 或 canonical 60s K。它不拥有
cache directory、coverage、metadata sidecar、缺口规划、K 线聚合或 retention；这些都归
`tqsdk-data::BacktestHistoryClient`。高周期和 sub-minute K 的本地派生也不应回流到 session。

交易日历的 holiday JSON 是官方公开静态文件，请求时不会携带天勤鉴权 token。
同一个 `SessionClient` 会缓存已解析的 holiday payload，重复
`get_trading_calendar(...)` 不会反复下载同一文件。返回的 `TradingCalendarDay.date`
是 `chrono::NaiveDate`，不再是字符串。

如需替换交易日历静态文件地址，可使用：

- 环境变量 `TQ_CHINESE_HOLIDAY_URL`
- `SessionClientBuilder::holiday_url(...)`

`query_graphql_value()` 与 replay 的 `*_value()` helper 只会在对应 domain 已启用时工作。query domain 现在可以承载在官方 `ins_query` websocket 链路上，也保留显式 HTTP query route 的定制能力。
`query_graphql_value()` 会在 `SessionClient` 内部串行化完整 query lifecycle，
因此通过它构建的 metadata helpers（例如 `query_symbol_info()`）可以在同一个
session facade 上并发调用。raw `query_graphql()` 只提交 command id；调用方如果手动
组合 `query_graphql()` / `wait_command_completed()`，仍需自行保证推进顺序。
如果要启用官方默认的 live query 语义而不显式覆盖 query endpoint，应调用 `SessionClientBuilder::enable_query()`。

`SessionClientBuilder` 还提供了命名明确的 market-target 快捷方法：

- `stock_market()`
- `futures_market()`
- `stock_backtest_market()`
- `futures_backtest_market()`
- `holiday_url(...)`
- `trade_target_tqkq()`
- `trade_target_tqkq_numbered(<1..99>)`
- `trade_target_tqkq_stock()`
- `trade_target_tqkq_stock_numbered(<1..99>)`

优先使用这些命名方法，而不是直接写 `market_target(bool, bool)` 这种裸布尔组合。

如果使用官方内置 `TqKq` / `TqKqStock` 账户，还可以在 session 已建立后直接生成对应登录命令：

- `session.tqkq_login_command().await`
- `session.tqkq_login_command_numbered(<1..99>).await`
- `session.tqkq_stock_login_command().await`
- `session.tqkq_stock_login_command_numbered(<1..99>).await`

这样上层 facade 或 example 不需要再额外手写一遍 `auth_id -> account_id/password` 的派生逻辑。

如果调用方需要在进入 live 行情订阅前做权限护栏，可以直接用：

- `has_feature("futr").await`
- `has_feature("sec").await`
- `check_md_grants(&["SHFE.au2606", "SSE.510300"]).await`

这样上层 crate 就不需要再各自重复一份权限判断逻辑。

它不直接定义高层 diff-backed 用户 API，也不把某一种消费风格硬编码进核心。

可以把它理解为：

- `tqsdk-session` 负责一次性 direct query / schema / metadata
- `tqsdk-wait` 负责 `wait_update()` 风格的持续状态消费
- 多消费者 event/fan-out 风格由调用方基于 `RuntimeReader` / `UpdateCursor` 自建

## 示例

最小可编译示例见：

- [examples/query_symbol_info.rs](examples/query_symbol_info.rs)
- [examples/query_command_wait.rs](examples/query_command_wait.rs)
- [examples/quote_progress.rs](examples/quote_progress.rs)
- [examples/trade_login_tqkq.rs](examples/trade_login_tqkq.rs)
- [examples/api_contract_s27_metadata_service_queries.rs](examples/api_contract_s27_metadata_service_queries.rs)

这个示例展示的是最推荐的 direct-query 使用路径：

- 调用方自带 Tokio runtime
- 用 `SessionClientBuilder::enable_query()` 打开官方 query domain
- 直接通过 `SessionClient` 发起一次性 metadata query
- 如果调用方自己提交了底层 `RuntimeCommand`，可以用 `wait_command_completed(command_id).await` 只等待该命令完成，而不引入更高层 facade 语义
- 需要自己读取命令状态时，优先使用 `command_status_typed(command_id)`，保留旧
  `command_status(command_id)` 作为字符串兼容 helper

其中 `query_command_wait.rs` 展示的是最底层的一种写法：

- 调用方直接提交 `RuntimeCommand::Query(QueryCommand::Fetch { .. })`
- 用 `wait_command_completed(command_id).await` 等到底层命令完成
- 再通过 `query_result(query_id)` 回到统一状态树读取结果

而 `api_contract_s27_metadata_service_queries.rs` 展示的是完整 metadata /
service direct-query pack：

- 合约列表、主连、期权链和多档期权查询继续属于 session metadata one-shot API
- 交易日历、结算价、排名和 EDB 继续属于 session service one-shot API
- wait/自建消费层可以通过 `session()` 复用同一个底层 session，但不复制这些
  direct-query API

而 `quote_progress.rs` 展示的是面向高性能用户的纯 substrate live 行情路径：

- 通过 `SessionClient::subscribe_quotes(...)` 提交最薄行情订阅命令
- 用 `SessionClient::progress_once()` 推进 live session
- 用 `RuntimeReader::cursor()` / `RuntimeReader::next()` 自己消费 commit 边界
- 用 `RuntimeReader::read_market_state()` 走热路径 market partition 读取最新 quote

当上层 facade 或多个消费者需要表达“我正在使用这批行情/窗口”而不是裸提交命令时，
使用 session-scoped interest API：`ensure_quotes(...)`、`ensure_trading_status(...)`
和 `ensure_chart(...)`。它们返回 lease，在同一个 `SessionClient` 内做去重和引用计数；
只有第一个 owner 会提交订阅 / `SetChart`，最后一个 owner 显式 `close().await` 时才会
提交 unsubscribe / `CancelChart`。`subscribe_quotes(...)` / `unsubscribe_quotes(...)`
仍保留为低层命令 helper。

而 `trade_login_tqkq.rs` 展示的是同一层 substrate 的另一条典型路径：

- 用 `SessionClientBuilder::trade_target_tqkq*()` 预声明 trade route
- 用 `SessionClient::tqkq_login_command*()` 从当前 auth context 派生官方内置模拟账户登录命令
- 仅靠 `progress_once()` 推进到底层 trade state tree，而不引入 `wait_update()` facade

如果上层 facade 需要在策略启动前确认行情和交易初始截面已经可用，可以用
`StartupRecoverySpec` + `SessionClient::startup_recovery_status(...)` 读取
revision-bound readiness。这个接口只检查状态，不提交订阅或登录命令；订阅、
登录和等待形状仍由 `tqsdk-wait` 或调用方自建消费层负责。

如果上层 facade 需要对同一用户下单 intent 做进程内/session 内去重，可以用
`OrderIntentRecord` + `SessionClient::remember_order_intent(...)` 记录稳定
client order id 与 runtime order id 的对应关系。这个 ledger 会随
`SessionClient::clone()` 和 `TqApi::into_session()` 共享，但不是跨进程持久化存储，
也不替代 runtime command ledger 或交易回报对账。

## 建议的 Direct Query 接口层次

按当前分层，`tqsdk-session` 里的 direct query 再细分为三层 trait：

### 第一层：`SessionRawQuery`

- `query_graphql(...).await`
- `query_graphql_value(...).await`
- `refresh_schema(...).await`
- `refresh_schema_value(...).await`

这一层的目标是保证所有一次性 query/schema 都已经有可用底座。
其中 value-style GraphQL helper 内部串行化 query route；command-style raw
helper 仍是底层 escape hatch。

### 第二层：`SessionMetadataQuery`

这些接口仍然属于 `tqsdk-session`，因为它们只是一次性 request/response：

- `query_symbol_info(...)` 已实现，返回 typed `SymbolInfo`；它对齐官方
  `query_symbol_info` 合约信息表字段，包括 `trading_time.day/night`、涨跌停、
  昨结算、开仓限额、到期/行权字段、标的合约和期权方向
- `query_instrument_specs(...)` 已实现，用 `InstrumentSpec` 表达合约规格，
  只保留 tick size、合约乘数、交易所、品种、到期和标的等下单校验常用字段
- `query_quotes(...)` 已实现
- `query_cont_quotes(...)` 已实现
- `query_options(...)` 已实现
- `query_atm_options(...)` 已实现
- `query_all_level_options(...)` 已实现
- `query_all_level_finance_options(...)` 已实现
- `get_trading_calendar(...)` 已实现，返回的 `TradingCalendarDay.date` 是
  `chrono::NaiveDate`
- `query_symbol_settlement(...)` 已实现
- `query_symbol_ranking(...)` 已实现
- `query_edb_data(...)` 已实现

其中：

- `get_trading_calendar(...)`
- `query_symbol_settlement(...)`
- `query_symbol_ranking(...)`
- `query_edb_data(...)`

这一批已经先落地，因为它们和当前 core 里的 `TradingCalendarDay`、`SymbolSettlement`、`SymbolRanking`、`EdbIndexData` typed contract 直接对应。

### 第三层：`SessionServiceQuery`

- `get_trading_calendar(...)`
- `query_symbol_settlement(...)`
- `query_symbol_ranking(...)`
- `query_edb_data(...)`

这些接口虽然请求目标是官方独立 HTTP 服务，但语义依然是一次性 request/response，因此继续放在 `tqsdk-session`，而不是进入 `tqsdk-wait` 或自建 live 消费层。

`SessionDirectQuery` 只是把这三层统一组合成一个总 trait，便于上层在泛型约束里一次性声明完整 direct-query 能力。

这些 request / response DTO 会保持薄结构，但按可发布 crate 的方式预留扩展空间：

- `OptionQueryFilter` 可用 `new()` / `Default::default()` 构造，其他请求结构通过 `new(...)` 构造
- 结构标记为 `non_exhaustive`，后续官方协议增加字段时可以扩展而不破坏用户代码
- 参数合法性仍在发起请求前统一校验，避免在 DTO 层引入重逻辑

### 不应放进 `tqsdk-session` 的高层派生接口

下面这些虽然在 Python 里也表现成“查询”，但已经不只是薄的 request/response 包装，更像研究工具层：

- `query_his_cont_quotes(...)`
- `query_option_greeks(...)`
- 各种 DataFrame / polars 形状兼容接口

这些更适合留给独立的 `tqsdk-data`，而不是进入 `tqsdk-session`。
