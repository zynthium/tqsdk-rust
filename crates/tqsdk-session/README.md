# `tqsdk-session`

共享的 session / direct-query 薄层。

这个 crate 负责把会话生命周期、route 驱动、schema / metadata / direct query 这类和具体 facade 无关的能力先抽出来，作为 `tqsdk-wait`、`tqsdk-stream` 等上层 facade 的共同底座。

它不是只给 facade 内部复用的隐藏层。对需要“一次性 query / metadata / schema 访问”的用户，`tqsdk-session` 本身就是正确入口。

它同时保持一个明确约束：

- 它是纯 async substrate，不内置 runtime
- 调用方必须自己提供 Tokio runtime
- direct service helper（交易日历、结算价、排名、EDB）也要求当前已经处于 Tokio runtime 中

它当前已经提供：

- `SessionClientBuilder`
- `SessionClient`
- lazy establish 的 live session owner
- `flush_outbound()`
- `drive_pending_once()`
- `drive_route_once()`
- `command_state()`
- `command_status()`
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
- `SessionRawQuery`
- `SessionMetadataQuery`
- `SessionServiceQuery`
- `SessionDirectQuery`
- `refresh_auth(...).await`
- `refresh_auth_value(...).await`
- `replay_step(...).await`
- `replay_step_value(...).await`
- `replay_reset(...).await`
- `replay_reset_value(...).await`
- `get_trading_calendar(...).await`
- `query_symbol_settlement(...).await`
- `query_symbol_ranking(...).await`
- `query_edb_data(...).await`
- `SymbolRankingType`
- `EdbDataAlign`
- `EdbDataFill`

默认会预置官方静态文件域名 `https://files.shinnytech.com` 作为 schema/file-backed metadata 的基地址，因此文件型 schema/metadata 刷新不需要额外传入环境变量。

对于一次性的官方 metadata / query 服务，当前也直接内置官方端点，而不是再额外暴露环境变量或 builder 配置：

- 交易日历：`https://files.shinnytech.com/shinny_chinese_holiday.json`
- 结算价：`https://md-settlement-system-fc-api.shinnytech.com/mss`
- 持仓排名：`https://symbol-ranking-system-fc-api.shinnytech.com/srs`
- EDB：`https://edb.shinnytech.com/data/index_data`

`query_graphql_value()` 与 replay 的 `*_value()` helper 只会在对应 domain 已启用时工作。query domain 现在可以承载在官方 `ins_query` websocket 链路上，也保留显式 HTTP query route 的定制能力。
如果要启用官方默认的 live query 语义而不显式覆盖 query endpoint，应调用 `SessionClientBuilder::enable_query()`。

如果调用方需要在进入 live 行情订阅前做权限护栏，可以直接用：

- `has_feature("futr").await`
- `has_feature("sec").await`
- `check_md_grants(&["SHFE.au2606", "SSE.510300"]).await`

这样上层 crate 就不需要再各自重复一份权限判断逻辑。

它不直接定义高层 diff-backed 用户 API，也不把某一种消费风格硬编码进核心。

可以把它理解为：

- `tqsdk-session` 负责一次性 direct query / schema / metadata
- `tqsdk-wait` 负责 `wait_update()` 风格的持续状态消费
- `tqsdk-stream` 负责 stream/event 风格的持续状态消费

## 示例

最小可编译示例见 [examples/query_symbol_info.rs](examples/query_symbol_info.rs)。

这个示例展示的是最推荐的 direct-query 使用路径：

- 调用方自带 Tokio runtime
- 用 `SessionClientBuilder::enable_query()` 打开官方 query domain
- 直接通过 `SessionClient` 发起一次性 metadata query

## 建议的 Direct Query 接口层次

按当前分层，`tqsdk-session` 里的 direct query 再细分为三层 trait：

### 第一层：`SessionRawQuery`

- `query_graphql(...).await`
- `query_graphql_value(...).await`
- `refresh_schema(...).await`
- `refresh_schema_value(...).await`

这一层的目标是保证所有一次性 query/schema 都已经有可用底座。

### 第二层：`SessionMetadataQuery`

这些接口仍然属于 `tqsdk-session`，因为它们只是一次性 request/response：

- `query_symbol_info(...)` 已实现
- `query_quotes(...)` 已实现
- `query_cont_quotes(...)` 已实现
- `query_options(...)` 已实现
- `query_atm_options(...)` 已实现
- `query_all_level_options(...)` 已实现
- `query_all_level_finance_options(...)` 已实现
- `get_trading_calendar(...)` 已实现
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

这些接口虽然请求目标是官方独立 HTTP 服务，但语义依然是一次性 request/response，因此继续放在 `tqsdk-session`，而不是进入 `tqsdk-wait` / `tqsdk-stream`。

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
