# `tqsdk-session`

共享的 session / direct-query 薄层。

这个 crate 负责把会话生命周期、route 驱动、schema / metadata / direct query 这类和具体 facade 无关的能力先抽出来，作为 `tqsdk-wait`、`tqsdk-stream` 等上层 facade 的共同底座。

它不是只给 facade 内部复用的隐藏层。对需要“一次性 query / metadata / schema 访问”的用户，`tqsdk-session` 本身就是正确入口。

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
- `replay_state()`
- `query_graphql(...).await`
- `query_graphql_value(...).await`
- `refresh_schema(...).await`
- `refresh_schema_value(...).await`
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

它不直接定义高层 diff-backed 用户 API，也不把某一种消费风格硬编码进核心。

可以把它理解为：

- `tqsdk-session` 负责一次性 direct query / schema / metadata
- `tqsdk-wait` 负责 `wait_update()` 风格的持续状态消费
- 未来 `tqsdk-stream` 负责 stream/event 风格的持续状态消费

## 建议的 Direct Query 接口层次

按当前分层，`tqsdk-session` 里的 direct query 再细分为三层：

### 第一层：已经存在的原始入口

- `query_graphql(...).await`
- `query_graphql_value(...).await`
- `refresh_schema(...).await`
- `refresh_schema_value(...).await`

这一层的目标是保证所有一次性 query/schema 都已经有可用底座。

### 第二层：typed/thin wrappers

这些接口仍然属于 `tqsdk-session`，因为它们只是一次性 request/response：

- `query_symbol_info(...)`
- `query_quotes(...)`
- `query_cont_quotes(...)`
- `query_options(...)`
- `query_atm_options(...)`
- `query_all_level_options(...)`
- `query_all_level_finance_options(...)`
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

### 第三层：不应放进 `tqsdk-session` 的高层派生接口

下面这些虽然在 Python 里也表现成“查询”，但已经不只是薄的 request/response 包装，更像研究工具层：

- `query_his_cont_quotes(...)`
- `query_option_greeks(...)`
- 各种 DataFrame / polars 形状兼容接口

这些更适合留给后续独立的 research/tools crate，而不是进入 `tqsdk-session`。
