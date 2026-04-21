# `tqsdk-session`

共享的 session / direct-query 薄层。

这个 crate 负责把会话生命周期、route 驱动、schema / metadata / direct query 这类和具体 facade 无关的能力先抽出来，作为 `tqsdk-wait`、`tqsdk-stream` 等上层 facade 的共同底座。

它当前已经提供：

- `SessionClientBuilder`
- `SessionClient`
- lazy establish 的 live session owner
- `flush_outbound()`
- `drive_pending_once()`
- `drive_route_once()`
- `command_state()`
- `command_status()`
- `query_graphql(...).await`
- `query_graphql_value(...).await`
- `refresh_schema(...).await`
- `refresh_schema_value(...).await`

默认会预置官方静态文件域名 `https://files.shinnytech.com` 作为 schema/file-backed metadata 的基地址，因此文件型 schema/metadata 刷新不需要额外传入环境变量。

它不直接定义高层用户 API，也不把某一种消费风格硬编码进核心。
