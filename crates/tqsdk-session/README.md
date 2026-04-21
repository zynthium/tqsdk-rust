# `tqsdk-session`

共享的 session / direct-query 薄层。

这个 crate 负责把会话生命周期、schema / metadata / direct query 这类和具体 facade 无关的能力先抽出来，作为 `tqsdk-wait`、`tqsdk-stream` 等上层 facade 的共同底座。

它不直接定义高层用户 API，也不把某一种消费风格硬编码进核心。
