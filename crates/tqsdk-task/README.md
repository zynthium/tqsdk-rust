# `tqsdk-task`

`tqsdk-task` 是建立在 `tqsdk-wait` 之上的执行工具层。

它的目标不是提供新的协议层能力，而是承接：

- `TargetPosTask`
- scheduler
- task registry
- symbol ownership
- 手动下单冲突保护

当前 crate 还处在脚手架阶段。

设计基线见 [../../docs/architecture/api-task.md](../../docs/architecture/api-task.md)。
