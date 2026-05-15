# Crate 选择

先使用 `scenario-router.md`。场景确定后，本文件解释 crate 边界。如果某个 Python TqSdk API 横跨多个关注点，写代码前先按 Rust crate 归属拆开。

## 决策表

| 用户目标 | 使用 | 原因 |
| --- | --- | --- |
| Python-style live market/trade loop | `tqsdk-wait` | single owner、`step()` / `step_until(...)`、`WaitStep::is_changing()`、live refs、serial windows |
| Multi-consumer async events or fan-out | `tqsdk-stream` | commit stream、filters、event streams、lag diagnostics、managed sinks |
| One-shot metadata/query/service calls | `tqsdk-session` | GraphQL/query、schema、symbol info、quotes metadata、calendar、settlement、ranking、EDB |
| Strategy execution helpers | `tqsdk-task` | `TaskHost`、`TargetPosTask`、scheduler、risk gate、typed order builders、fake broker tests |
| Historical/offline research | `tqsdk-data` | data pages、data series、downloads、CSV export、history cache、option Greeks、offline replay cache |
| Runtime substrate or custom facade | `tqsdk-core` plus `tqsdk-session` | commands、adapters、commit/revision/cursor、`RuntimeReader` hot path |

## 边界规则

- `tqsdk-session` 负责 one-shot request/response API：GraphQL、schema、metadata、calendar、settlement、ranking、EDB、auth refresh、replay control 和 low-level command wait helpers。
- `tqsdk-wait` 负责 Python-style single-owner live refs 和 `step()` 消费。它可以暴露 `session()`，但不能复制 direct-query API。
- `tqsdk-stream` 负责 multi-consumer commit/event streams、filters、lag diagnostics 和 stream sinks。它可以暴露 `session()`，但不能变成 metadata/query 层，也不能直接依赖 mmap history cache。
- `tqsdk-task` 负责 strategy execution、target position、schedulers、risk gates、ownership、multi-account order foundations、fake broker tests、replay strategy host 和 S31 trading desk profile。
- `tqsdk-data` 负责 research/offline data、history pages/series/downloads、CSV export、Python-compatible history cache、option Greeks 和 market-cache replay materialization；它不提供 live stream 写 mmap history cache 的 bridge。
- `tqsdk-core` 只负责 runtime substrate。不要重新导出 auth/http/TqKq 实现细节，也不要在这里增加 facade convenience API。

## 快速判断问题

请求不清楚时，按用户想消费什么来判断：

| 回答 | Crate |
| --- | --- |
| “会变化的 live object” | `tqsdk-wait` |
| “给多个消费者的 events” | `tqsdk-stream` |
| “一次 query result” | `tqsdk-session` |
| “managed order/strategy abstraction” | `tqsdk-task` |
| “historical rows/files/cache” | `tqsdk-data` |
| “把 live window 写进 history cache” | 当前 SDK 不提供；使用调用方 sidecar 或 stream sink |
| “runtime commits/cursors” | `tqsdk-core` plus `tqsdk-session` |

## 依赖写法

根据 SDK 分发方式选择一种依赖写法。

已发布 crate：

```toml
[dependencies]
tqsdk-wait = "<version>"
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

Git dependency：

```toml
[dependencies]
tqsdk-wait = { git = "https://github.com/OWNER/tqsdk-rust" }
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

本地 checkout：

```toml
[dependencies]
tqsdk-wait = { path = "../tqsdk-rust/crates/tqsdk-wait" }
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

把 `tqsdk-wait` 替换成选中的 crate。有些模式需要多个 crate，例如 `tqsdk-task` 加 `tqsdk-core` 来使用 typed trade enums。

`SessionClientBuilder::build()` 和 `DataClientBuilder::build()` 是同步构造器。`TqApiBuilder::build().await` 和 `TqStreamBuilder::build().await` 是 async，因为这些 facade 包装了 live session startup 行为。
