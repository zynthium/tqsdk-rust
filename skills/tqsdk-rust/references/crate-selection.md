# Crate 选择

先使用 `scenario-router.md`。场景确定后，本文件解释 crate 边界。如果某个 Python TqSdk API 横跨多个关注点，写代码前先按 Rust crate 归属拆开。

## 决策表

| 用户目标 | 使用 | 原因 |
| --- | --- | --- |
| Ordinary strategy, target position, light history access | `tqsdk` | 默认 facade、`prelude`、`Tq::next()`、常用 live refs、`TargetPos` wrapper、`Tq::history()` helper |
| Explicit Python-style live market/trade loop | `tqsdk-wait` | single owner、`step()` / `step_until(...)`、`WaitStep::is_changing()`、live refs、serial windows |
| Multi-consumer async events or fan-out | `tqsdk-stream` | commit stream、filters、event streams、row-batch market streams、lag diagnostics |
| One-shot metadata/query/service calls | `tqsdk-session` | GraphQL/query、schema、symbol info、quotes metadata、calendar、settlement、ranking、EDB |
| Strategy execution helpers | `tqsdk-task` | `TaskHost`、`TargetPosTask`、scheduler、risk gate、typed order builders、fake broker tests |
| Strategy backtest | `tqsdk` first; `tqsdk-wait` or `tqsdk-task + tqsdk-data` when explicit | 普通 live/backtest 同主体策略用默认 facade 的 `TqBuilder::{backtest,local_backtest}`；明确 Python-style wait builder 时用 `TqApiBuilder::futures_backtest`；本地确定性 `TqSim` 内部能力用 `StrategyBacktest` + `ReplayMarketSource`，历史 rows 可由 `tqsdk-data` 提供 |
| Historical/offline research | `tqsdk-data` | data pages、data series、downloads、CSV export、history cache、option Greeks |
| Runtime substrate or custom facade | `tqsdk-core` plus `tqsdk-session` | commands、adapters、commit/revision/cursor、`RuntimeReader` hot path |

## 边界规则

- `tqsdk` 是普通用户默认入口：`prelude`、`Tq` 主循环、常用 wait-style live refs、target-position wrapper、history helper 和 curated `advanced::*` 下钻入口。它不拥有第二套 runtime、状态树、direct query、stream、task 或 data 实现。
- `tqsdk-session` 负责 one-shot request/response API：GraphQL、schema、metadata、calendar、settlement、ranking、EDB、auth refresh、replay control 和 low-level command wait helpers。
- `tqsdk-wait` 负责 Python-style single-owner live refs 和 `step()` 消费，也承接 server/backtest-market 的 same-body wait 策略入口。它可以暴露 `session()`，但不能复制 direct-query API。
- `tqsdk-stream` 负责 multi-consumer commit/event streams、filters、row-batch market streams 和 lag diagnostics。它可以暴露 `session()`，但不能变成 metadata/query 层，也不能直接依赖 mmap history cache 或 managed sink/WAL。
- `tqsdk-task` 负责 strategy execution、target position、schedulers、risk gates、ownership、multi-account order foundations、fake broker tests、task-owned replay source、Python-compatible local `TqSim` backtest foundation 和 S31 trading desk profile。
- `tqsdk-data` 负责 research/offline data、history pages/series/downloads、CSV export、Python-compatible history cache 和 option Greeks；它不提供 live stream 写 mmap history cache 的 bridge，也不提供 JSONL market cache public API。
- `tqsdk-core` 只负责 runtime substrate。不要重新导出 auth/http/TqKq 实现细节，也不要在这里增加 facade convenience API。

## 快速判断问题

请求不清楚时，按用户想消费什么来判断：

| 回答 | Crate |
| --- | --- |
| “普通策略/默认入口/先跑起来” | `tqsdk` |
| “明确要 Python-style wait_update/live refs” | `tqsdk-wait` |
| “给多个消费者的 events” | `tqsdk-stream` |
| “一次 query result” | `tqsdk-session` |
| “managed order/strategy abstraction” | `tqsdk-task` |
| “像 Python TqBacktest 那样 live/backtest 同一策略主体” | `tqsdk` first; explicit wait API 用 `tqsdk-wait` |
| “本地历史 rows / replay event + TqSim 确定性策略回测” | `tqsdk-task` plus `tqsdk-data` |
| “historical rows/files/cache” | `tqsdk-data` |
| “把 live window 写进 history cache” | 当前 SDK 不提供；使用调用方 sidecar |
| “runtime commits/cursors” | `tqsdk-core` plus `tqsdk-session` |

## 依赖写法

根据 SDK 分发方式选择一种依赖写法。

已发布 crate：

```toml
[dependencies]
tqsdk = "<version>"
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

Git dependency：

```toml
[dependencies]
tqsdk = { git = "https://github.com/OWNER/tqsdk-rust" }
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

本地 checkout：

```toml
[dependencies]
tqsdk = { path = "../tqsdk-rust/crates/tqsdk" }
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

普通用户保留 `tqsdk`。高级模式把 `tqsdk` 替换成选中的 sibling crate，或额外添加 sibling crate；例如明确 Python-style wait loop 用 `tqsdk-wait`，低层 typed trade enums 可加 `tqsdk-core`。

`SessionClientBuilder::build()` 和 `DataClientBuilder::build()` 是同步构造器。`TqApiBuilder::build().await` 和 `TqStreamBuilder::build().await` 是 async，因为这些 facade 包装了 live session startup 行为。
