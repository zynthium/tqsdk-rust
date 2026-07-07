# Crate 选择

先使用 `scenario-router.md`。场景确定后，本文件解释 crate 边界。如果某个 Python TqSdk API 横跨多个关注点，写代码前先按 Rust crate 归属拆开。

## 决策表

| 用户目标 | 使用 | 原因 |
| --- | --- | --- |
| Ordinary strategy, target position, light history access | `tqsdk` | 默认 facade、`prelude`、`Tq::next()`、常用 live refs、`TargetPos` wrapper、`Tq::history()` helper |
| Explicit Python-style live market/trade loop | `tqsdk-wait` | single owner、`step()` / `step_until(...)`、`WaitStep::is_changing()`、live refs、serial windows |
| Multi-consumer async events or fan-out | caller-owned layer over `tqsdk-session + tqsdk-core` | shared session、`RuntimeReader`、`UpdateCursor`、commit boundary、caller-owned bounded fan-out / lag diagnostics |
| One-shot metadata/query/service calls | `tqsdk-session` | GraphQL/query、schema、symbol info、quotes metadata、calendar、settlement、ranking、EDB |
| Strategy execution helpers | `tqsdk-task` | `TaskHost`、`TargetPosTask`、scheduler、risk gate、typed order builders、fake broker tests |
| Strategy backtest | `tqsdk` first; `tqsdk-wait` or `tqsdk-task + tqsdk-data` when explicit | 普通 live/backtest 同主体策略用默认 facade 的 `.backtest(...)`：无缓存时官方服务端行情，有 `cache_dir` / `market_cache` 时持久缓存本地撮合；不要使用已删除的 `server_backtest(...)` alias；自定义数据源用 `.replay_backtest(...)`；明确 Python-style wait builder 时用 `TqApiBuilder::futures_backtest`；本地确定性 `TqSim` 内部能力用 `StrategyBacktest` + `ReplayMarketSource`，历史 rows 可由 `tqsdk-data` 提供 |
| Live tick recording into shared backtest cache | `tqsdk` first; `tqsdk-data` only for a pure writer | 普通策略优先用 `MarketCachePolicy::new(cache_dir).record_ticks(symbols)` 或 `.record_universe(expression)?` + `.market_cache(...)` 同时驱动 live recording 和 cache-backed backtest；运行中临时开启可用 `Tq::record_ticks(cache_dir, symbols)`；已有 tick rows 的上层 host 可直接用 `LiveTickCacheWriter::push_ticks(...)` |
| Same-process monitoring dashboard / cache inventory | `tqsdk` with `monitoring`; `tqsdk-monitor` only for advanced embedding | 普通 facade 用 `.monitoring(MonitoringConfig::localhost(port))`；`.market_cache(...)` 或 backtest `.cache_dir(...)` 会作为默认 inventory 来源；显式来源用 `with_cache_inventory(path)` |
| Historical/offline research | `tqsdk-data` | data pages、data series、downloads、CSV export、history cache、option Greeks |
| Runtime substrate or custom facade | `tqsdk-core` plus `tqsdk-session` | commands、adapters、commit/revision/cursor、`RuntimeReader` hot path |

## 边界规则

- `tqsdk` 是普通用户默认入口：`prelude`、`Tq` 主循环、常用 wait-style live refs、`quotes_universe(...)`、target-position wrapper、history helper、`MarketCachePolicy` 共享 live/backtest tick cache policy、显式 `record_ticks(...)` live tick cache recorder、recording health/report helpers 和 curated `advanced::*` 下钻入口。它不拥有第二套 runtime、状态树、direct query、task、data 或 event fan-out 实现。
- `tqsdk-monitor` 是可选观察者层：低开销 `MonitorSink`、进程内 `MonitorRegistry` / `MonitorSnapshot`、只读 localhost dashboard 和 cache inventory projection。它可以后台读取 `tqsdk-data::BacktestTickCache::inventory()`，但不能拥有 session、回测推进、TQBN 格式、coverage 写入或 cache 管理操作。
- `tqsdk-session` 负责 one-shot request/response API：GraphQL、schema、metadata、calendar、settlement、ranking、EDB、auth refresh、replay control 和 low-level command wait helpers。
- `tqsdk-wait` 负责 Python-style single-owner live refs 和 `step()` 消费，也承接 server/backtest-market 的 same-body wait 策略入口。它可以暴露 `session()`，但不能复制 direct-query API。
- `tqsdk-task` 负责 strategy execution、target position、schedulers、risk gates、ownership、multi-account order foundations、fake broker tests、task-owned replay source、Python-compatible local `TqSim` backtest foundation 和 S31 trading desk profile。
- `tqsdk-data` 负责 research/offline data、history pages/series/downloads、CSV export、TQBN history cache、tick-only `BacktestTickCache`、纯数据层 `LiveTickCacheWriter` 和 option Greeks；它不拥有 live session/订阅，也不提供 JSONL market cache public API。
- `tqsdk-core` 只负责 runtime substrate。不要重新导出 auth/http/TqKq 实现细节，也不要在这里增加 facade convenience API。
- 多消费者 event/fan-out 不是内置 facade；调用方在 `tqsdk-session` 上推进 session，并用 `RuntimeReader::cursor()` / `RuntimeReader::next()` 为每个 consumer 自建边界、过滤、channel、lag 处理和持久化 sidecar。

## 快速判断问题

请求不清楚时，按用户想消费什么来判断：

| 回答 | Crate |
| --- | --- |
| “普通策略/默认入口/先跑起来” | `tqsdk` |
| “明确要 Python-style wait_update/live refs” | `tqsdk-wait` |
| “给多个消费者的 events” | caller-owned layer over `tqsdk-session + tqsdk-core` |
| “一次 query result” | `tqsdk-session` |
| “managed order/strategy abstraction” | `tqsdk-task` |
| “像 Python TqBacktest 那样 live/backtest 同一策略主体” | `tqsdk` first; explicit wait API 用 `tqsdk-wait` |
| “本地历史 rows / replay event + TqSim 确定性策略回测” | `tqsdk-task` plus `tqsdk-data` |
| “historical rows/files/cache” | `tqsdk-data` |
| “把指定 live tick 写进回测共享缓存” | 首选 `tqsdk::MarketCachePolicy` + `.market_cache(...)`，symbol 集合可用 `.record_ticks(...)` 或 `.record_universe(...)` 声明；运行中临时开启用 `tqsdk::Tq::record_ticks(...)`；已有 rows 的 host 可用 `tqsdk-data::LiveTickCacheWriter` |
| “同进程监控/运行面板/cache inventory 统计” | `tqsdk` 启用 `monitoring` feature；高级嵌入才直接用 `tqsdk-monitor` |
| “把 live K 线 / commit events / 任意窗口写入持久化” | 当前 SDK 不内置；使用调用方 sidecar |
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

普通用户保留 `tqsdk`。高级模式把 `tqsdk` 替换成选中的 sibling crate，或额外添加 sibling crate；例如明确 Python-style wait loop 用 `tqsdk-wait`，低层 typed trade enums 或 runtime cursor 可加 `tqsdk-core`。

`SessionClientBuilder::build()` 和 `DataClientBuilder::build()` 是同步构造器。`TqApiBuilder::build().await` 是 async，因为 wait facade 包装了 live session startup 行为。
