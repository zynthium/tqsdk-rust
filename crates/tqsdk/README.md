# tqsdk

`tqsdk` 是 `tqsdk-rust` 的默认用户入口。它不物理合并内部 crate，也不改变
runtime contract；它只提供一个更容易开始的 facade：

- `tqsdk::prelude::*`
- `Tq::new()` (and `Tq::futures()` alias)
- Server-side backtest (`.backtest()`)
- Local offline backtest (`.local_backtest()` with `tqsdk_task::ReplayMarketSource`, optional `.default_price_tick(...)`)
- `Tq::next()` 主循环
- 常用 wait-style live refs 和 `Quote` 统一定义
- `TargetPos` 轻量 wrapper
- Local backtest 默认模拟账户常量 `LOCAL_BACKTEST_ACCOUNT_ID`
- `Tq::history()` helper
- `tqsdk::advanced::*` 下钻到底层 crate

## 示例

```rust
use tqsdk::prelude::*;

# async fn run() -> tqsdk::Result<()> {
let mut tq = Tq::futures()
    .auth_env()?
    .trade_target_tqkq()
    .connect()
    .await?;

let quote = tq.quote("SHFE.au2602").await?;
let target = tq.target_pos_tqkq("SHFE.au2602").await?;

while tq.next().await? {
    let q = quote.load()?;
    if q.last_price > 3600.0 {
        target.set(1)?;
    }
}
# Ok(())
# }
```

## Features

- `default = ["live", "services"]`：默认用户入口，包含 live 连接与服务查询能力。
- `live`：向内部 `session` / `wait` / `stream` / `task` / `data` crate 传播 live feature，并启用 TQ auth 派生的 TQKQ helper。
- `services`：向内部 crate 传播服务查询相关 HTTP 能力。
- `default-features = false`：保留 facade 类型和不依赖 live auth 的组合入口；live-only helper 不参与编译。

`tqsdk::advanced::*` 是 curated convenience，不是完整 sibling crate mirror。它只暴露默认 facade 常见下钻点：

```rust
use tqsdk::advanced::session::SessionClientBuilder;
use tqsdk::advanced::stream::TqStreamBuilder;
use tqsdk::advanced::runtime::RuntimeReader;
use tqsdk::advanced::task::StrategyReplaySourceBuilder;
```

需要完整 stream、task、data、session 或 core surface 的用户应直接依赖对应 sibling crate。这样可以让 `tqsdk` 的 semver surface 保持小，同时不限制高级用户使用底层能力。
其中 `advanced::stream` 面向已经明确需要多消费者 async stream 的用户；普通单 owner
策略仍应优先通过 `tqsdk::prelude::*` / `Tq::next()` 或直接使用 `tqsdk-wait`。

## 边界

`tqsdk` 不拥有第二棵状态树，不复制 direct query、stream、task 或 data
实现。能力归属仍然保持在内部 crate：

- direct query / metadata：`tqsdk-session`
- single-owner `wait_update()`：`tqsdk-wait`
- async multi-consumer stream：`tqsdk-stream`
- execution tooling：`tqsdk-task`
- research/offline data：`tqsdk-data`
