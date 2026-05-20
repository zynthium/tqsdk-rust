# tqsdk

`tqsdk` 是 `tqsdk-rust` 的默认用户入口。它不物理合并内部 crate，也不改变
runtime contract；它只提供一个更容易开始的 facade：

- `tqsdk::prelude::*`
- `Tq::futures()`
- `Tq::next()` 主循环
- 常用 wait-style live refs
- `TargetPos` 轻量 wrapper
- `DataClient` history helper
- `tqsdk::advanced::*` 下钻到底层 crate

## 示例

```rust
use tqsdk::prelude::*;

# async fn run() -> tqsdk::Result<()> {
let mut tq = Tq::futures()
    .auth("demo-user", "demo-pass")
    .trade_target_tqkq()
    .connect()
    .await?;

let quote = tq.quote("SHFE.au2602").await?;
let mut target = tq.target_pos("TQKQ", "SHFE.au2602")?;

while tq.next().await? {
    let q = quote.load()?;
    if q.last_price > 3600.0 {
        target.set(1).await?;
    }
}
# Ok(())
# }
```

高级用户可以继续使用：

```rust
use tqsdk::advanced::session::SessionClientBuilder;
use tqsdk::advanced::stream::TqStreamBuilder;
use tqsdk::advanced::runtime::RuntimeReader;
```

## 边界

`tqsdk` 不拥有第二棵状态树，不复制 direct query、stream、task 或 data
实现。能力归属仍然保持在内部 crate：

- direct query / metadata：`tqsdk-session`
- single-owner `wait_update()`：`tqsdk-wait`
- async stream：`tqsdk-stream`
- execution tooling：`tqsdk-task`
- research/offline data：`tqsdk-data`
