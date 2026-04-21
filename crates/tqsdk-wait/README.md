# `tqsdk-wait`

`tqsdk-wait` 是建立在 `tqsdk-core + tqsdk-session` 之上的 Python 风格单推进点 facade。

它的职责很窄：

- 提供单 owner 的 `TqApi`
- 提供真实驱动 live session 的 `wait_update()` 语义和最近一次用户可见 commit 的变化解释
- 提供基于状态树的轻量对象引用与窗口视图
- 提供 trade 命令的 wait 风格薄包装

它明确不负责：

- GraphQL / HTTP direct query
- schema / metadata direct facade
- downloader / `TargetPosTask` / callback / stream
- 本地订单 overlay 或第二棵状态树

这些一次性接口的正确归属始终是 `tqsdk-session`，即使后续面向高性能用户增加 `tqsdk-stream` 也不改变这一点。

## 当前公开面

当前 MVP 已包含：

- `TqApiBuilder`
- `TqApi`
- `wait_update(deadline).await`
- `is_changing(...)`
- `is_changing_fields(...)`
- `get_quote(...).await`
- `get_trading_status(...).await`
- `get_kline_serial(...).await`
- `get_tick_serial(...).await`
- `get_account(...)`
- `get_position(...)`
- `get_order(...)`
- `get_trade(...)`
- `insert_order(...).await`
- `cancel_order(...).await`
- `confirm_settlement(...).await`

## 设计边界

- market / trade 对象都只是状态树上的轻量 `Ref`
- serial 数据先暴露为 Rust 原生窗口视图，而不是 DataFrame 兼容层
- `insert_order` / `cancel_order` / `confirm_settlement` 只提交到底层 command contract，不做本地伪造状态
- direct query / schema refresh / metadata 查询继续放在 `tqsdk-session`
- 未来 `tqsdk-stream` 也只会承载这同一批 diff-backed 对象的另一种消费形状，而不会接管 direct query

## 示例

```rust
use tqsdk_wait::TqApiBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let mut api = TqApiBuilder::new(user, pass).build().await?;
    let quote = api.get_quote("SHFE.au2602").await?;

    loop {
        if !api.wait_update(None).await? {
            continue;
        }

        if api.is_changing(&quote)? {
            let snapshot = quote.load(&api)?;
            println!("{} {}", snapshot.datetime, snapshot.last_price);
        }
    }
}
```

完整可编译示例见 [examples/quote_wait.rs](examples/quote_wait.rs)。

## Builder 边界

`TqApiBuilder` 只补一层和 wait facade 直接相关的便利配置，例如：

- `market_target(...)`
- `trade_target(...)`
- `trade_target_with_url(...)`
- `replay_url(...)`

如果需要更细的 session 级配置，例如 direct query、schema 或其他未来扩展项，应先配置 `tqsdk_session::SessionClientBuilder`，再通过 `TqApiBuilder::from_session_builder(...)` 包装成 wait facade。
