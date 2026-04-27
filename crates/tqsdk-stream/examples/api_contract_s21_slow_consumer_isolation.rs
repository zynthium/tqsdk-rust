//! Scenario: 慢消费者隔离（bounded fan-out / lag 诊断子集）
//!
//! User goal:
//! - 写库 / 日志不能拖慢核心行情循环
//! - 慢消费者 lag 可见
//! - 核心策略消费者不受影响
//!
//! API contract:
//! - fan-out/backpressure 的底层 capacity 是 public config
//! - fan-out buffer capacity 可显式配置
//! - 慢消费者 lag 通过 typed diagnostic 暴露
//! - durable sink lifecycle / per-sink retry/storage policy 仍是 gap
//! - 不要求用户自建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 用户手写 mpsc/broadcast channel 隔离写库
//! - 写库 future 直接 await 在核心行情循环里
//! - provider 私有 driver handle
//! - 手写 Tokio 后台任务编排
//!
//! Regression signal:
//! - 一个日志消费者 lag 导致策略消费者也丢事件
//! - lag 只能表现为 stream 关闭或卡住
//! - 用户必须自己 spawn 任务保护核心循环
//!
//! Review questions:
//! - 当前 API 是否自然表达慢消费者隔离？
//! - hot path 是否有性能风险？
//! - 应通过 stream config 微调还是新增 sink abstraction？
//!
//! Current API note:
//! 当前 `tqsdk-stream` 暴露 root fan-out capacity 和 typed `Lagged`
//! diagnostic。持久化 sink runtime、per-sink retry/storage policy 仍是 gap。

use futures::StreamExt;
use tqsdk_stream::{StreamErrorKind, TqStreamBuilder};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;

    let stream = TqStreamBuilder::new(user, pass)
        .futures_market()
        .commit_channel_capacity(16_384)?
        .build()
        .await?;

    let mut strategy_commits = stream.commit_stream()?;
    let mut warehouse_commits = stream.commit_stream()?;

    if let Some(update) = strategy_commits.next().await {
        let commit = update?;
        println!("strategy revision={}", commit.revision.get());
    }

    if let Some(update) = warehouse_commits.next().await {
        match update {
            Ok(commit) => println!("warehouse revision={}", commit.revision.get()),
            Err(error) if error.diagnostic().kind == StreamErrorKind::Lagged => {
                println!("warehouse lagged: {}", error.diagnostic().message);
            }
            Err(error) => return Err(error.into()),
        }
    }

    Ok(())
}
