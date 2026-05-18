//! Scenario: 慢消费者隔离（bounded fan-out + lag diagnostics）
//!
//! User goal:
//! - 核心策略消费者和审计/写库 sidecar 各自独立推进
//! - fan-out buffer capacity 可显式配置
//! - 慢消费者 lag 可通过 typed diagnostic 观察
//! - SDK 不托管 durable sink、WAL、journal 或跨进程恢复
//!
//! API contract:
//! - `commit_channel_capacity(...)` 是 stream facade 的 public config
//! - 每个消费者通过自己的 `commit_stream()` 独立消费同一 runtime commit fan-out
//! - lag 通过 `StreamFacadeError::Lagged { skipped }` 暴露
//! - sidecar 的数据库写入、日志、重试、队列和落盘格式归用户或上层服务拥有
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - SDK-managed durable sink API
//! - SDK-managed WAL / commit journal / compaction / recovery
//! - provider 私有 driver handle
//!
//! Regression signal:
//! - 一个消费者 lag 导致其他消费者也卡住
//! - lag 只能表现为 stream 关闭或卡住
//! - stream facade 重新暴露 durable sink / WAL public API

use futures::StreamExt;
use tqsdk_stream::{StreamFacadeError, TqStreamBuilder};

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
    let mut audit_sidecar_commits = stream.commit_stream()?;

    if let Some(update) = strategy_commits.next().await {
        let commit = update?;
        println!("strategy revision={}", commit.revision.get());
    }

    if let Some(update) = audit_sidecar_commits.next().await {
        match update {
            Ok(commit) => {
                println!("audit sidecar revision={}", commit.revision.get());
                // User-owned durable work would happen here, outside SDK scope.
            }
            Err(StreamFacadeError::Lagged { skipped }) => {
                println!("audit sidecar lagged skipped={skipped}");
            }
            Err(error) => return Err(error.into()),
        }
    }

    Ok(())
}
