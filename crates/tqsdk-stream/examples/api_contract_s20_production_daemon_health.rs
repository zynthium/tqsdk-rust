//! Scenario: 生产守护进程（健康状态子集）
//!
//! User goal:
//! - 读取 daemon/runtime health snapshot
//! - 区分 session phase、reconnect diagnostics 和 driver closed
//! - 将健康状态输出给日志或指标系统
//!
//! API contract:
//! - health 是 typed public API
//! - 不解析 runtime state tree 字符串路径
//! - reconnect exhaustion 可直接读取
//! - health status / restart hint 可直接读取
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - provider 私有 driver handle
//! - 用户自行追踪心跳和 route phase
//! - 从日志字符串判断 reconnect exhaustion
//! - 手写 Tokio 后台任务编排
//!
//! Regression signal:
//! - 生产部署只能从日志字符串判断健康状态
//! - 用户必须读取 `system/session/*` 原始路径
//! - reconnect exhaustion 不能被指标系统读取
//!
//! Review questions:
//! - 当前 API 是否自然表达 daemon health 子集？
//! - 错误/健康状态是否类型安全？
//! - 哪些完整 daemon 能力仍是 API gap？
//!
//! Current API note:
//! 本示例只验证 health snapshot 子集。metrics hook、HTTP health endpoint、
//! ctrl-c graceful shutdown 和可靠 sink isolation 仍属于 `docs/scenarios/api_gaps/`
//! 中的生产 daemon gap。

use futures::StreamExt;
use tqsdk_stream::TqStreamBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let stream = TqStreamBuilder::new(user, pass)
        .futures_market()
        .build()
        .await?;

    let mut commits = stream.commit_stream()?;
    while let Some(commit) = commits.next().await.transpose()? {
        let health = stream.health()?;
        println!(
            "revision={} phase={:?} status={:?} healthy={} restart={}",
            commit.revision.get(),
            health.session_phase,
            health.status(),
            health.is_healthy(),
            health.should_restart()
        );

        if health.should_restart() {
            break;
        }
    }

    Ok(())
}
