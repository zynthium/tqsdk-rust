//! Scenario: 生产守护进程（health + graceful shutdown 子集）
//!
//! User goal:
//! - 读取 daemon/runtime health snapshot
//! - 区分 session phase、reconnect diagnostics 和 driver closed
//! - 等待重连恢复并得到 typed reconnect outcome
//! - 将健康状态输出给日志或指标系统
//! - shutdown 时 flush managed sink 并关闭 stream driver
//!
//! API contract:
//! - health 是 typed public API
//! - 不解析 runtime state tree 字符串路径
//! - reconnect exhaustion 可直接读取
//! - health status / restart hint 可直接读取
//! - reconnect monitor 返回 typed outcome/report
//! - graceful shutdown 返回 typed driver/sink report
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
//! - 用户必须自己轮询 session phase 判断重连恢复
//! - 用户只能靠 drop 隐式关闭 driver / sink
//!
//! Review questions:
//! - 当前 API 是否自然表达 daemon health 子集？
//! - 错误/健康状态是否类型安全？
//! - 哪些完整 daemon 能力仍是 API gap？
//!
//! Current API note:
//! 本示例验证 stream-layer health snapshot、typed reconnect monitor 和
//! graceful shutdown 子集。
//! strategy telemetry/export hook 和 ctrl-c shutdown signal 位于 `tqsdk-task`；
//! stream-level reconnect monitor 只等待并报告既有 session 恢复结果，不接管底层
//! reconnect 执行。WAL compaction / fsync policy 和跨进程 daemon 管理仍属于
//! `docs/scenarios/api_gaps/` 中的生产 daemon gap；Rust SDK 不规划 GUI 或内置
//! HTTP health/metrics endpoint 作为 S20 完成标准。

use std::time::Duration;

use futures::StreamExt;
use tqsdk_core::SharedCommitResult;
use tqsdk_stream::{StreamHealthStatus, StreamReconnectOutcome, StreamSinkFuture, TqStreamBuilder};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let stream = TqStreamBuilder::new(user, pass)
        .futures_market()
        .build()
        .await?;

    let health_log_sink = stream.spawn_commit_sink("health-log", write_health_log)?;
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

        if health.status() == StreamHealthStatus::Recovering {
            let reconnect = stream
                .reconnect_monitor()
                .timeout(Duration::from_secs(30))
                .wait()
                .await?;
            println!(
                "reconnect outcome={:?} attempts={:?} observed_commits={}",
                reconnect.outcome(),
                reconnect.last_reconnect().map(|event| event.attempt),
                reconnect.observed_commits()
            );
            if matches!(
                reconnect.outcome(),
                StreamReconnectOutcome::Exhausted
                    | StreamReconnectOutcome::TimedOut
                    | StreamReconnectOutcome::Closed
            ) {
                break;
            }
        }

        if health.should_restart() {
            break;
        }
    }

    let shutdown = stream
        .graceful_shutdown()
        .sink(health_log_sink)
        .shutdown()
        .await?;
    println!(
        "shutdown graceful={} driver_closed={} sinks={} sink_errors={}",
        shutdown.graceful(),
        shutdown.driver_closed(),
        shutdown.sink_reports().len(),
        shutdown.sink_errors().len()
    );

    Ok(())
}

fn write_health_log(commit: SharedCommitResult) -> StreamSinkFuture {
    Box::pin(async move {
        println!("health-log revision={}", commit.revision.get());
        Ok(())
    })
}
