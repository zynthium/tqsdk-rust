//! Scenario: 生产守护进程
//!
//! User goal:
//! - 暴露健康状态
//! - 自动重连
//! - 优雅关闭
//! - 输出日志与指标
//!
//! API contract:
//! - daemon/runtime health 是 public API
//! - reconnect、lag、route status、auth status 有 typed diagnostics
//! - graceful shutdown 能停止订阅、flush command、关闭 driver
//! - 不要求用户手写 supervisor task
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - provider 私有 driver handle
//! - 用户自行追踪心跳和 route phase
//! - 通过 drop 隐式完成 async shutdown
//! - 手写 Tokio 后台任务编排
//!
//! Regression signal:
//! - 生产部署只能从日志字符串判断健康状态
//! - shutdown 可能丢命令或悬挂订阅
//! - reconnect exhaustion 不能被指标系统读取
//!
//! Review questions:
//! - 当前 API 是否自然表达 daemon 运维需求？
//! - 错误/健康状态是否类型安全？
//! - 这是 stream facade 扩展还是单独 runtime crate？
//!
//! API gap:
//! `tqsdk-stream` 已有 session event/reconnect 事件、typed
//! `TqStream::health()` snapshot、`StreamHealthSnapshot::status()` 和
//! `should_restart()`；仍没有 metrics hooks、HTTP health endpoint、typed
//! graceful shutdown contract 和完整 supervisor API。
//!
//! 理想用户代码草案：
//! ```ignore
//! let daemon = TqDaemon::new(TqStreamBuilder::new(user, pass).futures_market())
//!     .health_endpoint("127.0.0.1:9000")
//!     .metrics(MetricsSink::prometheus())
//!     .graceful_shutdown_on_ctrl_c()
//!     .build()
//!     .await?;
//! daemon.run(strategy).await?;
//! ```

fn main() {}
