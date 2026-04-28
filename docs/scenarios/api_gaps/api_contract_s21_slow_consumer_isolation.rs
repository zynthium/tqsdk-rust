//! Scenario: 慢消费者隔离（剩余 gap：durable queue / cross-process commit replay）
//!
//! User goal:
//! - 写库 / 日志不能拖慢核心行情循环
//! - sink 可以独立重试、落盘、维护 WAL，并审计未完成 revision
//! - 核心策略消费者不受慢 sink 影响
//!
//! API contract:
//! - bounded fan-out、lag diagnostics、managed commit sink、有限重试和 JSONL WAL
//!   已由 `tqsdk-stream` 提供
//! - WAL fsync policy、本地 compaction 和 recovery report 已由 `tqsdk-stream` 提供
//! - durable queue 和跨进程 commit payload replay 尚未形成 public API
//! - 不要求用户自建 channel
//! - 不要求用户手写 Tokio supervisor task
//!
//! Forbidden:
//! - 用户手写 mpsc/broadcast channel 隔离写库
//! - 写库 future 直接 await 在核心行情循环里
//! - provider 私有 driver handle
//! - 手写 Tokio 后台任务编排
//!
//! Regression signal:
//! - sink 失败会关闭核心策略 consumer
//! - 有限重试或 JSONL WAL policy 只能散落在业务代码里
//! - durable queue / 跨进程 commit replay 只能散落在业务代码里
//!
//! Review questions:
//! - sink runtime 是否应该在 `tqsdk-stream` 之上独立成 tooling？
//! - 是否需要可靠队列或本地 WAL？
//! - 如何保持核心 commit fan-out 不被 sink 背压污染？
//!
//! Current API note:
//! `TqStreamBuilder::commit_channel_capacity(...)` 和
//! `StreamFacadeError::diagnostic()` 已覆盖 bounded fan-out / lag 可见性；
//! `TqStream::spawn_commit_sink_with_options(...)` / `StreamSinkOptions` /
//! `StreamSinkRetryPolicy` / `CommitSink` / `StreamSinkHandle`
//! 已覆盖 managed commit sink、有限重试、JSONL WAL、typed stats 和 shutdown
//! flush report、WAL fsync policy、本地 compaction 和 WAL recovery report。本文件只
//! 保留 durable queue 和跨进程 commit payload replay 能力 gap。
//!
//! 理想用户代码草案：
//! ```ignore
//! let daemon = TqStreamBuilder::new(user, pass)
//!     .futures_market()
//!     .commit_channel_capacity(16_384)?
//!     .build()
//!     .await?
//!     .durable_sink("warehouse", SqlSink::new(pool).durable_queue("/var/tq/wal"))
//!     .durable_sink("audit", JsonlSink::new("/var/log/tq-audit.jsonl").compact_daily())
//!     .build()
//!     .await?;
//! daemon.run().await?;
//! ```

fn main() {}
