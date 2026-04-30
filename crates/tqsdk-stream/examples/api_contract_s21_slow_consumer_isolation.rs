//! Scenario: 慢消费者隔离（managed commit sink + retry/WAL foundation）
//!
//! User goal:
//! - 写库 / 日志不能拖慢核心行情循环
//! - 慢消费者 lag 可见
//! - 核心策略消费者不受影响
//! - shutdown 时 sink 可以 flush
//! - sink 可以配置有限重试和本地 JSONL WAL
//! - WAL 可以配置 fsync 策略并做本地 JSONL compaction
//! - WAL 可以扫描出 delivered / pending / failed revision report
//! - commit metadata 可以写入本地 JSONL journal 并按 revision checkpoint 重放给 sink
//!
//! API contract:
//! - fan-out/backpressure 的底层 capacity 是 public config
//! - fan-out buffer capacity 可显式配置
//! - 慢消费者 lag 通过 typed diagnostic 暴露
//! - 写库 / 日志 sink 可由 SDK 托管，不要求用户手写 task/channel
//! - sink shutdown 返回 typed stats / flush report
//! - per-sink finite retry 和 JSONL WAL 是 public config
//! - WAL fsync policy 和 compaction 是 public config
//! - WAL recovery report 是 public API，但不伪装成 commit payload replay
//! - commit journal replay 是 public API，但不伪装成完整状态快照恢复或 daemon queue
//! - durable queue / 跨进程 daemon 化重放恢复仍是 gap
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
//! - sink shutdown 无法确认是否 flush
//! - retry/WAL policy 只能散落在业务代码里
//! - fsync 或 WAL compaction 只能散落在业务代码里
//! - 用户必须自己解析 WAL 判断未完成 revision
//! - 用户必须自己定义 commit journal 文件格式并重建 `CommitSink`
//!
//! Review questions:
//! - 当前 API 是否自然表达慢消费者隔离？
//! - hot path 是否有性能风险？
//! - 应通过 stream config 微调还是新增 sink abstraction？
//!
//! Current API note:
//! 当前 `tqsdk-stream` 暴露 root fan-out capacity、managed commit sink、
//! typed sink stats / shutdown report、typed `Lagged` diagnostic、有限重试和
//! JSONL WAL foundation、WAL fsync policy、本地 compaction 和 WAL recovery
//! report，以及可按 revision checkpoint 重放的 commit metadata journal。
//! 可靠 daemon queue 和完整状态快照恢复仍是 gap。

use futures::StreamExt;
use tqsdk_core::SharedCommitResult;
use tqsdk_stream::{
    StreamCommitJournal, StreamSinkFuture, StreamSinkProfile, StreamSinkRetryPolicy,
    StreamSinkWalCompaction, StreamSinkWalFsyncPolicy, StreamSinkWalRecovery, TqStreamBuilder,
};

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
    let wal_path = std::env::temp_dir().join("tqsdk-warehouse-sink.jsonl");
    let journal_path = std::env::temp_dir().join("tqsdk-warehouse-commit-journal.jsonl");
    let warehouse_options =
        StreamSinkProfile::reliable_jsonl(wal_path.clone(), journal_path.clone())
            .retry_policy(StreamSinkRetryPolicy::limited(3)?)
            .fsync_policy(StreamSinkWalFsyncPolicy::EveryRecord)
            .into_options();
    let warehouse_sink = stream.spawn_commit_sink_with_options(
        "warehouse",
        write_warehouse_commit,
        warehouse_options,
    )?;

    if let Some(update) = strategy_commits.next().await {
        let commit = update?;
        println!("strategy revision={}", commit.revision.get());
    }

    let report = warehouse_sink.shutdown().await?;
    println!(
        "sink={} status={:?} processed={} lagged={} errors={} retries={} wal_records={} journal_records={} flushed={}",
        report.name(),
        report.status(),
        report.stats().processed_commits(),
        report.stats().lagged_commits(),
        report.stats().errors(),
        report.stats().retry_attempts(),
        report.stats().wal_records(),
        report.stats().journal_records(),
        report.flushed()
    );

    let wal_recovery = StreamSinkWalRecovery::new().scan_jsonl(&wal_path)?;
    println!(
        "wal delivered={:?} pending={:?} failed={:?} lagged_records={} flush_failed_records={}",
        wal_recovery.delivered_revisions(),
        wal_recovery.pending_revisions(),
        wal_recovery.failed_revisions(),
        wal_recovery.lagged_records(),
        wal_recovery.flush_failed_records()
    );

    let journal_records = StreamCommitJournal::new().read_jsonl(&journal_path)?;
    println!("journal records={}", journal_records.len());

    let replay_report = StreamCommitJournal::new()
        .after_revision(wal_recovery.last_delivered_revision().unwrap_or(0))
        .replay_jsonl(&journal_path, write_warehouse_commit)
        .await?;
    println!(
        "journal replayed={} last_revision={:?}",
        replay_report.replayed_commits(),
        replay_report.last_replayed_revision()
    );

    let wal_compaction = StreamSinkWalCompaction::new()
        .retain_revisions_from(1)
        .retain_non_revision_records(false)
        .compact_jsonl(&wal_path)?;
    println!(
        "wal original={} retained={} dropped={}",
        wal_compaction.original_records(),
        wal_compaction.retained_records(),
        wal_compaction.dropped_records()
    );

    Ok(())
}

fn write_warehouse_commit(commit: SharedCommitResult) -> StreamSinkFuture {
    Box::pin(async move {
        println!("warehouse revision={}", commit.revision.get());
        Ok(())
    })
}
