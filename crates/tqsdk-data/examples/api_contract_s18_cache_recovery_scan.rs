//! Scenario: 本地行情缓存 recovery scan foundation
//!
//! User goal:
//! - 启动 cache writer / service 前扫描本地 cache 相关文件
//! - 识别 pending queue、interrupted drain、interrupted compaction 和 corrupt file
//! - 输出 typed recovery report，而不是让用户猜哪个文件可信
//!
//! API contract:
//! - recovery scan 是 data-layer 本地文件工具，不拥有 live session
//! - 扫描 cache、queue、processing queue 和 compaction staging
//! - 文件异常通过 typed report 暴露，已读进度不被隐藏
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 用户自己解析 queue / processing / staging 文件
//! - 用户自己推断 interrupted drain / compaction 状态
//! - provider 私有 protocol type
//! - 把 recovery scan 下沉到 `tqsdk-core` / `tqsdk-session`
//!
//! Regression signal:
//! - writer crash 后只能靠用户人工检查 cache 文件
//! - corrupt file 会丢失已读进度
//! - recovery report 暴露 provider 内部协议字段
//!
//! Review questions:
//! - 当前 API 是否自然表达本地 recovery scan substrate？
//! - 是否仍明确排除 writer election / service facade？
//! - 是否存在状态一致性或数据丢失风险？

use tqsdk_core::Quote;
use tqsdk_data::{MarketCacheEvent, MarketCacheRecoveryScan, MarketCacheWriter};

fn main() -> tqsdk_data::Result<()> {
    let base = std::env::temp_dir().join("tqsdk-cache-recovery-scan-example");
    let cache_path = base.with_extension("cache.jsonl");
    let queue_path = base.with_extension("queue.jsonl");
    let processing_path = base.with_extension("processing.jsonl");
    let staging_path = base.with_extension("compact.tmp");
    let _ = std::fs::remove_file(&cache_path);
    let _ = std::fs::remove_file(&queue_path);
    let _ = std::fs::remove_file(&processing_path);
    let _ = std::fs::remove_file(&staging_path);

    write_one(&cache_path, 1_000, 480.5)?;
    write_one(&queue_path, 2_000, 481.0)?;
    write_one(&processing_path, 3_000, 481.5)?;

    let report = MarketCacheRecoveryScan::new(&cache_path)
        .queue_path(&queue_path)
        .processing_queue_path(&processing_path)
        .compaction_staging_path(&staging_path)
        .scan()?;

    println!(
        "cache_events={} queue_events={} processing_events={} pending={} interrupted_drain={} writer_recovery={}",
        report.cache.readable_events,
        report.queue.readable_events,
        report.processing_queue.readable_events,
        report.has_pending_queue_events(),
        report.has_interrupted_drain(),
        report.requires_writer_recovery()
    );

    Ok(())
}

fn write_one(
    path: impl AsRef<std::path::Path>,
    event_time_ns: i64,
    last_price: f64,
) -> tqsdk_data::Result<()> {
    let mut writer = MarketCacheWriter::create(path)?;
    writer.write_event(&MarketCacheEvent::quote(
        "live",
        "SHFE.au2602",
        event_time_ns,
        Some(event_time_ns),
        Quote {
            last_price,
            ..Quote::default()
        },
    )?)?;
    writer.flush()
}
