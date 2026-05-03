//! Scenario: 本地行情缓存 file service foundation
//!
//! User goal:
//! - 用一个本地 cache service facade 组合 writer election、recovery、reader checkpoint 和 compaction
//! - 启动时恢复 pending queue，运行中写入标准行情事件，退出时 flush 并 reader-protected compact
//! - 遇到 writer 已被占用时得到 typed busy report，而不是解析 lock 错误字符串
//!
//! API contract:
//! - service facade 是 data-layer 本地文件工具，不拥有 live session
//! - service 打开时执行 writer election 和 recovery action
//! - shutdown 返回 flush / compaction / queue 状态
//! - 不内置 HTTP health endpoint、GUI 或系统级进程管理器
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 用户自己解析 lock lease 文件格式
//! - 用户自己解析 reader manifest JSON
//! - 用户自己移动 queue / processing queue 文件
//! - provider 私有 protocol type
//! - 把 cache service 下沉到 `tqsdk-core` / `tqsdk-session`
//!
//! Regression signal:
//! - cache service 打开必须手动拼接 election / recovery / compaction ownership
//! - writer busy 只能通过字符串错误判断
//! - shutdown 不能确认 queue 是否 flush 完成
//!
//! Review questions:
//! - 当前 API 是否自然表达本地 file service foundation？
//! - 是否仍明确排除完整跨进程 daemon orchestration？
//! - 是否避免 reader checkpoint 与 compaction 的一致性风险？

use tqsdk_core::Quote;
use tqsdk_data::{
    DataError, MarketCacheCompaction, MarketCacheEvent, MarketCacheReader,
    MarketCacheReaderCheckpoint, MarketCacheService, MarketCacheServiceConfig, MarketCacheWriter,
};

fn main() -> tqsdk_data::Result<()> {
    let base = std::env::temp_dir().join("tqsdk-cache-service-foundation-example");
    let cache_path = base.with_extension("cache.jsonl");
    let queue_path = base.with_extension("queue.jsonl");
    let processing_path = base.with_extension("processing.jsonl");
    let lock_path = base.with_extension("lock");
    let manifest_path = base.with_extension("readers.json");
    let staging_path = base.with_extension("compact.tmp");
    let _ = std::fs::remove_file(&cache_path);
    let _ = std::fs::remove_file(&queue_path);
    let _ = std::fs::remove_file(&processing_path);
    let _ = std::fs::remove_file(&lock_path);
    let _ = std::fs::remove_file(&manifest_path);
    let _ = std::fs::remove_file(&staging_path);

    let floor_event = quote_event(2_000, 1_500, 481.0)?;
    write_events(
        &cache_path,
        &[quote_event(1_000, 500, 480.0)?, floor_event.clone()],
    )?;
    write_events(&queue_path, &[quote_event(3_000, 2_500, 482.0)?])?;

    let opened = MarketCacheService::open(
        MarketCacheServiceConfig::new(&cache_path)
            .queue_path(&queue_path)
            .processing_queue_path(&processing_path)
            .lock_path(&lock_path)
            .reader_manifest_path(&manifest_path)
            .compaction_staging_path(&staging_path)
            .compaction_policy(MarketCacheCompaction::new().retain_event_time_from(2_500)),
    )?;
    let open_report = opened.report().clone();
    let service = opened.into_service().ok_or(DataError::InvalidState(
        "market cache service writer is busy",
    ))?;

    service.record_reader_checkpoint(MarketCacheReaderCheckpoint::from_event(
        "research-a",
        "last-read",
        &floor_event,
    ))?;
    service.enqueue_event(&quote_event(4_000, 3_500, 483.0)?)?;

    let shutdown = service.shutdown()?;
    let event_times = read_event_times(&cache_path)?;

    println!(
        "writer={:?} recovered={} flushed={} compacted={} queue_empty={} event_times={:?}",
        open_report.writer.status,
        open_report
            .recovery
            .as_ref()
            .map_or(0, |report| report.recovered_events()),
        shutdown.flush_report.written_events,
        shutdown
            .compaction_report
            .as_ref()
            .map_or(0, |report| report.compaction.compaction.written_events),
        shutdown.queue_empty,
        event_times
    );

    Ok(())
}

fn quote_event(
    received_at_ns: i64,
    exchange_time_ns: i64,
    last_price: f64,
) -> tqsdk_data::Result<MarketCacheEvent> {
    MarketCacheEvent::quote(
        "live",
        "SHFE.au2602",
        received_at_ns,
        Some(exchange_time_ns),
        Quote {
            last_price,
            ..Quote::default()
        },
    )
}

fn write_events(
    path: impl AsRef<std::path::Path>,
    events: &[MarketCacheEvent],
) -> tqsdk_data::Result<()> {
    let mut writer = MarketCacheWriter::create(path)?;
    for event in events {
        writer.write_event(event)?;
    }
    writer.flush()
}

fn read_event_times(path: impl AsRef<std::path::Path>) -> tqsdk_data::Result<Vec<i64>> {
    MarketCacheReader::open(path)?
        .map(|event| event.map(|event| event.event_time_ns()))
        .collect()
}
