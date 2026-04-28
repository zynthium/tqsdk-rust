//! Scenario: 本地行情缓存维护（queue / lock / index / compaction foundation）
//!
//! User goal:
//! - live 进程先把标准行情事件写入本地 durable queue
//! - 用 SDK 提供的 lock file 避免多个 writer 同时维护同一缓存文件
//! - 将 queue drain 到 cache 文件
//! - 建立本地 index 并按保留策略 compact cache
//! - 从 compacted cache replay 标准行情事件
//!
//! API contract:
//! - queue / lock / index / compaction 是明确 public API
//! - cache payload 使用 SDK 标准 `Quote` / `Kline` / `Tick`
//! - cache 维护不依赖 live session
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 用户自己定义 queue / index / compaction 文件格式
//! - 业务代码直接写 state tree dump
//! - provider 私有 protocol type
//! - cache 维护污染 `tqsdk-core` / `tqsdk-session`
//!
//! Regression signal:
//! - 用户必须自己手写 JSONL queue 和 replay 排序
//! - 用户必须自己实现 cache lock / index / compaction
//! - cache maintenance API 反向依赖 live facade
//!
//! Review questions:
//! - 当前 API 是否自然表达本地 cache 维护 foundation？
//! - durable daemon / stale lock recovery 是否仍被明确排除？
//! - 是否存在热路径性能风险？

use tqsdk_core::Quote;
use tqsdk_data::{
    MarketCacheCompaction, MarketCacheEvent, MarketCacheIndex, MarketCacheLock,
    MarketCachePayloadKind, MarketCacheQueue, MarketCacheReader, MarketCacheReplay,
    MarketCacheWriter,
};

fn main() -> tqsdk_data::Result<()> {
    let base = std::env::temp_dir().join("tqsdk-cache-maintenance-example");
    let queue_path = base.with_extension("queue.jsonl");
    let cache_path = base.with_extension("cache.jsonl");
    let compacted_path = base.with_extension("compacted.jsonl");
    let lock_path = base.with_extension("lock");
    let _ = std::fs::remove_file(&queue_path);
    let _ = std::fs::remove_file(&cache_path);
    let _ = std::fs::remove_file(&compacted_path);
    let _ = std::fs::remove_file(&lock_path);

    let _lock = MarketCacheLock::acquire(&lock_path)?;
    let queue = MarketCacheQueue::open(&queue_path)?.with_sync_on_enqueue(true);

    queue.enqueue_event(&MarketCacheEvent::quote(
        "live",
        "SHFE.au2602",
        1_000,
        Some(900),
        Quote {
            last_price: 480.5,
            ..Quote::default()
        },
    )?)?;
    queue.enqueue_event(&MarketCacheEvent::quote(
        "live",
        "SHFE.au2602",
        2_000,
        Some(1_900),
        Quote {
            last_price: 481.0,
            ..Quote::default()
        },
    )?)?;

    let mut writer = MarketCacheWriter::create(&cache_path)?;
    let drain_report = queue.drain_to_writer(&mut writer)?;
    println!("drained {} cache events", drain_report.written_events);

    let index = MarketCacheIndex::from_reader(MarketCacheReader::open(&cache_path)?)?;
    if let Some(entry) = index.entry("live", "SHFE.au2602", MarketCachePayloadKind::Quote) {
        println!(
            "indexed quote events={} time_range={}..={}",
            entry.events, entry.min_event_time_ns, entry.max_event_time_ns
        );
    }

    let compaction_report = MarketCacheCompaction::new()
        .retain_event_time_from(1_000)
        .retain_symbol("SHFE.au2602")
        .compact_file(&cache_path, &compacted_path)?;
    println!(
        "compacted read={} written={} dropped={}",
        compaction_report.read_events,
        compaction_report.written_events,
        compaction_report.dropped_events
    );

    let replay = MarketCacheReplay::from_reader(MarketCacheReader::open(&compacted_path)?)?;
    for event in replay {
        println!(
            "source={} symbol={} event_time_ns={}",
            event.source,
            event.symbol,
            event.event_time_ns()
        );
    }

    Ok(())
}
