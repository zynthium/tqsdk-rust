//! Scenario: 本地行情缓存 compaction ownership foundation
//!
//! User goal:
//! - cache writer 在持有 writer lease 时执行 compact
//! - compact 时读取 reader manifest，避免删除仍被 reader checkpoint 依赖的数据
//! - 输出 typed compaction ownership report，而不是让用户手动计算 reader floor
//!
//! API contract:
//! - compaction ownership 是 data-layer 本地文件工具，不拥有 live session
//! - compaction action 必须要求已获得的 writer lease
//! - reader-protected compaction 自动合并 retention policy 和 reader floor
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 用户自己解析 reader manifest 文件
//! - 用户自己计算 compaction floor
//! - reader-protected compaction 使用 symbol/source/payload 过滤误删共享 cache
//! - provider 私有 protocol type
//! - 把 compaction ownership 下沉到 `tqsdk-core` / `tqsdk-session`
//!
//! Regression signal:
//! - compaction 可以在未获得 writer lease 时运行
//! - compaction policy 能删除早于 active reader checkpoint 的必要数据
//! - 用户必须理解 reader manifest 内部 JSON 格式
//!
//! Review questions:
//! - 当前 API 是否自然表达 compaction ownership substrate？
//! - 是否避免 compaction 与 reader checkpoint 的一致性风险？
//! - 是否仍明确排除完整跨进程 service facade？

use tqsdk_core::Quote;
use tqsdk_data::{
    MarketCacheCompaction, MarketCacheCompactionOwnership, MarketCacheEvent, MarketCacheReader,
    MarketCacheReaderCheckpoint, MarketCacheReaderManifest, MarketCacheWriter,
    MarketCacheWriterElection,
};

fn main() -> tqsdk_data::Result<()> {
    let base = std::env::temp_dir().join("tqsdk-cache-compaction-ownership-example");
    let cache_path = base.with_extension("cache.jsonl");
    let staging_path = base.with_extension("compact.tmp");
    let manifest_path = base.with_extension("readers.json");
    let lock_path = base.with_extension("lock");
    let _ = std::fs::remove_file(&cache_path);
    let _ = std::fs::remove_file(&staging_path);
    let _ = std::fs::remove_file(&manifest_path);
    let _ = std::fs::remove_file(&lock_path);

    let floor_event = quote_event(2_000, 1_500, 481.0)?;
    write_events(
        &cache_path,
        &[
            quote_event(1_000, 500, 480.0)?,
            floor_event.clone(),
            quote_event(3_000, 2_500, 482.0)?,
        ],
    )?;
    MarketCacheReaderManifest::open(&manifest_path)?.record_checkpoint(
        MarketCacheReaderCheckpoint::from_event("research-a", "last-read", &floor_event),
    )?;

    let mut lease = MarketCacheWriterElection::new(&lock_path)
        .elect()?
        .into_lease()
        .ok_or(tqsdk_data::DataError::InvalidState(
            "market cache writer lease is busy",
        ))?;

    let report = MarketCacheCompactionOwnership::new(&cache_path)
        .staging_path(&staging_path)
        .reader_manifest_path(&manifest_path)
        .policy(MarketCacheCompaction::new().retain_event_time_from(2_500))
        .compact(&mut lease)?;
    let event_times = read_event_times(&cache_path)?;

    println!(
        "reader_floor={:?} effective_min={:?} written={} event_times={:?}",
        report.reader_floor_event_time_ns,
        report.effective_min_event_time_ns,
        report.compaction.compaction.written_events,
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
