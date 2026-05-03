//! Scenario: 本地行情缓存 writer election / recovery action foundation
//!
//! User goal:
//! - 启动 cache writer 前先竞选唯一 writer lease
//! - writer 崩溃后在明确 lease ownership 下恢复 processing queue 和 queue
//! - 输出 typed election / recovery report，而不是让用户手动解析 lock / queue 文件
//!
//! API contract:
//! - writer election 是 data-layer 本地文件所有权工具，不拥有 live session
//! - recovery action 必须要求已获得的 writer lease
//! - recovery action 恢复 processing queue 和 queue，并保留 typed scan report
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 用户自己解析 lock lease 文件格式
//! - 用户自己移动 processing queue 文件
//! - 用户在未获得 writer lease 时执行 recovery action
//! - provider 私有 protocol type
//! - 把 writer election / recovery action 下沉到 `tqsdk-core` / `tqsdk-session`
//!
//! Regression signal:
//! - 多进程 writer 竞争只能通过解析 `DataError` 字符串判断
//! - recovery action 不需要 writer lease 也能运行
//! - writer crash 后必须人工判断 queue / processing / cache 哪个文件可信
//!
//! Review questions:
//! - 当前 API 是否自然表达 writer election substrate？
//! - recovery action 是否避免无锁写入导致的缓存一致性风险？
//! - 是否仍明确排除完整跨进程 service facade？

use std::time::Duration;

use tqsdk_core::Quote;
use tqsdk_data::{
    DataError, MarketCacheEvent, MarketCachePayload, MarketCacheReader, MarketCacheRecoveryAction,
    MarketCacheWriter, MarketCacheWriterElection,
};

fn main() -> tqsdk_data::Result<()> {
    let base = std::env::temp_dir().join("tqsdk-cache-writer-recovery-example");
    let cache_path = base.with_extension("cache.jsonl");
    let queue_path = base.with_extension("queue.jsonl");
    let processing_path = base.with_extension("processing.jsonl");
    let lock_path = base.with_extension("lock");
    let _ = std::fs::remove_file(&cache_path);
    let _ = std::fs::remove_file(&queue_path);
    let _ = std::fs::remove_file(&processing_path);
    let _ = std::fs::remove_file(&lock_path);

    write_one(&cache_path, 1_000, 480.0)?;
    write_one(&processing_path, 2_000, 481.0)?;
    write_one(&queue_path, 3_000, 482.0)?;

    let election = MarketCacheWriterElection::new(&lock_path)
        .stale_after(Duration::from_secs(30))
        .elect()?;
    let election_report = election.report().clone();
    let mut lease = election
        .into_lease()
        .ok_or(DataError::InvalidState("market cache writer lease is busy"))?;

    let recovery = MarketCacheRecoveryAction::new(&cache_path)
        .queue_path(&queue_path)
        .processing_queue_path(&processing_path)
        .recover(&mut lease)?;
    let prices = read_prices(&cache_path)?;

    println!(
        "writer={:?} recovered_stale={} recovered_events={} follow_up={} prices={:?}",
        election_report.status,
        election_report.recovered_stale,
        recovery.recovered_events(),
        recovery.requires_follow_up(),
        prices
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

fn read_prices(path: impl AsRef<std::path::Path>) -> tqsdk_data::Result<Vec<f64>> {
    MarketCacheReader::open(path)?
        .map(|event| match event?.payload {
            MarketCachePayload::Quote(quote) => Ok(quote.last_price),
            _ => Err(DataError::InvalidState("expected quote cache event")),
        })
        .collect()
}
