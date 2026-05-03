//! Scenario: 本地行情缓存守护进程 foundation（lease / queue / rotation / shutdown）
//!
//! User goal:
//! - 用一个本地 cache daemon facade 管理 queue、lock lease 和 cache 文件
//! - 写入标准行情事件，不暴露 provider 私有协议
//! - 退出时 flush queue，按保留策略 compact，并返回 shutdown report
//!
//! API contract:
//! - daemon facade 是 data-layer 本地文件工具，不拥有 live session
//! - stale lock recovery 通过明确的 lease timeout 配置开启
//! - queue drain 失败能保留 queue 并暴露 typed progress report
//! - shutdown 返回 flush / compaction / queue 状态
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 手写 lock lease 文件格式
//! - 手写 queue ack / retry 状态
//! - 手写 cache file rotation
//! - 内置 HTTP health endpoint、GUI 或后台 Tokio supervisor
//! - provider 私有 protocol type
//!
//! Regression signal:
//! - 用户必须自己解析 lock / queue / compaction 文件
//! - shutdown 不能确认 queue 是否 flush 完成
//! - cache daemon API 反向依赖 live facade 或 runtime state
//!
//! Review questions:
//! - 当前 API 是否自然表达本地 daemon foundation？
//! - 是否仍然明确排除完整 production daemon？
//! - 是否存在状态一致性或热路径性能风险？

use std::time::Duration;

use tqsdk_core::Quote;
use tqsdk_data::{
    MarketCacheCompaction, MarketCacheDaemon, MarketCacheDaemonConfig, MarketCacheEvent,
    MarketCacheReader,
};

fn main() -> tqsdk_data::Result<()> {
    let base = std::env::temp_dir().join("tqsdk-cache-daemon-foundation-example");
    let cache_path = base.with_extension("cache.jsonl");
    let queue_path = base.with_extension("queue.jsonl");
    let lock_path = base.with_extension("lock");
    let staging_path = base.with_extension("compact.tmp");
    let _ = std::fs::remove_file(&cache_path);
    let _ = std::fs::remove_file(&queue_path);
    let _ = std::fs::remove_file(&lock_path);
    let _ = std::fs::remove_file(&staging_path);

    let daemon = MarketCacheDaemon::open(
        MarketCacheDaemonConfig::new(&cache_path)
            .queue_path(&queue_path)
            .lock_path(&lock_path)
            .compaction_staging_path(&staging_path)
            .stale_lock_after(Duration::from_secs(30))
            .with_sync_on_enqueue(true)
            .compaction_policy(MarketCacheCompaction::new().retain_event_time_from(1_000)),
    )?;

    daemon.enqueue_event(&MarketCacheEvent::quote(
        "live",
        "SHFE.au2602",
        1_000,
        Some(900),
        Quote {
            last_price: 480.5,
            ..Quote::default()
        },
    )?)?;

    let report = daemon.shutdown()?;
    println!(
        "flushed={} compacted={} queue_empty={}",
        report.flush_report.written_events,
        report
            .compaction_report
            .as_ref()
            .map_or(0, |report| report.compaction.written_events),
        report.queue_empty
    );

    for event in MarketCacheReader::open(&cache_path)? {
        let event = event?;
        println!(
            "source={} symbol={} event_time_ns={}",
            event.source,
            event.symbol,
            event.event_time_ns()
        );
    }

    Ok(())
}
