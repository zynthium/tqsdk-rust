//! Scenario: 本地行情缓存后台 supervisor foundation
//!
//! User goal:
//! - 用 process-local supervisor 周期性 flush 本地 queue
//! - 自动续租 lock lease，避免长期运行进程被误判为 stale writer
//! - 优雅关闭时 flush 剩余 queue，compact cache，并拿到 shutdown report
//!
//! API contract:
//! - supervisor 仍是 data-layer 本地文件工具，不拥有 live session
//! - 用户不手写后台任务、channel、lease heartbeat 或 queue rotation
//! - queue flush 使用 rotating drain，降低 enqueue 与 drain 的状态一致性风险
//! - shutdown report 暴露周期 flush、lease renew、pre-shutdown flush 和最终状态
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 手写 Tokio 后台任务编排
//! - 手写 channel / ack / retry queue
//! - 手写 lock lease heartbeat
//! - 内置 HTTP health endpoint 或 GUI
//! - provider 私有 protocol type
//!
//! Regression signal:
//! - 用户必须自己管理 lease renew 线程
//! - 用户必须自己实现 queue rotation 才能避免 flush 覆盖新写入
//! - supervisor API 反向依赖 live facade 或 runtime state
//!
//! Review questions:
//! - 当前 API 是否自然表达本地后台 cache supervisor？
//! - 是否仍明确排除跨进程 cache service？
//! - 是否存在状态一致性或热路径性能风险？

use std::time::Duration;

use tqsdk_core::Quote;
use tqsdk_data::{
    MarketCacheCompaction, MarketCacheDaemon, MarketCacheDaemonConfig, MarketCacheEvent,
    MarketCacheReader, MarketCacheSupervisorConfig,
};

fn main() -> tqsdk_data::Result<()> {
    let base = std::env::temp_dir().join("tqsdk-cache-supervisor-foundation-example");
    let cache_path = base.with_extension("cache.jsonl");
    let queue_path = base.with_extension("queue.jsonl");
    let lock_path = base.with_extension("lock");
    let staging_path = base.with_extension("compact.tmp");
    let processing_path = base.with_extension("processing.jsonl");
    let _ = std::fs::remove_file(&cache_path);
    let _ = std::fs::remove_file(&queue_path);
    let _ = std::fs::remove_file(&lock_path);
    let _ = std::fs::remove_file(&staging_path);
    let _ = std::fs::remove_file(&processing_path);

    let daemon = MarketCacheDaemon::open(
        MarketCacheDaemonConfig::new(&cache_path)
            .queue_path(&queue_path)
            .lock_path(&lock_path)
            .compaction_staging_path(&staging_path)
            .stale_lock_after(Duration::from_secs(30))
            .with_sync_on_enqueue(true)
            .compaction_policy(MarketCacheCompaction::new().retain_event_time_from(1_000)),
    )?;

    let supervisor = daemon.spawn_supervisor(
        MarketCacheSupervisorConfig::new()
            .flush_interval(Duration::from_millis(50))
            .lease_renew_interval(Duration::from_millis(50))
            .processing_queue_path(&processing_path),
    )?;

    supervisor.enqueue_event(&MarketCacheEvent::quote(
        "live",
        "SHFE.au2602",
        2_000,
        Some(1_500),
        Quote {
            last_price: 480.5,
            ..Quote::default()
        },
    )?)?;

    std::thread::sleep(Duration::from_millis(120));

    let report = supervisor.shutdown()?;
    println!(
        "periodic_flushes={} lease_renewals={} pre_shutdown_written={} final_written={} queue_empty={}",
        report.periodic_flushes,
        report.lease_renewals,
        report.pre_shutdown_flush_report.written_events,
        report.shutdown.flush_report.written_events,
        report.shutdown.queue_empty
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
