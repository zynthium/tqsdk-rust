//! Scenario: 本地行情缓存 reader manifest foundation
//!
//! User goal:
//! - 多个研究 / 回放 reader 记录各自处理到的 cache event
//! - cache compaction 可以知道最早仍被 reader 依赖的事件时间
//! - 运维代码可以看到 reader lag，而不解析私有 manifest 文件
//!
//! API contract:
//! - manifest 是 data-layer 本地文件工具，不拥有 live session
//! - checkpoint 基于标准 `MarketCacheEvent`，不暴露 provider 私有协议
//! - compaction floor 和 reader lag 通过 typed API 暴露
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 用户自己定义 reader checkpoint 文件格式
//! - 用户自己解析 manifest JSON
//! - provider 私有 protocol type
//! - 把 reader coordination 下沉到 `tqsdk-core` / `tqsdk-session`
//!
//! Regression signal:
//! - compaction 需要用户手写 reader checkpoint 扫描
//! - reader lag 只能通过业务日志推断
//! - manifest API 反向依赖 live facade 或 runtime state
//!
//! Review questions:
//! - 当前 API 是否自然表达 reader checkpoint substrate？
//! - 是否仍明确排除完整跨进程 cache service？
//! - 是否存在 compaction 状态一致性风险？

use tqsdk_core::Quote;
use tqsdk_data::{MarketCacheEvent, MarketCacheReaderCheckpoint, MarketCacheReaderManifest};

fn main() -> tqsdk_data::Result<()> {
    let manifest_path = std::env::temp_dir().join("tqsdk-cache-reader-manifest-example.json");
    let _ = std::fs::remove_file(&manifest_path);

    let first = MarketCacheEvent::quote(
        "live",
        "SHFE.au2602",
        2_000,
        Some(1_000),
        Quote {
            last_price: 480.5,
            ..Quote::default()
        },
    )?;
    let second = MarketCacheEvent::quote(
        "live",
        "SHFE.au2602",
        3_000,
        Some(1_500),
        Quote {
            last_price: 481.0,
            ..Quote::default()
        },
    )?;

    let manifest = MarketCacheReaderManifest::open(&manifest_path)?;
    manifest.record_checkpoint(MarketCacheReaderCheckpoint::from_event(
        "research-job",
        "last-close-study",
        &first,
    ))?;
    manifest.record_checkpoint(MarketCacheReaderCheckpoint::from_event(
        "replay-job",
        "risk-replay",
        &second,
    ))?;

    println!(
        "compaction_floor_event_time_ns={:?}",
        manifest.compaction_floor_event_time_ns()?
    );
    for lag in manifest.reader_lag_report(2_000)? {
        println!(
            "reader={} checkpoint={} lag_event_time_ns={}",
            lag.reader_id, lag.checkpoint_id, lag.lag_event_time_ns
        );
    }

    Ok(())
}
