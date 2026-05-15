//! Scenario: 本地行情缓存读写（cache record / replay foundation）
//!
//! User goal:
//! - 将标准行情对象写入本地缓存文件
//! - 从缓存文件读取标准行情对象
//! - 按事件时间顺序回放缓存记录
//!
//! API contract:
//! - cache writer/reader 是明确 public API
//! - 缓存 payload 使用 SDK 标准 `Quote` / `Kline` / `Tick`
//! - replay ordering 由 SDK 提供
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 用户自己定义缓存文件格式
//! - 业务代码直接写 state tree dump
//! - provider 私有 protocol type
//! - cache 读写污染 `tqsdk-core` / `tqsdk-session`
//!
//! Regression signal:
//! - 多进程共享只能靠用户手写 CSV/JSON
//! - cache replay 无法复用标准 market schema
//! - 用户需要自己处理排序
//!
//! Review questions:
//! - 当前 API 是否自然表达本地缓存 foundation？
//! - cache maintenance / live sink gap 是否被明确排除？
//! - 是否存在热路径性能风险？
//!
//! Current API note:
//! 本示例只验证离线 cache record、JSONL reader/writer 和 deterministic replay。
//! `tqsdk-data` 不再公开 queue / lock / index / compaction /
//! service / daemon / supervisor 这类跨进程或准跨进程编排表面。

use tqsdk_core::Quote;
use tqsdk_data::{MarketCacheEvent, MarketCacheReader, MarketCacheReplay, MarketCacheWriter};

fn main() -> tqsdk_data::Result<()> {
    let path = std::env::temp_dir().join("tqsdk-cache-example.jsonl");

    let quote = Quote {
        last_price: 480.5,
        ..Quote::default()
    };

    let mut writer = MarketCacheWriter::create(&path)?;
    writer.write_event(&MarketCacheEvent::quote(
        "example",
        "SHFE.au2602",
        1_000,
        Some(900),
        quote,
    )?)?;
    writer.flush()?;

    let replay = MarketCacheReplay::from_reader(MarketCacheReader::open(&path)?)?;
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
