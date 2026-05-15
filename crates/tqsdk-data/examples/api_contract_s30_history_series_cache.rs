//! Scenario: 看盘软件历史序列 mmap 缓存
//!
//! User goal:
//! - 看盘软件启动后快速加载最近 K 线 / tick 历史窗口
//! - 图表缩放、拖拽和回看时只补齐缺失历史区间
//! - 同一合约周期重复打开时复用 Python 兼容磁盘缓存
//! - 从现有 `get_*_data_series` 拿到 typed rows 和 cache report
//!
//! API contract:
//! - Primary user layer: 看盘软件 / 交易终端 / 研究用户
//! - Intended crate path: `tqsdk-data`
//! - `DataClient::from_session(...)` 默认不启用历史序列缓存
//! - `DataClientBuilder::history_cache_enabled(true)` 显式开启缓存
//! - 未设置目录时使用 Python 兼容默认目录 `~/.tqsdk/data_series_1`
//! - `history_cache_dir(...)` 可以指定自定义目录
//! - `history_cache_max_bytes(...)` / `history_cache_retention_days(...)` 可以配置容量和保留期
//! - 首版 backend 是 mmap，并使用 Python `DataSeries` 兼容文件名和二进制列布局
//! - Python/Rust 历史缓存文件可互通和交替使用，但首版不承诺同目录同时写
//! - cache miss 使用官方 `DataSeries` 的 `set_chart` 序列补齐缺口
//! - `HistorySeriesCache::read_*_data_series` 提供 cache-only 读取，缺口返回 typed miss
//! - `HistorySeriesCache::scan()` 提供 schema/version 与损坏文件 report
//!
//! Forbidden:
//! - `tqsdk-core` / `tqsdk-session` / `tqsdk-wait` / `tqsdk-stream` 拥有历史文件缓存
//! - `TqApi::kline` 或 `TqStream` live window 依赖 data cache
//! - 高频交易 hot path 依赖历史序列 mmap cache
//! - 用户手写 cache 文件格式、lock 文件、range 合并或 chart command
//! - Python 与 Rust SDK 同时写同一历史序列缓存目录
//!
//! Regression signal:
//! - 开启缓存后仍每次全量下载相同历史窗口
//! - cache miss 不走官方 `focus_datetime + focus_position=0 + view_width=2000` 序列
//! - 缓存命中绕过 `tq_dl` 权限校验
//! - 缓存损坏或无法打开时 panic 或静默返回坏数据
//! - 历史缓存污染 live runtime revision / commit 语义
//!
//! Review questions:
//! - builder opt-in 是否足够显式，且未改变 `DataClient::from_session` 默认行为？
//! - Python 兼容默认目录和自定义目录是否同时覆盖迁移与隔离需求？
//! - mmap 首版 contract 是否仍限制在 `tqsdk-data` 的 offline materialization 边界内？
//! - mutable tail refresh 是否足以避免把未稳定 K 线永久当作缓存命中？

use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use tqsdk_data::{DataClientBuilder, KlineDataSeriesRequest};
use tqsdk_session::SessionClientBuilder;

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_TEST_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".to_string());
    let cache_dir = std::env::var_os("TQ_HISTORY_CACHE_DIR");

    let session = SessionClientBuilder::new(user, pass)
        .futures_market()
        .build()?;
    let mut builder = DataClientBuilder::new()
        .with_session(session)
        .history_cache_enabled(true)
        .history_cache_retention_days(30);
    if let Some(cache_dir) = cache_dir {
        builder = builder.history_cache_dir(cache_dir);
    }
    let client = builder.build()?;

    let end = Utc::now();
    let start = end - ChronoDuration::hours(4);
    let series = client
        .get_kline_data_series(
            KlineDataSeriesRequest::new(
                symbol.clone(),
                Duration::from_secs(60),
                start
                    .timestamp_nanos_opt()
                    .ok_or("invalid start timestamp")?,
                end.timestamp_nanos_opt().ok_or("invalid end timestamp")?,
            )
            .with_timeout(Duration::from_secs(30)),
        )
        .await?;

    if let Some(report) = series.cache_report() {
        println!(
            "symbol={} rows={} hit_rows={} downloaded_ranges={} cache_dir={}",
            symbol,
            series.len(),
            report.hit_rows,
            report.downloaded_ranges.len(),
            report.cache_dir.display()
        );
    }

    Ok(())
}
