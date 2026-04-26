//! Scenario: 研究场景
//!
//! User goal:
//! - 拉取 K线
//! - 计算指标
//! - 批量处理数据
//!
//! API contract:
//! - 使用 `tqsdk-data` research/offline public API
//! - 返回 owned Rust typed rows
//! - 不把 DataFrame/polars 作为必需依赖
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - live `wait_update()` facade
//! - provider 内部 session / protocol type
//! - 手写 chart command
//! - `serde_json::Value`
//!
//! Regression signal:
//! - 研究代码必须走实时订阅循环
//! - K线分页细节泄漏到普通批处理用户
//! - 指标计算需要依赖内部 cache
//!
//! Review questions:
//! - 当前 API 是否自然表达研究批处理？
//! - 样板代码是否可接受？
//! - 是否有离线/实时 crate 边界混淆？

use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use tqsdk_data::{DataClient, KlineDataSeriesRequest};
use tqsdk_session::SessionClientBuilder;

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_TEST_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".to_string());
    let end = Utc::now();
    let start = end - ChronoDuration::hours(4);

    let session = SessionClientBuilder::new(user, pass)
        .futures_market()
        .build()?;
    let client = DataClient::from_session(session);
    let series = client
        .get_kline_data_series(KlineDataSeriesRequest::new(
            symbol.clone(),
            Duration::from_secs(60),
            start
                .timestamp_nanos_opt()
                .ok_or("invalid start timestamp")?,
            end.timestamp_nanos_opt().ok_or("invalid end timestamp")?,
        ))
        .await?;

    let closes: Vec<f64> = series.rows().iter().map(|row| row.close).collect();
    let sample = closes.len().min(20);
    let sma = if sample == 0 {
        f64::NAN
    } else {
        closes.iter().rev().take(sample).sum::<f64>() / sample as f64
    };
    println!("symbol={} rows={} sma20={}", symbol, series.len(), sma);

    Ok(())
}
