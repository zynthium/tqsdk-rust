//! Scenario: Data 历史下载与 CSV 导出
//!
//! User goal:
//! - 查询历史主连换月记录
//! - 按时间范围拉取 K线和 tick 历史数据
//! - 把历史数据导出到调用方提供的 async writer
//!
//! API contract:
//! - Primary user layer: 研究 / 数据用户
//! - Intended crate path: `tqsdk-data`
//! - Lower-level escape hatch: `get_kline_data_page` / `get_tick_data_page`
//! - `query_his_cont_quotes` 使用 `(&[&str], usize, Option<NaiveDate>)`
//! - download/export 能力留在 data，不回流到 session/wait/自建消费层
//!
//! Forbidden:
//! - `TqApi` live subscription
//! - live fan-out
//! - direct `RuntimeCommand::MarketChartCommand`
//! - background downloader daemon
//! - DataFrame/polars
//! - 需要用户解析的 JSON
//!
//! Regression signal:
//! - 历史下载必须走 live `wait_update()` 或 live fan-out
//! - CSV 导出要求用户打开内部 chart command 或手写分页 loop
//! - 历史主连查询被移动到 session direct query
//! - download/export 下沉到 wait/session/task
//!
//! Review questions:
//! - 研究用户是否能从 data crate 自然完成历史主连、下载和 CSV materialization？
//! - 批量下载是否仍是 pull-based async substrate，而不是后台 daemon？
//! - lower-level page escape hatch 是否保留但没有暴露 runtime internals？

use std::time::Duration;

use chrono::{Duration as ChronoDuration, NaiveDate, Utc};
use tqsdk_data::{DataClient, KlineDataSeriesRequest, TickDataSeriesRequest};
use tqsdk_session::SessionClientBuilder;

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

fn parse_optional_date(key: &str) -> Result<Option<NaiveDate>, Box<dyn std::error::Error>> {
    std::env::var(key)
        .ok()
        .map(|value| NaiveDate::parse_from_str(&value, "%Y-%m-%d"))
        .transpose()
        .map_err(Into::into)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_TEST_SYMBOL").unwrap_or_else(|_| "SHFE.ao2609".to_string());
    let cont_symbol =
        std::env::var("TQ_CONT_SYMBOL").unwrap_or_else(|_| "KQ.m@SHFE.au".to_string());
    let cont_days = std::env::var("TQ_CONT_DAYS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10);
    let cont_end_date = parse_optional_date("TQ_CONT_END_DATE")?;
    let kline_seconds = std::env::var("TQ_KLINE_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(60);
    let page_view_width = std::env::var("TQ_HISTORY_PAGE_VIEW_WIDTH")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(128);

    let cont_symbols = [cont_symbol.as_str()];
    let cont_rows = DataClient::new()
        .query_his_cont_quotes(&cont_symbols, cont_days, cont_end_date)
        .await?;
    println!(
        "historical_cont_quotes symbols={} rows={}",
        cont_symbol,
        cont_rows.len()
    );

    let session = SessionClientBuilder::new(user, pass)
        .futures_market()
        .build()?;
    let client = DataClient::from_session(session);
    let end_dt = Utc::now();
    let kline_start_dt = end_dt - ChronoDuration::hours(4);
    let tick_start_dt = end_dt - ChronoDuration::minutes(30);

    let kline_request = KlineDataSeriesRequest::new(
        symbol.clone(),
        Duration::from_secs(kline_seconds),
        kline_start_dt
            .timestamp_nanos_opt()
            .ok_or("invalid kline start timestamp")?,
        end_dt
            .timestamp_nanos_opt()
            .ok_or("invalid kline end timestamp")?,
    )
    .with_page_view_width(page_view_width)
    .with_timeout(Duration::from_secs(30));
    let tick_request = TickDataSeriesRequest::new(
        symbol.clone(),
        tick_start_dt
            .timestamp_nanos_opt()
            .ok_or("invalid tick start timestamp")?,
        end_dt
            .timestamp_nanos_opt()
            .ok_or("invalid tick end timestamp")?,
    )
    .with_page_view_width(page_view_width)
    .with_timeout(Duration::from_secs(30));

    let mut kline_download = client.kline_data_download(kline_request.clone())?;
    let kline_rows = kline_download.collect_remaining().await?;
    println!(
        "kline_download symbol={} duration_ns={} rows={} pages={} done={}",
        kline_download.symbol(),
        kline_download.duration_ns(),
        kline_rows.len(),
        kline_download.progress().emitted_pages(),
        kline_download.is_finished()
    );

    let mut tick_download = client.tick_data_download(tick_request.clone())?;
    let tick_rows = tick_download.collect_remaining().await?;
    println!(
        "tick_download symbol={} rows={} pages={} done={}",
        tick_download.symbol(),
        tick_rows.len(),
        tick_download.progress().emitted_pages(),
        tick_download.is_finished()
    );

    let mut kline_sink = tokio::io::sink();
    let kline_summary = client
        .export_kline_data_csv(kline_request, &mut kline_sink)
        .await?;
    println!(
        "kline_csv symbol={} rows={} pages={} range=[{}, {})",
        kline_summary.symbol,
        kline_summary.rows_written,
        kline_summary.pages_written,
        kline_summary.start_datetime_ns,
        kline_summary.end_datetime_ns
    );

    let mut tick_sink = tokio::io::sink();
    let tick_summary = client
        .export_tick_data_csv(tick_request, &mut tick_sink)
        .await?;
    println!(
        "tick_csv symbol={} rows={} pages={} range=[{}, {})",
        tick_summary.symbol,
        tick_summary.rows_written,
        tick_summary.pages_written,
        tick_summary.start_datetime_ns,
        tick_summary.end_datetime_ns
    );

    Ok(())
}
