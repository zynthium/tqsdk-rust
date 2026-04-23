use std::error::Error;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use tqsdk_data::{DataClient, KlineDataSeriesRequest};
use tqsdk_session::SessionClientBuilder;

fn read_env(name: &str) -> Result<String, Box<dyn Error>> {
    std::env::var(name).map_err(|_| format!("missing environment variable {name}").into())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let auth_user = read_env("TQ_AUTH_USER")?;
    let auth_pass = read_env("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_TEST_SYMBOL").unwrap_or_else(|_| "SHFE.ao2609".to_string());
    let seconds = std::env::var("TQ_KLINE_DURATION_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(60);
    let lookback_minutes = std::env::var("TQ_HISTORY_LOOKBACK_MINUTES")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(240);
    let page_view_width = std::env::var("TQ_HISTORY_PAGE_VIEW_WIDTH")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(2_000);

    let session = SessionClientBuilder::new(auth_user, auth_pass)
        .market_target(false, false)
        .build()?;
    let client = DataClient::from_session(session);
    let end_dt = Utc::now();
    let start_dt = end_dt - ChronoDuration::minutes(lookback_minutes);
    let series = client
        .get_kline_data_series(
            KlineDataSeriesRequest::new(
                symbol.clone(),
                Duration::from_secs(seconds),
                start_dt
                    .timestamp_nanos_opt()
                    .expect("start_dt should fit in i64"),
                end_dt
                    .timestamp_nanos_opt()
                    .expect("end_dt should fit in i64"),
            )
            .with_page_view_width(page_view_width)
            .with_timeout(Duration::from_secs(30)),
        )
        .await?;

    println!(
        "symbol={} duration_ns={} range=[{}, {}) rows={}",
        series.symbol(),
        series.duration_ns(),
        series.start_datetime_ns(),
        series.end_datetime_ns(),
        series.len()
    );
    for row in series.rows() {
        println!(
            "{}\t{}\t{}\t{}\t{}\t{}",
            row.id, row.datetime, row.open, row.high, row.low, row.close
        );
    }

    Ok(())
}
