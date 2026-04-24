use std::error::Error;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use tqsdk_data::{DataClient, TickDataSeriesRequest};
use tqsdk_session::SessionClientBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let auth_user = std::env::var("TQ_AUTH_USER")?;
    let auth_pass = std::env::var("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_TEST_SYMBOL").unwrap_or_else(|_| "SHFE.ao2609".to_string());
    let lookback_minutes = std::env::var("TQ_HISTORY_LOOKBACK_MINUTES")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(30);
    let page_view_width = std::env::var("TQ_HISTORY_PAGE_VIEW_WIDTH")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(128);

    let session = SessionClientBuilder::new(auth_user, auth_pass)
        .futures_market()
        .build()?;
    let client = DataClient::from_session(session);
    let end_dt = Utc::now();
    let start_dt = end_dt - ChronoDuration::minutes(lookback_minutes);

    let mut download = client.tick_data_download(
        TickDataSeriesRequest::new(
            symbol.clone(),
            start_dt
                .timestamp_nanos_opt()
                .expect("start_dt should fit in i64"),
            end_dt
                .timestamp_nanos_opt()
                .expect("end_dt should fit in i64"),
        )
        .with_page_view_width(page_view_width)
        .with_timeout(Duration::from_secs(30)),
    )?;

    while let Some(page) = download.next_page().await? {
        let progress = page.progress();
        println!(
            "page={} page_rows={} total_rows={} progress={:.2}%",
            progress.emitted_pages(),
            page.len(),
            progress.emitted_rows(),
            progress.completion_percent()
        );
        for row in page.rows() {
            println!(
                "id={} datetime={} last_price={} volume={} open_interest={}",
                row.id, row.datetime, row.last_price, row.volume, row.open_interest
            );
        }
    }

    let progress = download.progress();
    println!(
        "done symbol={} range=[{}, {}) rows={}",
        download.symbol(),
        download.start_datetime_ns(),
        download.end_datetime_ns(),
        progress.emitted_rows()
    );

    Ok(())
}
