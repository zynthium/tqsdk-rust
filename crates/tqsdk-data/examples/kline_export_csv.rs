use std::error::Error;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use tokio::fs::File;
use tqsdk_data::{DataClient, KlineDataSeriesRequest};
use tqsdk_session::SessionClientBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let auth_user = std::env::var("TQ_AUTH_USER")?;
    let auth_pass = std::env::var("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_TEST_SYMBOL").unwrap_or_else(|_| "SHFE.ao2609".to_string());
    let output_path = std::env::var("TQ_EXPORT_PATH")
        .unwrap_or_else(|_| "/tmp/tqsdk-kline-export.csv".to_string());
    let end_dt = Utc::now();
    let start_dt = end_dt - ChronoDuration::minutes(60);

    let session = SessionClientBuilder::new(auth_user, auth_pass)
        .market_target(false, false)
        .build()?;
    let client = DataClient::from_session(session);
    let mut file = File::create(output_path.as_str()).await?;

    let summary = client
        .export_kline_data_csv(
            KlineDataSeriesRequest::new(
                symbol.as_str(),
                Duration::from_secs(60),
                start_dt
                    .timestamp_nanos_opt()
                    .expect("start_dt should fit in i64"),
                end_dt
                    .timestamp_nanos_opt()
                    .expect("end_dt should fit in i64"),
            )
            .with_page_view_width(128)
            .with_timeout(Duration::from_secs(30)),
            &mut file,
        )
        .await?;

    println!(
        "symbol={} rows={} pages={} output={}",
        summary.symbol, summary.rows_written, summary.pages_written, output_path
    );

    Ok(())
}
