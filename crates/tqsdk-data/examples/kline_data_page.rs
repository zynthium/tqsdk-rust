use std::error::Error;
use std::time::Duration;

use chrono::Utc;
use tqsdk_data::{DataClient, KlineDataPageRequest};
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
    let view_width = std::env::var("TQ_HISTORY_PAGE_VIEW_WIDTH")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(128);

    let session = SessionClientBuilder::new(auth_user, auth_pass)
        .futures_market()
        .build()?;
    let client = DataClient::from_session(session);
    let focus_datetime_ns = Utc::now()
        .timestamp_nanos_opt()
        .expect("focus datetime should fit in i64");
    let page = client
        .get_kline_data_page(
            KlineDataPageRequest::new(symbol.clone(), Duration::from_secs(seconds), view_width)
                .with_focus_datetime_ns(focus_datetime_ns)
                .with_focus_position(0)
                .with_timeout(Duration::from_secs(30)),
        )
        .await?;

    println!(
        "symbol={} duration_ns={} chart=[{}, {}] rows={}",
        page.symbol(),
        page.duration_ns(),
        page.chart_left_id(),
        page.chart_right_id(),
        page.len()
    );
    for row in page.rows() {
        println!(
            "{}\t{}\t{}\t{}\t{}\t{}",
            row.id, row.datetime, row.open, row.high, row.low, row.close
        );
    }

    Ok(())
}
