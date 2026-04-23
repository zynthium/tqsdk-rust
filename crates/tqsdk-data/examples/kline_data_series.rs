use std::error::Error;
use std::time::Duration;

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
    let data_length = std::env::var("TQ_KLINE_LENGTH")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(16);

    let session = SessionClientBuilder::new(auth_user, auth_pass).build()?;
    let client = DataClient::from_session(session);
    let series = client
        .get_kline_data_series(
            KlineDataSeriesRequest::new(symbol.clone(), Duration::from_secs(seconds), data_length)
                .with_timeout(Duration::from_secs(30)),
        )
        .await?;

    println!(
        "symbol={} duration_ns={} rows={}",
        series.symbol(),
        series.duration_ns(),
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
