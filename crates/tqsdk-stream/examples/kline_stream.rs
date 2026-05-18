use std::error::Error;
use std::time::Duration;

use futures::StreamExt;
use tqsdk_stream::TqStreamBuilder;

fn read_env(key: &str) -> Result<String, Box<dyn Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

fn read_usize_env(key: &str, default: usize) -> Result<usize, Box<dyn Error>> {
    match std::env::var(key) {
        Ok(raw) => Ok(raw.parse()?),
        Err(_) => Ok(default),
    }
}

fn read_u64_env(key: &str, default: u64) -> Result<u64, Box<dyn Error>> {
    match std::env::var(key) {
        Ok(raw) => Ok(raw.parse()?),
        Err(_) => Ok(default),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_TEST_SYMBOL").unwrap_or_else(|_| "SHFE.ao2609".to_string());
    let view_width = read_usize_env("TQ_HISTORY_VIEW_WIDTH", 64)?;
    let timeout_secs = read_u64_env("TQ_STREAM_TIMEOUT_SECS", 30)?;

    let stream = TqStreamBuilder::new(user, pass).build().await?;
    let mut batches = stream
        .kline_stream(&symbol, Duration::from_secs(60), view_width)
        .await?;
    let chart_id = batches.chart_id().to_string();

    let update = match tokio::time::timeout(Duration::from_secs(timeout_secs), batches.next()).await
    {
        Ok(Some(update)) => update?,
        Ok(None) => return Err("kline stream closed".into()),
        Err(_) => {
            let snapshot = stream.reader().read();
            let chart = snapshot.get_path(&["charts", chart_id.as_str()]).cloned();
            return Err(format!(
                "timed out waiting for ready kline window: chart_id={chart_id} chart={chart:?}"
            )
            .into());
        }
    };
    let last = update.value.rows().last().ok_or("kline batch is empty")?;

    println!(
        "revision={:?} chart_id={} symbol={} width={} kind={:?} rows={} last_datetime={} last_close={}",
        update.commit.revision,
        update.value.chart_id(),
        update.value.symbol(),
        update.value.view_width(),
        update.value.kind(),
        update.value.len(),
        last.datetime,
        last.close
    );

    batches.close().await?;

    Ok(())
}
