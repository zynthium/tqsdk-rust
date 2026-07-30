use std::time::Duration;

use tqsdk_data::{
    BacktestHistoryClient, BacktestHistoryEvent, BacktestHistoryPolicy, BacktestHistoryRequest,
    Result,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cache_dir = ".tqsdk/backtest-history";
    let start_ns = 1_000_i64;
    let end_ns = 2_000_i64;
    let client = BacktestHistoryClient::builder(cache_dir)
        .policy(BacktestHistoryPolicy::RemoteOnMiss)
        .auth_env()
        .build()?;

    let requests = [
        BacktestHistoryRequest::tick(1, "KQ.i@SHFE.au", start_ns, end_ns),
        BacktestHistoryRequest::kline(2, "KQ.i@SHFE.au", Duration::from_secs(15), start_ns, end_ns),
        BacktestHistoryRequest::kline(
            3,
            "KQ.i@SHFE.au",
            Duration::from_secs(5 * 60),
            start_ns,
            end_ns,
        ),
    ];
    let mut run = client.query_batch(requests).await?;
    while let Some(event) = run.next().await {
        match event {
            BacktestHistoryEvent::Chunk(chunk) => {
                println!("request {}: {} rows", chunk.request_id, chunk.rows.len());
            }
            BacktestHistoryEvent::RequestCompleted(report) => {
                println!(
                    "request {} completed with {} rows",
                    report.request_id, report.rows
                );
            }
            BacktestHistoryEvent::RequestFailed(failure) => {
                eprintln!("request {} failed: {}", failure.request_id, failure.error);
            }
        }
    }
    let _report = run.finish().await;

    // Batch materialization is available as `run.collect_all(max_total_bytes)`;
    // callers must always supply the total memory limit explicitly.
    Ok(())
}
