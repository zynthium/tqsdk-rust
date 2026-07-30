use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tqsdk_data::{
    BacktestHistoryAuthProvider, BacktestHistoryClient, BacktestHistoryCredentials,
    BacktestHistoryEvent, BacktestHistoryPolicy, BacktestHistoryRequest, DataError,
};

#[tokio::test]
async fn local_query_contract_is_available_without_remote_configuration() {
    let root = std::env::temp_dir().join(format!(
        "tqsdk-backtest-history-api-contract-{}",
        std::process::id()
    ));
    let client = BacktestHistoryClient::builder(root)
        .policy(BacktestHistoryPolicy::CacheOnly)
        .logical_concurrency(32)
        .blocking_workers(1)
        .per_symbol_buffer_bytes(16 * 1024 * 1024)
        .collect_limit_bytes(512 * 1024 * 1024)
        .build()
        .unwrap();

    let request =
        BacktestHistoryRequest::kline(7, "SHFE.au2602", Duration::from_secs(15), 1_000, 2_000);
    let mut run = client.query(request).await.unwrap();
    while let Some(event) = run.next().await {
        assert!(matches!(
            event,
            BacktestHistoryEvent::Chunk(_)
                | BacktestHistoryEvent::RequestCompleted(_)
                | BacktestHistoryEvent::RequestFailed(_)
        ));
    }
    let report = run.finish().await;
    assert_eq!(report.completed.len() + report.failed.len(), 1);
}

#[tokio::test]
async fn request_and_builder_validation_happen_before_a_run_starts() {
    let root = std::env::temp_dir().join(format!(
        "tqsdk-backtest-history-api-validation-{}",
        std::process::id()
    ));
    assert_validation(
        BacktestHistoryClient::builder(root.clone())
            .logical_concurrency(0)
            .build(),
    );

    let client = BacktestHistoryClient::builder(root)
        .policy(BacktestHistoryPolicy::CacheOnly)
        .build()
        .unwrap();
    assert_validation(
        client
            .query(BacktestHistoryRequest::tick(1, "", 1, 2))
            .await,
    );

    assert_validation(
        client
            .query(BacktestHistoryRequest::kline(
                1,
                "SHFE.au2602",
                Duration::ZERO,
                1,
                2,
            ))
            .await,
    );

    assert_validation(
        client
            .query_batch([
                BacktestHistoryRequest::tick(7, "SHFE.au2602", 1, 2),
                BacktestHistoryRequest::tick(7, "SHFE.ag2602", 1, 2),
            ])
            .await,
    );

    assert_validation(
        client
            .query(
                BacktestHistoryRequest::tick(8, "SHFE.au2602", 10, 20)
                    .with_provisional_as_of_ns(21),
            )
            .await,
    );
}

#[tokio::test]
async fn cache_only_run_does_not_load_configured_credentials() {
    let calls = Arc::new(AtomicUsize::new(0));
    let provider = CountingAuthProvider {
        calls: Arc::clone(&calls),
    };
    let client = BacktestHistoryClient::builder(std::env::temp_dir().join(format!(
        "tqsdk-backtest-history-api-auth-{}",
        std::process::id()
    )))
    .policy(BacktestHistoryPolicy::CacheOnly)
    .auth_provider(provider)
    .build()
    .unwrap();

    let report = client
        .query(BacktestHistoryRequest::tick(9, "SHFE.au2602", 1, 2))
        .await
        .unwrap()
        .finish()
        .await;
    assert_eq!(report.failed.len(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn collect_enforces_its_single_request_contract() {
    let client = BacktestHistoryClient::builder(std::env::temp_dir().join(format!(
        "tqsdk-backtest-history-api-collect-{}",
        std::process::id()
    )))
    .policy(BacktestHistoryPolicy::CacheOnly)
    .build()
    .unwrap();

    let error = client
        .query(BacktestHistoryRequest::tick(10, "SHFE.au2602", 1, 2))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(matches!(error, DataError::RequestFailed { .. }));

    let error = client
        .query_batch([
            BacktestHistoryRequest::tick(11, "SHFE.au2602", 1, 2),
            BacktestHistoryRequest::tick(12, "SHFE.ag2602", 1, 2),
        ])
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(matches!(error, DataError::Validation(_)));

    let error = client
        .query(BacktestHistoryRequest::tick(13, "SHFE.au2602", 1, 2))
        .await
        .unwrap()
        .collect_all(0)
        .await
        .unwrap_err();
    assert!(matches!(error, DataError::Validation(_)));
}

#[cfg(not(all(feature = "live", feature = "services")))]
#[tokio::test]
async fn remote_on_miss_reports_feature_unavailability_before_loading_authentication() {
    let client = BacktestHistoryClient::builder(std::env::temp_dir().join(format!(
        "tqsdk-backtest-history-api-feature-gate-{}",
        std::process::id()
    )))
    .build()
    .unwrap();

    let error = client
        .query(BacktestHistoryRequest::tick(14, "SHFE.au2602", 1, 2))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        DataError::RequestFailed { message, .. }
            if message.contains("remote backtest history fill requires")
    ));
}

#[cfg(not(all(feature = "live", feature = "services")))]
#[tokio::test]
async fn remote_kq_main_metadata_miss_reports_feature_unavailability_before_loading_authentication()
{
    let calls = Arc::new(AtomicUsize::new(0));
    let client = BacktestHistoryClient::builder(std::env::temp_dir().join(format!(
        "tqsdk-backtest-history-api-kq-main-feature-gate-{}",
        std::process::id()
    )))
    .auth_provider(CountingAuthProvider {
        calls: Arc::clone(&calls),
    })
    .build()
    .unwrap();

    let error = client
        .query(BacktestHistoryRequest::tick(15, "KQ.m@SHFE.au", 1, 2))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        DataError::RequestFailed { message, .. }
            if message.contains("remote backtest history fill requires")
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

struct CountingAuthProvider {
    calls: Arc<AtomicUsize>,
}

fn assert_validation<T>(result: tqsdk_data::Result<T>) {
    assert!(matches!(result, Err(DataError::Validation(_))));
}

impl BacktestHistoryAuthProvider for CountingAuthProvider {
    fn load<'a>(
        &'a self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = tqsdk_data::Result<BacktestHistoryCredentials>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(BacktestHistoryCredentials::new("unused", "unused"))
        })
    }
}
