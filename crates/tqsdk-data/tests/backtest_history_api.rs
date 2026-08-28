use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tqsdk_data::{
    BacktestHistoryAuthProvider, BacktestHistoryClient, BacktestHistoryCredentials,
    BacktestHistoryEvent, BacktestHistoryFillCancellation, BacktestHistoryFillConfig,
    BacktestHistoryFillFamily, BacktestHistoryFillProgress, BacktestHistoryFillSymbolResult,
    BacktestHistoryFillSymbolStatus, BacktestHistoryFillTerminalReport,
    BacktestHistoryFillTerminalStatus, BacktestHistoryPolicy, BacktestHistoryRequest,
    BacktestTickCache, DataError,
};

#[test]
fn orchestration_config_defaults_are_bounded_and_explicit() {
    let config = BacktestHistoryFillConfig::default();

    assert_eq!(config.symbol_batch_size(), 1);
    assert_eq!(config.symbol_concurrency(), 2);
    assert_eq!(config.idle_timeout(), Duration::from_secs(60));
    assert_eq!(config.batch_timeout(), None);
    assert_eq!(config.lock_wait(), None);
}

#[test]
fn orchestration_config_rejects_invalid_values_without_clamping() {
    assert_validation(BacktestHistoryFillConfig::default().with_symbol_batch_size(0));
    assert_validation(BacktestHistoryFillConfig::default().with_symbol_batch_size(5));
    assert_validation(BacktestHistoryFillConfig::default().with_symbol_concurrency(0));
    assert_validation(BacktestHistoryFillConfig::default().with_symbol_concurrency(5));
    assert_validation(BacktestHistoryFillConfig::default().with_idle_timeout(Duration::ZERO));
    assert_validation(
        BacktestHistoryFillConfig::default().with_batch_timeout(Some(Duration::ZERO)),
    );
    assert_validation(BacktestHistoryFillConfig::default().with_lock_wait(Some(Duration::ZERO)));

    let config = BacktestHistoryFillConfig::default()
        .with_symbol_batch_size(4)
        .unwrap()
        .with_symbol_concurrency(4)
        .unwrap()
        .with_idle_timeout(Duration::from_secs(7))
        .unwrap()
        .with_batch_timeout(Some(Duration::from_secs(11)))
        .unwrap()
        .with_lock_wait(Some(Duration::from_secs(13)))
        .unwrap();
    assert_eq!(config.symbol_batch_size(), 4);
    assert_eq!(config.symbol_concurrency(), 4);
    assert_eq!(config.idle_timeout(), Duration::from_secs(7));
    assert_eq!(config.batch_timeout(), Some(Duration::from_secs(11)));
    assert_eq!(config.lock_wait(), Some(Duration::from_secs(13)));
    assert_eq!(config.without_batch_timeout().batch_timeout(), None);
}

#[test]
fn orchestration_cancellation_is_cloneable_and_monotonic() {
    let cancellation = BacktestHistoryFillCancellation::new();
    let observer = cancellation.clone();
    assert!(!observer.is_cancelled());
    cancellation.cancel();
    assert!(observer.is_cancelled());
}

#[test]
fn orchestration_progress_and_terminal_report_are_cache_family_neutral() {
    let progress = BacktestHistoryFillProgress::BatchStarted {
        family: BacktestHistoryFillFamily::Daily,
        batch_number: 1,
        total_batches: 1,
        requested_range: (1, 2),
        pending_batches: 0,
        active_batches: 1,
        symbols: vec!["KQ.i@SHFE.au".to_string()],
    };
    assert!(matches!(
        progress,
        BacktestHistoryFillProgress::BatchStarted {
            family: BacktestHistoryFillFamily::Daily,
            ..
        }
    ));

    let complete = BacktestHistoryFillSymbolResult {
        request_id: 7,
        symbol: "KQ.i@SHFE.au".to_string(),
        family: BacktestHistoryFillFamily::Daily,
        requested_range: (1, 2),
        status: BacktestHistoryFillSymbolStatus::Complete,
        rows_written: 1,
        remote_used: true,
        remote_filled_ranges: vec![(1, 2)],
        error: None,
    };
    let failed = BacktestHistoryFillSymbolResult {
        request_id: 8,
        symbol: "KQ.i@SHFE.ag".to_string(),
        family: BacktestHistoryFillFamily::Minute,
        requested_range: (1, 2),
        status: BacktestHistoryFillSymbolStatus::Failed,
        rows_written: 0,
        remote_used: false,
        remote_filled_ranges: Vec::new(),
        error: Some("fixture failure".to_string()),
    };
    let interrupted = BacktestHistoryFillSymbolResult {
        request_id: 9,
        symbol: "SHFE.cu2601".to_string(),
        family: BacktestHistoryFillFamily::Tick,
        requested_range: (1, 2),
        status: BacktestHistoryFillSymbolStatus::Interrupted,
        rows_written: 0,
        remote_used: false,
        remote_filled_ranges: Vec::new(),
        error: Some("cancelled".to_string()),
    };
    let report =
        BacktestHistoryFillTerminalReport::from_symbols(vec![complete, failed, interrupted]);

    assert_eq!(report.status(), BacktestHistoryFillTerminalStatus::Failed);
    assert_eq!(report.completed_symbols(), 1);
    assert_eq!(report.failed_symbols(), 1);
    assert_eq!(report.interrupted_symbols(), 1);
    assert_eq!(report.rows_written(), 1);
}

#[tokio::test]
async fn orchestration_run_isolates_symbol_failures_and_emits_all_family_progress() {
    let root = std::env::temp_dir().join(format!(
        "tqsdk-backtest-history-orchestration-failures-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let client = BacktestHistoryClient::builder(&root)
        .policy(BacktestHistoryPolicy::CacheOnly)
        .build()
        .unwrap();
    let progress = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&progress);

    let report = client
        .orchestrate_fill(
            [
                BacktestHistoryRequest::tick(101, "SHFE.au2602", 1, 2),
                BacktestHistoryRequest::kline(102, "SHFE.ag2602", Duration::from_secs(60), 1, 2),
                BacktestHistoryRequest::kline(
                    103,
                    "KQ.i@SHFE.cu",
                    Duration::from_secs(24 * 60 * 60),
                    1,
                    2,
                ),
            ],
            BacktestHistoryFillConfig::default(),
            BacktestHistoryFillCancellation::new(),
            move |event| observed.lock().unwrap().push(event),
        )
        .await
        .unwrap();

    assert_eq!(report.status(), BacktestHistoryFillTerminalStatus::Failed);
    assert_eq!(report.failed_symbols(), 3);
    assert_eq!(report.symbols().len(), 3);
    let progress = progress.lock().unwrap();
    for family in [
        BacktestHistoryFillFamily::Tick,
        BacktestHistoryFillFamily::Minute,
        BacktestHistoryFillFamily::Daily,
    ] {
        assert!(progress.iter().any(|event| matches!(
            event,
            BacktestHistoryFillProgress::Planning {
                family: observed,
                ..
            } if *observed == family
        )));
    }
    assert!(matches!(
        progress.last(),
        Some(BacktestHistoryFillProgress::Finished {
            status: BacktestHistoryFillTerminalStatus::Failed,
            failed_symbols: 3,
            ..
        })
    ));
}

#[tokio::test]
async fn orchestration_run_honors_preexisting_cancellation_without_touching_cache() {
    let root = std::env::temp_dir().join(format!(
        "tqsdk-backtest-history-orchestration-cancel-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let client = BacktestHistoryClient::builder(&root)
        .policy(BacktestHistoryPolicy::CacheOnly)
        .build()
        .unwrap();
    let cancellation = BacktestHistoryFillCancellation::new();
    cancellation.cancel();

    let report = client
        .orchestrate_fill(
            [BacktestHistoryRequest::tick(104, "SHFE.au2602", 1, 2)],
            BacktestHistoryFillConfig::default(),
            cancellation,
            |_| {},
        )
        .await
        .unwrap();

    assert_eq!(
        report.status(),
        BacktestHistoryFillTerminalStatus::Interrupted
    );
    assert_eq!(report.interrupted_symbols(), 1);
    assert!(!root.exists());
}

#[tokio::test]
async fn orchestration_run_uses_validated_batch_size_for_one_cache_family() {
    let root = std::env::temp_dir().join(format!(
        "tqsdk-backtest-history-orchestration-batches-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let client = BacktestHistoryClient::builder(&root)
        .policy(BacktestHistoryPolicy::CacheOnly)
        .build()
        .unwrap();
    let progress = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&progress);
    let config = BacktestHistoryFillConfig::default()
        .with_symbol_batch_size(2)
        .unwrap();

    let report = client
        .orchestrate_fill(
            [
                BacktestHistoryRequest::tick(105, "SHFE.au2602", 1, 2),
                BacktestHistoryRequest::tick(106, "SHFE.ag2602", 1, 2),
                BacktestHistoryRequest::tick(107, "SHFE.cu2602", 1, 2),
            ],
            config,
            BacktestHistoryFillCancellation::new(),
            move |event| observed.lock().unwrap().push(event),
        )
        .await
        .unwrap();

    assert_eq!(report.symbols().len(), 3);
    let progress = progress.lock().unwrap();
    assert_eq!(
        progress
            .iter()
            .filter(|event| matches!(event, BacktestHistoryFillProgress::BatchStarted { .. }))
            .count(),
        2
    );
    assert!(progress.iter().any(|event| matches!(
        event,
        BacktestHistoryFillProgress::Planning {
            total_batches: 2,
            symbol_batch_size: 2,
            ..
        }
    )));
}

#[tokio::test]
async fn orchestration_cancellation_interrupts_root_lock_wait() {
    let root = std::env::temp_dir().join(format!(
        "tqsdk-backtest-history-orchestration-lock-wait-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let cache = BacktestTickCache::open(&root).unwrap();
    let exclusive = cache.try_acquire_consistency_read_lock().unwrap();
    let client = BacktestHistoryClient::builder(&root).build().unwrap();
    let cancellation = BacktestHistoryFillCancellation::new();
    let signal = cancellation.clone();
    let config = BacktestHistoryFillConfig::default()
        .with_lock_wait(Some(Duration::from_secs(5)))
        .unwrap();
    let task = tokio::spawn(async move {
        client
            .orchestrate_fill(
                [BacktestHistoryRequest::tick(108, "SHFE.au2602", 1, 2)],
                config,
                cancellation,
                |_| {},
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    signal.cancel();
    let report = task.await.unwrap().unwrap();

    assert_eq!(
        report.status(),
        BacktestHistoryFillTerminalStatus::Interrupted
    );
    assert_eq!(report.interrupted_symbols(), 1);
    drop(exclusive);
    let _ = std::fs::remove_dir_all(root);
}

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
async fn remote_on_miss_run_joins_the_shared_cache_root_gate() {
    let root = std::env::temp_dir().join(format!(
        "tqsdk-backtest-history-api-root-gate-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let cache = BacktestTickCache::open(&root).unwrap();
    let exclusive = cache.try_acquire_consistency_read_lock().unwrap();
    let client = BacktestHistoryClient::builder(root.clone())
        .build()
        .unwrap();

    let result = client
        .query(BacktestHistoryRequest::tick(8, "SHFE.au2602", 1, 2))
        .await;

    assert!(matches!(result, Err(DataError::CacheBusy { .. })));
    drop(exclusive);
    let _ = std::fs::remove_dir_all(root);
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
