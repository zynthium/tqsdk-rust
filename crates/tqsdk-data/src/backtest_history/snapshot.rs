//! Validated read-only snapshot handle and strict no-row inspection.

use std::fmt;
use std::path::Path;

use super::executor::{StrictInspectionFailure, strict_inspect_request};
use super::metadata::{StrictMetadataErrorKind, validate_active_snapshot_strict};
use super::planner;
use super::report::{
    BacktestHistoryChunk, BacktestHistoryCollected, BacktestHistoryEvent,
    BacktestHistoryFailureReason, BacktestHistoryRequestReport,
};
use super::request::{BacktestHistoryPolicy, BacktestHistoryRequest};
use super::snapshot_manifest::{
    BacktestHistorySnapshotFileRole, BacktestHistorySnapshotManifestBuilder, SnapshotManifestError,
    SnapshotManifestErrorKind, ValidatedSnapshotManifest, open_current_manifest,
    open_generation_manifest,
};
use super::{
    BacktestHistoryClient, BacktestHistoryFailureReasons, BacktestHistoryRun,
    BacktestHistorySnapshotQueryResources, classify_snapshot_failure,
};
use crate::DataError;

/// Typed failure returned by the strict snapshot seam.
#[derive(Debug, Clone)]
pub struct BacktestHistorySnapshotError {
    reason: BacktestHistoryFailureReason,
    request_id: Option<u64>,
    symbol: Option<String>,
    message: String,
}

impl BacktestHistorySnapshotError {
    fn new(
        reason: BacktestHistoryFailureReason,
        request_id: Option<u64>,
        symbol: Option<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            reason,
            request_id,
            symbol,
            message: message.into(),
        }
    }

    #[must_use]
    pub const fn reason(&self) -> &BacktestHistoryFailureReason {
        &self.reason
    }

    #[must_use]
    pub const fn request_id(&self) -> Option<u64> {
        self.request_id
    }

    #[must_use]
    pub fn symbol(&self) -> Option<&str> {
        self.symbol.as_deref()
    }
}

impl fmt::Display for BacktestHistorySnapshotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message.as_str())
    }
}

impl std::error::Error for BacktestHistorySnapshotError {}

/// Successful strict inspection. No history rows are scanned or retained.
#[derive(Debug, Clone)]
pub struct BacktestHistoryInspection {
    report: BacktestHistoryRequestReport,
}

impl BacktestHistoryInspection {
    #[must_use]
    pub const fn report(&self) -> &BacktestHistoryRequestReport {
        &self.report
    }

    #[must_use]
    pub fn into_report(self) -> BacktestHistoryRequestReport {
        self.report
    }
}

/// Typed event emitted by a query pinned to one immutable snapshot.
#[derive(Debug, Clone)]
pub enum BacktestHistorySnapshotEvent {
    /// Provisional rows; terminal success still must follow.
    Chunk(BacktestHistoryChunk),
    /// The request completed successfully.
    RequestCompleted(BacktestHistoryRequestReport),
    /// The request failed with a machine-readable snapshot reason.
    RequestFailed {
        error: BacktestHistorySnapshotError,
        emitted_rows: usize,
    },
}

/// Single-request run that preserves typed terminal snapshot failures.
pub struct BacktestHistorySnapshotRun {
    inner: BacktestHistoryRun,
    failure_reasons: BacktestHistoryFailureReasons,
    request_id: u64,
    symbol: String,
}

impl fmt::Debug for BacktestHistorySnapshotRun {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BacktestHistorySnapshotRun")
            .field("request_id", &self.request_id)
            .field("symbol", &self.symbol)
            .finish_non_exhaustive()
    }
}

impl BacktestHistorySnapshotRun {
    fn new(inner: BacktestHistoryRun, request_id: u64, symbol: String) -> Self {
        let failure_reasons = inner.failure_reasons();
        Self {
            inner,
            failure_reasons,
            request_id,
            symbol,
        }
    }

    /// Receives the next chunk or typed terminal event.
    pub async fn next(&mut self) -> Option<BacktestHistorySnapshotEvent> {
        self.inner.next().await.map(|event| match event {
            BacktestHistoryEvent::Chunk(chunk) => BacktestHistorySnapshotEvent::Chunk(chunk),
            BacktestHistoryEvent::RequestCompleted(report) => {
                BacktestHistorySnapshotEvent::RequestCompleted(report)
            }
            BacktestHistoryEvent::RequestFailed(failure) => {
                BacktestHistorySnapshotEvent::RequestFailed {
                    error: snapshot_terminal_error(
                        &self.failure_reasons,
                        failure.request_id,
                        failure.symbol,
                        failure.error,
                    ),
                    emitted_rows: failure.emitted_rows,
                }
            }
        })
    }

    /// Collects the single successful response using the configured byte cap.
    pub async fn collect(self) -> Result<BacktestHistoryCollected, BacktestHistorySnapshotError> {
        let failure_reasons = self.failure_reasons;
        let request_id = self.request_id;
        let symbol = self.symbol;
        match self.inner.collect().await {
            Ok(collected) => Ok(collected),
            Err(DataError::RequestFailed {
                request_id,
                message,
                ..
            }) => Err(snapshot_terminal_error(
                &failure_reasons,
                request_id,
                symbol,
                message,
            )),
            Err(error) => Err(BacktestHistorySnapshotError::new(
                classify_snapshot_failure(&error, false),
                Some(request_id),
                Some(symbol),
                error.to_string(),
            )),
        }
    }

    /// Drains the run and returns its sole terminal success or typed failure.
    pub async fn finish(
        self,
    ) -> Result<BacktestHistoryRequestReport, BacktestHistorySnapshotError> {
        let failure_reasons = self.failure_reasons;
        let request_id = self.request_id;
        let symbol = self.symbol;
        let mut report = self.inner.finish().await;
        if let Some(failure) = report.failed.pop() {
            return Err(snapshot_terminal_error(
                &failure_reasons,
                failure.request_id,
                failure.symbol,
                failure.error,
            ));
        }
        report.completed.pop().ok_or_else(|| {
            BacktestHistorySnapshotError::new(
                BacktestHistoryFailureReason::Internal,
                Some(request_id),
                Some(symbol),
                "snapshot query ended without a terminal result",
            )
        })
    }
}

/// Validated, immutable snapshot generation opened through `CURRENT`.
#[derive(Clone)]
pub struct BacktestHistorySnapshot {
    manifest: ValidatedSnapshotManifest,
    client: BacktestHistoryClient,
}

impl fmt::Debug for BacktestHistorySnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BacktestHistorySnapshot")
            .field("snapshot_id", &self.manifest.snapshot_id())
            .field("identity_sha256", &self.manifest.identity_sha256())
            .finish_non_exhaustive()
    }
}

impl BacktestHistorySnapshot {
    /// Opens and fully validates the generation selected by `CURRENT`.
    pub fn open(history_root: impl AsRef<Path>) -> Result<Self, BacktestHistorySnapshotError> {
        let manifest = open_current_manifest(history_root.as_ref()).map_err(map_manifest_error)?;
        Self::from_validated_manifest(manifest)
    }

    /// Opens and fully validates one unpublished staging or retained generation.
    ///
    /// The generation must be a direct child of `history_root/staging` or
    /// `history_root/snapshots`; this does not read or mutate `CURRENT`.
    pub fn open_generation(
        history_root: impl AsRef<Path>,
        generation_dir: impl AsRef<Path>,
    ) -> Result<Self, BacktestHistorySnapshotError> {
        let manifest = open_generation_manifest(history_root.as_ref(), generation_dir.as_ref())
            .map_err(map_manifest_error)?;
        Self::from_validated_manifest(manifest)
    }

    pub(crate) fn from_validated_manifest(
        manifest: ValidatedSnapshotManifest,
    ) -> Result<Self, BacktestHistorySnapshotError> {
        debug_assert_eq!(
            manifest.cache_dir().parent(),
            Some(manifest.generation_dir())
        );
        for symbol in manifest.catalog_symbols() {
            validate_active_snapshot_strict(manifest.cache_dir(), symbol.as_str())
                .map_err(|error| map_metadata_error(error, None, Some(symbol.clone())))?;
        }
        let client = BacktestHistoryClient::builder(manifest.cache_dir())
            .policy(BacktestHistoryPolicy::CacheOnly)
            // Snapshot queries are intentionally one request per run. Keeping
            // the logical concurrency at one also fixes the bounded event
            // queue at two entries for daemon memory accounting.
            .logical_concurrency(1)
            .build()
            .map_err(|error| {
                BacktestHistorySnapshotError::new(
                    BacktestHistoryFailureReason::SnapshotUnavailable,
                    None,
                    None,
                    error.to_string(),
                )
            })?;
        Ok(Self { manifest, client })
    }

    #[must_use]
    pub fn snapshot_id(&self) -> &str {
        self.manifest.snapshot_id()
    }

    #[must_use]
    pub fn identity_sha256(&self) -> &str {
        self.manifest.identity_sha256()
    }

    /// UTC creation time recorded in the validated manifest.
    #[must_use]
    pub fn created_at(&self) -> &str {
        self.manifest.created_at()
    }

    #[must_use]
    pub fn metadata_snapshot_hash(&self) -> &str {
        self.manifest.metadata_snapshot_hash()
    }

    #[must_use]
    pub const fn catalog_complete(&self) -> bool {
        self.manifest.catalog_complete()
    }

    /// Explicit symbol universe recorded by the validated manifest.
    #[must_use]
    pub fn catalog_symbols(&self) -> &[String] {
        self.manifest.catalog_symbols()
    }

    /// Distinct data and metadata file roles present in this generation.
    #[must_use]
    pub fn file_roles(&self) -> &[BacktestHistorySnapshotFileRole] {
        self.manifest.file_roles()
    }

    /// Rebuild recipe preserving the validated creation/catalog contract.
    #[doc(hidden)]
    #[must_use]
    pub fn manifest_builder(&self) -> BacktestHistorySnapshotManifestBuilder {
        self.manifest.manifest_builder()
    }

    /// Validates request semantics, catalog authority, metadata, coverage and finality without
    /// scanning row payloads.
    pub async fn inspect(
        &self,
        request: BacktestHistoryRequest,
    ) -> Result<BacktestHistoryInspection, BacktestHistorySnapshotError> {
        let validated = request.validate().map_err(|error| {
            BacktestHistorySnapshotError::new(
                BacktestHistoryFailureReason::InvalidRequest,
                None,
                None,
                error.to_string(),
            )
        })?;
        let request_id = validated.request_id;
        let symbol = validated.symbol.clone();
        planner::validate_source_policy(&validated).map_err(|error| {
            BacktestHistorySnapshotError::new(
                BacktestHistoryFailureReason::InvalidRequest,
                Some(request_id),
                Some(symbol.clone()),
                error.to_string(),
            )
        })?;

        if !self.manifest.catalog_contains(symbol.as_str()) {
            let reason = if self.manifest.catalog_complete() {
                BacktestHistoryFailureReason::SymbolNotFound
            } else {
                BacktestHistoryFailureReason::MetadataIncomplete
            };
            return Err(BacktestHistorySnapshotError::new(
                reason,
                Some(request_id),
                Some(symbol),
                "requested symbol is absent from the snapshot catalog",
            ));
        }
        let report = strict_inspect_request(self.client.config.clone(), validated)
            .await
            .map_err(|failure| map_inspection_failure(request_id, symbol.as_str(), failure))?;
        Ok(BacktestHistoryInspection { report })
    }

    /// Starts a cache-only query pinned to this immutable generation.
    pub async fn query(
        &self,
        request: BacktestHistoryRequest,
    ) -> Result<BacktestHistorySnapshotRun, BacktestHistorySnapshotError> {
        self.query_inner(request, None).await
    }

    /// Starts a cache-only query pinned to this immutable generation using a
    /// caller-shared, non-blocking scan allocation budget.
    pub async fn query_with_resources(
        &self,
        request: BacktestHistoryRequest,
        resources: BacktestHistorySnapshotQueryResources,
    ) -> Result<BacktestHistorySnapshotRun, BacktestHistorySnapshotError> {
        self.query_inner(request, Some(resources)).await
    }

    async fn query_inner(
        &self,
        request: BacktestHistoryRequest,
        resources: Option<BacktestHistorySnapshotQueryResources>,
    ) -> Result<BacktestHistorySnapshotRun, BacktestHistorySnapshotError> {
        let inspection = self.inspect(request.clone()).await?;
        let request_id = inspection.report().request_id;
        let symbol = inspection.report().symbol.clone();
        let lifecycle_pin = self.manifest.lifecycle_pin();
        let run = match resources {
            Some(resources) => {
                self.client
                    .query_with_lifecycle_pin_and_resources(request, lifecycle_pin, resources)
                    .await
            }
            None => {
                self.client
                    .query_with_lifecycle_pin(request, lifecycle_pin)
                    .await
            }
        }
        .map_err(|error| {
            let (reason, message) = map_planning_error(error);
            BacktestHistorySnapshotError::new(
                reason,
                Some(request_id),
                Some(symbol.clone()),
                message,
            )
        })?;
        Ok(BacktestHistorySnapshotRun::new(run, request_id, symbol))
    }
}

fn snapshot_terminal_error(
    failure_reasons: &BacktestHistoryFailureReasons,
    request_id: u64,
    symbol: String,
    message: String,
) -> BacktestHistorySnapshotError {
    let reason = failure_reasons
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&request_id)
        .cloned()
        .unwrap_or(BacktestHistoryFailureReason::Internal);
    BacktestHistorySnapshotError::new(reason, Some(request_id), Some(symbol), message)
}

fn map_metadata_error(
    error: super::metadata::StrictMetadataError,
    request_id: Option<u64>,
    symbol: Option<String>,
) -> BacktestHistorySnapshotError {
    let reason = match error.kind() {
        StrictMetadataErrorKind::Corrupt => BacktestHistoryFailureReason::SnapshotCorrupt,
        StrictMetadataErrorKind::Incompatible => BacktestHistoryFailureReason::SnapshotIncompatible,
        StrictMetadataErrorKind::Missing => BacktestHistoryFailureReason::MetadataIncomplete,
    };
    BacktestHistorySnapshotError::new(reason, request_id, symbol, error.message())
}

pub(crate) fn map_manifest_error(error: SnapshotManifestError) -> BacktestHistorySnapshotError {
    let reason = match error.kind() {
        SnapshotManifestErrorKind::Unavailable => BacktestHistoryFailureReason::SnapshotUnavailable,
        SnapshotManifestErrorKind::Corrupt => BacktestHistoryFailureReason::SnapshotCorrupt,
        SnapshotManifestErrorKind::Incompatible => {
            BacktestHistoryFailureReason::SnapshotIncompatible
        }
    };
    BacktestHistorySnapshotError::new(reason, None, None, error.message())
}

fn map_inspection_failure(
    request_id: u64,
    symbol: &str,
    failure: StrictInspectionFailure,
) -> BacktestHistorySnapshotError {
    let (reason, message) = match failure {
        StrictInspectionFailure::CoverageIncomplete(missing_ranges) => (
            BacktestHistoryFailureReason::CoverageIncomplete { missing_ranges },
            "snapshot cache coverage is incomplete".to_string(),
        ),
        StrictInspectionFailure::Provisional { as_of_ns } => (
            BacktestHistoryFailureReason::ProvisionalData { as_of_ns },
            "snapshot request would return provisional data".to_string(),
        ),
        StrictInspectionFailure::Planning(error) => map_planning_error(error),
        StrictInspectionFailure::Source(error) => map_source_error(error),
    };
    BacktestHistorySnapshotError::new(reason, Some(request_id), Some(symbol.to_string()), message)
}

fn map_planning_error(error: DataError) -> (BacktestHistoryFailureReason, String) {
    let reason = match &error {
        DataError::Validation(_) => BacktestHistoryFailureReason::InvalidRequest,
        DataError::InvalidState(_) | DataError::CacheMiss(_) => {
            BacktestHistoryFailureReason::MetadataIncomplete
        }
        DataError::InvalidResponse(_) | DataError::Io(_) | DataError::Json(_) => {
            BacktestHistoryFailureReason::SnapshotCorrupt
        }
        DataError::CacheBusy { .. }
        | DataError::FeatureDisabled(_)
        | DataError::RemoteBacktestHistoryFillUnavailable => {
            BacktestHistoryFailureReason::SnapshotUnavailable
        }
        DataError::Timeout(_) => BacktestHistoryFailureReason::HistoryTimeout,
        DataError::Session(_)
        | DataError::PermissionDenied(_)
        | DataError::CollectLimitExceeded { .. }
        | DataError::RequestFailed { .. } => BacktestHistoryFailureReason::Internal,
        #[cfg(feature = "services")]
        DataError::Http(_) => BacktestHistoryFailureReason::Internal,
    };
    (reason, error.to_string())
}

fn map_source_error(error: DataError) -> (BacktestHistoryFailureReason, String) {
    let reason = match &error {
        DataError::CacheMiss(miss) => BacktestHistoryFailureReason::CoverageIncomplete {
            missing_ranges: miss.missing_ranges.clone(),
        },
        DataError::InvalidResponse(_) | DataError::Io(_) | DataError::Json(_) => {
            BacktestHistoryFailureReason::SnapshotCorrupt
        }
        DataError::InvalidState(_) | DataError::Validation(_) => {
            BacktestHistoryFailureReason::SnapshotIncompatible
        }
        DataError::CacheBusy { .. }
        | DataError::FeatureDisabled(_)
        | DataError::RemoteBacktestHistoryFillUnavailable => {
            BacktestHistoryFailureReason::SnapshotUnavailable
        }
        DataError::Timeout(_) => BacktestHistoryFailureReason::HistoryTimeout,
        DataError::Session(_)
        | DataError::PermissionDenied(_)
        | DataError::CollectLimitExceeded { .. }
        | DataError::RequestFailed { .. } => BacktestHistoryFailureReason::Internal,
        #[cfg(feature = "services")]
        DataError::Http(_) => BacktestHistoryFailureReason::Internal,
    };
    (reason, error.to_string())
}

#[cfg(test)]
mod resource_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::{DailyKlineCache, DailyKlineCacheSnapshot};

    struct RejectingBudget;

    impl super::super::BacktestHistorySnapshotResourceBudget for RejectingBudget {
        fn try_reserve(
            &self,
            _bytes: usize,
        ) -> Option<super::super::BacktestHistorySnapshotResourceReservation> {
            None
        }
    }

    struct AllowingBudget;

    impl super::super::BacktestHistorySnapshotResourceBudget for AllowingBudget {
        fn try_reserve(
            &self,
            _bytes: usize,
        ) -> Option<super::super::BacktestHistorySnapshotResourceReservation> {
            Some(super::super::BacktestHistorySnapshotResourceReservation::new(()))
        }
    }

    pub(super) struct TrackingBudget {
        pub(super) used: Arc<AtomicUsize>,
    }

    impl super::super::BacktestHistorySnapshotResourceBudget for TrackingBudget {
        fn try_reserve(
            &self,
            bytes: usize,
        ) -> Option<super::super::BacktestHistorySnapshotResourceReservation> {
            self.used.fetch_add(bytes, Ordering::AcqRel);
            Some(
                super::super::BacktestHistorySnapshotResourceReservation::new(TrackedReservation {
                    used: Arc::clone(&self.used),
                    bytes,
                }),
            )
        }
    }

    struct TrackedReservation {
        used: Arc<AtomicUsize>,
        bytes: usize,
    }

    impl Drop for TrackedReservation {
        fn drop(&mut self) {
            self.used.fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }

    struct ActivePin(Arc<AtomicBool>);

    impl Drop for ActivePin {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[tokio::test]
    async fn scan_resources_reject_before_daily_reader_allocates_and_release_after_failure() {
        let cache_dir = std::env::temp_dir().join(format!(
            "tqsdk-snapshot-scan-resources-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let symbol = "SHFE.au2612";
        let start_ns = 1_704_153_600_000_000_000_i64;
        let end_ns = start_ns + 86_400_000_000_000;
        DailyKlineCache::open(&cache_dir)
            .unwrap()
            .store_final_range(
                symbol,
                start_ns,
                end_ns,
                &DailyKlineCacheSnapshot::cst_v1(),
                &[],
            )
            .unwrap();
        let client = BacktestHistoryClient::builder(&cache_dir)
            .policy(BacktestHistoryPolicy::CacheOnly)
            .build()
            .unwrap();
        let daily_reader_opens = Arc::new(AtomicUsize::new(0));
        let resources = BacktestHistorySnapshotQueryResources::new(Arc::new(RejectingBudget), ())
            .with_daily_reader_open_probe(Arc::clone(&daily_reader_opens));

        let run = client
            .query_with_resources(
                BacktestHistoryRequest::kline(
                    1,
                    symbol,
                    Duration::from_secs(86_400),
                    start_ns,
                    end_ns,
                ),
                resources.clone(),
            )
            .await
            .unwrap();
        let snapshot_run = BacktestHistorySnapshotRun::new(run, 1, symbol.to_string());
        let error = snapshot_run.collect().await.unwrap_err();

        assert!(matches!(
            error.reason(),
            BacktestHistoryFailureReason::ResponseTooLarge { limit_bytes: 0, .. }
        ));
        assert_eq!(daily_reader_opens.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn active_pin_outlives_dropped_run_until_detached_blocking_scan_exits() {
        let cache_dir = std::env::temp_dir().join(format!(
            "tqsdk-snapshot-active-pin-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let symbol = "SHFE.au2612";
        let start_ns = 1_704_153_600_000_000_000_i64;
        let end_ns = start_ns + 86_400_000_000_000;
        DailyKlineCache::open(&cache_dir)
            .unwrap()
            .store_final_range(
                symbol,
                start_ns,
                end_ns,
                &DailyKlineCacheSnapshot::cst_v1(),
                &[],
            )
            .unwrap();
        let client = BacktestHistoryClient::builder(&cache_dir)
            .policy(BacktestHistoryPolicy::CacheOnly)
            .build()
            .unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let resources = BacktestHistorySnapshotQueryResources::new(
            Arc::new(AllowingBudget),
            ActivePin(Arc::clone(&dropped)),
        );
        let lifecycle_pin: super::super::BacktestHistoryLifecyclePin = Arc::new(());
        let (gate, entered) =
            crate::backtest_history::store_worker::BlockingScanTestGate::install();
        let run = client
            .query_with_lifecycle_pin_and_resources(
                BacktestHistoryRequest::kline(
                    1,
                    symbol,
                    Duration::from_secs(86_400),
                    start_ns,
                    end_ns,
                ),
                lifecycle_pin,
                resources,
            )
            .await
            .unwrap();
        tokio::task::spawn_blocking(move || entered.recv_timeout(Duration::from_secs(1)))
            .await
            .unwrap()
            .expect("blocking scan must enter test gate");
        drop(run);
        assert!(!dropped.load(Ordering::Acquire));
        gate.release();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !dropped.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("active pin must drop after detached blocking scan exits");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::resource_tests::TrackingBudget;
    use super::*;
    use crate::backtest_history::store_worker::BlockingScanTestGate;
    use crate::{DailyKlineCache, DailyKlineCacheSnapshot};

    #[tokio::test]
    async fn blocking_reader_join_failure_remains_typed_on_snapshot_run() {
        let cache_dir = std::env::temp_dir().join(format!(
            "tqsdk-snapshot-typed-reader-failure-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let symbol = "SHFE.au2612";
        let start_ns = 1_704_153_600_000_000_000_i64;
        let end_ns = start_ns + 86_400_000_000_000;
        DailyKlineCache::open(&cache_dir)
            .unwrap()
            .store_final_range(
                symbol,
                start_ns,
                end_ns,
                &DailyKlineCacheSnapshot::cst_v1(),
                &[],
            )
            .unwrap();
        let client = BacktestHistoryClient::builder(&cache_dir)
            .policy(BacktestHistoryPolicy::CacheOnly)
            .build()
            .unwrap();
        let lifecycle_pin: super::super::BacktestHistoryLifecyclePin = Arc::new(());
        let (gate, entered) = BlockingScanTestGate::install_panicking();
        let run = client
            .query_with_lifecycle_pin(
                BacktestHistoryRequest::kline(
                    1,
                    symbol,
                    Duration::from_secs(86_400),
                    start_ns,
                    end_ns,
                ),
                lifecycle_pin,
            )
            .await
            .unwrap();
        let snapshot_run = BacktestHistorySnapshotRun::new(run, 1, symbol.to_string());

        tokio::task::spawn_blocking(move || entered.recv_timeout(Duration::from_secs(1)))
            .await
            .unwrap()
            .expect("blocking scan must enter the test gate");
        gate.release();
        let error = snapshot_run.collect().await.unwrap_err();
        assert_eq!(error.reason(), &BacktestHistoryFailureReason::Internal);

        let _ = std::fs::remove_dir_all(cache_dir);
    }

    #[tokio::test]
    async fn projected_rows_stay_budgeted_after_delivery_until_run_drop() {
        let cache_dir = std::env::temp_dir().join(format!(
            "tqsdk-snapshot-projected-row-budget-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let symbol = "SHFE.au2612";
        let start_ns = 1_704_153_600_000_000_000_i64;
        let end_ns = start_ns + 86_400_000_000_000;
        DailyKlineCache::open(&cache_dir)
            .unwrap()
            .store_final_range(
                symbol,
                start_ns,
                end_ns,
                &DailyKlineCacheSnapshot::cst_v1(),
                &[tqsdk_core::Kline {
                    id: 1,
                    datetime: start_ns,
                    close: 1.0,
                    ..Default::default()
                }],
            )
            .unwrap();
        let client = BacktestHistoryClient::builder(&cache_dir)
            .policy(BacktestHistoryPolicy::CacheOnly)
            .build()
            .unwrap();
        let used = Arc::new(AtomicUsize::new(0));
        let resources = BacktestHistorySnapshotQueryResources::new(
            Arc::new(TrackingBudget {
                used: Arc::clone(&used),
            }),
            (),
        );
        let mut run = client
            .query_with_resources(
                BacktestHistoryRequest::kline(
                    1,
                    symbol,
                    Duration::from_secs(86_400),
                    start_ns,
                    end_ns,
                ),
                resources,
            )
            .await
            .unwrap();

        let chunk = match run.next().await {
            Some(BacktestHistoryEvent::Chunk(chunk)) => chunk,
            Some(BacktestHistoryEvent::RequestCompleted(_)) => {
                panic!("query completed before delivering its projected row")
            }
            Some(BacktestHistoryEvent::RequestFailed(failure)) => {
                panic!("query failed before delivering its projected row: {failure:?}")
            }
            None => panic!("query ended before delivering its projected row"),
        };
        assert_eq!(chunk.rows.len(), 1);
        drop(chunk);
        assert!(
            used.load(Ordering::Acquire) > 0,
            "delivered projected rows must remain charged to the run"
        );

        drop(run);
        tokio::time::timeout(Duration::from_secs(1), async {
            while used.load(Ordering::Acquire) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropping the run must release projected-row reservations");
        let _ = std::fs::remove_dir_all(cache_dir);
    }
}
