//! Validated read-only snapshot handle and strict no-row inspection.

use std::fmt;
use std::path::Path;

use super::BacktestHistoryClient;
use super::executor::{StrictInspectionFailure, strict_inspect_request};
use super::metadata::{StrictMetadataErrorKind, validate_active_snapshot_strict};
use super::planner;
use super::report::{BacktestHistoryFailureReason, BacktestHistoryRequestReport};
use super::request::{BacktestHistoryPolicy, BacktestHistoryRequest};
use super::snapshot_manifest::{
    SnapshotManifestError, SnapshotManifestErrorKind, ValidatedSnapshotManifest,
    open_current_manifest,
};
use crate::DataError;

/// Typed failure returned by the strict snapshot seam.
#[derive(Debug)]
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

    #[must_use]
    pub fn metadata_snapshot_hash(&self) -> &str {
        self.manifest.metadata_snapshot_hash()
    }

    #[must_use]
    pub const fn catalog_complete(&self) -> bool {
        self.manifest.catalog_complete()
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

fn map_manifest_error(error: SnapshotManifestError) -> BacktestHistorySnapshotError {
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
