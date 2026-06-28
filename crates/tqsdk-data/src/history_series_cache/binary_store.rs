use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::Result;

use super::{
    BINARY_HISTORY_SERIES_FORMAT_ID, HISTORY_SERIES_CACHE_SCHEMA_VERSION, HistorySeriesCacheInner,
    HistorySeriesCacheMaintenanceReport, HistorySeriesCacheScanReport, HistorySeriesCoverageCommit,
    HistorySeriesCoverageReport, HistorySeriesCoverageRequest, HistorySeriesReadRequest,
    HistorySeriesReader, HistorySeriesSegmentReport, HistorySeriesStore, HistorySeriesWriteSegment,
};

#[derive(Clone)]
pub(super) struct BinaryHistorySeriesStore {
    inner: Arc<HistorySeriesCacheInner>,
}

impl BinaryHistorySeriesStore {
    pub(super) fn new(root_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&root_dir)?;
        Ok(Self {
            inner: Arc::new(HistorySeriesCacheInner::new(root_dir)),
        })
    }

    pub(super) fn from_inner(inner: Arc<HistorySeriesCacheInner>) -> Self {
        Self { inner }
    }

    pub(super) fn inner(&self) -> &Arc<HistorySeriesCacheInner> {
        &self.inner
    }
}

impl HistorySeriesStore for BinaryHistorySeriesStore {
    fn format_id(&self) -> &'static str {
        BINARY_HISTORY_SERIES_FORMAT_ID
    }

    fn schema_version(&self) -> u32 {
        HISTORY_SERIES_CACHE_SCHEMA_VERSION
    }

    fn root_dir(&self) -> &Path {
        self.inner.root_dir.as_path()
    }

    fn uses_mmap_backend(&self) -> bool {
        true
    }

    fn scan(&self) -> Result<HistorySeriesCacheScanReport> {
        super::scan_with_inner(&self.inner)
    }

    fn enforce_limits(
        &self,
        max_bytes: Option<u64>,
        retention_days: Option<u64>,
    ) -> Result<HistorySeriesCacheMaintenanceReport> {
        super::enforce_limits_with_inner(&self.inner, max_bytes, retention_days)
    }

    fn coverage(
        &self,
        request: HistorySeriesCoverageRequest,
    ) -> Result<HistorySeriesCoverageReport> {
        super::coverage_with_inner(&self.inner, request)
    }

    fn write_segment(
        &self,
        segment: HistorySeriesWriteSegment<'_>,
    ) -> Result<HistorySeriesSegmentReport> {
        super::write_segment_with_inner(&self.inner, segment)
    }

    fn commit_coverage(
        &self,
        commit: HistorySeriesCoverageCommit,
    ) -> Result<HistorySeriesCoverageReport> {
        super::commit_coverage_with_inner(&self.inner, commit)
    }

    fn open_reader(
        &self,
        request: HistorySeriesReadRequest,
    ) -> Result<Box<dyn HistorySeriesReader>> {
        super::open_reader_with_inner(&self.inner, request)
    }
}
