use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{DataError, Result};

use super::{
    HISTORY_SERIES_CACHE_SCHEMA_VERSION, HistorySeriesCacheMaintenanceReport,
    HistorySeriesCacheScanReport, HistorySeriesCoverageCommit, HistorySeriesCoverageReport,
    HistorySeriesCoverageRequest, HistorySeriesReadRequest, HistorySeriesReader,
    HistorySeriesSegmentReport, HistorySeriesStore, HistorySeriesWriteSegment,
    SERIES_FILE_HISTORY_SERIES_FORMAT_ID,
};

const ROOT_DIR_NAME: &str = "series";
const TICK_FILE_NAME: &str = "tick.tqseries";

#[derive(Debug, Clone)]
pub(super) struct SeriesFileHistoryStore {
    root_dir: Arc<PathBuf>,
}

impl SeriesFileHistoryStore {
    pub(super) fn new(root_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(root_dir.join(ROOT_DIR_NAME))?;
        Ok(Self {
            root_dir: Arc::new(root_dir),
        })
    }

    pub(super) fn series_path(&self, symbol: &str, duration_ns: i64) -> PathBuf {
        self.root_dir
            .join(ROOT_DIR_NAME)
            .join(escape_symbol_path_component(symbol))
            .join(if duration_ns == 0 {
                TICK_FILE_NAME.to_string()
            } else {
                format!("{duration_ns}.tqseries")
            })
    }
}

impl HistorySeriesStore for SeriesFileHistoryStore {
    fn format_id(&self) -> &'static str {
        SERIES_FILE_HISTORY_SERIES_FORMAT_ID
    }

    fn schema_version(&self) -> u32 {
        HISTORY_SERIES_CACHE_SCHEMA_VERSION
    }

    fn root_dir(&self) -> &Path {
        self.root_dir.as_path()
    }

    fn uses_mmap_backend(&self) -> bool {
        false
    }

    fn scan(&self) -> Result<HistorySeriesCacheScanReport> {
        super::empty_scan_report(self.root_dir.as_path())
    }

    fn enforce_limits(
        &self,
        _max_bytes: Option<u64>,
        _retention_days: Option<u64>,
    ) -> Result<HistorySeriesCacheMaintenanceReport> {
        Ok(HistorySeriesCacheMaintenanceReport::default())
    }

    fn coverage(
        &self,
        request: HistorySeriesCoverageRequest,
    ) -> Result<HistorySeriesCoverageReport> {
        Ok(HistorySeriesCoverageReport {
            cached_ranges: Vec::new(),
            missing_ranges: vec![(request.range_start_ns, request.range_end_ns)],
            symbol: request.symbol,
            kind: request.kind,
            range_start_ns: request.range_start_ns,
            range_end_ns: request.range_end_ns,
        })
    }

    fn write_segment(
        &self,
        _segment: HistorySeriesWriteSegment<'_>,
    ) -> Result<HistorySeriesSegmentReport> {
        Err(DataError::InvalidState(
            "series-file store write path is not wired",
        ))
    }

    fn commit_coverage(
        &self,
        commit: HistorySeriesCoverageCommit,
    ) -> Result<HistorySeriesCoverageReport> {
        Ok(HistorySeriesCoverageReport {
            symbol: commit.symbol,
            kind: commit.kind,
            range_start_ns: commit.range_start_ns,
            range_end_ns: commit.range_end_ns,
            cached_ranges: vec![(commit.range_start_ns, commit.range_end_ns)],
            missing_ranges: Vec::new(),
        })
    }

    fn open_reader(
        &self,
        _request: HistorySeriesReadRequest,
    ) -> Result<Box<dyn HistorySeriesReader>> {
        Err(DataError::InvalidState(
            "series-file store reader path is not wired",
        ))
    }
}

fn escape_symbol_path_component(symbol: &str) -> String {
    symbol.replace('/', "%2F")
}
