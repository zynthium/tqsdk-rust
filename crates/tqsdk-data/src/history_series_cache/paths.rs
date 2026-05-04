use std::path::{Path, PathBuf};

use super::{DEFAULT_CACHE_DIR, HistorySeriesCacheFileKind, HistorySeriesCacheFileStatus};

pub(super) fn canonical_or_original(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

pub(super) fn default_cache_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .map(|home| home.join(DEFAULT_CACHE_DIR))
        .unwrap_or_else(|| std::env::temp_dir().join("tqsdk_data_series_1"))
}

pub(super) fn parse_data_file_name(filename: &str) -> Option<(String, i64, (i64, i64))> {
    if filename.starts_with('.')
        || filename.ends_with(".lock")
        || filename.ends_with(".temp")
        || filename.contains(".merge.")
    {
        return None;
    }
    let parts = filename.split('.').collect::<Vec<_>>();
    if parts.len() < 4 {
        return None;
    }
    let end = parts.last()?.parse::<i64>().ok()?;
    let start = parts.get(parts.len() - 2)?.parse::<i64>().ok()?;
    let duration_ns = parts.get(parts.len() - 3)?.parse::<i64>().ok()?;
    if start >= end {
        return None;
    }
    let symbol = parts[..parts.len() - 3].join(".");
    if symbol.is_empty() {
        return None;
    }
    Some((symbol, duration_ns, (start, end)))
}

pub(super) fn classify_non_segment_file(
    filename: &str,
) -> (HistorySeriesCacheFileKind, HistorySeriesCacheFileStatus) {
    if filename.starts_with('.') || filename.ends_with(".lock") {
        (
            HistorySeriesCacheFileKind::Lock,
            HistorySeriesCacheFileStatus::Ignored,
        )
    } else if filename.ends_with(".temp") {
        (
            HistorySeriesCacheFileKind::Temp,
            HistorySeriesCacheFileStatus::IncompleteWrite,
        )
    } else if filename.contains(".merge.") {
        (
            HistorySeriesCacheFileKind::MergeTemp,
            HistorySeriesCacheFileStatus::IncompleteWrite,
        )
    } else {
        (
            HistorySeriesCacheFileKind::Unknown,
            HistorySeriesCacheFileStatus::Ignored,
        )
    }
}
