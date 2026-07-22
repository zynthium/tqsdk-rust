use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use tqsdk_core::Tick;

use crate::{BacktestTickCache, DataError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveTickCacheWriteReport {
    pub cache_dir: PathBuf,
    pub symbol: String,
    pub appended_rows: usize,
    pub committed_ranges: Vec<(i64, i64)>,
    pub last_seen_id: Option<i64>,
    pub gap_detected: bool,
}

#[derive(Clone)]
pub struct LiveTickCacheWriter {
    cache: BacktestTickCache,
    states: BTreeMap<String, LiveTickSymbolState>,
}

#[derive(Debug, Clone, Default)]
struct LiveTickSymbolState {
    segment_start_id: Option<i64>,
    segment_start_ns: Option<i64>,
    last_seen_id: Option<i64>,
    segment_rows: usize,
}

#[derive(Debug, Clone, Copy)]
struct PendingCoverageCommit {
    range_start_ns: i64,
    range_end_ns: i64,
    rows: usize,
    id_range: (i64, i64),
}

impl LiveTickCacheWriter {
    #[must_use]
    pub fn new(cache: BacktestTickCache) -> Self {
        Self {
            cache,
            states: BTreeMap::new(),
        }
    }

    pub fn open(root_dir: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::new(BacktestTickCache::open(root_dir)?))
    }

    #[must_use]
    pub fn cache(&self) -> &BacktestTickCache {
        &self.cache
    }

    pub fn push_ticks(
        &mut self,
        symbol: impl AsRef<str>,
        rows: impl IntoIterator<Item = Tick>,
    ) -> Result<LiveTickCacheWriteReport> {
        let symbol = symbol.as_ref();
        if symbol.is_empty() {
            return Err(DataError::InvalidState(
                "live tick cache writer symbol must not be empty",
            ));
        }

        let mut rows = rows.into_iter().collect::<Vec<_>>();
        normalize_live_tick_rows(&mut rows);

        let mut state = self.states.get(symbol).cloned().unwrap_or_default();
        let mut accepted_rows = Vec::new();
        let mut pending_commit = None;
        let mut commits = Vec::new();
        let mut gap_detected = false;

        for row in rows {
            if state
                .last_seen_id
                .is_some_and(|last_seen| row.id <= last_seen)
            {
                continue;
            }

            let contiguous = state
                .last_seen_id
                .is_none_or(|last_seen| row.id == last_seen.saturating_add(1));
            if !contiguous {
                gap_detected = true;
                if let Some(commit) = pending_commit.take() {
                    commits.push(commit);
                }
                state.start_segment(row.id, row.datetime);
            } else if state.segment_start_id.is_none() {
                state.start_segment(row.id, row.datetime);
            }

            let segment_start_ns = state.segment_start_ns.ok_or(DataError::InvalidState(
                "live tick cache writer segment start is missing",
            ))?;
            if row.datetime < segment_start_ns {
                return Err(DataError::InvalidState(
                    "live tick cache writer tick datetime moved backwards",
                ));
            }

            state.last_seen_id = Some(row.id);
            state.segment_rows = state.segment_rows.saturating_add(1);
            pending_commit = Some(PendingCoverageCommit {
                range_start_ns: segment_start_ns,
                range_end_ns: row.datetime.checked_add(1).ok_or(DataError::InvalidState(
                    "live tick cache writer datetime range overflow",
                ))?,
                rows: state.segment_rows,
                id_range: (
                    state.segment_start_id.ok_or(DataError::InvalidState(
                        "live tick cache writer segment id start is missing",
                    ))?,
                    row.id.checked_add(1).ok_or(DataError::InvalidState(
                        "live tick cache writer id range overflow",
                    ))?,
                ),
            });
            accepted_rows.push(row);
        }

        if let Some(commit) = pending_commit {
            commits.push(commit);
        }

        let appended_rows = accepted_rows.len();
        if !accepted_rows.is_empty() {
            self.cache.append_partial_ticks_with_coverage(
                symbol,
                accepted_rows,
                commits.iter().map(|commit| {
                    (
                        commit.range_start_ns,
                        commit.range_end_ns,
                        commit.rows,
                        Some(commit.id_range),
                    )
                }),
            )?;
        }

        let report = LiveTickCacheWriteReport {
            cache_dir: self.cache.cache_dir().to_path_buf(),
            symbol: symbol.to_string(),
            appended_rows,
            committed_ranges: commits
                .iter()
                .map(|commit| (commit.range_start_ns, commit.range_end_ns))
                .collect(),
            last_seen_id: state.last_seen_id,
            gap_detected,
        };
        self.states.insert(symbol.to_string(), state);
        Ok(report)
    }
}

fn normalize_live_tick_rows(rows: &mut Vec<Tick>) {
    if rows.windows(2).all(|pair| pair[0].id < pair[1].id) {
        return;
    }
    rows.sort_by_key(|row| (row.id, row.datetime, row.epoch));
    rows.dedup_by(|left, right| left.id == right.id);
}

impl LiveTickSymbolState {
    fn start_segment(&mut self, id: i64, datetime_ns: i64) {
        self.segment_start_id = Some(id);
        self.segment_start_ns = Some(datetime_ns);
        self.segment_rows = 0;
    }
}

#[cfg(test)]
mod tests {
    use tqsdk_core::Tick;

    use super::normalize_live_tick_rows;

    #[test]
    fn live_tick_normalization_keeps_strictly_ordered_rows_in_place() {
        let mut rows = vec![
            Tick {
                id: 1,
                datetime: 1_000,
                ..Tick::default()
            },
            Tick {
                id: 2,
                datetime: 2_000,
                ..Tick::default()
            },
        ];

        normalize_live_tick_rows(&mut rows);

        assert_eq!(rows.iter().map(|row| row.id).collect::<Vec<_>>(), [1, 2]);
    }

    #[test]
    fn live_tick_normalization_restores_legacy_id_dedup_behavior() {
        let mut rows = vec![
            Tick {
                id: 2,
                datetime: 2_000,
                epoch: Some(2),
                ..Tick::default()
            },
            Tick {
                id: 1,
                datetime: 1_000,
                epoch: Some(1),
                ..Tick::default()
            },
            Tick {
                id: 2,
                datetime: 3_000,
                epoch: Some(3),
                ..Tick::default()
            },
        ];

        normalize_live_tick_rows(&mut rows);

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, 1);
        assert_eq!(rows[1].id, 2);
        assert_eq!(rows[1].datetime, 2_000);
    }
}
