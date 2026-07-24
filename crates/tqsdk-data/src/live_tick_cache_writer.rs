use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tqsdk_core::Tick;

use crate::{BacktestTickCache, DataError, Result};

const LIVE_TICK_WRITE_BUFFER_ROWS: usize = 128;
const LIVE_TICK_WRITE_MAX_LATENCY: Duration = Duration::from_millis(250);

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
    shared: Arc<LiveTickCacheWriterShared>,
}

struct LiveTickCacheWriterShared {
    cache: BacktestTickCache,
    states: Mutex<BTreeMap<String, LiveTickSymbolState>>,
}

#[derive(Debug, Clone, Default)]
struct LiveTickSymbolState {
    segment_start_id: Option<i64>,
    segment_start_ns: Option<i64>,
    last_seen_id: Option<i64>,
    segment_rows: usize,
    pending_rows: Vec<Tick>,
    pending_commits: Vec<PendingCoverageCommit>,
    pending_since: Option<Instant>,
    pending_gap_detected: bool,
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
            shared: Arc::new(LiveTickCacheWriterShared {
                cache,
                states: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    pub fn open(root_dir: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::new(BacktestTickCache::open(root_dir)?))
    }

    #[must_use]
    pub fn cache(&self) -> &BacktestTickCache {
        &self.shared.cache
    }

    /// Accepts decoded ticks, buffering consecutive single-row calls into durable batches.
    ///
    /// `appended_rows` reports rows written by this call; it is zero while a short tail remains
    /// buffered. Call [`Self::flush`] before inspecting coverage or ending a long-lived host.
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

        let now = Instant::now();
        let mut states = self.shared.states.lock().map_err(|_| {
            DataError::InvalidState("live tick cache writer state lock is poisoned")
        })?;
        let mut state = states.get(symbol).cloned().unwrap_or_default();
        let pending_rows_before = state.pending_rows.len();
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
            let commit = PendingCoverageCommit {
                range_start_ns: segment_start_ns,
                range_end_ns: row.datetime.checked_add(1).ok_or(DataError::InvalidState(
                    "live tick cache writer datetime range overflow",
                ))?,
                rows: state.segment_rows,
                id_range: (
                    state.segment_start_id.ok_or(DataError::InvalidState(
                        "live tick cache writer segment id start is missing",
                    ))?,
                    row.id,
                ),
            };
            state.update_pending_commit(commit);
            state.pending_rows.push(row);
        }

        let accepted_rows = state.pending_rows.len().saturating_sub(pending_rows_before);
        if accepted_rows > 0 && state.pending_since.is_none() {
            state.pending_since = Some(now);
        }
        state.pending_gap_detected |= gap_detected;

        let should_flush = !state.pending_rows.is_empty()
            && (accepted_rows > 1
                || state.pending_rows.len() >= LIVE_TICK_WRITE_BUFFER_ROWS
                || state.pending_gap_detected
                || state.pending_since.is_some_and(|pending_since| {
                    now.saturating_duration_since(pending_since) >= LIVE_TICK_WRITE_MAX_LATENCY
                }));
        let report = if should_flush {
            flush_pending_symbol(&self.shared.cache, symbol, &mut state)?
        } else {
            LiveTickCacheWriteReport {
                cache_dir: self.shared.cache.cache_dir().to_path_buf(),
                symbol: symbol.to_string(),
                appended_rows: 0,
                committed_ranges: Vec::new(),
                last_seen_id: state.last_seen_id,
                gap_detected,
            }
        };
        states.insert(symbol.to_string(), state);
        Ok(report)
    }

    /// Flushes all buffered live ticks and returns one report per written symbol.
    pub fn flush(&mut self) -> Result<Vec<LiveTickCacheWriteReport>> {
        self.shared.flush_all_pending()
    }
}

impl LiveTickCacheWriterShared {
    fn flush_all_pending(&self) -> Result<Vec<LiveTickCacheWriteReport>> {
        let mut states = self.states.lock().map_err(|_| {
            DataError::InvalidState("live tick cache writer state lock is poisoned")
        })?;
        let symbols = states
            .iter()
            .filter(|(_, state)| !state.pending_rows.is_empty())
            .map(|(symbol, _)| symbol.clone())
            .collect::<Vec<_>>();
        let mut reports = Vec::with_capacity(symbols.len());
        for symbol in symbols {
            let mut state = states.get(&symbol).cloned().unwrap_or_default();
            let report = flush_pending_symbol(&self.cache, &symbol, &mut state)?;
            states.insert(symbol, state);
            reports.push(report);
        }
        Ok(reports)
    }
}

impl Drop for LiveTickCacheWriterShared {
    fn drop(&mut self) {
        let _ = self.flush_all_pending();
    }
}

fn flush_pending_symbol(
    cache: &BacktestTickCache,
    symbol: &str,
    state: &mut LiveTickSymbolState,
) -> Result<LiveTickCacheWriteReport> {
    let appended_rows = state.pending_rows.len();
    let committed_ranges = state
        .pending_commits
        .iter()
        .map(|commit| (commit.range_start_ns, commit.range_end_ns))
        .collect::<Vec<_>>();
    if appended_rows > 0 {
        cache.append_partial_ticks_with_coverage(
            symbol,
            state.pending_rows.clone(),
            state.pending_commits.iter().map(|commit| {
                (
                    commit.range_start_ns,
                    commit.range_end_ns,
                    commit.rows,
                    Some(commit.id_range),
                )
            }),
        )?;
    }
    let gap_detected = state.pending_gap_detected;
    state.pending_rows.clear();
    state.pending_commits.clear();
    state.pending_since = None;
    state.pending_gap_detected = false;
    Ok(LiveTickCacheWriteReport {
        cache_dir: cache.cache_dir().to_path_buf(),
        symbol: symbol.to_string(),
        appended_rows,
        committed_ranges,
        last_seen_id: state.last_seen_id,
        gap_detected,
    })
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

    fn update_pending_commit(&mut self, commit: PendingCoverageCommit) {
        if let Some(pending) = self
            .pending_commits
            .last_mut()
            .filter(|pending| pending.id_range.0 == commit.id_range.0)
        {
            *pending = commit;
        } else {
            self.pending_commits.push(commit);
        }
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
