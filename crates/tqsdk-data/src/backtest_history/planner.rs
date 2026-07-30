//! Deterministic source selection and cache-range expansion for backtest
//! history requests.

use crate::aggregation::KlineSessionTemplate;
use crate::backtest_tick_cache::{
    backtest_tick_trading_day_for_timestamp_ns, backtest_tick_trading_day_range,
};
use crate::minute_kline_cache::{MINUTE_KLINE_DURATION_NS, MinuteKlineCacheSnapshot};
use crate::{BacktestHistoryMetadataCache, DataError, Result};

use super::report::{
    BacktestHistoryFinality, BacktestHistoryPhysicalSegment, BacktestHistoryRequestReport,
};
use super::request::{
    BacktestHistoryKind, BacktestHistoryRequestId, ValidatedBacktestHistoryRequest,
};

/// Durable base source selected for a query request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum PlannedBaseSource {
    Tick,
    CanonicalMinute,
}

/// Physical-cache interval consumed for one logical request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedSourceSlice {
    pub(crate) cache_symbol: String,
    pub(crate) range: (i64, i64),
    pub(crate) physical_rank: usize,
}

/// Fully validated, cache-oriented plan for one public request.
#[derive(Debug, Clone)]
pub(crate) struct PlannedBacktestHistoryRequest {
    pub(crate) request_id: BacktestHistoryRequestId,
    pub(crate) symbol: String,
    pub(crate) kind: BacktestHistoryKind,
    pub(crate) duration_ns: Option<i64>,
    pub(crate) base_source: PlannedBaseSource,
    pub(crate) requested_range: (i64, i64),
    pub(crate) effective_end_ns: i64,
    pub(crate) expanded_source_range: (i64, i64),
    pub(crate) source_slices: Vec<PlannedSourceSlice>,
    pub(crate) physical_segments: Vec<BacktestHistoryPhysicalSegment>,
    pub(crate) session: KlineSessionTemplate,
    pub(crate) minute_snapshot: MinuteKlineCacheSnapshot,
    pub(crate) snapshot_hash: String,
    pub(crate) finality: BacktestHistoryFinality,
}

impl PlannedBacktestHistoryRequest {
    pub(crate) fn report_template(
        &self,
        rows: usize,
        cached_ranges: Vec<(i64, i64)>,
        remote_filled_ranges: Vec<(i64, i64)>,
        remote_used: bool,
    ) -> BacktestHistoryRequestReport {
        BacktestHistoryRequestReport {
            request_id: self.request_id,
            symbol: self.symbol.clone(),
            kind: self.kind,
            rows,
            physical_segments: self.physical_segments.clone(),
            snapshot_hash: self.snapshot_hash.clone(),
            coverage: super::report::BacktestHistoryCoverageReport {
                requested_range: self.requested_range,
                expanded_source_range: self.expanded_source_range,
                cached_ranges,
                remote_filled_ranges,
                finality: self.finality,
            },
            remote_used,
        }
    }
}

/// Validates public source policy before an asynchronous run begins.
pub(crate) fn validate_source_policy(request: &ValidatedBacktestHistoryRequest) -> Result<()> {
    let base_source = classify_request(request)?;
    if request.provisional_as_of_ns.is_some() && base_source != PlannedBaseSource::Tick {
        return Err(DataError::Validation(
            "provisional_as_of_ns is supported only for Tick and sub-minute Kline requests"
                .to_string(),
        ));
    }
    Ok(())
}

/// Builds an offline plan. Concrete and index symbols use the deterministic
/// CST fallback until a persisted metadata snapshot is available. Main
/// continuous symbols intentionally require their persisted physical mapping,
/// preserving genuine CacheOnly offline semantics.
pub(crate) fn plan_request(
    cache_dir: &std::path::Path,
    request: ValidatedBacktestHistoryRequest,
) -> Result<PlannedBacktestHistoryRequest> {
    validate_source_policy(&request)?;
    let base_source = classify_request(&request)?;
    let metadata = BacktestHistoryMetadataCache::open_read_only(cache_dir)
        .load_active(request.symbol.as_str())?;
    if request.symbol.starts_with("KQ.m@") && metadata.is_none() {
        return Err(DataError::InvalidState(
            "KQ.m backtest history requires a persisted metadata sidecar",
        ));
    }

    let effective_end_ns = request
        .provisional_as_of_ns
        .map_or(request.end_ns, |as_of_ns| request.end_ns.min(as_of_ns));
    if effective_end_ns <= request.start_ns {
        return Err(DataError::Validation(
            "provisional_as_of_ns leaves no queryable backtest history range".to_string(),
        ));
    }

    let (session, minute_snapshot, snapshot_hash, physical_segments, source_mapping_segments) =
        match metadata {
            Some(snapshot) => {
                let minute_snapshot = MinuteKlineCacheSnapshot::new(
                    snapshot.schema_version,
                    snapshot.snapshot_hash.clone(),
                    snapshot.session.snapshot_hash().to_string(),
                )?;
                let source_mapping_segments = snapshot.physical_segments.clone();
                (
                    snapshot.session,
                    minute_snapshot,
                    snapshot.snapshot_hash,
                    intersect_segments(
                        source_mapping_segments.clone(),
                        (request.start_ns, effective_end_ns),
                    ),
                    source_mapping_segments,
                )
            }
            None => {
                let session = KlineSessionTemplate::cst_trading_day();
                let physical_segments = vec![BacktestHistoryPhysicalSegment {
                    physical_symbol: request.symbol.clone(),
                    start_ns: request.start_ns,
                    end_ns: effective_end_ns,
                }];
                (
                    session,
                    MinuteKlineCacheSnapshot::cst_v1(),
                    "cst-trading-day-v1".to_string(),
                    physical_segments.clone(),
                    physical_segments,
                )
            }
        };
    if !segments_cover_range(
        physical_segments.as_slice(),
        (request.start_ns, effective_end_ns),
    ) {
        return Err(DataError::InvalidState(
            "backtest history metadata does not cover the requested range",
        ));
    }

    let source_segments = match base_source {
        PlannedBaseSource::Tick if request.symbol.starts_with("KQ.m@") => source_mapping_segments
            .iter()
            .filter_map(|segment| {
                let requested_start_ns = segment.start_ns.max(request.start_ns);
                let requested_end_ns = segment.end_ns.min(effective_end_ns);
                (requested_start_ns < requested_end_ns)
                    .then_some((segment, (requested_start_ns, requested_end_ns)))
            })
            .enumerate()
            .map(|(rank, (segment, requested))| {
                let expanded = expand_tick_source_range(requested, request.duration_ns, &session)?;
                let range = (
                    expanded.0.max(segment.start_ns),
                    expanded.1.min(segment.end_ns),
                );
                if range.0 >= range.1 {
                    return Err(DataError::InvalidState(
                        "KQ.m physical segment cannot supply an expanded Tick source range",
                    ));
                }
                Ok(PlannedSourceSlice {
                    cache_symbol: segment.physical_symbol.clone(),
                    range,
                    physical_rank: rank,
                })
            })
            .collect::<Result<Vec<_>>>()?,
        PlannedBaseSource::Tick => vec![PlannedSourceSlice {
            cache_symbol: request.symbol.clone(),
            range: expand_tick_source_range(
                (request.start_ns, effective_end_ns),
                request.duration_ns,
                &session,
            )?,
            physical_rank: 0,
        }],
        PlannedBaseSource::CanonicalMinute => vec![PlannedSourceSlice {
            cache_symbol: request.symbol.clone(),
            range: expand_minute_source_range(
                (request.start_ns, effective_end_ns),
                request.duration_ns.unwrap_or(MINUTE_KLINE_DURATION_NS),
                &session,
            )?,
            physical_rank: 0,
        }],
    };
    let expanded_source_range = source_segments
        .iter()
        .map(|slice| slice.range)
        .reduce(|left, right| (left.0.min(right.0), left.1.max(right.1)))
        .ok_or(DataError::InvalidState(
            "backtest history plan has no source slices",
        ))?;

    Ok(PlannedBacktestHistoryRequest {
        request_id: request.request_id,
        symbol: request.symbol,
        kind: request.kind,
        duration_ns: request.duration_ns,
        base_source,
        requested_range: (request.start_ns, request.end_ns),
        effective_end_ns,
        expanded_source_range,
        source_slices: source_segments,
        physical_segments,
        session,
        minute_snapshot,
        snapshot_hash,
        finality: request
            .provisional_as_of_ns
            .map_or(BacktestHistoryFinality::Final, |as_of_ns| {
                BacktestHistoryFinality::Provisional { as_of_ns }
            }),
    })
}

pub(crate) fn classify_duration(duration_ns: i64) -> Result<PlannedBaseSource> {
    match duration_ns {
        value if value > 0 && value < MINUTE_KLINE_DURATION_NS => Ok(PlannedBaseSource::Tick),
        MINUTE_KLINE_DURATION_NS => Ok(PlannedBaseSource::CanonicalMinute),
        value if value > MINUTE_KLINE_DURATION_NS && value % MINUTE_KLINE_DURATION_NS == 0 => {
            Ok(PlannedBaseSource::CanonicalMinute)
        }
        _ => Err(DataError::Validation(
            "Kline duration must be below 60s, exactly 60s, or an integer multiple of 60s"
                .to_string(),
        )),
    }
}

fn classify_request(request: &ValidatedBacktestHistoryRequest) -> Result<PlannedBaseSource> {
    match request.duration_ns {
        None => Ok(PlannedBaseSource::Tick),
        Some(duration_ns) => classify_duration(duration_ns),
    }
}

fn intersect_segments(
    segments: Vec<BacktestHistoryPhysicalSegment>,
    requested: (i64, i64),
) -> Vec<BacktestHistoryPhysicalSegment> {
    segments
        .into_iter()
        .filter_map(|segment| {
            let start_ns = segment.start_ns.max(requested.0);
            let end_ns = segment.end_ns.min(requested.1);
            (start_ns < end_ns).then_some(BacktestHistoryPhysicalSegment {
                physical_symbol: segment.physical_symbol,
                start_ns,
                end_ns,
            })
        })
        .collect()
}

fn segments_cover_range(
    segments: &[BacktestHistoryPhysicalSegment],
    requested: (i64, i64),
) -> bool {
    let mut cursor = requested.0;
    for segment in segments {
        if segment.end_ns <= cursor || segment.start_ns >= requested.1 {
            continue;
        }
        if segment.start_ns > cursor {
            return false;
        }
        cursor = cursor.max(segment.end_ns);
        if cursor >= requested.1 {
            return true;
        }
    }
    false
}

fn expand_tick_source_range(
    requested: (i64, i64),
    duration_ns: Option<i64>,
    session: &KlineSessionTemplate,
) -> Result<(i64, i64)> {
    let day = backtest_tick_trading_day_for_timestamp_ns(requested.0)?;
    let day_range = backtest_tick_trading_day_range(day)?;
    let end_ns = duration_ns.map_or(Ok(requested.1), |duration_ns| {
        expanded_bar_end(requested.1, duration_ns, session)
    })?;
    Ok((day_range.start_ns.min(requested.0), end_ns.max(requested.1)))
}

fn expand_minute_source_range(
    requested: (i64, i64),
    duration_ns: i64,
    session: &KlineSessionTemplate,
) -> Result<(i64, i64)> {
    let start_ns = expanded_bar_start(requested.0, duration_ns, session)?;
    let end_ns = expanded_bar_end(requested.1, duration_ns, session)?;
    Ok((start_ns.min(requested.0), end_ns.max(requested.1)))
}

fn expanded_bar_start(
    timestamp_ns: i64,
    duration_ns: i64,
    session: &KlineSessionTemplate,
) -> Result<i64> {
    let Some(position) = session.locate(timestamp_ns)? else {
        return Ok(timestamp_ns);
    };
    let offset = timestamp_ns
        .checked_sub(position.window_start_ns)
        .ok_or_else(|| {
            DataError::Validation("kline source start predates its session".to_string())
        })?;
    let bucket = offset
        .div_euclid(duration_ns)
        .checked_mul(duration_ns)
        .ok_or_else(|| DataError::Validation("kline source bucket overflow".to_string()))?;
    position
        .window_start_ns
        .checked_add(bucket)
        .ok_or_else(|| DataError::Validation("kline source start overflow".to_string()))
}

fn expanded_bar_end(end_ns: i64, duration_ns: i64, session: &KlineSessionTemplate) -> Result<i64> {
    let last_ns = end_ns.saturating_sub(1);
    let Some(position) = session.locate(last_ns)? else {
        return Ok(end_ns);
    };
    let start_ns = expanded_bar_start(last_ns, duration_ns, session)?;
    start_ns
        .checked_add(duration_ns)
        .map(|end| end.min(position.window_end_ns))
        .ok_or_else(|| DataError::Validation("kline source end overflow".to_string()))
}

pub(crate) fn bar_end_ns(
    start_ns: i64,
    duration_ns: i64,
    session: &KlineSessionTemplate,
) -> Result<i64> {
    let Some(position) = session.locate(start_ns)? else {
        return start_ns
            .checked_add(duration_ns)
            .ok_or_else(|| DataError::Validation("kline bar end overflow".to_string()));
    };
    start_ns
        .checked_add(duration_ns)
        .map(|end| end.min(position.window_end_ns))
        .ok_or_else(|| DataError::Validation("kline bar end overflow".to_string()))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::backtest_history::request::BacktestHistoryRequest;

    #[test]
    fn source_policy_accepts_the_declared_duration_matrix() {
        for duration in [15, 59, 60, 5 * 60, 15 * 60, 30 * 60, 60 * 60] {
            let request = BacktestHistoryRequest::kline(
                1,
                "SHFE.au2608",
                Duration::from_secs(duration),
                1,
                2,
            )
            .validate()
            .unwrap();
            assert!(validate_source_policy(&request).is_ok(), "{duration}s");
        }
        for duration in [61, 90] {
            let request = BacktestHistoryRequest::kline(
                1,
                "SHFE.au2608",
                Duration::from_secs(duration),
                1,
                2,
            )
            .validate()
            .unwrap();
            assert!(validate_source_policy(&request).is_err(), "{duration}s");
        }
    }

    #[test]
    fn provisional_policy_rejects_canonical_minute_and_larger_periods() {
        let sub_minute =
            BacktestHistoryRequest::kline(1, "SHFE.au2608", Duration::from_secs(15), 1, 2)
                .with_provisional_as_of_ns(2)
                .validate()
                .unwrap();
        assert!(validate_source_policy(&sub_minute).is_ok());

        let minute = BacktestHistoryRequest::kline(1, "SHFE.au2608", Duration::from_secs(60), 1, 2)
            .with_provisional_as_of_ns(2)
            .validate()
            .unwrap();
        assert!(validate_source_policy(&minute).is_err());
    }
}
