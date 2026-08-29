//! Deterministic source selection and cache-range expansion for backtest
//! history requests.

use crate::aggregation::{KlineSessionPosition, KlineSessionTemplate};
use crate::backtest_tick_cache::{
    backtest_tick_trading_day_for_timestamp_ns, backtest_tick_trading_day_range,
};
use crate::daily_kline_cache::DAILY_KLINE_DURATION_NS;
use crate::minute_kline_cache::{MINUTE_KLINE_DURATION_NS, MinuteKlineCacheSnapshot};
use crate::{
    BacktestHistoryTradingDay, DataError, Result, resolve_backtest_metadata_snapshot,
    resolve_minute_cache_metadata_snapshot,
};

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
    CanonicalDaily,
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
    let effective_end_ns = request
        .provisional_as_of_ns
        .map_or(request.end_ns, |as_of_ns| request.end_ns.min(as_of_ns));
    if effective_end_ns <= request.start_ns {
        return Err(DataError::Validation(
            "provisional_as_of_ns leaves no queryable backtest history range".to_string(),
        ));
    }
    let metadata = match base_source {
        PlannedBaseSource::CanonicalMinute => resolve_minute_cache_metadata_snapshot(
            cache_dir,
            request.symbol.as_str(),
            request.start_ns,
            effective_end_ns,
        )?,
        PlannedBaseSource::Tick | PlannedBaseSource::CanonicalDaily => {
            resolve_backtest_metadata_snapshot(
                cache_dir,
                request.symbol.as_str(),
                request.start_ns,
                effective_end_ns,
            )?
        }
    };
    // A native daily request for a concrete symbol uses a single server-side
    // series, so its retained sidecar only binds existing cache partitions; it
    // does not limit the downloadable range. Synthetic KQ.*@ symbols still
    // require their persisted dated mapping.
    let is_physical_native_daily = matches!(base_source, PlannedBaseSource::CanonicalDaily)
        && !request.symbol.starts_with("KQ.");
    if request.symbol.starts_with("KQ.m@") && metadata.is_none() {
        return Err(DataError::InvalidState(
            "KQ.m backtest history requires a persisted metadata sidecar",
        ));
    }

    let (
        session,
        trading_days,
        minute_snapshot,
        snapshot_hash,
        physical_segments,
        source_mapping_segments,
    ) = match metadata {
        Some(snapshot) => {
            let minute_snapshot = MinuteKlineCacheSnapshot::new(
                snapshot.schema_version,
                snapshot.snapshot_hash.clone(),
                snapshot.session.snapshot_hash().to_string(),
            )?;
            let source_mapping_segments = snapshot.physical_segments.clone();
            (
                snapshot.session,
                Some(snapshot.trading_days),
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
                None,
                MinuteKlineCacheSnapshot::cst_v1(),
                "cst-trading-day-v1".to_string(),
                physical_segments.clone(),
                physical_segments,
            )
        }
    };
    if !is_physical_native_daily
        && !segments_cover_range(
            physical_segments.as_slice(),
            (request.start_ns, effective_end_ns),
        )
    {
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
                let expanded = expand_tick_source_range(
                    requested,
                    request.duration_ns,
                    &session,
                    trading_days.as_deref(),
                )?;
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
                trading_days.as_deref(),
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
        PlannedBaseSource::CanonicalDaily => vec![PlannedSourceSlice {
            cache_symbol: request.symbol.clone(),
            range: (request.start_ns, effective_end_ns),
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
        value
            if value > MINUTE_KLINE_DURATION_NS
                && value < DAILY_KLINE_DURATION_NS
                && value % MINUTE_KLINE_DURATION_NS == 0 =>
        {
            Ok(PlannedBaseSource::CanonicalMinute)
        }
        value
            if (DAILY_KLINE_DURATION_NS..=28 * DAILY_KLINE_DURATION_NS).contains(&value)
                && value % DAILY_KLINE_DURATION_NS == 0 =>
        {
            Ok(PlannedBaseSource::CanonicalDaily)
        }
        _ => Err(DataError::Validation(
            "Kline duration must be below 60s, an integer multiple of 60s below 1d, or an integer number of days from 1d through 28d"
                .to_string(),
        )),
    }
}

pub(crate) fn classify_request(
    request: &ValidatedBacktestHistoryRequest,
) -> Result<PlannedBaseSource> {
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
    trading_days: Option<&[BacktestHistoryTradingDay]>,
) -> Result<(i64, i64)> {
    let day = backtest_tick_trading_day_for_timestamp_ns(requested.0)?;
    let day_range = backtest_tick_trading_day_range(day)?;
    let warmup_start_ns = duration_ns
        .map(|_| preceding_trading_day_cycle_start(requested.0, session, trading_days))
        .transpose()?
        .flatten();
    let end_ns = duration_ns.map_or(Ok(requested.1), |duration_ns| {
        // Official server-backtest charts assign a Tick exactly at a derived
        // bar's end to that preceding bar. Source scans are half-open, so
        // include that one nanosecond without changing public Tick ranges.
        expanded_bar_end(requested.1, duration_ns, session)?
            .checked_add(1)
            .ok_or_else(|| DataError::Validation("tick kline source end overflow".to_string()))
    })?;
    Ok((
        warmup_start_ns
            .unwrap_or(day_range.start_ns)
            .min(day_range.start_ns)
            .min(requested.0),
        end_ns.max(requested.1),
    ))
}

/// Finds the complete prior real trading cycle needed to seed a Tick-derived
/// Kline's opening price and open interest. Metadata spans at least fourteen
/// calendar days around every remote request, but an older sidecar may not;
/// lacking a matching day deliberately falls back to the Tick partition start.
fn preceding_trading_day_cycle_start(
    timestamp_ns: i64,
    session: &KlineSessionTemplate,
    trading_days: Option<&[BacktestHistoryTradingDay]>,
) -> Result<Option<i64>> {
    let Some(trading_days) = trading_days else {
        return Ok(None);
    };
    let (cycle_start_ns, cycle_end_ns) = session.cycle_bounds(timestamp_ns)?;
    let Some(current_day_start_ns) = trading_days
        .iter()
        .find(|day| day.start_ns > cycle_start_ns && day.start_ns < cycle_end_ns)
        .map(|day| day.start_ns)
    else {
        return Ok(None);
    };
    let Some(previous_trading_day) = trading_days
        .iter()
        .rev()
        .find(|day| day.is_trading_day && day.end_ns <= current_day_start_ns)
    else {
        return Ok(None);
    };
    session
        .cycle_bounds(previous_trading_day.start_ns)
        .map(|(start_ns, _)| Some(start_ns))
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
    let (grid_start_ns, _) = kline_bar_grid_bounds(duration_ns, position);
    let offset = timestamp_ns.checked_sub(grid_start_ns).ok_or_else(|| {
        DataError::Validation("kline source start predates its bar grid".to_string())
    })?;
    let bucket = offset
        .div_euclid(duration_ns)
        .checked_mul(duration_ns)
        .ok_or_else(|| DataError::Validation("kline source bucket overflow".to_string()))?;
    grid_start_ns
        .checked_add(bucket)
        .ok_or_else(|| DataError::Validation("kline source start overflow".to_string()))
}

fn expanded_bar_end(end_ns: i64, duration_ns: i64, session: &KlineSessionTemplate) -> Result<i64> {
    let last_ns = end_ns.saturating_sub(1);
    let Some(position) = session.locate(last_ns)? else {
        return Ok(end_ns);
    };
    let (_, grid_end_ns) = kline_bar_grid_bounds(duration_ns, position);
    let start_ns = expanded_bar_start(last_ns, duration_ns, session)?;
    start_ns
        .checked_add(duration_ns)
        .map(|end| end.min(grid_end_ns))
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
    let (_, grid_end_ns) = kline_bar_grid_bounds(duration_ns, position);
    start_ns
        .checked_add(duration_ns)
        .map(|end| end.min(grid_end_ns))
        .ok_or_else(|| DataError::Validation("kline bar end overflow".to_string()))
}

fn kline_bar_grid_bounds(duration_ns: i64, position: KlineSessionPosition) -> (i64, i64) {
    if duration_ns > MINUTE_KLINE_DURATION_NS {
        (position.trading_day_start_ns, position.trading_day_end_ns)
    } else {
        (position.window_start_ns, position.window_end_ns)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::aggregation::KlineSessionWindow;
    use crate::backtest_history::request::BacktestHistoryRequest;
    use crate::{
        BACKTEST_HISTORY_METADATA_SCHEMA_VERSION, BacktestHistoryMarketKind,
        BacktestHistoryMetadataCache, BacktestHistoryMetadataSnapshot,
    };

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
    fn daily_periods_use_native_daily_base_and_cap_at_twenty_eight_days() {
        const DAY_NS: i64 = 86_400_000_000_000;
        for days in [1, 2, 5, 28] {
            assert_eq!(
                classify_duration(days * DAY_NS).unwrap(),
                PlannedBaseSource::CanonicalDaily,
                "{days}d"
            );
        }
        let error = classify_duration(29 * DAY_NS).unwrap_err();
        assert!(error.to_string().contains("1d through 28d"));
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

    #[test]
    fn subminute_plan_warms_from_the_previous_real_trading_cycle() {
        let root = temp_dir("subminute-warmup");
        let symbol = "KQ.i@SHFE.au";
        let requested_start_ns = utc_ns(2026, 1, 5, 1, 0, 0);
        let requested_end_ns = requested_start_ns + 15 * 1_000_000_000;
        let previous_cycle_start_ns = utc_ns(2026, 1, 1, 10, 0, 0);
        let snapshot = BacktestHistoryMetadataSnapshot {
            schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
            market_kind: BacktestHistoryMarketKind::Futures,
            logical_symbol: symbol.to_string(),
            captured_at_ns: requested_start_ns,
            trading_days: vec![
                trading_day("2026-01-02", true, utc_ns(2026, 1, 1, 16, 0, 0)),
                trading_day("2026-01-03", false, utc_ns(2026, 1, 2, 16, 0, 0)),
                trading_day("2026-01-04", false, utc_ns(2026, 1, 3, 16, 0, 0)),
                trading_day("2026-01-05", true, utc_ns(2026, 1, 4, 16, 0, 0)),
            ],
            session: KlineSessionTemplate::cst_trading_day(),
            physical_segments: vec![BacktestHistoryPhysicalSegment {
                physical_symbol: symbol.to_string(),
                start_ns: previous_cycle_start_ns,
                end_ns: requested_end_ns,
            }],
            snapshot_hash: String::new(),
        };
        BacktestHistoryMetadataCache::open(&root)
            .unwrap()
            .store_snapshot(snapshot)
            .unwrap();

        let plan = plan_request(
            &root,
            BacktestHistoryRequest::kline(
                1,
                symbol,
                Duration::from_secs(15),
                requested_start_ns,
                requested_end_ns,
            )
            .validate()
            .unwrap(),
        )
        .unwrap();

        assert_eq!(plan.source_slices.len(), 1);
        assert_eq!(plan.source_slices[0].range.0, previous_cycle_start_ns);
    }

    #[test]
    fn subminute_main_continuous_plan_never_warms_across_a_physical_segment() {
        let root = temp_dir("subminute-main-warmup");
        let logical_symbol = "KQ.m@SHFE.au";
        let physical_symbol = "SHFE.au2602";
        let requested_start_ns = utc_ns(2026, 1, 5, 1, 0, 0);
        let requested_end_ns = requested_start_ns + 15 * 1_000_000_000;
        let snapshot = BacktestHistoryMetadataSnapshot {
            schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
            market_kind: BacktestHistoryMarketKind::Futures,
            logical_symbol: logical_symbol.to_string(),
            captured_at_ns: requested_start_ns,
            trading_days: vec![
                trading_day("2026-01-02", true, utc_ns(2026, 1, 1, 16, 0, 0)),
                trading_day("2026-01-03", false, utc_ns(2026, 1, 2, 16, 0, 0)),
                trading_day("2026-01-04", false, utc_ns(2026, 1, 3, 16, 0, 0)),
                trading_day("2026-01-05", true, utc_ns(2026, 1, 4, 16, 0, 0)),
            ],
            session: KlineSessionTemplate::cst_trading_day(),
            physical_segments: vec![BacktestHistoryPhysicalSegment {
                physical_symbol: physical_symbol.to_string(),
                start_ns: requested_start_ns,
                end_ns: requested_end_ns,
            }],
            snapshot_hash: String::new(),
        };
        BacktestHistoryMetadataCache::open(&root)
            .unwrap()
            .store_snapshot(snapshot)
            .unwrap();

        let plan = plan_request(
            &root,
            BacktestHistoryRequest::kline(
                1,
                logical_symbol,
                Duration::from_secs(15),
                requested_start_ns,
                requested_end_ns,
            )
            .validate()
            .unwrap(),
        )
        .unwrap();

        assert_eq!(plan.source_slices.len(), 1);
        assert_eq!(plan.source_slices[0].cache_symbol, physical_symbol);
        assert_eq!(plan.source_slices[0].range.0, requested_start_ns);
    }

    #[test]
    fn tick_plan_uses_retained_metadata_when_active_snapshot_misses_range() {
        let root = temp_dir("tick-retained-metadata");
        let symbol = "CFFEX.T2609";
        let requested_start_ns = utc_ns(2026, 8, 17, 1, 0, 0);
        let requested_end_ns = requested_start_ns + 15 * 1_000_000_000;
        let cache = BacktestHistoryMetadataCache::open(&root).unwrap();
        cache
            .store_snapshot(BacktestHistoryMetadataSnapshot {
                schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
                market_kind: BacktestHistoryMarketKind::Futures,
                logical_symbol: symbol.to_string(),
                captured_at_ns: utc_ns(2026, 8, 1, 0, 0, 0),
                trading_days: vec![trading_day("2026-08-01", true, utc_ns(2026, 8, 1, 0, 0, 0))],
                session: KlineSessionTemplate::cst_trading_day(),
                physical_segments: vec![BacktestHistoryPhysicalSegment {
                    physical_symbol: symbol.to_string(),
                    start_ns: utc_ns(2026, 7, 1, 0, 0, 0),
                    end_ns: utc_ns(2026, 8, 14, 0, 0, 0),
                }],
                snapshot_hash: String::new(),
            })
            .unwrap();
        let retained = cache
            .store_snapshot_for_remote_miss(BacktestHistoryMetadataSnapshot {
                schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
                market_kind: BacktestHistoryMarketKind::Futures,
                logical_symbol: symbol.to_string(),
                captured_at_ns: requested_start_ns,
                trading_days: vec![trading_day("2026-08-17", true, requested_start_ns)],
                session: KlineSessionTemplate::cst_trading_day(),
                physical_segments: vec![BacktestHistoryPhysicalSegment {
                    physical_symbol: symbol.to_string(),
                    start_ns: requested_start_ns,
                    end_ns: requested_end_ns,
                }],
                snapshot_hash: String::new(),
            })
            .unwrap();

        let plan = plan_request(
            &root,
            BacktestHistoryRequest::tick(1, symbol, requested_start_ns, requested_end_ns)
                .validate()
                .unwrap(),
        )
        .unwrap();

        assert_eq!(plan.snapshot_hash, retained.snapshot_hash);
    }

    #[test]
    fn daily_physical_plan_ignores_a_narrow_metadata_sidecar() {
        let root = temp_dir("daily-physical-narrow-metadata");
        let symbol = "CFFEX.T2609";
        let requested_start_ns = utc_ns(2020, 1, 1, 0, 0, 0);
        let requested_end_ns = utc_ns(2026, 8, 20, 0, 0, 0);
        let retained = BacktestHistoryMetadataCache::open(&root)
            .unwrap()
            .store_snapshot(BacktestHistoryMetadataSnapshot {
                schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
                market_kind: BacktestHistoryMarketKind::Futures,
                logical_symbol: symbol.to_string(),
                captured_at_ns: requested_end_ns,
                trading_days: vec![trading_day("2026-08-17", true, requested_end_ns)],
                session: KlineSessionTemplate::cst_trading_day(),
                physical_segments: vec![BacktestHistoryPhysicalSegment {
                    physical_symbol: symbol.to_string(),
                    start_ns: utc_ns(2026, 8, 1, 0, 0, 0),
                    end_ns: requested_end_ns,
                }],
                snapshot_hash: String::new(),
            })
            .unwrap();

        let plan = plan_request(
            &root,
            BacktestHistoryRequest::kline(
                1,
                symbol,
                Duration::from_secs(24 * 60 * 60),
                requested_start_ns,
                requested_end_ns,
            )
            .validate()
            .unwrap(),
        )
        .unwrap();

        assert_eq!(plan.snapshot_hash, retained.snapshot_hash);
        assert_eq!(
            plan.source_slices[0].range,
            (requested_start_ns, requested_end_ns)
        );
    }

    #[test]
    fn higher_minute_ranges_keep_the_trading_day_grid_across_breaks() {
        const MINUTE_NS: i64 = 60 * 1_000_000_000;
        const HOUR_NS: i64 = 60 * MINUTE_NS;
        let session = KlineSessionTemplate::new(
            "shfe-day-breaks",
            vec![
                KlineSessionWindow::new(15 * HOUR_NS, 16 * HOUR_NS + 15 * MINUTE_NS).unwrap(),
                KlineSessionWindow::new(
                    16 * HOUR_NS + 30 * MINUTE_NS,
                    17 * HOUR_NS + 30 * MINUTE_NS,
                )
                .unwrap(),
                KlineSessionWindow::new(19 * HOUR_NS + 30 * MINUTE_NS, 21 * HOUR_NS).unwrap(),
            ],
        )
        .unwrap();
        let ten_am = utc_ns(2026, 1, 5, 2, 0, 0);
        let ten_thirty_am = utc_ns(2026, 1, 5, 2, 30, 0);
        let eleven_am = utc_ns(2026, 1, 5, 3, 0, 0);

        assert_eq!(
            expanded_bar_start(ten_thirty_am, HOUR_NS, &session).unwrap(),
            ten_am
        );
        assert_eq!(
            expanded_bar_end(ten_thirty_am + MINUTE_NS, HOUR_NS, &session).unwrap(),
            eleven_am
        );
        assert_eq!(bar_end_ns(ten_am, HOUR_NS, &session).unwrap(), eleven_am);
    }

    fn trading_day(date: &str, is_trading_day: bool, start_ns: i64) -> BacktestHistoryTradingDay {
        BacktestHistoryTradingDay {
            date: date.to_string(),
            is_trading_day,
            start_ns,
            end_ns: start_ns + 24 * 60 * 60 * 1_000_000_000,
        }
    }

    fn utc_ns(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> i64 {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
            .single()
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap()
    }

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "tqsdk-backtest-history-planner-{name}-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }
}
