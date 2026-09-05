//! Deterministic source selection and cache-range expansion for backtest
//! history requests.

use chrono::{Datelike, NaiveDate, Weekday};

use crate::aggregation::{KlineSessionPosition, KlineSessionTemplate};
use crate::backtest_tick_cache::{
    backtest_tick_trading_day_for_timestamp_ns, backtest_tick_trading_day_range,
};
use crate::daily_kline_cache::{DAILY_KLINE_DURATION_NS, DailyKlineCache};
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

/// Metadata and source selection retained for one context request.
///
/// This deliberately owns every value needed to derive a wider range without
/// reopening metadata or changing the physical mapping.
#[derive(Debug, Clone)]
pub(crate) struct FrozenBacktestHistoryPlanBasis {
    pub(crate) symbol: String,
    pub(crate) kind: BacktestHistoryKind,
    pub(crate) duration_ns: Option<i64>,
    pub(crate) base_source: PlannedBaseSource,
    pub(crate) session: KlineSessionTemplate,
    pub(crate) trading_days: Option<Vec<BacktestHistoryTradingDay>>,
    pub(crate) minute_snapshot: MinuteKlineCacheSnapshot,
    pub(crate) snapshot_hash: String,
    pub(crate) source_mapping_segments: Vec<BacktestHistoryPhysicalSegment>,
    pub(crate) finality: BacktestHistoryFinality,
    pub(crate) direct_native_daily: bool,
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
    pub(crate) proven_empty_ranges: Vec<(i64, i64)>,
    pub(crate) physical_segments: Vec<BacktestHistoryPhysicalSegment>,
    pub(crate) session: KlineSessionTemplate,
    pub(crate) minute_snapshot: MinuteKlineCacheSnapshot,
    pub(crate) snapshot_hash: String,
    pub(crate) finality: BacktestHistoryFinality,
    pub(crate) context_basis: FrozenBacktestHistoryPlanBasis,
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

impl FrozenBacktestHistoryPlanBasis {
    /// Derives one range plan without reopening metadata or changing source selection.
    pub(crate) fn plan_range(
        &self,
        request_id: BacktestHistoryRequestId,
        requested_range: (i64, i64),
    ) -> Result<PlannedBacktestHistoryRequest> {
        if requested_range.0 >= requested_range.1 {
            return Err(DataError::Validation(
                "context source range must have positive width".to_string(),
            ));
        }
        let effective_end_ns = requested_range.1;
        let physical_segments = if self.source_mapping_segments.is_empty()
            || (self.direct_native_daily && !self.symbol.starts_with("KQ.m@"))
        {
            vec![BacktestHistoryPhysicalSegment {
                physical_symbol: self.symbol.clone(),
                start_ns: requested_range.0,
                end_ns: effective_end_ns,
            }]
        } else {
            intersect_segments(
                self.source_mapping_segments.clone(),
                (requested_range.0, effective_end_ns),
            )
        };
        if self.symbol.starts_with("KQ.m@") && physical_segments.is_empty() {
            return Err(DataError::InvalidState(
                "frozen context metadata does not cover requested main-contract range",
            ));
        }
        let source_slices = match self.base_source {
            PlannedBaseSource::Tick if self.symbol.starts_with("KQ.m@") => physical_segments
                .iter()
                .enumerate()
                .map(|(physical_rank, segment)| {
                    let range = expand_tick_source_range(
                        (segment.start_ns, segment.end_ns),
                        self.duration_ns,
                        &self.session,
                        self.trading_days.as_deref(),
                    )?;
                    Ok(PlannedSourceSlice {
                        cache_symbol: segment.physical_symbol.clone(),
                        range: (range.0.max(segment.start_ns), range.1.min(segment.end_ns)),
                        physical_rank,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            PlannedBaseSource::Tick => vec![PlannedSourceSlice {
                cache_symbol: self.symbol.clone(),
                range: expand_tick_source_range(
                    requested_range,
                    self.duration_ns,
                    &self.session,
                    self.trading_days.as_deref(),
                )?,
                physical_rank: 0,
            }],
            PlannedBaseSource::CanonicalMinute if self.symbol.starts_with("KQ.") => {
                physical_segments
                    .iter()
                    .enumerate()
                    .map(|(physical_rank, segment)| {
                        let range = expand_minute_source_range(
                            (segment.start_ns, segment.end_ns),
                            self.duration_ns.expect("minute source has duration"),
                            &self.session,
                        )?;
                        Ok(PlannedSourceSlice {
                            cache_symbol: segment.physical_symbol.clone(),
                            range: (range.0.max(segment.start_ns), range.1.min(segment.end_ns)),
                            physical_rank,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?
            }
            PlannedBaseSource::CanonicalMinute => vec![PlannedSourceSlice {
                cache_symbol: self.symbol.clone(),
                range: expand_minute_source_range(
                    requested_range,
                    self.duration_ns.expect("minute source has duration"),
                    &self.session,
                )?,
                physical_rank: 0,
            }],
            PlannedBaseSource::CanonicalDaily => vec![PlannedSourceSlice {
                cache_symbol: self.symbol.clone(),
                range: requested_range,
                physical_rank: 0,
            }],
        };
        let expanded_source_range = source_slices
            .iter()
            .map(|slice| slice.range)
            .reduce(|left, right| (left.0.min(right.0), left.1.max(right.1)))
            .ok_or(DataError::InvalidState("context plan has no source slices"))?;
        let proven_empty_ranges = if self.base_source == PlannedBaseSource::Tick {
            known_non_trading_tick_ranges(
                self.trading_days.as_deref().unwrap_or_default(),
                expanded_source_range,
            )?
        } else {
            Vec::new()
        };
        let source_slices = if proven_empty_ranges.is_empty() {
            source_slices
        } else {
            exclude_proven_empty_tick_ranges(source_slices, proven_empty_ranges.as_slice())
        };
        Ok(PlannedBacktestHistoryRequest {
            request_id,
            symbol: self.symbol.clone(),
            kind: self.kind,
            duration_ns: self.duration_ns,
            base_source: self.base_source,
            requested_range,
            effective_end_ns,
            expanded_source_range,
            source_slices,
            proven_empty_ranges,
            physical_segments,
            session: self.session.clone(),
            minute_snapshot: self.minute_snapshot.clone(),
            snapshot_hash: self.snapshot_hash.clone(),
            finality: self.finality,
            context_basis: self.clone(),
        })
    }
}

/// Validates public source policy before an asynchronous run begins.
pub(crate) fn validate_source_policy(request: &ValidatedBacktestHistoryRequest) -> Result<()> {
    let base_source = classify_request(request)?;
    if request.provisional_as_of_ns.is_some()
        && base_source != PlannedBaseSource::Tick
        && request.duration_ns != Some(crate::MINUTE_KLINE_DURATION_NS)
    {
        return Err(DataError::Validation(
            "provisional_as_of_ns is supported only for Tick and Kline requests up to 60 seconds"
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
    // A cached raw 1d native series is self-describing: its file header carries
    // the immutable cache identity. Do not deserialize the much larger metadata
    // sidecar merely to rediscover that identity. Multi-day aggregation remains
    // metadata-backed because it needs the instrument session definition.
    let cached_direct_daily_snapshot =
        if is_direct_native_daily_cache_request(&request, base_source) {
            DailyKlineCache::open_read_only(cache_dir).stored_snapshot(request.symbol.as_str())?
        } else {
            None
        };
    let metadata = match base_source {
        PlannedBaseSource::CanonicalMinute => resolve_minute_cache_metadata_snapshot(
            cache_dir,
            request.symbol.as_str(),
            request.start_ns,
            effective_end_ns,
        )?,
        PlannedBaseSource::Tick => resolve_backtest_metadata_snapshot(
            cache_dir,
            request.symbol.as_str(),
            request.start_ns,
            effective_end_ns,
        )?,
        PlannedBaseSource::CanonicalDaily if cached_direct_daily_snapshot.is_some() => None,
        PlannedBaseSource::CanonicalDaily => resolve_backtest_metadata_snapshot(
            cache_dir,
            request.symbol.as_str(),
            request.start_ns,
            effective_end_ns,
        )?,
    };
    // A native daily request, except KQ.m@ main continuous, uses a single
    // server-side series. Its retained sidecar only binds existing cache
    // partitions; it does not limit the downloadable range. KQ.m@ still
    // requires its persisted dated physical mapping.
    let is_direct_native_daily = matches!(base_source, PlannedBaseSource::CanonicalDaily)
        && !request.symbol.starts_with("KQ.m@");
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
            let minute_snapshot =
                cached_direct_daily_snapshot.unwrap_or_else(MinuteKlineCacheSnapshot::cst_v1);
            let snapshot_hash = minute_snapshot.calendar_hash.clone();
            (
                session,
                None,
                minute_snapshot,
                snapshot_hash,
                physical_segments.clone(),
                physical_segments,
            )
        }
    };
    let physical_minute_request = matches!(base_source, PlannedBaseSource::CanonicalMinute)
        && !request.symbol.starts_with("KQ.");
    if !is_direct_native_daily
        && !physical_minute_request
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
        PlannedBaseSource::CanonicalMinute if physical_minute_request => source_mapping_segments
            .iter()
            .filter_map(|segment| {
                let requested_start_ns = segment.start_ns.max(request.start_ns);
                let requested_end_ns = segment.end_ns.min(effective_end_ns);
                (requested_start_ns < requested_end_ns)
                    .then_some((segment, (requested_start_ns, requested_end_ns)))
            })
            .enumerate()
            .map(|(rank, (segment, requested))| {
                let expanded = expand_minute_source_range(
                    requested,
                    request.duration_ns.unwrap_or(MINUTE_KLINE_DURATION_NS),
                    &session,
                )?;
                let range = (
                    expanded.0.max(segment.start_ns),
                    expanded.1.min(segment.end_ns),
                );
                if range.0 >= range.1 {
                    return Err(DataError::InvalidState(
                        "physical contract cannot supply expanded canonical-minute source range",
                    ));
                }
                Ok(PlannedSourceSlice {
                    cache_symbol: request.symbol.clone(),
                    range,
                    physical_rank: rank,
                })
            })
            .collect::<Result<Vec<_>>>()?,
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
    let proven_empty_ranges = if base_source == PlannedBaseSource::Tick {
        known_non_trading_tick_ranges(
            trading_days.as_deref().unwrap_or_default(),
            expanded_source_range,
        )?
    } else {
        Vec::new()
    };
    let source_segments = if proven_empty_ranges.is_empty() {
        source_segments
    } else {
        exclude_proven_empty_tick_ranges(source_segments, proven_empty_ranges.as_slice())
    };

    Ok(PlannedBacktestHistoryRequest {
        request_id: request.request_id,
        symbol: request.symbol.clone(),
        kind: request.kind,
        duration_ns: request.duration_ns,
        base_source,
        requested_range: (request.start_ns, request.end_ns),
        effective_end_ns,
        expanded_source_range,
        source_slices: source_segments,
        proven_empty_ranges,
        physical_segments,
        session: session.clone(),
        minute_snapshot: minute_snapshot.clone(),
        snapshot_hash: snapshot_hash.clone(),
        finality: request
            .provisional_as_of_ns
            .map_or(BacktestHistoryFinality::Final, |as_of_ns| {
                BacktestHistoryFinality::Provisional { as_of_ns }
            }),
        context_basis: FrozenBacktestHistoryPlanBasis {
            symbol: request.symbol,
            kind: request.kind,
            duration_ns: request.duration_ns,
            base_source,
            session,
            trading_days,
            minute_snapshot,
            snapshot_hash,
            source_mapping_segments,
            finality: request
                .provisional_as_of_ns
                .map_or(BacktestHistoryFinality::Final, |as_of_ns| {
                    BacktestHistoryFinality::Provisional { as_of_ns }
                }),
            direct_native_daily: is_direct_native_daily,
        },
    })
}

fn known_non_trading_tick_ranges(
    trading_days: &[BacktestHistoryTradingDay],
    requested_range: (i64, i64),
) -> Result<Vec<(i64, i64)>> {
    let mut ranges = Vec::new();
    for day in trading_days.iter().filter(|day| !day.is_trading_day) {
        let date = NaiveDate::parse_from_str(day.date.as_str(), "%Y-%m-%d").map_err(|error| {
            DataError::InvalidResponse(format!(
                "invalid trading calendar date {}: {error}",
                day.date
            ))
        })?;
        if matches!(date.weekday(), Weekday::Sat | Weekday::Sun) {
            continue;
        }
        let day_range = backtest_tick_trading_day_range(date)?;
        let range = (
            day_range.start_ns.max(requested_range.0),
            day_range.end_ns.min(requested_range.1),
        );
        if range.0 < range.1 {
            ranges.push(range);
        }
    }
    Ok(merge_adjacent_ranges(ranges))
}

fn exclude_proven_empty_tick_ranges(
    slices: Vec<PlannedSourceSlice>,
    proven_empty_ranges: &[(i64, i64)],
) -> Vec<PlannedSourceSlice> {
    slices
        .into_iter()
        .flat_map(|slice| {
            let mut remaining = vec![slice.range];
            for &empty in proven_empty_ranges {
                remaining = remaining
                    .into_iter()
                    .flat_map(|range| subtract_range(range, empty))
                    .collect();
            }
            remaining.into_iter().map(move |range| PlannedSourceSlice {
                cache_symbol: slice.cache_symbol.clone(),
                range,
                physical_rank: slice.physical_rank,
            })
        })
        .collect()
}

fn subtract_range(range: (i64, i64), excluded: (i64, i64)) -> Vec<(i64, i64)> {
    let overlap = (range.0.max(excluded.0), range.1.min(excluded.1));
    if overlap.0 >= overlap.1 {
        return vec![range];
    }
    let mut remaining = Vec::with_capacity(2);
    if range.0 < overlap.0 {
        remaining.push((range.0, overlap.0));
    }
    if overlap.1 < range.1 {
        remaining.push((overlap.1, range.1));
    }
    remaining
}

fn merge_adjacent_ranges(mut ranges: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
    ranges.sort_unstable();
    let mut merged: Vec<(i64, i64)> = Vec::new();
    for range in ranges {
        if let Some(previous) = merged.last_mut()
            && range.0 <= previous.1
        {
            previous.1 = previous.1.max(range.1);
        } else {
            merged.push(range);
        }
    }
    merged
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

pub(crate) fn is_direct_native_daily_cache_request(
    request: &ValidatedBacktestHistoryRequest,
    base_source: PlannedBaseSource,
) -> bool {
    matches!(base_source, PlannedBaseSource::CanonicalDaily)
        && request.duration_ns == Some(DAILY_KLINE_DURATION_NS)
        && !request.symbol.starts_with("KQ.m@")
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
        BacktestHistoryMetadataCache, BacktestHistoryMetadataSnapshot, DailyKlineCache,
        MinuteKlineCacheSnapshot,
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
    fn tick_source_slices_exclude_metadata_proven_weekday_holidays() {
        let april_3 = chrono::NaiveDate::from_ymd_opt(2024, 4, 3).unwrap();
        let april_4 = chrono::NaiveDate::from_ymd_opt(2024, 4, 4).unwrap();
        let april_5 = chrono::NaiveDate::from_ymd_opt(2024, 4, 5).unwrap();
        let april_8 = chrono::NaiveDate::from_ymd_opt(2024, 4, 8).unwrap();
        let start_ns = backtest_tick_trading_day_range(april_3).unwrap().start_ns;
        let end_ns = backtest_tick_trading_day_range(april_8).unwrap().end_ns;
        let holidays = vec![
            trading_day("2024-04-04", false, utc_ns(2024, 4, 3, 16, 0, 0)),
            trading_day("2024-04-05", false, utc_ns(2024, 4, 4, 16, 0, 0)),
            trading_day("2024-04-06", false, utc_ns(2024, 4, 5, 16, 0, 0)),
            trading_day("2024-04-07", false, utc_ns(2024, 4, 6, 16, 0, 0)),
        ];

        let proven_empty =
            known_non_trading_tick_ranges(holidays.as_slice(), (start_ns, end_ns)).unwrap();
        assert_eq!(
            proven_empty,
            vec![(
                backtest_tick_trading_day_range(april_4).unwrap().start_ns,
                backtest_tick_trading_day_range(april_5).unwrap().end_ns,
            )]
        );

        let slices = exclude_proven_empty_tick_ranges(
            vec![PlannedSourceSlice {
                cache_symbol: "SHFE.cu2502".to_string(),
                range: (start_ns, end_ns),
                physical_rank: 0,
            }],
            proven_empty.as_slice(),
        );
        assert_eq!(slices.len(), 2);
        assert_eq!(slices[0].range.1, proven_empty[0].0);
        assert_eq!(slices[1].range.0, proven_empty[0].1);

        let holiday_only = exclude_proven_empty_tick_ranges(
            vec![PlannedSourceSlice {
                cache_symbol: "SHFE.cu2502".to_string(),
                range: proven_empty[0],
                physical_rank: 0,
            }],
            proven_empty.as_slice(),
        );
        assert!(holiday_only.is_empty());
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
    fn provisional_policy_accepts_canonical_minute_and_rejects_larger_periods() {
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
        assert!(validate_source_policy(&minute).is_ok());

        let larger =
            BacktestHistoryRequest::kline(1, "SHFE.au2608", Duration::from_secs(120), 1, 2)
                .with_provisional_as_of_ns(2)
                .validate()
                .unwrap();
        assert!(validate_source_policy(&larger).is_err());
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
    fn canonical_minute_physical_plan_clips_to_metadata_coverage() {
        let root = temp_dir("minute-physical-metadata-coverage");
        let symbol = "CZCE.AP401";
        let requested_start_ns = utc_ns(2024, 1, 1, 0, 0, 0);
        let source_start_ns = utc_ns(2024, 1, 3, 0, 0, 0);
        let requested_end_ns = utc_ns(2024, 2, 1, 0, 0, 0);
        let cache = BacktestHistoryMetadataCache::open(&root).unwrap();
        let snapshot = cache
            .store_snapshot(BacktestHistoryMetadataSnapshot {
                schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
                market_kind: BacktestHistoryMarketKind::Futures,
                logical_symbol: symbol.to_string(),
                captured_at_ns: requested_end_ns,
                trading_days: vec![trading_day("2024-01-03", true, source_start_ns)],
                session: KlineSessionTemplate::cst_trading_day(),
                physical_segments: vec![BacktestHistoryPhysicalSegment {
                    physical_symbol: symbol.to_string(),
                    start_ns: source_start_ns,
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
                Duration::from_secs(60),
                requested_start_ns,
                requested_end_ns,
            )
            .validate()
            .unwrap(),
        )
        .unwrap();

        assert_eq!(plan.snapshot_hash, snapshot.snapshot_hash);
        assert_eq!(plan.source_slices.len(), 1);
        assert_eq!(plan.source_slices[0].cache_symbol, symbol);
        assert_eq!(
            plan.source_slices[0].range,
            (source_start_ns, requested_end_ns)
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn canonical_minute_logical_plan_rejects_narrow_metadata_sidecar() {
        let root = temp_dir("minute-logical-narrow-metadata");
        let symbol = "KQ.i@CZCE.AP";
        let requested_start_ns = utc_ns(2024, 1, 1, 0, 0, 0);
        let source_start_ns = utc_ns(2024, 1, 3, 0, 0, 0);
        let requested_end_ns = utc_ns(2024, 2, 1, 0, 0, 0);
        let cache = BacktestHistoryMetadataCache::open(&root).unwrap();
        cache
            .store_snapshot(BacktestHistoryMetadataSnapshot {
                schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
                market_kind: BacktestHistoryMarketKind::Futures,
                logical_symbol: symbol.to_string(),
                captured_at_ns: requested_end_ns,
                trading_days: vec![trading_day("2024-01-03", true, source_start_ns)],
                session: KlineSessionTemplate::cst_trading_day(),
                physical_segments: vec![BacktestHistoryPhysicalSegment {
                    physical_symbol: symbol.to_string(),
                    start_ns: source_start_ns,
                    end_ns: requested_end_ns,
                }],
                snapshot_hash: String::new(),
            })
            .unwrap();

        let error = plan_request(
            &root,
            BacktestHistoryRequest::kline(
                1,
                symbol,
                Duration::from_secs(60),
                requested_start_ns,
                requested_end_ns,
            )
            .validate()
            .unwrap(),
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("metadata does not cover the requested range")
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn daily_physical_plan_ignores_a_narrow_metadata_sidecar() {
        let root = temp_dir("daily-physical-narrow-metadata");
        let symbol = "CFFEX.T2609";
        let requested_start_ns = utc_ns(2020, 1, 1, 0, 0, 0);
        let requested_end_ns = utc_ns(2026, 8, 20, 0, 0, 0);
        let cache = BacktestHistoryMetadataCache::open(&root).unwrap();
        let retained = cache
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

        let index_symbol = "KQ.i@CFFEX.IC";
        let index_retained = cache
            .store_snapshot(BacktestHistoryMetadataSnapshot {
                schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
                market_kind: BacktestHistoryMarketKind::Futures,
                logical_symbol: index_symbol.to_string(),
                captured_at_ns: requested_end_ns,
                trading_days: vec![trading_day("2026-08-17", true, requested_end_ns)],
                session: KlineSessionTemplate::cst_trading_day(),
                physical_segments: vec![BacktestHistoryPhysicalSegment {
                    physical_symbol: index_symbol.to_string(),
                    start_ns: utc_ns(2026, 8, 1, 0, 0, 0),
                    end_ns: requested_end_ns,
                }],
                snapshot_hash: String::new(),
            })
            .unwrap();
        let index_plan = plan_request(
            &root,
            BacktestHistoryRequest::kline(
                2,
                index_symbol,
                Duration::from_secs(24 * 60 * 60),
                requested_start_ns,
                requested_end_ns,
            )
            .validate()
            .unwrap(),
        )
        .unwrap();
        assert_eq!(index_plan.snapshot_hash, index_retained.snapshot_hash);
        assert_eq!(
            index_plan.source_slices[0].range,
            (requested_start_ns, requested_end_ns)
        );
    }

    #[test]
    fn direct_daily_plan_reuses_file_snapshot_without_metadata_sidecar() {
        let root = temp_dir("daily-file-snapshot");
        let symbol = "CFFEX.T2609";
        let start_ns = utc_ns(2026, 8, 17, 0, 0, 0);
        let end_ns = start_ns + DAILY_KLINE_DURATION_NS;
        let snapshot =
            MinuteKlineCacheSnapshot::new(7, "legacy-daily-cache-snapshot", "legacy-daily-session")
                .unwrap();
        DailyKlineCache::open(&root)
            .unwrap()
            .store_final_range_at(
                symbol,
                start_ns,
                end_ns,
                &snapshot,
                &[],
                end_ns + DAILY_KLINE_DURATION_NS,
            )
            .unwrap();

        let plan = plan_request(
            &root,
            BacktestHistoryRequest::kline(
                1,
                symbol,
                Duration::from_secs(24 * 60 * 60),
                start_ns,
                end_ns,
            )
            .validate()
            .unwrap(),
        )
        .unwrap();

        assert_eq!(plan.minute_snapshot, snapshot);
        assert_eq!(plan.snapshot_hash, "legacy-daily-cache-snapshot");
        std::fs::remove_dir_all(root).unwrap();
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
