//! Anchor-centred, row-counted historical reads.

use std::collections::BTreeMap;
use std::future::Future;

use tqsdk_core::{Kline, Tick};

use super::{
    BacktestHistoryCollected, BacktestHistoryFailureReason, BacktestHistoryKind,
    BacktestHistoryRequest, BacktestHistoryRequestId, BacktestHistoryRequestReport,
    BacktestHistoryRows, BacktestHistorySnapshotError,
};

const INITIAL_TICK_SPAN_NS: i64 = 1_000_000_000;
const MAX_CONTEXT_SCANS: usize = 16;
const MAX_CONTEXT_SPAN_NS: i64 = 90 * 24 * 60 * 60 * 1_000_000_000;

/// One anchor-centred historical read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestHistoryContextRequest {
    pub request_id: BacktestHistoryRequestId,
    pub symbol: String,
    pub kind: BacktestHistoryKind,
    pub anchor_ns: i64,
    /// Optional Tick row id used with `anchor_ns` as one stable sort key.
    pub anchor_row_id: Option<i64>,
    pub before_rows: usize,
    pub after_rows: usize,
    /// Authoritative final upper bound, when caller has one.
    ///
    /// Relay supplies its request-scoped server-time bound. Offline callers leave this unset.
    pub effective_end_ns: Option<i64>,
}

impl BacktestHistoryContextRequest {
    #[must_use]
    pub fn new(
        request_id: BacktestHistoryRequestId,
        symbol: impl Into<String>,
        kind: BacktestHistoryKind,
        anchor_ns: i64,
        before_rows: usize,
        after_rows: usize,
    ) -> Self {
        Self {
            request_id,
            symbol: symbol.into(),
            kind,
            anchor_ns,
            anchor_row_id: None,
            before_rows,
            after_rows,
            effective_end_ns: None,
        }
    }

    #[must_use]
    pub fn with_anchor_row_id(mut self, anchor_row_id: i64) -> Self {
        self.anchor_row_id = Some(anchor_row_id);
        self
    }

    #[must_use]
    pub fn with_effective_end_ns(mut self, effective_end_ns: i64) -> Self {
        self.effective_end_ns = Some(effective_end_ns);
        self
    }

    pub(crate) fn validate(&self) -> Result<(), BacktestHistorySnapshotError> {
        if self.symbol.trim().is_empty() {
            return Err(context_error(self, "context symbol must not be empty"));
        }
        if matches!(self.kind, BacktestHistoryKind::Kline { .. }) && self.anchor_row_id.is_some() {
            return Err(context_error(
                self,
                "anchor_row_id is only valid for Tick context requests",
            ));
        }
        self.before_rows
            .checked_add(1)
            .and_then(|value| value.checked_add(self.after_rows))
            .ok_or_else(|| context_error(self, "context row count overflows usize"))?;
        if self
            .effective_end_ns
            .is_some_and(|effective_end_ns| effective_end_ns <= self.anchor_ns)
        {
            return Err(context_error(
                self,
                "context effective_end_ns must be after anchor_ns",
            ));
        }
        Ok(())
    }
}

pub(crate) fn initial_context_range(request: &BacktestHistoryContextRequest) -> (i64, i64) {
    let span = initial_span_ns(request);
    let end = request
        .anchor_ns
        .saturating_add(span)
        .saturating_add(1)
        .min(request.effective_end_ns.unwrap_or(i64::MAX));
    (request.anchor_ns.saturating_sub(span), end)
}

/// Proven endpoint reached while satisfying an anchor-centred request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BacktestHistoryContextBoundary {
    /// Retained for sources that can prove a historical lower bound.
    HistoryStart,
    /// Request-scoped final upper bound reached.
    FutureEnd { effective_end_ns: i64 },
}

/// Materialized anchor-centred result.
#[derive(Debug, Clone)]
pub struct BacktestHistoryContextResult {
    pub rows: BacktestHistoryRows,
    pub report: BacktestHistoryRequestReport,
    pub requested_anchor_ns: i64,
    pub matched_anchor_ns: i64,
    pub matched_anchor_row_id: Option<i64>,
    pub anchor_index: usize,
    pub requested_before: usize,
    pub requested_after: usize,
    pub actual_before: usize,
    pub actual_after: usize,
    pub complete: bool,
    pub left_boundary: Option<BacktestHistoryContextBoundary>,
    pub right_boundary: Option<BacktestHistoryContextBoundary>,
}

/// Executes bounded delta scans under a caller-provided fixed read view.
pub(crate) async fn collect_context<F, Fut>(
    request: BacktestHistoryContextRequest,
    mut scan: F,
) -> Result<BacktestHistoryContextResult, BacktestHistorySnapshotError>
where
    F: FnMut(BacktestHistoryRequest) -> Fut,
    Fut: Future<Output = Result<BacktestHistoryCollected, BacktestHistorySnapshotError>>,
{
    request.validate()?;
    let mut accumulator = ContextAccumulator::new(request.kind);
    let mut left_span = initial_span_ns(&request);
    let mut right_span = left_span;
    let mut left = request.anchor_ns.saturating_sub(left_span);
    let upper_bound = request.effective_end_ns.unwrap_or(i64::MAX);
    let mut right = request
        .anchor_ns
        .saturating_add(left_span)
        .saturating_add(1)
        .min(upper_bound);
    let mut scans = 0usize;

    collect_range(&request, left, right, &mut accumulator, &mut scan).await?;
    scans += 1;

    loop {
        let Some(anchor) = accumulator.anchor(&request) else {
            if left == i64::MIN {
                return Err(BacktestHistorySnapshotError::new(
                    BacktestHistoryFailureReason::AnchorNotFound,
                    Some(request.request_id),
                    Some(request.symbol.clone()),
                    "no history row exists at or before context anchor",
                ));
            }
            if scans >= MAX_CONTEXT_SCANS {
                return Err(BacktestHistorySnapshotError::new(
                    BacktestHistoryFailureReason::HistoryTimeout,
                    Some(request.request_id),
                    Some(request.symbol.clone()),
                    "context scan limit reached before locating anchor",
                ));
            }
            let next_left = left.saturating_sub(left_span);
            collect_range(&request, next_left, left, &mut accumulator, &mut scan).await?;
            scans += 1;
            left = next_left;
            left_span = left_span.saturating_mul(2).max(1);
            continue;
        };

        let (actual_before, actual_after) = accumulator.counts_around(anchor.index);
        let need_left = actual_before < request.before_rows;
        let need_right = actual_after < request.after_rows;
        if !need_left && !need_right {
            return finalize_context(&request, left, right, &mut scan, None).await;
        }

        let mut right_boundary = None;
        if need_left {
            if left == i64::MIN || scans >= MAX_CONTEXT_SCANS {
                return Err(BacktestHistorySnapshotError::new(
                    BacktestHistoryFailureReason::HistoryTimeout,
                    Some(request.request_id),
                    Some(request.symbol.clone()),
                    "context scan limit reached before requested preceding rows",
                ));
            }
            let next_left = left.saturating_sub(left_span);
            collect_range(&request, next_left, left, &mut accumulator, &mut scan).await?;
            scans += 1;
            left = next_left;
            left_span = left_span.saturating_mul(2).max(1);
        }
        if need_right {
            if right >= upper_bound {
                right_boundary = request.effective_end_ns.map(|effective_end_ns| {
                    BacktestHistoryContextBoundary::FutureEnd { effective_end_ns }
                });
            } else if scans >= MAX_CONTEXT_SCANS {
                return Err(BacktestHistorySnapshotError::new(
                    BacktestHistoryFailureReason::HistoryTimeout,
                    Some(request.request_id),
                    Some(request.symbol.clone()),
                    "context scan limit reached before requested following rows",
                ));
            } else {
                let next_right = right.saturating_add(right_span).min(upper_bound);
                collect_range(&request, right, next_right, &mut accumulator, &mut scan).await?;
                scans += 1;
                right = next_right;
                right_span = right_span.saturating_mul(2).max(1);
            }
        }
        if let Some(boundary) = right_boundary {
            return finalize_context(&request, left, right, &mut scan, Some(boundary)).await;
        }
    }
}

async fn collect_range<F, Fut>(
    context: &BacktestHistoryContextRequest,
    start_ns: i64,
    end_ns: i64,
    accumulator: &mut ContextAccumulator,
    scan: &mut F,
) -> Result<(), BacktestHistorySnapshotError>
where
    F: FnMut(BacktestHistoryRequest) -> Fut,
    Fut: Future<Output = Result<BacktestHistoryCollected, BacktestHistorySnapshotError>>,
{
    if start_ns >= end_ns {
        return Ok(());
    }
    if end_ns.saturating_sub(start_ns) > MAX_CONTEXT_SPAN_NS {
        return Err(BacktestHistorySnapshotError::new(
            BacktestHistoryFailureReason::HistoryTimeout,
            Some(context.request_id),
            Some(context.symbol.clone()),
            "context scan span exceeds bounded history window",
        ));
    }
    let request = match context.kind {
        BacktestHistoryKind::Tick => BacktestHistoryRequest::tick(
            context.request_id,
            context.symbol.clone(),
            start_ns,
            end_ns,
        ),
        BacktestHistoryKind::Kline { duration } => BacktestHistoryRequest::kline(
            context.request_id,
            context.symbol.clone(),
            duration,
            start_ns,
            end_ns,
        ),
    };
    let collected = scan(request).await?;
    if collected.rows.len() > candidate_row_limit(context) {
        return Err(BacktestHistorySnapshotError::new(
            BacktestHistoryFailureReason::ContextScanLimitExceeded {
                limit_rows: candidate_row_limit(context),
            },
            Some(context.request_id),
            Some(context.symbol.clone()),
            "one context scan exceeded its candidate row allowance",
        ));
    }
    accumulator.append(collected.rows).map_err(|message| {
        BacktestHistorySnapshotError::new(
            BacktestHistoryFailureReason::Internal,
            Some(context.request_id),
            Some(context.symbol.clone()),
            message,
        )
    })?;
    if accumulator.len() > candidate_row_limit(context) {
        return Err(BacktestHistorySnapshotError::new(
            BacktestHistoryFailureReason::ContextScanLimitExceeded {
                limit_rows: candidate_row_limit(context),
            },
            Some(context.request_id),
            Some(context.symbol.clone()),
            "context candidate window exceeds bounded row budget",
        ));
    }
    Ok(())
}

async fn finalize_context<F, Fut>(
    context: &BacktestHistoryContextRequest,
    start_ns: i64,
    end_ns: i64,
    scan: &mut F,
    right_boundary: Option<BacktestHistoryContextBoundary>,
) -> Result<BacktestHistoryContextResult, BacktestHistorySnapshotError>
where
    F: FnMut(BacktestHistoryRequest) -> Fut,
    Fut: Future<Output = Result<BacktestHistoryCollected, BacktestHistorySnapshotError>>,
{
    let request = match context.kind {
        BacktestHistoryKind::Tick => BacktestHistoryRequest::tick(
            context.request_id,
            context.symbol.clone(),
            start_ns,
            end_ns,
        ),
        BacktestHistoryKind::Kline { duration } => BacktestHistoryRequest::kline(
            context.request_id,
            context.symbol.clone(),
            duration,
            start_ns,
            end_ns,
        ),
    };
    let collected = scan(request).await?;
    if collected.rows.len() > candidate_row_limit(context) {
        return Err(BacktestHistorySnapshotError::new(
            BacktestHistoryFailureReason::ContextScanLimitExceeded {
                limit_rows: candidate_row_limit(context),
            },
            Some(context.request_id),
            Some(context.symbol.clone()),
            "final context scan exceeded its candidate row allowance",
        ));
    }
    let mut accumulator = ContextAccumulator::new(context.kind);
    accumulator.append(collected.rows).map_err(|message| {
        BacktestHistorySnapshotError::new(
            BacktestHistoryFailureReason::Internal,
            Some(context.request_id),
            Some(context.symbol.clone()),
            message,
        )
    })?;
    let anchor = accumulator.anchor(context).ok_or_else(|| {
        BacktestHistorySnapshotError::new(
            BacktestHistoryFailureReason::AnchorNotFound,
            Some(context.request_id),
            Some(context.symbol.clone()),
            "final context scan no longer contains anchor",
        )
    })?;
    Ok(accumulator.finish(context, anchor, collected.request, right_boundary))
}

fn initial_span_ns(request: &BacktestHistoryContextRequest) -> i64 {
    let rows = request
        .before_rows
        .saturating_add(request.after_rows)
        .saturating_add(1) as i64;
    match request.kind {
        BacktestHistoryKind::Tick => INITIAL_TICK_SPAN_NS,
        BacktestHistoryKind::Kline { duration } => i64::try_from(duration.as_nanos())
            .unwrap_or(i64::MAX)
            .saturating_mul(rows.saturating_mul(2).max(1))
            .clamp(
                i64::try_from(duration.as_nanos())
                    .unwrap_or(i64::MAX)
                    .saturating_mul(2),
                14 * 24 * 60 * 60 * 1_000_000_000,
            )
            .max(1),
    }
}

fn candidate_row_limit(request: &BacktestHistoryContextRequest) -> usize {
    request
        .before_rows
        .saturating_add(request.after_rows)
        .saturating_add(1)
        .saturating_mul(2)
        .max(1_024)
}

fn context_error(
    request: &BacktestHistoryContextRequest,
    message: &'static str,
) -> BacktestHistorySnapshotError {
    BacktestHistorySnapshotError::new(
        BacktestHistoryFailureReason::InvalidRequest,
        Some(request.request_id),
        Some(request.symbol.clone()),
        message,
    )
}

#[derive(Debug)]
enum ContextAccumulator {
    Ticks(BTreeMap<(i64, i64), Tick>),
    Klines {
        duration_ns: i64,
        rows: BTreeMap<i64, Kline>,
    },
}

#[derive(Debug, Clone, Copy)]
struct AnchorMatch {
    index: usize,
    timestamp_ns: i64,
    row_id: Option<i64>,
}

impl ContextAccumulator {
    fn new(kind: BacktestHistoryKind) -> Self {
        match kind {
            BacktestHistoryKind::Tick => Self::Ticks(BTreeMap::new()),
            BacktestHistoryKind::Kline { duration } => Self::Klines {
                duration_ns: i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX),
                rows: BTreeMap::new(),
            },
        }
    }

    fn append(&mut self, rows: BacktestHistoryRows) -> Result<(), &'static str> {
        match (self, rows) {
            (Self::Ticks(target), BacktestHistoryRows::Ticks(rows)) => {
                for row in rows {
                    target.insert((row.datetime, row.id), row);
                }
                Ok(())
            }
            (
                Self::Klines {
                    duration_ns,
                    rows: target,
                },
                BacktestHistoryRows::Klines {
                    duration_ns: incoming,
                    rows,
                },
            ) if *duration_ns == incoming => {
                for row in rows {
                    target.insert(row.datetime, row);
                }
                Ok(())
            }
            _ => Err("context scan returned a row kind different from request kind"),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Ticks(rows) => rows.len(),
            Self::Klines { rows, .. } => rows.len(),
        }
    }

    fn anchor(&self, request: &BacktestHistoryContextRequest) -> Option<AnchorMatch> {
        match self {
            Self::Ticks(rows) => {
                let max_id = request.anchor_row_id.unwrap_or(i64::MAX);
                let (&(timestamp_ns, row_id), _) =
                    rows.range(..=(request.anchor_ns, max_id)).next_back()?;
                let index = rows
                    .range(..=(timestamp_ns, row_id))
                    .count()
                    .saturating_sub(1);
                Some(AnchorMatch {
                    index,
                    timestamp_ns,
                    row_id: Some(row_id),
                })
            }
            Self::Klines { rows, .. } => {
                let (&timestamp_ns, _) = rows.range(..=request.anchor_ns).next_back()?;
                let index = rows.range(..=timestamp_ns).count().saturating_sub(1);
                Some(AnchorMatch {
                    index,
                    timestamp_ns,
                    row_id: None,
                })
            }
        }
    }

    fn counts_around(&self, anchor_index: usize) -> (usize, usize) {
        let len = match self {
            Self::Ticks(rows) => rows.len(),
            Self::Klines { rows, .. } => rows.len(),
        };
        (
            anchor_index,
            len.saturating_sub(anchor_index.saturating_add(1)),
        )
    }

    fn finish(
        self,
        request: &BacktestHistoryContextRequest,
        anchor: AnchorMatch,
        mut report: BacktestHistoryRequestReport,
        right_boundary: Option<BacktestHistoryContextBoundary>,
    ) -> BacktestHistoryContextResult {
        let (rows, actual_before, actual_after) = match self {
            Self::Ticks(rows) => {
                let values = rows.into_values().collect::<Vec<_>>();
                let start = anchor.index.saturating_sub(request.before_rows);
                let end = anchor
                    .index
                    .saturating_add(request.after_rows)
                    .saturating_add(1)
                    .min(values.len());
                (
                    BacktestHistoryRows::Ticks(values[start..end].to_vec()),
                    anchor.index.saturating_sub(start),
                    end.saturating_sub(anchor.index.saturating_add(1)),
                )
            }
            Self::Klines { duration_ns, rows } => {
                let values = rows.into_values().collect::<Vec<_>>();
                let start = anchor.index.saturating_sub(request.before_rows);
                let end = anchor
                    .index
                    .saturating_add(request.after_rows)
                    .saturating_add(1)
                    .min(values.len());
                (
                    BacktestHistoryRows::Klines {
                        duration_ns,
                        rows: values[start..end].to_vec(),
                    },
                    anchor.index.saturating_sub(start),
                    end.saturating_sub(anchor.index.saturating_add(1)),
                )
            }
        };
        report.rows = rows.len();
        let complete = actual_before == request.before_rows && actual_after == request.after_rows;
        BacktestHistoryContextResult {
            rows,
            report,
            requested_anchor_ns: request.anchor_ns,
            matched_anchor_ns: anchor.timestamp_ns,
            matched_anchor_row_id: anchor.row_id,
            anchor_index: actual_before,
            requested_before: request.before_rows,
            requested_after: request.after_rows,
            actual_before,
            actual_after,
            complete,
            left_boundary: None,
            right_boundary,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tqsdk_core::{Kline, Tick};

    use super::{
        BacktestHistoryContextRequest, BacktestHistoryKind, BacktestHistoryRows, ContextAccumulator,
    };

    #[test]
    fn tick_anchor_uses_timestamp_and_id() {
        let request = BacktestHistoryContextRequest::new(
            7,
            "SHFE.au2608",
            BacktestHistoryKind::Tick,
            100,
            0,
            0,
        )
        .with_anchor_row_id(2);
        let mut rows = ContextAccumulator::new(request.kind);
        rows.append(BacktestHistoryRows::Ticks(vec![
            Tick {
                datetime: 100,
                id: 1,
                ..Tick::default()
            },
            Tick {
                datetime: 100,
                id: 2,
                ..Tick::default()
            },
            Tick {
                datetime: 100,
                id: 3,
                ..Tick::default()
            },
        ]))
        .unwrap();
        let anchor = rows.anchor(&request).unwrap();
        assert_eq!((anchor.timestamp_ns, anchor.row_id), (100, Some(2)));
    }

    #[test]
    fn kline_anchor_floors_to_bar_start() {
        let request = BacktestHistoryContextRequest::new(
            8,
            "SHFE.au2608",
            BacktestHistoryKind::Kline {
                duration: Duration::from_secs(1),
            },
            150,
            0,
            0,
        );
        let mut rows = ContextAccumulator::new(request.kind);
        rows.append(BacktestHistoryRows::Klines {
            duration_ns: 1_000_000_000,
            rows: vec![
                Kline {
                    datetime: 100,
                    ..Kline::default()
                },
                Kline {
                    datetime: 200,
                    ..Kline::default()
                },
            ],
        })
        .unwrap();
        assert_eq!(rows.anchor(&request).unwrap().timestamp_ns, 100);
    }
}
