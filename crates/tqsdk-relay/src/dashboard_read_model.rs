use std::collections::{BTreeMap, VecDeque};

use serde::Serialize;

use crate::engine::RelayEvent;
use crate::observability::{DEFAULT_DATA_STALE_AFTER_SECS, MetricsSnapshot, RelaySourceStage};
use crate::symbol_metrics::{
    SymbolFlow, SymbolIntegrity, SymbolMetricsContext, SymbolMetricsQuery, SymbolMetricsSnapshot,
    SymbolMetricsSummary, SymbolProblemSeverity, SymbolSession, SymbolStatus,
    SymbolSubscriptionCounts, SymbolTelemetryReadModel, SymbolTelemetrySnapshot,
    SymbolTradingPhase, SymbolTradingPhaseSource,
};

const DASHBOARD_TIMELINE_HISTORY_WINDOW_MILLIS: u64 = 300_000;
const DASHBOARD_TIMELINE_HISTORY_MIN_SAMPLE_INTERVAL_MILLIS: u64 = 2_000;
const DASHBOARD_TIMELINE_HISTORY_SAMPLE_LIMIT: usize = 180;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DashboardSnapshot {
    pub received_at_unix_millis: u64,
    pub metrics: MetricsSnapshot,
    pub global: SymbolMetricsSummary,
    pub timeline: DashboardTimelineSample,
    pub timeline_symbols: Vec<DashboardSymbolRow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeline_history: Option<DashboardTimelineHistory>,
    pub page: DashboardSymbolMetricsSnapshot,
    pub events: Vec<RelayEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DashboardSymbolMetricsSnapshot {
    pub now_unix_millis: u64,
    pub data_stale_after_millis: u64,
    pub summary: SymbolMetricsSummary,
    pub filtered_total: usize,
    pub symbols: Vec<DashboardSymbolRow>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DashboardSymbolRow {
    pub symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instrument_name: Option<String>,
    #[serde(skip_serializing_if = "is_symbol_status_live")]
    pub status: SymbolStatus,
    #[serde(skip_serializing_if = "is_symbol_session_open")]
    pub session: SymbolSession,
    #[serde(skip_serializing_if = "is_symbol_phase_continuous")]
    pub phase: SymbolTradingPhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase_source: Option<SymbolTradingPhaseSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_trade_status: Option<String>,
    #[serde(skip_serializing_if = "is_symbol_flow_flowing")]
    pub flow: SymbolFlow,
    #[serde(skip_serializing_if = "is_symbol_integrity_intact")]
    pub integrity: SymbolIntegrity,
    #[serde(skip_serializing_if = "is_false")]
    pub problem: bool,
    #[serde(skip_serializing_if = "is_symbol_problem_severity_live")]
    pub problem_severity: SymbolProblemSeverity,
    #[serde(skip_serializing_if = "is_false")]
    pub subscribed: bool,
    #[serde(skip_serializing_if = "is_zero_usize")]
    pub quote_subscriber_count: usize,
    #[serde(skip_serializing_if = "is_zero_usize")]
    pub chart_subscriber_count: usize,
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub ticks_ingested: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receive_gap_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_receive_gap_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub market_time_lag_ms: Option<u64>,
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub invalid_rows: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_invalid_row_error: Option<String>,
}

impl DashboardSymbolMetricsSnapshot {
    fn from_symbol_metrics(snapshot: SymbolMetricsSnapshot) -> Self {
        Self {
            now_unix_millis: snapshot.now_unix_millis,
            data_stale_after_millis: snapshot.data_stale_after_millis,
            summary: snapshot.summary,
            filtered_total: snapshot.filtered_total,
            symbols: snapshot
                .symbols
                .into_iter()
                .map(DashboardSymbolRow::from_symbol_metrics)
                .collect(),
        }
    }
}

impl DashboardSymbolRow {
    fn from_symbol_metrics(row: SymbolTelemetrySnapshot) -> Self {
        let expose_phase_detail = row.phase != SymbolTradingPhase::Continuous;
        Self {
            symbol: row.symbol,
            instrument_name: row.instrument_name,
            status: row.status,
            session: row.session,
            phase: row.phase,
            phase_source: expose_phase_detail.then_some(row.phase_source).flatten(),
            raw_trade_status: expose_phase_detail
                .then_some(row.raw_trade_status)
                .flatten(),
            flow: row.flow,
            integrity: row.integrity,
            problem: row.problem,
            problem_severity: row.problem_severity,
            subscribed: row.subscribed,
            quote_subscriber_count: row.quote_subscriber_count,
            chart_subscriber_count: row.chart_subscriber_count,
            ticks_ingested: row.ticks_ingested,
            receive_gap_ms: row.receive_gap_ms,
            avg_receive_gap_ms: row.avg_receive_gap_ms,
            market_time_lag_ms: row.market_time_lag_ms,
            invalid_rows: row.invalid_rows,
            last_invalid_row_error: row.last_invalid_row_error,
        }
    }
}

fn dashboard_symbol_rows(rows: &[SymbolTelemetrySnapshot]) -> Vec<DashboardSymbolRow> {
    rows.iter()
        .cloned()
        .map(DashboardSymbolRow::from_symbol_metrics)
        .collect()
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

fn is_zero_usize(value: &usize) -> bool {
    *value == 0
}

fn is_symbol_status_live(value: &SymbolStatus) -> bool {
    *value == SymbolStatus::Live
}

fn is_symbol_session_open(value: &SymbolSession) -> bool {
    *value == SymbolSession::Open
}

fn is_symbol_phase_continuous(value: &SymbolTradingPhase) -> bool {
    *value == SymbolTradingPhase::Continuous
}

fn is_symbol_flow_flowing(value: &SymbolFlow) -> bool {
    *value == SymbolFlow::Flowing
}

fn is_symbol_integrity_intact(value: &SymbolIntegrity) -> bool {
    *value == SymbolIntegrity::Intact
}

fn is_symbol_problem_severity_live(value: &SymbolProblemSeverity) -> bool {
    *value == SymbolProblemSeverity::Live
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DashboardTimelineSeverity {
    Live,
    Closed,
    Auction,
    Warn,
    Bad,
    Unknown,
    NoSample,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DashboardTimelineScope {
    pub severity: DashboardTimelineSeverity,
    pub total: usize,
    pub problem: usize,
    pub receive_gap_ms: Option<u64>,
    pub avg_receive_gap_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DashboardTimelineSample {
    pub global: DashboardTimelineScope,
    pub subscribed: DashboardTimelineScope,
    pub exchanges: BTreeMap<String, DashboardTimelineScope>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DashboardTimelineSymbolSample {
    pub severity: DashboardTimelineSeverity,
    pub receive_gap_ms: Option<u64>,
    pub avg_receive_gap_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DashboardTimelineHistorySample {
    pub sampled_at_unix_millis: u64,
    pub sample: DashboardTimelineSample,
    pub symbols: BTreeMap<String, DashboardTimelineSymbolSample>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct DashboardTimelineHistory {
    pub samples: Vec<DashboardTimelineHistorySample>,
}

#[derive(Debug, Clone)]
pub(crate) struct DashboardTimelineHistoryCache {
    samples: VecDeque<DashboardTimelineHistorySample>,
}

impl Default for DashboardTimelineHistoryCache {
    fn default() -> Self {
        Self {
            samples: VecDeque::with_capacity(DASHBOARD_TIMELINE_HISTORY_SAMPLE_LIMIT),
        }
    }
}

impl DashboardTimelineHistoryCache {
    pub(crate) fn push(&mut self, sample: DashboardTimelineHistorySample) {
        let sampled_at = sample.sampled_at_unix_millis;
        self.prune(sampled_at);
        if let Some(last) = self.samples.back_mut() {
            if sampled_at
                < last
                    .sampled_at_unix_millis
                    .saturating_add(DASHBOARD_TIMELINE_HISTORY_MIN_SAMPLE_INTERVAL_MILLIS)
            {
                *last = sample;
                self.prune(sampled_at);
                return;
            }
        }
        self.samples.push_back(sample);
        self.prune(sampled_at);
    }

    pub(crate) fn snapshot(&self) -> DashboardTimelineHistory {
        DashboardTimelineHistory {
            samples: self.samples.iter().cloned().collect(),
        }
    }

    fn prune(&mut self, now_unix_millis: u64) {
        let cutoff = now_unix_millis.saturating_sub(DASHBOARD_TIMELINE_HISTORY_WINDOW_MILLIS);
        while self
            .samples
            .front()
            .is_some_and(|sample| sample.sampled_at_unix_millis < cutoff)
        {
            self.samples.pop_front();
        }
        while self.samples.len() > DASHBOARD_TIMELINE_HISTORY_SAMPLE_LIMIT {
            self.samples.pop_front();
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DashboardSnapshotInputs {
    pub received_at_unix_millis: u64,
    pub metrics: MetricsSnapshot,
    pub symbols: SymbolTelemetryReadModel,
    pub subscriptions: BTreeMap<String, SymbolSubscriptionCounts>,
    pub events: Vec<RelayEvent>,
}

impl DashboardSnapshotInputs {
    #[must_use]
    pub fn symbol_metrics_snapshot(&self, query: &SymbolMetricsQuery) -> SymbolMetricsSnapshot {
        self.symbols.snapshot_at_with_context(
            self.received_at_unix_millis,
            DEFAULT_DATA_STALE_AFTER_SECS.saturating_mul(1_000),
            &self.subscriptions,
            query,
            symbol_metrics_context_for_stage(self.metrics.upstream_stage),
        )
    }

    #[must_use]
    pub fn dashboard_snapshot(&self, query: &SymbolMetricsQuery) -> DashboardSnapshot {
        let global_page = self.symbol_metrics_snapshot(&SymbolMetricsQuery::default());
        let timeline = dashboard_timeline(&global_page.symbols);
        let timeline_symbols = dashboard_symbol_rows(&global_page.symbols);
        let page = self.symbol_metrics_snapshot(query);
        DashboardSnapshot {
            received_at_unix_millis: self.received_at_unix_millis,
            metrics: self.metrics.clone(),
            global: global_page.summary,
            timeline,
            timeline_symbols,
            timeline_history: None,
            page: DashboardSymbolMetricsSnapshot::from_symbol_metrics(page),
            events: self.events.clone(),
        }
    }

    #[must_use]
    pub fn dashboard_snapshot_and_timeline_sample(
        &self,
        query: &SymbolMetricsQuery,
    ) -> (DashboardSnapshot, DashboardTimelineHistorySample) {
        let global_page = self.symbol_metrics_snapshot(&SymbolMetricsQuery::default());
        let timeline_sample =
            dashboard_timeline_history_sample(self.received_at_unix_millis, &global_page.symbols);
        let timeline_symbols = dashboard_symbol_rows(&global_page.symbols);
        let page = self.symbol_metrics_snapshot(query);
        let dashboard = DashboardSnapshot {
            received_at_unix_millis: self.received_at_unix_millis,
            metrics: self.metrics.clone(),
            global: global_page.summary,
            timeline: timeline_sample.sample.clone(),
            timeline_symbols,
            timeline_history: None,
            page: DashboardSymbolMetricsSnapshot::from_symbol_metrics(page),
            events: self.events.clone(),
        };
        (dashboard, timeline_sample)
    }

    #[must_use]
    pub(crate) fn timeline_history_sample(&self) -> DashboardTimelineHistorySample {
        let global_page = self.symbol_metrics_snapshot(&SymbolMetricsQuery::default());
        dashboard_timeline_history_sample(self.received_at_unix_millis, &global_page.symbols)
    }

    #[must_use]
    pub fn into_dashboard_snapshot(self, query: &SymbolMetricsQuery) -> DashboardSnapshot {
        self.dashboard_snapshot(query)
    }

    #[must_use]
    pub fn into_dashboard_snapshot_and_timeline_sample(
        self,
        query: &SymbolMetricsQuery,
    ) -> (DashboardSnapshot, DashboardTimelineHistorySample) {
        self.dashboard_snapshot_and_timeline_sample(query)
    }
}

fn exchange_of(symbol: &str) -> String {
    crate::symbol_identity::exchange_id_for_symbol(symbol)
        .map(str::to_ascii_uppercase)
        .unwrap_or_else(|| "UNKNOWN".to_string())
}

fn dashboard_scope_for<'a>(
    rows: impl IntoIterator<Item = &'a SymbolTelemetrySnapshot>,
) -> DashboardTimelineScope {
    let mut total = 0;
    let mut problem = 0;
    let mut bad = false;
    let mut warn = false;
    let mut auction = false;
    let mut all_closed = true;
    let mut all_no_sample = true;
    let mut max_receive_gap_ms = None::<u64>;
    let mut avg_gap_total = 0_u128;
    let mut avg_gap_count = 0_u128;

    for row in rows {
        total += 1;
        if row.problem {
            problem += 1;
        }
        bad |= row.problem_severity == SymbolProblemSeverity::Bad;
        warn |= row.problem_severity == SymbolProblemSeverity::Warn;
        auction |= row.phase.is_auction();
        all_closed &= row.session == SymbolSession::Closed;
        all_no_sample &= row.flow == SymbolFlow::NoSample;
        if let Some(gap) = row.receive_gap_ms {
            max_receive_gap_ms = Some(max_receive_gap_ms.map_or(gap, |current| current.max(gap)));
        }
        if let Some(gap) = row.avg_receive_gap_ms {
            avg_gap_total = avg_gap_total.saturating_add(u128::from(gap));
            avg_gap_count = avg_gap_count.saturating_add(1);
        }
    }

    let severity = if total == 0 {
        DashboardTimelineSeverity::Unknown
    } else if all_closed {
        DashboardTimelineSeverity::Closed
    } else if bad {
        DashboardTimelineSeverity::Bad
    } else if warn {
        DashboardTimelineSeverity::Warn
    } else if auction {
        DashboardTimelineSeverity::Auction
    } else if all_no_sample {
        DashboardTimelineSeverity::NoSample
    } else {
        DashboardTimelineSeverity::Live
    };

    DashboardTimelineScope {
        severity,
        total,
        problem,
        receive_gap_ms: max_receive_gap_ms,
        avg_receive_gap_ms: (avg_gap_count > 0).then(|| {
            (avg_gap_total / avg_gap_count)
                .try_into()
                .unwrap_or(u64::MAX)
        }),
    }
}

fn dashboard_timeline(rows: &[SymbolTelemetrySnapshot]) -> DashboardTimelineSample {
    let mut exchanges = BTreeMap::new();
    for row in rows {
        exchanges
            .entry(exchange_of(&row.symbol))
            .or_insert_with(Vec::new)
            .push(row);
    }

    DashboardTimelineSample {
        global: dashboard_scope_for(rows.iter()),
        subscribed: dashboard_scope_for(rows.iter().filter(|row| row.subscribed)),
        exchanges: exchanges
            .into_iter()
            .map(|(exchange, rows)| (exchange, dashboard_scope_for(rows)))
            .collect(),
    }
}

pub(crate) fn symbol_metrics_context_for_stage(stage: RelaySourceStage) -> SymbolMetricsContext {
    SymbolMetricsContext {
        initializing_universe: matches!(
            stage,
            RelaySourceStage::Subscribing | RelaySourceStage::Backfilling
        ),
        initializing_pending_samples: true,
    }
}

fn dashboard_timeline_history_sample(
    sampled_at_unix_millis: u64,
    rows: &[SymbolTelemetrySnapshot],
) -> DashboardTimelineHistorySample {
    DashboardTimelineHistorySample {
        sampled_at_unix_millis,
        sample: dashboard_timeline(rows),
        symbols: rows
            .iter()
            .map(|row| {
                (
                    row.symbol.clone(),
                    DashboardTimelineSymbolSample {
                        severity: dashboard_symbol_severity(row),
                        receive_gap_ms: row.receive_gap_ms,
                        avg_receive_gap_ms: row.avg_receive_gap_ms,
                    },
                )
            })
            .collect(),
    }
}

fn dashboard_symbol_severity(row: &SymbolTelemetrySnapshot) -> DashboardTimelineSeverity {
    if row.session == SymbolSession::Closed {
        DashboardTimelineSeverity::Closed
    } else if row.problem_severity == SymbolProblemSeverity::Initializing {
        DashboardTimelineSeverity::NoSample
    } else if row.problem_severity == SymbolProblemSeverity::Bad
        || row.integrity == SymbolIntegrity::ConfirmedGap
    {
        DashboardTimelineSeverity::Bad
    } else if row.problem_severity == SymbolProblemSeverity::Warn
        || row.integrity == SymbolIntegrity::Suspected
        || row.flow == SymbolFlow::Silent
    {
        DashboardTimelineSeverity::Warn
    } else if row.phase.is_auction() {
        DashboardTimelineSeverity::Auction
    } else if row.flow == SymbolFlow::NoSample {
        DashboardTimelineSeverity::NoSample
    } else if row.session == SymbolSession::Unknown {
        DashboardTimelineSeverity::Unknown
    } else {
        DashboardTimelineSeverity::Live
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timeline_history_sample(sampled_at_unix_millis: u64) -> DashboardTimelineHistorySample {
        let scope = DashboardTimelineScope {
            severity: DashboardTimelineSeverity::Live,
            total: 1,
            problem: 0,
            receive_gap_ms: Some(0),
            avg_receive_gap_ms: Some(0),
        };
        DashboardTimelineHistorySample {
            sampled_at_unix_millis,
            sample: DashboardTimelineSample {
                global: scope.clone(),
                subscribed: scope,
                exchanges: BTreeMap::new(),
            },
            symbols: BTreeMap::new(),
        }
    }

    #[test]
    fn timeline_history_keeps_samples_at_min_interval_boundary() {
        let mut history = DashboardTimelineHistoryCache::default();

        history.push(timeline_history_sample(10_000));
        history.push(timeline_history_sample(11_999));
        let samples = history.snapshot().samples;
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].sampled_at_unix_millis, 11_999);

        history.push(timeline_history_sample(13_999));
        let samples = history.snapshot().samples;
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[1].sampled_at_unix_millis, 13_999);
    }
}
