use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::{self, IsTerminal, Write};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{NaiveDate, SecondsFormat, Utc};
use clap::ValueEnum;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use serde_json::{Value, json};
use tqsdk::{
    BacktestRemoteFillPhase, BacktestRemoteFillProgress, BacktestRemoteFillTelemetry,
    RemoteFillPlan,
};
use tqsdk_cache::{FillReport, FillReportSymbolDayStats, MinuteFillReport};
use tqsdk_data::{
    BacktestHistoryFillProgress, BacktestHistoryPhase, backtest_tick_trading_day_for_timestamp_ns,
    backtest_tick_trading_day_range,
};

const RENDER_INTERVAL: Duration = Duration::from_millis(100);
const PLAIN_RENDER_INTERVAL: Duration = Duration::from_secs(1);
const TTY_RENDER_HZ: u8 = 1;
const TTY_SPINNER_INTERVAL: Duration = Duration::from_secs(1);
const RECENT_RATE_WINDOW: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum ProgressMode {
    Auto,
    Tty,
    Plain,
    Jsonl,
    Off,
}

#[derive(Debug, Clone)]
pub(crate) struct ProgressCalendar {
    pub source: String,
    pub days: Vec<NaiveDate>,
}

#[derive(Clone)]
pub(crate) struct FillProgress {
    shared: Option<Arc<Mutex<ProgressState>>>,
    jsonl_tx: Option<mpsc::Sender<JsonlEvent>>,
}

pub(crate) struct FillProgressSession {
    progress: FillProgress,
    renderer: Option<thread::JoinHandle<()>>,
    jsonl_writer: Option<JsonlWriter>,
}

enum JsonlEvent {
    Record(Value),
    Finish,
}

struct JsonlWriter {
    tx: mpsc::Sender<JsonlEvent>,
    worker: thread::JoinHandle<()>,
}

impl FillProgressSession {
    pub(crate) fn new(mode: ProgressMode, max_bars: usize, cache_kind: &'static str) -> Self {
        let mode = resolve_mode(mode);
        if matches!(mode, ResolvedProgressMode::Off) {
            return Self {
                progress: FillProgress {
                    shared: None,
                    jsonl_tx: None,
                },
                renderer: None,
                jsonl_writer: None,
            };
        }

        let shared = Arc::new(Mutex::new(ProgressState::new_with_cache_kind(
            mode, max_bars, cache_kind,
        )));
        let renderer = match mode {
            ResolvedProgressMode::Tty | ResolvedProgressMode::Plain => {
                let renderer_state = Arc::clone(&shared);
                Some(thread::spawn(move || render_loop(renderer_state)))
            }
            ResolvedProgressMode::Jsonl | ResolvedProgressMode::Off => None,
        };
        let jsonl_writer = matches!(mode, ResolvedProgressMode::Jsonl).then(|| {
            let (tx, rx) = mpsc::channel();
            let worker = thread::spawn(move || render_jsonl(rx));
            JsonlWriter { tx, worker }
        });
        Self {
            progress: FillProgress {
                shared: Some(shared),
                jsonl_tx: jsonl_writer.as_ref().map(|writer| writer.tx.clone()),
            },
            renderer,
            jsonl_writer,
        }
    }

    pub(crate) fn observer(&self) -> FillProgress {
        self.progress.clone()
    }

    pub(crate) fn finish(mut self, status: ProgressTerminalStatus, summary: impl Into<String>) {
        self.progress.finish(status, summary);
        if let Some(renderer) = self.renderer.take() {
            let _ = renderer.join();
        }
        if let Some(writer) = self.jsonl_writer.take() {
            writer.finish();
        }
    }
}

impl Drop for FillProgressSession {
    fn drop(&mut self) {
        if !self.progress.is_finished() {
            self.progress.finish(
                ProgressTerminalStatus::Failed,
                "fill failed before operation completed",
            );
        }
        if let Some(renderer) = self.renderer.take() {
            let _ = renderer.join();
        }
        if let Some(writer) = self.jsonl_writer.take() {
            writer.finish();
        }
    }
}

impl JsonlWriter {
    fn finish(self) {
        let _ = self.tx.send(JsonlEvent::Finish);
        let _ = self.worker.join();
    }
}

impl FillProgress {
    pub(crate) fn planning(&self, message: impl Into<String>) {
        self.with_state(|state| state.planning = message.into());
    }

    pub(crate) fn calendar_ready(&self, calendar: ProgressCalendar) {
        self.with_state(|state| {
            state.calendar = Some(calendar);
            state.recalculate_days();
        });
    }

    pub(crate) fn calendar_unavailable(&self, message: impl Into<String>) {
        self.with_state(|state| state.calendar_error = Some(message.into()));
    }

    pub(crate) fn set_scope(&self, symbols: &[String], requested_range: (i64, i64)) {
        self.with_state(|state| state.set_scope(symbols, requested_range));
    }

    pub(crate) fn observe_progress(&self, event: &BacktestRemoteFillProgress) {
        self.with_state(|state| state.apply_progress(event));
    }

    pub(crate) fn observe_history_progress(&self, event: &BacktestHistoryFillProgress) {
        self.with_state(|state| state.apply_history_progress(event));
    }

    pub(crate) fn observe_telemetry(&self, event: &BacktestRemoteFillTelemetry) {
        self.with_state(|state| state.apply_telemetry(event));
    }

    pub(crate) fn final_report(&self, report: &FillReport) {
        self.with_state(|state| state.apply_final_report(report));
    }

    pub(crate) fn final_minute_report(&self, report: &MinuteFillReport) {
        self.with_state(|state| state.apply_final_minute_report(report));
    }

    fn finish(&self, status: ProgressTerminalStatus, summary: impl Into<String>) {
        let summary = summary.into();
        self.with_state(|state| {
            state.finished = Some(ProgressCompletion { status, summary });
        });
    }

    fn is_finished(&self) -> bool {
        self.shared
            .as_ref()
            .and_then(|shared| shared.lock().ok().map(|state| state.finished.is_some()))
            .unwrap_or(true)
    }

    fn with_state(&self, update: impl FnOnce(&mut ProgressState)) {
        let Some(shared) = &self.shared else {
            return;
        };
        let Ok(mut state) = shared.lock() else {
            return;
        };
        if state.finished.is_some() {
            return;
        }
        update(&mut state);
        state.revision = state.revision.saturating_add(1);
        if matches!(state.mode, ResolvedProgressMode::Jsonl) {
            if let Some(tx) = &self.jsonl_tx {
                let _ = tx.send(JsonlEvent::Record(state.jsonl_record()));
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolvedProgressMode {
    Tty,
    Plain,
    Jsonl,
    Off,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProgressTerminalStatus {
    Complete,
    Failed,
    Interrupted,
}

impl ProgressTerminalStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
        }
    }
}

fn resolve_mode(mode: ProgressMode) -> ResolvedProgressMode {
    match mode {
        ProgressMode::Off => ResolvedProgressMode::Off,
        ProgressMode::Tty => ResolvedProgressMode::Tty,
        ProgressMode::Plain => ResolvedProgressMode::Plain,
        ProgressMode::Jsonl => ResolvedProgressMode::Jsonl,
        ProgressMode::Auto
            if io::stderr().is_terminal()
                && std::env::var("TERM").ok().as_deref() != Some("dumb") =>
        {
            ResolvedProgressMode::Tty
        }
        ProgressMode::Auto => ResolvedProgressMode::Plain,
    }
}

#[derive(Clone)]
struct ProgressState {
    mode: ResolvedProgressMode,
    max_bars: usize,
    cache_kind: &'static str,
    planning: String,
    calendar: Option<ProgressCalendar>,
    calendar_error: Option<String>,
    plan: Option<RemoteFillPlan>,
    inspection: Option<InspectionProgress>,
    symbols: BTreeMap<String, SymbolProgress>,
    completed_batches: BTreeSet<usize>,
    history_rows_by_batch: BTreeMap<usize, usize>,
    final_rows: Option<usize>,
    total_batches: usize,
    history_fill: bool,
    history_batch_started: bool,
    failed: bool,
    revision: u64,
    finished: Option<ProgressCompletion>,
    started_at: Instant,
}

#[derive(Clone)]
struct ProgressCompletion {
    status: ProgressTerminalStatus,
    summary: String,
}

#[derive(Clone)]
struct InspectionProgress {
    total_ranges: usize,
    checked_ranges: usize,
    complete_ranges: usize,
    incomplete_ranges: usize,
    physical_symbol: String,
    requested_range: (i64, i64),
}

#[derive(Clone, Default)]
struct SymbolProgress {
    requested_ranges: Vec<(i64, i64)>,
    missing_ranges: Vec<(i64, i64)>,
    planned_days: BTreeSet<NaiveDate>,
    missing_days: BTreeSet<NaiveDate>,
    covered_days: BTreeSet<NaiveDate>,
    received_days: BTreeSet<NaiveDate>,
    history_day_counts: Option<HistoryDayCounts>,
    history_received_ranges: Vec<(i64, i64)>,
    history_streamed_ranges: BTreeMap<(usize, i64, i64), i64>,
    rows_by_stream: BTreeMap<(usize, usize, i64, i64), usize>,
    final_rows: Option<usize>,
    active_batches: BTreeSet<usize>,
    active: bool,
    phase: Option<BacktestRemoteFillPhase>,
    error: Option<String>,
    last_event_sequence: u64,
    latest_trading_day: Option<NaiveDate>,
    retries: usize,
    split_fallback: bool,
}

#[derive(Clone, Copy, Default)]
struct HistoryDayCounts {
    covered: usize,
    planned: usize,
    missing: usize,
    received: usize,
}

impl SymbolProgress {
    fn day_counts(&self, history_fill: bool) -> (usize, usize, usize, usize) {
        if history_fill {
            let counts = self.history_day_counts.unwrap_or_default();
            (
                counts.covered,
                counts.planned,
                counts.received,
                counts.missing,
            )
        } else {
            (
                self.covered_days.len(),
                self.planned_days.len(),
                self.received_days.len(),
                self.missing_days.len(),
            )
        }
    }
}

impl ProgressState {
    #[cfg(test)]
    fn new(mode: ResolvedProgressMode, max_bars: usize) -> Self {
        Self::new_with_cache_kind(mode, max_bars, "tick")
    }

    fn new_with_cache_kind(
        mode: ResolvedProgressMode,
        max_bars: usize,
        cache_kind: &'static str,
    ) -> Self {
        Self {
            mode,
            max_bars,
            cache_kind,
            planning: "planning cache fill".to_string(),
            calendar: None,
            calendar_error: None,
            plan: None,
            inspection: None,
            symbols: BTreeMap::new(),
            completed_batches: BTreeSet::new(),
            history_rows_by_batch: BTreeMap::new(),
            final_rows: None,
            total_batches: 0,
            history_fill: false,
            history_batch_started: false,
            failed: false,
            revision: 0,
            finished: None,
            started_at: Instant::now(),
        }
    }

    fn set_scope(&mut self, symbols: &[String], requested_range: (i64, i64)) {
        if self.history_fill {
            return;
        }
        for symbol in symbols {
            let entry = self.symbols.entry(symbol.clone()).or_default();
            if entry.requested_ranges.is_empty() {
                entry.requested_ranges.push(requested_range);
            }
            if entry.missing_ranges.is_empty() {
                entry.missing_ranges.push(requested_range);
            }
        }
        self.recalculate_days();
    }

    fn apply_plan_symbol_ranges(
        &mut self,
        physical_symbol: &str,
        requested_ranges: &[(i64, i64)],
        missing_ranges: &[(i64, i64)],
    ) {
        let entry = self.symbols.entry(physical_symbol.to_string()).or_default();
        if entry.requested_ranges.is_empty() {
            entry.requested_ranges = requested_ranges.to_vec();
        }
        entry.missing_ranges = missing_ranges.to_vec();
    }

    fn apply_progress(&mut self, event: &BacktestRemoteFillProgress) {
        match event {
            BacktestRemoteFillProgress::FillStarted { total_batches, .. } => {
                self.total_batches = *total_batches;
            }
            BacktestRemoteFillProgress::BatchStarted {
                batch_number,
                requested_range,
                symbols,
                ..
            } => {
                for symbol in symbols {
                    let entry = self.symbols.entry(symbol.clone()).or_default();
                    entry.active_batches.insert(*batch_number);
                    entry.active = true;
                    entry.phase = Some(BacktestRemoteFillPhase::Started);
                    entry.last_event_sequence = self.revision;
                    entry
                        .rows_by_stream
                        .entry((*batch_number, 0, requested_range.0, requested_range.1))
                        .or_default();
                }
            }
            BacktestRemoteFillProgress::BatchFinished {
                batch_number,
                symbols,
                ..
            } => {
                self.completed_batches.insert(*batch_number);
                for symbol in symbols {
                    if let Some(entry) = self.symbols.get_mut(symbol) {
                        entry.active_batches.remove(batch_number);
                        entry.active = !entry.active_batches.is_empty();
                        if !entry.active {
                            entry.phase = Some(BacktestRemoteFillPhase::Finished);
                        }
                        entry.last_event_sequence = self.revision;
                    }
                }
            }
            BacktestRemoteFillProgress::BatchFailed {
                batch_number,
                symbols,
                error,
                ..
            } => {
                self.failed = true;
                for symbol in symbols {
                    let entry = self.symbols.entry(symbol.clone()).or_default();
                    entry.active_batches.remove(batch_number);
                    entry.active = !entry.active_batches.is_empty();
                    if !entry.active {
                        entry.phase = Some(BacktestRemoteFillPhase::Failed);
                    }
                    entry.error = Some(error.clone());
                    entry.last_event_sequence = self.revision;
                }
            }
            BacktestRemoteFillProgress::TickObserved { .. } => {}
        }
    }

    fn apply_history_progress(&mut self, event: &BacktestHistoryFillProgress) {
        self.history_fill = true;
        match event {
            BacktestHistoryFillProgress::Planning { total_batches, .. } => {
                self.total_batches = *total_batches;
            }
            BacktestHistoryFillProgress::BatchStarted {
                batch_number,
                total_batches,
                requested_range,
                symbols,
                ..
            } => {
                self.history_batch_started = true;
                self.total_batches = *total_batches;
                for symbol in symbols {
                    {
                        let entry = self.symbols.entry(symbol.clone()).or_default();
                        if !entry.requested_ranges.contains(requested_range) {
                            entry.requested_ranges.push(*requested_range);
                        }
                        if !entry.missing_ranges.contains(requested_range) {
                            entry.missing_ranges.push(*requested_range);
                        }
                        entry.active_batches.insert(*batch_number);
                        entry.active = true;
                        entry.phase = Some(BacktestRemoteFillPhase::Started);
                        entry.last_event_sequence = self.revision;
                        entry
                            .rows_by_stream
                            .entry((*batch_number, 0, requested_range.0, requested_range.1))
                            .or_default();
                    }
                    self.recalculate_history_symbol_days(symbol);
                }
            }
            BacktestHistoryFillProgress::Telemetry {
                batch_number,
                requested_range,
                event,
                ..
            } => {
                self.history_batch_started = true;
                let entry = self.symbols.entry(event.symbol.clone()).or_default();
                entry.active_batches.insert(*batch_number);
                entry.active = true;
                entry.phase = Some(history_phase(event.phase));
                entry.last_event_sequence = self.revision;
                entry
                    .rows_by_stream
                    .entry((*batch_number, 0, requested_range.0, requested_range.1))
                    .and_modify(|rows| *rows = (*rows).max(event.completed_rows))
                    .or_insert(event.completed_rows);
                if let Some(cursor_ns) = event.latest_cursor_ns
                    && let Ok(cursor_day) = backtest_tick_trading_day_for_timestamp_ns(cursor_ns)
                {
                    entry.latest_trading_day = Some(
                        entry
                            .latest_trading_day
                            .map_or(cursor_day, |latest| latest.max(cursor_day)),
                    );
                    if let Some((_, completed_end_ns)) =
                        completed_history_range_through_cursor(*requested_range, cursor_ns)
                    {
                        entry
                            .history_streamed_ranges
                            .entry((*batch_number, requested_range.0, requested_range.1))
                            .and_modify(|end_ns| *end_ns = (*end_ns).max(completed_end_ns))
                            .or_insert(completed_end_ns);
                    }
                }
                if matches!(event.phase, BacktestHistoryPhase::Retry) {
                    entry.retries = entry.retries.saturating_add(1);
                }
                self.recalculate_history_symbol_days(&event.symbol);
            }
            BacktestHistoryFillProgress::BatchFinished {
                batch_number,
                requested_range,
                symbols,
                rows_written,
                ..
            } => {
                self.history_batch_started = true;
                let streamed_rows = symbols
                    .iter()
                    .filter_map(|symbol| self.symbols.get(symbol))
                    .flat_map(|entry| entry.rows_by_stream.iter())
                    .filter(|((stream_batch_number, ..), _)| stream_batch_number == batch_number)
                    .map(|(_, rows)| *rows)
                    .sum::<usize>();
                let completed_rows = (*rows_written).max(streamed_rows);
                self.completed_batches.insert(*batch_number);
                self.history_rows_by_batch
                    .entry(*batch_number)
                    .and_modify(|rows| *rows = (*rows).max(completed_rows))
                    .or_insert(completed_rows);
                for symbol in symbols {
                    if let Some(entry) = self.symbols.get_mut(symbol) {
                        entry.active_batches.remove(batch_number);
                        entry.history_streamed_ranges.remove(&(
                            *batch_number,
                            requested_range.0,
                            requested_range.1,
                        ));
                        entry.active = !entry.active_batches.is_empty();
                        if !entry.active {
                            entry.phase = Some(BacktestRemoteFillPhase::Finished);
                        }
                        entry
                            .received_days
                            .extend(entry.missing_days.iter().copied());
                        if !entry.history_received_ranges.contains(requested_range) {
                            entry.history_received_ranges.push(*requested_range);
                        }
                    }
                    self.recalculate_history_symbol_days(symbol);
                }
                if let [symbol] = symbols.as_slice()
                    && let Some(entry) = self.symbols.get_mut(symbol)
                {
                    entry
                        .rows_by_stream
                        .entry((*batch_number, 0, requested_range.0, requested_range.1))
                        .and_modify(|rows| *rows = (*rows).max(completed_rows))
                        .or_insert(completed_rows);
                }
            }
            BacktestHistoryFillProgress::BatchFailed {
                batch_number,
                requested_range,
                symbols,
                error,
                ..
            } => {
                self.history_batch_started = true;
                self.failed = true;
                for symbol in symbols {
                    if let Some(entry) = self.symbols.get_mut(symbol) {
                        entry.active_batches.remove(batch_number);
                        entry.history_streamed_ranges.remove(&(
                            *batch_number,
                            requested_range.0,
                            requested_range.1,
                        ));
                        entry.active = !entry.active_batches.is_empty();
                        if !entry.active {
                            entry.phase = Some(BacktestRemoteFillPhase::Failed);
                        }
                        entry.error = Some(error.clone());
                        entry.last_event_sequence = self.revision;
                    }
                    self.recalculate_history_symbol_days(symbol);
                }
            }
            BacktestHistoryFillProgress::Finished { .. } => {}
        }
    }

    fn apply_telemetry(&mut self, event: &BacktestRemoteFillTelemetry) {
        if let Some(inspection) = event.inspection_progress() {
            let Some(physical_symbol) = event.physical_symbol() else {
                return;
            };
            let Some(requested_range) = event.requested_range() else {
                return;
            };
            self.inspection = Some(InspectionProgress {
                total_ranges: inspection.total_ranges(),
                checked_ranges: inspection.checked_ranges(),
                complete_ranges: inspection.complete_ranges(),
                incomplete_ranges: inspection.incomplete_ranges(),
                physical_symbol: physical_symbol.to_string(),
                requested_range,
            });
            return;
        }

        if let Some(plan) = event.plan() {
            self.inspection = None;
            self.plan = Some(plan.clone());
            self.total_batches = plan.logical_batches();
            for plan_symbol in plan.physical_symbols() {
                self.apply_plan_symbol_ranges(
                    plan_symbol.physical_symbol(),
                    plan_symbol.requested_ranges(),
                    plan_symbol.missing_ranges(),
                );
            }
            self.recalculate_days();
            return;
        }

        let Some(symbol) = event.physical_symbol() else {
            return;
        };
        let Some(range) = event.requested_range() else {
            return;
        };
        let event_sequence = self.revision;
        let entry = self.symbols.entry(symbol.to_string()).or_default();
        entry.phase = Some(event.phase());
        entry.last_event_sequence = event_sequence;
        let terminal = matches!(
            event.phase(),
            BacktestRemoteFillPhase::Finished
                | BacktestRemoteFillPhase::Failed
                | BacktestRemoteFillPhase::Cancelled
        );
        if let Some(batch_id) = event.logical_batch_id() {
            if terminal {
                entry.active_batches.remove(&batch_id);
            } else {
                entry.active_batches.insert(batch_id);
            }
        }
        entry.active = !entry.active_batches.is_empty() || !terminal;
        if matches!(event.phase(), BacktestRemoteFillPhase::Retrying) {
            entry.retries = entry.retries.saturating_add(1);
        }
        if matches!(event.phase(), BacktestRemoteFillPhase::SplitFallback) {
            entry.split_fallback = true;
        }
        if matches!(event.phase(), BacktestRemoteFillPhase::Failed) {
            self.failed = true;
        }
        if let Some(error) = event.error() {
            entry.error = Some(error.to_string());
        }
        if let Some(batch_id) = event.logical_batch_id() {
            entry.rows_by_stream.insert(
                (batch_id, event.attempt(), range.0, range.1),
                event.accepted_rows(),
            );
        }
        if let Some(cursor_ns) = event.latest_cursor_ns() {
            if let Ok(cursor_day) = backtest_tick_trading_day_for_timestamp_ns(cursor_ns) {
                entry.latest_trading_day = Some(cursor_day);
            }
            entry.received_days.extend(completed_days_through_cursor(
                &entry.missing_days,
                cursor_ns,
            ));
        }
        if matches!(event.phase(), BacktestRemoteFillPhase::Finished) {
            let completed_days = days_for_ranges(&[range], self.calendar.as_ref());
            entry
                .covered_days
                .extend(entry.missing_days.intersection(&completed_days).copied());
            entry
                .received_days
                .extend(entry.missing_days.intersection(&completed_days).copied());
        }
    }

    fn apply_final_report(&mut self, report: &FillReport) {
        for symbol in &report.physical_symbols {
            let Some(stats) = &symbol.day_stats else {
                continue;
            };
            self.apply_day_stats(stats);
        }
    }

    fn apply_final_minute_report(&mut self, report: &MinuteFillReport) {
        self.final_rows = Some(report.rows_written);
        for symbol in &report.symbols {
            let entry = self.symbols.entry(symbol.symbol.clone()).or_default();
            entry.final_rows = Some(symbol.rows_written);
            entry.active_batches.clear();
            entry.active = false;
            entry.phase = Some(if symbol.after.complete {
                BacktestRemoteFillPhase::Finished
            } else {
                BacktestRemoteFillPhase::Failed
            });
        }
    }

    fn apply_day_stats(&mut self, stats: &FillReportSymbolDayStats) {
        let Some(symbol) = self.symbols.get_mut(&stats.symbol) else {
            return;
        };
        if symbol.planned_days.len() == stats.planned_days
            && stats.covered_days == stats.planned_days
        {
            symbol.covered_days = symbol.planned_days.clone();
        }
        if symbol.missing_days.len() == stats.missing_days
            && stats.received_days == stats.missing_days
        {
            symbol.received_days = symbol.missing_days.clone();
        }
    }

    fn recalculate_days(&mut self) {
        if self.history_fill {
            let symbols = self.symbols.keys().cloned().collect::<Vec<_>>();
            for symbol in symbols {
                self.recalculate_history_symbol_days(&symbol);
            }
            return;
        }
        for symbol in self.symbols.values_mut() {
            symbol.planned_days = days_for_ranges(&symbol.requested_ranges, self.calendar.as_ref());
            symbol.missing_days = days_for_ranges(&symbol.missing_ranges, self.calendar.as_ref());
            symbol.covered_days = symbol
                .planned_days
                .difference(&symbol.missing_days)
                .copied()
                .collect();
            symbol
                .received_days
                .retain(|day| symbol.missing_days.contains(day));
        }
    }

    fn recalculate_history_symbol_days(&mut self, symbol: &str) {
        let calendar = self.calendar.as_ref();
        let Some(symbol) = self.symbols.get_mut(symbol) else {
            return;
        };
        let mut received_ranges = symbol.history_received_ranges.clone();
        received_ranges.extend(
            symbol
                .history_streamed_ranges
                .iter()
                .map(|((_, start_ns, _), end_ns)| (*start_ns, *end_ns)),
        );
        symbol.history_day_counts = Some(HistoryDayCounts {
            covered: day_count_for_ranges(&symbol.history_received_ranges, calendar),
            planned: day_count_for_ranges(&symbol.requested_ranges, calendar),
            missing: day_count_for_ranges(&symbol.missing_ranges, calendar),
            received: day_count_for_ranges(&received_ranges, calendar),
        });
    }

    fn visible_symbols(&self) -> Vec<String> {
        let mut active = self
            .symbols
            .iter()
            .filter(|(_, state)| state.active)
            .collect::<Vec<_>>();
        active.sort_by(|(left_symbol, left), (right_symbol, right)| {
            right
                .last_event_sequence
                .cmp(&left.last_event_sequence)
                .then_with(|| left_symbol.cmp(right_symbol))
        });

        let mut terminal = self
            .symbols
            .iter()
            .filter(|(_, state)| {
                !state.active
                    && matches!(
                        state.phase,
                        Some(
                            BacktestRemoteFillPhase::Finished
                                | BacktestRemoteFillPhase::Failed
                                | BacktestRemoteFillPhase::Cancelled
                        )
                    )
            })
            .collect::<Vec<_>>();
        terminal.sort_by(|(left_symbol, left), (right_symbol, right)| {
            right
                .last_event_sequence
                .cmp(&left.last_event_sequence)
                .then_with(|| left_symbol.cmp(right_symbol))
        });

        active
            .into_iter()
            .chain(terminal)
            .take(self.max_bars)
            .map(|(symbol, _)| symbol.clone())
            .collect()
    }

    fn coverage_counts(&self) -> (usize, usize, usize, usize, usize) {
        let mut covered = 0;
        let mut planned = 0;
        let mut received = 0;
        let mut missing = 0;
        let mut rows = self.history_rows_by_batch.values().copied().sum::<usize>();
        for symbol in self.symbols.values() {
            let (symbol_covered, symbol_planned, symbol_received, symbol_missing) =
                symbol.day_counts(self.history_fill);
            covered += symbol_covered;
            planned += symbol_planned;
            received += symbol_received;
            missing += symbol_missing;
            if self.history_fill {
                rows += symbol
                    .rows_by_stream
                    .iter()
                    .filter(|((batch_number, ..), _)| {
                        !self.completed_batches.contains(batch_number)
                    })
                    .map(|(_, rows)| *rows)
                    .sum::<usize>();
            } else {
                rows += symbol.rows_by_stream.values().copied().sum::<usize>();
            }
        }
        (covered, planned, received, missing, rows)
    }

    fn display_rows(&self, rows: usize) -> Option<usize> {
        self.final_rows
            .or_else(|| (self.cache_kind != "minute").then_some(rows))
    }

    fn display_symbol_rows(&self, symbol: &SymbolProgress) -> Option<usize> {
        symbol.final_rows.or_else(|| {
            (self.cache_kind != "minute")
                .then(|| symbol.rows_by_stream.values().copied().sum::<usize>())
        })
    }

    fn jsonl_record(&self) -> Value {
        let (covered, planned, received, missing, rows) = self.coverage_counts();
        let display_rows = self.display_rows(rows);
        let (event, status, summary) = match &self.finished {
            Some(completion) => (
                "complete",
                completion.status.as_str(),
                Some(completion.summary.as_str()),
            ),
            None if self.inspection.is_some() => ("inspection", "running", None),
            None if self.plan.is_some() => (
                "snapshot",
                if self.failed { "failed" } else { "running" },
                None,
            ),
            None if self.history_fill && self.history_batch_started => (
                "batch",
                if self.failed { "failed" } else { "running" },
                None,
            ),
            None => (
                "planning",
                if self.failed { "failed" } else { "running" },
                None,
            ),
        };
        json!({
            "schema_version": 2,
            "kind": "tqsdk-cache.progress",
            "cache_kind": self.cache_kind,
            "event": event,
            "sequence": self.revision,
            "emitted_at": Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            "elapsed_ms": elapsed_millis(self.started_at),
            "status": status,
            "batch": {
                "completed": self.completed_batches.len(),
                "total": self.total_batches,
            },
            "coverage": {
                "covered_days": covered,
                "planned_days": planned,
                "received_days": received,
                "missing_days": missing,
                "rows": display_rows,
                "rows_known": display_rows.is_some(),
            },
            "calendar": {
                "source": self
                    .calendar
                    .as_ref()
                    .map(|calendar| calendar.source.as_str())
                    .unwrap_or("partition_fallback"),
                "trading_days": self.calendar.as_ref().map_or(0, |calendar| calendar.days.len()),
                "error": &self.calendar_error,
            },
            "inspection": self.inspection.as_ref().map(|inspection| json!({
                "total_ranges": inspection.total_ranges,
                "checked_ranges": inspection.checked_ranges,
                "complete_ranges": inspection.complete_ranges,
                "incomplete_ranges": inspection.incomplete_ranges,
                "physical_symbol": inspection.physical_symbol,
                "requested_range": {
                    "start_ns": inspection.requested_range.0,
                    "end_ns": inspection.requested_range.1,
                },
            })),
            "symbols": self
                .symbols
                .iter()
                .filter(|(_, state)| state.active || state.error.is_some())
            .map(|(symbol, state)| {
                let (covered, planned, received, missing) = state.day_counts(self.history_fill);
                json!({
                    "symbol": symbol,
                    "phase": state.phase.map(phase_name).unwrap_or("pending"),
                    "trading_day": state.latest_trading_day.map(|day| day.to_string()),
                    "coverage_days": {
                    "covered": covered,
                    "planned": planned,
                    "received": received,
                    "missing": missing,
                    },
                    "rows": self.display_symbol_rows(state),
                    "attempt_retries": state.retries,
                    "split_fallback": state.split_fallback,
                    "error": state.error,
                })
            })
                .collect::<Vec<_>>(),
            "summary": summary,
        })
    }
}

fn render_jsonl(rx: mpsc::Receiver<JsonlEvent>) {
    while let Ok(event) = rx.recv() {
        match event {
            JsonlEvent::Record(record) => write_jsonl_progress(&record),
            JsonlEvent::Finish => return,
        }
    }
}

fn write_jsonl_progress(record: &Value) {
    let stderr = io::stderr();
    let mut stderr = stderr.lock();
    if serde_json::to_writer(&mut stderr, record).is_ok() {
        let _ = stderr.write_all(b"\n");
        let _ = stderr.flush();
    }
}

fn elapsed_millis(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn days_for_ranges(
    ranges: &[(i64, i64)],
    calendar: Option<&ProgressCalendar>,
) -> BTreeSet<NaiveDate> {
    let calendar_days =
        calendar.map(|calendar| calendar.days.iter().copied().collect::<BTreeSet<_>>());
    let mut days = BTreeSet::new();
    for (start_ns, end_ns) in ranges {
        if start_ns >= end_ns {
            continue;
        }
        let Ok(mut day) = backtest_tick_trading_day_for_timestamp_ns(*start_ns) else {
            continue;
        };
        let Ok(last_day) = backtest_tick_trading_day_for_timestamp_ns(end_ns.saturating_sub(1))
        else {
            continue;
        };
        while day <= last_day {
            if calendar_days
                .as_ref()
                .is_none_or(|days| days.contains(&day))
            {
                days.insert(day);
            }
            let Some(next) = day.succ_opt() else {
                break;
            };
            day = next;
        }
    }
    days
}

fn day_count_for_ranges(ranges: &[(i64, i64)], calendar: Option<&ProgressCalendar>) -> usize {
    let ranges = normalized_trading_day_ranges(ranges);
    if let Some(calendar) = calendar {
        return calendar
            .days
            .iter()
            .filter(|day| {
                ranges
                    .iter()
                    .any(|(start_day, end_day)| start_day <= *day && *day <= end_day)
            })
            .count();
    }

    ranges.iter().fold(0, |count, (start_day, end_day)| {
        let range_days = usize::try_from(
            end_day
                .signed_duration_since(*start_day)
                .num_days()
                .saturating_add(1),
        )
        .unwrap_or(usize::MAX);
        count.saturating_add(range_days)
    })
}

fn completed_history_range_through_cursor(
    requested_range: (i64, i64),
    cursor_ns: i64,
) -> Option<(i64, i64)> {
    let cursor_day = backtest_tick_trading_day_for_timestamp_ns(cursor_ns).ok()?;
    let completed_end_ns = backtest_tick_trading_day_range(cursor_day)
        .ok()?
        .start_ns
        .min(requested_range.1);
    (completed_end_ns > requested_range.0).then_some((requested_range.0, completed_end_ns))
}

fn normalized_trading_day_ranges(ranges: &[(i64, i64)]) -> Vec<(NaiveDate, NaiveDate)> {
    let mut days = ranges
        .iter()
        .filter_map(|(start_ns, end_ns)| {
            if start_ns >= end_ns {
                return None;
            }
            let start_day = backtest_tick_trading_day_for_timestamp_ns(*start_ns).ok()?;
            let end_day =
                backtest_tick_trading_day_for_timestamp_ns(end_ns.saturating_sub(1)).ok()?;
            Some((start_day, end_day))
        })
        .collect::<Vec<_>>();
    days.sort_unstable();

    let mut merged = Vec::<(NaiveDate, NaiveDate)>::new();
    for (start_day, end_day) in days {
        if let Some((_, previous_end)) = merged.last_mut()
            && (start_day <= *previous_end
                || previous_end
                    .succ_opt()
                    .is_some_and(|next_day| start_day <= next_day))
        {
            *previous_end = (*previous_end).max(end_day);
        } else {
            merged.push((start_day, end_day));
        }
    }
    merged
}

fn completed_days_through_cursor(
    days: &BTreeSet<NaiveDate>,
    cursor_ns: i64,
) -> BTreeSet<NaiveDate> {
    days.iter()
        .copied()
        .filter(|day| {
            backtest_tick_trading_day_range(*day)
                .map(|range| range.end_ns <= cursor_ns)
                .unwrap_or(false)
        })
        .collect()
}

fn render_loop(shared: Arc<Mutex<ProgressState>>) {
    let mode = shared
        .lock()
        .map(|state| state.mode)
        .unwrap_or(ResolvedProgressMode::Off);
    match mode {
        ResolvedProgressMode::Tty => render_tty(shared),
        ResolvedProgressMode::Plain => render_plain(shared),
        ResolvedProgressMode::Jsonl | ResolvedProgressMode::Off => {}
    }
}

struct RecentRowsRate {
    samples: VecDeque<(Instant, usize)>,
}

impl RecentRowsRate {
    fn new(started_at: Instant, rows: usize) -> Self {
        Self {
            samples: VecDeque::from([(started_at, rows)]),
        }
    }

    fn observe(&mut self, rows: usize) -> usize {
        self.observe_at(Instant::now(), rows)
    }

    fn observe_at(&mut self, observed_at: Instant, rows: usize) -> usize {
        self.samples.push_back((observed_at, rows));
        let cutoff = observed_at
            .checked_sub(RECENT_RATE_WINDOW)
            .unwrap_or(observed_at);
        while self.samples.len() > 1
            && self
                .samples
                .front()
                .is_some_and(|(sampled_at, _)| *sampled_at < cutoff)
        {
            self.samples.pop_front();
        }
        let Some((sampled_at, sampled_rows)) = self.samples.front().copied() else {
            return 0;
        };
        let elapsed = observed_at
            .saturating_duration_since(sampled_at)
            .as_secs_f64();
        if elapsed <= f64::EPSILON {
            return 0;
        }
        (rows.saturating_sub(sampled_rows) as f64 / elapsed) as usize
    }
}

fn render_plain(shared: Arc<Mutex<ProgressState>>) {
    let mut rendered_revision = u64::MAX;
    let mut last_rendered_at = None;
    let started_at = shared
        .lock()
        .map(|state| state.started_at)
        .unwrap_or_else(|_| Instant::now());
    let mut recent_rate = RecentRowsRate::new(started_at, 0);
    loop {
        thread::sleep(RENDER_INTERVAL);
        let snapshot = match shared.lock() {
            Ok(state) => state.clone(),
            Err(_) => return,
        };
        if snapshot.revision != rendered_revision {
            if snapshot.finished.is_none()
                && last_rendered_at.is_some_and(|rendered_at: Instant| {
                    rendered_at.elapsed() < PLAIN_RENDER_INTERVAL
                })
            {
                continue;
            }
            rendered_revision = snapshot.revision;
            last_rendered_at = Some(Instant::now());
            let (covered, planned, received, missing, rows) = snapshot.coverage_counts();
            let display_rows = snapshot.display_rows(rows);
            let rows_per_second = display_rows.map(|rows| recent_rate.observe(rows));
            if let Some(inspection) = &snapshot.inspection {
                eprintln!(
                    "tqsdk-cache: phase=inspection checked_ranges={}/{} complete_ranges={} incomplete_ranges={} symbol={} range=[{}, {})",
                    inspection.checked_ranges,
                    inspection.total_ranges,
                    inspection.complete_ranges,
                    inspection.incomplete_ranges,
                    inspection.physical_symbol,
                    inspection.requested_range.0,
                    inspection.requested_range.1,
                );
            } else if snapshot.plan.is_none() && !snapshot.history_fill {
                eprintln!("tqsdk-cache: phase=planning message={}", snapshot.planning);
            } else {
                eprintln!(
                    "tqsdk-cache: phase=fill status={} batches={}/{} coverage_days={}/{} received_days={}/{} rows={} recent_rows_per_sec={} calendar={}{}",
                    if snapshot.failed { "failed" } else { "running" },
                    snapshot.completed_batches.len(),
                    snapshot.total_batches,
                    covered,
                    planned,
                    received,
                    missing,
                    display_rows
                        .map(|rows| rows.to_string())
                        .unwrap_or_else(|| "n/a".to_string()),
                    rows_per_second
                        .map(|rate| rate.to_string())
                        .unwrap_or_else(|| "n/a".to_string()),
                    snapshot
                        .calendar
                        .as_ref()
                        .map(|calendar| calendar.source.as_str())
                        .unwrap_or("partition_fallback"),
                    snapshot
                        .calendar_error
                        .as_ref()
                        .map(|error| format!(" calendar_error={error:?}"))
                        .unwrap_or_default(),
                );
                for (symbol, state) in snapshot
                    .symbols
                    .iter()
                    .filter(|(_, state)| state.active || state.error.is_some())
                {
                    let trading_day = state
                        .latest_trading_day
                        .map(|day| day.to_string())
                        .unwrap_or_else(|| "-".to_string());
                    let (covered, planned, received, missing) =
                        state.day_counts(snapshot.history_fill);
                    eprintln!(
                        "tqsdk-cache: phase=symbol symbol={} state={} trading_day={} coverage_days={}/{} received_days={}/{} rows={} attempt_retries={} split_fallback={}{}",
                        symbol,
                        state.phase.map(phase_name).unwrap_or("pending"),
                        trading_day,
                        covered,
                        planned,
                        received,
                        missing,
                        snapshot
                            .display_symbol_rows(state)
                            .map(|rows| rows.to_string())
                            .unwrap_or_else(|| "n/a".to_string()),
                        state.retries,
                        state.split_fallback,
                        state
                            .error
                            .as_deref()
                            .map(|error| format!(" error={error:?}"))
                            .unwrap_or_default(),
                    );
                }
            }
        }
        if let Some(completion) = snapshot.finished {
            eprintln!(
                "tqsdk-cache: phase=complete status={} summary={:?}",
                completion.status.as_str(),
                completion.summary
            );
            return;
        }
    }
}

fn render_tty(shared: Arc<Mutex<ProgressState>>) {
    let multi = MultiProgress::with_draw_target(ProgressDrawTarget::stderr_with_hz(TTY_RENDER_HZ));
    let planning = multi.add(ProgressBar::new_spinner());
    planning.set_style(spinner_style());
    planning.enable_steady_tick(TTY_SPINNER_INTERVAL);
    let mut planning_visible = true;
    let mut inspection = None;
    let mut global = None;
    let mut symbol_bars = BTreeMap::<String, ProgressBar>::new();
    let mut rendered_revision = u64::MAX;
    let mut last_rate_refresh = Instant::now();
    let started_at = shared
        .lock()
        .map(|state| state.started_at)
        .unwrap_or_else(|_| Instant::now());
    let mut recent_rate = RecentRowsRate::new(started_at, 0);

    loop {
        thread::sleep(RENDER_INTERVAL);
        let snapshot = match shared.lock() {
            Ok(state) => state.clone(),
            Err(_) => return,
        };
        if snapshot.revision != rendered_revision
            || last_rate_refresh.elapsed() >= Duration::from_secs(1)
        {
            rendered_revision = snapshot.revision;
            last_rate_refresh = Instant::now();
            if let Some(inspection_state) = &snapshot.inspection {
                if inspection.is_none() {
                    if planning_visible {
                        planning.finish_and_clear();
                        multi.remove(&planning);
                        planning_visible = false;
                    }
                    let bar = multi.add(ProgressBar::new(inspection_state.total_ranges as u64));
                    bar.set_style(global_style());
                    inspection = Some(bar);
                }
                if let Some(inspection) = &inspection {
                    inspection.set_length(inspection_state.total_ranges as u64);
                    inspection.set_position(inspection_state.checked_ranges as u64);
                    inspection.set_message(format!(
                        "检查缓存 | 命中 {} | 缺口 {} | {}",
                        inspection_state.complete_ranges,
                        inspection_state.incomplete_ranges,
                        inspection_state.physical_symbol,
                    ));
                }
            } else if snapshot.plan.is_none() && !snapshot.history_fill {
                planning.set_message(snapshot.planning.clone());
            } else {
                if let Some(inspection) = inspection.take() {
                    inspection.finish_and_clear();
                    multi.remove(&inspection);
                } else if planning_visible {
                    planning.finish_and_clear();
                    multi.remove(&planning);
                    planning_visible = false;
                }
                let active_count = snapshot
                    .symbols
                    .iter()
                    .filter(|(_, state)| state.active)
                    .count();
                let visible = snapshot
                    .visible_symbols()
                    .into_iter()
                    .collect::<BTreeSet<_>>();
                let additional_active = active_count.saturating_sub(snapshot.max_bars);
                if global.is_none() {
                    let bar = multi.add(ProgressBar::new(snapshot.total_batches as u64));
                    bar.set_style(global_style());
                    global = Some(bar);
                }
                if let Some(global) = &global {
                    let (covered, planned, received, missing, raw_rows) =
                        snapshot.coverage_counts();
                    let display_rows = snapshot.display_rows(raw_rows);
                    let rows = display_rows
                        .map(|rows| rows.to_string())
                        .unwrap_or_else(|| "n/a".to_string());
                    let rate = display_rows
                        .map(|rows| recent_rate.observe(rows).to_string())
                        .unwrap_or_else(|| "n/a".to_string());
                    global.set_length(snapshot.total_batches as u64);
                    global.set_position(snapshot.completed_batches.len() as u64);
                    global.set_message(format!(
                    "{} | 覆盖 {covered}/{planned} | 本轮接收 {received}/{missing} | {rows} rows | recent {rate}/s{}",
                        if snapshot.failed { "failed" } else { "running" },
                        if additional_active == 0 {
                            String::new()
                        } else {
                            format!(" | +{additional_active} active")
                        }
                    ));
                }
                for (symbol, bar) in &symbol_bars {
                    if !visible.contains(symbol) {
                        bar.finish_and_clear();
                    }
                }
                symbol_bars.retain(|symbol, _| visible.contains(symbol));
                for symbol in &visible {
                    let bar = symbol_bars.entry(symbol.clone()).or_insert_with(|| {
                        let bar = multi.add(ProgressBar::new(0));
                        bar.set_style(symbol_style());
                        bar
                    });
                    let state = &snapshot.symbols[symbol];
                    let (covered, planned, received, missing) =
                        state.day_counts(snapshot.history_fill);
                    bar.set_prefix(display_symbol(symbol));
                    bar.set_length(missing as u64);
                    bar.set_position(received as u64);
                    let retry = if state.retries > 0 {
                        format!(" | retry {}", state.retries)
                    } else {
                        String::new()
                    };
                    let split = if state.split_fallback { " | split" } else { "" };
                    let trading_day = state
                        .latest_trading_day
                        .map(|day| day.to_string())
                        .unwrap_or_else(|| "-".to_string());
                    bar.set_message(format!(
                        "{} | {} | 覆盖 {}/{} | 本轮接收 {}/{} | {} rows{}{}{}",
                        state.phase.map(phase_name).unwrap_or("pending"),
                        trading_day,
                        covered,
                        planned,
                        received,
                        missing,
                        snapshot
                            .display_symbol_rows(state)
                            .map(|rows| rows.to_string())
                            .unwrap_or_else(|| "n/a".to_string()),
                        retry,
                        split,
                        state
                            .error
                            .as_deref()
                            .map(|error| format!(" | {error}"))
                            .unwrap_or_default(),
                    ));
                }
            }
        }
        if let Some(completion) = snapshot.finished {
            for bar in symbol_bars.values() {
                bar.finish_and_clear();
            }
            if let Some(inspection) = inspection.take() {
                inspection.finish_and_clear();
                multi.remove(&inspection);
            }
            if let Some(global) = &global {
                global.finish_with_message(completion.summary.clone());
            } else {
                if planning_visible {
                    planning.finish_and_clear();
                    multi.remove(&planning);
                }
                let _ = multi.println(format!(
                    "{}: {}",
                    completion.status.as_str(),
                    completion.summary
                ));
            }
            return;
        }
    }
}

fn display_symbol(symbol: &str) -> String {
    const MAX_CHARS: usize = 18;
    let mut chars = symbol.chars();
    let prefix = chars.by_ref().take(MAX_CHARS - 1).collect::<String>();
    if chars.next().is_some() {
        format!("{prefix}…")
    } else {
        prefix
    }
}

fn phase_name(phase: BacktestRemoteFillPhase) -> &'static str {
    match phase {
        BacktestRemoteFillPhase::Inspecting => "inspecting",
        BacktestRemoteFillPhase::PlanReady => "plan_ready",
        BacktestRemoteFillPhase::Started => "started",
        BacktestRemoteFillPhase::Streaming => "streaming",
        BacktestRemoteFillPhase::Retrying => "retrying",
        BacktestRemoteFillPhase::SplitFallback => "split_fallback",
        BacktestRemoteFillPhase::Finished => "finished",
        BacktestRemoteFillPhase::Failed => "failed",
        BacktestRemoteFillPhase::Cancelled => "cancelled",
        _ => "unknown",
    }
}

fn history_phase(phase: BacktestHistoryPhase) -> BacktestRemoteFillPhase {
    match phase {
        BacktestHistoryPhase::Inspect => BacktestRemoteFillPhase::Inspecting,
        BacktestHistoryPhase::WaitForFill => BacktestRemoteFillPhase::PlanReady,
        BacktestHistoryPhase::Fill
        | BacktestHistoryPhase::Read
        | BacktestHistoryPhase::Aggregate => BacktestRemoteFillPhase::Streaming,
        BacktestHistoryPhase::Retry => BacktestRemoteFillPhase::Retrying,
    }
}

fn spinner_style() -> ProgressStyle {
    ProgressStyle::with_template("{spinner:.cyan} {msg}")
        .expect("valid fill spinner progress template")
        .tick_strings(&["-", "\\", "|", "/"])
}

fn global_style() -> ProgressStyle {
    ProgressStyle::with_template("{elapsed_precise} {bar:28.cyan/blue} {pos:>4}/{len:4} {msg}")
        .expect("valid fill global progress template")
        .progress_chars("##-")
}

fn symbol_style() -> ProgressStyle {
    ProgressStyle::with_template("  {prefix:>18} {bar:18.green/blue} {pos:>3}/{len:3} {msg}")
        .expect("valid fill symbol progress template")
        .progress_chars("##-")
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        time::{Duration, Instant},
    };

    use super::{
        ProgressCalendar, ProgressMode, ProgressState, RecentRowsRate, ResolvedProgressMode,
        SymbolProgress, completed_days_through_cursor, days_for_ranges, resolve_mode,
    };
    use chrono::NaiveDate;
    use tqsdk::BacktestRemoteFillPhase;
    use tqsdk_cache::FillReportSymbolDayStats;
    use tqsdk_data::{
        BacktestHistoryFillFamily, BacktestHistoryFillProgress, BacktestHistoryPhase,
        BacktestHistoryTelemetryEvent, backtest_tick_trading_day_range,
    };

    #[test]
    fn recent_rows_rate_drops_after_a_stall_and_recovers_from_new_rows() {
        let started_at = Instant::now();
        let mut rate = RecentRowsRate::new(started_at, 0);

        assert_eq!(
            rate.observe_at(started_at + Duration::from_secs(10), 1_000),
            100
        );
        assert_eq!(
            rate.observe_at(started_at + Duration::from_secs(70), 1_000),
            0
        );
        assert_eq!(
            rate.observe_at(started_at + Duration::from_secs(80), 2_000),
            100
        );
    }

    #[test]
    fn history_batch_finished_records_rows_without_telemetry() {
        let mut state = ProgressState::new(ResolvedProgressMode::Plain, 8);
        let symbol = "SHFE.au2608".to_string();
        let requested_range = (0, 86_400_000_000_000);

        state.apply_history_progress(&BacktestHistoryFillProgress::BatchStarted {
            family: BacktestHistoryFillFamily::Daily,
            batch_number: 1,
            total_batches: 1,
            requested_range,
            pending_batches: 0,
            active_batches: 1,
            symbols: vec![symbol.clone()],
        });
        state.apply_history_progress(&BacktestHistoryFillProgress::BatchFinished {
            family: BacktestHistoryFillFamily::Daily,
            batch_number: 1,
            total_batches: 1,
            requested_range,
            symbols: vec![symbol.clone()],
            rows_written: 37,
            elapsed: Duration::from_secs(1),
        });

        assert_eq!(state.coverage_counts().4, 37);
        assert_eq!(
            state.symbols[&symbol]
                .rows_by_stream
                .values()
                .sum::<usize>(),
            37
        );
    }

    #[test]
    fn history_batch_finish_does_not_regress_streamed_rows() {
        let mut state = ProgressState::new(ResolvedProgressMode::Plain, 8);
        let symbol = "SHFE.au2608".to_string();
        let requested_range = (0, 86_400_000_000_000);

        state.apply_history_progress(&BacktestHistoryFillProgress::BatchStarted {
            family: BacktestHistoryFillFamily::Tick,
            batch_number: 1,
            total_batches: 1,
            requested_range,
            pending_batches: 0,
            active_batches: 1,
            symbols: vec![symbol.clone()],
        });
        state.apply_history_progress(&BacktestHistoryFillProgress::Telemetry {
            family: BacktestHistoryFillFamily::Tick,
            batch_number: 1,
            total_batches: 1,
            requested_range,
            event: BacktestHistoryTelemetryEvent {
                request_id: Some(1),
                symbol: symbol.clone(),
                phase: BacktestHistoryPhase::Fill,
                completed_rows: 42,
                latest_cursor_ns: None,
                message: "streaming".to_string(),
            },
        });
        state.apply_history_progress(&BacktestHistoryFillProgress::BatchFinished {
            family: BacktestHistoryFillFamily::Tick,
            batch_number: 1,
            total_batches: 1,
            requested_range,
            symbols: vec![symbol.clone()],
            rows_written: 37,
            elapsed: Duration::from_secs(1),
        });

        assert_eq!(state.coverage_counts().4, 42);
        assert_eq!(
            state.symbols[&symbol]
                .rows_by_stream
                .values()
                .sum::<usize>(),
            42
        );
    }

    #[test]
    fn history_finished_ranges_count_as_covered() {
        let mut state = ProgressState::new(ResolvedProgressMode::Plain, 8);
        let symbol = "SHFE.au2608".to_string();
        let range = backtest_tick_trading_day_range(
            NaiveDate::from_ymd_opt(2026, 7, 20).expect("valid test day"),
        )
        .expect("valid test range");
        let requested_range = (range.start_ns, range.end_ns);

        state.apply_history_progress(&BacktestHistoryFillProgress::BatchStarted {
            family: BacktestHistoryFillFamily::Daily,
            batch_number: 1,
            total_batches: 1,
            requested_range,
            pending_batches: 0,
            active_batches: 1,
            symbols: vec![symbol.clone()],
        });
        state.apply_history_progress(&BacktestHistoryFillProgress::BatchFinished {
            family: BacktestHistoryFillFamily::Daily,
            batch_number: 1,
            total_batches: 1,
            requested_range,
            symbols: vec![symbol.clone()],
            rows_written: 37,
            elapsed: Duration::from_secs(1),
        });

        assert_eq!(state.symbols[&symbol].day_counts(true), (1, 1, 1, 1));
        assert_eq!(state.coverage_counts(), (1, 1, 1, 1, 37));
    }

    #[test]
    fn history_symbol_remains_active_until_its_last_batch_finishes() {
        let mut state = ProgressState::new(ResolvedProgressMode::Plain, 8);
        let symbol = "SHFE.au2608".to_string();
        let first = backtest_tick_trading_day_range(
            NaiveDate::from_ymd_opt(2026, 7, 20).expect("valid first test day"),
        )
        .expect("valid first test range");
        let second = backtest_tick_trading_day_range(
            NaiveDate::from_ymd_opt(2026, 7, 21).expect("valid second test day"),
        )
        .expect("valid second test range");
        let first_range = (first.start_ns, first.end_ns);
        let second_range = (second.start_ns, second.end_ns);

        for (batch_number, requested_range) in [(1, first_range), (2, second_range)] {
            state.apply_history_progress(&BacktestHistoryFillProgress::BatchStarted {
                family: BacktestHistoryFillFamily::Daily,
                batch_number,
                total_batches: 2,
                requested_range,
                pending_batches: 2 - batch_number,
                active_batches: batch_number,
                symbols: vec![symbol.clone()],
            });
        }
        state.apply_history_progress(&BacktestHistoryFillProgress::BatchFinished {
            family: BacktestHistoryFillFamily::Daily,
            batch_number: 1,
            total_batches: 2,
            requested_range: first_range,
            symbols: vec![symbol.clone()],
            rows_written: 1,
            elapsed: Duration::from_secs(1),
        });

        assert!(state.symbols[&symbol].active);

        state.apply_history_progress(&BacktestHistoryFillProgress::BatchFailed {
            family: BacktestHistoryFillFamily::Daily,
            batch_number: 2,
            total_batches: 2,
            requested_range: second_range,
            symbols: vec![symbol.clone()],
            error: "provider timeout".to_string(),
        });

        assert!(!state.symbols[&symbol].active);
        assert_eq!(
            state.symbols[&symbol].error.as_deref(),
            Some("provider timeout")
        );
        assert_eq!(
            state.jsonl_record()["symbols"][0]["error"],
            "provider timeout"
        );
    }

    #[test]
    fn history_progress_keeps_long_day_ranges_compact() {
        let first_day = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let last_day = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let first_range = backtest_tick_trading_day_range(first_day).unwrap();
        let last_range = backtest_tick_trading_day_range(last_day).unwrap();
        let requested_range = (first_range.start_ns, last_range.end_ns);
        let expected_days =
            usize::try_from(last_day.signed_duration_since(first_day).num_days() + 1).unwrap();
        let symbol = "SHFE.au2608".to_string();
        let mut state = ProgressState::new(ResolvedProgressMode::Plain, 8);

        state.apply_history_progress(&BacktestHistoryFillProgress::BatchStarted {
            family: BacktestHistoryFillFamily::Daily,
            batch_number: 1,
            total_batches: 2,
            requested_range,
            pending_batches: 1,
            active_batches: 1,
            symbols: vec![symbol.clone()],
        });

        assert!(state.symbols[&symbol].planned_days.is_empty());
        assert!(state.symbols[&symbol].missing_days.is_empty());
        assert_eq!(
            state.symbols[&symbol].day_counts(true),
            (0, expected_days, 0, expected_days)
        );
        assert_eq!(
            state.coverage_counts(),
            (0, expected_days, 0, expected_days, 0)
        );
    }

    #[test]
    fn history_streaming_cursor_advances_received_without_committing_coverage() {
        let first_day = NaiveDate::from_ymd_opt(2026, 7, 20).unwrap();
        let second_day = NaiveDate::from_ymd_opt(2026, 7, 21).unwrap();
        let first_range = backtest_tick_trading_day_range(first_day).unwrap();
        let second_range = backtest_tick_trading_day_range(second_day).unwrap();
        let requested_range = (first_range.start_ns, second_range.end_ns);
        let symbol = "SHFE.au2608".to_string();
        let mut state = ProgressState::new(ResolvedProgressMode::Plain, 8);

        state.apply_history_progress(&BacktestHistoryFillProgress::BatchStarted {
            family: BacktestHistoryFillFamily::Tick,
            batch_number: 1,
            total_batches: 1,
            requested_range,
            pending_batches: 0,
            active_batches: 1,
            symbols: vec![symbol.clone()],
        });
        state.apply_history_progress(&BacktestHistoryFillProgress::Telemetry {
            family: BacktestHistoryFillFamily::Tick,
            batch_number: 1,
            total_batches: 1,
            requested_range,
            event: BacktestHistoryTelemetryEvent {
                request_id: Some(1),
                symbol: symbol.clone(),
                phase: BacktestHistoryPhase::Fill,
                completed_rows: 1,
                latest_cursor_ns: Some(first_range.start_ns.saturating_add(1)),
                message: "first partition streaming".to_string(),
            },
        });

        assert_eq!(state.symbols[&symbol].day_counts(true), (0, 2, 0, 2));
        assert_eq!(state.coverage_counts(), (0, 2, 0, 2, 1));

        state.apply_history_progress(&BacktestHistoryFillProgress::Telemetry {
            family: BacktestHistoryFillFamily::Tick,
            batch_number: 1,
            total_batches: 1,
            requested_range,
            event: BacktestHistoryTelemetryEvent {
                request_id: Some(1),
                symbol: symbol.clone(),
                phase: BacktestHistoryPhase::Fill,
                completed_rows: 42,
                latest_cursor_ns: Some(second_range.start_ns),
                message: "streaming".to_string(),
            },
        });

        assert_eq!(state.symbols[&symbol].day_counts(true), (0, 2, 1, 2));
        assert_eq!(state.coverage_counts(), (0, 2, 1, 2, 42));

        state.apply_history_progress(&BacktestHistoryFillProgress::Telemetry {
            family: BacktestHistoryFillFamily::Tick,
            batch_number: 1,
            total_batches: 1,
            requested_range,
            event: BacktestHistoryTelemetryEvent {
                request_id: Some(1),
                symbol: symbol.clone(),
                phase: BacktestHistoryPhase::Fill,
                completed_rows: 40,
                latest_cursor_ns: Some(first_range.end_ns.saturating_sub(1)),
                message: "out-of-order streaming".to_string(),
            },
        });

        assert_eq!(state.symbols[&symbol].day_counts(true), (0, 2, 1, 2));
        assert_eq!(state.coverage_counts(), (0, 2, 1, 2, 42));
        assert_eq!(state.symbols[&symbol].history_streamed_ranges.len(), 1);

        let mut failed_state = state.clone();
        failed_state.apply_history_progress(&BacktestHistoryFillProgress::BatchFailed {
            family: BacktestHistoryFillFamily::Tick,
            batch_number: 1,
            total_batches: 1,
            requested_range,
            symbols: vec![symbol.clone()],
            error: "provider timeout".to_string(),
        });
        assert_eq!(failed_state.symbols[&symbol].day_counts(true), (0, 2, 0, 2));
        assert!(
            failed_state.symbols[&symbol]
                .history_streamed_ranges
                .is_empty()
        );

        state.apply_history_progress(&BacktestHistoryFillProgress::BatchFinished {
            family: BacktestHistoryFillFamily::Tick,
            batch_number: 1,
            total_batches: 1,
            requested_range,
            symbols: vec![symbol.clone()],
            rows_written: 42,
            elapsed: Duration::from_secs(1),
        });

        assert_eq!(state.symbols[&symbol].day_counts(true), (2, 2, 2, 2));
        assert!(state.symbols[&symbol].history_streamed_ranges.is_empty());
    }

    #[test]
    fn history_progress_scales_to_provider_roster_without_day_sets() {
        const PROVIDER_ROSTER_SIZE: usize = 5_378;

        let first_day = NaiveDate::from_ymd_opt(1989, 12, 29).unwrap();
        let last_day = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let first_range = backtest_tick_trading_day_range(first_day).unwrap();
        let last_range = backtest_tick_trading_day_range(last_day).unwrap();
        let requested_range = (first_range.start_ns, last_range.end_ns);
        let days_per_symbol =
            usize::try_from(last_day.signed_duration_since(first_day).num_days() + 1).unwrap();
        let mut state = ProgressState::new(ResolvedProgressMode::Plain, 8);

        for batch_number in 0..PROVIDER_ROSTER_SIZE {
            state.apply_history_progress(&BacktestHistoryFillProgress::BatchStarted {
                family: BacktestHistoryFillFamily::Daily,
                batch_number,
                total_batches: PROVIDER_ROSTER_SIZE,
                requested_range,
                pending_batches: PROVIDER_ROSTER_SIZE - batch_number - 1,
                active_batches: 1,
                symbols: vec![format!("SHFE.au{batch_number:04}")],
            });
        }

        assert_eq!(state.symbols.len(), PROVIDER_ROSTER_SIZE);
        assert!(
            state
                .symbols
                .values()
                .all(|symbol| symbol.planned_days.is_empty())
        );
        assert_eq!(
            state.coverage_counts(),
            (
                0,
                days_per_symbol * PROVIDER_ROSTER_SIZE,
                0,
                days_per_symbol * PROVIDER_ROSTER_SIZE,
                0,
            )
        );
    }

    #[test]
    fn minute_scope_stabilizes_the_denominator_before_telemetry_arrives() {
        let mut state =
            ProgressState::new_with_cache_kind(ResolvedProgressMode::Plain, 8, "minute");
        let day = NaiveDate::from_ymd_opt(2026, 7, 20).expect("valid test day");
        let range = backtest_tick_trading_day_range(day).expect("valid test range");
        let symbols = vec!["SHFE.au2608".to_string(), "DCE.i2609".to_string()];

        state.set_scope(&symbols, (range.start_ns, range.end_ns));
        assert_eq!(state.coverage_counts(), (0, 2, 0, 2, 0));

        // Repeated planning callbacks must not re-expand an already fixed scope.
        state.set_scope(&symbols, (range.start_ns, range.end_ns));
        assert_eq!(state.coverage_counts(), (0, 2, 0, 2, 0));
    }

    #[test]
    fn minute_remote_plan_keeps_the_user_requested_denominator() {
        let mut state =
            ProgressState::new_with_cache_kind(ResolvedProgressMode::Plain, 8, "minute");
        let first_day = NaiveDate::from_ymd_opt(2026, 7, 20).expect("valid first test day");
        let second_day = NaiveDate::from_ymd_opt(2026, 7, 21).expect("valid second test day");
        let first_range =
            backtest_tick_trading_day_range(first_day).expect("valid first test range");
        let second_range =
            backtest_tick_trading_day_range(second_day).expect("valid second test range");
        let symbol = "SHFE.au2608".to_string();

        state.set_scope(
            std::slice::from_ref(&symbol),
            (first_range.start_ns, second_range.end_ns),
        );
        state.apply_plan_symbol_ranges(
            &symbol,
            &[(first_range.start_ns, first_range.end_ns)],
            &[(first_range.start_ns, first_range.end_ns)],
        );
        state.recalculate_days();

        assert_eq!(state.coverage_counts(), (1, 2, 0, 1, 0));
    }

    #[test]
    fn minute_rows_wait_for_the_canonical_terminal_report() {
        let mut state =
            ProgressState::new_with_cache_kind(ResolvedProgressMode::Plain, 8, "minute");
        let symbol = "SHFE.au2608".to_string();
        state
            .symbols
            .entry(symbol.clone())
            .or_default()
            .rows_by_stream
            .insert((1, 1, 0, 1), 88);

        assert_eq!(state.display_rows(88), None);
        assert_eq!(state.display_symbol_rows(&state.symbols[&symbol]), None);

        state.final_rows = Some(71);
        state
            .symbols
            .get_mut(&symbol)
            .expect("symbol exists")
            .final_rows = Some(70);
        assert_eq!(state.display_rows(88), Some(71));
        assert_eq!(state.display_symbol_rows(&state.symbols[&symbol]), Some(70));
    }

    #[test]
    fn partition_days_preserve_night_session_day_boundaries() {
        let trading_day = NaiveDate::from_ymd_opt(2026, 7, 20).unwrap();
        let range = backtest_tick_trading_day_range(trading_day).unwrap();
        let days = days_for_ranges(&[(range.start_ns, range.end_ns)], None);

        assert_eq!(days, BTreeSet::from([trading_day]));
    }

    #[test]
    fn calendar_filters_partition_days_for_progress_totals() {
        let first_day = NaiveDate::from_ymd_opt(2026, 7, 20).unwrap();
        let second_day = NaiveDate::from_ymd_opt(2026, 7, 21).unwrap();
        let first_range = backtest_tick_trading_day_range(first_day).unwrap();
        let second_range = backtest_tick_trading_day_range(second_day).unwrap();
        let calendar = ProgressCalendar {
            source: "test".to_string(),
            days: vec![second_day],
        };

        let days = days_for_ranges(
            &[
                (first_range.start_ns, first_range.end_ns),
                (second_range.start_ns, second_range.end_ns),
            ],
            Some(&calendar),
        );

        assert_eq!(days, BTreeSet::from([second_day]));
    }

    #[test]
    fn physical_symbol_denominator_uses_its_own_requested_range() {
        let first_day = NaiveDate::from_ymd_opt(2026, 7, 20).unwrap();
        let second_day = NaiveDate::from_ymd_opt(2026, 7, 21).unwrap();
        let first_range = backtest_tick_trading_day_range(first_day).unwrap();
        let second_range = backtest_tick_trading_day_range(second_day).unwrap();
        let mut state = ProgressState::new(super::ResolvedProgressMode::Plain, 8);
        state.calendar = Some(ProgressCalendar {
            source: "test".to_string(),
            days: vec![first_day, second_day],
        });
        state.symbols.insert(
            "SHFE.au2608".to_string(),
            SymbolProgress {
                requested_ranges: vec![(first_range.start_ns, first_range.end_ns)],
                missing_ranges: vec![(first_range.start_ns, first_range.end_ns)],
                ..SymbolProgress::default()
            },
        );
        state.symbols.insert(
            "SHFE.au2610".to_string(),
            SymbolProgress {
                requested_ranges: vec![(second_range.start_ns, second_range.end_ns)],
                missing_ranges: vec![(second_range.start_ns, second_range.end_ns)],
                ..SymbolProgress::default()
            },
        );

        state.recalculate_days();

        assert_eq!(state.symbols["SHFE.au2608"].planned_days.len(), 1);
        assert_eq!(state.symbols["SHFE.au2610"].planned_days.len(), 1);
    }

    #[test]
    fn streaming_cursor_counts_only_completed_tqbn_partitions() {
        let first_day = NaiveDate::from_ymd_opt(2026, 7, 20).unwrap();
        let second_day = NaiveDate::from_ymd_opt(2026, 7, 21).unwrap();
        let first_range = backtest_tick_trading_day_range(first_day).unwrap();
        let second_range = backtest_tick_trading_day_range(second_day).unwrap();
        let days = BTreeSet::from([first_day, second_day]);

        assert!(completed_days_through_cursor(&days, first_range.start_ns + 1).is_empty());
        assert_eq!(
            completed_days_through_cursor(&days, second_range.start_ns),
            BTreeSet::from([first_day])
        );
        assert_eq!(
            completed_days_through_cursor(&days, second_range.end_ns),
            BTreeSet::from([first_day, second_day])
        );
    }

    #[test]
    fn partial_report_counts_do_not_forge_day_assignment() {
        let first_day = NaiveDate::from_ymd_opt(2026, 7, 20).unwrap();
        let second_day = NaiveDate::from_ymd_opt(2026, 7, 21).unwrap();
        let mut state = ProgressState::new(super::ResolvedProgressMode::Plain, 8);
        state.symbols.insert(
            "SHFE.au2608".to_string(),
            SymbolProgress {
                planned_days: BTreeSet::from([first_day, second_day]),
                missing_days: BTreeSet::from([first_day, second_day]),
                ..SymbolProgress::default()
            },
        );

        state.apply_day_stats(&FillReportSymbolDayStats {
            symbol: "SHFE.au2608".to_string(),
            planned_days: 2,
            covered_days: 1,
            missing_days: 2,
            received_days: 1,
        });

        assert!(state.symbols["SHFE.au2608"].covered_days.is_empty());
        assert!(state.symbols["SHFE.au2608"].received_days.is_empty());
    }

    #[test]
    fn explicit_tty_mode_bypasses_terminal_auto_detection() {
        assert_eq!(resolve_mode(ProgressMode::Tty), ResolvedProgressMode::Tty);
    }

    #[test]
    fn visible_symbols_keep_the_most_recent_terminal_result_after_active_work() {
        let mut state = ProgressState::new(super::ResolvedProgressMode::Tty, 2);
        state.symbols.insert(
            "KQ.m@GFEX.pd".to_string(),
            SymbolProgress {
                active: true,
                phase: Some(BacktestRemoteFillPhase::Started),
                last_event_sequence: 1,
                ..SymbolProgress::default()
            },
        );
        state.symbols.insert(
            "KQ.m@GFEX.ps".to_string(),
            SymbolProgress {
                phase: Some(BacktestRemoteFillPhase::Finished),
                last_event_sequence: 2,
                ..SymbolProgress::default()
            },
        );
        state.symbols.insert(
            "KQ.m@GFEX.pt".to_string(),
            SymbolProgress {
                phase: Some(BacktestRemoteFillPhase::Finished),
                last_event_sequence: 3,
                ..SymbolProgress::default()
            },
        );

        assert_eq!(
            state.visible_symbols(),
            vec!["KQ.m@GFEX.pd".to_string(), "KQ.m@GFEX.pt".to_string()]
        );
    }
}
