use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use super::report::{BacktestHistoryPhase, BacktestHistoryTelemetryEvent};
use super::request::BacktestHistoryRequestId;

/// Producer side of the non-blocking, best-effort telemetry channel.
///
/// Unlike a bounded row channel, progress updates are coalesced. A slow
/// telemetry reader therefore cannot delay cache reads, remote fills, or row
/// delivery.
#[derive(Clone)]
pub(crate) struct TelemetryHub {
    shared: Arc<TelemetryState>,
}

struct TelemetryState {
    latest: Mutex<Vec<BacktestHistoryTelemetryEvent>>,
    terminal: Mutex<VecDeque<BacktestHistoryTelemetryEvent>>,
    progress: Mutex<BTreeMap<TelemetryProgressKey, TelemetryProgress>>,
    closed: AtomicBool,
    notified: Notify,
}

type TelemetryProgressKey = (Option<BacktestHistoryRequestId>, String, u8);

#[derive(Default)]
struct TelemetryProgress {
    committed_rows: usize,
    current_slice_rows: usize,
    latest_cursor_ns: Option<i64>,
}

impl TelemetryHub {
    pub(crate) fn new() -> Self {
        Self {
            shared: Arc::new(TelemetryState {
                latest: Mutex::new(Vec::new()),
                terminal: Mutex::new(VecDeque::new()),
                progress: Mutex::new(BTreeMap::new()),
                closed: AtomicBool::new(false),
                notified: Notify::new(),
            }),
        }
    }

    pub(crate) fn stream(&self) -> BacktestHistoryTelemetryStream {
        BacktestHistoryTelemetryStream {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Replaces the previous progress value for this request/symbol/phase key.
    pub(crate) fn emit(&self, event: BacktestHistoryTelemetryEvent) {
        let mut event = self.accumulate(event, false);
        let mut latest = self
            .shared
            .latest
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(previous) = latest.iter_mut().find(|previous| {
            previous.request_id == event.request_id
                && previous.symbol == event.symbol
                && previous.phase == event.phase
        }) {
            event.completed_rows = event.completed_rows.max(previous.completed_rows);
            event.latest_cursor_ns = previous
                .latest_cursor_ns
                .into_iter()
                .chain(event.latest_cursor_ns)
                .max();
            *previous = event;
        } else {
            latest.push(event);
        }
        drop(latest);
        self.shared.notified.notify_one();
    }

    /// Keeps terminal telemetry until a reader observes it or the run ends.
    pub(crate) fn emit_terminal(&self, event: BacktestHistoryTelemetryEvent) {
        let event = self.accumulate(event, true);
        self.shared
            .terminal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push_back(event);
        self.shared.notified.notify_one();
    }

    fn accumulate(
        &self,
        mut event: BacktestHistoryTelemetryEvent,
        slice_terminal: bool,
    ) -> BacktestHistoryTelemetryEvent {
        let mut progress = self
            .shared
            .progress
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let key = (
            event.request_id,
            event.symbol.clone(),
            telemetry_phase_key(event.phase),
        );
        let entry = progress.entry(key).or_default();
        entry.current_slice_rows = entry.current_slice_rows.max(event.completed_rows);
        entry.latest_cursor_ns = entry
            .latest_cursor_ns
            .into_iter()
            .chain(event.latest_cursor_ns)
            .max();
        event.completed_rows = entry
            .committed_rows
            .saturating_add(entry.current_slice_rows);
        if slice_terminal {
            entry.committed_rows = event.completed_rows;
            entry.current_slice_rows = 0;
        } else {
            event.latest_cursor_ns = entry.latest_cursor_ns;
        }
        event
    }

    pub(crate) fn close(&self) {
        self.shared.closed.store(true, Ordering::Release);
        self.shared.notified.notify_waiters();
    }
}

fn telemetry_phase_key(phase: BacktestHistoryPhase) -> u8 {
    match phase {
        BacktestHistoryPhase::Inspect => 0,
        BacktestHistoryPhase::WaitForFill => 1,
        BacktestHistoryPhase::Fill => 2,
        BacktestHistoryPhase::Retry => 3,
        BacktestHistoryPhase::Read => 4,
        BacktestHistoryPhase::Aggregate => 5,
    }
}

/// Independent, best-effort progress stream for a backtest history run.
pub struct BacktestHistoryTelemetryStream {
    shared: Arc<TelemetryState>,
}

impl BacktestHistoryTelemetryStream {
    /// Receives the next terminal or latest coalesced progress update.
    pub async fn next(&mut self) -> Option<BacktestHistoryTelemetryEvent> {
        loop {
            let notified = self.shared.notified.notified();
            if let Some(event) = self
                .shared
                .terminal
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop_front()
            {
                return Some(event);
            }
            if let Some(event) = self
                .shared
                .latest
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop()
            {
                return Some(event);
            }
            if self.shared.closed.load(Ordering::Acquire) {
                return None;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TelemetryHub;
    use crate::backtest_history::{BacktestHistoryPhase, BacktestHistoryTelemetryEvent};

    #[tokio::test]
    async fn coalescing_keeps_cursor_monotonic() {
        let telemetry = TelemetryHub::new();
        let mut stream = telemetry.stream();
        let event = |completed_rows, latest_cursor_ns| BacktestHistoryTelemetryEvent {
            request_id: Some(7),
            symbol: "SHFE.au2608".to_string(),
            phase: BacktestHistoryPhase::Fill,
            completed_rows,
            latest_cursor_ns,
            message: "streaming".to_string(),
        };

        telemetry.emit(event(42, Some(200)));
        telemetry.emit(event(40, Some(100)));

        let observed = stream.next().await.expect("coalesced telemetry");
        assert_eq!(observed.completed_rows, 42);
        assert_eq!(observed.latest_cursor_ns, Some(200));
    }
}
