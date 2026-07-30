use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use super::report::BacktestHistoryTelemetryEvent;

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
    closed: AtomicBool,
    notified: Notify,
}

impl TelemetryHub {
    pub(crate) fn new() -> Self {
        Self {
            shared: Arc::new(TelemetryState {
                latest: Mutex::new(Vec::new()),
                terminal: Mutex::new(VecDeque::new()),
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

    /// Replaces the previous progress value for this request/phase pair.
    pub(crate) fn emit(&self, event: BacktestHistoryTelemetryEvent) {
        let mut latest = self
            .shared
            .latest
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(previous) = latest.iter_mut().find(|previous| {
            previous.request_id == event.request_id && previous.phase == event.phase
        }) {
            *previous = event;
        } else {
            latest.push(event);
        }
        drop(latest);
        self.shared.notified.notify_one();
    }

    /// Keeps terminal telemetry until a reader observes it or the run ends.
    pub(crate) fn emit_terminal(&self, event: BacktestHistoryTelemetryEvent) {
        self.shared
            .terminal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push_back(event);
        self.shared.notified.notify_one();
    }

    pub(crate) fn close(&self) {
        self.shared.closed.store(true, Ordering::Release);
        self.shared.notified.notify_waiters();
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
