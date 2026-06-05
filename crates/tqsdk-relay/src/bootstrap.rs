#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::interest::SourceKey;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapRequest {
    pub source: SourceKey,
    pub start_id: i64,
    pub end_id: i64,
}

#[derive(Debug)]
pub struct BootstrapQueue {
    max_inflight: usize,
    min_interval: Duration,
    inflight: usize,
    last_start: Option<Instant>,
    pending: VecDeque<BootstrapRequest>,
}

impl BootstrapQueue {
    #[must_use]
    pub fn new(max_inflight: usize, min_interval: Duration) -> Self {
        assert!(max_inflight > 0, "max_inflight must be greater than zero");
        Self {
            max_inflight,
            min_interval,
            inflight: 0,
            last_start: None,
            pending: VecDeque::new(),
        }
    }

    pub fn enqueue(&mut self, request: BootstrapRequest) {
        if let Some(existing) = self
            .pending
            .iter_mut()
            .find(|existing| existing.source == request.source)
        {
            existing.start_id = existing.start_id.min(request.start_id);
            existing.end_id = existing.end_id.max(request.end_id);
            return;
        }
        self.pending.push_back(request);
    }

    pub fn poll_ready(&mut self, now: Instant) -> Option<BootstrapRequest> {
        if self.inflight >= self.max_inflight {
            return None;
        }
        if !self.request_interval_elapsed(now) {
            return None;
        }

        let request = self.pending.pop_front()?;
        self.inflight += 1;
        self.last_start = Some(now);
        Some(request)
    }

    pub fn complete_one(&mut self) {
        self.inflight = self.inflight.saturating_sub(1);
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    #[must_use]
    pub fn inflight(&self) -> usize {
        self.inflight
    }

    fn request_interval_elapsed(&self, now: Instant) -> bool {
        match self
            .last_start
            .and_then(|last_start| now.checked_duration_since(last_start))
        {
            None => self.last_start.is_none(),
            Some(elapsed) => elapsed >= self.min_interval,
        }
    }
}
