#![cfg_attr(not(test), forbid(unsafe_code))]

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelaySourceStatus {
    Connecting,
    Up,
    Degraded,
    Down,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthSnapshot {
    pub ready: bool,
    pub upstream_status: RelaySourceStatus,
    pub downstream_clients: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub downstream_clients: usize,
    pub quote_subscriptions: usize,
    pub chart_subscriptions: usize,
    pub ticks_ingested: u64,
    pub bootstrap_pending: usize,
    pub bootstrap_inflight: usize,
}
