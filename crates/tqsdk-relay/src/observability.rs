#![cfg_attr(not(test), forbid(unsafe_code))]

use serde::Serialize;

pub const DEFAULT_DATA_STALE_AFTER_SECS: u64 = 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RelaySourceStatus {
    Connecting,
    Up,
    Degraded,
    Down,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RelaySourceStage {
    Connecting,
    Subscribing,
    Backfilling,
    Live,
    Degraded,
    Down,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HealthSnapshot {
    pub ready: bool,
    pub market_data_ready: bool,
    pub process_started: bool,
    pub downstream_listening: bool,
    pub upstream_status: RelaySourceStatus,
    pub upstream_stage: RelaySourceStage,
    pub upstream_stage_started_unix_secs: Option<u64>,
    pub upstream_connected: bool,
    pub upstream_transport_connected: bool,
    pub upstream_subscription_sent: bool,
    pub universe_ready: bool,
    pub data_fresh: bool,
    pub downstream_clients: usize,
    pub upstream_symbols: usize,
    pub ticks_ingested: u64,
    pub upstream_frames_received: u64,
    pub upstream_events_decoded: u64,
    pub upstream_invalid_tick_rows: u64,
    pub last_upstream_invalid_tick_row_error: Option<String>,
    pub last_universe_refresh_unix_secs: Option<u64>,
    pub last_universe_refresh_error: Option<String>,
    pub last_tick_unix_secs: Option<u64>,
    pub last_upstream_frame_unix_secs: Option<u64>,
    pub data_stale_after_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MetricsSnapshot {
    pub downstream_clients: usize,
    pub quote_subscriptions: usize,
    pub chart_subscriptions: usize,
    pub ticks_ingested: u64,
    pub bootstrap_pending: usize,
    pub bootstrap_inflight: usize,
    pub upstream_stage: RelaySourceStage,
    pub upstream_stage_started_unix_secs: Option<u64>,
    pub upstream_transport_connected: bool,
    pub upstream_subscription_sent: bool,
    pub upstream_frames_received: u64,
    pub upstream_events_decoded: u64,
    pub upstream_symbols: usize,
    pub upstream_ins_list_chars: usize,
    pub upstream_ins_list_warn_chars: Option<usize>,
    pub upstream_ins_list_max_chars: Option<usize>,
    pub upstream_ins_list_over_warn: bool,
    pub upstream_invalid_tick_rows: u64,
    pub last_upstream_invalid_tick_row_error: Option<String>,
    pub last_universe_refresh_unix_secs: Option<u64>,
    pub last_universe_refresh_error: Option<String>,
    pub last_tick_unix_secs: Option<u64>,
    pub last_upstream_frame_unix_secs: Option<u64>,
}
