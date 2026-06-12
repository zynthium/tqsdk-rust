#![cfg_attr(not(test), forbid(unsafe_code))]

use serde::Serialize;

pub const DEFAULT_DATA_STALE_AFTER_SECS: u64 = 30;
pub const FRAME_IDLE_WARN_AFTER_MS: u64 = 2_000;
pub const FRAME_IDLE_CRITICAL_AFTER_MS: u64 = 5_000;
pub const EVENT_IDLE_WARN_AFTER_MS: u64 = 3_000;
pub const EVENT_IDLE_CRITICAL_AFTER_MS: u64 = 8_000;
pub const DECODE_HEALTH_WINDOW_SECS: u64 = 60;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowIdleHealth {
    NoSample,
    Live,
    Warn,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DecodeHealth {
    Healthy,
    Degraded,
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
    pub lifetime_invalid_rows: u64,
    pub recent_invalid_rows_1m: u64,
    pub current_decode_health: DecodeHealth,
    pub last_upstream_peek_delay_ms: Option<u64>,
    pub last_upstream_decode_ms: Option<u64>,
    pub last_upstream_invalid_tick_row_error: Option<String>,
    pub last_invalid_row_unix_secs: Option<u64>,
    pub last_universe_refresh_unix_secs: Option<u64>,
    pub last_universe_refresh_error: Option<String>,
    pub last_tick_unix_secs: Option<u64>,
    pub last_upstream_frame_unix_secs: Option<u64>,
    pub last_decoded_event_unix_secs: Option<u64>,
    pub upstream_frame_idle_ms: Option<u64>,
    pub upstream_frame_idle_health: FlowIdleHealth,
    pub upstream_event_idle_ms: Option<u64>,
    pub upstream_event_idle_health: FlowIdleHealth,
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
    pub last_decoded_event_unix_secs: Option<u64>,
    pub upstream_frame_idle_ms: Option<u64>,
    pub upstream_frame_idle_health: FlowIdleHealth,
    pub upstream_frame_idle_warn_after_ms: u64,
    pub upstream_frame_idle_critical_after_ms: u64,
    pub upstream_event_idle_ms: Option<u64>,
    pub upstream_event_idle_health: FlowIdleHealth,
    pub upstream_event_idle_warn_after_ms: u64,
    pub upstream_event_idle_critical_after_ms: u64,
    pub upstream_symbols: usize,
    pub upstream_ins_list_chars: usize,
    pub upstream_ins_list_warn_chars: Option<usize>,
    pub upstream_ins_list_max_chars: Option<usize>,
    pub upstream_ins_list_over_warn: bool,
    pub upstream_invalid_tick_rows: u64,
    pub lifetime_invalid_rows: u64,
    pub recent_invalid_rows_1m: u64,
    pub current_decode_health: DecodeHealth,
    pub last_upstream_peek_delay_ms: Option<u64>,
    pub last_upstream_decode_ms: Option<u64>,
    pub last_upstream_invalid_tick_row_error: Option<String>,
    pub last_invalid_row_unix_secs: Option<u64>,
    pub last_universe_refresh_unix_secs: Option<u64>,
    pub last_universe_refresh_error: Option<String>,
    pub last_tick_unix_secs: Option<u64>,
    pub last_upstream_frame_unix_secs: Option<u64>,
}
