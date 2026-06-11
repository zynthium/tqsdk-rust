import type { DashboardSnapshot, RelayMetrics, SymbolMetricsSnapshot, SymbolRow } from '../lib/types';

export const NOW = 1_700_000_100_000;

export function metrics(overrides: Partial<RelayMetrics> = {}): RelayMetrics {
  return {
    upstream_stage: 'live',
    upstream_stage_started_unix_secs: 1_700_000_000,
    upstream_transport_connected: true,
    upstream_subscription_sent: true,
    last_upstream_frame_unix_secs: 1_700_000_099,
    last_decoded_event_unix_secs: 1_700_000_099,
    upstream_frame_idle_ms: 1_000,
    upstream_frame_idle_health: 'live',
    upstream_frame_idle_warn_after_ms: 2_000,
    upstream_frame_idle_critical_after_ms: 5_000,
    upstream_event_idle_ms: 1_000,
    upstream_event_idle_health: 'live',
    upstream_event_idle_warn_after_ms: 3_000,
    upstream_event_idle_critical_after_ms: 8_000,
    upstream_frames_received: 10,
    upstream_events_decoded: 20,
    upstream_invalid_tick_rows: 0,
    lifetime_invalid_rows: 0,
    recent_invalid_rows_1m: 0,
    current_decode_health: 'healthy',
    last_invalid_row_unix_secs: null,
    upstream_symbols: 2,
    downstream_clients: 1,
    ticks_ingested: 20,
    quote_subscriptions: 1,
    chart_subscriptions: 0,
    data_stale_after_secs: 30,
    ...overrides,
  };
}

export function row(overrides: Partial<SymbolRow> = {}): SymbolRow {
  return {
    symbol: 'SHFE.au2602',
    instrument_name: '沪金2602',
    status: 'live',
    coverage: 'covered',
    session: 'open',
    flow: 'flowing',
    integrity: 'intact',
    problem: false,
    problem_severity: 'live',
    in_universe: true,
    subscribed: false,
    quote_subscriber_count: 0,
    chart_subscriber_count: 0,
    ticks_ingested: 5,
    source_epoch: 0,
    last_tick_id: 5,
    gap_event_count: 0,
    estimated_missing_rows: 0,
    duplicate_rows: 0,
    out_of_order_rows: 0,
    last_gap_unix_millis: null,
    receive_gap_ms: 900,
    market_time_lag_ms: 1200,
    last_receive_unix_millis: NOW - 900,
    last_tick_datetime_ns: (NOW - 1200) * 1_000_000,
    last_price: 610.2,
    last_volume: 100,
    last_open_interest: 200,
    invalid_rows: 0,
    last_invalid_row_error: null,
    ...overrides,
  };
}

export function symbolSnapshot(rows: SymbolRow[]): SymbolMetricsSnapshot {
  const summaryRows = rows;
  return {
    now_unix_millis: NOW,
    data_stale_after_millis: 30_000,
    summary: {
      total: summaryRows.length,
      live: summaryRows.filter((item) => item.status === 'live').length,
      closed: summaryRows.filter((item) => item.status === 'closed').length,
      stale: summaryRows.filter((item) => item.status === 'stale').length,
      missing: summaryRows.filter((item) => item.status === 'missing').length,
      inactive: summaryRows.filter((item) => item.status === 'inactive').length,
      subscribed: summaryRows.filter((item) => item.subscribed).length,
      problem: summaryRows.filter((item) => item.problem).length,
      subscribed_problem: summaryRows.filter((item) => item.problem && item.subscribed).length,
      universe_total: summaryRows.filter((item) => item.in_universe).length,
      universe_observed: summaryRows.filter(
        (item) => item.in_universe && item.last_receive_unix_millis != null,
      ).length,
      active_invalid_rows: summaryRows.reduce(
        (sum, item) => sum + (item.problem ? Number(item.invalid_rows || 0) : 0),
        0,
      ),
      gap_event_count: summaryRows.reduce((sum, item) => sum + Number(item.gap_event_count || 0), 0),
      estimated_missing_rows: summaryRows.reduce(
        (sum, item) => sum + Number(item.estimated_missing_rows || 0),
        0,
      ),
      duplicate_rows: summaryRows.reduce((sum, item) => sum + Number(item.duplicate_rows || 0), 0),
      out_of_order_rows: summaryRows.reduce(
        (sum, item) => sum + Number(item.out_of_order_rows || 0),
        0,
      ),
      p95_receive_gap_ms: summaryRows.reduce<number | null>((max, item) => {
        if (item.receive_gap_ms == null) return max;
        return max == null ? item.receive_gap_ms : Math.max(max, item.receive_gap_ms);
      }, null),
    },
    filtered_total: rows.length,
    symbols: rows,
  };
}

export function dashboardSnapshot(
  rows: SymbolRow[],
  metricOverrides: Partial<RelayMetrics> = {},
): DashboardSnapshot {
  const page = symbolSnapshot(rows);
  return {
    received_at_unix_millis: NOW,
    metrics: metrics(metricOverrides),
    global: page.summary,
    global_symbols: page.symbols,
    page,
    events: [],
  };
}
