import type { RelayMetrics, SymbolMetricsSnapshot, SymbolRow } from '../lib/types';

export const NOW = 1_700_000_100_000;

export function metrics(overrides: Partial<RelayMetrics> = {}): RelayMetrics {
  return {
    upstream_stage: 'live',
    upstream_stage_started_unix_secs: 1_700_000_000,
    upstream_transport_connected: true,
    upstream_subscription_sent: true,
    last_upstream_frame_unix_secs: 1_700_000_099,
    upstream_frames_received: 10,
    upstream_events_decoded: 20,
    upstream_invalid_tick_rows: 0,
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
    problem: false,
    problem_severity: 'live',
    in_universe: true,
    subscribed: false,
    quote_subscriber_count: 0,
    chart_subscriber_count: 0,
    ticks_ingested: 5,
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
  return {
    now_unix_millis: NOW,
    data_stale_after_millis: 30_000,
    summary: {
      total: rows.length,
      live: rows.filter((item) => item.status === 'live').length,
      closed: rows.filter((item) => item.status === 'closed').length,
      stale: rows.filter((item) => item.status === 'stale').length,
      missing: rows.filter((item) => item.status === 'missing').length,
      inactive: rows.filter((item) => item.status === 'inactive').length,
      subscribed: rows.filter((item) => item.subscribed).length,
      p95_receive_gap_ms: rows.reduce<number | null>((max, item) => {
        if (item.receive_gap_ms == null) return max;
        return max == null ? item.receive_gap_ms : Math.max(max, item.receive_gap_ms);
      }, null),
    },
    symbols: rows,
  };
}
