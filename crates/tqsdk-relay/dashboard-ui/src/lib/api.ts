import type {
  DashboardSnapshot,
  DashboardSymbolMetricsSnapshot,
  DashboardSymbolRow,
  DashboardTimelineHistory,
  RelaySnapshot,
  SymbolMetricsSnapshot,
  SymbolRow,
  TimelineHistory,
} from './types';

export type FetchRelaySnapshotOptions = {
  includeTimelineHistory?: boolean;
};

export class DashboardApiError extends Error {
  constructor(
    public readonly path: string,
    public readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

async function fetchJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(path, { cache: 'no-store', signal });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message =
      typeof body === 'object' && body != null && 'error' in body && typeof body.error === 'string'
        ? body.error
        : `HTTP ${response.status}`;
    throw new DashboardApiError(path, response.status, message);
  }
  return body as T;
}

export async function fetchRelaySnapshot(
  signal?: AbortSignal,
  options: FetchRelaySnapshotOptions = {},
) {
  const path = options.includeTimelineHistory
    ? '/dashboard-snapshot?timeline_history=1'
    : '/dashboard-snapshot';
  const snapshot = await fetchJson<DashboardSnapshot>(path, signal);
  const relaySnapshot: RelaySnapshot = {
    received_at_unix_millis: snapshot.received_at_unix_millis,
    metrics: snapshot.metrics,
    global: snapshot.global,
    timeline: snapshot.timeline,
    timeline_history: snapshot.timeline_history,
    page: normalizeDashboardPage(snapshot.page),
    events: snapshot.events,
    receivedAt: snapshot.received_at_unix_millis || Date.now(),
  };
  if (snapshot.timeline_history) {
    relaySnapshot.timelineHistory = normalizeTimelineHistory(snapshot.timeline_history);
  }
  return relaySnapshot;
}

function normalizeDashboardPage(page: DashboardSymbolMetricsSnapshot): SymbolMetricsSnapshot {
  return {
    ...page,
    symbols: page.symbols.map(normalizeDashboardSymbolRow),
  };
}

function normalizeDashboardSymbolRow(row: DashboardSymbolRow): SymbolRow {
  return {
    symbol: row.symbol,
    instrument_name: row.instrument_name ?? null,
    status: row.status ?? 'live',
    coverage: row.coverage ?? 'covered',
    session: row.session ?? 'open',
    flow: row.flow ?? 'flowing',
    integrity: row.integrity ?? 'intact',
    problem: row.problem ?? false,
    problem_severity: row.problem_severity ?? 'live',
    in_universe: row.in_universe ?? true,
    subscribed: row.subscribed ?? false,
    quote_subscriber_count: row.quote_subscriber_count ?? 0,
    chart_subscriber_count: row.chart_subscriber_count ?? 0,
    ticks_ingested: row.ticks_ingested ?? 0,
    source_epoch: row.source_epoch ?? 0,
    last_tick_id: row.last_tick_id ?? null,
    gap_event_count: row.gap_event_count ?? 0,
    estimated_missing_rows: row.estimated_missing_rows ?? 0,
    duplicate_rows: row.duplicate_rows ?? 0,
    out_of_order_rows: row.out_of_order_rows ?? 0,
    last_gap_unix_millis: row.last_gap_unix_millis ?? null,
    receive_gap_ms: row.receive_gap_ms ?? null,
    avg_receive_gap_ms: row.avg_receive_gap_ms ?? null,
    market_time_lag_ms: row.market_time_lag_ms ?? null,
    last_receive_unix_millis: row.last_receive_unix_millis ?? null,
    last_tick_datetime_ns: row.last_tick_datetime_ns ?? null,
    invalid_rows: row.invalid_rows ?? 0,
    last_invalid_row_error: row.last_invalid_row_error ?? null,
  };
}

function normalizeTimelineHistory(history: DashboardTimelineHistory): TimelineHistory {
  return {
    samples: history.samples.map((sample) => ({
      sampledAt: sample.sampled_at_unix_millis,
      sample: sample.sample,
      symbols: sample.symbols,
    })),
  };
}
