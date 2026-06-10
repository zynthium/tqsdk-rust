export type UpstreamStage =
  | 'connecting'
  | 'subscribing'
  | 'backfilling'
  | 'live'
  | 'degraded'
  | 'down';

export type SymbolStatus = 'live' | 'closed' | 'stale' | 'missing' | 'inactive';
export type ProblemSeverity = 'live' | 'closed' | 'warn' | 'bad';
export type OverallSeverity = 'healthy' | 'warning' | 'critical' | 'warming';

export type SymbolSort =
  | 'symbol_asc'
  | 'status_asc'
  | 'receive_gap_ms_desc'
  | 'market_time_lag_ms_desc'
  | 'ticks_ingested_desc';

export type RelayMetrics = {
  upstream_stage: UpstreamStage;
  upstream_stage_started_unix_secs: number | null;
  upstream_transport_connected: boolean;
  upstream_subscription_sent: boolean;
  last_upstream_frame_unix_secs: number | null;
  upstream_frames_received: number;
  upstream_events_decoded: number;
  upstream_invalid_tick_rows: number;
  upstream_symbols: number;
  downstream_clients: number;
  ticks_ingested: number;
  quote_subscriptions?: number;
  chart_subscriptions?: number;
  data_stale_after_secs: number;
};

export type SymbolMetricsSummary = {
  total: number;
  live: number;
  closed: number;
  stale: number;
  missing: number;
  inactive: number;
  subscribed: number;
  p95_receive_gap_ms: number | null;
};

export type SymbolRow = {
  symbol: string;
  instrument_name: string | null;
  status: SymbolStatus;
  problem: boolean;
  problem_severity: ProblemSeverity;
  in_universe: boolean;
  subscribed: boolean;
  quote_subscriber_count: number;
  chart_subscriber_count: number;
  ticks_ingested: number;
  receive_gap_ms: number | null;
  market_time_lag_ms: number | null;
  last_receive_unix_millis: number | null;
  last_tick_datetime_ns: number | null;
  last_price: number | null;
  last_volume: number | null;
  last_open_interest: number | null;
  invalid_rows: number;
  last_invalid_row_error: string | null;
};

export type SymbolMetricsSnapshot = {
  now_unix_millis: number;
  data_stale_after_millis: number;
  summary: SymbolMetricsSummary;
  symbols: SymbolRow[];
};

export type RelaySnapshot = {
  metrics: RelayMetrics;
  symbols: SymbolMetricsSnapshot;
  receivedAt: number;
};

export type DashboardFilters = {
  statuses: SymbolStatus[];
  subscribedOnly: boolean;
  q: string;
  sort: SymbolSort;
  limit: number;
};

export type DashboardViewState = {
  paused: boolean;
  fullscreen: boolean;
  selectedExchange: string | null;
  selectedSymbol: string | null;
  filters: DashboardFilters;
};

export type IntegrityModel = {
  overall: OverallSeverity;
  sampledAt: number;
  metrics: RelayMetrics;
  snapshot: SymbolMetricsSnapshot;
  rows: SymbolRow[];
  problems: SymbolRow[];
  subscribedProblems: SymbolRow[];
  issueCount: number;
  invalidRowCount: number;
  activeInvalidRowCount: number;
  upstreamIdleMs: number | null;
  coverageRatio: number;
  observedUniverse: number;
  totalUniverse: number;
  frameRate: number | null;
  eventRate: number | null;
  continuityScore: number;
};

export type HistorySample = {
  sampledAt: number;
  frameRate: number | null;
  eventRate: number | null;
  coverageRatio: number;
  issueCount: number;
  upstreamIdleMs: number | null;
  continuityScore: number;
};

export type RuntimeHistory = {
  limit: number;
  samples: HistorySample[];
};

export type TimelineSeverity = 'live' | 'closed' | 'warn' | 'bad';

export type TimelineSample = {
  sampledAt: number;
  exchangeSeverity: Record<string, TimelineSeverity>;
  subscribedSeverity: TimelineSeverity;
  globalSeverity: TimelineSeverity;
};

export type TimelineHistory = {
  samples: TimelineSample[];
};

export type LocalIncident = {
  id: string;
  at: number;
  scope: string;
  type: string;
  detail: string;
  impact: string;
  severity: TimelineSeverity;
};

export type IncidentLedger = {
  limit: number;
  knownStatuses: Map<string, SymbolStatus>;
  incidents: LocalIncident[];
};
