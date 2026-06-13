export type UpstreamStage =
  | 'connecting'
  | 'subscribing'
  | 'backfilling'
  | 'live'
  | 'degraded'
  | 'down';

export type SymbolStatus = 'live' | 'closed' | 'initializing' | 'stale' | 'missing' | 'inactive';
export type SymbolCoverage = 'covered' | 'uncovered';
export type SymbolSession = 'open' | 'closed' | 'unknown';
export type SymbolFlow = 'flowing' | 'silent' | 'no_sample';
export type SymbolIntegrity = 'intact' | 'suspected' | 'confirmed_gap';
export type ProblemSeverity = 'live' | 'closed' | 'initializing' | 'warn' | 'bad';
export type OverallSeverity = 'healthy' | 'warning' | 'critical' | 'warming' | 'closed';
export type FlowIdleHealth = 'no_sample' | 'live' | 'warn' | 'critical';
export type DecodeHealth = 'healthy' | 'degraded';

export type RelayMetrics = {
  upstream_stage: UpstreamStage;
  upstream_stage_started_unix_secs: number | null;
  upstream_transport_connected: boolean;
  upstream_subscription_sent: boolean;
  last_upstream_frame_unix_secs: number | null;
  last_decoded_event_unix_secs: number | null;
  upstream_frame_idle_ms: number | null;
  upstream_frame_idle_health: FlowIdleHealth;
  upstream_frame_idle_warn_after_ms: number;
  upstream_frame_idle_critical_after_ms: number;
  upstream_event_idle_ms: number | null;
  upstream_event_idle_health: FlowIdleHealth;
  upstream_event_idle_warn_after_ms: number;
  upstream_event_idle_critical_after_ms: number;
  upstream_frames_received: number;
  upstream_events_decoded: number;
  upstream_invalid_tick_rows: number;
  lifetime_invalid_rows: number;
  recent_invalid_rows_1m: number;
  current_decode_health: DecodeHealth;
  last_invalid_row_unix_secs: number | null;
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
  initializing: number;
  stale: number;
  missing: number;
  inactive: number;
  subscribed: number;
  problem: number;
  subscribed_problem: number;
  universe_total: number;
  universe_observed: number;
  active_invalid_rows: number;
  // Raw TQ diff row-id diagnostics; not confirmed market-data integrity failures.
  gap_event_count: number;
  estimated_missing_rows: number;
  duplicate_rows: number;
  out_of_order_rows: number;
  p95_receive_gap_ms: number | null;
};

export type SymbolRow = {
  symbol: string;
  instrument_name: string | null;
  status: SymbolStatus;
  coverage: SymbolCoverage;
  session: SymbolSession;
  flow: SymbolFlow;
  integrity: SymbolIntegrity;
  problem: boolean;
  problem_severity: ProblemSeverity;
  in_universe: boolean;
  subscribed: boolean;
  quote_subscriber_count: number;
  chart_subscriber_count: number;
  ticks_ingested: number;
  source_epoch: number;
  // Raw TQ diff row-id diagnostics. Diff patches may skip, repeat, or backfill row ids.
  last_tick_id: number | null;
  gap_event_count: number;
  estimated_missing_rows: number;
  duplicate_rows: number;
  out_of_order_rows: number;
  last_gap_unix_millis: number | null;
  receive_gap_ms: number | null;
  avg_receive_gap_ms: number | null;
  market_time_lag_ms: number | null;
  last_receive_unix_millis: number | null;
  last_tick_datetime_ns: number | null;

  invalid_rows: number;
  last_invalid_row_error: string | null;
};

export type SymbolMetricsSnapshot = {
  now_unix_millis: number;
  data_stale_after_millis: number;
  summary: SymbolMetricsSummary;
  filtered_total: number;
  symbols: SymbolRow[];
};

export type RelayEventKind =
  | 'universe_refreshed'
  | 'universe_refresh_failed'
  | 'flow_incident'
  | 'decode_incident';

export type RelayEvent = {
  sequence: number;
  at_unix_secs: number;
  kind: RelayEventKind;
  detail: string;
};

export type TimelineSeverity = 'live' | 'closed' | 'warn' | 'bad' | 'unknown' | 'no_sample';

export type DashboardTimelineScope = {
  severity: TimelineSeverity;
  total: number;
  problem: number;
  receive_gap_ms: number | null;
  avg_receive_gap_ms: number | null;
};

export type DashboardTimelineSample = {
  global: DashboardTimelineScope;
  subscribed: DashboardTimelineScope;
  exchanges: Record<string, DashboardTimelineScope>;
};

export type DashboardTimelineHistorySample = {
  sampled_at_unix_millis: number;
  sample: DashboardTimelineSample;
  symbols: Record<string, TimelineSymbolSample>;
};

export type DashboardTimelineHistory = {
  samples: DashboardTimelineHistorySample[];
};

export type DashboardSnapshot = {
  received_at_unix_millis: number;
  metrics: RelayMetrics;
  global: SymbolMetricsSummary;
  timeline: DashboardTimelineSample;
  timeline_history?: DashboardTimelineHistory;
  page: SymbolMetricsSnapshot;
  events: RelayEvent[];
};

export type RelaySnapshot = {
  received_at_unix_millis: number;
  metrics: RelayMetrics;
  global: SymbolMetricsSummary;
  timeline: DashboardTimelineSample;
  timeline_history?: DashboardTimelineHistory;
  page: SymbolMetricsSnapshot;
  events: RelayEvent[];
  receivedAt: number;
  timelineHistory?: TimelineHistory;
};

export type IntegrityModel = {
  overall: OverallSeverity;
  isMarketClosed: boolean;
  sampledAt: number;
  metrics: RelayMetrics;
  snapshot: SymbolMetricsSnapshot;
  global: SymbolMetricsSummary;
  rows: SymbolRow[];
  globalRows: SymbolRow[];
  problems: SymbolRow[];
  globalProblems: SymbolRow[];
  subscribedProblems: SymbolRow[];
  issueCount: number;
  subscribedProblemCount: number;
  invalidRowCount: number;
  activeInvalidRowCount: number;
  confirmedIntegrityIssueCount: number;
  diffRowDiscontinuityCount: number;
  outOfOrderRowCount: number;
  estimatedMissingRows: number;
  upstreamIdleMs: number | null;
  eventIdleMs: number | null;
  frameFlowHealth: FlowIdleHealth;
  eventFlowHealth: FlowIdleHealth;
  decodeHealth: DecodeHealth;
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

export type TimelineSample = {
  sampledAt: number;
  sample: DashboardTimelineSample;
  symbols: Record<string, TimelineSymbolSample>;
};

export type TimelineSymbolSample = {
  severity: TimelineSeverity;
  receive_gap_ms: number | null;
  avg_receive_gap_ms: number | null;
};

export type TimelineHistory = {
  samples: TimelineSample[];
};

export type LocalIncident = {
  id: string;
  at: number;
  scope: string;
  scope_symbol: string;
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
