import type {
  IntegrityModel,
  FlowIdleHealth,
  ProblemSeverity,
  RelayMetrics,
  SymbolMetricsSummary,
  SymbolMetricsSnapshot,
  SymbolRow,
  SymbolStatus,
} from './types';

const WARMING_STAGES = new Set<string>(['connecting', 'subscribing', 'backfilling']);

export function statusLabel(status: SymbolStatus): string {
  return {
    live: '正常',
    closed: '休盘',
    stale: '静默',
    missing: '未收到',
    inactive: '未纳入',
  }[status];
}

export function severityForRow(row: SymbolRow): ProblemSeverity {
  return row.problem_severity;
}

export function frameIdleMs(metrics: RelayMetrics, nowMillis: number): number | null {
  if (metrics.upstream_frame_idle_ms != null) return Number(metrics.upstream_frame_idle_ms);
  if (metrics.last_upstream_frame_unix_secs == null) return null;
  return Math.max(0, nowMillis - metrics.last_upstream_frame_unix_secs * 1_000);
}

function eventIdleMs(metrics: RelayMetrics, nowMillis: number): number | null {
  if (metrics.upstream_event_idle_ms != null) return Number(metrics.upstream_event_idle_ms);
  if (metrics.last_decoded_event_unix_secs == null) return null;
  return Math.max(0, nowMillis - metrics.last_decoded_event_unix_secs * 1_000);
}

function flowHealthFor(idleMs: number | null, warnAfterMs: number, criticalAfterMs: number): FlowIdleHealth {
  if (idleMs == null) return 'no_sample';
  if (idleMs > criticalAfterMs) return 'critical';
  if (idleMs > warnAfterMs) return 'warn';
  return 'live';
}

export function deriveIntegrity(
  metrics: RelayMetrics,
  snapshot: SymbolMetricsSnapshot,
  sampledAt: number,
  previous?: IntegrityModel | null,
  global: SymbolMetricsSummary = snapshot.summary,
  globalRowsInput?: SymbolRow[],
): IntegrityModel {
  const rows = Array.isArray(snapshot.symbols) ? snapshot.symbols : [];
  const globalRows = Array.isArray(globalRowsInput) ? globalRowsInput : rows;
  const visibleProblems = rows.filter((row) => row.problem);
  const globalProblems = globalRows.filter((row) => row.problem);
  const observedUniverse = Number(global.universe_observed ?? 0);
  const totalUniverse = Number(global.universe_total || metrics.upstream_symbols || global.total || 0);
  const coverageRatio = totalUniverse > 0 ? observedUniverse / totalUniverse : 0;
  const subscribedProblems = globalProblems.filter((row) => row.subscribed);
  const invalidRowCount = Number(metrics.upstream_invalid_tick_rows || 0);
  const activeInvalidRowCount = Number(
    global.active_invalid_rows || globalProblems.reduce((sum, row) => sum + Number(row.invalid_rows || 0), 0),
  );
  const gapEventCount = Number(global.gap_event_count || 0);
  const duplicateRowCount = Number(global.duplicate_rows || 0);
  const outOfOrderRowCount = Number(global.out_of_order_rows || 0);
  const confirmedIntegrityIssueCount = 0;
  const diffRowDiscontinuityCount = gapEventCount + duplicateRowCount + outOfOrderRowCount;
  const estimatedMissingRows = Number(global.estimated_missing_rows || 0);
  const upstreamIdleMs = frameIdleMs(metrics, sampledAt);
  const eventIdle = eventIdleMs(metrics, sampledAt);
  const frameFlowHealth =
    metrics.upstream_frame_idle_health ??
    flowHealthFor(
      upstreamIdleMs,
      Number(metrics.upstream_frame_idle_warn_after_ms || 2_000),
      Number(metrics.upstream_frame_idle_critical_after_ms || 5_000),
    );
  const eventFlowHealth =
    metrics.upstream_event_idle_health ??
    flowHealthFor(
      eventIdle,
      Number(metrics.upstream_event_idle_warn_after_ms || 3_000),
      Number(metrics.upstream_event_idle_critical_after_ms || 8_000),
    );
  const decodeHealth = metrics.current_decode_health ?? 'healthy';
  const sourceCritical = metrics.upstream_stage === 'down' || metrics.upstream_stage === 'degraded';
  const idleCritical = frameFlowHealth === 'critical' || eventFlowHealth === 'critical';
  const idleWarn = frameFlowHealth === 'warn' || eventFlowHealth === 'warn';
  const decodeWarn = decodeHealth === 'degraded';
  const warming = WARMING_STAGES.has(metrics.upstream_stage);
  const elapsedSeconds = previous ? Math.max(0.001, (sampledAt - previous.sampledAt) / 1_000) : null;
  const frameRate =
    elapsedSeconds && previous
      ? Math.max(0, (metrics.upstream_frames_received - previous.metrics.upstream_frames_received) / elapsedSeconds)
      : null;
  const eventRate =
    elapsedSeconds && previous
      ? Math.max(0, (metrics.upstream_events_decoded - previous.metrics.upstream_events_decoded) / elapsedSeconds)
      : null;
  const issueCount = Number(global.problem ?? globalProblems.length);
  const subscribedProblemCount = Number(global.subscribed_problem ?? subscribedProblems.length);
  const continuityScore = Math.max(
    0,
    100 -
      Math.min(55, issueCount * 9) -
      Math.min(25, (1 - coverageRatio) * 25) -
      (sourceCritical || idleCritical ? 20 : 0),
  );
  const overall =
    sourceCritical || idleCritical || subscribedProblemCount > 0 || confirmedIntegrityIssueCount > 0
      ? 'critical'
      : warming && globalRows.length === 0
        ? 'warming'
        : idleWarn || decodeWarn || issueCount > 0 || coverageRatio < 0.98
          ? 'warning'
          : 'healthy';

  return {
    overall,
    sampledAt,
    metrics,
    snapshot,
    global,
    rows,
    globalRows,
    problems: visibleProblems,
    globalProblems,
    subscribedProblems,
    issueCount,
    subscribedProblemCount,
    invalidRowCount,
    activeInvalidRowCount,
    confirmedIntegrityIssueCount,
    diffRowDiscontinuityCount,
    outOfOrderRowCount,
    estimatedMissingRows,
    upstreamIdleMs,
    eventIdleMs: eventIdle,
    frameFlowHealth,
    eventFlowHealth,
    decodeHealth,
    coverageRatio,
    observedUniverse,
    totalUniverse,
    frameRate,
    eventRate,
    continuityScore,
  };
}
