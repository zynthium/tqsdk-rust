import type {
  IntegrityModel,
  ProblemSeverity,
  RelayMetrics,
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
  if (metrics.last_upstream_frame_unix_secs == null) return null;
  return Math.max(0, nowMillis - metrics.last_upstream_frame_unix_secs * 1_000);
}

export function deriveIntegrity(
  metrics: RelayMetrics,
  snapshot: SymbolMetricsSnapshot,
  sampledAt: number,
  previous?: IntegrityModel | null,
): IntegrityModel {
  const rows = Array.isArray(snapshot.symbols) ? snapshot.symbols : [];
  const universeRows = rows.filter((row) => row.in_universe);
  const observedUniverse = universeRows.filter((row) => row.last_receive_unix_millis != null).length;
  const totalUniverse = universeRows.length || Number(metrics.upstream_symbols || snapshot.summary.total || 0);
  const coverageRatio = totalUniverse > 0 ? observedUniverse / totalUniverse : 0;
  const problems = rows.filter((row) => row.problem);
  const subscribedProblems = problems.filter((row) => row.subscribed);
  const invalidRowCount = Number(metrics.upstream_invalid_tick_rows || 0);
  const activeInvalidRowCount = rows.reduce(
    (sum, row) => sum + (row.problem ? Number(row.invalid_rows || 0) : 0),
    0,
  );
  const upstreamIdleMs = frameIdleMs(metrics, sampledAt);
  const staleAfterMs = Number(snapshot.data_stale_after_millis || metrics.data_stale_after_secs * 1_000 || 30_000);
  const sourceCritical = metrics.upstream_stage === 'down' || metrics.upstream_stage === 'degraded';
  const idleCritical = upstreamIdleMs != null && upstreamIdleMs > staleAfterMs;
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
  const issueCount = problems.length + activeInvalidRowCount;
  const continuityScore = Math.max(
    0,
    100 -
      Math.min(55, issueCount * 9) -
      Math.min(25, (1 - coverageRatio) * 25) -
      (sourceCritical || idleCritical ? 20 : 0),
  );
  const overall =
    sourceCritical || idleCritical || subscribedProblems.length > 0
      ? 'critical'
      : warming && rows.length === 0
        ? 'warming'
        : issueCount > 0 || coverageRatio < 0.98
          ? 'warning'
          : 'healthy';

  return {
    overall,
    sampledAt,
    metrics,
    snapshot,
    rows,
    problems,
    subscribedProblems,
    issueCount,
    invalidRowCount,
    activeInvalidRowCount,
    upstreamIdleMs,
    coverageRatio,
    observedUniverse,
    totalUniverse,
    frameRate,
    eventRate,
    continuityScore,
  };
}
