import type { IntegrityModel, SymbolRow, TimelineHistory, TimelineSample, TimelineSeverity } from './types';

export const EXCHANGES = ['SHFE', 'DCE', 'CZCE', 'INE', 'GFEX', 'CFFEX'];

export function createTimelineHistory(): TimelineHistory {
  return { samples: [] };
}

export function exchangeOf(symbol: string): string {
  return symbol.split('.')[0]?.toUpperCase() || 'UNKNOWN';
}

export function timelineSeverityForRows(rows: SymbolRow[]): TimelineSeverity {
  if (rows.length === 0) return 'unknown';
  if (rows.every((row) => row.session === 'closed')) return 'closed';
  if (rows.some((row) => row.problem_severity === 'bad')) return 'bad';
  if (rows.some((row) => row.problem_severity === 'warn')) return 'warn';
  if (rows.every((row) => row.flow === 'no_sample')) return 'no_sample';
  if (rows.every((row) => row.session === 'unknown')) return 'unknown';
  return 'live';
}

function timelineSeverityForRow(row: SymbolRow): TimelineSeverity {
  if (row.session === 'closed') return 'closed';
  if (row.problem_severity === 'bad' || row.integrity === 'confirmed_gap') return 'bad';
  if (row.problem_severity === 'warn' || row.integrity === 'suspected' || row.flow === 'silent') return 'warn';
  if (row.flow === 'no_sample') return 'no_sample';
  if (row.session === 'unknown') return 'unknown';
  return 'live';
}

export function pushTimelineSample(history: TimelineHistory, model: IntegrityModel): TimelineHistory {
  const rows = model.globalRows;
  const exchangeSeverity: Record<string, TimelineSeverity> = {};
  const symbolSeverity: Record<string, TimelineSeverity> = {};
  for (const exchange of EXCHANGES) {
    exchangeSeverity[exchange] = timelineSeverityForRows(
      rows.filter((row) => exchangeOf(row.symbol) === exchange),
    );
  }
  for (const row of rows) {
    symbolSeverity[row.symbol] = timelineSeverityForRow(row);
  }
  const subscribedRows = rows.filter((row) => row.subscribed);
  const sample: TimelineSample = {
    sampledAt: model.sampledAt,
    exchangeSeverity,
    symbolSeverity,
    subscribedSeverity: timelineSeverityForRows(subscribedRows),
    globalSeverity:
      model.overall === 'critical'
        ? 'bad'
        : model.overall === 'warning'
          ? 'warn'
          : model.overall === 'warming'
            ? 'unknown'
            : 'live',
  };
  history.samples.push(sample);
  history.samples = history.samples.filter((item) => item.sampledAt >= model.sampledAt - 300_000);
  return history;
}

export function timelineBuckets(
  history: TimelineHistory,
  now: number,
  bucketCount = 60,
): Array<TimelineSample | null> {
  const bucketMs = 300_000 / bucketCount;
  return Array.from({ length: bucketCount }, (_, index) => {
    const start = now - 300_000 + index * bucketMs;
    const end = start + bucketMs;
    for (let sampleIndex = history.samples.length - 1; sampleIndex >= 0; sampleIndex -= 1) {
      const sample = history.samples[sampleIndex];
      if (sample.sampledAt >= start && sample.sampledAt < end) return sample;
    }
    return null;
  });
}
