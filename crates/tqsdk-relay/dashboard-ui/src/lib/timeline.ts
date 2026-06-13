import type { DashboardTimelineSample, SymbolRow, TimelineHistory, TimelineSample, TimelineSeverity } from './types';

export const EXCHANGES = ['SHFE', 'DCE', 'CZCE', 'INE', 'GFEX', 'CFFEX'];

export function createTimelineHistory(): TimelineHistory {
  return { samples: [] };
}

export function exchangeOf(symbol: string): string {
  const normalized = symbol.includes('@') ? symbol.split('@')[1] : symbol;
  return normalized?.split('.')[0]?.toUpperCase() || 'UNKNOWN';
}

function timelineSeverityForRow(row: SymbolRow): TimelineSeverity {
  if (row.session === 'closed') return 'closed';
  if (row.problem_severity === 'bad' || row.integrity === 'confirmed_gap') return 'bad';
  if (row.problem_severity === 'warn' || row.integrity === 'suspected' || row.flow === 'silent') return 'warn';
  if (row.flow === 'no_sample') return 'no_sample';
  if (row.session === 'unknown') return 'unknown';
  return 'live';
}

export function pushTimelineSample(
  history: TimelineHistory,
  sample: DashboardTimelineSample,
  sampledAt: number,
  rows: SymbolRow[] = [],
): TimelineHistory {
  history.samples.push({
    sampledAt,
    sample,
    symbols: Object.fromEntries(
      rows.map((row) => [
        row.symbol,
        {
          severity: timelineSeverityForRow(row),
          receive_gap_ms: row.receive_gap_ms,
          avg_receive_gap_ms: row.avg_receive_gap_ms,
        },
      ]),
    ),
  });
  history.samples = history.samples.filter((item) => item.sampledAt >= sampledAt - 300_000);
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
      const insideBucket =
        sample.sampledAt >= start &&
        (sample.sampledAt < end || (index === bucketCount - 1 && sample.sampledAt <= end));
      if (insideBucket) return sample;
    }
    return null;
  });
}
