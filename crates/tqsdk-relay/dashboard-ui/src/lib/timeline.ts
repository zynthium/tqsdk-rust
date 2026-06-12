import type { DashboardTimelineSample, TimelineHistory, TimelineSample } from './types';

export const EXCHANGES = ['SHFE', 'DCE', 'CZCE', 'INE', 'GFEX', 'CFFEX'];

export function createTimelineHistory(): TimelineHistory {
  return { samples: [] };
}

export function exchangeOf(symbol: string): string {
  return symbol.split('.')[0]?.toUpperCase() || 'UNKNOWN';
}

export function pushTimelineSample(
  history: TimelineHistory,
  sample: DashboardTimelineSample,
  sampledAt: number,
): TimelineHistory {
  history.samples.push({ sampledAt, sample });
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
      if (sample.sampledAt >= start && sample.sampledAt < end) return sample;
    }
    return null;
  });
}
