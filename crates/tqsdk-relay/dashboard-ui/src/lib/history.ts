import type { IntegrityModel, RuntimeHistory } from './types';

export function createHistory(limit = 150): RuntimeHistory {
  return { limit, samples: [] };
}

export function pushHistorySample(history: RuntimeHistory, model: IntegrityModel): RuntimeHistory {
  history.samples.push({
    sampledAt: model.sampledAt,
    frameRate: model.frameRate,
    eventRate: model.eventRate,
    coverageRatio: model.coverageRatio,
    issueCount: model.issueCount,
    upstreamIdleMs: model.upstreamIdleMs,
    continuityScore: model.continuityScore,
  });
  if (history.samples.length > history.limit) {
    history.samples.splice(0, history.samples.length - history.limit);
  }
  return history;
}

export function sparkPoints(values: Array<number | null>, width = 160, height = 20): string {
  const valid = values.filter((value): value is number => value != null && Number.isFinite(value));
  if (valid.length === 0) return '';
  const min = Math.min(...valid);
  const max = Math.max(...valid);
  const range = Math.max(0.0001, max - min);
  return values
    .map((value, index) => {
      const safe = value == null || !Number.isFinite(value) ? min : value;
      const x = values.length === 1 ? 0 : (index / (values.length - 1)) * width;
      const y = height - ((safe - min) / range) * height;
      return `${Math.round(x)},${Math.round(y)}`;
    })
    .join(' ');
}
