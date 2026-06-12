import { describe, expect, it } from 'vitest';
import { createTimelineHistory, pushTimelineSample, timelineBuckets } from './timeline';
import { dashboardTimeline, NOW, row } from '../test/fixtures';

describe('timeline', () => {
  it('records exchange and subscribed continuity without treating closed as bad', () => {
    const history = createTimelineHistory();
    const sample = dashboardTimeline([
      row({
        symbol: 'SHFE.au2602',
        status: 'closed',
        session: 'closed',
        problem: false,
        problem_severity: 'closed',
      }),
      row({
        symbol: 'DCE.m2609',
        status: 'stale',
        problem: true,
        problem_severity: 'warn',
        subscribed: true,
      }),
    ]);

    pushTimelineSample(history, sample, NOW);
    const buckets = timelineBuckets(history, NOW + 1, 60);

    expect(buckets.at(-1)?.sample.exchanges.SHFE.severity).toBe('closed');
    expect(buckets.at(-1)?.sample.exchanges.DCE.severity).toBe('warn');
    expect(buckets.at(-1)?.sample.subscribed.severity).toBe('warn');
    expect(buckets.at(-1)?.sample.global.problem).toBe(1);
  });

  it('distinguishes missing bucket samples from unknown and no-sample symbol states', () => {
    const history = createTimelineHistory();
    const sample = dashboardTimeline([
      row({
        symbol: 'DCE.m2609',
        status: 'missing',
        problem: true,
        problem_severity: 'bad',
        session: 'unknown',
        flow: 'no_sample',
        integrity: 'suspected',
      }),
    ]);

    pushTimelineSample(history, sample, NOW);
    const buckets = timelineBuckets(history, NOW + 1, 60);

    expect(buckets[0]).toBeNull();
    expect(buckets.at(-1)?.sample.exchanges.DCE.severity).toBe('bad');
    expect(buckets.at(-1)?.sample.exchanges.SHFE).toBeUndefined();
  });

  it('lets closed sessions override stale flow coloring', () => {
    const history = createTimelineHistory();
    const sample = dashboardTimeline([
      row({
        symbol: 'SHFE.al2608',
        status: 'closed',
        session: 'closed',
        flow: 'silent',
        integrity: 'suspected',
        problem: false,
        problem_severity: 'closed',
      }),
    ]);

    pushTimelineSample(history, sample, NOW);
    const buckets = timelineBuckets(history, NOW + 1, 60);

    expect(buckets.at(-1)?.sample.exchanges.SHFE.severity).toBe('closed');
    expect(buckets.at(-1)?.sample.global.severity).toBe('closed');
  });
});
