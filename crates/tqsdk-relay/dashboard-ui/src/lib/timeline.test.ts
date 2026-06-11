import { describe, expect, it } from 'vitest';
import { createTimelineHistory, pushTimelineSample, timelineBuckets } from './timeline';
import { deriveIntegrity } from './integrity-model';
import { metrics, NOW, row, symbolSnapshot } from '../test/fixtures';

describe('timeline', () => {
  it('records exchange and subscribed continuity without treating closed as bad', () => {
    const history = createTimelineHistory();
    const model = deriveIntegrity(
      metrics(),
      symbolSnapshot([
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
      ]),
      NOW,
    );

    pushTimelineSample(history, model);
    const buckets = timelineBuckets(history, NOW + 1, 60);

    expect(buckets.at(-1)?.exchangeSeverity.SHFE).toBe('closed');
    expect(buckets.at(-1)?.exchangeSeverity.DCE).toBe('warn');
    expect(buckets.at(-1)?.subscribedSeverity).toBe('warn');
    expect(buckets.at(-1)?.symbolSeverity['DCE.m2609']).toBe('warn');
  });

  it('distinguishes missing bucket samples from unknown and no-sample symbol states', () => {
    const history = createTimelineHistory();
    const model = deriveIntegrity(
      metrics(),
      symbolSnapshot([
        row({
          symbol: 'DCE.m2609',
          status: 'missing',
          problem: true,
          problem_severity: 'bad',
          session: 'unknown',
          flow: 'no_sample',
          integrity: 'suspected',
        }),
      ]),
      NOW,
    );

    pushTimelineSample(history, model);
    const buckets = timelineBuckets(history, NOW + 1, 60);

    expect(buckets[0]).toBeNull();
    expect(buckets.at(-1)?.exchangeSeverity.DCE).toBe('bad');
    expect(buckets.at(-1)?.exchangeSeverity.SHFE).toBe('unknown');
    expect(buckets.at(-1)?.symbolSeverity['DCE.m2609']).toBe('bad');
  });
});
