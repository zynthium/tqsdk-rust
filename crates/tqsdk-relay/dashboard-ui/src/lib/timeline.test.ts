import { describe, expect, it } from 'vitest';
import { createTimelineHistory, exchangeOf, pushTimelineSample, timelineBuckets } from './timeline';
import { dashboardTimeline, NOW, row } from '../test/fixtures';

describe('timeline', () => {
  it('groups continuous contracts by underlying exchange', () => {
    expect(exchangeOf('KQ.i@DCE.m')).toBe('DCE');
    expect(exchangeOf('KQ.m@SHFE.au')).toBe('SHFE');
    expect(exchangeOf('DCE.m2609')).toBe('DCE');
  });

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

  it('keeps initializing symbol history neutral even after live stage starts', () => {
    const history = createTimelineHistory();
    const rows = [
      row({ symbol: 'SHFE.au2602' }),
      row({
        symbol: 'DCE.m2609',
        status: 'initializing',
        problem: false,
        problem_severity: 'initializing',
        flow: 'no_sample',
        integrity: 'suspected',
        session: 'unknown',
        receive_gap_ms: null,
        avg_receive_gap_ms: null,
        market_time_lag_ms: null,
        last_receive_unix_millis: null,
        last_tick_datetime_ns: null,
      }),
    ];
    const sample = dashboardTimeline(rows);

    pushTimelineSample(history, sample, NOW, rows);
    const buckets = timelineBuckets(history, NOW + 1, 60);

    expect(buckets.at(-1)?.sample.global.problem).toBe(0);
    expect(buckets.at(-1)?.symbols['DCE.m2609'].severity).toBe('no_sample');
  });

  it('includes a sample at the exact right edge of the latest bucket', () => {
    const history = createTimelineHistory();
    const sample = dashboardTimeline([row({ symbol: 'DCE.m2609' })]);

    pushTimelineSample(history, sample, NOW);
    const buckets = timelineBuckets(history, NOW, 60);

    expect(buckets.at(-1)?.sample.exchanges.DCE.severity).toBe('live');
  });
});
