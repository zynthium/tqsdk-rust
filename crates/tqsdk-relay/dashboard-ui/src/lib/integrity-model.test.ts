import { describe, expect, it } from 'vitest';
import { deriveIntegrity, severityForRow, statusLabel } from './integrity-model';
import { metrics, NOW, row, symbolSnapshot } from '../test/fixtures';

describe('deriveIntegrity', () => {
  it('keeps closed rows out of active problem count', () => {
    const closed = row({
      status: 'closed',
      problem: false,
      problem_severity: 'closed',
      invalid_rows: 7,
      receive_gap_ms: 90_000,
      market_time_lag_ms: 90_000,
    });
    const stale = row({
      symbol: 'DCE.m2609',
      instrument_name: '豆粕2609',
      status: 'stale',
      problem: true,
      problem_severity: 'warn',
      receive_gap_ms: 90_000,
    });

    const model = deriveIntegrity(metrics(), symbolSnapshot([closed, stale]), NOW);

    expect(severityForRow(closed)).toBe('closed');
    expect(model.problems.map((item) => item.symbol)).toEqual(['DCE.m2609']);
    expect(model.issueCount).toBe(1);
    expect(model.subscribedProblems).toHaveLength(0);
  });

  it('treats subscribed inactive rows as critical operational problems', () => {
    const inactive = row({
      symbol: 'CZCE.AP610',
      instrument_name: '苹果610',
      status: 'inactive',
      problem: true,
      problem_severity: 'bad',
      in_universe: false,
      subscribed: true,
      quote_subscriber_count: 1,
      receive_gap_ms: null,
      market_time_lag_ms: null,
      last_receive_unix_millis: null,
      last_tick_datetime_ns: null,
    });

    const model = deriveIntegrity(metrics(), symbolSnapshot([inactive]), NOW);

    expect(model.overall).toBe('critical');
    expect(model.subscribedProblems.map((item) => item.symbol)).toEqual(['CZCE.AP610']);
    expect(model.coverageRatio).toBe(0);
  });

  it('exposes startup warming state without false critical alarm', () => {
    const model = deriveIntegrity(
      metrics({
        upstream_stage: 'backfilling',
        last_upstream_frame_unix_secs: null,
        upstream_frames_received: 0,
        upstream_events_decoded: 0,
      }),
      symbolSnapshot([]),
      NOW,
    );

    expect(model.overall).toBe('warming');
    expect(model.upstreamIdleMs).toBeNull();
    expect(model.issueCount).toBe(0);
  });

  it('computes rates from previous sample', () => {
    const previous = deriveIntegrity(
      metrics({ upstream_frames_received: 10, upstream_events_decoded: 20 }),
      symbolSnapshot([row()]),
      NOW,
    );
    const next = deriveIntegrity(
      metrics({ upstream_frames_received: 14, upstream_events_decoded: 30 }),
      symbolSnapshot([row()]),
      NOW + 2_000,
      previous,
    );

    expect(next.frameRate).toBe(2);
    expect(next.eventRate).toBe(5);
  });
});

describe('statusLabel', () => {
  it('maps backend status to Chinese labels', () => {
    expect(statusLabel('live')).toBe('正常');
    expect(statusLabel('closed')).toBe('休盘');
    expect(statusLabel('stale')).toBe('静默');
    expect(statusLabel('missing')).toBe('未收到');
    expect(statusLabel('inactive')).toBe('未纳入');
  });
});
