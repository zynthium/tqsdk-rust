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

  it('detects global market closure and suppresses upstream idle alerts', () => {
    const closedRow1 = row({ status: 'closed', problem: false, problem_severity: 'closed' });
    const closedRow2 = row({ status: 'closed', problem: false, problem_severity: 'closed' });

    const model = deriveIntegrity(
      metrics({
        upstream_event_idle_health: 'critical',
      }),
      symbolSnapshot([closedRow1, closedRow2]),
      NOW,
    );

    expect(model.isMarketClosed).toBe(true);
    expect(model.overall).toBe('closed');
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
        upstream_frame_idle_ms: null,
        upstream_frame_idle_health: 'no_sample',
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

  it('keeps global health critical when the visible page is filtered to healthy rows', () => {
    const healthy = row({ symbol: 'SHFE.au2602', problem: false, problem_severity: 'live' });
    const subscribedInactive = row({
      symbol: 'CZCE.AP610',
      status: 'inactive',
      problem: true,
      problem_severity: 'bad',
      in_universe: false,
      subscribed: true,
      quote_subscriber_count: 1,
      last_receive_unix_millis: null,
      receive_gap_ms: null,
    });
    const globalSnapshot = symbolSnapshot([healthy, subscribedInactive]);
    const visiblePage = symbolSnapshot([healthy]);

    const model = deriveIntegrity(
      metrics(),
      visiblePage,
      NOW,
      null,
      globalSnapshot.summary,
    );

    expect(model.rows.map((item) => item.symbol)).toEqual(['SHFE.au2602']);
    expect(model.globalProblems).toHaveLength(0);
    expect(model.issueCount).toBe(1);
    expect(model.subscribedProblemCount).toBe(1);
    expect(model.overall).toBe('critical');
  });

  it('does not count every invalid row as a separate issue', () => {
    const bad = row({
      problem: true,
      problem_severity: 'bad',
      invalid_rows: 100,
      last_invalid_row_error: 'decode failed',
    });

    const model = deriveIntegrity(metrics(), symbolSnapshot([bad]), NOW);

    expect(model.issueCount).toBe(1);
    expect(model.activeInvalidRowCount).toBe(100);
  });

  it('treats diff row id skips as diagnostics instead of confirmed integrity failures', () => {
    const gapped = row({
      gap_event_count: 1,
      estimated_missing_rows: 2,
      last_gap_unix_millis: NOW - 1_000,
    });

    const model = deriveIntegrity(metrics(), symbolSnapshot([gapped]), NOW);

    expect(model.diffRowDiscontinuityCount).toBe(1);
    expect(model.estimatedMissingRows).toBe(2);
    expect(model.overall).toBe('healthy');
    expect(model.continuityScore).toBe(100);
  });

  it('keeps out-of-order tick rows as diff diagnostics without warning', () => {
    const lateRows = row({
      out_of_order_rows: 1_532,
    });

    const model = deriveIntegrity(metrics(), symbolSnapshot([lateRows]), NOW);

    expect(model.diffRowDiscontinuityCount).toBe(1_532);
    expect(model.outOfOrderRowCount).toBe(1_532);
    expect(model.estimatedMissingRows).toBe(0);
    expect(model.overall).toBe('healthy');
  });

  it('counts diff row discontinuities without making the relay critical', () => {
    const mixed = row({
      gap_event_count: 2,
      duplicate_rows: 3,
      out_of_order_rows: 5,
      estimated_missing_rows: 44,
    });

    const model = deriveIntegrity(metrics(), symbolSnapshot([mixed]), NOW);

    expect(model.diffRowDiscontinuityCount).toBe(10);
    expect(model.outOfOrderRowCount).toBe(5);
    expect(model.estimatedMissingRows).toBe(44);
    expect(model.overall).toBe('healthy');
  });

  it('uses global frame-flow thresholds instead of the 30s symbol stale threshold', () => {
    const model = deriveIntegrity(
      metrics({
        upstream_frame_idle_ms: 5_001,
        upstream_frame_idle_health: 'critical',
        upstream_event_idle_ms: 1_000,
        upstream_event_idle_health: 'live',
      }),
      symbolSnapshot([row({ receive_gap_ms: 5_001, problem: false, problem_severity: 'live' })]),
      NOW,
    );

    expect(model.upstreamIdleMs).toBe(5_001);
    expect(model.frameFlowHealth).toBe('critical');
    expect(model.overall).toBe('critical');
  });

  it('allows decode health to recover while keeping lifetime invalid row count', () => {
    const model = deriveIntegrity(
      metrics({
        upstream_invalid_tick_rows: 7,
        lifetime_invalid_rows: 7,
        recent_invalid_rows_1m: 0,
        current_decode_health: 'healthy',
      }),
      symbolSnapshot([row()]),
      NOW,
    );

    expect(model.invalidRowCount).toBe(7);
    expect(model.decodeHealth).toBe('healthy');
    expect(model.overall).toBe('healthy');
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
