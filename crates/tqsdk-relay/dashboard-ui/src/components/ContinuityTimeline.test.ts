import { fireEvent, render } from '@testing-library/svelte';
import { describe, expect, it } from 'vitest';
import ContinuityTimeline from './ContinuityTimeline.svelte';
import type { TimelineSample } from '../lib/types';
import { NOW, row } from '../test/fixtures';

describe('ContinuityTimeline', () => {
  it('leaves closed-session heatmap cells uncolored', async () => {
    const sample: TimelineSample = {
      sampledAt: NOW,
      sample: {
        global: { severity: 'live', total: 1, problem: 0, receive_gap_ms: 0, avg_receive_gap_ms: 0 },
        subscribed: { severity: 'unknown', total: 0, problem: 0, receive_gap_ms: null, avg_receive_gap_ms: null },
        exchanges: {
          SHFE: { severity: 'closed', total: 1, problem: 0, receive_gap_ms: 0, avg_receive_gap_ms: 0 },
        },
      },
      symbols: {
        'SHFE.au2602': { severity: 'closed', receive_gap_ms: 0, avg_receive_gap_ms: 0 },
      },
    };

    const view = render(ContinuityTimeline, {
      buckets: [sample],
      rows: [
        row({
          symbol: 'SHFE.au2602',
          status: 'closed',
          session: 'closed',
          problem: false,
          problem_severity: 'closed',
        }),
      ],
    });

    await fireEvent.click(view.getByRole('button', { name: /SHFE/ }));
    const closedCells = view.baseElement.querySelectorAll('.cell.unknown');

    expect(closedCells.length).toBeGreaterThanOrEqual(2);
    expect(view.baseElement.querySelector('.cell.closed')).toBeNull();
  });

  it('uses closed-session color for missing buckets when the latest exchange sample is closed', () => {
    const sample: TimelineSample = {
      sampledAt: NOW,
      sample: {
        global: { severity: 'closed', total: 1, problem: 0, receive_gap_ms: 0, avg_receive_gap_ms: 0 },
        subscribed: { severity: 'unknown', total: 0, problem: 0, receive_gap_ms: null, avg_receive_gap_ms: null },
        exchanges: {
          SHFE: { severity: 'closed', total: 1, problem: 0, receive_gap_ms: 0, avg_receive_gap_ms: 0 },
        },
      },
      symbols: {
        'SHFE.al2608': { severity: 'closed', receive_gap_ms: 0, avg_receive_gap_ms: 0 },
      },
    };
    const view = render(ContinuityTimeline, {
      buckets: [sample, null],
      rows: [
        row({
          symbol: 'SHFE.al2608',
          status: 'closed',
          session: 'closed',
          problem: false,
          problem_severity: 'closed',
        }),
      ],
    });

    const closedCells = view.baseElement.querySelectorAll('.cell.unknown');

    expect(closedCells.length).toBeGreaterThanOrEqual(2);
  });

  it('shows aggregate exchange counts inside heatmap rows', async () => {
    const sample: TimelineSample = {
      sampledAt: NOW,
      sample: {
        global: { severity: 'warn', total: 1, problem: 1, receive_gap_ms: 31_000, avg_receive_gap_ms: 24_000 },
        subscribed: { severity: 'warn', total: 1, problem: 1, receive_gap_ms: 31_000, avg_receive_gap_ms: 24_000 },
        exchanges: {
          SHFE: { severity: 'warn', total: 1, problem: 1, receive_gap_ms: 31_000, avg_receive_gap_ms: 24_000 },
        },
      },
      symbols: {
        'SHFE.au2602': { severity: 'warn', receive_gap_ms: 31_000, avg_receive_gap_ms: 24_000 },
      },
    };
    const view = render(ContinuityTimeline, {
      buckets: [sample],
      rows: [
        row({
          symbol: 'SHFE.au2602',
          instrument_name: '沪金2602',
          status: 'stale',
          session: 'open',
          problem: true,
          problem_severity: 'warn',
          receive_gap_ms: 31_000,
          avg_receive_gap_ms: 24_000,
          market_time_lag_ms: 45_000,
          ticks_ingested: 1_234,
          quote_subscriber_count: 1,
          chart_subscriber_count: 2,
          invalid_rows: 7,
        }),
      ],
    });

    await fireEvent.click(view.getByRole('button', { name: /SHFE/ }));
    expect(view.getByText('SHFE')).toBeTruthy();
    expect(view.getAllByText('1/1').length).toBeGreaterThanOrEqual(3);
    expect(view.getAllByText('⌁ 24.0s').length).toBeGreaterThanOrEqual(3);
    expect(view.getByText('沪金2602')).toBeTruthy();
    expect(view.getByText('Tick 1,234')).toBeTruthy();
    expect(view.getByText('订阅 3')).toBeTruthy();
    expect(view.getByText('warn')).toBeTruthy();
    expect(view.baseElement.querySelectorAll('.cell.warn')).toHaveLength(4);

    const cells = view.baseElement.querySelectorAll('.cell');
    await fireEvent.mouseMove(cells[cells.length - 1]);

    expect(view.getByText('31.0s')).toBeTruthy();
    expect(view.getByText('24.0s')).toBeTruthy();
    expect(view.getByText('45.0s')).toBeTruthy();
    expect(view.getByText(/异常记录数/)).toBeTruthy();
    expect(view.getByText('7')).toBeTruthy();
  });
});
