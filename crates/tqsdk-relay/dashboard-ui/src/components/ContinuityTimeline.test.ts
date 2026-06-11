import { fireEvent, render } from '@testing-library/svelte';
import { describe, expect, it } from 'vitest';
import ContinuityTimeline from './ContinuityTimeline.svelte';
import type { TimelineSample } from '../lib/types';
import { NOW, row } from '../test/fixtures';

describe('ContinuityTimeline', () => {
  it('leaves closed-session heatmap cells uncolored', async () => {
    const sample: TimelineSample = {
      sampledAt: NOW,
      exchangeSeverity: { SHFE: 'closed' },
      symbolSeverity: { 'SHFE.au2602': 'closed' },
      subscribedSeverity: 'unknown',
      globalSeverity: 'live',
      exchangeLatency: {},
      symbolLatency: {},
      subscribedLatency: 0,
      globalLatency: 0,
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

  it('uses closed-session color for missing buckets when the current symbol is closed', async () => {
    const view = render(ContinuityTimeline, {
      buckets: [null],
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

    await fireEvent.click(view.getByRole('button', { name: /SHFE/ }));

    const closedCells = view.baseElement.querySelectorAll('.cell.unknown');

    expect(closedCells.length).toBeGreaterThanOrEqual(2);
  });

  it('shows symbol health details inside expanded heatmap rows', async () => {
    const sample: TimelineSample = {
      sampledAt: NOW,
      exchangeSeverity: { SHFE: 'warn' },
      symbolSeverity: { 'SHFE.au2602': 'warn' },
      subscribedSeverity: 'warn',
      globalSeverity: 'warn',
      exchangeLatency: {},
      symbolLatency: {},
      subscribedLatency: 0,
      globalLatency: 0,
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
          market_time_lag_ms: 45_000,
          ticks_ingested: 1_234,
          quote_subscriber_count: 1,
          chart_subscriber_count: 2,
          invalid_rows: 7,
        }),
      ],
    });

    await fireEvent.click(view.getByRole('button', { name: /SHFE/ }));
    const cells = view.baseElement.querySelectorAll('.cell');
    await fireEvent.mouseMove(cells[cells.length - 1]);

    expect(view.getAllByText('静默').length).toBeGreaterThanOrEqual(1);
    expect(view.getByText('31.0s')).toBeTruthy();
    expect(view.getByText('45.0s')).toBeTruthy();
    expect(view.getByText('Tick 1,234')).toBeTruthy();
    expect(view.getByText('订阅 3')).toBeTruthy();
    expect(view.getByText('warn')).toBeTruthy();
    expect(view.getByText(/异常记录数/)).toBeTruthy();
    expect(view.getByText('7')).toBeTruthy();
  });
});
