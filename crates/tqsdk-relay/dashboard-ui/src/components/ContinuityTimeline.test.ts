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

    const closedCells = view.baseElement.querySelectorAll('.cell.closed_unmarked');

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

    const closedCells = view.baseElement.querySelectorAll('.cell.closed_unmarked');

    expect(closedCells.length).toBeGreaterThanOrEqual(2);
  });
});
