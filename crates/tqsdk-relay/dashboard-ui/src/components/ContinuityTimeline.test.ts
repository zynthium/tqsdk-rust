import { render } from '@testing-library/svelte';
import { describe, expect, it } from 'vitest';
import ContinuityTimeline from './ContinuityTimeline.svelte';
import type { TimelineSample } from '../lib/types';
import { NOW, row } from '../test/fixtures';

describe('ContinuityTimeline', () => {
  it('leaves closed-session heatmap cells uncolored', () => {
    const sample: TimelineSample = {
      sampledAt: NOW,
      exchangeSeverity: { SHFE: 'closed' },
      symbolSeverity: { 'SHFE.au2602': 'closed' },
      subscribedSeverity: 'unknown',
      globalSeverity: 'live',
    };

    const root = render(ContinuityTimeline, {
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
    }).baseElement;

    const closedCell = root.querySelector<HTMLElement>('.cell.closed_unmarked');

    expect(closedCell).not.toBeNull();
    expect(root.querySelector('.cell.closed')).toBeNull();
  });
});
