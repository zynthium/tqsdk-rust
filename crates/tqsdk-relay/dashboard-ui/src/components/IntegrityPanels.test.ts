import { render, screen } from '@testing-library/svelte';
import { describe, expect, it } from 'vitest';
import IntegrityHero from './IntegrityHero.svelte';
import IntegrityTrend from './IntegrityTrend.svelte';
import { deriveIntegrity } from '../lib/integrity-model';
import type { RuntimeHistory } from '../lib/types';
import { NOW, dashboardSnapshot, row, symbolSnapshot } from '../test/fixtures';

function buildModel() {
  const rows = [
    row({
      symbol: 'DCE.m2609',
      instrument_name: '豆粕2609',
      status: 'stale',
      problem: true,
      problem_severity: 'warn',
      receive_gap_ms: 31_000,
      avg_receive_gap_ms: 24_000,
      market_time_lag_ms: 45_000,
    }),
  ];
  const snapshot = dashboardSnapshot(rows);
  return deriveIntegrity(snapshot.metrics, symbolSnapshot(rows), NOW, null, snapshot.global);
}

function buildHistory(model = buildModel()): RuntimeHistory {
  return {
    limit: 60,
    samples: [
      {
        sampledAt: NOW - 2_000,
        frameRate: 4,
        eventRate: 7,
        coverageRatio: model.coverageRatio,
        issueCount: 0,
        upstreamIdleMs: 800,
        continuityScore: 98,
      },
      {
        sampledAt: NOW,
        frameRate: model.frameRate,
        eventRate: model.eventRate,
        coverageRatio: model.coverageRatio,
        issueCount: model.issueCount,
        upstreamIdleMs: model.upstreamIdleMs,
        continuityScore: model.continuityScore,
      },
    ],
  };
}

describe('Integrity panels', () => {
  it('renders IntegrityHero with utility-first shell and chips', () => {
    const model = buildModel();

    render(IntegrityHero, { model });

    const panel = screen.getByTestId('integrity-hero');
    expect(panel.className).toContain('panel-shell');
    expect(panel.className).toContain('[grid-template-columns:auto_minmax(0,1fr)_minmax(140px,180px)]');
    expect(panel.className).toContain('px-[22px]');

    const chip = screen.getByText('合约覆盖').parentElement as HTMLElement;
    expect(chip.className).toContain('rounded-[7px]');
    expect(chip.className).toContain('bg-[#071a2b99]');
  });

  it('renders IntegrityTrend with utility-first outer layout while keeping chart styles', () => {
    const model = buildModel();
    const history = buildHistory(model);

    render(IntegrityTrend, { model, history });

    const panel = screen.getByTestId('integrity-trend');
    expect(panel.className).toContain('panel-shell');
    expect(panel.className).toContain('px-3');

    const chart = screen.getByLabelText('integrity trend').parentElement as HTMLElement;
    expect(chart.className).toContain('border-l');
    expect(chart.className).toContain('border-b');
    expect(chart.className).toContain('relative');
  });
});
