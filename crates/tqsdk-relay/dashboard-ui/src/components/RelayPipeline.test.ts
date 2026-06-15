import { render, screen } from '@testing-library/svelte';
import { describe, expect, it } from 'vitest';
import RelayPipeline from './RelayPipeline.svelte';
import { deriveIntegrity } from '../lib/integrity-model';
import { NOW, dashboardSnapshot, row, symbolSnapshot } from '../test/fixtures';

function buildModel() {
  const rows = [
    row({
      symbol: 'DCE.m2609',
      instrument_name: '豆粕2609',
      status: 'missing',
      problem: true,
      problem_severity: 'bad',
      subscribed: true,
      quote_subscriber_count: 2,
      chart_subscriber_count: 1,
      receive_gap_ms: 45_000,
      avg_receive_gap_ms: 24_000,
      market_time_lag_ms: 52_000,
    }),
  ];
  const snapshot = dashboardSnapshot(rows, {
    upstream_stage: 'degraded',
    downstream_clients: 3,
    upstream_frames_received: 42,
    recent_invalid_rows_1m: 2,
    current_decode_health: 'degraded',
  });
  return deriveIntegrity(snapshot.metrics, symbolSnapshot(rows), NOW, null, snapshot.global);
}

describe('RelayPipeline', () => {
  it('renders utility-first shell and explicit severity classes', () => {
    const model = buildModel();

    render(RelayPipeline, { model });

    const panel = screen.getByTestId('relay-pipeline');
    expect(panel.className).toContain('panel-shell');
    expect(panel.className).toContain('grid');
    expect(panel.className).toContain('min-h-[68px]');

    const sourceNode = screen.getByText('上游连接').parentElement?.parentElement as HTMLElement;
    expect(sourceNode.className).toContain('grid-cols-[30px_1fr_8px]');
    expect(sourceNode.className).toContain('border-[#ff536a66]');

    const state = screen.getByText('降级');
    expect(state.className).toContain('text-[color:var(--relay-bad)]');

    const dot = sourceNode.lastElementChild as HTMLElement;
    expect(dot.className).toContain('bg-[color:var(--relay-bad)]');
  });
});
