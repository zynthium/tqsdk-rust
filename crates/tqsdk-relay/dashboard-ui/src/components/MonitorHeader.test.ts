import { render, screen } from '@testing-library/svelte';
import { describe, expect, it } from 'vitest';
import MonitorHeader from './MonitorHeader.svelte';
import { deriveIntegrity } from '../lib/integrity-model';
import { NOW, dashboardSnapshot, row, symbolSnapshot } from '../test/fixtures';

describe('MonitorHeader', () => {
  it('renders header controls with utility-first layout classes', () => {
    const rows = [
      row({
        subscribed: true,
      }),
    ];
    const snapshot = dashboardSnapshot(rows);
    const model = deriveIntegrity(snapshot.metrics, symbolSnapshot(rows), NOW, null, snapshot.global);

    render(MonitorHeader, {
      model,
      error: '上游连接异常',
      paused: false,
      fullscreen: false,
    });

    const header = screen.getByTestId('monitor-header');
    expect(header.className).toContain('grid-cols-[1fr_auto_1fr]');
    expect(header.className).toContain('min-h-11');
    expect(header.className).toContain('items-center');

    expect(screen.getByText('tqsdk-relay 行情完整性监控中心')).toBeTruthy();
    expect(screen.getByRole('button', { name: '暂停' })).toBeTruthy();
    expect(screen.getByRole('button', { name: '全屏' })).toBeTruthy();

    const banner = screen.getByText('上游连接异常');
    expect(banner.className).toContain('fixed');
    expect(banner.className).toContain('top-14');
    expect(banner.className).toContain('border-[color:color-mix(in_srgb,var(--relay-bad)_70%,transparent)]');
  });
});
