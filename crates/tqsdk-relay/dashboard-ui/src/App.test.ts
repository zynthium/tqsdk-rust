import { render, screen, waitFor } from '@testing-library/svelte';
import { afterEach, describe, expect, it, vi } from 'vitest';
import App from './App.svelte';
import { dashboardSnapshot, row } from './test/fixtures';

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('App', () => {
  it('renders snapshot-driven relay dashboard without direct DOM mutation helpers', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: string | URL | Request) => {
        const path = typeof input === 'string' ? input : input instanceof URL ? input.pathname : input.url;
        if (path.includes('/dashboard-snapshot')) {
          return Response.json(
            dashboardSnapshot([
              row({ symbol: 'SHFE.au2602', instrument_name: '沪金2602' }),
              row({
                symbol: 'DCE.m2609',
                instrument_name: '豆粕2609',
                status: 'stale',
                problem: true,
                problem_severity: 'warn',
              }),
            ]),
          );
        }
        return Response.json({ error: 'not found' }, { status: 404 });
      }),
    );

    render(App);

    expect(await screen.findByText('tqsdk-relay 行情完整性监控中心')).toBeTruthy();
    await waitFor(() => expect(screen.getByText('当前关注 · 问题合约')).toBeTruthy());
    expect(screen.getAllByText(/豆粕2609/).length).toBeGreaterThan(0);
    expect(screen.getByText('无样本')).toBeTruthy();
    expect(screen.queryByText('DCE.m2609')).toBeNull();
    expect(screen.getAllByText('连续性').length).toBeGreaterThanOrEqual(1);
    expect(screen.queryByText('活跃合约健康排行')).toBeNull();
    expect(screen.getByText('完整性趋势')).toBeTruthy();
  });

  it('does not present diff row id diagnostics as tick errors', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: string | URL | Request) => {
        const path = typeof input === 'string' ? input : input instanceof URL ? input.pathname : input.url;
        if (path.includes('/dashboard-snapshot')) {
          return Response.json(
            dashboardSnapshot([
              row({
                gap_event_count: 2,
                duplicate_rows: 3,
                out_of_order_rows: 1_532,
                estimated_missing_rows: 44,
              }),
            ]),
          );
        }
        return Response.json({ error: 'not found' }, { status: 404 });
      }),
    );

    render(App);

    expect(await screen.findByText('行情链路连续')).toBeTruthy();
    expect(screen.queryByText('Tick乱序')).toBeNull();
    expect(screen.queryByText('Tick缺口')).toBeNull();
    expect(screen.queryByText('Tick异常')).toBeNull();
    expect(screen.queryByText('近期坏行')).toBeNull();
    expect(screen.queryByText(/缺口/)).toBeNull();
  });
});
