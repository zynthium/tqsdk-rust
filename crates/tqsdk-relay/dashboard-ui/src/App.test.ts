import { render, screen, waitFor } from '@testing-library/svelte';
import { describe, expect, it, vi } from 'vitest';
import App from './App.svelte';
import { dashboardSnapshot, row } from './test/fixtures';

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
    expect(screen.getByText('未知')).toBeTruthy();
    expect(screen.getByText('无样本')).toBeTruthy();
    expect(screen.queryByText('DCE.m2609')).toBeNull();
    expect(screen.getByText('连续性评分')).toBeTruthy();
    expect(screen.getByText('活跃合约健康排行')).toBeTruthy();
    expect(screen.queryByText('完整性趋势')).toBeNull();
  });
});
