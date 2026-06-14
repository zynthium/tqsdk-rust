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

  it('keeps startup and backfill cards neutral while contracts initialize', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: string | URL | Request) => {
        const path = typeof input === 'string' ? input : input instanceof URL ? input.pathname : input.url;
        if (path.includes('/dashboard-snapshot')) {
          return Response.json(
            dashboardSnapshot(
              [
                row({
                  status: 'initializing',
                  flow: 'no_sample',
                  problem: false,
                  problem_severity: 'initializing',
                  receive_gap_ms: null,
                  avg_receive_gap_ms: null,
                  market_time_lag_ms: null,
                  last_receive_unix_millis: null,
                  last_tick_datetime_ns: null,
                }),
              ],
              {
                upstream_stage: 'backfilling',
                upstream_frame_idle_ms: 9_000,
                upstream_frame_idle_health: 'critical',
                upstream_event_idle_ms: null,
                upstream_event_idle_health: 'no_sample',
                upstream_frames_received: 0,
                upstream_events_decoded: 0,
              },
            ),
          );
        }
        return Response.json({ error: 'not found' }, { status: 404 });
      }),
    );

    render(App);

    expect(await screen.findByText('启动观测中')).toBeTruthy();
    expect(screen.getAllByText('补历史').length).toBeGreaterThanOrEqual(2);
    expect(screen.getAllByText('初始化 1').length).toBeGreaterThanOrEqual(2);
    expect(screen.getByText('观测')).toBeTruthy();
    expect(screen.queryByText('链路异常')).toBeNull();
    expect(screen.queryByText('需关注')).toBeNull();
  });

  it('keeps live-stage initialization cards neutral until all contracts have samples', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: string | URL | Request) => {
        const path = typeof input === 'string' ? input : input instanceof URL ? input.pathname : input.url;
        if (path.includes('/dashboard-snapshot')) {
          return Response.json(
            dashboardSnapshot(
              [
                row({ symbol: 'SHFE.au2602', instrument_name: '沪金2602' }),
                row({
                  symbol: 'DCE.m2609',
                  instrument_name: '豆粕2609',
                  status: 'initializing',
                  flow: 'no_sample',
                  problem: false,
                  problem_severity: 'initializing',
                  receive_gap_ms: null,
                  avg_receive_gap_ms: null,
                  market_time_lag_ms: null,
                  last_receive_unix_millis: null,
                  last_tick_datetime_ns: null,
                }),
              ],
              {
                upstream_stage: 'live',
                upstream_frame_idle_ms: 9_000,
                upstream_frame_idle_health: 'critical',
                upstream_event_idle_ms: 9_000,
                upstream_event_idle_health: 'critical',
              },
            ),
          );
        }
        return Response.json({ error: 'not found' }, { status: 404 });
      }),
    );

    render(App);

    expect(await screen.findByText('启动观测中')).toBeTruthy();
    expect(screen.getAllByText('初始化 1').length).toBeGreaterThanOrEqual(2);
    expect(screen.getAllByText('补历史').length).toBeGreaterThanOrEqual(2);
    expect(screen.queryByText('行情静默预警')).toBeNull();
    expect(screen.queryByText('链路异常')).toBeNull();
    expect(screen.queryByText('需关注')).toBeNull();
  });

  it('shows placeholders instead of delay values while the market is closed', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: string | URL | Request) => {
        const path = typeof input === 'string' ? input : input instanceof URL ? input.pathname : input.url;
        if (path.includes('/dashboard-snapshot')) {
          return Response.json(
            dashboardSnapshot(
              [
                row({
                  symbol: 'SHFE.au2602',
                  instrument_name: '沪金2602',
                  status: 'closed',
                  session: 'closed',
                  flow: 'no_sample',
                  problem: false,
                  problem_severity: 'closed',
                  receive_gap_ms: null,
                  avg_receive_gap_ms: null,
                  market_time_lag_ms: null,
                }),
                row({
                  symbol: 'DCE.m2609',
                  instrument_name: '豆粕2609',
                  status: 'closed',
                  session: 'closed',
                  flow: 'no_sample',
                  problem: false,
                  problem_severity: 'closed',
                  receive_gap_ms: null,
                  avg_receive_gap_ms: null,
                  market_time_lag_ms: null,
                }),
              ],
              {
                upstream_stage: 'live',
                upstream_frame_idle_ms: 9_000,
                upstream_frame_idle_health: 'critical',
                upstream_event_idle_ms: 9_000,
                upstream_event_idle_health: 'critical',
              },
            ),
          );
        }
        return Response.json({ error: 'not found' }, { status: 404 });
      }),
    );

    render(App);

    expect(await screen.findByText('全市场休盘')).toBeTruthy();
    expect(screen.getByText('帧 -- / 事件 --')).toBeTruthy();
    expect(screen.getAllByText('⌁ --').length).toBeGreaterThanOrEqual(3);
    expect(screen.queryByText('行情静默预警')).toBeNull();
    expect(screen.queryByText('链路异常')).toBeNull();
    expect(screen.queryByText('9.0s')).toBeNull();
  });
});
