import { fireEvent, render } from '@testing-library/svelte';
import { beforeEach, describe, expect, it } from 'vitest';
import ContinuityTimeline from './ContinuityTimeline.svelte';
import type { TimelineSample } from '../lib/types';
import { NOW, row } from '../test/fixtures';

describe('ContinuityTimeline', () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it('leaves closed-session heatmap cells uncolored', async () => {
    const sample: TimelineSample = {
      sampledAt: NOW,
      sample: {
        global: { severity: 'closed', total: 1, problem: 0, receive_gap_ms: null, avg_receive_gap_ms: null },
        subscribed: { severity: 'unknown', total: 0, problem: 0, receive_gap_ms: null, avg_receive_gap_ms: null },
        exchanges: {
          SHFE: { severity: 'closed', total: 1, problem: 0, receive_gap_ms: null, avg_receive_gap_ms: null },
        },
      },
      symbols: {
        'SHFE.au2602': { severity: 'closed', receive_gap_ms: null, avg_receive_gap_ms: null },
      },
    };

    const view = render(ContinuityTimeline, {
      buckets: [sample],
      rows: [
        row({
          symbol: 'SHFE.au2602',
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
    });

    await fireEvent.click(view.getByRole('button', { name: /SHFE/ }));
    const closedCells = view.baseElement.querySelectorAll('.cell.unknown');
    const symbolRow = view.getByTestId('timeline-symbol-row');

    expect(closedCells.length).toBeGreaterThanOrEqual(2);
    expect(view.baseElement.querySelector('.cell.closed')).toBeNull();
    expect(symbolRow.getAttribute('aria-label')).toContain('--');
    expect(view.getAllByText('⌁ --').length).toBeGreaterThanOrEqual(3);

    const cells = view.baseElement.querySelectorAll('.cell');
    await fireEvent.mouseMove(cells[cells.length - 1], { clientX: 20, clientY: 20 });

    expect(view.getAllByText('--')).toHaveLength(3);
    expect(view.queryByText('0ms')).toBeNull();
  });

  it('uses closed-session color for missing buckets when the latest exchange sample is closed', () => {
    const sample: TimelineSample = {
      sampledAt: NOW,
      sample: {
        global: { severity: 'closed', total: 1, problem: 0, receive_gap_ms: null, avg_receive_gap_ms: null },
        subscribed: { severity: 'unknown', total: 0, problem: 0, receive_gap_ms: null, avg_receive_gap_ms: null },
        exchanges: {
          SHFE: { severity: 'closed', total: 1, problem: 0, receive_gap_ms: null, avg_receive_gap_ms: null },
        },
      },
      symbols: {
        'SHFE.al2608': { severity: 'closed', receive_gap_ms: null, avg_receive_gap_ms: null },
      },
    };
    const view = render(ContinuityTimeline, {
      buckets: [sample, null],
      rows: [
        row({
          symbol: 'SHFE.al2608',
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

  it('filters expanded symbol rows with panel-local search', async () => {
    const sample: TimelineSample = {
      sampledAt: NOW,
      sample: {
        global: { severity: 'warn', total: 2, problem: 1, receive_gap_ms: 31_000, avg_receive_gap_ms: 24_000 },
        subscribed: { severity: 'unknown', total: 0, problem: 0, receive_gap_ms: null, avg_receive_gap_ms: null },
        exchanges: {
          DCE: { severity: 'warn', total: 2, problem: 1, receive_gap_ms: 31_000, avg_receive_gap_ms: 24_000 },
        },
      },
      symbols: {
        'DCE.m2609': { severity: 'warn', receive_gap_ms: 31_000, avg_receive_gap_ms: 24_000 },
        'DCE.i2609': { severity: 'live', receive_gap_ms: 900, avg_receive_gap_ms: 900 },
      },
    };
    const view = render(ContinuityTimeline, {
      buckets: [sample],
      rows: [
        row({
          symbol: 'DCE.m2609',
          instrument_name: '豆粕2609',
          status: 'stale',
          problem: true,
          problem_severity: 'warn',
          receive_gap_ms: 31_000,
        }),
        row({ symbol: 'DCE.i2609', instrument_name: '铁矿2609' }),
      ],
    });

    await fireEvent.click(view.getByRole('button', { name: /DCE/ }));
    expect(view.getByText('豆粕2609')).toBeTruthy();
    expect(view.getByText('铁矿2609')).toBeTruthy();

    await fireEvent.input(view.getByPlaceholderText('搜索合约或中文名'), { target: { value: '铁矿' } });

    expect(view.queryByText('豆粕2609')).toBeNull();
    expect(view.getByText('铁矿2609')).toBeTruthy();
  });

  it('keeps expanded exchange symbols in stable contract-name order by product', async () => {
    const sample: TimelineSample = {
      sampledAt: NOW,
      sample: {
        global: { severity: 'warn', total: 4, problem: 1, receive_gap_ms: 60_000, avg_receive_gap_ms: 16_000 },
        subscribed: { severity: 'unknown', total: 0, problem: 0, receive_gap_ms: null, avg_receive_gap_ms: null },
        exchanges: {
          DCE: { severity: 'warn', total: 4, problem: 1, receive_gap_ms: 60_000, avg_receive_gap_ms: 16_000 },
        },
      },
      symbols: {
        'DCE.i2609': { severity: 'warn', receive_gap_ms: 60_000, avg_receive_gap_ms: 60_000 },
        'KQ.i@DCE.m': { severity: 'live', receive_gap_ms: 900, avg_receive_gap_ms: 900 },
        'DCE.m2609': { severity: 'live', receive_gap_ms: 800, avg_receive_gap_ms: 800 },
        'KQ.m@DCE.m': { severity: 'live', receive_gap_ms: 700, avg_receive_gap_ms: 700 },
      },
    };
    const view = render(ContinuityTimeline, {
      buckets: [sample],
      rows: [
        row({
          symbol: 'DCE.i2609',
          instrument_name: '铁矿2609',
          status: 'stale',
          problem: true,
          problem_severity: 'warn',
          receive_gap_ms: 60_000,
        }),
        row({ symbol: 'KQ.i@DCE.m', instrument_name: '豆粕加权' }),
        row({ symbol: 'DCE.m2609', instrument_name: '豆粕2609' }),
        row({ symbol: 'KQ.m@DCE.m', instrument_name: '豆粕主连' }),
      ],
    });

    await fireEvent.click(view.getByRole('button', { name: /DCE/ }));

    const symbolRows = view.getAllByTestId('timeline-symbol-row').map((item) => item.textContent ?? '');
    expect(symbolRows).toHaveLength(4);
    expect(symbolRows[0]).toContain('豆粕2609');
    expect(symbolRows[1]).toContain('豆粕加权');
    expect(symbolRows[2]).toContain('豆粕主连');
    expect(symbolRows[3]).toContain('铁矿2609');
  });

  it('shows symbols directly when exchange grouping is disabled', async () => {
    const sample: TimelineSample = {
      sampledAt: NOW,
      sample: {
        global: { severity: 'warn', total: 2, problem: 1, receive_gap_ms: 31_000, avg_receive_gap_ms: 24_000 },
        subscribed: { severity: 'unknown', total: 0, problem: 0, receive_gap_ms: null, avg_receive_gap_ms: null },
        exchanges: {
          DCE: { severity: 'warn', total: 1, problem: 1, receive_gap_ms: 31_000, avg_receive_gap_ms: 24_000 },
          SHFE: { severity: 'live', total: 1, problem: 0, receive_gap_ms: 900, avg_receive_gap_ms: 900 },
        },
      },
      symbols: {
        'DCE.m2609': { severity: 'warn', receive_gap_ms: 31_000, avg_receive_gap_ms: 24_000 },
        'SHFE.au2602': { severity: 'live', receive_gap_ms: 900, avg_receive_gap_ms: 900 },
      },
    };
    const view = render(ContinuityTimeline, {
      buckets: [sample],
      rows: [
        row({
          symbol: 'DCE.m2609',
          instrument_name: '豆粕2609',
          status: 'stale',
          problem: true,
          problem_severity: 'warn',
          receive_gap_ms: 31_000,
        }),
        row({ symbol: 'SHFE.au2602', instrument_name: '沪金2602' }),
      ],
    });

    await fireEvent.click(view.getByLabelText('不分交易所'));

    expect(view.queryByRole('button', { name: /DCE/ })).toBeNull();
    expect(view.queryByRole('button', { name: /SHFE/ })).toBeNull();
    expect(view.getByText('豆粕2609')).toBeTruthy();
    expect(view.getByText('沪金2602')).toBeTruthy();
  });

  it('keeps flat symbols in stable contract-name order', async () => {
    const sample: TimelineSample = {
      sampledAt: NOW,
      sample: {
        global: { severity: 'warn', total: 3, problem: 1, receive_gap_ms: 60_000, avg_receive_gap_ms: 16_000 },
        subscribed: { severity: 'unknown', total: 0, problem: 0, receive_gap_ms: null, avg_receive_gap_ms: null },
        exchanges: {
          DCE: { severity: 'warn', total: 2, problem: 1, receive_gap_ms: 60_000, avg_receive_gap_ms: 16_000 },
          SHFE: { severity: 'live', total: 1, problem: 0, receive_gap_ms: 800, avg_receive_gap_ms: 800 },
        },
      },
      symbols: {
        'DCE.i2609': { severity: 'warn', receive_gap_ms: 60_000, avg_receive_gap_ms: 60_000 },
        'DCE.m2609': { severity: 'live', receive_gap_ms: 800, avg_receive_gap_ms: 800 },
        'SHFE.au2602': { severity: 'live', receive_gap_ms: 700, avg_receive_gap_ms: 700 },
      },
    };
    const view = render(ContinuityTimeline, {
      buckets: [sample],
      rows: [
        row({
          symbol: 'DCE.i2609',
          instrument_name: '铁矿2609',
          status: 'stale',
          problem: true,
          problem_severity: 'warn',
          receive_gap_ms: 60_000,
        }),
        row({ symbol: 'SHFE.au2602', instrument_name: '沪金2602' }),
        row({ symbol: 'DCE.m2609', instrument_name: '豆粕2609' }),
      ],
    });

    await fireEvent.click(view.getByLabelText('不分交易所'));

    const symbolRows = view.getAllByTestId('timeline-symbol-row').map((item) => item.textContent ?? '');
    expect(symbolRows).toHaveLength(3);
    expect(symbolRows[0]).toContain('豆粕2609');
    expect(symbolRows[1]).toContain('沪金2602');
    expect(symbolRows[2]).toContain('铁矿2609');
  });

  it('shows every visible symbol when exchange grouping is disabled', async () => {
    const rows = Array.from({ length: 35 }, (_, index) =>
      row({
        symbol: `DCE.m${2600 + index}`,
        instrument_name: `豆粕${2600 + index}`,
      }),
    );
    const symbols: TimelineSample['symbols'] = {};
    for (const symbolRow of rows) {
      symbols[symbolRow.symbol] = {
        severity: 'live',
        receive_gap_ms: 900,
        avg_receive_gap_ms: 900,
      };
    }
    const sample: TimelineSample = {
      sampledAt: NOW,
      sample: {
        global: { severity: 'live', total: rows.length, problem: 0, receive_gap_ms: 900, avg_receive_gap_ms: 900 },
        subscribed: { severity: 'unknown', total: 0, problem: 0, receive_gap_ms: null, avg_receive_gap_ms: null },
        exchanges: {
          DCE: { severity: 'live', total: rows.length, problem: 0, receive_gap_ms: 900, avg_receive_gap_ms: 900 },
        },
      },
      symbols,
    };
    const view = render(ContinuityTimeline, {
      buckets: [sample],
      rows,
    });

    await fireEvent.click(view.getByLabelText('不分交易所'));

    expect(view.queryByRole('button', { name: /DCE/ })).toBeNull();
    expect(view.getAllByTestId('timeline-symbol-row')).toHaveLength(rows.length);
  });

  it('keeps open-session filter, search, and view mode across remounts', async () => {
    const sample: TimelineSample = {
      sampledAt: NOW,
      sample: {
        global: { severity: 'warn', total: 3, problem: 1, receive_gap_ms: 31_000, avg_receive_gap_ms: 24_000 },
        subscribed: { severity: 'unknown', total: 0, problem: 0, receive_gap_ms: null, avg_receive_gap_ms: null },
        exchanges: {
          DCE: { severity: 'warn', total: 2, problem: 1, receive_gap_ms: 31_000, avg_receive_gap_ms: 24_000 },
          CZCE: { severity: 'closed', total: 1, problem: 0, receive_gap_ms: null, avg_receive_gap_ms: null },
        },
      },
      symbols: {
        'DCE.m2609': { severity: 'warn', receive_gap_ms: 31_000, avg_receive_gap_ms: 24_000 },
        'DCE.i2609': { severity: 'live', receive_gap_ms: 900, avg_receive_gap_ms: 900 },
        'CZCE.AP610': { severity: 'closed', receive_gap_ms: null, avg_receive_gap_ms: null },
      },
    };
    const props = {
      buckets: [sample],
      rows: [
        row({
          symbol: 'DCE.m2609',
          instrument_name: '豆粕2609',
          status: 'stale',
          session: 'open',
          problem: true,
          problem_severity: 'warn',
          receive_gap_ms: 31_000,
        }),
        row({
          symbol: 'DCE.i2609',
          instrument_name: '铁矿2609',
          session: 'open',
        }),
        row({
          symbol: 'CZCE.AP610',
          instrument_name: '苹果610',
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
    };

    const first = render(ContinuityTimeline, props);
    await fireEvent.click(first.getByLabelText('只看开盘中品种'));
    await fireEvent.click(first.getByLabelText('不分交易所'));
    await fireEvent.input(first.getByPlaceholderText('搜索合约或中文名'), { target: { value: '铁矿' } });
    await fireEvent.click(first.getByRole('button', { name: 'Sparkline' }));
    first.unmount();

    const second = render(ContinuityTimeline, props);
    expect((second.getByLabelText('只看开盘中品种') as HTMLInputElement).checked).toBe(true);
    expect((second.getByLabelText('不分交易所') as HTMLInputElement).checked).toBe(true);
    expect((second.getByPlaceholderText('搜索合约或中文名') as HTMLInputElement).value).toBe('铁矿');
    expect(second.getByRole('button', { name: 'Sparkline' }).classList.contains('active')).toBe(true);

    expect(second.queryByRole('button', { name: /DCE/ })).toBeNull();
    expect(second.queryByRole('button', { name: /CZCE/ })).toBeNull();
    expect(second.queryByText('豆粕2609')).toBeNull();
    expect(second.getByText('铁矿2609')).toBeTruthy();
  });
});
