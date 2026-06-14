import { describe, expect, it, vi } from 'vitest';
import { dashboardSnapshot, row } from '../test/fixtures';
import { fetchRelaySnapshot } from './api';

describe('fetchRelaySnapshot', () => {
  it('fetches one atomic dashboard snapshot endpoint', async () => {
    const response = dashboardSnapshot([
      row({
        symbol: 'SHFE.au2602',
        instrument_name: '沪金2602',
        receive_gap_ms: 900,
        avg_receive_gap_ms: 900,
        market_time_lag_ms: 1_200,
        ticks_ingested: 5,
      }),
    ]);
    response.page.symbols = [
      {
        symbol: 'SHFE.au2602',
        instrument_name: '沪金2602',
        receive_gap_ms: 900,
        avg_receive_gap_ms: 900,
        market_time_lag_ms: 1_200,
        ticks_ingested: 5,
      },
    ];
    const fetch = vi.fn(async (_input: string | URL | Request) => Response.json(response));
    vi.stubGlobal('fetch', fetch);

    const snapshot = await fetchRelaySnapshot();

    expect(fetch).toHaveBeenCalledTimes(1);
    expect(String(fetch.mock.calls[0][0])).toBe('/dashboard-snapshot');
    expect(snapshot.global.total).toBe(1);
    expect(snapshot.page.symbols).toHaveLength(1);
    expect(snapshot.page.symbols[0].status).toBe('live');
    expect(snapshot.page.symbols[0].session).toBe('open');
    expect(snapshot.page.symbols[0].problem).toBe(false);
    expect(snapshot.page.symbols[0].quote_subscriber_count).toBe(0);
    expect(snapshot.page.symbols[0].last_receive_unix_millis).toBeNull();
    expect(snapshot.timelineHistory).toBeUndefined();
  });

  it('normalizes unfiltered continuity timeline rows separately from page rows', async () => {
    const response = {
      ...dashboardSnapshot([
        row({
          symbol: 'SHFE.au2602',
          instrument_name: '沪金2602',
        }),
        row({
          symbol: 'DCE.m2609',
          instrument_name: '豆粕2609',
        }),
      ]),
      page: {
        ...dashboardSnapshot([row({ symbol: 'SHFE.au2602' })]).page,
        symbols: [{ symbol: 'SHFE.au2602' }],
      },
      timeline_symbols: [
        { symbol: 'SHFE.au2602', instrument_name: '沪金2602' },
        {
          symbol: 'DCE.m2609',
          instrument_name: '豆粕2609',
          session: 'closed',
          status: 'closed',
        },
      ],
    };
    const fetch = vi.fn(async (_input: string | URL | Request) => Response.json(response));
    vi.stubGlobal('fetch', fetch);

    const snapshot = await fetchRelaySnapshot();

    expect(snapshot.page.symbols).toHaveLength(1);
    expect(snapshot.timelineSymbols).toHaveLength(2);
    expect(snapshot.timelineSymbols[1].symbol).toBe('DCE.m2609');
    expect(snapshot.timelineSymbols[1].session).toBe('closed');
  });

  it('can request and normalize the cached continuity timeline history', async () => {
    const response = {
      ...dashboardSnapshot([row()]),
      timeline_history: {
        samples: [
          {
            sampled_at_unix_millis: 1_700_013_598_000,
            sample: dashboardSnapshot([row()]).timeline,
            symbols: {
              'SHFE.au2602': {
                severity: 'live',
                receive_gap_ms: 900,
                avg_receive_gap_ms: 900,
              },
            },
          },
        ],
      },
    };
    const fetch = vi.fn(async (_input: string | URL | Request) => Response.json(response));
    vi.stubGlobal('fetch', fetch);

    const snapshot = await fetchRelaySnapshot(undefined, { includeTimelineHistory: true });

    expect(String(fetch.mock.calls[0][0])).toBe('/dashboard-snapshot?timeline_history=1');
    expect(snapshot.timelineHistory?.samples).toHaveLength(1);
    expect(snapshot.timelineHistory?.samples[0].sampledAt).toBe(1_700_013_598_000);
    expect(snapshot.timelineHistory?.samples[0].symbols['SHFE.au2602'].severity).toBe('live');
  });
});
