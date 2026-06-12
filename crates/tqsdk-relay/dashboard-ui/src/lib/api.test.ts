import { describe, expect, it, vi } from 'vitest';
import { dashboardSnapshot, row } from '../test/fixtures';
import { fetchRelaySnapshot, symbolQueryString } from './api';

describe('symbolQueryString', () => {
  it('encodes dashboard filters using existing relay query contract', () => {
    expect(
      symbolQueryString({
        statuses: ['live', 'stale'],
        sessions: [],
        subscribedOnly: true,
        q: '沪金 2602',
        sort: 'receive_gap_ms_desc',
        limit: 200,
      }),
    ).toBe(
      'status=live%2Cstale&subscribed=1&q=%E6%B2%AA%E9%87%91+2602&sort=receive_gap_ms_desc&limit=200',
    );
  });

  it('omits empty optional filters', () => {
    expect(
      symbolQueryString({
        statuses: [],
        sessions: [],
        subscribedOnly: false,
        q: '',
        sort: 'symbol_asc',
        limit: 200,
      }),
    ).toBe('sort=symbol_asc&limit=200');
  });

  it('fetches one atomic dashboard snapshot endpoint', async () => {
    const fetch = vi.fn(async (_input: string | URL | Request) => Response.json(dashboardSnapshot([row()])));
    vi.stubGlobal('fetch', fetch);

    const snapshot = await fetchRelaySnapshot({
      statuses: [],
      sessions: [],
      subscribedOnly: false,
      q: '',
      sort: 'symbol_asc',
      limit: 200,
    });

    expect(fetch).toHaveBeenCalledTimes(1);
    expect(String(fetch.mock.calls[0][0])).toBe('/dashboard-snapshot?sort=symbol_asc&limit=200');
    expect(snapshot.global.total).toBe(1);
    expect(snapshot.page.symbols).toHaveLength(1);
  });
});
