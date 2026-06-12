import { describe, expect, it, vi } from 'vitest';
import { dashboardSnapshot, row } from '../test/fixtures';
import { fetchRelaySnapshot } from './api';

describe('fetchRelaySnapshot', () => {
  it('fetches one atomic dashboard snapshot endpoint', async () => {
    const fetch = vi.fn(async (_input: string | URL | Request) => Response.json(dashboardSnapshot([row()])));
    vi.stubGlobal('fetch', fetch);

    const snapshot = await fetchRelaySnapshot();

    expect(fetch).toHaveBeenCalledTimes(1);
    expect(String(fetch.mock.calls[0][0])).toBe('/dashboard-snapshot');
    expect(snapshot.global.total).toBe(1);
    expect(snapshot.page.symbols).toHaveLength(1);
  });
});
