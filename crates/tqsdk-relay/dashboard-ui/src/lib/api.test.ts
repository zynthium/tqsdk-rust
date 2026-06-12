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
    expect(snapshot.timelineHistory).toBeUndefined();
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
