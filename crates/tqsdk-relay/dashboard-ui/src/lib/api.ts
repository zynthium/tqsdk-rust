import type { DashboardSnapshot, DashboardTimelineHistory, RelaySnapshot, TimelineHistory } from './types';

export type FetchRelaySnapshotOptions = {
  includeTimelineHistory?: boolean;
};

export class DashboardApiError extends Error {
  constructor(
    public readonly path: string,
    public readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

async function fetchJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(path, { cache: 'no-store', signal });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message =
      typeof body === 'object' && body != null && 'error' in body && typeof body.error === 'string'
        ? body.error
        : `HTTP ${response.status}`;
    throw new DashboardApiError(path, response.status, message);
  }
  return body as T;
}

export async function fetchRelaySnapshot(
  signal?: AbortSignal,
  options: FetchRelaySnapshotOptions = {},
) {
  const path = options.includeTimelineHistory
    ? '/dashboard-snapshot?timeline_history=1'
    : '/dashboard-snapshot';
  const snapshot = await fetchJson<DashboardSnapshot>(path, signal);
  const relaySnapshot: RelaySnapshot = {
    ...snapshot,
    receivedAt: snapshot.received_at_unix_millis || Date.now(),
  };
  if (snapshot.timeline_history) {
    relaySnapshot.timelineHistory = normalizeTimelineHistory(snapshot.timeline_history);
  }
  return relaySnapshot;
}

function normalizeTimelineHistory(history: DashboardTimelineHistory): TimelineHistory {
  return {
    samples: history.samples.map((sample) => ({
      sampledAt: sample.sampled_at_unix_millis,
      sample: sample.sample,
      symbols: sample.symbols,
    })),
  };
}
