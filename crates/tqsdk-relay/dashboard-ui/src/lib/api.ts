import type { DashboardSnapshot, RelaySnapshot } from './types';

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

export async function fetchRelaySnapshot(signal?: AbortSignal) {
  const snapshot = await fetchJson<DashboardSnapshot>('/dashboard-snapshot', signal);
  return {
    ...snapshot,
    receivedAt: snapshot.received_at_unix_millis || Date.now(),
  } satisfies RelaySnapshot;
}
