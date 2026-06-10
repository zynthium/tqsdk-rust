import type { DashboardFilters, RelayMetrics, SymbolMetricsSnapshot } from './types';

export class DashboardApiError extends Error {
  constructor(
    public readonly path: string,
    public readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

export function symbolQueryString(filters: DashboardFilters): string {
  const params = new URLSearchParams();
  if (filters.statuses.length > 0) params.set('status', filters.statuses.join(','));
  if (filters.subscribedOnly) params.set('subscribed', '1');
  if (filters.q.trim()) params.set('q', filters.q.trim());
  params.set('sort', filters.sort);
  params.set('limit', String(filters.limit));
  return params.toString();
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

export async function fetchRelaySnapshot(filters: DashboardFilters, signal?: AbortSignal) {
  const query = symbolQueryString(filters);
  const [metrics, symbols] = await Promise.all([
    fetchJson<RelayMetrics>('/metrics', signal),
    fetchJson<SymbolMetricsSnapshot>(`/symbol-metrics?${query}`, signal),
  ]);
  return { metrics, symbols, receivedAt: Date.now() };
}
