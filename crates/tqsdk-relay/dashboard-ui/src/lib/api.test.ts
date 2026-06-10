import { describe, expect, it } from 'vitest';
import { symbolQueryString } from './api';

describe('symbolQueryString', () => {
  it('encodes dashboard filters using existing relay query contract', () => {
    expect(
      symbolQueryString({
        statuses: ['live', 'stale'],
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
        subscribedOnly: false,
        q: '',
        sort: 'symbol_asc',
        limit: 200,
      }),
    ).toBe('sort=symbol_asc&limit=200');
  });
});
