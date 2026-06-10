import { expect, test } from '@playwright/test';
import { metrics, row, symbolSnapshot } from '../src/test/fixtures';

test('dashboard renders relay integrity view from intercepted snapshots', async ({ page }) => {
  await page.route('**/metrics', async (route) => {
    await route.fulfill({ json: metrics({ upstream_frames_received: 20, upstream_events_decoded: 40 }) });
  });
  await page.route('**/symbol-metrics?*', async (route) => {
    await route.fulfill({
      json: symbolSnapshot([
        row({ symbol: 'SHFE.au2602', instrument_name: '沪金2602', subscribed: true, quote_subscriber_count: 1 }),
        row({
          symbol: 'DCE.m2609',
          instrument_name: '豆粕2609',
          status: 'stale',
          problem: true,
          problem_severity: 'warn',
          receive_gap_ms: 90_000,
        }),
        row({
          symbol: 'CZCE.AP610',
          instrument_name: '苹果610',
          status: 'closed',
          problem: false,
          problem_severity: 'closed',
        }),
      ]),
    });
  });

  await page.goto('/dashboard/');

  await expect(page.getByText('tqsdk-relay 行情完整性监控中心')).toBeVisible();
  await expect(page.getByRole('cell', { name: '沪金2602' })).toBeVisible();
  await expect(page.getByRole('cell', { name: '豆粕2609' })).toBeVisible();
  await expect(page.getByTestId('continuity-timeline')).toBeVisible();
  await expect(page.getByTestId('score-gauge')).toBeVisible();
});
