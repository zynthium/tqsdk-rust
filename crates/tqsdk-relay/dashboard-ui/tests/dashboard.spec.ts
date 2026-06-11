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

test('dashboard keeps document fixed and scrolls overflowing panels internally', async ({ page }) => {
  const rows = Array.from({ length: 120 }, (_, index) => {
    const symbol = `DCE.m${2600 + index}`;
    const stale = index % 3 === 0;
    return row({
      symbol,
      instrument_name: `豆粕${2600 + index}`,
      status: stale ? 'stale' : 'live',
      problem: stale,
      problem_severity: stale ? 'warn' : 'live',
      receive_gap_ms: stale ? 90_000 + index * 1_000 : 900,
      ticks_ingested: index,
    });
  });

  await page.route('**/metrics', async (route) => {
    await route.fulfill({ json: metrics({ upstream_frames_received: 20, upstream_events_decoded: 40 }) });
  });
  await page.route('**/symbol-metrics?*', async (route) => {
    await route.fulfill({ json: symbolSnapshot(rows) });
  });

  await page.goto('/dashboard/');
  await expect(page.getByTestId('symbol-health-table')).toBeVisible();

  const viewport = await page.evaluate(() => ({
    documentClientHeight: document.documentElement.clientHeight,
    documentScrollHeight: document.documentElement.scrollHeight,
    appClientHeight: document.querySelector<HTMLElement>('#app')?.clientHeight ?? 0,
    appScrollHeight: document.querySelector<HTMLElement>('#app')?.scrollHeight ?? 0,
  }));
  expect(viewport.documentScrollHeight).toBeLessThanOrEqual(viewport.documentClientHeight + 1);
  expect(viewport.appScrollHeight).toBeLessThanOrEqual(viewport.appClientHeight + 1);

  await page.mouse.wheel(0, 1200);
  await expect.poll(() => page.evaluate(() => window.scrollY)).toBe(0);

  const tablePanel = page.getByTestId('symbol-health-table');
  const panelMetrics = await tablePanel.evaluate((element) => ({
    clientHeight: element.clientHeight,
    scrollHeight: element.scrollHeight,
    overflowY: getComputedStyle(element).overflowY,
  }));
  expect(panelMetrics.overflowY).toBe('auto');
  expect(panelMetrics.scrollHeight).toBeGreaterThan(panelMetrics.clientHeight);

  const scrollTop = await tablePanel.evaluate((element) => {
    element.scrollTop = 120;
    return element.scrollTop;
  });
  expect(scrollTop).toBeGreaterThan(0);
});
