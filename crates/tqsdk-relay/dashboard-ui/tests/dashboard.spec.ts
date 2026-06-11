import { expect, test } from '@playwright/test';
import { dashboardSnapshot, row } from '../src/test/fixtures';

test('dashboard renders relay integrity view from intercepted snapshots', async ({ page }) => {
  await page.route('**/dashboard-snapshot?*', async (route) => {
    await route.fulfill({
      json: dashboardSnapshot(
        [
          row({
            symbol: 'SHFE.au2602',
            instrument_name: '沪金2602',
            subscribed: true,
            quote_subscriber_count: 1,
          }),
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
            session: 'closed',
            problem: false,
            problem_severity: 'closed',
          }),
        ],
        { upstream_frames_received: 20, upstream_events_decoded: 40 },
      ),
    });
  });

  await page.goto('/dashboard/');

  await expect(page.getByText('tqsdk-relay 行情完整性监控中心')).toBeVisible();
  await expect(page.getByTestId('integrity-hero')).toBeVisible();
  await expect(page.getByTestId('score-gauge')).toBeVisible();
  await expect(page.getByTestId('attention-list').getByText('豆粕2609')).toBeVisible();
  await expect(page.getByText('DCE.m2609')).toHaveCount(0);
  await expect(page.getByTestId('continuity-timeline')).toBeVisible();
  const pipelineTextLayout = await page.getByTestId('relay-pipeline').evaluate((element) =>
    Array.from(element.querySelectorAll<HTMLElement>('.node')).map((node) => {
      const nodeRect = node.getBoundingClientRect();
      const textRects = Array.from(node.querySelectorAll<HTMLElement>('.name, .state, .meta')).map((text) =>
        text.getBoundingClientRect(),
      );
      return {
        nodeHeight: nodeRect.height,
        textInsideNode: textRects.every((textRect) => textRect.top >= nodeRect.top && textRect.bottom <= nodeRect.bottom),
      };
    }),
  );
  expect(pipelineTextLayout.every((node) => node.nodeHeight <= 64)).toBe(true);
  expect(pipelineTextLayout.every((node) => node.textInsideNode)).toBe(true);
  await expect(page.getByText('活跃合约健康排行')).toHaveCount(0);
  const dceRow = page.getByRole('button', { name: /DCE.*1\/1/ });
  await dceRow.click();
  const timeline = page.getByTestId('continuity-timeline');
  await expect(timeline.getByText('豆粕2609')).toBeVisible();
  await expect(timeline.getByText('距 1m30s')).toBeVisible();
  await expect(timeline.getByText(/Tick/)).toBeVisible();
  await expect(page.getByText('完整性趋势')).toHaveCount(0);
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

  await page.route('**/dashboard-snapshot?*', async (route) => {
    await route.fulfill({
      json: dashboardSnapshot(rows, { upstream_frames_received: 20, upstream_events_decoded: 40 }),
    });
  });

  await page.goto('/dashboard/');
  await expect(page.getByTestId('attention-list')).toBeVisible();
  await expect(page.getByTestId('symbol-health-table')).toHaveCount(0);
  await page.getByRole('button', { name: /DCE.*40\/120/ }).click();
  await expect(page.getByTestId('continuity-timeline').getByTestId('timeline-symbol-row')).toHaveCount(30);

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

  const attentionPanel = page.getByTestId('attention-list');
  const panelMetrics = await attentionPanel.evaluate((element) => ({
    clientHeight: element.clientHeight,
    scrollHeight: element.scrollHeight,
    overflowY: getComputedStyle(element).overflowY,
  }));
  expect(panelMetrics.overflowY).toBe('auto');
  expect(panelMetrics.scrollHeight).toBeGreaterThan(panelMetrics.clientHeight);

  const scrollTop = await attentionPanel.evaluate((element) => {
    element.scrollTop = 120;
    return element.scrollTop;
  });
  expect(scrollTop).toBeGreaterThan(0);

  const sticky = await attentionPanel.evaluate((element) => {
    const panelTop = element.getBoundingClientRect().top;
    const cardHeader = element.querySelector('.panel-title')?.getBoundingClientRect();
    return {
      cardHeaderTop: cardHeader ? Math.abs(cardHeader.top - panelTop) : Number.NaN,
    };
  });
  expect(sticky.cardHeaderTop).toBeLessThanOrEqual(12);

  const tableHeader = await page.getByTestId('incident-table').locator('thead th').first().evaluate((element) => ({
    position: getComputedStyle(element).position,
    top: getComputedStyle(element).top,
  }));
  expect(tableHeader.position).toBe('sticky');
  expect(tableHeader.top).toBe('38px');
});

test('continuity timeline expands an exchange into prioritized symbol rows', async ({ page }) => {
  await page.route('**/dashboard-snapshot?*', async (route) => {
    await route.fulfill({
      json: dashboardSnapshot(
        [
          row({
            symbol: 'DCE.m2609',
            instrument_name: '豆粕2609',
            status: 'stale',
            problem: true,
            problem_severity: 'warn',
            receive_gap_ms: 90_000,
          }),
          row({
            symbol: 'DCE.i2609',
            instrument_name: '铁矿2609',
            status: 'live',
            problem: false,
            problem_severity: 'live',
            receive_gap_ms: 900,
          }),
          row({ symbol: 'SHFE.au2602', instrument_name: '沪金2602' }),
        ],
        { upstream_frames_received: 20, upstream_events_decoded: 40 },
      ),
    });
  });

  await page.goto('/dashboard/');
  const dceRow = page.getByRole('button', { name: /DCE.*1\/2/ });
  await expect(dceRow).toBeVisible();
  await dceRow.click();

  const timeline = page.getByTestId('continuity-timeline');
  await expect(timeline.getByText('豆粕2609')).toBeVisible();
  await expect(timeline.getByText('铁矿2609')).toBeVisible();
  const labelColumnWidth = await timeline.locator('.timeline').evaluate((element) => {
    const firstColumn = getComputedStyle(element).gridTemplateColumns.split(' ')[0];
    return Number.parseFloat(firstColumn);
  });
  expect(labelColumnWidth).toBeGreaterThanOrEqual(112);
  await expect(timeline.getByText('DCE.m2609')).toHaveCount(0);
  await expect(timeline.getByText('DCE.i2609')).toHaveCount(0);
});
