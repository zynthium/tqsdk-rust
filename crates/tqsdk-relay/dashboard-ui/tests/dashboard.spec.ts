import { expect, test } from '@playwright/test';
import { dashboardSnapshot, row } from '../src/test/fixtures';

test('dashboard renders relay integrity view from intercepted snapshots', async ({ page }) => {
  await page.route('**/dashboard-snapshot*', async (route) => {
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
  await expect(page.getByTestId('dashboard-controls')).toHaveCount(0);
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
  const timeline = page.getByTestId('continuity-timeline');
  await expect(timeline.getByPlaceholder('搜索合约或中文名')).toBeVisible();
  const dceRow = timeline.getByRole('button', { name: /DCE.*1\/1/ });
  await expect(dceRow).toBeVisible();
  await dceRow.click();
  await expect(timeline.getByText('豆粕2609')).toBeVisible();
  await expect(timeline.getByText(/Tick/)).toBeVisible();
  await expect(page.getByText('完整性趋势')).toBeVisible();
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

  await page.route('**/dashboard-snapshot*', async (route) => {
    await route.fulfill({
      json: dashboardSnapshot(rows, { upstream_frames_received: 20, upstream_events_decoded: 40 }),
    });
  });

  await page.goto('/dashboard/');
  await expect(page.getByTestId('attention-list')).toBeVisible();
  await expect(page.getByTestId('symbol-health-table')).toHaveCount(0);
  const timeline = page.getByTestId('continuity-timeline');
  const dceRow = timeline.getByRole('button', { name: /DCE.*40\/120/ });
  await expect(dceRow).toBeVisible();
  await dceRow.click();
  await expect(timeline.getByTestId('timeline-symbol-row')).toHaveCount(rows.length);

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

  const attentionList = page.getByTestId('attention-list').locator('.list-panel-list');
  const panelMetrics = await attentionList.evaluate((element) => ({
    clientHeight: element.clientHeight,
    scrollHeight: element.scrollHeight,
    overflowY: getComputedStyle(element).overflowY,
  }));
  expect(panelMetrics.overflowY).toBe('auto');
  expect(panelMetrics.scrollHeight).toBeGreaterThan(panelMetrics.clientHeight);

  const scrollTop = await attentionList.evaluate((element) => {
    element.scrollTop = 120;
    return element.scrollTop;
  });
  expect(scrollTop).toBeGreaterThan(0);

  const sticky = await page.getByTestId('attention-list').evaluate((element) => {
    const panelTop = element.getBoundingClientRect().top;
    const cardHeader = element.querySelector('.panel-title')?.getBoundingClientRect();
    return {
      cardHeaderTop: cardHeader ? Math.abs(cardHeader.top - panelTop) : Number.NaN,
    };
  });
  expect(sticky.cardHeaderTop).toBeLessThanOrEqual(12);
});

test('continuity timeline renders aggregate exchange rows and expands page symbols', async ({ page }) => {
  await page.route('**/dashboard-snapshot*', async (route) => {
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
  const timeline = page.getByTestId('continuity-timeline');
  const dceRow = timeline.getByRole('button', { name: /DCE.*1\/2/ });
  await expect(dceRow).toBeVisible();
  await dceRow.click();
  await expect(timeline.getByText('豆粕2609')).toBeVisible();
  await expect(timeline.getByText('铁矿2609')).toBeVisible();
  await timeline.getByPlaceholder('搜索合约或中文名').fill('铁矿');
  await expect(timeline.getByText('豆粕2609')).toHaveCount(0);
  await expect(timeline.getByText('铁矿2609')).toBeVisible();
  const labelColumnWidth = await timeline.locator('.timeline').evaluate((element) => {
    const firstColumn = getComputedStyle(element).gridTemplateColumns.split(' ')[0];
    return Number.parseFloat(firstColumn);
  });
  expect(labelColumnWidth).toBeGreaterThanOrEqual(112);
  await expect(timeline.getByText('DCE.m2609')).toHaveCount(0);
  await expect(timeline.getByText('DCE.i2609')).toHaveCount(0);
});

test('continuity timeline keeps panel filters and view mode after reload', async ({ page }) => {
  await page.route('**/dashboard-snapshot*', async (route) => {
    await route.fulfill({
      json: dashboardSnapshot(
        [
          row({
            symbol: 'DCE.m2609',
            instrument_name: '豆粕2609',
            session: 'open',
            status: 'stale',
            problem: true,
            problem_severity: 'warn',
            receive_gap_ms: 90_000,
          }),
          row({
            symbol: 'DCE.i2609',
            instrument_name: '铁矿2609',
            session: 'open',
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
  const timeline = page.getByTestId('continuity-timeline');
  await timeline.getByPlaceholder('搜索合约或中文名').fill('铁矿');
  await timeline.getByLabel('只看开盘中品种').check();
  await timeline.getByLabel('不分交易所').check();
  await timeline.getByRole('button', { name: 'Sparkline' }).click();

  await page.reload();

  await expect(timeline.getByPlaceholder('搜索合约或中文名')).toHaveValue('铁矿');
  await expect(timeline.getByLabel('只看开盘中品种')).toBeChecked();
  await expect(timeline.getByLabel('不分交易所')).toBeChecked();
  await expect(timeline.getByRole('button', { name: 'Sparkline' })).toHaveClass(/active/);
  await expect(timeline.getByText('铁矿2609')).toBeVisible();
  await expect(timeline.getByRole('button', { name: /DCE/ })).toHaveCount(0);
  await expect(timeline.getByRole('button', { name: /CZCE/ })).toHaveCount(0);
});
