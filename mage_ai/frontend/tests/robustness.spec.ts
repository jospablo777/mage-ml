import { APIRequestContext } from '@playwright/test';

import { MageApi, OUTPUT_LOADER, OUTPUT_TRANSFORMER } from './api';
import { expect, test } from './base';

/*
 * Regressions from the UI audit: error text shown as text, the dependency graph centered
 * in its panel, and no page wider than a 1024 px screen.
 */

const SUFFIX = Date.now().toString(36);
const LOADER = `robust_load_${SUFFIX}`;
const TRANSFORMER = `robust_transform_${SUFFIX}`;

let apiContext: APIRequestContext;
let api: MageApi;
let pipelineUUID: string;

test.beforeAll(async ({ playwright }, testInfo) => {
  apiContext = await playwright.request.newContext({
    baseURL: process.env.MAGE_E2E_API_URL || testInfo.project.use.baseURL,
  });
  api = await MageApi.signIn(apiContext);
  pipelineUUID = await api.createPipeline(`ui_robust_${SUFFIX}`);
  await api.addBlock(pipelineUUID, { content: OUTPUT_LOADER, name: LOADER, type: 'data_loader' });
  await api.addBlock(pipelineUUID, {
    content: OUTPUT_TRANSFORMER,
    name: TRANSFORMER,
    type: 'transformer',
    upstream_blocks: [LOADER],
  });
});

test.afterAll(async () => {
  await api.deletePipeline(pipelineUUID);
  await apiContext.dispose();
});

test('an error with markup in it is shown as text', async ({ page }) => {
  const payload = 'Pipeline <img src=x onerror="window.__xss=1"> already exists.';
  await page.route(/\/api\/pipelines\?/, route => route.fulfill({
    body: JSON.stringify({
      error: {
        code: 500,
        errors: [],
        exception: payload,
        message: `Traceback (most recent call last):\n  File "x.py", line 1\nException: ${payload}`,
      },
    }),
    contentType: 'application/json',
  }));
  await page.goto('/pipelines');
  await page.getByText('show traceback').first().click();

  await expect(page.getByText('<img src=x', { exact: false }).first()).toBeVisible();
  expect(await page.evaluate(() => (window as any).__xss)).toBeUndefined();
  await expect(page.locator('img[src="x"]')).toHaveCount(0);
});

test('the dependency graph fits and is centered in the editor panel', async ({ page }) => {
  await page.goto(`/pipelines/${pipelineUUID}/edit`);
  await page.locator('foreignObject').first().waitFor();
  // The graph fits after each layout; give the last one time to land.
  await page.waitForTimeout(2500);

  const { container, graph, left, right } = await page.evaluate(() => {
    const svg = document.querySelector('foreignObject').closest('svg');
    const box = svg.parentElement.getBoundingClientRect();
    const group = svg.querySelector(':scope > g').getBoundingClientRect();

    return {
      container: box.x + box.width / 2,
      graph: group.x + group.width / 2,
      left: group.left - box.left,
      right: box.right - group.right,
    };
  });
  expect(Math.abs(container - graph)).toBeLessThan(4);
  expect(left).toBeGreaterThanOrEqual(-2);
  expect(right).toBeGreaterThanOrEqual(-2);
});

test('pages fit a 1024 px wide screen', async ({ page }) => {
  await page.setViewportSize({ height: 700, width: 1024 });
  for (const path of [
    '/overview',
    '/pipelines',
    `/pipelines/${pipelineUUID}/edit`,
    `/pipelines/${pipelineUUID}/runs`,
    `/pipelines/${pipelineUUID}/triggers`,
    '/monitors',
    '/settings/workspace/preferences',
  ]) {
    await page.goto(path);
    await page.waitForLoadState();
    await page.waitForTimeout(1500);
    const overflow = await page.evaluate(() => document.documentElement.scrollWidth - innerWidth);
    expect(overflow, path).toBeLessThanOrEqual(0);
  }
});

test('the code keeps its width in the editor on an 820 px wide screen', async ({ page }) => {
  // The side panels kept their default widths and left the code 20 px.
  await page.setViewportSize({ height: 1180, width: 820 });
  await page.goto(`/pipelines/${pipelineUUID}/edit`);
  const editor = page.locator('.monaco-editor').first();
  await editor.waitFor();
  await page.waitForTimeout(1500);

  const box = await editor.boundingBox();
  expect(box.width).toBeGreaterThanOrEqual(240);
  expect(box.x + box.width).toBeLessThanOrEqual(820);
});

test('the trigger page shows its run settings at their width', async ({ page }) => {
  // The panel's width was set before the page knew it had a panel, and it stayed 200 px:
  // labels broke mid-word.
  const { pipeline_schedule: trigger } = await api.call(
    'POST',
    `pipelines/${pipelineUUID}/pipeline_schedules`,
    { pipeline_schedule: { name: `robust_api_${SUFFIX}`, schedule_type: 'api' } },
  );
  await page.setViewportSize({ height: 900, width: 1440 });
  await page.goto(`/pipelines/${pipelineUUID}/triggers/${trigger.id}/edit`);
  const heading = page.getByText('Run settings');
  await heading.waitFor();
  await page.waitForTimeout(1500);

  const width = await heading.evaluate(el => el.closest('aside').getBoundingClientRect().width);
  expect(width).toBeGreaterThanOrEqual(380);

});
