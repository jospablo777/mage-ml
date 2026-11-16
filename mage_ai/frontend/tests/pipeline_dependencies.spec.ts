import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * The Dependencies page draws the pipelines that start or feed other pipelines and lists
 * each link: here an after-run trigger. orchestration/pipeline_dependencies.py finds them.
 */

const SUFFIX = Date.now().toString(36);

let apiContext: APIRequestContext;
let api: MageApi;
let upstream: string;
let downstream: string;

test.beforeAll(async ({ playwright }, testInfo) => {
  apiContext = await playwright.request.newContext({
    baseURL: process.env.MAGE_E2E_API_URL || testInfo.project.use.baseURL,
  });
  api = await MageApi.signIn(apiContext);
  upstream = await api.createPipeline(`deps_orders_${SUFFIX}`);
  downstream = await api.createPipeline(`deps_cleanup_${SUFFIX}`);
  await api.call('POST', `pipelines/${downstream}/pipeline_schedules`, {
    pipeline_schedule: {
      name: `after orders ${SUFFIX}`,
      schedule_type: 'api',
      settings: { after_pipeline: upstream, after_status: 'failed' },
    },
  });
});

test.afterAll(async () => {
  for (const uuid of [downstream, upstream]) {
    if (uuid) {
      await api.deletePipeline(uuid);
    }
  }
  await apiContext?.dispose();
});

test('an after-run trigger shows as a link between two pipelines', async ({ page }) => {
  await page.goto('/pipeline-dependencies');

  await expect(page.getByText(/^\d+ links? between \d+ pipelines?$/)).toBeVisible({
    timeout: 30_000,
  });
  const row = page.getByRole('row').filter({ hasText: `after ${upstream} fails` });
  await expect(row).toBeVisible({ timeout: 30_000 });
  await expect(row.getByRole('link', { name: upstream })).toBeVisible();
  await expect(row.getByText(`trigger after orders ${SUFFIX}`, { exact: false })).toBeVisible();
  // Both pipelines are nodes of the graph, with their last run.
  await expect(page.locator('foreignObject').filter({ hasText: downstream })).toBeVisible();
  await expect(page.locator('foreignObject').filter({ hasText: downstream })
    .getByText('never run')).toBeVisible();

  await row.getByRole('link', { name: downstream }).click();
  await expect(page).toHaveURL(new RegExp(`/pipelines/${downstream}/triggers`));
});
