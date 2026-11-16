import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * The audit log in Settings > User management: a change made through the API is listed
 * with its user, what it wrote and its outcome, and the list filters by action.
 */

const SUFFIX = Date.now().toString(36);

let apiContext: APIRequestContext;
let api: MageApi;
let pipelineUUID: string;

test.beforeAll(async ({ playwright }, testInfo) => {
  apiContext = await playwright.request.newContext({
    baseURL: process.env.MAGE_E2E_API_URL || testInfo.project.use.baseURL,
  });
  api = await MageApi.signIn(apiContext);
  pipelineUUID = await api.createPipeline(`audited_${SUFFIX}`);
});

test.afterAll(async () => {
  if (pipelineUUID) {
    await api.deletePipeline(pipelineUUID);
  }
  await apiContext?.dispose();
});

test('a created pipeline is in the audit log with what it wrote', async ({ page }) => {
  await page.goto('/settings/workspace/audit-log');
  await expect(page.getByText(/^\d+ changes? made through MageML$/)).toBeVisible({
    timeout: 30_000,
  });

  const row = page.getByRole('row').filter({ hasText: 'pipelines' })
    .filter({ hasText: 'create' }).first();
  await expect(row).toBeVisible();
  await expect(row.getByText('succeeded')).toBeVisible();
  await row.click();
  await expect(page.getByText(`"name": "audited_${SUFFIX}"`, { exact: false })).toBeVisible();

  await page.locator('select').filter({ hasText: 'Any action' }).selectOption('delete');
  await page.getByRole('button', { name: 'Filter' }).click();
  await expect(page).toHaveURL(/action=delete/);
  await expect(page.getByRole('row').filter({ hasText: 'create' })).toHaveCount(0);
});
