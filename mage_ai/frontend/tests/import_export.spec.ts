import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * Settings > Workspace > Import and export: a pipeline exported as a zip, the zip planned
 * against the project (the pipeline it has is kept), then imported after the pipeline was
 * deleted, which creates it again. data_preparation/project_archive.py does the work.
 */

const SUFFIX = Date.now().toString(36);
const NAME = `exported_${SUFFIX}`;

let apiContext: APIRequestContext;
let api: MageApi;
let pipelineUUID: string;

test.beforeAll(async ({ playwright }, testInfo) => {
  apiContext = await playwright.request.newContext({
    baseURL: process.env.MAGE_E2E_API_URL || testInfo.project.use.baseURL,
  });
  api = await MageApi.signIn(apiContext);
  pipelineUUID = await api.createPipeline(NAME);
  await api.addBlock(pipelineUUID, {
    content: '@data_loader\ndef load(*args, **kwargs):\n    return 1\n',
    name: `load_${SUFFIX}`,
    type: 'data_loader',
  });
});

test.afterAll(async () => {
  await api.deletePipeline(pipelineUUID).catch(() => null);
  await apiContext?.dispose();
});

test('a pipeline is exported, planned and imported again', async ({ page }) => {
  await page.goto('/settings/workspace/import-export');
  await page.getByText(NAME, { exact: true }).click();

  const [download] = await Promise.all([
    page.waitForEvent('download'),
    page.getByRole('button', { name: 'Export 1 pipeline' }).click(),
  ]);
  const archive = await download.path();

  await page.getByLabel('Archive to import').setInputFiles(archive);
  const row = page.getByRole('row').filter({ hasText: NAME });
  await expect(row.getByText('kept as it is in this project')).toBeVisible({ timeout: 30_000 });

  await api.deletePipeline(pipelineUUID);
  await page.getByLabel('Archive to import').setInputFiles([]);
  await page.getByLabel('Archive to import').setInputFiles(archive);
  await expect(row.getByText('created', { exact: true })).toBeVisible({ timeout: 30_000 });
  await page.getByRole('button', { name: 'Import', exact: true }).click();
  await expect(page.getByText(/^Imported: \d+ files written\.$/)).toBeVisible();

  const detail = await api.call('GET', `pipelines/${pipelineUUID}`);
  expect(detail.pipeline.blocks.map(({ uuid }) => uuid)).toEqual([`load_${SUFFIX}`]);
});
