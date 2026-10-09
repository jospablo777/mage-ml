import { APIRequestContext } from '@playwright/test';

import { MageApi, OUTPUT_LOADER, OUTPUT_TRANSFORMER } from './api';
import { expect, test } from './base';

/*
 * Block fusion from the pipeline settings: turning it on shows the chains that run as
 * stages, and a run from a trigger completes with them.
 */

const SUFFIX = Date.now().toString(36);
const LOADER = `fusion_load_${SUFFIX}`;
const TRANSFORMER = `fusion_transform_${SUFFIX}`;

let apiContext: APIRequestContext;
let api: MageApi;
let pipelineUUID: string;

test.beforeAll(async ({ playwright }, testInfo) => {
  // Against `next dev`, MAGE_E2E_API_URL points at the Mage server, which serves the API.
  apiContext = await playwright.request.newContext({
    baseURL: process.env.MAGE_E2E_API_URL || testInfo.project.use.baseURL,
  });
  api = await MageApi.signIn(apiContext);
  pipelineUUID = await api.createPipeline(`ui_fusion_${SUFFIX}`);
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

test('turn on block fusion, see the plan, and run the pipeline', async ({ page }) => {
  const title = 'Run chains of blocks together (block fusion)';
  await page.goto(`/pipelines/${pipelineUUID}/settings`);
  await expect(page.getByText(title, { exact: true })).toBeVisible();
  // The innermost element with the title and a switch: the setting's row.
  const row = page.locator('div')
    .filter({ has: page.getByText(title, { exact: true }) })
    .filter({ has: page.locator('input[type="checkbox"]') })
    .last();
  await row.locator('input[type="checkbox"] + span').click();
  await page.getByRole('button', { name: 'Save pipeline settings' }).click();

  await expect.poll(async () => {
    const { pipeline } = await api.call('GET', `pipelines/${pipelineUUID}`);

    return [pipeline.block_fusion, pipeline.fusion_plan];
  }).toEqual(['chains', [[LOADER, TRANSFORMER]]]);

  // The plan comes from the server, for the saved pipeline.
  await page.reload();
  await expect(page.getByText(LOADER, { exact: true })).toBeVisible();
  await expect(page.getByText(`→ ${TRANSFORMER}`, { exact: true })).toBeVisible();

  const run = await api.runOnce(pipelineUUID, `fusion_${SUFFIX}`);
  expect(run.status).toBe('completed');
});
