import { APIRequestContext } from '@playwright/test';

import { MageApi, OUTPUT_LOADER, OUTPUT_TRANSFORMER } from './api';
import { expect, test } from './base';

/*
 * The Compile panel says where a chain of blocks stops and why, from the one planner
 * (mage_ai/orchestration/planning.py): a block whose settings turn fusion off runs on its
 * own, and the panel names it with that reason.
 */

const SUFFIX = Date.now().toString(36);
const LOADER = `stops_load_${SUFFIX}`;
const TRANSFORMER = `stops_transform_${SUFFIX}`;

let apiContext: APIRequestContext;
let api: MageApi;
let pipelineUUID: string;

test.beforeAll(async ({ playwright }, testInfo) => {
  apiContext = await playwright.request.newContext({
    baseURL: process.env.MAGE_E2E_API_URL || testInfo.project.use.baseURL,
  });
  api = await MageApi.signIn(apiContext);
  pipelineUUID = await api.createPipeline(`compile_stops_${SUFFIX}`);
  await api.addBlock(pipelineUUID, { content: OUTPUT_LOADER, name: LOADER, type: 'data_loader' });
  await api.addBlock(pipelineUUID, {
    content: OUTPUT_TRANSFORMER,
    name: TRANSFORMER,
    type: 'transformer',
    upstream_blocks: [LOADER],
  });
  await api.call('PUT', `pipelines/${pipelineUUID}/blocks/${TRANSFORMER}`, {
    block: { configuration: { fusion: false } },
  });
});

test.afterAll(async () => {
  await api?.deletePipeline(pipelineUUID);
  await apiContext?.dispose();
});

test('the Compile panel says where a chain stops and why', async ({ page }) => {
  await page.goto(`/pipelines/${pipelineUUID}/edit`);
  await page.getByRole('button', { name: /^(Compile|Compiled|Optimized)/ }).first().click();

  await expect(page.getByText('Where chains stop', { exact: true })).toBeVisible();
  await expect(page.getByText(`${LOADER} → ${TRANSFORMER}`, { exact: true })).toBeVisible();
  await expect(page.getByText(`${TRANSFORMER} runs on its own: its settings turn fusion off`))
    .toBeVisible();
});
