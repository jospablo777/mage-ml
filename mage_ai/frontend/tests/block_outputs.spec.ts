import { APIRequestContext } from '@playwright/test';

import { MageApi, OUTPUT_LOADER, OUTPUT_TRANSFORMER } from './api';
import { expect, test } from './base';
import { scrollOutputToLastColumn } from './utils';

// Module scope is evaluated again in each worker, so a retry creates a new pipeline.
const SUFFIX = Date.now().toString(36);
const LOADER = `ui_load_${SUFFIX}`;
const TRANSFORMER = `ui_transform_${SUFFIX}`;

let apiContext: APIRequestContext;
let api: MageApi;
let pipelineUUID: string;

test.beforeAll(async ({ playwright }, testInfo) => {
  apiContext = await playwright.request.newContext({
    baseURL: testInfo.project.use.baseURL,
  });
  api = await MageApi.signIn(apiContext);
  pipelineUUID = await api.createPipeline(`ui_outputs_${SUFFIX}`);
  await api.addBlock(pipelineUUID, {
    content: OUTPUT_LOADER,
    name: LOADER,
    type: 'data_loader',
  });
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

test('run a block in the editor and render its output table', async ({ page }) => {
  await page.goto(`/pipelines/${pipelineUUID}/edit`);

  // Monaco renders the code lines over its input element, so click a rendered line.
  const editor = page.locator('.monaco-editor .view-lines').first();
  await expect(editor).toBeVisible();
  await editor.click();
  await page.keyboard.press('ControlOrMeta+Enter');

  await expect(page.getByRole('columnheader', { exact: true, name: 'attrs' })).toBeVisible({
    timeout: 90000,
  });
  await scrollOutputToLastColumn(page);
  await expect(page.getByRole('columnheader', { exact: true, name: 'tags' })).toBeVisible();
  // Outputs open in ColumnAtlas; nested pandas values are stored as JSON text.
  await expect(page.getByRole('gridcell', { exact: true, name: '["y", "z"]' })).toBeVisible();
});

test('show the output of a block run started by a trigger', async ({ page }) => {
  // The scheduler picks up the trigger on its next tick, then runs both blocks.
  test.setTimeout(240000);
  const run = await api.runOnce(pipelineUUID, 'ui_outputs_once');
  expect(run.status).toBe('completed');

  await page.goto(`/pipelines/${pipelineUUID}/runs/${run.id}`);
  await page.getByRole('row')
    .filter({ hasText: TRANSFORMER })
    .getByRole('cell', { name: 'completed' })
    .click();

  await expect(page.getByRole('button', { name: 'Block output' })).toBeVisible();
  await scrollOutputToLastColumn(page);
  await expect(page.getByRole('columnheader', { exact: true, name: 'n_tags' })).toBeVisible();
  // The transformer records the dtypes it received from the loader's stored output.
  const dtypes = page.getByRole('gridcell', { name: /^id=int64,ni=Int64,cat=category,/ }).first();
  await expect(dtypes).toBeVisible();
});
