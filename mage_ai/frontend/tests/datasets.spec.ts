import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * Datasets: a block output registered from the page is found by its owner, and its page
 * says when it was published, how fresh its data is (stale: its days end a week ago),
 * whether its checks pass, its columns and its run, and shows the output.
 */

const SUFFIX = Date.now().toString(36);
const LOADER = `sales_${SUFFIX}`;
const DATASET = `daily_sales_${SUFFIX}`;
const OWNER = `owner_${SUFFIX}@example.com`;

let apiContext: APIRequestContext;
let api: MageApi;
let pipelineUUID: string;
let runID: number;

test.beforeAll(async ({ playwright }, testInfo) => {
  test.setTimeout(240_000);
  apiContext = await playwright.request.newContext({
    baseURL: process.env.MAGE_E2E_API_URL || testInfo.project.use.baseURL,
  });
  api = await MageApi.signIn(apiContext);
  pipelineUUID = await api.createPipeline(`ui_datasets_${SUFFIX}`);
  await api.addBlock(pipelineUUID, {
    content: `
import pandas as pd

@data_loader
def load(*args, **kwargs):
    days = pd.date_range('2026-01-01', periods=3, freq='D', tz='UTC')
    return pd.DataFrame({'day': days, 'sales': [10, 20, 30]})
`,
    name: LOADER,
    type: 'data_loader',
  });
  const run = await api.runOnce(pipelineUUID, `datasets_${SUFFIX}`);
  expect(run.status).toBe('completed');
  runID = run.id;
});

test.afterAll(async () => {
  await api.call('DELETE', `datasets/${DATASET}`).catch(() => null);
  await api.deletePipeline(pipelineUUID).catch(() => null);
  await apiContext.dispose();
});

test('a registered output is found and shows its freshness, checks and output', async ({
  page,
}) => {
  test.setTimeout(120_000);
  await page.goto('/datasets');
  const form = page.getByRole('form', { name: 'Register a dataset' });
  await expect(form).toBeVisible({ timeout: 60_000 });
  await form.getByLabel('Dataset id').fill(DATASET);
  await form.getByLabel('Dataset pipeline').fill(pipelineUUID);
  await form.getByLabel('Dataset block').fill(LOADER);
  await form.getByLabel('Dataset owner').fill(OWNER);
  await form.getByLabel('Dataset time column').fill('day');
  await form.getByLabel('Dataset max age').fill('1d');
  await form.getByRole('button', { name: 'Register the dataset' }).click();

  const detail = page.getByRole('region', { name: `Dataset ${DATASET}` });
  await expect(detail.getByText('Published', { exact: false }).first())
    .toBeVisible({ timeout: 30_000 });
  await expect(detail.getByText(`by run ${runID}`, { exact: false })).toBeVisible();
  await expect(detail.getByText(/Stale: data \d+ days old, 1d allowed/)).toBeVisible();
  await expect(detail.getByText('Checks passing')).toBeVisible();
  await expect(detail.getByRole('table', { name: `Columns of ${DATASET}` })
    .getByText('sales')).toBeVisible();
  await expect(detail.getByText('The published output')).toBeVisible();

  await page.getByLabel('Search datasets').fill(OWNER);
  await expect(page.getByRole('link', { name: DATASET, exact: true })).toBeVisible();
  await page.getByLabel('Search datasets').fill(`nothing_${SUFFIX}`);
  await expect(page.getByText(`No dataset matches nothing_${SUFFIX}.`)).toBeVisible();
});
