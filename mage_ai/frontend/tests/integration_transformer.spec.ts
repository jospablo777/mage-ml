import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * An integration pipeline's transformer gets a stream's records in DataFrames of up to
 * 10,000 rows. Its block settings set how many, 1 for code written for one record at a
 * time. Runs are tested in integration_tests/postgres/test_integration_transformer.py.
 */

const SUFFIX = Date.now().toString(36);
const TRANSFORMER = `shout_${SUFFIX}`;

let apiContext: APIRequestContext;
let api: MageApi;
let pipelineUUID: string;

test.beforeAll(async ({ playwright }, testInfo) => {
  apiContext = await playwright.request.newContext({
    baseURL: process.env.MAGE_E2E_API_URL || testInfo.project.use.baseURL,
  });
  api = await MageApi.signIn(apiContext);
  const created = await api.call('POST', 'pipelines', {
    pipeline: { name: `records_per_call_${SUFFIX}`, type: 'integration' },
  });
  pipelineUUID = created.pipeline.uuid;
  // The editor shows transformers once the pipeline has its source.
  await api.call('POST', `pipelines/${pipelineUUID}/blocks`, {
    block: {
      content: 'source: postgresql\nconfig:\n  host: localhost\n',
      language: 'yaml',
      name: `source_${SUFFIX}`,
      type: 'data_loader',
    },
  });
  await api.addBlock(pipelineUUID, {
    content: '@transformer\ndef shout(df, *args, **kwargs):\n    return df\n',
    name: TRANSFORMER,
    type: 'transformer',
    upstream_blocks: [`source_${SUFFIX}`],
  });
});

test.afterAll(async () => {
  if (pipelineUUID) {
    await api.deletePipeline(pipelineUUID);
  }
  await apiContext?.dispose();
});

async function savedRecordsPerCall() {
  const { block } = await api.call(
    'GET', `pipelines/${pipelineUUID}/blocks/${TRANSFORMER}?block_type=transformer`,
  );

  return block?.configuration?.records_per_call ?? null;
}

test('a transformer is set to get one record per call', async ({ page }) => {
  await page.goto(`/pipelines/${pipelineUUID}/edit?sideview=block_settings`);
  // A click on the code selects the block (one on its name opens the rename dialog).
  await page.getByText('def shout', { exact: false }).first().click();

  await expect(page.getByText('Records per call', { exact: true })).toBeVisible({
    timeout: 30_000,
  });
  await expect(page.getByText('about 100 times', { exact: false })).toBeVisible();
  const input = page.locator('input[name="records_per_call"]');
  await input.fill('1');
  await page.getByRole('button', { name: 'Update block settings' }).click();
  await expect.poll(savedRecordsPerCall, { timeout: 15_000 }).toBe(1);

  await input.fill('');
  await page.getByRole('button', { name: 'Update block settings' }).click();
  await expect.poll(savedRecordsPerCall, { timeout: 15_000 }).toBe(null);
});
