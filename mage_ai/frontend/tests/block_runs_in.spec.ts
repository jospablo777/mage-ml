import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * Choosing where a Python block runs where the block is: in the dialog that creates it,
 * and from the Runs in chip in its header.
 */

const SUFFIX = Date.now().toString(36);
const WORKER = `e2e-runs-${SUFFIX}`;

let apiContext: APIRequestContext;
let api: MageApi;
let uuid: string;

test.beforeAll(async ({ playwright }, testInfo) => {
  apiContext = await playwright.request.newContext({ baseURL: testInfo.project.use.baseURL });
  api = await MageApi.signIn(apiContext);
  await api.call('POST', 'python_workers', { python_worker: {
    dependencies: ['pyarrow'], label: 'Runs worker', name: WORKER, template: 'empty',
  } });
  uuid = await api.createPipeline(`runs_in_${SUFFIX}`);
});

test.afterAll(async () => {
  await api.deletePipeline(uuid).catch(() => null);
  await api.call('DELETE', `python_workers/${WORKER}`).catch(() => null);
  await apiContext.dispose();
});

test('a new Python block runs where the dialog says, and the chip changes it', async ({ page }) => {
  test.setTimeout(180_000);
  await page.goto(`/pipelines/${uuid}/edit`);
  await page.getByText('All blocks').first().click();
  await page.getByText('Transformer', { exact: true }).filter({ visible: true }).first().hover();
  await page.getByRole('menuitem', { exact: true, name: 'Python' })
    .filter({ visible: true }).first().hover();
  await page.getByText('Base template (generic)', { exact: true })
    .filter({ visible: true }).first().click();
  await page.getByLabel('Runs in', { exact: true }).selectOption(WORKER);
  await page.getByRole('button', { name: 'Save and add' }).click();

  const chip = page.getByRole('button', { name: /^Runs in: Runs worker/ });
  await expect(chip).toBeVisible({ timeout: 60_000 });
  const worker = async () => (await api.call('GET', `pipelines/${uuid}`))
    .pipeline.blocks[0].configuration?.python_worker ?? null;
  expect(await worker()).toBe(WORKER);

  // From the chip: back to MageML's Python.
  await chip.click();
  await page.getByText('MageML\'s Python', { exact: true }).filter({ visible: true }).first()
    .click();
  await expect.poll(worker).toBe(null);
  await expect(page.getByRole('button', { name: 'Runs in: MageML\'s Python' })).toBeVisible();
});
