import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * A block's trigger rule, set in its block settings: all_done runs it once its upstream
 * blocks finished, however they ended. Runs are tested in
 * integration_tests/scheduling/test_trigger_rules.py.
 */

const SUFFIX = Date.now().toString(36);
const LOADER = `load_${SUFFIX}`;
const CLEANUP = `cleanup_${SUFFIX}`;

let apiContext: APIRequestContext;
let api: MageApi;
let pipelineUUID: string;

test.beforeAll(async ({ playwright }, testInfo) => {
  apiContext = await playwright.request.newContext({
    baseURL: process.env.MAGE_E2E_API_URL || testInfo.project.use.baseURL,
  });
  api = await MageApi.signIn(apiContext);
  pipelineUUID = await api.createPipeline(`trigger_rules_${SUFFIX}`);
  await api.addBlock(pipelineUUID, {
    content: '@data_loader\ndef load(*args, **kwargs):\n    return 1\n',
    name: LOADER,
    type: 'data_loader',
  });
  await api.addBlock(pipelineUUID, {
    content: '@data_exporter\ndef cleanup(data, *args, **kwargs):\n    pass\n',
    name: CLEANUP,
    type: 'data_exporter',
    upstream_blocks: [LOADER],
  });
});

test.afterAll(async () => {
  if (pipelineUUID) {
    await api.deletePipeline(pipelineUUID);
  }
  await apiContext?.dispose();
});

async function savedRule() {
  const { block } = await api.call(
    'GET', `pipelines/${pipelineUUID}/blocks/${CLEANUP}?block_type=data_exporter`,
  );

  return block?.configuration?.trigger_rule ?? null;
}

test('a cleanup block is set to run however its upstream blocks end', async ({ page }) => {
  await page.goto(`/pipelines/${pipelineUUID}/edit?sideview=block_settings`);
  // A click on the code selects the block (one on its name opens the rename dialog).
  await page.getByText('def cleanup', { exact: false }).first().click();

  const rule = page.locator('select').filter({
    has: page.getByRole('option', { name: /\(all_done\)/ }),
  });
  await expect(rule).toBeVisible({ timeout: 30_000 });
  await expect(page.getByText('A failed block still fails the run.', { exact: false }))
    .toBeVisible();
  await rule.selectOption('all_done');
  await page.getByRole('button', { name: 'Update block settings' }).click();
  await expect.poll(savedRule, { timeout: 15_000 }).toBe('all_done');
  // A setting, not one of the block's variables.
  await expect(page.getByText('trigger_rule', { exact: true })).toHaveCount(0);

  await rule.selectOption('');
  await page.getByRole('button', { name: 'Update block settings' }).click();
  await expect.poll(savedRule, { timeout: 15_000 }).toBe(null);
});
