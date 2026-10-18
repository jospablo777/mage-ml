import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

// A streaming pipeline started from a template (C4): the Kafka template's source, transformer,
// printing sink and sample are there, and the transformer runs on the sample at once.
const NAME = `stream_template_${Date.now().toString(36)}`;

let apiContext: APIRequestContext;
let api: MageApi;

test.beforeAll(async ({ playwright }, testInfo) => {
  apiContext = await playwright.request.newContext({ baseURL: testInfo.project.use.baseURL });
  api = await MageApi.signIn(apiContext);
});

test.afterAll(async () => {
  await api.deletePipeline(NAME).catch(() => null);
  await apiContext.dispose();
});

test('a stream from the Kafka template runs on its sample', async ({ page }) => {
  await page.goto('/pipelines');
  await expect(page.getByText('Name', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'New' }).click();
  await page.getByRole('menuitem', { name: 'Streaming' }).click();
  await page.getByPlaceholder('Pipeline name...').fill(NAME);
  await page.getByLabel('Start from').selectOption('kafka');
  await page.getByRole('button', { name: 'Create' }).click();

  await page.waitForURL(new RegExp(`/pipelines/${NAME}/edit`));
  await expect(page.getByText(`${NAME}_kafka`).first()).toBeVisible();
  await expect(page.getByText(`${NAME}_print`).first()).toBeVisible();
  // The compile panel shows how the stream runs on the engine.
  await page.getByRole('button', { name: 'Compile' }).first().click();
  await expect(page.getByText('How the stream runs: 1 run of blocks, 0 language boundaries'))
    .toBeVisible();
  await page.keyboard.press('Escape');

  await page.goto(`/pipelines/${NAME}/edit?sideview=samples`);
  await expect(page.getByText('example', { exact: true })).toBeVisible();
  // The transformer is the second block; its editor takes ⌘↵.
  const editor = page.locator('.monaco-editor .view-lines').nth(1);
  await expect(editor).toBeVisible();
  await editor.click();
  await page.keyboard.press('ControlOrMeta+Enter');

  await expect(page.getByText('5 in → 4 out')).toBeVisible({ timeout: 60000 });
  await expect(page.getByText('1 message dropped', { exact: false })).toBeVisible();
});
