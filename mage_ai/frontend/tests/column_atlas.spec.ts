import { APIRequestContext, Page } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';
import { scrollOutputToLastColumn } from './utils';

// Module scope is evaluated again in each worker, so a retry creates a new pipeline.
const SUFFIX = Date.now().toString(36);
const LOADER = `atlas_orders_${SUFFIX}`;

const ORDERS = `
import numpy as np
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_data(*args, **kwargs):
    n = 600
    return pd.DataFrame({
        'order_id': np.arange(1, n + 1),
        'city': pd.Categorical(np.array(['Cartago', 'Heredia', 'Limón'])[np.arange(n) % 3]),
        'amount': np.round(np.arange(n) * 1.5, 2),
        'quantity': pd.array([None if i % 50 == 0 else i % 7 for i in range(n)], dtype='Int64'),
        'paid': np.arange(n) % 4 != 0,
        'ordered_at': pd.date_range('2024-01-01', periods=n, freq='h', tz='UTC'),
    })
`;

// A different number of rows on every run.
const CHANGING = `
import time
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_data(*args, **kwargs):
    n = 100 + time.time_ns() // 1000 % 900
    return pd.DataFrame({'n': range(n), 'run_rows': n})
`;

let apiContext: APIRequestContext;
let api: MageApi;
let pipelineUUID: string;

test.beforeAll(async ({ playwright }, testInfo) => {
  apiContext = await playwright.request.newContext({
    baseURL: testInfo.project.use.baseURL,
  });
  api = await MageApi.signIn(apiContext);
  pipelineUUID = await api.createPipeline(`atlas_${SUFFIX}`);
  await api.addBlock(pipelineUUID, { content: ORDERS, name: LOADER, type: 'data_loader' });
});

test.afterAll(async () => {
  await api.deletePipeline(pipelineUUID);
  await apiContext.dispose();
});

async function runLoader(page: Page) {
  await page.goto(`/pipelines/${pipelineUUID}/edit`);
  // Monaco renders the code lines over its input element, so click a rendered line.
  const editor = page.locator('.monaco-editor .view-lines').first();
  await expect(editor).toBeVisible();
  await editor.click();
  await page.keyboard.press('ControlOrMeta+Enter');
}

test('explore a block output: profiles, sort, filter, full window', async ({ page }) => {
  await runLoader(page);
  const atlas = page.getByRole('region', { name: `Output ${LOADER} / output_0` });
  await expect(atlas).toBeVisible({ timeout: 90000 });
  await expect(atlas.getByText('600 rows × 6 columns')).toBeVisible();
  await expect(atlas.getByRole('columnheader', { exact: true, name: 'order_id' })).toBeVisible();
  // Profiles load in the headers: quantity has a missing value every 50 rows.
  await expect(atlas.getByText('2.0% missing')).toBeVisible();

  // Sort by amount, descending.
  const amount = atlas.getByRole('columnheader', { exact: true, name: 'amount' });
  await amount.locator('.ca-head-main').click();
  await amount.locator('.ca-head-main').click();
  await expect(amount).toHaveAttribute('aria-sort', 'descending');
  await expect(atlas.getByRole('row').nth(1).getByRole('gridcell').first()).toHaveText('600');

  // Filter on the city.
  await atlas.getByRole('button', { name: 'Filter city' }).click();
  await atlas.getByLabel('Condition').selectOption('eq');
  await atlas.getByLabel('Value').fill('Limón');
  await atlas.getByRole('button', { name: 'Add filter' }).click();
  await expect(atlas.getByText('200 rows of 600 × 6 columns')).toBeVisible();
  await expect(atlas.getByRole('button', { name: 'Remove filter city = Limón' })).toBeVisible();

  // The full window shares the view and opens the column panel.
  await atlas.getByRole('button', { name: 'Expand' }).click();
  const dialog = page.getByRole('dialog', { name: `Output ${LOADER} / output_0` });
  await expect(dialog).toBeVisible();
  const expanded = dialog.getByRole('region', { name: `Output ${LOADER} / output_0` });
  await expect(expanded.getByText('200 rows of 600 × 6 columns')).toBeVisible();
  const panel = expanded.getByRole('complementary', { name: 'Columns' });
  await panel.getByText('city', { exact: true }).click();
  await expect(panel.getByText('Most frequent')).toBeVisible();
  await panel.getByText('amount', { exact: true }).click();
  await expect(panel.getByText('Std. deviation')).toBeVisible();

  // Cell inspector, then Escape closes it and the window.
  await expanded.getByRole('gridcell', { exact: true, name: 'Limón' }).first().click();
  await page.keyboard.press('Enter');
  await expect(expanded.getByRole('dialog', { name: 'Cell value' })).toBeVisible();
  await page.keyboard.press('Escape');
  await expect(expanded.getByRole('dialog', { name: 'Cell value' })).toBeHidden();
  await page.keyboard.press('Escape');
  await expect(dialog).toBeHidden();

  // Columns past the edge render as the grid scrolls.
  await scrollOutputToLastColumn(atlas);
  await expect(atlas.getByRole('columnheader', { exact: true, name: 'ordered_at' })).toBeVisible();
});

test('fall back to the plain table when the explorer cannot open the output', async ({ page }) => {
  await page.route('**/api/column_atlas_queries**', route => route.fulfill({
    body: JSON.stringify({ error: { code: 404, message: 'Not available.', type: 'column_atlas_error' } }),
    contentType: 'application/json',
    status: 404,
  }));
  await runLoader(page);
  await expect(page.getByRole('columnheader', { exact: true, name: 'ordered_at' })).toBeVisible({
    timeout: 90000,
  });
  await expect(page.getByRole('region', { name: /^Output / })).toHaveCount(0);
  await expect(page.getByRole('cell', { exact: true, name: 'Limón' }).first()).toBeVisible();
});

test('a block that runs again shows its new output', async ({ page }) => {
  const pipeline = await api.createPipeline(`atlas_rerun_${SUFFIX}`);
  try {
    await api.addBlock(pipeline, { content: CHANGING, name: `rerun_${SUFFIX}`, type: 'data_loader' });
    await page.goto(`/pipelines/${pipeline}/edit`);
    const editor = page.locator('.monaco-editor .view-lines').first();
    await expect(editor).toBeVisible();

    const atlas = page.getByRole('region', { name: `Output rerun_${SUFFIX} / output_0` });
    const rowCount = atlas.locator('.ca-title span').first();
    await editor.click();
    await page.keyboard.press('ControlOrMeta+Enter');
    await expect(rowCount).toHaveText(/^[\d,]+ rows × 2 columns$/, { timeout: 90000 });
    const first = await rowCount.textContent();

    // The same explorer stays on the page and reads the new output.
    await editor.click();
    await page.keyboard.press('ControlOrMeta+Enter');
    await expect(rowCount).not.toHaveText(first, { timeout: 90000 });
    const second = await rowCount.textContent();
    const rows = Number(second.split(' ')[0].replace(/,/g, ''));
    await expect(atlas.getByRole('gridcell', { exact: true, name: String(rows) }).first())
      .toBeVisible();
  } finally {
    await api.deletePipeline(pipeline);
  }
});
