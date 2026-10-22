import { Page } from '@playwright/test';

import { expect, test } from './base';

/*
 * The Python templates of the block menus come from the server's template records: the
 * sources are listed by family, every source file has its item, and a block made from one
 * starts with the source's code.
 */

async function newPipeline(page: Page) {
  await page.goto('/pipelines');
  await page.getByRole('button', { name: 'New' }).click();
  await page.getByRole('menuitem', { name: 'Standard (batch)' }).click();
  await page.getByRole('button', { exact: true, name: 'Create' }).click();
  await page.waitForURL('**/pipelines/**/edit**');
}

// The visible element with the text; each block type's menu has its own, hidden ones.
function visible(page: Page, text: string) {
  return page.getByText(text, { exact: true }).filter({ visible: true }).first();
}

test.beforeEach(async ({ page }) => {
  await page.setViewportSize({ height: 900, width: 1440 });
  await newPipeline(page);
});

test('Python sources are listed by family and make their blocks', async ({ page }) => {
  await page.getByText('All blocks').first().click();
  // The Python families follow the R, Rust and SQL items, under a Python title.
  await visible(page, 'Data loader').hover();
  for (const family of ['APIs', 'Cloud storage', 'Databases', 'Vector databases']) {
    await expect(visible(page, family)).toBeVisible();
  }
  // A source no menu reached before the records held them.
  await visible(page, 'Vector databases').hover();
  await expect(visible(page, 'Qdrant')).toBeVisible();

  await visible(page, 'Databases').hover();
  await visible(page, 'PostgreSQL').click();
  await page.getByRole('button', { name: 'Save and add' }).click();
  await expect(page.getByText('from mage_ai.io.postgres import Postgres').first())
    .toBeVisible();
});

test('SQL templates are listed under SQL and make their blocks', async ({ page }) => {
  // The editor asks for the templates a few seconds after it opens.
  await page.waitForResponse(response => response.url().includes('/api/block_templates'));
  await page.getByText('All blocks').first().click();
  await visible(page, 'Transformer').hover();
  // The menu item; the page has other SQL text.
  await page.getByRole('menuitem', { exact: true, name: 'SQL' })
    .filter({ visible: true }).first().hover();
  for (const name of ['Base template (generic)', 'Deduplicate', 'Date spine', 'Running total']) {
    await expect(visible(page, name)).toBeVisible();
  }
  await visible(page, 'Deduplicate').click();
  await page.getByRole('button', { name: 'Save and add' }).click();
  await expect(page.getByText('SELECT DISTINCT ON (id) *').first()).toBeVisible();
});

test('Python transformer templates are listed by family and make Polars blocks', async ({ page }) => {
  await page.waitForResponse(response => response.url().includes('/api/block_templates'));
  await page.getByText('All blocks').first().click();
  await visible(page, 'Transformer').hover();
  for (const family of ['Data cleaning', 'Feature extraction', 'Machine learning']) {
    await expect(visible(page, family)).toBeVisible();
  }
  await visible(page, 'Machine learning').hover();
  for (const name of ['Model inference', 'Train a model']) {
    await expect(visible(page, name)).toBeVisible();
  }
  await visible(page, 'Data cleaning').hover();
  await visible(page, 'Clean data').click();
  await page.getByRole('button', { name: 'Save and add' }).click();
  await expect(page.getByText('import polars as pl').first()).toBeVisible();
});
