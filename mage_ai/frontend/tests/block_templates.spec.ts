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

function menuItem(page: Page, name: string) {
  return page.getByRole('menuitem', { exact: true, name }).filter({ visible: true }).first();
}

test('Python comes first in each block type, its base template on top', async ({ page }) => {
  await page.getByText('All blocks').first().click();
  for (const blockType of ['Data loader', 'Transformer', 'Data exporter']) {
    await visible(page, blockType).hover();
    const languages = await page.getByRole('menuitem').filter({ visible: true })
      .allInnerTexts();
    const python = languages.findIndex(text => text.trim() === 'Python');
    expect(python, blockType).toBeGreaterThanOrEqual(0);
    expect(python, blockType).toBeLessThan(languages.findIndex(text => text.trim() === 'SQL'));
    await menuItem(page, 'Python').hover();
    await expect(visible(page, 'Base template (generic)')).toBeVisible();
  }
  await visible(page, 'Base template (generic)').click();
  await page.getByRole('button', { name: 'Save and add' }).click();
  await expect(page.getByText('@data_exporter').first()).toBeVisible();
});

test('Python sources are listed by family and make their blocks', async ({ page }) => {
  await page.getByText('All blocks').first().click();
  // The Python families are under Python, as SQL's are under SQL.
  await visible(page, 'Data loader').hover();
  await menuItem(page, 'Python').hover();
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
  await menuItem(page, 'SQL').hover();
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
  await menuItem(page, 'Python').hover();
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
