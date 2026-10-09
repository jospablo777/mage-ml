import { Page } from '@playwright/test';

import { expect, test } from './base';

/*
 * R blocks in the notebook: the block menus list SQL and R above the Python templates,
 * R opens its templates, and a block made from one has the template's code. With
 * MAGE_E2E_R=1, the server has an R environment and the block also runs.
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

async function openRTemplates(page: Page, blockType: string) {
  await page.getByText('All blocks').first().click();
  await visible(page, blockType).hover();
  await visible(page, 'R').hover();
}

test.describe('R blocks', () => {
  test.beforeEach(async ({ page }) => {
    // A laptop screen, where submenus that opened to the right were cut off.
    await page.setViewportSize({ height: 720, width: 1280 });
    await newPipeline(page);
  });

  test('the R templates of each block type are listed and visible', async ({ page }) => {
    const templates = {
      'Data exporter': ['Local file', 'Amazon S3', 'API', 'PostgreSQL', 'MySQL', 'DuckDB', 'SQLite'],
      'Data loader': ['Local file', 'Amazon S3', 'API', 'PostgreSQL', 'MySQL', 'DuckDB', 'SQLite'],
      Transformer: ['Clean data', 'Aggregate', 'Join', 'Reshape'],
    };
    for (const [blockType, names] of Object.entries(templates)) {
      await openRTemplates(page, blockType);
      await expect(visible(page, 'Base template (generic)')).toBeInViewport();
      for (const name of names) {
        await expect(visible(page, name)).toBeInViewport();
      }
      await page.keyboard.press('Escape');
      await page.mouse.click(5, 700);
    }
  });

  test('a block made from an R template has its code', async ({ page }) => {
    await openRTemplates(page, 'Data loader');
    await visible(page, 'Local file').click();
    await expect(page.getByText('Language')).toBeVisible();
    await page.getByRole('button', { name: 'Save and add' }).click();

    await expect(page.getByText('read_file').first()).toBeVisible();
    await expect(page.getByText('@data_loader').first()).toBeVisible();

    test.skip(!process.env.MAGE_E2E_R, 'The server has no R environment.');
    await page.locator('.monaco-editor').first().click();
    await page.keyboard.press(process.platform === 'darwin' ? 'Meta+Enter' : 'Control+Enter');
    // The project's data/input.csv has a row whose name is ñandú.
    await expect(page.getByText('ñandú').first()).toBeVisible({ timeout: 90000 });
  });
});
