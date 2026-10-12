import { Page } from '@playwright/test';

import { MageApi } from './api';

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
  // The Local file template reads data/input.csv; the run test looks for its ñandú row.
  test.beforeAll(async ({ playwright }, testInfo) => {
    const context = await playwright.request.newContext({
      baseURL: process.env.MAGE_E2E_API_URL || testInfo.project.use.baseURL,
    });
    const api = await MageApi.signIn(context);
    await api.writeFile('data', 'input.csv', 'name,count\nñandú,3\nkoala,5\n');
    await context.dispose();
  });

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

  test('an R block on a machine without R says how to set R up', async (
    { page, playwright }, testInfo,
  ) => {
    const context = await playwright.request.newContext({
      baseURL: process.env.MAGE_E2E_API_URL || testInfo.project.use.baseURL,
    });
    const api = await MageApi.signIn(context);
    const { language_setup: setup } = await api.call('GET', 'language_setups/r');
    await context.dispose();
    await page.evaluate(() => window.localStorage.removeItem(
      'mage_language_setup_notice_hidden_r',
    ));

    await openRTemplates(page, 'Transformer');
    await visible(page, 'Clean data').click();
    await page.getByRole('button', { name: 'Save and add' }).click();
    await expect(page.getByText('@transformer').first()).toBeVisible();

    const notice = page.getByRole('dialog', { name: 'Set up R to run R blocks' });
    if (setup.ready) {
      // R is set up on this server: the block is made and nothing interrupts.
      await page.waitForTimeout(2000);
      await expect(notice).toHaveCount(0);
      return;
    }
    await expect(notice).toBeVisible();
    await expect(notice.getByText('mage r setup')).toBeVisible();
    await expect(notice.getByRole('link', { name: 'R setup guide' }))
      .toHaveAttribute('href', /guides\/blocks\/r-setup\.md$/);
    await expect(notice.getByText(setup.problems[0])).toBeVisible();

    // "Don't show this again" keeps the next R block quiet.
    await notice.getByText("Don't show this again for R").click();
    await notice.getByRole('button', { name: 'Got it' }).click();
    await expect(notice).toHaveCount(0);
    expect(await page.evaluate(() => window.localStorage.getItem(
      'mage_language_setup_notice_hidden_r',
    ))).toBe('1');
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
