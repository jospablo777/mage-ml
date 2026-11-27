import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * R and Rust on the Workers page: both listed, the Rust environment set up from its page,
 * a crate that does not exist refused with cargo's reason while the file stays as it was,
 * and a crate that resolves saved and listed. R says how to set it up or what stops it.
 */

let apiContext: APIRequestContext;
let api: MageApi;
let original: string = null;

test.beforeAll(async ({ playwright }, testInfo) => {
  apiContext = await playwright.request.newContext({ baseURL: testInfo.project.use.baseURL });
  api = await MageApi.signIn(apiContext);
});

test.afterAll(async () => {
  if (original !== null) {
    await api.call('PUT', 'language_environments/rust', {
      language_environment: { manifest_text: original },
    }).catch(() => null);
  }
  await apiContext.dispose();
});

// A line after `anyhow = "1"`, inserted without Monaco closing the quotes as they are typed.
async function addCrate(page, line: string) {
  const editor = page.locator('.monaco-editor').first();
  await editor.getByText('anyhow = "1"', { exact: true }).click();
  await page.keyboard.press('End');
  await page.keyboard.press('Enter');
  await page.keyboard.insertText(line);
}

test('the Rust environment is set up and its crates changed from the Workers page', async ({
  page,
}) => {
  test.setTimeout(300_000);
  await page.goto('/workers');
  const table = page.getByRole('table', { name: 'R and Rust' });
  await expect(table).toBeVisible();
  await expect(table.getByRole('link', { exact: true, name: 'Open R' })).toBeVisible();
  await table.getByRole('link', { name: 'Open Rust' }).click();
  await expect(page).toHaveURL(/\/workers\/languages\/rust$/);

  const setUp = page.getByRole('button', { name: 'Set up the Rust environment' });
  const crates = page.getByRole('table', { name: 'Packages' });
  await expect(setUp.or(crates)).toBeVisible({ timeout: 60_000 });
  if (await setUp.isVisible()) {
    await setUp.click();
  }
  await expect(crates.getByText('mage-block', { exact: true })).toBeVisible({ timeout: 60_000 });

  const shown = await api.call('GET', 'language_environments/rust');
  original = shown.language_environment.manifest_text;
  expect(original).toContain('anyhow = "1"');

  // A crate that does not exist: cargo's reason, and the file as it was.
  await addCrate(page, 'no-such-crate-mageml-e2e = "1"');
  await page.getByRole('button', { name: 'Check and save' }).click();
  const refused = page.getByRole('alert').filter({ hasText: 'Not saved' });
  await expect(refused).toContainText('cargo cannot resolve these crates; nothing changed.', {
    timeout: 120_000,
  });
  await refused.getByText('Show cargo\'s output').click();
  await expect(refused).toContainText('no matching package named `no-such-crate-mageml-e2e`');
  const after = await api.call('GET', 'language_environments/rust');
  expect(after.language_environment.manifest_text).toEqual(original);

  // One that resolves: saved, and listed.
  await page.getByRole('button', { name: 'Discard changes' }).click();
  await addCrate(page, 'itoa = "1"');
  await page.getByRole('button', { name: 'Check and save' }).click();
  await expect(page.getByText('Saved. Rust blocks build with it at their next run.'))
    .toBeVisible({ timeout: 120_000 });
  await expect(crates.getByText('itoa', { exact: true })).toBeVisible();
});

test('the R environment says how to set it up or what stops its blocks', async ({ page }) => {
  await page.goto('/workers/languages/r');
  await expect(page.getByText('R', { exact: true }).first()).toBeVisible();
  const commands = page.getByLabel('Commands');
  await expect(commands).toContainText('mage r', { timeout: 60_000 });
  const shown = (await api.call('GET', 'language_environments/r')).language_environment;
  // The state the server reports: on a machine without R, what stops R blocks.
  const label = shown.problems?.length ? 'Needs attention' : (
    shown.exists ? 'Ready' : 'Not set up');
  await expect(page.getByText(label, { exact: true })).toBeVisible();
  if (!shown.exists) {
    await expect(commands).toContainText('mage r init');
  }
  for (const problem of shown.problems || []) {
    await expect(page.getByText(problem, { exact: true })).toBeVisible();
  }
});

test('a named Rust environment is created, chosen in a block header and removed', async ({
  page,
}) => {
  test.setTimeout(180_000);
  // A leftover from an interrupted run.
  await api.call('DELETE', 'language_environments/rust.e2e_named').catch(() => null);
  await page.goto('/workers');
  await page.getByRole('button', { name: 'New environment' }).click();
  await page.getByLabel('Language').selectOption('rust');
  await page.getByLabel('Name').fill('Bad Name');
  await expect(page.getByText('Lowercase letters, digits, _ and -')).toBeVisible();
  await expect(page.getByRole('button', { name: 'Create environment' })).toBeDisabled();
  await page.getByLabel('Name').fill('e2e_named');
  await page.getByLabel('Start from').selectOption('minimal');
  await page.getByRole('button', { name: 'Create environment' }).click();

  await expect(page).toHaveURL(/\/workers\/languages\/rust\.e2e_named$/, { timeout: 60_000 });
  await expect(page.getByText('Rust e2e_named', { exact: true })).toBeVisible();
  await expect(page.getByText(/Rust blocks run here when they choose e2e_named/)).toBeVisible();
  await expect(page.getByRole('table', { name: 'Packages' }).getByText('mage-block', {
    exact: true,
  })).toBeVisible({ timeout: 60_000 });
  await expect(page.getByText('No pipeline\'s blocks choose it yet.')).toBeVisible();

  // A Rust block chooses it in its header.
  const pipeline = await api.createPipeline(`e2e named rust ${Date.now()}`);
  try {
    await api.addBlock(pipeline, {
      content: 'use mage::prelude::*;\n\nfn transform(d: LazyFrame) -> LazyFrame {\n    d\n}\n',
      name: 'fast',
      language: 'rust',
      type: 'transformer',
    });
    await page.goto(`/pipelines/${pipeline}/edit`);
    const chip = page.getByRole('button', { name: 'Runs in: Rust default' });
    await expect(chip).toBeVisible({ timeout: 60_000 });
    await chip.click();
    await page.getByText('e2e_named', { exact: true }).click();
    await expect(page.getByRole('button', { name: 'Runs in: e2e_named' })).toBeVisible();
    await expect.poll(async () => {
      const shown = await api.call('GET', 'language_environments/rust.e2e_named');
      return (shown.language_environment.pipelines || []).map(p => p.blocks.join(','));
    }, { timeout: 30_000 }).toEqual(['fast']);

    // In use: not removed, and the page says by what.
    await page.goto('/workers/languages/rust.e2e_named');
    await expect(page.getByRole('table', { name: 'Pipelines that use it' })).toContainText('fast');
    await expect(page.getByRole('button', { name: 'Remove environment' })).toBeDisabled();
  } finally {
    await api.deletePipeline(pipeline);
  }

  await page.goto('/workers');
  const table = page.getByRole('table', { name: 'R and Rust' });
  await expect(table.getByRole('link', { name: 'Open Rust e2e_named' })).toBeVisible();
  await table.getByRole('link', { name: 'Open Rust e2e_named' }).click();

  await page.getByRole('button', { name: 'Remove environment' }).click();
  await page.getByRole('button', { name: 'Yes, remove it' }).click();
  await expect(page).toHaveURL(/\/workers$/, { timeout: 60_000 });
  await expect(table.getByRole('link', { name: 'Open Rust e2e_named' })).toHaveCount(0);
});
