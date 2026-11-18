import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * The Workers page, from the home page's left navigation: a worker is created with its
 * packages and versions, prepared, its packages changed (a change that does not lock
 * changes nothing and says why), renamed, and deleted.
 */

const SUFFIX = Date.now().toString(36);
const NAME = `e2e-page-${SUFFIX}`;

let apiContext: APIRequestContext;
let api: MageApi;

test.beforeAll(async ({ playwright }, testInfo) => {
  apiContext = await playwright.request.newContext({ baseURL: testInfo.project.use.baseURL });
  api = await MageApi.signIn(apiContext);
});

test.afterAll(async () => {
  await api.call('DELETE', `folders/${encodeURIComponent(`python_workers/${NAME}`)}`)
    .catch(() => null);
  await apiContext.dispose();
});

test('a worker is created, changed and deleted from the Workers page', async ({ page }) => {
  test.setTimeout(300_000);
  await page.goto('/overview');
  // The navigation opens with its labels on hover, as people use it.
  await page.locator('[class*="NavigationItemStyle"]').first().hover();
  await page.getByText('Workers', { exact: true }).filter({ visible: true }).first().click();
  await expect(page).toHaveURL(/\/workers$/);
  await expect(page.getByText('Workers', { exact: true }).first()).toBeVisible();

  const add = page.getByRole('button', { name: /^(New worker|Add the first worker)$/ });
  await add.click();
  await page.getByRole('radio', { name: /^Empty/ }).click();
  await page.getByLabel('Name', { exact: true }).fill(NAME);
  await page.getByLabel('Display name').fill('Page worker');

  // Names people type that are not the package: translated, and said so.
  const quick = page.getByLabel('Add packages');
  await quick.fill('sklearn six==1.16.0');
  await quick.press('Enter');
  await expect(page.getByText('sklearn is installed as scikit-learn.')).toBeVisible();
  await page.getByRole('button', { name: 'Remove scikit-learn' }).click();
  await expect(page.getByLabel('Version of six')).toHaveValue('1.16.0', { timeout: 30_000 });

  await page.getByRole('button', { name: 'Create and prepare' }).click();
  await expect(page).toHaveURL(new RegExp(`/workers/${NAME}$`), { timeout: 120_000 });
  await expect(page.getByText('Page worker', { exact: true }).first()).toBeVisible();
  await expect(page.getByText('Ready', { exact: true }).first()).toBeVisible({
    timeout: 180_000,
  });

  // A version that does not exist: uv's reason, and nothing changed.
  await quick.fill('six==99.0');
  await quick.press('Enter');
  await page.getByRole('button', { name: 'Save and prepare' }).click();
  const alert = page.getByRole('alert').filter({ hasText: 'Not saved' });
  await expect(alert).toBeVisible({ timeout: 60_000 });
  await expect(alert).toContainText('so nothing changed');
  const { python_worker: unchanged } = await api.call('GET', `python_workers/${NAME}`);
  // The Empty template starts with pyarrow, for the tables that reach the worker.
  expect(unchanged.dependencies).toEqual(['pyarrow', 'six==1.16.0']);

  // Latest instead: the lock moves six on, and the page says from what to what.
  await page.getByLabel('Version of six').selectOption('');
  await page.getByRole('button', { name: 'Save and prepare' }).click();
  await expect(page.getByText(/Saved: 1 package changed/)).toBeVisible({ timeout: 120_000 });
  const { python_worker: latest } = await api.call('GET', `python_workers/${NAME}`);
  expect(latest.dependencies).toEqual(['pyarrow', 'six']);

  await page.getByRole('button', { name: /^Used by/ }).click();
  await expect(page.getByText('No block runs in it yet.')).toBeVisible();

  await page.getByRole('button', { name: /^Security/ }).click();
  await expect(page.getByText(/From uv audit, against the OSV database/)).toBeVisible({
    timeout: 120_000,
  });

  await page.getByRole('button', { name: /^Settings/ }).click();
  await page.getByLabel('Display name').fill('Renamed worker');
  await page.getByRole('button', { exact: true, name: 'Save' }).click();
  await expect(page.getByText('Renamed worker', { exact: true }).first()).toBeVisible();

  await page.getByRole('button', { name: 'Delete worker' }).click();
  await page.getByRole('button', { name: 'Yes, delete it' }).click();
  await expect(page).toHaveURL(/\/workers$/);
  const { python_workers: workers } = await api.call('GET', 'python_workers');
  expect(workers.map(w => w.name)).not.toContain(NAME);
});
