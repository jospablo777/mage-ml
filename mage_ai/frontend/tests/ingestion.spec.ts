import { execFileSync } from 'child_process';
import path from 'path';
import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * Guided ingestion: the page checks a PostgreSQL source, lists its tables with their keys
 * (a table without one cannot be chosen), samples one, checks the destination, says what
 * the sync will do, creates the pipeline, and starts it there, the copy's progress table by
 * table. Needs a server with logical replication,
 * MAGE_E2E_POSTGRES=host:port:database:user:password, and an engine built with the
 * PostgreSQL connector (MAGE_ENGINE_PATH on the server).
 */

const SERVER = process.env.MAGE_E2E_POSTGRES;
const SUFFIX = Date.now().toString(36);
const SCHEMA = `ui_ingest_${SUFFIX}`;
const PIPELINE = `ui_sync_${SUFFIX}`;
const PYTHON = process.env.MAGE_E2E_PYTHON
  || path.resolve(__dirname, '../../../.venv/bin/python');

// Runs the statements; the first column of the last one's rows, one per line.
function sql(statements: string): string {
  const [host, port, database, user, password] = SERVER.split(':');
  return execFileSync(PYTHON, ['-c', `
import psycopg2, sys
db = psycopg2.connect(host=sys.argv[1], port=sys.argv[2], dbname=sys.argv[3], user=sys.argv[4],
                      password=sys.argv[5], sslmode='disable')
db.autocommit = True
cursor = db.cursor()
cursor.execute(sys.argv[6])
if cursor.description:
    print('\\n'.join(str(row[0]) for row in cursor.fetchall()))
`, host, port, database, user, password, statements]).toString().trim();
}

let apiContext: APIRequestContext;
let api: MageApi;

test.beforeAll(async ({ playwright }, testInfo) => {
  test.skip(!SERVER, 'MAGE_E2E_POSTGRES names no PostgreSQL server');
  sql(`
    CREATE SCHEMA ${SCHEMA};
    CREATE TABLE ${SCHEMA}.orders (id integer PRIMARY KEY, item text);
    CREATE TABLE ${SCHEMA}.notes (body text);
    INSERT INTO ${SCHEMA}.orders VALUES (1, 'tea'), (2, 'cake');
  `);
  apiContext = await playwright.request.newContext({
    baseURL: process.env.MAGE_E2E_API_URL || testInfo.project.use.baseURL,
  });
  api = await MageApi.signIn(apiContext);
});

test.afterAll(async () => {
  if (!SERVER) {
    return;
  }
  await api.call('DELETE', `pipelines/${PIPELINE}/stream_runs/current`).catch(() => null);
  await api.deletePipeline(PIPELINE).catch(() => null);
  await apiContext.dispose();
  // The sync's slots (a run from the page reads with <slot>_dev_<user>) and publication.
  sql(`
    SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
      WHERE slot_name LIKE 'mageml_${PIPELINE}%';
    DROP PUBLICATION IF EXISTS mageml_${PIPELINE};
    DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE;
    DROP SCHEMA IF EXISTS ${SCHEMA}_copy CASCADE;
  `);
});

test('a database sync is checked, reviewed and created as a pipeline', async ({ page }) => {
  test.setTimeout(120_000);
  const [host, port, database, user, password] = SERVER.split(':');
  await page.goto('/ingestion');
  await expect(page.getByText('Copy a database into PostgreSQL and keep it in step'))
    .toBeVisible({ timeout: 60_000 });
  await page.getByLabel('Source host').fill(host);
  await page.getByLabel('Source port').fill(port);
  await page.getByLabel('Source database').fill(database);
  await page.getByLabel('Source user').fill(user);
  await page.getByLabel('Source password').fill(password);
  await page.getByLabel('Source TLS').selectOption('disable');
  await page.getByRole('button', { name: 'Check the source' }).click();

  await expect(page.getByText(/Ready: logical replication/)).toBeVisible({ timeout: 30_000 });
  const orders = page.getByText(`${SCHEMA}.orders`, { exact: true });
  await expect(orders).toBeVisible();
  // A table without a key cannot mirror.
  await expect(page.getByRole('group', { name: `${SCHEMA}.notes` }).getByText('no primary key'))
    .toBeVisible();

  await page.getByRole('group', { name: `${SCHEMA}.orders` }).getByText('Sample').click();
  await expect(page.getByRole('table', { name: `Sample of ${SCHEMA}.orders` })
    .getByText('cake')).toBeVisible();

  await orders.click();
  await page.getByLabel('Destination schema').fill(`${SCHEMA}_copy`);
  await page.getByRole('button', { name: 'Check the destination' }).click();
  await expect(page.getByText(/does not exist; it will be made/)).toBeVisible();

  await page.getByRole('button', { name: 'Review what the sync will do' }).click();
  const review = page.getByRole('list', { name: 'What the sync does' });
  await expect(review.getByText(`Copies every row of ${SCHEMA}.orders first`, { exact: false }))
    .toBeVisible();
  await expect(review.getByText('A row deleted in the source is deleted in the copy.'))
    .toBeVisible();

  await page.getByLabel('Pipeline name').fill(PIPELINE);
  await page.getByRole('button', { name: 'Create the pipeline' }).click();
  await expect(page.getByText(`Created ${PIPELINE}.`, { exact: false })).toBeVisible();
  await expect(page.getByRole('link', { name: 'open it in the editor' }))
    .toHaveAttribute('href', `/pipelines/${PIPELINE}/edit`);

  // Started from the page: the sink it writes to is named first, and the copy shows table
  // by table until it is done.
  const asked = [];
  page.on('dialog', (dialog) => {
    asked.push(dialog.message());
    dialog.accept();
  });
  await page.getByRole('button', { name: 'Start on the local engine' }).click();
  await expect(page.getByText('Initial copy done: 1 table, 2 rows'))
    .toBeVisible({ timeout: 90_000 });
  expect(asked[0]).toContain(`${PIPELINE}_copy`);
  await expect(page.getByRole('list', { name: 'Initial copy' })
    .getByRole('listitem', { name: `${SCHEMA}.orders` })).toContainText('Done, 2 rows');
  expect(sql(`SELECT item FROM ${SCHEMA}_copy.copy_orders ORDER BY id`)).toBe('tea\ncake');
  await page.getByRole('button', { name: 'Stop' }).click();
  await expect(page.getByText('Stopped', { exact: true })).toBeVisible({ timeout: 60_000 });
});
