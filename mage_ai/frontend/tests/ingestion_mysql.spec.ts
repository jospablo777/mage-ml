import { execFileSync } from 'child_process';
import path from 'path';
import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * Guided ingestion from MySQL (ADR-0048): the page checks the binary log's settings, lists
 * the tables with their keys, copies one into PostgreSQL and starts the sync, the copy's
 * progress shown until it is done. Needs MySQL with binlog_row_metadata=FULL
 * (MAGE_E2E_MYSQL=host:port:user:password), PostgreSQL (MAGE_E2E_POSTGRES=host:port:database:
 * user:password) and an engine built with the mysql feature (MAGE_ENGINE_PATH on the server).
 */

const MYSQL = process.env.MAGE_E2E_MYSQL;
const POSTGRES = process.env.MAGE_E2E_POSTGRES;
const SUFFIX = Date.now().toString(36);
const DATABASE = `ui_mysql_${SUFFIX}`;
const SCHEMA = `ui_mysql_copy_${SUFFIX}`;
const PIPELINE = `ui_mysql_sync_${SUFFIX}`;
const PYTHON = process.env.MAGE_E2E_PYTHON
  || path.resolve(__dirname, '../../../.venv/bin/python');

function mysql(statements: string[]) {
  const [host, port, user, password] = MYSQL.split(':');
  execFileSync(PYTHON, ['-c', `
import json, sys
import mysql.connector
db = mysql.connector.connect(host=sys.argv[1], port=int(sys.argv[2]), user=sys.argv[3],
                             password=sys.argv[4], autocommit=True)
cursor = db.cursor()
for statement in json.loads(sys.argv[5]):
    cursor.execute(statement)
`, host, port, user, password, JSON.stringify(statements)]);
}

function postgres(statement: string): string {
  const [host, port, database, user, password] = POSTGRES.split(':');
  return execFileSync(PYTHON, ['-c', `
import psycopg2, sys
db = psycopg2.connect(host=sys.argv[1], port=sys.argv[2], dbname=sys.argv[3], user=sys.argv[4],
                      password=sys.argv[5], sslmode='disable')
db.autocommit = True
cursor = db.cursor()
cursor.execute(sys.argv[6])
if cursor.description:
    print('\\n'.join(str(row[0]) for row in cursor.fetchall()))
`, host, port, database, user, password, statement]).toString().trim();
}

let apiContext: APIRequestContext;
let api: MageApi;

test.beforeAll(async ({ playwright }, testInfo) => {
  test.skip(!MYSQL || !POSTGRES, 'MAGE_E2E_MYSQL and MAGE_E2E_POSTGRES name the servers');
  mysql([
    `CREATE DATABASE ${DATABASE}`,
    `CREATE TABLE ${DATABASE}.orders (id INT PRIMARY KEY, item VARCHAR(20), price DECIMAL(6,2))`,
    `CREATE TABLE ${DATABASE}.notes (body TEXT)`,
    `INSERT INTO ${DATABASE}.orders VALUES (1, 'tea', 2.50), (2, 'cake', 4.00)`,
  ]);
  apiContext = await playwright.request.newContext({
    baseURL: process.env.MAGE_E2E_API_URL || testInfo.project.use.baseURL,
  });
  api = await MageApi.signIn(apiContext);
});

test.afterAll(async () => {
  if (!MYSQL || !POSTGRES) {
    return;
  }
  await api.call('DELETE', `pipelines/${PIPELINE}/stream_runs/current`).catch(() => null);
  await api.deletePipeline(PIPELINE).catch(() => null);
  await apiContext.dispose();
  mysql([`DROP DATABASE IF EXISTS ${DATABASE}`]);
  postgres(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`);
});

test('a MySQL table is checked, copied into PostgreSQL and kept in step', async ({ page }) => {
  test.setTimeout(180_000);
  const [host, port, user, password] = MYSQL.split(':');
  const [pgHost, pgPort, pgDatabase, pgUser, pgPassword] = POSTGRES.split(':');
  await page.goto('/ingestion');
  await expect(page.getByText('Copy a database into PostgreSQL and keep it in step'))
    .toBeVisible({ timeout: 60_000 });
  await page.getByLabel('Source type').selectOption('mysql');
  await expect(page.getByLabel('Source port')).toHaveValue('3306');
  await page.getByLabel('Source host').fill(host);
  await page.getByLabel('Source port').fill(port);
  await page.getByLabel('Source database').fill(DATABASE);
  await page.getByLabel('Source user').fill(user);
  await page.getByLabel('Source password').fill(password);
  await page.getByLabel('Source TLS').selectOption('disable');
  await page.getByRole('button', { name: 'Check the source' }).click();

  await expect(page.getByText(/Ready: binlog_row_metadata/)).toBeVisible({ timeout: 30_000 });
  await expect(page.getByRole('group', { name: `${DATABASE}.notes` })
    .getByText('no primary key')).toBeVisible();
  await page.getByRole('group', { name: `${DATABASE}.orders` }).getByText('Sample').click();
  await expect(page.getByRole('table', { name: `Sample of ${DATABASE}.orders` })
    .getByText('cake')).toBeVisible();
  await page.getByText(`${DATABASE}.orders`, { exact: true }).click();

  // A MySQL source goes into another server: PostgreSQL.
  await expect(page.getByText('The same server as the source')).toHaveCount(0);
  await page.getByLabel('Destination host').fill(pgHost);
  await page.getByLabel('Destination port').fill(pgPort);
  await page.getByLabel('Destination database').fill(pgDatabase);
  await page.getByLabel('Destination user').fill(pgUser);
  await page.getByLabel('Destination password').fill(pgPassword);
  await page.getByLabel('Destination TLS').selectOption('disable');
  await page.getByLabel('Destination schema').fill(SCHEMA);
  await page.getByRole('button', { name: 'Check the destination' }).click();
  await expect(page.getByText(/does not exist; it will be made/)).toBeVisible();

  await page.getByRole('button', { name: 'Review what the sync will do' }).click();
  await expect(page.getByRole('list', { name: 'What the sync does' })
    .getByText('(the binary log)', { exact: false })).toBeVisible();
  await page.getByLabel('Pipeline name').fill(PIPELINE);
  await page.getByRole('button', { name: 'Create the pipeline' }).click();
  await expect(page.getByText(`Created ${PIPELINE}.`, { exact: false })).toBeVisible();

  page.on('dialog', dialog => dialog.accept());
  await page.getByRole('button', { name: 'Start on the local engine' }).click();
  await expect(page.getByText('Initial copy done: 1 table, 2 rows'))
    .toBeVisible({ timeout: 90_000 });
  expect(postgres(`SELECT item || ' ' || price FROM ${SCHEMA}.copy_orders ORDER BY id`))
    .toBe('tea 2.50\ncake 4.00');

  // A change after the copy follows.
  mysql([`UPDATE ${DATABASE}.orders SET item = 'green tea' WHERE id = 1`]);
  await expect.poll(
    () => postgres(`SELECT item FROM ${SCHEMA}.copy_orders WHERE id = 1`),
    { intervals: [1000], timeout: 30_000 },
  ).toBe('green tea');
  await page.getByRole('button', { name: 'Stop' }).click();
  await expect(page.getByText('Stopped', { exact: true })).toBeVisible({ timeout: 60_000 });
});
