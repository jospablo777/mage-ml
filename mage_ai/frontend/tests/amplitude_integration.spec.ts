import { ChildProcess, spawn } from 'child_process';
import path from 'path';

import { APIRequestContext } from '@playwright/test';
import { parse } from 'yaml';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * An Amplitude data integration pipeline set up in the editor, against a local server that
 * answers as Amplitude's APIs do (`mage examples amplitude-server`): pick the source,
 * read its documentation, type the settings, test the connection, pick streams that come
 * with their keys and sync mode, and preview the events; then pick Amplitude as the
 * destination and test its API key. Runs are tested in
 * integration_tests/postgres/test_amplitude_pipeline.py and test_amplitude_destination.py.
 */

const PORT = Number(process.env.MAGE_E2E_AMPLITUDE_PORT || 18766);
const AMPLITUDE_URL = `http://127.0.0.1:${PORT}`;
const SHOTS = process.env.MAGE_E2E_SCREENSHOTS;
const SUFFIX = Date.now().toString(36);
const REPO = path.resolve(__dirname, '..', '..', '..');

let apiContext: APIRequestContext;
let api: MageApi;
let amplitude: ChildProcess;
let pipelineUUID: string;

async function shot(page, name: string) {
  if (SHOTS) {
    await page.screenshot({ fullPage: true, path: `${SHOTS}/${name}.png` });
  }
}

test.beforeAll(async ({ playwright }, testInfo) => {
  amplitude = spawn(
    process.env.MAGE_E2E_PYTHON || 'python3',
    ['-m', 'mage_ai.cli.main', 'examples', 'amplitude-server', '--port', String(PORT)],
    { cwd: REPO, stdio: 'ignore' },
  );
  await expect.poll(async () => {
    try {
      return (await fetch(`${AMPLITUDE_URL}/api/2/taxonomy/category`)).status;
    } catch {
      return 0;
    }
  }, { timeout: 30_000 }).toBe(403);

  apiContext = await playwright.request.newContext({
    baseURL: process.env.MAGE_E2E_API_URL || testInfo.project.use.baseURL,
  });
  api = await MageApi.signIn(apiContext);
  const created = await api.call('POST', 'pipelines', {
    pipeline: { name: `amplitude_ui_${SUFFIX}`, type: 'integration' },
  });
  pipelineUUID = created.pipeline.uuid;
});

test.afterAll(async () => {
  amplitude?.kill();
  if (pipelineUUID) {
    await api.deletePipeline(pipelineUUID);
  }
  await apiContext?.dispose();
});

test('an Amplitude source is set up, tested and previewed in the editor', async ({ page }) => {
  test.setTimeout(300_000);
  await page.goto(`/pipelines/${pipelineUUID}/edit`);

  // Picking the source makes its block at once and shows its settings and documentation.
  const sourceSelect = page.locator('select').first();
  await expect(sourceSelect).toBeVisible({ timeout: 60_000 });
  await sourceSelect.selectOption('amplitude');
  await expect(page.getByText('Documentation: amplitude')).toBeVisible({ timeout: 30_000 });
  await expect(page.getByRole('button', { name: 'Save and add' })).toHaveCount(0);
  await expect(page.getByText(`${pipelineUUID}_source: amplitude`).first()).toBeVisible();
  await page.getByText('Documentation: amplitude').click();
  await expect(page.getByText('How events sync')).toBeVisible();
  await shot(page, '01_source_and_docs');
  await page.getByText('Documentation: amplitude').click();

  // Empty keys: the connection test says what is missing and where to find it.
  await page.getByRole('button', { name: 'Test connection' }).click();
  await expect(page.getByText('Settings > Projects > your project > General')).toBeVisible({
    timeout: 30_000,
  });

  // The settings, typed as a user does.
  const editor = page.locator('.monaco-editor').first();
  // The template's text cleared from its end, without platform shortcuts.
  await editor.scrollIntoViewIfNeeded();
  await editor.getByText('cohort_ids').click();
  await page.keyboard.press('End');
  const template = await editor.locator('.view-lines').innerText();
  for (let i = 0; i < template.length + 10; i++) {
    await page.keyboard.press('Backspace');
  }
  await page.keyboard.type([
    'api_key: test-api-key',
    'secret_key: test-secret-key',
    'region: US',
    `host: ${AMPLITUDE_URL}`,
    'start_date: 2024-01-01',
  ].join('\n'));
  await shot(page, '01b_typed');
  await page.getByRole('button', { name: 'Test connection' }).click({ timeout: 10_000 });
  await expect(page.getByText('Connected successfully!')).toBeVisible({ timeout: 30_000 });
  await shot(page, '02_connected');

  // Streams say what they hold; three are picked.
  await page.getByRole('button', { name: 'View and select streams' }).click();
  await expect(page.getByText('Select streams to sync')).toBeVisible({ timeout: 60_000 });
  await expect(page.getByText('Every event with its event and user properties')).toBeVisible();
  await shot(page, '03_streams');
  for (const stream of ['events', 'active_users', 'event_types']) {
    await page.locator('tr').filter({ has: page.getByText(stream, { exact: true }) })
      .locator('label').first().click();
  }
  await page.getByRole('button', { name: 'Confirm 3 streams' }).click();
  await expect(page.getByText('Select streams to sync')).toHaveCount(0);
  await shot(page, '04_selected');

  // Each stream comes with its keys and sync mode, so nothing needs setting by hand.
  const catalogStreams = async () => {
    const detail = await api.call(
      'GET', `pipelines/${pipelineUUID}?includes_content=true&include_block_catalog=true`,
    );
    const source = detail.pipeline.blocks.find(({ type }) => type === 'data_loader');
    expect(source.content).toContain(`host: ${AMPLITUDE_URL}`);
    const content = parse(source.content || '') || {};
    const catalog = (content.catalog?.streams?.length ? content.catalog : null)
      || detail.pipeline.data_integration?.catalog;
    return Object.fromEntries((catalog?.streams || []).map(s => [s.tap_stream_id, s]));
  };
  await expect.poll(async () => Object.keys(await catalogStreams()).sort(), {
    timeout: 60_000,
  }).toEqual(['active_users', 'event_types', 'events']);
  const streams = await catalogStreams();
  await page.waitForTimeout(3000);
  await shot(page, '04b_settings');
  expect(streams.events.replication_method).toBe('INCREMENTAL');
  expect(streams.events.unique_constraints).toEqual(['uuid']);
  expect(streams.events.unique_conflict_method).toBe('UPDATE');

  // The events of the newest complete hour, previewed.
  await page.getByRole('button', { exact: true, name: 'events' }).click();
  await expect(page.getByText('events schema')).toBeVisible();
  await shot(page, '05_events_settings');
  await page.getByRole('button', { name: 'Load sample data' }).first().click();
  // The newest events, in the Data panel.
  await expect(page.getByText(/^insert-\d+$/).first()).toBeVisible({ timeout: 60_000 });
  await shot(page, '06_sample');
});

test('an Amplitude destination is set up and its API key tested', async ({ page }) => {
  test.setTimeout(180_000);
  await page.goto(`/pipelines/${pipelineUUID}/edit`);

  // The destination list shows once the source is set (the test above).
  const destinationSelect = page.locator('select')
    .filter({ has: page.locator('option[value="delta_lake_s3"]') });
  await expect(destinationSelect).toBeVisible({ timeout: 60_000 });
  await destinationSelect.selectOption('amplitude');
  await expect(page.getByText(`${pipelineUUID}_destination: amplitude`).first()).toBeVisible({
    timeout: 30_000,
  });
  const docs = page.getByText('Documentation: amplitude').last();
  await docs.click();
  await expect(page.getByText('Sent once')).toBeVisible();
  await shot(page, '07_destination_docs');
  await docs.click();

  // The template reads the key from AMPLITUDE_API_KEY, unset here: the test says so.
  const testButton = page.getByRole('button', { name: 'Test connection' }).last();
  await testButton.click();
  await expect(page.getByText(/api_key rendered as None/)).toBeVisible({
    timeout: 30_000,
  });

  const editor = page.locator('.monaco-editor').filter({ hasText: 'streams' }).last();
  await editor.scrollIntoViewIfNeeded();
  await editor.getByText('streams').click();
  await page.keyboard.press('End');
  const template = await editor.locator('.view-lines').innerText();
  for (let i = 0; i < template.length + 10; i++) {
    await page.keyboard.press('Backspace');
  }
  // Flow style: the editor indents typed new lines by itself.
  await page.keyboard.type([
    'api_key: test-api-key',
    `host: ${AMPLITUDE_URL}`,
    'streams: {active_users: {user_properties: true}}',
  ].join('\n'));
  await testButton.click({ timeout: 10_000 });
  await expect(page.getByText('Connected successfully!')).toBeVisible({ timeout: 30_000 });
  await shot(page, '08_destination_connected');

  await expect.poll(async () => {
    const detail = await api.call('GET', `pipelines/${pipelineUUID}?includes_content=true`);
    const destination = detail.pipeline.blocks.find(({ type }) => type === 'data_exporter');
    return parse(destination?.content || '') || {};
  }, { timeout: 30_000 }).toMatchObject({
    config: { api_key: 'test-api-key', streams: { active_users: { user_properties: true } } },
    destination: 'amplitude',
  });
});
