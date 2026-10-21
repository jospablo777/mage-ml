import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

// A streaming pipeline started on the local engine from the editor's Run panel: the sinks
// are named before it starts, the panel shows its state, and it pauses, resumes and stops.
// Needs Kafka (MAGE_TEST_KAFKA_BOOTSTRAP) and an engine built with it (MAGE_ENGINE_PATH on
// the server).
const BOOTSTRAP = process.env.MAGE_TEST_KAFKA_BOOTSTRAP;
const SUFFIX = Date.now().toString(36);

let apiContext: APIRequestContext;
let api: MageApi;
let pipelineUUID: string;

test.skip(!BOOTSTRAP, 'set MAGE_TEST_KAFKA_BOOTSTRAP');

test.beforeAll(async ({ playwright }, testInfo) => {
  apiContext = await playwright.request.newContext({ baseURL: testInfo.project.use.baseURL });
  api = await MageApi.signIn(apiContext);
  const body = await api.call('POST', 'pipelines', {
    pipeline: { name: `stream_run_${SUFFIX}`, type: 'streaming' },
  });
  pipelineUUID = body.pipeline.uuid;
  await api.call('POST', `pipelines/${pipelineUUID}/blocks`, {
    block: {
      content: `connector_type: kafka\nbootstrap_server: ${BOOTSTRAP}\ntopic: run_in_${SUFFIX}\n`
        + 'consumer_group: run_panel\n',
      language: 'yaml',
      name: `run_source_${SUFFIX}`,
      type: 'data_loader',
    },
  });
  await api.call('POST', `pipelines/${pipelineUUID}/blocks`, {
    block: {
      content: `connector_type: kafka\nbootstrap_server: ${BOOTSTRAP}\ntopic: run_out_${SUFFIX}\n`,
      language: 'yaml',
      name: `run_sink_${SUFFIX}`,
      type: 'data_exporter',
      upstream_blocks: [`run_source_${SUFFIX}`],
    },
  });
});

test.afterAll(async () => {
  await api.call('DELETE', `pipelines/${pipelineUUID}/stream_runs/current`).catch(() => null);
  await api.deletePipeline(pipelineUUID);
  await apiContext.dispose();
});

test('a stream starts on the local engine, pauses, resumes and stops', async ({ page }) => {
  const asked = [];
  page.on('dialog', (dialog) => {
    asked.push(dialog.message());
    dialog.accept();
  });
  await page.goto(`/pipelines/${pipelineUUID}/edit?sideview=stream_run`);
  await expect(page.getByText('Not running')).toBeVisible();

  await page.getByRole('button', { name: 'Start on the local engine' }).click();

  await expect(page.getByText(/^Running · /)).toBeVisible({ timeout: 90000 });
  expect(asked[0]).toContain(`run_sink_${SUFFIX}`);
  await expect(page.getByText('run_panel-dev-', { exact: false })).toBeVisible();
  await expect(page.getByText('Engine log')).toBeVisible();
  await page.getByLabel('Tap point').selectOption('source');
  await expect(page.getByText('No message yet.')).toBeVisible();

  await page.getByRole('button', { name: 'Pause' }).click();
  await expect(page.getByText(/^Paused · /)).toBeVisible();
  await page.getByRole('button', { name: 'Resume' }).click();
  await expect(page.getByText(/^Running · /)).toBeVisible();
  await page.getByRole('button', { name: 'Stop' }).click();
  await expect(page.getByText('Stopped', { exact: true })).toBeVisible({ timeout: 60000 });
});

test('the run panel checks the connections before a start', async ({ page }) => {
  await page.goto(`/pipelines/${pipelineUUID}/edit?sideview=stream_run`);
  await page.getByRole('button', { name: 'Check connections' }).click();

  // The source's topic was never made: the check says so, and the sink's is missing too.
  await expect(page.getByText('1 of 2 connections failed: the stream cannot start'))
    .toBeVisible({ timeout: 90000 });
  await expect(page.getByText(`Topic run_in_${SUFFIX} does not exist.`)).toBeVisible();
  await expect(page.getByText('Look at this')).toBeVisible();
});
