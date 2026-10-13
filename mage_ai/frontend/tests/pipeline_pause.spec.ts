import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * The play and pause button on the Pipelines page changes the status of a pipeline's
 * triggers and nothing else. It sent back the list's copy of the pipeline, which holds a
 * few of its settings, and the others were saved with their defaults in metadata.yaml.
 */

const SUFFIX = Date.now().toString(36);

let apiContext: APIRequestContext;
let api: MageApi;
let pipelineUUID: string;

test.beforeAll(async ({ playwright }, testInfo) => {
  apiContext = await playwright.request.newContext({
    baseURL: process.env.MAGE_E2E_API_URL || testInfo.project.use.baseURL,
  });
  api = await MageApi.signIn(apiContext);
  pipelineUUID = await api.createPipeline(`ui_pause_${SUFFIX}`);
  await api.call('PUT', `pipelines/${pipelineUUID}`, {
    pipeline: {
      block_fusion: 'off',
      retry_config: { delay: 7, retries: 3 },
      run_pipeline_in_one_process: true,
    },
  });
  // The button shows for pipelines with triggers. This one starts next year, so it never runs.
  await api.call('POST', `pipelines/${pipelineUUID}/pipeline_schedules`, {
    pipeline_schedule: {
      name: `pause_${SUFFIX}`,
      schedule_interval: '@daily',
      schedule_type: 'time',
      start_time: `${new Date().getUTCFullYear() + 1}-01-01 00:00:00`,
      status: 'inactive',
    },
  });
});

test.afterAll(async () => {
  await api.deletePipeline(pipelineUUID);
  await apiContext.dispose();
});

test('play and pause keep the pipeline settings', async ({ page }) => {
  const settings = async () => {
    const { pipeline } = await api.call('GET', `pipelines/${pipelineUUID}`);
    return {
      block_fusion: pipeline.block_fusion,
      retry_config: pipeline.retry_config,
      run_pipeline_in_one_process: pipeline.run_pipeline_in_one_process,
    };
  };
  const saved = await settings();
  expect(saved).toEqual({
    block_fusion: 'off',
    retry_config: { delay: 7, retries: 3 },
    run_pipeline_in_one_process: true,
  });

  await page.goto('/pipelines');
  await page.getByRole('textbox').first().fill(pipelineUUID);
  const row = page.getByRole('row').filter({ hasText: pipelineUUID });
  await expect(row).toHaveCount(1);

  for (const status of ['active', 'inactive']) {
    await row.getByRole('button').first().click();
    await expect.poll(async () => {
      const { pipeline_schedules: schedules } = await api.call(
        'GET', `pipelines/${pipelineUUID}/pipeline_schedules`,
      );
      return schedules.map(({ status: s }) => s);
    }).toEqual([status]);
    expect(await settings()).toEqual(saved);
  }
});
