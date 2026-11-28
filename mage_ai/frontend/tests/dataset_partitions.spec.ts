import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * A dataset of a partitioned pipeline shows its coverage: partitions done, and the others
 * by state. Here eu completed, apac failed and us never ran.
 */

const SUFFIX = Date.now().toString(36);
const DATASET = `regional_${SUFFIX}`;
const LOADER = `regional_load_${SUFFIX}`;

let apiContext: APIRequestContext;
let api: MageApi;
let pipelineUUID: string;

test.beforeAll(async ({ playwright }, testInfo) => {
  test.setTimeout(300_000);
  apiContext = await playwright.request.newContext({
    baseURL: process.env.MAGE_E2E_API_URL || testInfo.project.use.baseURL,
  });
  api = await MageApi.signIn(apiContext);
  pipelineUUID = await api.createPipeline(`ui_dataset_partitions_${SUFFIX}`);
  await api.addBlock(pipelineUUID, {
    content: `
import pandas as pd

@data_loader
def load(*args, **kwargs):
    if kwargs['region'] == 'apac':
        raise ValueError('no rows for apac')
    return pd.DataFrame({'region': [kwargs['region']], 'sales': [10]})
`,
    name: LOADER,
    type: 'data_loader',
  });
  await api.call('PUT', `pipelines/${pipelineUUID}`, {
    pipeline: { partitions: { key: 'region', values: ['eu', 'us', 'apac'] } },
  });
  const { pipeline_partition: partitions } = await api.call('POST', 'pipeline_partitions', {
    pipeline_partition: { action: 'run', pipeline_uuid: pipelineUUID, select: ['eu', 'apac'] },
  });
  for (const { run_id: id } of partitions.result.created) {
    await expect.poll(async () => (
      await api.call('GET', `pipeline_runs/${id}`)
    ).pipeline_run.status, { intervals: [2000], timeout: 120_000 })
      .toMatch(/^(completed|failed|cancelled)$/);
  }
  await api.call('POST', 'datasets', {
    dataset: { block: LOADER, id: DATASET, pipeline: pipelineUUID },
  });
});

test.afterAll(async () => {
  await api.call('DELETE', `datasets/${DATASET}`).catch(() => null);
  await api.deletePipeline(pipelineUUID).catch(() => null);
  await apiContext.dispose();
});

test('a partitioned dataset shows which partitions are done', async ({ page }) => {
  test.setTimeout(120_000);
  await page.goto(`/datasets?id=${DATASET}`);
  const coverage = page.getByRole('region', { name: `Dataset ${DATASET}` })
    .getByRole('group', { name: 'Partition coverage' });
  await expect(coverage.getByText('1 of 3 partitions done; 1 failed, 1 missing'))
    .toBeVisible({ timeout: 60_000 });
  await expect(coverage.getByText('Failed: apac')).toBeVisible();
  await expect(coverage.getByText('Missing: us')).toBeVisible();
  await expect(coverage.getByTitle('region eu: done')).toBeVisible();
  await expect(coverage.getByRole('link', { name: 'Partitions' }))
    .toHaveAttribute('href', `/pipelines/${pipelineUUID}/partitions`);
});
