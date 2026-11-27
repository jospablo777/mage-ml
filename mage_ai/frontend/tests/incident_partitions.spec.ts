import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * A failed partition's run shows the same partition of each upstream pipeline its
 * partitions declare: here the upstream completed eu and never ran us.
 */

const SUFFIX = Date.now().toString(36);
const UPSTREAM = `ui_incident_up_${SUFFIX}`;
const PARTITIONS = { key: 'region', values: ['eu', 'us'] };

let apiContext: APIRequestContext;
let api: MageApi;
let upstreamUUID: string;
let pipelineUUID: string;

async function runPartitions(uuid: string, select: string[]): Promise<number[]> {
  const { pipeline_partition: partitions } = await api.call(
    'POST', 'pipeline_partitions', { pipeline_partition: { action: 'run', pipeline_uuid: uuid, select } },
  );
  const ids = partitions.result.created.map(({ run_id: id }) => id);
  for (const id of ids) {
    await expect.poll(async () => (
      await api.call('GET', `pipeline_runs/${id}`)
    ).pipeline_run.status, { intervals: [2000], timeout: 120_000 })
      .toMatch(/^(completed|failed|cancelled)$/);
  }

  return ids;
}

test.beforeAll(async ({ playwright }, testInfo) => {
  test.setTimeout(300_000);
  apiContext = await playwright.request.newContext({
    baseURL: process.env.MAGE_E2E_API_URL || testInfo.project.use.baseURL,
  });
  api = await MageApi.signIn(apiContext);
  upstreamUUID = await api.createPipeline(UPSTREAM);
  await api.addBlock(upstreamUUID, {
    content: `
@data_loader
def load(*args, **kwargs):
    return [{'region': kwargs['region']}]
`,
    name: `up_load_${SUFFIX}`,
    type: 'data_loader',
  });
  await api.call('PUT', `pipelines/${upstreamUUID}`, { pipeline: { partitions: PARTITIONS } });

  pipelineUUID = await api.createPipeline(`ui_incident_down_${SUFFIX}`);
  await api.addBlock(pipelineUUID, {
    content: `
@data_loader
def load(*args, **kwargs):
    raise ValueError('no rows for ' + kwargs['region'])
`,
    name: `down_load_${SUFFIX}`,
    type: 'data_loader',
  });
  await api.call('PUT', `pipelines/${pipelineUUID}`, {
    pipeline: { partitions: { ...PARTITIONS, upstream: [upstreamUUID] } },
  });
});

test.afterAll(async () => {
  await api.deletePipeline(pipelineUUID).catch(() => null);
  await api.deletePipeline(upstreamUUID).catch(() => null);
  await apiContext.dispose();
});

test('a failed partition shows its upstream partitions', async ({ page }) => {
  test.setTimeout(300_000);
  const [upstreamRun] = await runPartitions(upstreamUUID, ['eu']);
  const [euRun, usRun] = await runPartitions(pipelineUUID, ['eu', 'us']);

  await page.goto(`/pipelines/${pipelineUUID}/runs/${euRun}`);
  const incident = page.getByTestId('incident');
  await expect(incident.getByText('Partition eu upstream:')).toBeVisible({ timeout: 60_000 });
  await expect(incident.getByText(/completed in/)).toBeVisible();
  await incident.getByRole('link', { name: `run ${upstreamRun}` }).click();
  await expect(page).toHaveURL(new RegExp(`/pipelines/${upstreamUUID}/runs/${upstreamRun}$`));

  await page.goto(`/pipelines/${pipelineUUID}/runs/${usRun}`);
  await expect(incident.getByText('Partition us upstream:')).toBeVisible({ timeout: 60_000 });
  await expect(incident.getByText('has not run')).toBeVisible();
});
