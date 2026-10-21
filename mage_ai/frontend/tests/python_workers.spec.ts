import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * Python workers in the pipeline settings: the project's workers are listed, one is chosen
 * for the pipeline's Python blocks, Prepare builds it from its uv.lock, and a run's blocks
 * run in it.
 */

const SUFFIX = Date.now().toString(36);
const WORKER = `e2e-tools-${SUFFIX}`;
const FOLDER = `python_workers/${WORKER}`;
// No dependencies: preparing needs no network.
const PYPROJECT = `[project]
name = "${WORKER}"
version = "0"
requires-python = ">=3.12"
dependencies = []
`;
const LOCK = `version = 1
revision = 3
requires-python = ">=3.12"

[[package]]
name = "${WORKER}"
version = "0"
source = { virtual = "." }
`;
const LOADER = `worker_load_${SUFFIX}`;
// Plain Python: the worker has no pandas, and the output is JSON.
const LOAD = `import sys


@data_loader
def load(*args, **kwargs):
    return {'python': sys.executable}
`;

let apiContext: APIRequestContext;
let api: MageApi;
let pipelineUUID: string;

test.beforeAll(async ({ playwright }, testInfo) => {
  apiContext = await playwright.request.newContext({
    baseURL: process.env.MAGE_E2E_API_URL || testInfo.project.use.baseURL,
  });
  api = await MageApi.signIn(apiContext);
  await api.writeFile(FOLDER, 'pyproject.toml', PYPROJECT);
  pipelineUUID = await api.createPipeline(`ui_worker_${SUFFIX}`);
  await api.addBlock(pipelineUUID, { content: LOAD, name: LOADER, type: 'data_loader' });
});

test.afterAll(async () => {
  await api.deletePipeline(pipelineUUID);
  await api.call('DELETE', `files/${encodeURIComponent(FOLDER)}`).catch(() => null);
  await apiContext.dispose();
});

test('a pipeline runs its Python blocks in a prepared worker', async ({ page }) => {
  await page.goto(`/pipelines/${pipelineUUID}/settings`);
  const row = page.locator('div')
    .filter({ has: page.getByText('Python worker', { exact: true }) })
    .filter({ has: page.locator('select') })
    .last();
  await row.locator('select').selectOption(WORKER);
  // Without a lock it cannot be prepared, and says what to do.
  await expect(page.getByText(`It has no uv.lock: run uv lock in ${FOLDER}`)).toBeVisible();
  await expect(page.getByRole('button', { name: `Prepare ${WORKER}` })).toBeDisabled();
  await page.getByRole('button', { name: 'Save pipeline settings' }).click();
  await expect.poll(async () => (
    await api.call('GET', `pipelines/${pipelineUUID}`)
  ).pipeline.environment).toEqual({ worker: WORKER });

  // Locked, the page prepares it and shows what uv printed.
  await api.writeFile(FOLDER, 'uv.lock', LOCK);
  await page.reload();
  await page.getByRole('button', { name: `Prepare ${WORKER}` }).click();
  await expect(page.getByText(/Prepared: \d+ packages from its uv.lock/)).toBeVisible({
    timeout: 120_000,
  });

  const run = await api.runOnce(pipelineUUID, `worker_${SUFFIX}`);
  expect(run.status).toBe('completed');
  const { block_runs: blockRuns } = await api.call('GET', `pipeline_runs/${run.id}/block_runs`);
  expect(blockRuns.map(b => b.status)).toEqual(['completed']);
});
