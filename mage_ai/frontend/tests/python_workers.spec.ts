import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * Python workers in the pipeline settings: the project's workers are listed by the names
 * people gave them, one is chosen for the pipeline's Python blocks, Prepare builds it from
 * its uv.lock, and a run's blocks run in it.
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

[tool.mageml.worker]
label = "Tools worker"
description = "Plain Python for the e2e test"
`;
// How the pages name it: its label, with the alias YAML uses.
const SHOWN = `Tools worker (${WORKER})`;
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
  await api.call('DELETE', `folders/${encodeURIComponent(FOLDER)}`).catch(() => null);
  await apiContext.dispose();
});

test('a pipeline runs its Python blocks in a prepared worker', async ({ page }) => {
  await page.goto(`/pipelines/${pipelineUUID}/settings`);
  const row = page.locator('div')
    .filter({ has: page.getByText('Where Python blocks run', { exact: true }) })
    .filter({ has: page.locator('select') })
    .last();
  await expect(row.locator('option', { hasText: SHOWN })).toHaveCount(1);
  await row.locator('select').selectOption(WORKER);
  await expect(page.getByText('Plain Python for the e2e test', { exact: true })).toBeVisible();
  // Without a lock it cannot be prepared, and says what to do.
  await expect(page.getByText(`It has no uv.lock: run uv lock in ${FOLDER}`)).toBeVisible();
  await expect(page.getByRole('button', { name: `Prepare ${SHOWN}` })).toBeDisabled();
  await page.getByRole('button', { name: 'Save pipeline settings' }).click();
  await expect.poll(async () => (
    await api.call('GET', `pipelines/${pipelineUUID}`)
  ).pipeline.environment).toEqual({ worker: WORKER });

  // The page says where each block runs, and that it fails until the worker is prepared.
  await page.reload();
  const blockRow = page.locator('tr').filter({ hasText: LOADER });
  await expect(blockRow).toContainText('Tools worker');
  await expect(blockRow).toContainText('the pipeline\'s default');
  await expect(blockRow).toContainText('Not prepared');
  // So does the block in the editor.
  await page.goto(`/pipelines/${pipelineUUID}/edit`);
  await expect(page.getByRole('button', { name: 'Python worker: Tools worker · not prepared' }))
    .toBeVisible();

  // Locked, the page prepares it and shows what uv printed.
  await api.writeFile(FOLDER, 'uv.lock', LOCK);
  await page.goto(`/pipelines/${pipelineUUID}/settings`);
  await page.getByRole('button', { name: `Prepare ${SHOWN}` }).click();
  await expect(page.getByText(/Prepared: \d+ packages from its uv.lock/)).toBeVisible({
    timeout: 120_000,
  });
  await page.goto(`/pipelines/${pipelineUUID}/edit`);
  await expect(page.getByRole('button', { name: 'Python worker: Tools worker' })).toBeVisible();

  const run = await api.runOnce(pipelineUUID, `worker_${SUFFIX}`);
  expect(run.status).toBe('completed');
  const { block_runs: blockRuns } = await api.call('GET', `pipeline_runs/${run.id}/block_runs`);
  expect(blockRuns.map(b => b.status)).toEqual(['completed']);
});

test('a Python worker is added from a template, locked and prepared', async ({ page }) => {
  // uv locks and installs pyarrow.
  test.setTimeout(240_000);
  const name = `e2e-added-${SUFFIX}`;
  await page.goto(`/pipelines/${pipelineUUID}/settings`);
  await page.getByText('Add a Python worker', { exact: true }).click();
  await page.getByLabel('Template').selectOption('empty');
  await page.getByLabel('Name in YAML').fill(name);
  await page.getByLabel('Name shown').fill('Added worker');
  await page.getByRole('button', { name: 'Add and prepare' }).click();
  try {
    // The first test's worker, prepared, may show until the list reloads: wait for this one.
    await expect.poll(async () => {
      const { python_workers: workers } = await api.call('GET', 'python_workers');
      const added = workers.find(w => w.name === name);

      return [added?.label, added?.locked, added?.prepared];
    }, { timeout: 180_000 }).toEqual(['Added worker', true, true]);
    await expect(page.getByText(/Prepared: \d+ packages from its uv.lock/)).toBeVisible();
    await expect(page.locator('select').filter({ has: page.locator(`option[value="${name}"]`) })
      .first()).toHaveValue(name);
  } finally {
    await api.call('DELETE', `folders/${encodeURIComponent(`python_workers/${name}`)}`)
      .catch(() => null);
  }
});

test('two workers with the same versions combine into one for a pipeline', async ({ page }) => {
  test.setTimeout(240_000);
  const pair = [`e2e-first-${SUFFIX}`, `e2e-second-${SUFFIX}`];
  const combined = `e2e-both-${SUFFIX}`;
  for (const worker of pair) {
    await api.writeFile(`python_workers/${worker}`, 'pyproject.toml',
      PYPROJECT.split(WORKER).join(worker).replace('Tools worker', worker));
    await api.writeFile(`python_workers/${worker}`, 'uv.lock', LOCK.split(WORKER).join(worker));
  }
  const uuid = await api.createPipeline(`ui_combine_${SUFFIX}`);
  await api.addBlock(uuid, { content: LOAD, name: `combine_load_${SUFFIX}`, type: 'data_loader' });
  await api.addBlock(uuid, {
    content: LOAD.replace('@data_loader', '@transformer').replace('def load(', 'def transform(data, '),
    name: `combine_next_${SUFFIX}`,
    type: 'transformer',
    upstream_blocks: [`combine_load_${SUFFIX}`],
  });
  for (const [index, worker] of pair.entries()) {
    const name = index === 0 ? `combine_load_${SUFFIX}` : `combine_next_${SUFFIX}`;
    await api.call('PUT', `pipelines/${uuid}/blocks/${name}`, {
      block: { configuration: { python_worker: worker } },
    });
  }
  try {
    // The API's answer first: when it is not "same", the failure says what it was.
    const { python_worker_combination: answer, error } = await api.call(
      'POST', 'python_worker_combinations', { python_worker_combination: { workers: pair } },
    );
    expect(answer?.state, JSON.stringify(answer || error)).toBe('same');
    await page.goto(`/pipelines/${uuid}/settings`);
    // The blocks' own workers are not prepared: each is prepared from here too.
    for (const worker of pair) {
      await expect(page.getByRole('button', { name: `Prepare ${worker}` })).toBeEnabled();
    }
    await expect(page.getByText(/can run as one worker with the same package versions/))
      .toBeVisible({ timeout: 60_000 });
    await page.getByText('Show what each worker installs', { exact: true }).click();
    await expect(page.getByText(/they share memory, and a crash/)).toBeVisible();
    await page.getByLabel('Name of the combined worker').fill(combined);
    await page.getByRole('button', { name: 'Combine into one worker' }).click();
    await expect.poll(async () => {
      const { pipeline } = await api.call('GET', `pipelines/${uuid}`);

      return pipeline.blocks.map(b => b.configuration?.python_worker);
    }, { timeout: 60_000 }).toEqual([combined, combined]);
  } finally {
    await api.deletePipeline(uuid);
    for (const worker of [...pair, combined]) {
      await api.call('DELETE', `folders/${encodeURIComponent(`python_workers/${worker}`)}`)
        .catch(() => null);
    }
  }
});
