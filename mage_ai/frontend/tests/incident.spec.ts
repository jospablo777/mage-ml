import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * A failed run's page opens on what failed: the block, its error, the end of its log and
 * the blocks that did not complete, with the ways to recover. Here the block is
 * fixed and retried from the panel, and the run completes.
 */

const SUFFIX = Date.now().toString(36);
const LOADER = `load_${SUFFIX}`;
const SCORE = `score_${SUFFIX}`;
const SAVE = `save_${SUFFIX}`;

const FAILING = `
@transformer
def transform(rows, *args, **kwargs):
    print('scoring the rows of the incident test')
    return rows['amount']
`;

const FIXED = `
@transformer
def transform(rows, *args, **kwargs):
    return dict(rows, amount=rows.get('amount', 0))
`;

let apiContext: APIRequestContext;
let api: MageApi;
let pipelineUUID: string;
let runID: number;

test.beforeAll(async ({ playwright }, testInfo) => {
  test.setTimeout(240_000);
  apiContext = await playwright.request.newContext({
    baseURL: process.env.MAGE_E2E_API_URL || testInfo.project.use.baseURL,
  });
  api = await MageApi.signIn(apiContext);
  pipelineUUID = await api.createPipeline(`ui_incident_${SUFFIX}`);
  await api.addBlock(pipelineUUID, {
    content: `
@data_loader
def load(*args, **kwargs):
    return [{'id': 1}]
`,
    name: LOADER,
    type: 'data_loader',
  });
  await api.addBlock(pipelineUUID, {
    content: FAILING, name: SCORE, type: 'transformer', upstream_blocks: [LOADER],
  });
  await api.addBlock(pipelineUUID, {
    content: `
@data_exporter
def save(values, *args, **kwargs):
    return values
`,
    name: SAVE,
    type: 'data_exporter',
    upstream_blocks: [SCORE],
  });
  const run = await api.runOnce(pipelineUUID, `incident_${SUFFIX}`);
  expect(run.status).toBe('failed');
  runID = run.id;
});

test.afterAll(async () => {
  await api.deletePipeline(pipelineUUID).catch(() => null);
  await apiContext.dispose();
});

test('the incidents page lists the pipeline still failing and opens its run', async ({
  page,
}) => {
  test.setTimeout(120_000);
  await page.goto('/incidents');
  const table = page.getByRole('table', { name: 'Incidents' });
  const row = table.getByRole('row').filter({ hasText: `ui_incident_${SUFFIX}` });
  await expect(row).toBeVisible({ timeout: 60_000 });
  await expect(row.getByText('Still failing')).toBeVisible();
  await expect(row.getByText(SCORE)).toBeVisible();
  await expect(row.getByText('KeyError: \'amount\'')).toBeVisible();
  await row.getByRole('link', { exact: true, name: `Run ${runID}` }).first().click();
  await expect(page).toHaveURL(new RegExp(`/pipelines/${pipelineUUID}/runs/${runID}$`));
  await expect(page.getByTestId('incident')).toBeVisible({ timeout: 60_000 });
});

test('a failed run shows what failed and recovers from the panel', async ({ page }) => {
  test.setTimeout(240_000);
  await page.goto(`/pipelines/${pipelineUUID}/runs/${runID}`);
  const incident = page.getByTestId('incident');

  await expect(incident.getByText(`${SCORE} failed: KeyError`)).toBeVisible({ timeout: 60_000 });
  await expect(incident.getByText(/scoring the rows of the incident test/)).toBeVisible();
  await expect(incident.getByText(`Upstream in this run: ${LOADER} completed`)).toBeVisible();
  await expect(incident.getByText(`Did not complete: ${SAVE} (cancelled)`)).toBeVisible();
  await expect(incident.getByText(/No completed run of this block to compare with/))
    .toBeVisible();

  // Fixed, then retried from the failed block: the run completes.
  await api.writeFile('transformers', `${SCORE}.py`, FIXED);
  await incident.getByRole('button', { name: `Retry from ${SCORE} and the blocks after it` })
    .click();
  await expect.poll(async () => (
    await api.call('GET', `pipeline_runs/${runID}`)
  ).pipeline_run.status, { intervals: [2000], timeout: 180_000 }).toBe('completed');
  await page.reload();
  await expect(incident).toHaveCount(0);

  // Its only failed run completed: the incidents page lists it no more.
  await page.goto('/incidents');
  await expect(page.getByText(`ui_incident_${SUFFIX}`)).toHaveCount(0, { timeout: 60_000 });
});
