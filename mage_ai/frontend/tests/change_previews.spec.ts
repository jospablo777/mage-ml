import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * A change preview: after a completed run, the transformer's code changes, and the page
 * previews it against the run: the transformer runs again on the run's stored input, and
 * its rows compare by id; the exporter never runs.
 */

const SUFFIX = Date.now().toString(36);
const LOADER = `load_${SUFFIX}`;
const SCORE = `score_${SUFFIX}`;
const SAVE = `save_${SUFFIX}`;

const SCORING = (factor: number, keep: number) => `
@transformer
def transform(frame, *args, **kwargs):
    frame['score'] = frame['amount'] * ${factor}
    return frame[frame['id'] < ${keep}]
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
  pipelineUUID = await api.createPipeline(`ui_preview_${SUFFIX}`);
  await api.addBlock(pipelineUUID, {
    content: `
import pandas as pd

@data_loader
def load(*args, **kwargs):
    return pd.DataFrame({'id': range(10), 'amount': [i * 5 for i in range(10)]})
`,
    name: LOADER,
    type: 'data_loader',
  });
  await api.addBlock(pipelineUUID, {
    content: SCORING(2, 10), name: SCORE, type: 'transformer', upstream_blocks: [LOADER],
  });
  await api.addBlock(pipelineUUID, {
    content: `
@data_exporter
def save(frame, *args, **kwargs):
    raise Exception('a preview ran the exporter')
`,
    name: SAVE,
    type: 'data_exporter',
    upstream_blocks: [SCORE],
  });
  // The exporter passes in the baseline run, then raises if a preview ever runs it.
  await api.writeFile('data_exporters', `${SAVE}.py`, `
@data_exporter
def save(frame, *args, **kwargs):
    return frame
`);
  const run = await api.runOnce(pipelineUUID, `preview_${SUFFIX}`);
  expect(run.status).toBe('completed');
  runID = run.id;
  await api.writeFile('transformers', `${SCORE}.py`, SCORING(3, 8));
  await api.writeFile('data_exporters', `${SAVE}.py`, `
@data_exporter
def save(frame, *args, **kwargs):
    raise Exception('a preview ran the exporter')
`);
});

test.afterAll(async () => {
  await api.deletePipeline(pipelineUUID).catch(() => null);
  await apiContext.dispose();
});

test('a preview shows the changed rows of the changed block before it ships', async ({
  page,
}) => {
  test.setTimeout(180_000);
  await page.goto(`/pipelines/${pipelineUUID}/previews`);
  await expect(page.getByText('See what a change does to the data before it ships'))
    .toBeVisible({ timeout: 60_000 });
  await page.getByRole('button', { name: 'Preview the current code' }).click();

  await expect(page.getByText(`1 of 3 blocks output different data than run ${runID}`))
    .toBeVisible({ timeout: 120_000 });
  // Scores 5 to 45 tripled instead of doubled (0 is 0 either way); ids 8 and 9 removed.
  await expect(page.getByText('7 changed', { exact: true })).toBeVisible();
  await expect(page.getByText('2 removed', { exact: true })).toBeVisible();
  await expect(page.getByText('1 unchanged', { exact: true })).toBeVisible();
  await expect(page.getByText('Changed values: score 7')).toBeVisible();
  await expect(page.getByText(/previews never run exporters/)).toBeVisible();
  await expect(page.getByRole('img', { name: 'output_0 rows' })).toBeVisible();

  // Both outputs open in ColumnAtlas: the preview's scores are tripled (id 7: 105), the
  // run's doubled, with ids 8 and 9 (id 9: 90).
  await page.getByRole('link', { exact: true, name: 'this preview' }).click();
  await expect(page.getByRole('gridcell', { exact: true, name: '105' })).toBeVisible({
    timeout: 30_000,
  });
  await page.getByRole('link', { exact: true, name: `run ${runID}` }).click();
  await expect(page.getByRole('gridcell', { exact: true, name: '90' })).toBeVisible({
    timeout: 30_000,
  });
  await expect(page.getByRole('gridcell', { exact: true, name: '105' })).toHaveCount(0);
});
