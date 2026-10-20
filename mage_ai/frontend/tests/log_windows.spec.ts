import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * A block that prints more lines than the logs page loads at once. The page showed only the
 * newest rotated file of a log, and sent every file whole; it now loads the newest lines of
 * each log and shows the earlier ones as a row that loads them, so the first line printed
 * is still one click away.
 */

const SUFFIX = Date.now().toString(36);
const LINES = 12000;

const LOADER = `
@data_loader
def load_data(*args, **kwargs):
    for index in range(${LINES}):
        print(f'printed line {index} of the window test')
    return {'lines': ${LINES}}
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
  pipelineUUID = await api.createPipeline(`ui_log_windows_${SUFFIX}`);
  await api.addBlock(pipelineUUID, {
    content: LOADER,
    name: `log_windows_${SUFFIX}`,
    type: 'data_loader',
  });
  const run = await api.runOnce(pipelineUUID, `log_windows_${SUFFIX}`);
  expect(run.status).toBe('completed');
  runID = run.id;
});

test.afterAll(async () => {
  await api.deletePipeline(pipelineUUID);
  await apiContext.dispose();
});

test('the earlier lines of a long log load from their row', async ({ page }) => {
  test.setTimeout(120_000);
  await page.goto(`/pipelines/${pipelineUUID}/logs?pipeline_run_id[]=${runID}`);
  const gap = page.getByText(/earlier lines of log_windows_/).first();
  await expect(gap).toBeVisible({ timeout: 60_000 });

  // The newest lines are loaded, the first one is not.
  const { logs } = await api.call(
    'GET', `pipelines/${pipelineUUID}/logs?pipeline_run_id[]=${runID}`,
  );
  const item = logs[0].block_run_logs.find(({ name }) => name.startsWith('log_windows_'));
  expect(item.first_line).toBeGreaterThan(0);
  expect(item.total_lines).toBeGreaterThan(LINES);
  expect(item.content).not.toContain('printed line 0 of');

  // Each click loads the lines before the earliest one shown, until the first is there.
  for (let clicks = 0; clicks < 5; clicks++) {
    const rows = page.getByText(/earlier lines of log_windows_/);
    if (await rows.count() === 0) {
      break;
    }
    await rows.first().click();
    await page.waitForTimeout(500);
  }
  await expect(page.getByText(/earlier lines of log_windows_/)).toHaveCount(0);

  // The table is virtualized: the first line is found through the page's search box, which
  // reads every line of the log on the server.
  await page.getByPlaceholder(/Search logs/).fill('printed line 0 of');
  await page.getByPlaceholder(/Search logs/).press('Enter');
  await expect(page.getByText('printed line 0 of the window test')).toBeVisible({
    timeout: 30_000,
  });
});
