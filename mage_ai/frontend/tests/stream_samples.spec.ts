import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

// A streaming pipeline's transformer runs on a sample of messages: pasted in the Samples
// panel, run with ⌘↵ by the service engine's Python worker, shown as input and output.
const SUFFIX = Date.now().toString(36);
const SOURCE = `samples_source_${SUFFIX}`;
const TRANSFORMER = `samples_double_${SUFFIX}`;

let apiContext: APIRequestContext;
let api: MageApi;
let pipelineUUID: string;

test.beforeAll(async ({ playwright }, testInfo) => {
  apiContext = await playwright.request.newContext({ baseURL: testInfo.project.use.baseURL });
  api = await MageApi.signIn(apiContext);
  const body = await api.call('POST', 'pipelines', {
    pipeline: { name: `stream_samples_${SUFFIX}`, type: 'streaming' },
  });
  pipelineUUID = body.pipeline.uuid;
  await api.call('POST', `pipelines/${pipelineUUID}/blocks`, {
    block: {
      content: 'connector_type: kafka\nbootstrap_server: localhost:9092\ntopic: orders\n'
        + 'consumer_group: orders\n',
      language: 'yaml',
      name: SOURCE,
      type: 'data_loader',
    },
  });
  await api.addBlock(pipelineUUID, {
    content: [
      '@transformer',
      'def double(messages, *args, **kwargs):',
      "    print(f'doubling {len(messages)}')",
      "    return [dict(m, twice=100 // m['n']) for m in messages if m['n'] != 3]",
      '',
    ].join('\n'),
    name: TRANSFORMER,
    type: 'transformer',
    upstream_blocks: [SOURCE],
  });
});

test.afterAll(async () => {
  await api.deletePipeline(pipelineUUID);
  await apiContext.dispose();
});

async function saveSample(page, name: string, lines: string) {
  const panel = page.locator('body');
  await panel.getByPlaceholder('Sample name').fill(name);
  await panel.getByPlaceholder(/order_id/).fill(lines);
  await panel.getByRole('button', { name: `Save as ${name}` }).click();
  await expect(panel.getByText(new RegExp(`Saved \\d+ messages as ${name}`))).toBeVisible();
}

async function runTransformer(page) {
  // The transformer is the second block; its editor takes ⌘↵.
  const editor = page.locator('.monaco-editor .view-lines').nth(1);
  await expect(editor).toBeVisible();
  await editor.click();
  await page.keyboard.press('ControlOrMeta+Enter');
}

test('a stream transformer runs on a pasted sample and shows input and output', async ({ page }) => {
  await page.goto(`/pipelines/${pipelineUUID}/edit?sideview=samples`);
  await expect(page.getByText('Capture from the source')).toBeVisible();
  await saveSample(page, 'numbers', '{"n": 1}\n{"n": 2}\n{"n": 3}');
  await expect(page.getByText('used by Run on sample')).toBeVisible();

  await runTransformer(page);

  await expect(page.getByText('3 in → 2 out')).toBeVisible({ timeout: 60000 });
  await expect(page.getByText('{"n":2,"twice":50}')).toBeVisible();
  await expect(page.getByText('1 message dropped', { exact: false })).toBeVisible();
  await expect(page.getByText(`${TRANSFORMER}: doubling 3`)).toBeVisible();
});

test('a failing message is pointed at', async ({ page }) => {
  await page.goto(`/pipelines/${pipelineUUID}/edit?sideview=samples`);
  await saveSample(page, 'zeros', '{"n": 1}\n{"n": 0}\n{"n": 2}');

  await runTransformer(page);

  await expect(page.getByText(`Failed in ${TRANSFORMER}`)).toBeVisible({ timeout: 60000 });
  await expect(page.getByText('ZeroDivisionError', { exact: false }).first()).toBeVisible();
  await expect(page.getByText('Message 2 fails on its own')).toBeVisible();
});
