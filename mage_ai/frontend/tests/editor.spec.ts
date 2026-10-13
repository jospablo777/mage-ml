import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * The editor's file tree and outputs: a click on a file opens it in the text editor, a
 * block file dragged onto the graph joins the pipeline, and an image output renders.
 */

const SUFFIX = Date.now().toString(36);
const SOURCE = `editor_source_${SUFFIX}`;
const TARGET = `editor_target_${SUFFIX}`;
const DRAGGED = `editor_drag_${SUFFIX}`;
const IMAGE = `editor_image_${SUFFIX}`;

const DRAGGED_CODE = `
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(data, *args, **kwargs):
    return data  # ${DRAGGED}
`;

const IMAGE_CODE = `
from IPython.display import display
from PIL import Image, ImageDraw

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_data(*args, **kwargs):
    image = Image.new('RGB', (120, 60), 'white')
    ImageDraw.Draw(image).rectangle([10, 10, 110, 50], fill='red')
    display(image)
    return {'drawn': True}
`;

let apiContext: APIRequestContext;
let api: MageApi;
let sourceUUID: string;
let targetUUID: string;

test.beforeAll(async ({ playwright }, testInfo) => {
  apiContext = await playwright.request.newContext({
    baseURL: process.env.MAGE_E2E_API_URL || testInfo.project.use.baseURL,
  });
  api = await MageApi.signIn(apiContext);
  // The dragged block's file comes from another pipeline.
  sourceUUID = await api.createPipeline(SOURCE);
  await api.addBlock(sourceUUID, { content: DRAGGED_CODE, name: DRAGGED, type: 'transformer' });
  targetUUID = await api.createPipeline(TARGET);
  await api.addBlock(targetUUID, { content: IMAGE_CODE, name: IMAGE, type: 'data_loader' });
});

// A fresh kernel for each test: on CI the kernel sometimes stopped running code after a
// few runs, and every later test of the server failed with it.
test.beforeEach(async () => {
  await api.restartKernel();
});

test.afterAll(async () => {
  await api.deletePipeline(targetUUID);
  await api.deletePipeline(sourceUUID);
  // A deleted pipeline keeps its blocks' files.
  for (const path of [`transformers/${DRAGGED}.py`, `data_loaders/${IMAGE}.py`]) {
    await api.call('DELETE', `files/${encodeURIComponent(path)}`);
  }
  await apiContext.dispose();
});

async function openFolder(page, name: string) {
  await page.getByText(name, { exact: true }).first().click();
}

test('a click on a file in the file tree opens it in the text editor', async ({ page }) => {
  await page.goto(`/pipelines/${targetUUID}/edit`);
  await openFolder(page, 'transformers');
  await page.getByText(`${DRAGGED}.py`, { exact: true }).first().click();

  // The file's code is in no block of this pipeline, so only the text editor shows it.
  await expect(page.locator('.monaco-editor').getByText(`# ${DRAGGED}`)).toBeVisible();
});

test('a block file dragged onto the graph joins the pipeline', async ({ page }) => {
  await page.goto(`/pipelines/${targetUUID}/edit`);
  await openFolder(page, 'transformers');
  const file = page.getByText(`${DRAGGED}.py`, { exact: true }).first();
  const box = await file.boundingBox();

  // A file starts dragging after the button is held for 300 ms.
  await page.mouse.move(box.x + 20, box.y + box.height / 2);
  await page.mouse.down();
  await page.waitForTimeout(600);
  const width = page.viewportSize().width;
  await page.mouse.move(width - 300, 400, { steps: 10 });
  await page.mouse.up();

  await expect.poll(async () => {
    const body = await api.call('GET', `pipelines/${targetUUID}`);
    return body.pipeline.blocks.map(b => b.uuid);
  }).toContain(DRAGGED);
});

test('an image a block displays renders at its size', async ({ page }) => {
  await page.goto(`/pipelines/${targetUUID}/edit`);
  const editor = page.locator('.monaco-editor .view-lines').first();
  await expect(editor).toBeVisible();
  await editor.click();
  await page.keyboard.press('ControlOrMeta+Enter');

  const image = page.locator('img[src^="data:image/png"]').first();
  await expect(image).toBeVisible({ timeout: 90000 });
  expect(await image.evaluate((e: HTMLImageElement) => [e.naturalWidth, e.naturalHeight]))
    .toEqual([120, 60]);
});
