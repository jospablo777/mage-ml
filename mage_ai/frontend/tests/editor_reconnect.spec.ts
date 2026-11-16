import { APIRequestContext, WebSocketRoute } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * A block whose run ended while the editor's connection was down showed running until the
 * kernel was interrupted: its idle message went to nobody (I-125). On reconnecting, the
 * editor asks the server which blocks still run.
 */

const SUFFIX = Date.now().toString(36);
const LOADER = `slow_${SUFFIX}`;

let apiContext: APIRequestContext;
let api: MageApi;
let pipelineUUID: string;

test.beforeAll(async ({ playwright }, testInfo) => {
  apiContext = await playwright.request.newContext({
    baseURL: process.env.MAGE_E2E_API_URL || testInfo.project.use.baseURL,
  });
  api = await MageApi.signIn(apiContext);
  pipelineUUID = await api.createPipeline(`reconnect_${SUFFIX}`);
  await api.addBlock(pipelineUUID, {
    content: [
      'import time',
      '',
      '',
      '@data_loader',
      'def slow(*args, **kwargs):',
      '    time.sleep(6)',
      '    return 1',
      '',
    ].join('\n'),
    name: LOADER,
    type: 'data_loader',
  });
});

test.beforeEach(async () => {
  await api.restartKernel();
});

test.afterAll(async () => {
  await api.deletePipeline(pipelineUUID);
  await apiContext?.dispose();
});

test('a block that ended while the editor was disconnected stops showing as running', async ({
  page,
}) => {
  test.setTimeout(180_000);
  // The kernel's messages pass through here, so the test can lose them as a dropped
  // connection does.
  let dropping = false;
  const sockets: WebSocketRoute[] = [];
  await page.routeWebSocket(/\/websocket\/$/, (socket) => {
    const server = socket.connectToServer();
    sockets.push(socket);
    server.onMessage((message) => {
      if (!dropping) {
        socket.send(message);
      }
    });
  });

  await page.goto(`/pipelines/${pipelineUUID}/edit`);
  const editor = page.locator('.monaco-editor .view-lines').first();
  await expect(editor).toBeVisible();
  await editor.click();
  await page.keyboard.press('ControlOrMeta+Enter');

  // The run's timer counts while the block runs.
  const timer = page.getByText(/^\d+s$/);
  await expect(timer).toBeVisible({ timeout: 60_000 });
  dropping = true;
  // The block sleeps 6 s; its idle message is lost.
  await page.waitForTimeout(12_000);
  await expect(timer).toBeVisible();

  dropping = false;
  await Promise.all(sockets.splice(0).map(socket => socket.close()));

  await expect(timer).toHaveCount(0, { timeout: 60_000 });
});
