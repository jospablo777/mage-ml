import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

// The Dead letters tab of a streaming pipeline: the dead-letter sink's topic and what is in
// it, or what to add when the stream has none. Needs Kafka (MAGE_TEST_KAFKA_BOOTSTRAP).
const BOOTSTRAP = process.env.MAGE_TEST_KAFKA_BOOTSTRAP;
const SUFFIX = Date.now().toString(36);

let apiContext: APIRequestContext;
let api: MageApi;
const pipelines: string[] = [];

test.skip(!BOOTSTRAP, 'set MAGE_TEST_KAFKA_BOOTSTRAP');

async function stream(name: string, deadLetter: boolean): Promise<string> {
  const body = await api.call('POST', 'pipelines', { pipeline: { name, type: 'streaming' } });
  const uuid = body.pipeline.uuid;
  pipelines.push(uuid);
  await api.call('POST', `pipelines/${uuid}/blocks`, {
    block: {
      content: `connector_type: kafka\nbootstrap_server: ${BOOTSTRAP}\ntopic: dl_in_${SUFFIX}\n`
        + 'consumer_group: dl\n',
      language: 'yaml',
      name: `${name}_source`,
      type: 'data_loader',
    },
  });
  if (deadLetter) {
    await api.call('POST', `pipelines/${uuid}/blocks`, {
      block: {
        content: `connector_type: kafka\nbootstrap_server: ${BOOTSTRAP}\n`
          + `topic: dl_dead_${SUFFIX}\ndead_letter: true\n`,
        language: 'yaml',
        name: `${name}_dead`,
        type: 'data_exporter',
        upstream_blocks: [`${name}_source`],
      },
    });
  }
  return uuid;
}

test.beforeAll(async ({ playwright }, testInfo) => {
  apiContext = await playwright.request.newContext({ baseURL: testInfo.project.use.baseURL });
  api = await MageApi.signIn(apiContext);
});

test.afterAll(async () => {
  for (const uuid of pipelines) {
    await api.deletePipeline(uuid);
  }
  await apiContext.dispose();
});

test('the Dead letters tab reads the dead-letter topic or says what to add', async ({ page }) => {
  const withSink = await stream(`dl_with_${SUFFIX}`, true);
  await page.goto(`/pipelines/${withSink}/edit?sideview=dead_letters`);
  await expect(page.getByText(`0 dead letters in dl_dead_${SUFFIX}`)).toBeVisible();

  const without = await stream(`dl_without_${SUFFIX}`, false);
  await page.goto(`/pipelines/${without}/edit?sideview=dead_letters`);
  await expect(page.getByText('The stream has no dead-letter sink', { exact: false }))
    .toBeVisible();
});
