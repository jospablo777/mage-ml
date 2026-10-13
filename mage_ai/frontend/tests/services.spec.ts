import { APIRequestContext } from '@playwright/test';

import { MageApi } from './api';
import { expect, test } from './base';

/*
 * From a pipeline's settings to a service and back: "Make it a service" starts a service
 * with the pipeline, saving it writes services/<name>/service.yaml, and the pipeline's
 * settings then list the service that runs it.
 */

const SUFFIX = Date.now().toString(36);

let apiContext: APIRequestContext;
let api: MageApi;
let pipelineUUID: string;
let serviceName: string;

test.beforeAll(async ({ playwright }, testInfo) => {
  apiContext = await playwright.request.newContext({
    baseURL: process.env.MAGE_E2E_API_URL || testInfo.project.use.baseURL,
  });
  api = await MageApi.signIn(apiContext);
  pipelineUUID = await api.createPipeline(`ui_service_${SUFFIX}`);
  serviceName = pipelineUUID.replace(/_/g, '-');
});

test.afterAll(async () => {
  await api.call('DELETE', `pipeline_services/${serviceName}`).catch(() => null);
  await api.deletePipeline(pipelineUUID);
  await apiContext.dispose();
});

test('a pipeline becomes a service and its settings show it', async ({ page }) => {
  await page.goto(`/pipelines/${pipelineUUID}/settings`);
  await page.getByRole('link', { name: 'Make it a service' }).click();

  await page.waitForURL('**/services?new=*');
  await expect(page.getByText(serviceName, { exact: true }).first()).toBeVisible();
  // The service's Save; the export section has a link button with the same name.
  await page.getByRole('button', { exact: true, name: 'Save' }).last().click();
  await expect(page.getByRole('button', { exact: true, name: 'Saved' })).toBeVisible();
  await expect(page.getByText(`services/${serviceName}/service.yaml`).first()).toBeVisible();

  const { pipeline_service: saved } = await api.call('GET', `pipeline_services/${serviceName}`);
  expect(saved.pipelines).toEqual([pipelineUUID]);
  expect(saved.path).toEqual(`services/${serviceName}/service.yaml`);

  await page.goto(`/pipelines/${pipelineUUID}/settings`);
  await expect(page.getByText('Used by')).toBeVisible();
  await page.getByRole('link', { exact: true, name: serviceName }).click();
  await page.waitForURL(`**/services?service=${serviceName}`);
  await expect(page.getByText(`services/${serviceName}/service.yaml`).first()).toBeVisible();
});
