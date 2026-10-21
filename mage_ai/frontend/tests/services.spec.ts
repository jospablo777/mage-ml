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

  // The DAG that runs the pipeline from Airflow through the service.
  await expect(page.getByText('Run it from Airflow')).toBeVisible();
  await expect(page.getByText(`pipeline='${pipelineUUID}',`)).toBeVisible();
  await expect(page.getByText(`"host": "http://${serviceName}:8080"`, { exact: false }))
    .toBeVisible();

  // Build and ship: the image is for most servers (amd64) until the service says otherwise,
  // and the choice saves as the service's platforms.
  await page.getByRole('button', { exact: true, name: 'Export' }).click();
  await expect(page.getByText('Build and ship the image')).toBeVisible({ timeout: 180_000 });
  const amd64 = page.getByRole('button', { exact: true, name: 'linux/amd64' });
  const arm64 = page.getByRole('button', { exact: true, name: 'linux/arm64' });
  await expect(amd64).toHaveAttribute('aria-pressed', 'true');
  await expect(arm64).toHaveAttribute('aria-pressed', 'false');
  await arm64.click();
  await page.getByRole('button', { name: 'Save as the service\'s platforms' }).click();
  await expect(page.getByRole('button', { name: 'Save as the service\'s platforms' })).toBeHidden();
  const { pipeline_service: built } = await api.call('GET', `pipeline_services/${serviceName}`);
  expect(built.platforms).toEqual(['linux/amd64', 'linux/arm64']);
  // A new panel reads them from the service.
  await page.reload();
  await page.getByRole('button', { exact: true, name: 'Export' }).click();
  await expect(arm64).toHaveAttribute('aria-pressed', 'true');
  await amd64.click();
  await expect(page.getByText('Choose where the image runs.')).toHaveCount(0);
  await arm64.click();
  await expect(page.getByText('Choose where the image runs.')).toBeVisible();
});
