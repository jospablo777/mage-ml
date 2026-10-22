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

test('a moved model version is promoted from the service page', async ({ page }) => {
  // A service that embeds a registry model; the registry's answers are stubbed.
  const name = `${serviceName}-models`;
  await api.call('POST', 'pipeline_services', { pipeline_service: {
    models: [{ name: 'churn', uri: 'models:/churn@champion' }],
    name,
    pipelines: [pipelineUUID],
  } });
  const models = [{
    built: '3', challengers: [], name: 'churn', registry: '5', status: 'moved',
    uri: 'models:/churn@champion',
  }];
  let promoted = null;
  await page.route('**/api/pipeline_service_models**', async (route) => {
    if (route.request().method() === 'POST') {
      promoted = route.request().postDataJSON().pipeline_service_model;
      await route.fulfill({ json: { pipeline_service_model: {
        models: [{ ...models[0], built: '3', status: 'moved' }],
        name,
        promoted: {
          at: '2026-10-22T08:00:00+00:00', by: 'admin@admin.com', from_version: '3',
          how: 'alias followed', model: 'churn', to_version: '5',
        },
        promotions: [],
      } } });
      return;
    }
    await route.fulfill({ json: { pipeline_service_model: { models, name, promotions: [] } } });
  });

  await page.goto(`/services?service=${name}`);
  await expect(page.getByText('Promote a model version')).toBeVisible();
  await expect(page.getByText('Image: version 3.', { exact: false })).toBeVisible();
  // The field shows its label as its placeholder until it has focus.
  await page.getByPlaceholder('Why (kept in promotions.yaml)').fill('better recall');
  await page.getByRole('button', { name: 'Promote version 5' }).click();
  // The section's subtitle says to build and ship too, so the result line is matched.
  await expect(page.getByText('churn: version 5 promoted', { exact: false })).toBeVisible();
  expect(promoted).toMatchObject({ model: 'churn', name, reason: 'better recall' });
  await api.call('DELETE', `pipeline_services/${name}`).catch(() => null);
});
