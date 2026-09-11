import { expect, test } from './base';

test('ensure all main pages load properly', async ({ page }) => {
  async function navigateToAndWaitTilLoaded(name: string) {
    await page.getByTestId('navigation_sidebar').hover();
    await page.getByRole('link', { name })
      .and(page.getByTestId('navigation_link'))
      .click();
    await page.waitForLoadState();

    const headerBreadcrumbTitleNode = page.getByTestId('page_header')
      .getByText(name, { exact: true });
    await expect(headerBreadcrumbTitleNode).toBeVisible();
  }

  await navigateToAndWaitTilLoaded('Overview');
  await navigateToAndWaitTilLoaded('Pipelines');
  await navigateToAndWaitTilLoaded('Triggers');
  await navigateToAndWaitTilLoaded('Pipeline runs');
  await navigateToAndWaitTilLoaded('Global data products');
  await navigateToAndWaitTilLoaded('Secrets');
  await navigateToAndWaitTilLoaded('Files');
  await navigateToAndWaitTilLoaded('Templates');
  await navigateToAndWaitTilLoaded('Version control');
  await navigateToAndWaitTilLoaded('Terminal');
});
