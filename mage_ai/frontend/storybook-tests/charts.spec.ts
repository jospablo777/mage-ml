import { expect, test } from '@playwright/test';

const stories = [
  'bargraphhorizontal--regular',
  'boxplothorizontal--default',
  'boxplothorizontal--one-sided',
  'boxplothorizontal--primary',
  'boxplothorizontal--secondary',
  'boxplothorizontal--danger',
  'histogram--regular',
  'piechart--regular',
];

for (const story of stories) {
  test(`render ${story}`, async ({ page }) => {
    const errors: string[] = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.goto(`/iframe.html?id=components-charts-${story}&viewMode=story`);
    await expect(page.locator('#storybook-root svg').first()).toBeVisible();
    expect(errors).toEqual([]);
  });
}
