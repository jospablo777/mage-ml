import { Locator, Page, expect, test } from '@playwright/test';

const STORY = '/iframe.html?id=components-pipelinedetail-pipelineexecution--streaming&viewMode=story';

function line(page: Page, n: number): Locator {
  return page.getByText(`message ${n}: `, { exact: false });
}

// The output's scrolling box: the nearest ancestor of a line that scrolls.
async function scrollBox(page: Page, n: number) {
  return line(page, n).evaluateHandle((node) => {
    let element = node.parentElement;
    while (element && element.scrollHeight <= element.clientHeight) {
      element = element.parentElement;
    }
    return element;
  });
}

// The output panel of a running stream: it follows the latest line, keeps rows in place as
// messages arrive, holds still while the reader scrolls up, and shows every line on request.
test('stream output follows the latest line and keeps rows', async ({ page }) => {
  const errors: string[] = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.goto(STORY);

  await expect(line(page, 900)).toBeVisible({ timeout: 15_000 });
  await line(page, 900).evaluate((node) => { (node as HTMLElement).dataset.kept = 'yes'; });

  await expect(line(page, 1200)).toBeInViewport({ timeout: 15_000 });
  // The same node: rows that stay are not rendered again.
  await expect(page.locator('[data-kept="yes"]')).toHaveText(/message 900: /);
  await expect(page.getByRole('button', { name: 'Show earlier output (700 lines)' })).toBeVisible();
  await expect(page.getByRole('button', { name: /Jump to latest/ })).toHaveCount(0);

  await page.getByRole('button', { name: 'Show earlier output (700 lines)' }).click();
  await expect(page.getByRole('button', { name: /Show earlier output/ })).toHaveCount(0);
  await expect(line(page, 1)).toHaveCount(1);
  expect(errors).toEqual([]);
});

test('stream output holds still while the reader scrolls up', async ({ page }) => {
  const errors: string[] = [];
  page.on('pageerror', error => errors.push(error.message));
  // A stream that outlasts the test: on a slow machine the 1,200 messages of the default
  // story had all arrived before the scroll, and no new lines meant no Jump to latest.
  await page.goto(`${STORY}&args=total:20000`);

  await expect(line(page, 300)).toBeVisible({ timeout: 15_000 });
  const box = await scrollBox(page, 300);
  await box.evaluate((element: HTMLElement) => { element.scrollTop = 0; });
  await expect(page.getByRole('button', { name: /Jump to latest \(\d+ new lines?\)/ }))
    .toBeVisible();
  const first = await box.evaluate((element: HTMLElement) => element.innerText.slice(0, 20));

  // Messages keep arriving; the view does not move.
  await page.waitForTimeout(1_000);
  expect(await box.evaluate((element: HTMLElement) => element.scrollTop)).toBe(0);
  expect(await box.evaluate((element: HTMLElement) => element.innerText.slice(0, 20)))
    .toBe(first);

  await page.getByRole('button', { name: /Jump to latest/ }).click();
  await expect(page.getByRole('button', { name: /Jump to latest/ })).toHaveCount(0);
  // Following again: the view stays at the bottom as lines keep arriving.
  await expect.poll(() => box.evaluate((element: HTMLElement) => (
    element.scrollHeight - element.scrollTop - element.clientHeight
  )), { timeout: 15_000 }).toBeLessThan(40);
  expect(errors).toEqual([]);
});
