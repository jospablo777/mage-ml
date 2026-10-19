import { Request } from '@playwright/test';

import { expect, test } from './base';

// The overview's run counts refresh by themselves, and a period tab shows its own period.
// They were fetched once when the page loaded (or a tab changed), so the dashboard needed a
// browser refresh to show new runs.
const STATS = '/api/monitor_stats/pipeline_run_count';

function startTime(request: Request): string {
  return new URL(request.url()).searchParams.get('start_time') || '';
}

test('the overview refreshes its run counts and fetches each period', async ({ page }) => {
  // The refresh is every minute, waited for in real time.
  test.setTimeout(150_000);
  const requests: Request[] = [];
  page.on('request', (request) => {
    if (request.url().includes(STATS)) {
      requests.push(request);
    }
  });
  await page.goto('/overview?tab=today');
  await expect.poll(() => requests.length).toBeGreaterThan(0);
  const today = startTime(requests[requests.length - 1]);

  // Within about a minute the counts are fetched again, for the same period.
  const before = requests.length;
  await expect.poll(() => requests.length, { timeout: 80_000 }).toBeGreaterThan(before);
  expect(startTime(requests[requests.length - 1])).toBe(today);

  // The next tab asks for its own period, which starts earlier.
  const beforeTab = requests.length;
  await page.getByText('Last 7 days', { exact: true }).first().click();
  await expect.poll(() => requests.length).toBeGreaterThan(beforeTab);
  const week = startTime(requests[requests.length - 1]);
  expect(week).not.toBe(today);
  expect(new Date(week).getTime()).toBeLessThan(new Date(today).getTime());
});
