import { expect, test } from './base';

/*
 * Settings > Workspace > Server health: the machine's load and memory, MageML's processes
 * and the size of its outputs, from server/health.py.
 */

test('the server health page shows the load, the memory and the processes', async ({ page }) => {
  await page.goto('/settings/workspace/server-health');

  await expect(page.getByText(/^MageML uses .+ in \d+ process(es)?; the load is/)).toBeVisible({
    timeout: 30_000,
  });
  await expect(page.getByText('Load, last minute', { exact: true })).toBeVisible();
  await expect(page.getByText('Memory used', { exact: true })).toBeVisible();
  const server = page.getByRole('row').filter({ hasText: /^server/ });
  await expect(server).toHaveCount(1);
  await expect(server.getByText(/ MB$/)).toBeVisible();
});
