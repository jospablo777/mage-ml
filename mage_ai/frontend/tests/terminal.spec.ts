import { expect, test } from './base';

test('run a shell command in the terminal', async ({ page }) => {
  // The terminal is one shell shared by every session and keeps earlier output, including
  // output from a retry of this test, so each run echoes its own token.
  const token = `mage-terminal-${Date.now().toString(36)}`;

  await page.goto('/terminal');

  // Each terminal line renders as a code element. Wait for the shell prompt.
  const lines = page.locator('p code').filter({ hasText: /\S/ });
  await expect(lines.first()).toBeVisible();
  await lines.last().click();

  // echo with a plain word behaves the same in bash, sh, cmd and PowerShell.
  await page.keyboard.type(`echo ${token}`);
  await page.keyboard.press('Enter');

  await expect(page.getByText(token, { exact: true })).toBeVisible();
});
