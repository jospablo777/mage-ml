import { expect, test } from '@playwright/test';

// The shared fixture in ./base signs in before every test, so this file uses the plain test.
test('reject a wrong password on the sign-in page', async ({ page }) => {
  await page.goto('/sign-in');
  await page.getByRole('textbox').first().fill('admin@admin.com');
  await page.locator('input[type="password"]').fill('not-the-admin-password');
  await page.locator('input[type="password"]').press('Enter');

  await expect(page.getByText('Email/username and/or password invalid.')).toBeVisible();
  await expect(page).toHaveURL(/\/sign-in/);
});
