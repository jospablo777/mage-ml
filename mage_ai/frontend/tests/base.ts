import { FeatureUUIDEnum } from '@interfaces/ProjectType';
import { test as base, expect } from '@playwright/test';
import { TSettingFeaturesToDisable, enableSettings } from './utils';

export const test = base.extend<{
  failOnClientError: boolean;
  settingFeaturesToDisable: TSettingFeaturesToDisable,
}>({
  failOnClientError: true,
  settingFeaturesToDisable: {
    [FeatureUUIDEnum.LOCAL_TIMEZONE]: true,
  },
  // eslint-disable-next-line sort-keys
  page: async ({ page, failOnClientError, settingFeaturesToDisable }, use) => {
    const pageErrors: Error[] = [];
    page.addListener('pageerror', (error) => {
      pageErrors.push(error);
    });
    // Temporary, CI only: what the editor sends and gets on its kernel WebSocket, to find
    // why a block run stops reaching the kernel on CI.
    if (process.env.CI) {
      page.on('websocket', (socket) => {
        if (!socket.url().endsWith('/websocket/')) {
          return;
        }
        const name = `[ws ${new Date().toISOString().slice(11, 23)}]`;
        console.log(`${name} open ${socket.url()}`);
        socket.on('framesent', ({ payload }) => {
          const text = String(payload);
          let uuid = '';
          try {
            uuid = JSON.parse(text).uuid;
          } catch {
            // Not JSON.
          }
          console.log(`${new Date().toISOString().slice(11, 23)} ws sent ${uuid} ${text.length} bytes`);
        });
        socket.on('framereceived', ({ payload }) => {
          const text = String(payload);
          let summary = text.slice(0, 80);
          try {
            const message = JSON.parse(text);
            summary = `${message.uuid} ${message.msg_type} ${message.execution_state}`;
          } catch {
            // Not JSON.
          }
          console.log(`${new Date().toISOString().slice(11, 23)} ws got ${summary}`);
        });
        socket.on('close', () => console.log(`${new Date().toISOString().slice(11, 23)} ws closed`));
        socket.on('socketerror', (error) => console.log(`ws error ${error}`));
      });
      page.on('console', (message) => {
        if (['error', 'warning'].includes(message.type())) {
          console.log(`[browser ${message.type()}] ${message.text().slice(0, 300)}`);
        }
      });
    }

    await page.goto('/sign-in');
    await page.getByPlaceholder('Email').click();
    await page.getByRole('textbox').first().fill('admin@admin.com');
    await page.getByRole('textbox').first().press('Tab');
    await page.locator('input[type="password"]').fill('admin');
    await page.locator('input[type="password"]').press('Enter');

    /*
     * Wait for redirected page, which can be /overview (if at least 1 pipeline run exists)
     * or /pipelines (if no pipeline runs exist), to load after signing in.
     */
    await expect(page.getByRole('button', { name: 'New' })).toBeVisible();

    await enableSettings(page, settingFeaturesToDisable);

    // Use subsequent tests.
    await use(page);

    if (failOnClientError) {
      expect(pageErrors).toHaveLength(0);
    }
  },
});

export { expect };
