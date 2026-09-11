import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './storybook-tests',
  forbidOnly: !!process.env.CI,
  workers: 1,
  use: {
    ...devices['Desktop Chrome'],
    baseURL: 'http://127.0.0.1:16006',
    trace: 'retain-on-failure',
  },
  webServer: {
    command: 'python -m http.server 16006 --bind 127.0.0.1 --directory storybook-static',
    url: 'http://127.0.0.1:16006/iframe.html',
    reuseExistingServer: false,
  },
});
