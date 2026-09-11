import { defineConfig, devices } from '@playwright/test';

const port = Number(process.env.MAGE_TEST_SERVER_PORT || 6789);
const externalBaseURL = process.env.MAGE_TEST_SERVER_URL;
const baseURL = externalBaseURL || `http://127.0.0.1:${port}`;

export default defineConfig({
  expect: { timeout: 45000 },
  forbidOnly: !!process.env.CI,
  fullyParallel: false,
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
  reporter: 'html',
  retries: process.env.CI ? 2 : 0,
  testDir: './tests',
  timeout: 100000,
  workers: 1,
  use: { baseURL, trace: 'retain-on-failure' },
  webServer: externalBaseURL ? undefined : {
    command: `python mage_ai/cli/main.py start test_project --host 127.0.0.1 --port ${port}`,
    cwd: '../../',
    env: { PYTHONPATH: '.', REQUIRE_USER_AUTHENTICATION: '1' },
    reuseExistingServer: false,
    url: baseURL,
    timeout: 120000,
  },
});
