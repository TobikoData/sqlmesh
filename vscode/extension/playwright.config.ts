import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: 'tests',
  timeout: 60_000,
  // TODO: When stable, allow retries in CI
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 2 : 4,
  reporter: [['html', { outputFolder: 'playwright-report' }], ['list']],
  projects: [
    {
      name: 'setup',
      testMatch: 'tests/extension.setup.ts',
      teardown: 'cleanup',
    },
    {
      name: 'cleanup',
      testMatch: 'tests/extension.teardown.ts',
    },
    {
      name: 'electron-vscode',
      use: {
        browserName: 'chromium',
        headless: true,
        launchOptions: {
          slowMo: process.env.CI ? 0 : 100,
        },
        viewport: { width: 1512, height: 944 },
        video: 'retain-on-failure',
        trace: 'retain-on-first-failure',
      },
      dependencies: ['setup'],
    },
  ],
})
