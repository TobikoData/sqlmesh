import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: 'tests',
  timeout: 60_000,
  retries: process.env.CI ? 1 : 0,
  workers: 1,
  reporter: [['html', { outputFolder: 'playwright-report' }], ['list']],
  projects: [
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
      },
    },
  ],
})
