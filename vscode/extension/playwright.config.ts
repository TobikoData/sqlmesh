import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: 'tests',
  timeout: 60_000,
  retries: process.env.CI ? 1 : 0,
  workers: 1,
  projects: [
    {
      name: 'electron-vscode',
      use: {
        // ⭢ we'll launch Electron ourselves – no browser needed
        browserName: 'chromium',
        headless: false, // headed makes screenshots deterministic
        launchOptions: {
          slowMo: process.env.CI ? 0 : 100,
        },
      },
    },
  ],
})
