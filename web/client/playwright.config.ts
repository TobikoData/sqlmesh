import { defineConfig, devices } from '@playwright/test'

const URL = process.env.UI_TEST_URL ?? 'http://127.0.0.1:8005'
const PROXY =
  process.env.UI_TEST_URL == null && process.env.CI == null
    ? { server: 'http://api:8000' }
    : undefined
const WEB_SERVER =
  process.env.UI_TEST_URL == null
    ? {
        command: 'npm run build && npm run preview',
        url: URL,
        reuseExistingServer: process.env.CI == null,
        timeout: 120000, // Two minutes
      }
    : undefined
const UI_TEST_BROWSER = process.env.UI_TEST_BROWSER ?? 'chromium'
const BROWSERS: any = {
  chromium: {
    name: 'chromium',
    use: { ...devices['Desktop Chrome'] },
  },
  firefox: {
    name: 'firefox',
    use: { ...devices['Desktop Firefox'] },
  },
  webkit: {
    name: 'webkit',
    use: { ...devices['Desktop Safari'] },
  },
}
const BROWSER = BROWSERS[UI_TEST_BROWSER]
const PROJECTS =
  process.env.UI_TEST_BROWSER == null || BROWSER == null
    ? Object.values(BROWSERS)
    : [BROWSER]

/**
 * See https://playwright.dev/docs/test-configuration.
 */
export default defineConfig({
  testDir: 'tests',
  /* Run tests in files in parallel */
  fullyParallel: true,
  /* Fail the build on CI if you accidentally left test.only in the source code. */
  forbidOnly: process.env.CI != null,
  /* Retry on CI only */
  retries: process.env.CI != null ? 2 : 0,
  /* Opt out of parallel tests on CI. */
  workers: process.env.CI != null ? 1 : undefined,
  /* Reporter to use. See https://playwright.dev/docs/test-reporters */
  reporter: 'html',
  /* Shared settings for all the projects below. See https://playwright.dev/docs/api/class-testoptions. */
  use: {
    /* Base URL to use in actions like `await page.goto('/')`. */
    baseURL: URL,

    /* Collect trace when retrying the failed test. See https://playwright.dev/docs/trace-viewer */
    trace: 'on-first-retry',

    proxy: PROXY,
  },

  /* Configure projects for major browsers */
  projects: PROJECTS,

  /* Run your local dev server before starting the tests */
  webServer: WEB_SERVER,
})
