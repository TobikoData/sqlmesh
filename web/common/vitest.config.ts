import { configDefaults, defineConfig } from 'vitest/config'

import viteConfig from './vite.config.js'

process.env.TZ = 'America/Los_Angeles' // set timezone to UTC-7 (UTC-8 depending on DST) for tests

export default defineConfig({
  ...viteConfig,
  test: {
    testTimeout: 10000,
    browser: {
      provider: 'playwright',
      enabled: true,
      headless: true,
      instances: [
        {
          browser: 'chromium',
        },
      ],
    },
    exclude: [...configDefaults.exclude],
    setupFiles: ['./tests/setup.ts'],
  },
})
