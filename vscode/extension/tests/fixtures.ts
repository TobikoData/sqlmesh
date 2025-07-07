import { test as base } from '@playwright/test'
import path from 'path'
import fs from 'fs-extra'
import os from 'os'
import {
  startCodeServer,
  stopCodeServer,
  CodeServerContext,
} from './utils_code_server'

// Worker-scoped fixture to start/stop VS Code server once per worker
// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export const test = base.extend<{}, { sharedCodeServer: CodeServerContext }>({
  sharedCodeServer: [
    // eslint-disable-next-line no-empty-pattern
    async ({}, use) => {
      // Create a temporary directory for the shared server
      const tempDir = await fs.mkdtemp(
        path.join(os.tmpdir(), 'vscode-test-shared-server-'),
      )

      // Start the code server once per worker
      const context = await startCodeServer({
        tempDir,
      })

      console.log(
        `Started shared VS Code server for worker ${test.info().workerIndex} on port ${context.codeServerPort}`,
      )

      // Provide the context to all tests in this worker
      await use(context)

      // Clean up after all tests in this worker are done
      console.log(`Stopping shared VS Code server`)
      await stopCodeServer(context)
    },
    { scope: 'worker', auto: true },
  ],
})

// Export expect and Page from Playwright for convenience
export { expect, Page } from '@playwright/test'
