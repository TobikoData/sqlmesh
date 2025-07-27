import path from 'path'
import {
  openServerPage,
  runCommand,
  SUSHI_SOURCE_PATH,
  waitForLoadedSQLMesh,
} from './utils'
import os from 'os'
import { test } from './fixtures'
import fs from 'fs-extra'
import { createPythonInterpreterSettingsSpecifier } from './utils_code_server'

test('Stop server works', async ({ page, sharedCodeServer }) => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'))
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  await createPythonInterpreterSettingsSpecifier(tempDir)

  // Navigate to code-server instance
  await openServerPage(page, tempDir, sharedCodeServer)
  await page.waitForSelector('[role="application"]', { timeout: 10000 })

  // Wait for the models folder to be visible in the file explorer
  await page.waitForSelector('text=models')

  // Click on the models folder, excluding external_models
  await page
    .getByRole('treeitem', { name: 'models', exact: true })
    .locator('a')
    .click()

  // Open the customers.sql model
  await page
    .getByRole('treeitem', { name: 'customers.sql', exact: true })
    .locator('a')
    .click()

  await page.waitForSelector('text=grain')
  await waitForLoadedSQLMesh(page)

  // Stop the server
  await runCommand(page, 'SQLMesh: Stop Server')

  // Await LSP server stopped message
  await page.waitForSelector('text=LSP server stopped')

  // Render the model
  await runCommand(page, 'SQLMesh: Render Model')

  // Await error message
  await page.waitForSelector(
    'text="Failed to render model: LSP client not ready."',
  )
})
