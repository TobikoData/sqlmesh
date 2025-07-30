import {
  openServerPage,
  runCommand,
  SUSHI_SOURCE_PATH,
  waitForLoadedSQLMesh,
} from './utils'
import { test } from './fixtures'
import fs from 'fs-extra'
import { createPythonInterpreterSettingsSpecifier } from './utils_code_server'

test('Stop server works', async ({ page, sharedCodeServer, tempDir }) => {
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  await createPythonInterpreterSettingsSpecifier(tempDir)

  // Navigate to code-server instance
  await openServerPage(page, tempDir, sharedCodeServer)

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

test('Stopped server only restarts when explicitly requested', async ({
  page,
  sharedCodeServer,
  tempDir,
}) => {
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  await createPythonInterpreterSettingsSpecifier(tempDir)

  // Navigate to code-server instance
  await openServerPage(page, tempDir, sharedCodeServer)

  // Wait for the models folder to be visible in the file explorer
  await page.waitForSelector('text=models')

  // Click on the models folder, excluding external_models
  await page
    .getByRole('treeitem', { name: 'models', exact: true })
    .locator('a')
    .click()

  // Open the customers.sql model
  await page
    .getByRole('treeitem', { name: 'marketing.sql', exact: true })
    .locator('a')
    .click()
  await page.waitForSelector('text=grain')
  await waitForLoadedSQLMesh(page)

  // Click on sushi.raw_marketing
  await page.getByText('sushi.raw_marketing;').click()

  // Open the preview hover
  await runCommand(page, 'Show Definition Preview Hover')

  // Assert that the hover is visible with text "Table of marketing status."
  await page.waitForSelector('text=Table of marketing status.', {
    timeout: 5_000,
    state: 'visible',
  })

  // Hit Esc to close the hover
  await page.keyboard.press('Escape')

  // Assert that the hover is no longer visible
  await page.waitForSelector('text=Table of marketing status.', {
    timeout: 5_000,
    state: 'hidden',
  })

  // Stop the server
  await runCommand(page, 'SQLMesh: Stop Server')

  // Await LSP server stopped message
  await page.waitForSelector('text=LSP server stopped')

  // Open the preview hover again
  await runCommand(page, 'Show Definition Preview Hover')

  // Assert that the hover is not visible
  await page.waitForSelector('text=Table of marketing status.', {
    timeout: 5_000,
    state: 'hidden',
  })

  // Restart the server explicitly
  await runCommand(page, 'SQLMesh: Restart Server')

  // Await LSP server started message
  await waitForLoadedSQLMesh(page)

  // Open the preview hover again
  await runCommand(page, 'Show Definition Preview Hover')

  // Assert that the hover is visible with text "Table of marketing status."
  await page.waitForSelector('text=Table of marketing status.', {
    timeout: 5_000,
    state: 'visible',
  })
})
